(in-package #:task-protocol/tests)

(deftest name-and-old-key-still-dedupes
  (let ((counter 0)
        (journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-compat")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step ("execute" :idempotency-key "ksar/foo")
              (incf counter)
              :first)
            (task-protocol:with-durable-step ("execute" :idempotency-key "ksar/foo")
              (incf counter)
              :second)))
    (ok (= 1 counter))
    (let ((steps (remove-if-not (lambda (e)
                                  (typep e 'task-protocol:step-completed))
                                (task-protocol:journal-events journal task))))
      (ok (= 1 (length steps)))
      (ok (eq :first (task-protocol:step-result (first steps)))))))

(deftest different-activation-id-executes-twice
  (let ((counter 0)
        (journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-acts"))
        (a1 (task-protocol:make-activation-id "act-1"))
        (a2 (task-protocol:make-activation-id "act-2")))
    (%run journal task
          (lambda ()
            (ok (eq :one
                    (task-protocol:with-durable-step
                        ("execute" :idempotency-key "ksar/foo"
                         :activation-id a1)
                      (incf counter)
                      :one)))
            (ok (eq :two
                    (task-protocol:with-durable-step
                        ("execute" :idempotency-key "ksar/foo"
                         :activation-id a2)
                      (incf counter)
                      :two)))))
    (ok (= 2 counter))
    (let ((steps (remove-if-not (lambda (e)
                                  (typep e 'task-protocol:step-completed))
                                (task-protocol:journal-events journal task))))
      (ok (= 2 (length steps))))))

(deftest same-activation-id-replay-skips-thunk
  (let ((counter 0)
        (journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-replay-act"))
        (run (task-protocol:make-run-id "run-9"))
        (act (task-protocol:make-activation-id "act-9")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step
                ("execute" :idempotency-key "ksar/foo"
                 :run-id run :activation-id act)
              (incf counter)
              :live)))
    (ok (= 1 counter))
    (let ((journal2 (task-protocol:copy-in-memory-journal journal))
          (task2 (task-protocol:make-durable-task :id "t-replay-act")))
      (%run journal2 task2
            (lambda ()
              (ok (eq :live
                      (task-protocol:with-durable-step
                          ("execute" :idempotency-key "ksar/foo"
                           :run-id run :activation-id act)
                        (incf counter)
                        :must-not-run)))))
      (ok (= 1 counter)))))

(deftest effect-receipt-distinct-from-step-result
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-receipt"))
        (act (task-protocol:make-activation-id "act-r")))
    (%run journal task
          (lambda ()
            (let ((got (task-protocol:with-durable-step
                           ("execute" :idempotency-key "ksar/foo"
                            :activation-id act)
                         (task-protocol:record-effect-receipt
                          task
                          (task-protocol:make-effect-receipt
                           :idempotency-key "write-board"
                           :payload (list :section "ks" :n 1)
                           :activation-id act))
                         :activation-ok)))
              (ok (eq :activation-ok got)))))
    (let* ((events (task-protocol:journal-events journal task))
           (steps (remove-if-not (lambda (e)
                                   (typep e 'task-protocol:step-completed))
                                 events))
           (receipts (remove-if-not (lambda (e)
                                      (typep e 'task-protocol:effect-receipt))
                                    events))
           (receipt (first receipts)))
      (ok (= 1 (length steps)))
      (ok (= 1 (length receipts)))
      (ok (eq :activation-ok (task-protocol:step-result (first steps))))
      (ok (not (eq :activation-ok (task-protocol:effect-receipt-payload receipt))))
      (ok (equal (list :section "ks" :n 1)
                 (task-protocol:effect-receipt-payload receipt)))
      (ok (integerp (task-protocol:effect-receipt-payload-hash receipt)))
      (ok (eql (task-protocol:payload-hash (list :section "ks" :n 1))
               (task-protocol:effect-receipt-payload-hash receipt)))
      (let ((found (task-protocol:find-effect-receipt
                    task "write-board" :activation-id act)))
        (ok (typep found 'task-protocol:effect-receipt)))
      (%run journal task
            (lambda ()
              (let ((again (task-protocol:record-effect-receipt
                            task
                            (task-protocol:make-effect-receipt
                             :idempotency-key "write-board"
                             :payload (list :section "ks" :n 99)
                             :activation-id act))))
                (ok (equal (list :section "ks" :n 1)
                           (task-protocol:effect-receipt-payload again))))))
      (ok (= 1 (length (remove-if-not
                        (lambda (e) (typep e 'task-protocol:effect-receipt))
                        (task-protocol:journal-events journal task))))))))

(deftest version-stamp-roundtrip
  (let* ((ev (make-instance 'task-protocol:step-completed
                            :task-id "t"
                            :name "n"
                            :result (list :x 1)
                            :idempotency-key "k"
                            :run-id (task-protocol:make-run-id "run-1")
                            :activation-id (task-protocol:make-activation-id "act-1")
                            :schema-version "0.2.0"
                            :code-version "code-9"
                            :config-version "cfg-3"))
         (plist (task-protocol:event-plist ev))
         (via-plist (task-protocol:event-from-plist plist))
         (encoded (task-protocol:encode-payload plist))
         (decoded (task-protocol:decode-payload encoded)))
    (ok (equal "0.2.0" (getf plist :schema-version)))
    (ok (equal "code-9" (getf plist :code-version)))
    (ok (equal "cfg-3" (getf plist :config-version)))
    (ok (equal "run-1" (getf plist :run-id)))
    (ok (equal "act-1" (getf plist :activation-id)))
    (ok (equal "0.2.0" (task-protocol:event-schema-version via-plist)))
    (ok (equal "run-1" (task-protocol:run-id-value
                        (task-protocol:event-run-id via-plist))))
    (ok (equal "act-1" (task-protocol:activation-id-value
                        (task-protocol:event-activation-id via-plist))))
    (ok (stringp encoded))
    (when (listp decoded)
      (let ((via-payload (task-protocol:event-from-plist decoded)))
        (ok (equal "0.2.0" (task-protocol:event-schema-version via-payload)))
        (ok (equal "code-9" (task-protocol:event-code-version via-payload)))
        (ok (equal "cfg-3" (task-protocol:event-config-version via-payload)))))))

(deftest schema-version-mismatch-signals
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-ver"))
        (act (task-protocol:make-activation-id "act-v")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step
                ("execute" :activation-id act :schema-version "0.2.0")
              1)))
    (let ((j2 (task-protocol:copy-in-memory-journal journal))
          (task2 (task-protocol:make-durable-task :id "t-ver")))
      (ok (signals
           (%run j2 task2
                 (lambda ()
                   (task-protocol:with-durable-step
                       ("execute" :activation-id act :schema-version "0.3.0")
                     2)))
           'task-protocol:task-replay-divergence)))
    (let ((j3 (task-protocol:copy-in-memory-journal journal))
          (task3 (task-protocol:make-durable-task :id "t-ver"))
          (got nil))
      (handler-bind ((task-protocol:task-replay-divergence
                      (lambda (c)
                        (let ((r (find-restart 'continue c)))
                          (when r (invoke-restart r))))))
        (%run j3 task3
              (lambda ()
                (setf got (task-protocol:with-durable-step
                              ("execute" :activation-id act
                               :schema-version "0.3.0")
                            2)))))
      (ok (eql 1 got)))))
