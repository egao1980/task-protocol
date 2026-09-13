(in-package #:task-protocol/tests)

(defun %run (journal task fn)
  (task-protocol:with-durable-task (task journal)
    (funcall fn)))

(deftest kill-and-replay-does-not-reexec
  (let ((counter 0)
        (journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-replay")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step ("inc")
              (incf counter)
              counter)))
    (ok (= 1 counter))
    (let ((journal2 (task-protocol:copy-in-memory-journal journal))
          (task2 (task-protocol:make-durable-task :id "t-replay")))
      (%run journal2 task2
            (lambda ()
              (task-protocol:with-durable-step ("inc")
                (incf counter)
                counter)))
      (ok (= 1 counter))
      (ok (equal 1 (task-protocol:step-result
                    (find-if (lambda (e)
                               (typep e 'task-protocol:step-completed))
                             (task-protocol:journal-events journal2 task2))))))))

(deftest idempotency-key-dedupe
  (let ((counter 0)
        (journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-idemp")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step ("work" :idempotency-key "k1")
              (incf counter)
              :first)
            (task-protocol:with-durable-step ("work" :idempotency-key "k1")
              (incf counter)
              :second)))
    (ok (= 1 counter))
    (let ((steps (remove-if-not (lambda (e)
                                  (typep e 'task-protocol:step-completed))
                                (task-protocol:journal-events journal task))))
      (ok (= 1 (length steps)))
      (ok (eq :first (task-protocol:step-result (first steps)))))))

(deftest timer-rearm-and-fire-due
  (let* ((journal (task-protocol:make-in-memory-journal))
         (task (task-protocol:make-durable-task :id "t-timer"))
         (t0 100000000)
         (fired nil)
         (next nil))
    (%run journal task
          (lambda ()
            (task-protocol:schedule-wake task (+ t0 10))
            (setf next (task-protocol:schedule-recurring
                        task (lambda (last) (+ last 60))
                        :now t0))))
    (ok (eq :waiting (task-protocol:durable-task-status task)))
    (ok (null (task-protocol:fire-due-timers journal t0)))
    (setf fired (task-protocol:fire-due-timers journal (+ t0 10)))
    (ok (>= (length fired) 1))
    (ok (every (lambda (e) (typep e 'task-protocol:timer-fired)) fired))
    (let ((sets (remove-if-not (lambda (e) (typep e 'task-protocol:timer-set))
                               (task-protocol:journal-events journal task))))
      (ok (>= (length sets) 2))
      (ok (find-if #'task-protocol:timer-recurring-p sets)))
    (ok (integerp next))))

(deftest join-children-all-and-any
  (let ((journal (task-protocol:make-in-memory-journal))
        (parent (task-protocol:make-durable-task :id "parent")))
    (%run journal parent
          (lambda ()
            (task-protocol:spawn-child-task
             parent (lambda (in) (list :a in)) :input 1)
            (task-protocol:spawn-child-task
             parent (lambda (in) (list :b in)) :input 2)
            (let ((all (task-protocol:join-children parent :policy :all))
                  (any (task-protocol:join-children parent :policy :any)))
              (ok (equal '((:a 1) (:b 2)) all))
              (ok (or (equal '(:a 1) any) (equal '(:b 2) any)))))))
  (let ((journal (task-protocol:make-in-memory-journal))
        (parent (task-protocol:make-durable-task :id "parent-any")))
    (%run journal parent
          (lambda ()
            (let ((slow (task-protocol:make-durable-task
                         :id "slow" :parent parent :journal journal)))
              (setf (task-protocol:durable-task-status slow) :running)
              (push slow (task-protocol:durable-task-children parent))
              (task-protocol:spawn-child-task
               parent (lambda (in) in) :input :ready)
              (ok (equal :ready
                         (task-protocol:join-children parent :policy :any)))
              (ok (null (task-protocol:join-children parent :policy :all))))))))

(deftest compact-journal-bounds-event-count
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-compact")))
    (%run journal task
          (lambda ()
            (dotimes (i 5)
              (let ((n i))
                (task-protocol:with-durable-step ((format nil "s-~d" n))
                  n)))
            (task-protocol:complete-task task :done)))
    (let ((before (length (task-protocol:journal-events journal task))))
      (ok (> before 3))
      (task-protocol:compact-journal journal task)
      (let ((after (task-protocol:journal-events journal task)))
        (ok (= 1 (length after)))
        (ok (typep (first after) 'task-protocol:journal-snapshot))
        (ok (< (length after) before))))
    (let ((task2 (task-protocol:make-durable-task :id "t-compact"))
          (j2 (task-protocol:copy-in-memory-journal journal)))
      (%run j2 task2
            (lambda ()
              (ok (eql 0 (task-protocol:with-durable-step ("s-0")
                           (error "must not re-exec"))))
              (ok (eq :done (task-protocol:durable-task-result task2))))))))

(deftest redact-event-strips-secrets
  (let* ((policy (task-protocol:make-redaction-policy))
         (event (make-instance 'task-protocol:step-completed
                               :task-id "t"
                               :name "login"
                               :result (list :user "ada"
                                             :password "hunter2"
                                             :authorization "Bearer x"
                                             :api-key "abcd"
                                             :ok t))))
    (task-protocol:redact-event policy event)
    (let ((r (task-protocol:step-result event)))
      (ok (equal "ada" (getf r :user)))
      (ok (eq t (getf r :ok)))
      (ok (equal '(:secret-ref "password") (getf r :password)))
      (ok (equal '(:secret-ref "authorization") (getf r :authorization)))
      (ok (equal '(:secret-ref "api-key") (getf r :api-key)))))
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-redact")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step ("creds")
              (list :secret "inline" :n 1))))
    (let ((step (find-if (lambda (e) (typep e 'task-protocol:step-completed))
                         (task-protocol:journal-events journal task))))
      (ok (equal '(:secret-ref "secret")
                 (getf (task-protocol:step-result step) :secret)))
      (ok (eql 1 (getf (task-protocol:step-result step) :n))))))

(deftest replay-divergence-signals
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-div")))
    (%run journal task
          (lambda ()
            (task-protocol:with-durable-step ("alpha")
              1)))
    (let ((j2 (task-protocol:copy-in-memory-journal journal))
          (task2 (task-protocol:make-durable-task :id "t-div")))
      (ok (signals
           (%run j2 task2
                 (lambda ()
                   (task-protocol:with-durable-step ("beta")
                     2)))
           'task-protocol:task-replay-divergence))
      (handler-case
          (%run (task-protocol:copy-in-memory-journal journal)
                (task-protocol:make-durable-task :id "t-div")
                (lambda ()
                  (task-protocol:with-durable-step ("beta")
                    2)))
        (task-protocol:task-replay-divergence (c)
          (ok (integerp (task-protocol:task-replay-divergence-journal-hash c))))))))

(deftest event-plist-roundtrip
  (let* ((ev (make-instance 'task-protocol:step-completed
                            :task-id "t"
                            :name "n"
                            :result (list :x 1 :y "z")
                            :idempotency-key "k"))
         (copy (task-protocol:event-from-plist (task-protocol:event-plist ev))))
    (ok (typep copy 'task-protocol:step-completed))
    (ok (equal "n" (task-protocol:step-name copy)))
    (ok (equal (list :x 1 :y "z") (task-protocol:step-result copy)))
    (ok (equal "k" (task-protocol:step-idempotency-key copy)))))

(deftest cron-next-fire-minute
  (let* ((t0 (encode-universal-time 0 10 3 1 1 2024 0))
         (next (task-protocol:compute-next-fire '(:minute 15 :hour 3) t0)))
    (multiple-value-bind (sec min hour)
        (decode-universal-time next 0)
      (declare (ignore sec))
      (ok (= 15 min))
      (ok (= 3 hour)))))

(deftest package-nickname
  (ok (eq (find-package '#:task-protocol)
          (find-package '#:stack-task))))
