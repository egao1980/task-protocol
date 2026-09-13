(in-package #:task-protocol/tests)

(deftest serialization-error-signals
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-ser")))
    (ok (signals
         (task-protocol:with-durable-task (task journal)
           (task-protocol:with-durable-step ("fn")
             #'identity))
         'task-protocol:task-serialization-error))))

(deftest serialization-use-value
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-ser-uv"))
        (got nil))
    (handler-bind ((task-protocol:task-serialization-error
                    (lambda (c)
                      (use-value "ok" c))))
      (task-protocol:with-durable-task (task journal)
        (setf got (task-protocol:with-durable-step ("fn")
                    #'identity))))
    (ok (equal "ok" got))))

(deftest skip-step-restart
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-skip"))
        (got :unset))
    (handler-bind ((error
                    (lambda (c)
                      (declare (ignore c))
                      (let ((r (find-restart 'task-protocol:skip-step)))
                        (when r (invoke-restart r))))))
      (task-protocol:with-durable-task (task journal)
        (setf got (task-protocol:with-durable-step ("boom")
                    (error "nope")))))
    (ok (null got))))

(deftest abort-task-restart
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "t-abort")))
    (handler-bind ((error
                    (lambda (c)
                      (declare (ignore c))
                      (let ((r (find-restart 'task-protocol:abort-task)))
                        (when r (invoke-restart r))))))
      (task-protocol:with-durable-task (task journal)
        (task-protocol:with-durable-step ("boom")
          (error "nope"))))
    (ok (eq :canceled (task-protocol:durable-task-status task)))))

(deftest retry-step-restart
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task
               :id "t-retry"
               :retry-policy (task-protocol:make-retry-policy :max-attempts 3)))
        (n 0)
        (got nil))
    (handler-bind ((error
                    (lambda (c)
                      (declare (ignore c))
                      (when (< n 2)
                        (let ((r (find-restart 'task-protocol:retry-step)))
                          (when r (invoke-restart r)))))))
      (task-protocol:with-durable-task (task journal)
        (setf got (task-protocol:with-durable-step ("flaky")
                    (incf n)
                    (when (< n 2)
                      (error "retry me"))
                    n))))
    (ok (eql 2 got))
    (ok (eql 2 n))))

(deftest task-timeout-signals
  (let ((task (task-protocol:make-durable-task
               :id "t-to"
               :retry-policy (task-protocol:make-retry-policy
                              :timeout (- (get-universal-time) 10)))))
    (ok (signals (task-protocol:check-task-timeout task)
                 'task-protocol:task-timeout))))
