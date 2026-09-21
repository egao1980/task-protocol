(in-package #:task-protocol/tests)

(deftest runtime-transition-does-not-mutate-status
  (let ((journal (task-protocol:make-in-memory-journal))
        (task (task-protocol:make-durable-task :id "rt-1")))
    (task-protocol:append-event
     journal (make-instance 'task-protocol:task-started :task-id "rt-1"))
    (task-protocol:journal-runtime-transition
     journal :task-id "rt-1" :runtime-id "demo"
     :from-phase :pending :to-phase :running
     :snapshot-ref "podman:/tmp/demo" :worker-id "podman"
     :secret-refs '((:name "vault" :key "token" :inject :env))
     :payload '(:authorization "Bearer hunter2" :ok t))
    (task-protocol:replay-journal journal task)
    (ok (eq :running (task-protocol:durable-task-status task)))
    (let ((ev (find-if #'task-protocol:runtime-transition-p
                       (task-protocol:journal-events journal task))))
      (ok (eq :running (task-protocol:runtime-transition-to ev)))
      (ok (equal "demo" (task-protocol:runtime-transition-id ev)))
      (ok (equal '((:name "vault" :key "token" :inject :env))
                 (task-protocol:runtime-transition-secret-refs ev)))
      (ok (equal '(:secret-ref "authorization")
                 (getf (task-protocol:runtime-transition-payload ev)
                       :authorization)))
      (ok (eq t (getf (task-protocol:runtime-transition-payload ev) :ok)))
      (ng (search "hunter2"
                  (prin1-to-string (task-protocol:event-plist ev))
                  :test #'char-equal)))))

(deftest runtime-transition-plist-roundtrip
  (let* ((ev (task-protocol:make-runtime-transition
              :task-id "rt-2" :runtime-id "ws"
              :from-phase :running :to-phase :suspended
              :snapshot-ref "host:/ws" :worker-id "podman"
              :secret-refs '((:name "v" :key "k" :inject :file))))
         (copy (task-protocol:event-from-plist (task-protocol:event-plist ev))))
    (ok (task-protocol:runtime-transition-p copy))
    (ok (eq :suspended (task-protocol:runtime-transition-to copy)))
    (ok (equal "ws" (task-protocol:runtime-transition-id copy)))
    (ok (equal "host:/ws" (task-protocol:runtime-transition-snapshot-ref copy)))))
