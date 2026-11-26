(defsystem "task-protocol"
  :version "0.3.0"
  :description "CLOS durable-task journal protocol for cl-stack (Temporal-shaped, not a DSL)"
  :author "egao1980"
  :license "MIT"
  :depends-on ()
  :properties (:cl-repo
               (:ci (:with ("task-protocol/telemetry"))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "conditions")
               (:file "events")
               (:file "codec")
               (:file "protocol")
               (:file "journal")
               (:file "lock")
               (:file "jobs")
               (:file "queue-memory"))
  :in-order-to ((test-op (test-op "task-protocol/tests"))))

(defsystem "task-protocol/telemetry"
  :version "0.3.0"
  :description "Span events for durable-task steps and timers"
  :author "egao1980"
  :license "MIT"
  :depends-on ("task-protocol" "telemetry-protocol")
  :serial t
  :pathname "src/telemetry"
  :components ((:file "package")
               (:file "instrument"))
  :in-order-to ((test-op (test-op "task-protocol/tests"))))

(defsystem "task-protocol/tests"
  ;; bordeaux-threads: already in the graph through rove; the queue tests
  ;; claim from several threads.
  :depends-on ("task-protocol" "task-protocol/telemetry" "rove"
               "bordeaux-threads")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "protocol-test")
               (:file "identity-test")
               (:file "codec-test")
               (:file "restarts-test")
               (:file "telemetry-test")
               (:file "runtime-test")
               (:file "job-support")
               (:file "job-queue-test")
               (:file "job-admission-test")
               (:file "job-progress-test")
               (:file "job-conditions-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
