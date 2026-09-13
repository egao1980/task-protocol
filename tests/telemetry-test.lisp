(in-package #:task-protocol/tests)

(defun %tel-attr (attrs key)
  (loop for (k v) on attrs by #'cddr
        when (equal k key)
          return v))

(defun %tel-span (spans name)
  (find name spans :key #'telemetry-protocol:telemetry-span-name :test #'equal))

(defun %tel-event (span name)
  (find name (telemetry-protocol:telemetry-span-events span)
        :key #'telemetry-protocol:telemetry-event-name :test #'equal))

(defmacro with-recording-task-telemetry (&body body)
  `(let ((telemetry-protocol:*telemetry-backend*
           (telemetry-protocol:make-recording-telemetry-backend))
         (telemetry-protocol:*tracer-provider* nil)
         (telemetry-protocol:*current-span* nil)
         (telemetry-protocol:*current-trace-id* nil)
         (task-protocol/telemetry:*task-span-link-trace-id* nil)
         (task-protocol/telemetry:*task-span-link-span-id* nil))
     ,@body))

(deftest step-emits-span-event
  (with-recording-task-telemetry
    (let ((journal (task-protocol:make-in-memory-journal))
          (task (task-protocol:make-durable-task :id "t-tel")))
      (telemetry-protocol:with-span ("task.run")
        (task-protocol:with-durable-task (task journal)
          (task-protocol:with-durable-step ("inc")
            1)))
      (let* ((span (%tel-span (telemetry-protocol:recorded-spans
                               telemetry-protocol:*telemetry-backend*)
                              "task.run"))
             (ev (and span (%tel-event span "step-completed")))
             (attrs (and ev (telemetry-protocol:telemetry-event-attributes ev))))
        (ok span)
        (ok ev)
        (ok (equal "inc" (%tel-attr attrs "task.step.name")))
        (ok (equal "t-tel" (%tel-attr attrs "task.id")))
        (ok (equal "step-completed" (%tel-attr attrs "task.event.type")))))))

(deftest timer-set-emits-span-event
  (with-recording-task-telemetry
    (let ((journal (task-protocol:make-in-memory-journal))
          (task (task-protocol:make-durable-task :id "t-timer"))
          (wake (+ (get-universal-time) 10)))
      (telemetry-protocol:with-span ("task.run")
        (task-protocol:with-durable-task (task journal)
          (task-protocol:schedule-wake task wake)))
      (let* ((span (%tel-span (telemetry-protocol:recorded-spans
                               telemetry-protocol:*telemetry-backend*)
                              "task.run"))
             (ev (and span (%tel-event span "timer-set")))
             (attrs (and ev (telemetry-protocol:telemetry-event-attributes ev))))
        (ok ev)
        (ok (equal wake (%tel-attr attrs "task.timer.time")))))))

(deftest resume-step-links-prior-span
  (with-recording-task-telemetry
    (let ((journal (task-protocol:make-in-memory-journal))
          (task (task-protocol:make-durable-task :id "t-link")))
      (telemetry-protocol:with-span ("run-1")
        (task-protocol:with-durable-task (task journal)
          (task-protocol:with-durable-step ("a")
            :one)))
      (let ((first-tid task-protocol/telemetry:*task-span-link-trace-id*)
            (first-sid task-protocol/telemetry:*task-span-link-span-id*))
        (ok (stringp first-tid))
        (ok (stringp first-sid))
        (telemetry-protocol:with-span ("run-2")
          (task-protocol:with-durable-task (task journal)
            (task-protocol:with-durable-step ("a")
              :replayed)
            (task-protocol:with-durable-step ("b")
              :two)))
        (let* ((span (%tel-span (telemetry-protocol:recorded-spans
                                 telemetry-protocol:*telemetry-backend*)
                                "run-2"))
               (ev (and span (%tel-event span "step-completed")))
               (attrs (and ev (telemetry-protocol:telemetry-event-attributes ev))))
          (ok span)
          (ok ev)
          (ok (equal "b" (%tel-attr attrs "task.step.name")))
          (ok (equal first-tid (%tel-attr attrs "link.trace_id")))
          (ok (equal first-sid (%tel-attr attrs "link.span_id"))))))))
