(in-package #:task-protocol/telemetry)

;;; Step/timer journal appends become span events. WITH-DURABLE-STEP is a
;;; macro, so we wrap APPEND-EVENT instead. LINK.TRACE_ID / LINK.SPAN_ID
;;; join a resume span to the previous live-run span (same image).

(defvar *task-span-link-trace-id* nil
  "Trace id of the last instrumented task event. Resume spans link here.")

(defvar *task-span-link-span-id* nil
  "Span id of the last instrumented task event. Resume spans link here.")

(defun instrument-task-step (event &key (span tel:*current-span*)
                                   (backend tel:*telemetry-backend*)
                                   link-trace-id link-span-id)
  "Add EVENT as a span event on SPAN (default *CURRENT-SPAN*).
   LINK-TRACE-ID / LINK-SPAN-ID (or the last recorded pair) are written
   as link.trace_id / link.span_id so a later resume span can point at
   the live-run span. NIL SPAN is a no-op (safe against the no-op backend)."
  (when (and event span)
    (let* ((type (event-type-keyword event))
           (name (string-downcase (symbol-name type)))
           (link-tid (or link-trace-id *task-span-link-trace-id*))
           (link-sid (or link-span-id *task-span-link-span-id*))
           (attrs (append
                   (list "task.id" (or (event-task-id event)
                                       (and *task* (durable-task-id *task*)))
                         "task.event.type" name
                         "task.event.seq" (event-seq event))
                   (when (typep event 'step-completed)
                     (list "task.step.name" (step-name event)
                           "task.step.idempotency_key"
                           (step-idempotency-key event)))
                   (when (or (typep event 'timer-set)
                             (typep event 'timer-fired))
                     (list "task.timer.time" (timer-time event)))
                   (when (and (typep event 'timer-set)
                              (timer-recurring-p event))
                     (list "task.timer.recurring" t))
                   (when link-tid (list "link.trace_id" link-tid))
                   (when link-sid (list "link.span_id" link-sid)))))
      (tel:add-span-event backend span name :attributes attrs)
      (setf *task-span-link-trace-id*
            (or (tel:telemetry-span-trace-id span) (tel:current-trace-id))
            *task-span-link-span-id* (tel:telemetry-span-id span))
      event)))

;;; :after on IN-MEMORY-JOURNAL only — an :around with the same (T T)
;;; specializers would replace the core redaction around.

(defmethod append-event :after ((journal in-memory-journal) event)
  (instrument-task-step event))
