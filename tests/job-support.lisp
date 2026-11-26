(in-package #:task-protocol/tests)

;;; 0.3.0 job queue contract: shared helpers.
;;;
;;; Conventions (CL / Rove pitfalls this suite avoids):
;;; - Conditions are caught or handled OUTSIDE Rove's OK, whose own ERROR
;;;   handler would otherwise run first; OK only sees plain values.
;;; - Restarts are looked up and invoked INSIDE the handler, while the
;;;   signalling frame is live, by name without the condition argument
;;;   (a restart may be associated with another condition).
;;; - Worker threads record their conditions; nothing signals across a
;;;   thread boundary.

(defun %queue (&rest resource-classes)
  "A fresh in-memory queue; RESOURCE-CLASSES are (NAME LIMIT) lists."
  (stack-task:make-in-memory-job-queue :resource-classes resource-classes))

(defun %catch (thunk)
  "The ERROR THUNK signals, or NIL when it returns normally."
  (handler-case (progn (funcall thunk) nil)
    (error (c) c)))

(defun %call-handling (type handler thunk)
  "Call THUNK with HANDLER (a function of the condition) run for every
   condition of TYPE. → (values THUNK's primary value, the list of TYPE
   conditions seen, oldest first)."
  (let ((seen '()))
    (values (handler-bind ((condition
                             (lambda (c)
                               (when (typep c type)
                                 (push c seen)
                                 (funcall handler c)))))
              (funcall thunk))
            (reverse seen))))

(defun %invoke (name &rest args)
  "Invoke the innermost restart named NAME with ARGS."
  (let ((r (find-restart name)))
    (if r
        (apply #'invoke-restart r args)
        (error "no ~s restart is active" name))))

(defun %restart-names (c)
  (mapcar #'restart-name (compute-restarts c)))

(defun %claimed (queue worker &rest keys)
  "CLAIM-JOB → the claimed job's id (NIL when nothing is claimed)."
  (let ((job (apply #'stack-task:claim-job queue worker keys)))
    (and job (stack-task:job-id job))))

(defun %state (queue id)
  (stack-task:job-state (stack-task:job-status queue id)))

(defun %event-seqs (events)
  (mapcar #'stack-task:event-seq events))

(defun %iota (n &optional (start 1))
  (loop for i from start below (+ start n) collect i))

(defun %transition-pairs (events)
  "(FROM TO) of each JOB-TRANSITION in EVENTS, in order."
  (loop for e in events
        when (stack-task:job-transition-p e)
          collect (list (stack-task:job-transition-from e)
                        (stack-task:job-transition-to e))))

(defun %run-threads (n fn &key (timeout 60))
  "Run (FN I) for I below N, each on its own thread; join them all.
   → the list of conditions the threads signalled (empty when clean).
   A thread still running after TIMEOUT seconds counts as an error."
  (let* ((lock (bt:make-lock "test threads"))
         (errors '())
         (threads
           (loop for i below n
                 collect (let ((i i))
                           (bt:make-thread
                            (lambda ()
                              (handler-case (funcall fn i)
                                (error (c)
                                  (bt:with-lock-held (lock)
                                    (push c errors)))))
                            :name (format nil "job-test-~d" i)))))
         (deadline (+ (get-internal-real-time)
                      (* timeout internal-time-units-per-second))))
    (dolist (th threads)
      (loop while (and (bt:thread-alive-p th)
                       (< (get-internal-real-time) deadline))
            do (sleep 0.01))
      (if (bt:thread-alive-p th)
          (push (make-condition 'simple-error
                                :format-control "thread ~a timed out"
                                :format-arguments (list (bt:thread-name th)))
                errors)
          (bt:join-thread th)))
    errors))

(defun %catch-value (thunk)
  "THUNK's primary value, or the ERROR it signals."
  (handler-case (funcall thunk)
    (error (c) c)))
