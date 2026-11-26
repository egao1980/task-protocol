(in-package #:task-protocol)

;;; 0.3.0 (given). A recursive lock for the in-memory job queue.
;;;
;;; Core keeps :DEPENDS-ON () — no threading library. This is the whole
;;; shim: the implementation's own recursive mutex, nothing else. A
;;; subscriber callback runs while the queue lock is held and may call
;;; back into the queue (JOB-STATUS, UNSUBSCRIBE-JOB-EVENTS), so the lock
;;; must be recursive. On an implementation without threads it is a no-op.

(defun %make-queue-lock (&optional (name "task-protocol job queue"))
  #+sbcl (sb-thread:make-mutex :name name)
  #+ecl (mp:make-lock :name name :recursive t)
  #+ccl (ccl:make-lock name)
  #-(or sbcl ecl ccl) (progn name nil))

(defmacro %with-queue-lock ((lock) &body body)
  "Run BODY holding LOCK (from %MAKE-QUEUE-LOCK). Re-entrant."
  #+sbcl `(sb-thread:with-recursive-lock (,lock) ,@body)
  #+ecl `(mp:with-lock (,lock) ,@body)
  #+ccl `(ccl:with-lock-grabbed (,lock) ,@body)
  #-(or sbcl ecl ccl) `(progn ,lock ,@body))
