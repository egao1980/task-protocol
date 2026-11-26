(in-package #:task-protocol)

(define-condition task-error (error)
  ((message :initarg :message :reader task-error-message :initform nil)
   (task :initarg :task :reader task-error-task :initform nil))
  (:report (lambda (c s)
             (format s "task error~@[: ~a~]" (task-error-message c)))))

(define-condition task-replay-divergence (task-error)
  ((journal-hash :initarg :journal-hash
                 :reader task-replay-divergence-journal-hash
                 :initform nil)
   (expected :initarg :expected :reader task-replay-divergence-expected
             :initform nil)
   (actual :initarg :actual :reader task-replay-divergence-actual
           :initform nil))
  (:report (lambda (c s)
             (format s "task replay divergence~@[: ~a~]~@[ (journal-hash ~s)~]"
                     (task-error-message c)
                     (task-replay-divergence-journal-hash c)))))

(define-condition task-serialization-error (task-error)
  ((value :initarg :value :reader task-serialization-error-value :initform nil))
  (:report (lambda (c s)
             (format s "task value is not serializable: ~s~@[: ~a~]"
                     (task-serialization-error-value c)
                     (task-error-message c)))))

(define-condition task-timeout (task-error)
  ((deadline :initarg :deadline :reader task-timeout-deadline :initform nil))
  (:report (lambda (c s)
             (format s "task timed out~@[ at ~s~]~@[: ~a~]"
                     (task-timeout-deadline c)
                     (task-error-message c)))))

(define-condition task-unknown-codec (task-error)
  ((codec :initarg :codec :reader task-unknown-codec-name :initform nil)
   (value :initarg :value :reader task-unknown-codec-value :initform nil))
  (:report (lambda (c s)
             (format s "unknown event codec~@[ ~s~]~@[: ~a~]"
                     (task-unknown-codec-name c)
                     (task-error-message c)))))

;;; 0.3.0 job queue conditions (given). Restarts are established by the
;;; signalling operation, documented on each generic function in jobs.lisp:
;;;   JOB-NOT-FOUND          USE-VALUE  — the value the operation returns
;;;   JOB-STATE-ERROR        CONTINUE   — no change; return the current snapshot
;;;   JOB-LEASE-LOST         CONTINUE   — (subclass of JOB-STATE-ERROR)
;;;   UNKNOWN-RESOURCE-CLASS USE-VALUE  — another class name (or NIL), re-checked
;;;   JOB-PROGRESS-ERROR     USE-VALUE  — another fraction, re-checked
;;;   TASK-SERIALIZATION-ERROR USE-VALUE (payload / result, via CANONICALIZE-VALUE)
;;; A subscriber callback that signals is wrapped in a CONTINUE restart
;;; (skip that delivery) — see SUBSCRIBE-JOB-EVENTS.

(define-condition job-error (task-error)
  ((job-id :initarg :job-id :reader job-error-job-id :initform nil))
  (:report (lambda (c s)
             (format s "job error~@[ ~s~]~@[: ~a~]"
                     (job-error-job-id c) (task-error-message c)))))

(define-condition job-not-found (job-error)
  ()
  (:report (lambda (c s)
             (format s "no job ~s in this queue~@[: ~a~]"
                     (job-error-job-id c) (task-error-message c)))))

(define-condition job-state-error (job-error)
  ((state :initarg :state :reader job-state-error-state :initform nil
          :documentation "The job's state when OPERATION was attempted.")
   (operation :initarg :operation :reader job-state-error-operation
              :initform nil
              :documentation "Keyword: :cancel :complete :fail :heartbeat
               :acknowledge-cancel :progress."))
  (:report (lambda (c s)
             (format s "job ~s is ~s; ~(~a~) is not allowed~@[: ~a~]"
                     (job-error-job-id c)
                     (job-state-error-state c)
                     (job-state-error-operation c)
                     (task-error-message c)))))

(define-condition job-lease-lost (job-state-error)
  ((worker-id :initarg :worker-id :reader job-lease-lost-worker-id
              :initform nil
              :documentation "The worker that attempted the operation.")
   (owner :initarg :owner :reader job-lease-lost-owner :initform nil
          :documentation "The worker that holds the job now."))
  (:report (lambda (c s)
             (format s "worker ~s does not hold job ~s (held by ~s); ~
                        ~(~a~) refused~@[: ~a~]"
                     (job-lease-lost-worker-id c)
                     (job-error-job-id c)
                     (job-lease-lost-owner c)
                     (job-state-error-operation c)
                     (task-error-message c)))))

(define-condition unknown-resource-class (job-error)
  ((name :initarg :name :reader unknown-resource-class-name :initform nil))
  (:report (lambda (c s)
             (format s "unknown resource class ~s~@[: ~a~]"
                     (unknown-resource-class-name c)
                     (task-error-message c)))))

(define-condition job-progress-error (job-error)
  ((fraction :initarg :fraction :reader job-progress-error-fraction
             :initform nil))
  (:report (lambda (c s)
             (format s "progress fraction ~s is not NIL or a real in [0, 1]~
                        ~@[: ~a~]"
                     (job-progress-error-fraction c)
                     (task-error-message c)))))
