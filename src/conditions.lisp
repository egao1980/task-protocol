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
