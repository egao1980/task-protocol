(in-package #:task-protocol)

;;; 0.3.0 in-memory job queue (STUBBED: every method below signals
;;; "not implemented"; the class and constructor are given).
;;;
;;; Thread-safe: every method runs under the queue's recursive lock
;;; (%WITH-QUEUE-LOCK, lock.lisp), so concurrent CLAIM-JOBs from several
;;; threads never exceed a resource-class limit nor claim one job twice.
;;; Events go to JOURNAL (an IN-MEMORY-JOURNAL by default) and are
;;; delivered to subscribers in the appending thread, lock held.
;;;
;;; The implementation may add slots to IN-MEMORY-JOB-QUEUE; it must keep
;;; the constructor's lambda list and the protocol semantics in jobs.lisp.

(defclass in-memory-job-queue (job-queue)
  ((lock :initform (%make-queue-lock) :reader %job-queue-lock))
  (:documentation "Job queue held in this image (see jobs.lisp)."))

(defun in-memory-job-queue-p (x)
  (typep x 'in-memory-job-queue))

(defun make-in-memory-job-queue (&key journal resource-classes)
  "A fresh IN-MEMORY-JOB-QUEUE journaling into JOURNAL (default: a new
   IN-MEMORY-JOURNAL). RESOURCE-CLASSES is a list of (NAME LIMIT), each
   passed to DEFINE-RESOURCE-CLASS in order."
  (let ((queue (make-instance 'in-memory-job-queue
                              :journal (or journal (make-in-memory-journal)))))
    (dolist (entry resource-classes queue)
      (destructuring-bind (name limit) entry
        (define-resource-class queue name :limit limit)))))

(defmethod define-resource-class ((queue in-memory-job-queue) name &key limit)
  (declare (ignore name limit))
  (%job-pending 'define-resource-class))

(defmethod find-resource-class ((queue in-memory-job-queue) name)
  (declare (ignore name))
  (%job-pending 'find-resource-class))

(defmethod list-resource-classes ((queue in-memory-job-queue))
  (%job-pending 'list-resource-classes))

(defmethod resource-class-usage ((queue in-memory-job-queue) name)
  (declare (ignore name))
  (%job-pending 'resource-class-usage))

(defmethod submit-job ((queue in-memory-job-queue) kind
                       &key payload priority resource-class now)
  (declare (ignore kind payload priority resource-class now))
  (%job-pending 'submit-job))

(defmethod job-status ((queue in-memory-job-queue) job-id)
  (declare (ignore job-id))
  (%job-pending 'job-status))

(defmethod list-jobs ((queue in-memory-job-queue) &key state kind resource-class)
  (declare (ignore state kind resource-class))
  (%job-pending 'list-jobs))

(defmethod cancel-job ((queue in-memory-job-queue) job-id &key reason force now)
  (declare (ignore job-id reason force now))
  (%job-pending 'cancel-job))

(defmethod cancel-requested-p ((queue in-memory-job-queue) job-id)
  (declare (ignore job-id))
  (%job-pending 'cancel-requested-p))

(defmethod claim-job ((queue in-memory-job-queue) worker-id
                      &key kinds now lease-seconds)
  (declare (ignore worker-id kinds now lease-seconds))
  (%job-pending 'claim-job))

(defmethod heartbeat-job ((queue in-memory-job-queue) job-id worker-id
                          &key now lease-seconds)
  (declare (ignore job-id worker-id now lease-seconds))
  (%job-pending 'heartbeat-job))

(defmethod complete-job ((queue in-memory-job-queue) job-id worker-id
                         &key result now)
  (declare (ignore job-id worker-id result now))
  (%job-pending 'complete-job))

(defmethod fail-job ((queue in-memory-job-queue) job-id worker-id
                     &key error now)
  (declare (ignore job-id worker-id error now))
  (%job-pending 'fail-job))

(defmethod acknowledge-cancel ((queue in-memory-job-queue) job-id worker-id
                               &key now)
  (declare (ignore job-id worker-id now))
  (%job-pending 'acknowledge-cancel))

(defmethod expire-job-leases ((queue in-memory-job-queue) &key now)
  (declare (ignore now))
  (%job-pending 'expire-job-leases))

(defmethod append-job-event ((queue in-memory-job-queue) job-id event)
  (declare (ignore job-id event))
  (%job-pending 'append-job-event))

(defmethod report-job-progress ((queue in-memory-job-queue) job-id worker-id
                                &key phase fraction message now)
  (declare (ignore job-id worker-id phase fraction message now))
  (%job-pending 'report-job-progress))

(defmethod job-events ((queue in-memory-job-queue) job-id &key after-seq)
  (declare (ignore job-id after-seq))
  (%job-pending 'job-events))

(defmethod subscribe-job-events ((queue in-memory-job-queue) job-id callback
                                 &key after-seq)
  (declare (ignore job-id callback after-seq))
  (%job-pending 'subscribe-job-events))

(defmethod unsubscribe-job-events ((queue in-memory-job-queue) subscription)
  (declare (ignore subscription))
  (%job-pending 'unsubscribe-job-events))
