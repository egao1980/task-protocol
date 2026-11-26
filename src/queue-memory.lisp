(in-package #:task-protocol)

;;; 0.3.0 in-memory job queue.
;;;
;;; Thread-safe: every method runs under the queue's recursive lock
;;; (%WITH-QUEUE-LOCK, lock.lisp), so concurrent CLAIM-JOBs from several
;;; threads never exceed a resource-class limit nor claim one job twice.
;;; Events go to JOURNAL (an IN-MEMORY-JOURNAL by default) and are
;;; delivered to subscribers in the appending thread, lock held.
;;;
;;; Representation: one table id → record, one vector of records in
;;; submission order (the FIFO key; lease expiry never moves a record, so a
;;; re-queued job keeps its place) and a per-class count of :running jobs.
;;; Every state change goes through %TRANSITION, the only writer of that
;;; count and the only place a job leaves or enters :running.
;;;
;;; Invariant (whenever the lock is free): USAGE[c] is the number of
;;; :running records of class c, and each subscription has been handed
;;; exactly its job's events with AFTER-SEQ < SEQ <= LAST-SEQ, in order.

(defclass in-memory-job-queue (job-queue)
  ((lock :initform (%make-queue-lock) :reader %job-queue-lock)
   (jobs :initform (make-hash-table :test 'equal) :reader %job-table
         :documentation "Job id → %JOB-RECORD.")
   (order :initform (make-array 16 :adjustable t :fill-pointer 0)
          :reader %job-order
          :documentation "Records in submission order.")
   (next-id :initform 0 :accessor %job-next-id)
   (classes :initform '() :accessor %job-classes
            :documentation "RESOURCE-CLASSes in definition order.")
   (usage :initform (make-hash-table :test 'eq) :reader %job-usage
          :documentation "Class name → number of :running jobs.")
   (subscriptions :initform (make-hash-table :test 'equal)
                  :reader %job-subscriptions
                  :documentation "Job id → list of JOB-SUBSCRIPTIONs."))
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

;;; Records and helpers (private)

(defclass %job-record (job)
  ((cancel-reason :initform nil :accessor %job-cancel-reason
                  :documentation "REASON given to the first CANCEL-JOB."))
  (:documentation "The live, mutable record behind a job. Never returned:
   operations hand out %COPY-JOB snapshots."))

(defun %read-restart-value (prompt)
  "Interactive reader for a USE-VALUE restart."
  (format *query-io* "~a: " prompt)
  (force-output *query-io*)
  (list (read *query-io*)))

(defmacro %with-job ((var queue job-id) &body body)
  "Run BODY under QUEUE's lock with VAR bound to JOB-ID's record. An
   unknown id signals JOB-NOT-FOUND; the value given to its USE-VALUE
   restart is returned from the whole form (i.e. from the operation)."
  (let ((q (gensym "QUEUE")) (id (gensym "ID")) (blk (gensym "WITH-JOB")))
    `(let ((,q ,queue) (,id ,job-id))
       (block ,blk
         (%with-queue-lock ((%job-queue-lock ,q))
           (let ((,var (or (gethash ,id (%job-table ,q))
                           (restart-case (error 'job-not-found :job-id ,id)
                             (use-value (value)
                               :report "Return a value from the operation instead."
                               :interactive (lambda ()
                                              (%read-restart-value "Value"))
                               (return-from ,blk value))))))
             ,@body))))))

(defun %now (now)
  (or now (get-universal-time)))

(defun %worker-refused-p (rec worker-id operation)
  "NIL when WORKER-ID holds the :running REC. Otherwise signal
   JOB-STATE-ERROR (not :running) or JOB-LEASE-LOST (another holder) with
   a CONTINUE restart; → T when CONTINUE was taken. The restart covers
   only the signal, never the work the caller does afterwards."
  (restart-case
      (progn
        (cond ((not (eq (job-state rec) :running))
               (error 'job-state-error :job-id (job-id rec)
                                       :state (job-state rec)
                                       :operation operation))
              ((not (equal (job-worker-id rec) worker-id))
               (error 'job-lease-lost :job-id (job-id rec)
                                      :state :running
                                      :operation operation
                                      :worker-id worker-id
                                      :owner (job-worker-id rec))))
        nil)
    (continue ()
      :report "Leave the job unchanged."
      t)))

(defun %transition (queue rec to reason now
                    &key (worker-id (job-worker-id rec)) set)
  "The one place a job changes state (call with QUEUE's lock held).
   Moves REC's class slot, clears holder and lease when leaving :running,
   applies SET (a plist of JOB slot names and values), stamps UPDATED-AT
   and, for a terminal state, FINISHED-AT, then journals one
   JOB-TRANSITION through APPEND-JOB-EVENT. → REC."
  (let ((from (job-state rec))
        (class (job-resource-class rec))
        (usage (%job-usage queue)))
    (assert (job-transition-allowed-p from to) ()
            "job ~s: transition ~s → ~s is not allowed" (job-id rec) from to)
    (when class
      (when (eq from :running) (decf (gethash class usage 0)))
      (when (eq to :running) (incf (gethash class usage 0))))
    (when (eq from :running)
      (setf (slot-value rec 'worker-id) nil
            (slot-value rec 'lease-until) nil))
    (loop for (slot value) on set by #'cddr
          do (setf (slot-value rec slot) value))
    (setf (slot-value rec 'state) to
          (slot-value rec 'updated-at) now)
    (when (job-terminal-state-p to)
      (setf (slot-value rec 'finished-at) now))
    (append-job-event queue (job-id rec)
                      (make-instance 'job-transition
                                     :from-state from :to-state to
                                     :worker-id worker-id :reason reason
                                     :timestamp now))
    rec))

(defun %fresh-job-id (queue)
  "\"job-N\", skipping ids the queue or its journal already has, so the
   new job's seqs start at 1."
  (loop for id = (format nil "job-~d" (incf (%job-next-id queue)))
        unless (or (gethash id (%job-table queue))
                   (journal-events (job-queue-journal queue) id))
          return id))

(defun %admissible-p (queue class)
  "T when a job of CLASS (NIL = unlimited) may start now."
  (or (null class)
      (let ((rc (find-resource-class queue class)))
        (or (null rc)
            (< (gethash class (%job-usage queue) 0)
               (resource-class-limit rc))))))

(defun %cancel-why (rec)
  (or (%job-cancel-reason rec) :canceled))

;;; Resource classes

(defmethod define-resource-class ((queue in-memory-job-queue) name &key limit)
  (check-type name keyword)
  (check-type limit (integer 0))
  (%with-queue-lock ((%job-queue-lock queue))
    (let ((rc (find name (%job-classes queue) :key #'resource-class-name)))
      (if rc
          ;; Same position; preempts nothing, only changes admission.
          (setf (slot-value rc 'limit) limit)
          (setf rc (make-instance 'resource-class :name name :limit limit)
                (%job-classes queue) (append (%job-classes queue) (list rc))))
      rc)))

(defmethod find-resource-class ((queue in-memory-job-queue) name)
  (%with-queue-lock ((%job-queue-lock queue))
    (find name (%job-classes queue) :key #'resource-class-name)))

(defmethod list-resource-classes ((queue in-memory-job-queue))
  (%with-queue-lock ((%job-queue-lock queue))
    (copy-list (%job-classes queue))))

(defmethod resource-class-usage ((queue in-memory-job-queue) name)
  (%with-queue-lock ((%job-queue-lock queue))
    (values (gethash name (%job-usage queue) 0))))

;;; Client operations

(defmethod submit-job ((queue in-memory-job-queue) kind
                       &key payload (priority 0) resource-class now)
  (let ((now (%now now)))
    ;; Validate everything first: a failure leaves no id, record or event.
    (check-type priority integer)
    (let ((payload (canonicalize-value payload)))
      (loop while (and resource-class
                       (null (find-resource-class queue resource-class)))
            do (restart-case (error 'unknown-resource-class :name resource-class)
                 (use-value (name)
                   :report "Use another resource class (or NIL); it is checked again."
                   :interactive (lambda () (%read-restart-value "Class name"))
                   (setf resource-class name))))
      ;; Classes are never removed, so the name checked above stays valid.
      (%with-queue-lock ((%job-queue-lock queue))
        (let* ((id (%fresh-job-id queue))
               (rec (make-instance '%job-record
                                   :id id :kind kind :payload payload
                                   :priority priority
                                   :resource-class resource-class
                                   :state nil :submitted-at now :attempts 0)))
          (setf (gethash id (%job-table queue)) rec)
          (vector-push-extend rec (%job-order queue))
          (%transition queue rec :queued :submitted now)
          id)))))

(defmethod job-status ((queue in-memory-job-queue) job-id)
  (%with-job (rec queue job-id)
    (%copy-job rec)))

(defmethod list-jobs ((queue in-memory-job-queue) &key state kind resource-class)
  (%with-queue-lock ((%job-queue-lock queue))
    (loop for rec across (%job-order queue)
          when (and (or (null state)
                        (if (listp state)
                            (member (job-state rec) state)
                            (eq (job-state rec) state)))
                    (or (null kind) (equal (job-kind rec) kind))
                    (or (null resource-class)
                        (eq (job-resource-class rec) resource-class)))
            collect (%copy-job rec))))

(defmethod cancel-job ((queue in-memory-job-queue) job-id &key reason force now)
  (let ((now (%now now))
        (why (or reason :canceled)))
    (%with-job (rec queue job-id)
      (ecase (job-state rec)
        (:queued
         (%transition queue rec :canceled :canceled now :set (list 'failure why)))
        (:running
         (cond (force
                (%transition queue rec :canceled :forced now
                             :set (list 'failure (or reason
                                                     (%job-cancel-reason rec)
                                                     :canceled))))
               ((not (job-cancel-requested rec))
                (setf (slot-value rec 'cancel-requested) t
                      (%job-cancel-reason rec) why
                      (slot-value rec 'updated-at) now))))
        (:canceled nil)
        ((:completed :failed)
         (restart-case (error 'job-state-error :job-id job-id
                                               :state (job-state rec)
                                               :operation :cancel)
           (continue ()
             :report "Leave the job unchanged."
             nil))))
      (%copy-job rec))))

(defmethod cancel-requested-p ((queue in-memory-job-queue) job-id)
  (%with-job (rec queue job-id)
    (or (job-cancel-requested rec)
        (eq (job-state rec) :canceled))))

;;; Worker operations

(defmethod claim-job ((queue in-memory-job-queue) worker-id
                      &key kinds now lease-seconds)
  (let ((now (%now now))
        (lease-seconds (or lease-seconds 30)))
    ;; One critical section: expire, scan, admit, mutate, journal.
    (%with-queue-lock ((%job-queue-lock queue))
      (expire-job-leases queue :now now)
      (let ((best nil))
        ;; Submission order + strict > keeps the earliest of a priority.
        (loop for rec across (%job-order queue)
              when (and (eq (job-state rec) :queued)
                        (or (null best) (> (job-priority rec) (job-priority best)))
                        (or (null kinds)
                            (member (job-kind rec) kinds :test #'equal))
                        (%admissible-p queue (job-resource-class rec)))
                do (setf best rec))
        (when best
          (%transition queue best :running :claimed now
                       :worker-id worker-id
                       :set (list 'worker-id worker-id
                                  'lease-until (+ now lease-seconds)
                                  'started-at now
                                  'attempts (1+ (job-attempts best))))
          (%copy-job best))))))

(defmethod heartbeat-job ((queue in-memory-job-queue) job-id worker-id
                          &key now lease-seconds)
  (let ((now (%now now))
        (lease-seconds (or lease-seconds 30)))
    (%with-job (rec queue job-id)
      (unless (%worker-refused-p rec worker-id :heartbeat)
        (setf (slot-value rec 'lease-until) (+ now lease-seconds)
              (slot-value rec 'updated-at) now))
      (%copy-job rec))))

(defmethod complete-job ((queue in-memory-job-queue) job-id worker-id
                         &key result now)
  (let ((now (%now now)))
    (%with-job (rec queue job-id)
      (unless (%worker-refused-p rec worker-id :complete)
        ;; May signal; nothing has changed yet, so the job stays :running.
        (let ((result (canonicalize-value result)))
          (%transition queue rec :completed :completed now
                       :worker-id worker-id :set (list 'result result))))
      (%copy-job rec))))

(defmethod fail-job ((queue in-memory-job-queue) job-id worker-id
                     &key error now)
  (let ((now (%now now)))
    (%with-job (rec queue job-id)
      (unless (%worker-refused-p rec worker-id :fail)
        (%transition queue rec :failed :failed now
                     :worker-id worker-id
                     :set (list 'failure (if (typep error 'condition)
                                             (princ-to-string error)
                                             error))))
      (%copy-job rec))))

(defmethod acknowledge-cancel ((queue in-memory-job-queue) job-id worker-id
                               &key now)
  (let ((now (%now now)))
    (%with-job (rec queue job-id)
      (unless (%worker-refused-p rec worker-id :acknowledge-cancel)
        (%transition queue rec :canceled :canceled now
                     :worker-id worker-id
                     :set (list 'failure (%cancel-why rec))))
      (%copy-job rec))))

(defmethod expire-job-leases ((queue in-memory-job-queue) &key now)
  (let ((now (%now now)))
    (%with-queue-lock ((%job-queue-lock queue))
      (loop for rec across (%job-order queue)
            when (and (eq (job-state rec) :running)
                      (job-lease-until rec)
                      (< (job-lease-until rec) now))
              collect (progn
                        (if (job-cancel-requested rec)
                            (%transition queue rec :canceled :lease-expired now
                                         :set (list 'failure (%cancel-why rec)))
                            (%transition queue rec :queued :lease-expired now))
                        (%copy-job rec))))))

;;; Progress and subscriptions

(defun %deliver (sub event)
  "Hand EVENT to SUB if it is active and EVENT is past its LAST-SEQ.
   LAST-SEQ moves before the call, so a skipped delivery is not repeated."
  (when (and (job-subscription-active-p sub)
             (> (event-seq event) (job-subscription-last-seq sub)))
    (setf (job-subscription-last-seq sub) (event-seq event))
    (restart-case (funcall (job-subscription-callback sub) event)
      (continue ()
        :report "Skip this delivery; the event stays journaled."
        nil))))

(defun %pump (queue sub &optional event)
  "Deliver to SUB, in SEQ order, every journaled event of its job with
   SEQ > LAST-SEQ. Fast path: EVENT is exactly the next one."
  (if (and event
           (eql (event-seq event) (1+ (job-subscription-last-seq sub))))
      (%deliver sub event)
      (dolist (e (journal-events (job-queue-journal queue)
                                 (job-subscription-job-id sub)))
        (%deliver sub e))))

(defmethod append-job-event ((queue in-memory-job-queue) job-id event)
  (%with-job (rec queue job-id)
    (declare (ignore rec))
    (setf (event-task-id event) job-id)
    (append-event (job-queue-journal queue) event)
    ;; A copy: a callback may unsubscribe.
    (dolist (sub (copy-list (gethash job-id (%job-subscriptions queue))))
      (%pump queue sub event))
    event))

(defmethod report-job-progress ((queue in-memory-job-queue) job-id worker-id
                                &key phase fraction message now)
  (let ((now (%now now)))
    (%with-job (rec queue job-id)
      ;; The state check comes first: CONTINUE there journals nothing.
      (unless (%worker-refused-p rec worker-id :progress)
        (loop until (or (null fraction)
                        (and (realp fraction) (<= 0 fraction 1)))
              do (restart-case (error 'job-progress-error :job-id job-id
                                                          :fraction fraction)
                   (use-value (value)
                     :report "Use another fraction; it is checked again."
                     :interactive (lambda () (%read-restart-value "Fraction"))
                     (setf fraction value))))
        (append-job-event queue job-id
                          (make-instance 'job-progress
                                         :phase phase :fraction fraction
                                         :message message :worker-id worker-id
                                         :timestamp now))))))

(defmethod job-events ((queue in-memory-job-queue) job-id &key after-seq)
  (let ((after-seq (or after-seq 0)))
    (%with-job (rec queue job-id)
      (declare (ignore rec))
      (remove-if-not (lambda (e) (> (event-seq e) after-seq))
                     (journal-events (job-queue-journal queue) job-id)))))

(defmethod subscribe-job-events ((queue in-memory-job-queue) job-id callback
                                 &key after-seq)
  (%with-job (rec queue job-id)
    (declare (ignore rec))
    (let ((sub (make-instance 'job-subscription
                              :job-id job-id :callback callback
                              :last-seq (or after-seq 0))))
      ;; Register, then replay, in one critical section shared with every
      ;; appender. A re-entrant append during replay pumps this
      ;; subscription itself; the replay then skips what it delivered.
      (setf (gethash job-id (%job-subscriptions queue))
            (append (gethash job-id (%job-subscriptions queue)) (list sub)))
      (%pump queue sub)
      sub)))

(defmethod unsubscribe-job-events ((queue in-memory-job-queue) subscription)
  (%with-queue-lock ((%job-queue-lock queue))
    (setf (job-subscription-active-p subscription) nil)
    (let ((id (job-subscription-job-id subscription)))
      (setf (gethash id (%job-subscriptions queue))
            (remove subscription (gethash id (%job-subscriptions queue)))))
    subscription))
