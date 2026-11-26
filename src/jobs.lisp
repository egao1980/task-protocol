(in-package #:task-protocol)

;;; 0.3.0 job queue protocol (given): the job model, resource classes,
;;; admission, leases, progress events and subscriptions. Backend-neutral
;;; semantics live in the generic-function documentation below; this file
;;; is the spec. The in-memory backend is queue-memory.lisp; the SQL
;;; backend (task-backend-sql) implements the same generic functions.
;;;
;;; Model
;;; -----
;;; A JOB is one unit of queued work: ID (a string, unique within its
;;; queue), KIND (any object, compared with EQUAL), PAYLOAD (journaled
;;; data, canonicalized with CANONICALIZE-VALUE), PRIORITY (integer,
;;; larger first, default 0), RESOURCE-CLASS (a keyword naming a class
;;; defined on the queue, or NIL for unlimited), STATE, timestamps
;;; (universal time), RESULT or ERROR, the WORKER-ID and LEASE-UNTIL of
;;; the current claim, ATTEMPTS (number of claims so far) and the
;;; cooperative CANCEL-REQUESTED flag.
;;;
;;; Operations never hand out the live record: every JOB they return is a
;;; snapshot; mutating it changes nothing in the queue.
;;;
;;; States and transitions (+JOB-TRANSITIONS+)
;;; ------------------------------------------
;;;   (none)   → :queued      SUBMIT-JOB
;;;   :queued  → :running     CLAIM-JOB
;;;   :queued  → :canceled    CANCEL-JOB
;;;   :running → :completed   COMPLETE-JOB
;;;   :running → :failed      FAIL-JOB
;;;   :running → :canceled    ACKNOWLEDGE-CANCEL, CANCEL-JOB :FORCE T,
;;;                           lease expiry of a cancel-requested job
;;;   :running → :queued      lease expiry (the job is claimable again)
;;; :completed, :failed and :canceled are terminal.
;;;
;;; Every state change appends exactly one JOB-TRANSITION event to the
;;; job's journal (EVENT-TASK-ID = job id) through APPEND-JOB-EVENT, with
;;; its timestamp set to the operation's NOW. A cancel request on a running
;;; job changes no state and appends nothing. So a job that is submitted,
;;; claimed, reports two progress events and completes has the journal
;;;   seq 1 JOB-TRANSITION nil → :queued
;;;   seq 2 JOB-TRANSITION :queued → :running
;;;   seq 3 JOB-PROGRESS, seq 4 JOB-PROGRESS
;;;   seq 5 JOB-TRANSITION :running → :completed
;;;
;;; Order
;;; -----
;;; CLAIM-JOB takes the admissible :queued job with the largest PRIORITY;
;;; among equal priorities, the earliest submitted (FIFO). A job whose
;;; resource class is full is skipped, not waited on: it does not block
;;; jobs of other classes behind it. A job re-queued by lease expiry keeps
;;; its original submission position.
;;;
;;; Admission
;;; ---------
;;; A resource class has a NAME and a LIMIT (integer ≥ 0; 0 admits
;;; nothing). Its usage is the number of :running jobs of that class.
;;; CLAIM-JOB admits a job only while usage < limit, atomically: under
;;; any number of concurrent claimers, usage never exceeds the limit. The
;;; slot is released when the job leaves :running — complete, fail,
;;; cancel (acknowledged or forced) or lease expiry. A cancel-requested
;;; job still holds its slot until it leaves :running. Lowering a limit
;;; below the current usage preempts nothing; it only stops admission.
;;;
;;; Leases
;;; ------
;;; Same vocabulary as task-backend-sql worker leases: a claim by
;;; WORKER-ID sets LEASE-UNTIL = NOW + LEASE-SECONDS (default 30);
;;; HEARTBEAT-JOB extends it. A lease is expired when LEASE-UNTIL < NOW
;;; (strictly; see LEASE-CLAIMABLE-P in task-backend-sql). Expiry happens
;;; in EXPIRE-JOB-LEASES, which CLAIM-JOB calls first with its own NOW.
;;; Nothing else expires leases implicitly (JOB-STATUS and LIST-JOBS are
;;; pure reads). Worker operations (COMPLETE-JOB, FAIL-JOB, HEARTBEAT-JOB,
;;; ACKNOWLEDGE-CANCEL, REPORT-JOB-PROGRESS) take the WORKER-ID: on a job
;;; that is not :running they signal JOB-STATE-ERROR; on a :running job
;;; held by another worker they signal JOB-LEASE-LOST.
;;;
;;; Time
;;; ----
;;; Every operation that changes or reads time takes :NOW (universal time,
;;; default (GET-UNIVERSAL-TIME)), so tests and replays are deterministic.

(deftype job-state ()
  '(member :queued :running :completed :failed :canceled))

(defparameter +job-terminal-states+ '(:completed :failed :canceled))

(defparameter +job-transitions+
  '((nil :queued)
    (:queued :running :canceled)
    (:running :completed :failed :canceled :queued))
  "Allowed job state changes: each entry is (FROM . TOs). The only place
   the job state machine is written down (a logic-protocol migration
   candidate, see the rules-and-logic note in the README).")

(defun job-transition-allowed-p (from to)
  "T when the job state machine allows FROM → TO."
  (and (member to (rest (assoc from +job-transitions+))) t))

(defun job-terminal-state-p (state)
  (and (member state +job-terminal-states+) t))

;;; Job

(defclass job ()
  ((id :initarg :id :reader job-id)
   (kind :initarg :kind :reader job-kind :initform nil)
   (payload :initarg :payload :reader job-payload :initform nil)
   (priority :initarg :priority :reader job-priority :initform 0)
   (resource-class :initarg :resource-class :reader job-resource-class
                   :initform nil)
   (state :initarg :state :reader job-state :initform :queued)
   (submitted-at :initarg :submitted-at :reader job-submitted-at
                 :initform nil)
   (started-at :initarg :started-at :reader job-started-at :initform nil
               :documentation "NOW of the latest claim.")
   (finished-at :initarg :finished-at :reader job-finished-at :initform nil
                :documentation "NOW of the move to a terminal state.")
   (updated-at :initarg :updated-at :reader job-updated-at :initform nil
               :documentation "NOW of the latest change of any kind.")
   (result :initarg :result :reader job-result :initform nil)
   (failure :initarg :failure :reader job-failure :initform nil
            :documentation "FAIL-JOB's error: a string (a condition is stored
           as its report string), or the cancel reason.")
   (worker-id :initarg :worker-id :reader job-worker-id :initform nil
              :documentation "The holder while :running, else NIL.")
   (lease-until :initarg :lease-until :reader job-lease-until :initform nil
                :documentation "While :running, else NIL.")
   (attempts :initarg :attempts :reader job-attempts :initform 0
             :documentation "Number of claims so far.")
   (cancel-requested :initarg :cancel-requested
                     :reader job-cancel-requested :initform nil
                     :documentation "T once CANCEL-JOB was called on it while
                      :running."))
  (:documentation "A snapshot of one queued job (see the file header)."))

(defun job-p (x)
  (typep x 'job))

(defun %copy-job (job &rest overrides)
  "A fresh JOB with JOB's slots, OVERRIDES (initargs) taking precedence."
  (apply #'make-instance 'job
         (append overrides
                 (list :id (job-id job)
                       :kind (job-kind job)
                       :payload (job-payload job)
                       :priority (job-priority job)
                       :resource-class (job-resource-class job)
                       :state (job-state job)
                       :submitted-at (job-submitted-at job)
                       :started-at (job-started-at job)
                       :finished-at (job-finished-at job)
                       :updated-at (job-updated-at job)
                       :result (job-result job)
                       :failure (job-failure job)
                       :worker-id (job-worker-id job)
                       :lease-until (job-lease-until job)
                       :attempts (job-attempts job)
                       :cancel-requested (job-cancel-requested job)))))

(defmethod print-object ((job job) stream)
  (print-unreadable-object (job stream :type t)
    (format stream "~s ~s ~s" (job-id job) (job-kind job) (job-state job))))

;;; Resource classes

(defclass resource-class ()
  ((name :initarg :name :reader resource-class-name)
   (limit :initarg :limit :reader resource-class-limit))
  (:documentation "A named cap on concurrently :running jobs."))

(defun resource-class-p (x)
  (typep x 'resource-class))

;;; Subscriptions

(defclass job-subscription ()
  ((job-id :initarg :job-id :reader job-subscription-job-id)
   (callback :initarg :callback :reader job-subscription-callback)
   (last-seq :initarg :last-seq :accessor job-subscription-last-seq
             :initform 0
             :documentation "SEQ of the last event delivered (or AFTER-SEQ
              before the first delivery).")
   (active :initarg :active :accessor job-subscription-active-p
           :initform t))
  (:documentation "A live subscription to one job's events."))

;;; Queue

(defclass job-queue ()
  ((journal :initarg :journal :reader job-queue-journal))
  (:documentation "Protocol class of job queues. JOURNAL receives every
   job's events (task-id = job id) through APPEND-EVENT."))

(defun job-queue-p (x)
  (typep x 'job-queue))

(defun %job-pending (name)
  "The stub of every 0.3.0 operation not implemented yet."
  (error 'task-error
         :message (format nil "task-protocol 0.3.0: ~(~a~) is not implemented yet"
                          name)))

;;; Resource class generic functions

(defgeneric define-resource-class (queue name &key limit)
  (:documentation
   "Define or redefine resource class NAME (a keyword) on QUEUE with
    LIMIT, an integer ≥ 0 (CHECK-TYPE: TYPE-ERROR with STORE-VALUE).
    Redefinition changes the limit and preempts nothing.
    → the RESOURCE-CLASS."))

(defgeneric find-resource-class (queue name)
  (:documentation "The RESOURCE-CLASS named NAME on QUEUE, or NIL."))

(defgeneric list-resource-classes (queue)
  (:documentation "QUEUE's RESOURCE-CLASSes, in definition order."))

(defgeneric resource-class-usage (queue name)
  (:documentation
   "Number of :running jobs of class NAME on QUEUE (0 for an unknown or
    unused class)."))

;;; Client generic functions

(defgeneric submit-job (queue kind &key payload priority resource-class now)
  (:documentation
   "Enqueue a job of KIND. → its id, a fresh string unique within QUEUE.
    PAYLOAD goes through CANONICALIZE-VALUE (a non-serializable value
    signals TASK-SERIALIZATION-ERROR; USE-VALUE substitutes). PRIORITY is
    an integer, default 0 (CHECK-TYPE). RESOURCE-CLASS is NIL or the name
    of a class defined on QUEUE; an unknown name signals
    UNKNOWN-RESOURCE-CLASS with a USE-VALUE restart taking another name
    (or NIL), which is checked again. The job starts :queued with
    SUBMITTED-AT = UPDATED-AT = NOW and ATTEMPTS 0, and the journal gets
    JOB-TRANSITION nil → :queued, reason :submitted."))

(defgeneric job-status (queue job-id)
  (:documentation
   "A snapshot JOB for JOB-ID. An unknown id signals JOB-NOT-FOUND with a
    USE-VALUE restart whose value JOB-STATUS returns."))

(defgeneric list-jobs (queue &key state kind resource-class)
  (:documentation
   "Snapshots of QUEUE's jobs in submission order, filtered by STATE (a
    state keyword or a list of them), KIND (EQUAL) and RESOURCE-CLASS
    (EQ); NIL means no filter on that key."))

(defgeneric cancel-job (queue job-id &key reason force now)
  (:documentation
   "Cancel JOB-ID. → the snapshot after the call.
    The cancel reason is REASON, or :canceled when REASON is NIL.
    :queued → :canceled at once: FAILURE = the cancel reason, FINISHED-AT
    = NOW, JOB-TRANSITION reason :canceled; it is never claimed.
    :running → sets CANCEL-REQUESTED, remembers the cancel reason and
    stays :running (no event); the worker sees it through
    CANCEL-REQUESTED-P or a snapshot and calls ACKNOWLEDGE-CANCEL.
    Repeating the request is a no-op. With FORCE T (a worker backend that
    killed the process) a :running job moves to :canceled at once:
    FAILURE = the cancel reason, holder and lease cleared, slot released,
    JOB-TRANSITION reason :forced; the old holder's later worker calls
    signal JOB-STATE-ERROR. FORCE on a :queued job is the plain cancel.
    :canceled → no-op, returns the snapshot.
    :completed / :failed → JOB-STATE-ERROR (operation :cancel) with a
    CONTINUE restart: no change, return the snapshot.
    Unknown id → JOB-NOT-FOUND with USE-VALUE (the value is returned)."))

(defgeneric cancel-requested-p (queue job-id)
  (:documentation
   "T when a running job should stop: CANCEL-JOB was called on it, or it
    is already :canceled. The cooperative flag a running job polls.
    Unknown id → JOB-NOT-FOUND with USE-VALUE."))

;;; Worker generic functions

(defgeneric claim-job (queue worker-id &key kinds now lease-seconds)
  (:documentation
   "Calls (EXPIRE-JOB-LEASES QUEUE :NOW NOW), then atomically claims the
    next admissible :queued job (see Order and Admission in the file
    header) whose KIND is in KINDS (EQUAL; NIL = any kind). The job moves
    to :running with WORKER-ID, STARTED-AT = NOW, LEASE-UNTIL = NOW +
    LEASE-SECONDS (default 30), ATTEMPTS + 1, and the journal gets
    JOB-TRANSITION :queued → :running, reason :claimed, with WORKER-ID.
    → the snapshot, or NIL when nothing is admissible."))

(defgeneric heartbeat-job (queue job-id worker-id &key now lease-seconds)
  (:documentation
   "Extend WORKER-ID's lease: LEASE-UNTIL = NOW + LEASE-SECONDS (default
    30). No journal event. → the snapshot (carrying CANCEL-REQUESTED).
    Not :running → JOB-STATE-ERROR (:heartbeat); held by another worker →
    JOB-LEASE-LOST; both with CONTINUE (no change, return the snapshot).
    Unknown id → JOB-NOT-FOUND with USE-VALUE."))

(defgeneric complete-job (queue job-id worker-id &key result now)
  (:documentation
   ":running → :completed. RESULT goes through CANONICALIZE-VALUE
    (TASK-SERIALIZATION-ERROR with USE-VALUE). FINISHED-AT = NOW; WORKER-ID
    and LEASE-UNTIL become NIL; the slot is released; JOB-TRANSITION
    :running → :completed, reason :completed. Allowed on a
    cancel-requested job (the work finished first). → the snapshot.
    Errors as for HEARTBEAT-JOB, operation :complete."))

(defgeneric fail-job (queue job-id worker-id &key error now)
  (:documentation
   ":running → :failed, FAILURE = ERROR: a string, or a condition stored as its
    report string (PRINC-TO-STRING). FINISHED-AT = NOW; holder and lease
    cleared; slot released; JOB-TRANSITION :running → :failed, reason
    :failed. → the snapshot. Errors as for HEARTBEAT-JOB, operation
    :fail."))

(defgeneric acknowledge-cancel (queue job-id worker-id &key now)
  (:documentation
   "The worker stops a cancel-requested job: :running → :canceled,
    FAILURE = the cancel reason (or :canceled), FINISHED-AT = NOW, slot
    released, JOB-TRANSITION :running → :canceled, reason :canceled.
    Allowed without a prior request (the worker gives up). → the
    snapshot. Errors as for HEARTBEAT-JOB, operation
    :acknowledge-cancel."))

(defgeneric expire-job-leases (queue &key now)
  (:documentation
   "Every :running job whose LEASE-UNTIL < NOW loses its lease and its
    slot: a cancel-requested one → :canceled (FAILURE = its cancel
    reason), otherwise → :queued (holder
    and lease cleared, ATTEMPTS kept, original FIFO position kept). Each
    gets one JOB-TRANSITION, reason :lease-expired. → the snapshots of
    the jobs it changed, in submission order."))

;;; Progress and subscription generic functions

(defgeneric append-job-event (queue job-id event)
  (:documentation
   "Journal EVENT (a TASK-EVENT) for JOB-ID: sets EVENT-TASK-ID to JOB-ID,
    APPEND-EVENTs it to the queue journal (which assigns SEQ), then
    delivers it to the job's active subscriptions. Every event the queue
    journals goes through here; a consumer may journal its own events for
    a job the same way. → EVENT. Unknown id → JOB-NOT-FOUND with
    USE-VALUE."))

(defgeneric report-job-progress (queue job-id worker-id
                                 &key phase fraction message now)
  (:documentation
   "Append a JOB-PROGRESS (PHASE, FRACTION, MESSAGE, WORKER-ID, timestamp
    NOW) through APPEND-JOB-EVENT. FRACTION is NIL or a real in [0, 1];
    otherwise JOB-PROGRESS-ERROR with a USE-VALUE restart taking another
    fraction, which is checked again. Does not extend the lease. → the
    event. Not :running → JOB-STATE-ERROR (:progress); another holder →
    JOB-LEASE-LOST; CONTINUE on both returns NIL and journals nothing.
    Unknown id → JOB-NOT-FOUND with USE-VALUE."))

(defgeneric job-events (queue job-id &key after-seq)
  (:documentation
   "The job's journaled events with SEQ > AFTER-SEQ (default 0), in SEQ
    order: the pull form of a subscription (polling clients, SSE
    Last-Event-ID). Unknown id → JOB-NOT-FOUND with USE-VALUE."))

(defgeneric subscribe-job-events (queue job-id callback &key after-seq)
  (:documentation
   "Call CALLBACK with each event of JOB-ID whose SEQ > AFTER-SEQ (default
    0): first the journaled ones (replay), then each new one as it is
    appended. Delivery is in SEQ order, each event once, no gaps, even
    while other threads append during the replay. CALLBACK may run on the
    appending thread; in the in-memory backend it runs with the queue lock
    held (re-entrant: it may call JOB-STATUS or UNSUBSCRIBE-JOB-EVENTS).
    A condition from CALLBACK propagates to the appender inside a
    CONTINUE restart that skips that delivery (the subscription stays
    active; the event stays journaled). The subscription outlives
    terminal states until unsubscribed. → a JOB-SUBSCRIPTION.
    Unknown id → JOB-NOT-FOUND with USE-VALUE."))

(defgeneric unsubscribe-job-events (queue subscription)
  (:documentation
   "Stop SUBSCRIPTION: no delivery after this returns. Idempotent.
    → SUBSCRIPTION."))

;;; Durable-task replay ignores job events (given): a job's journal can be
;;; replayed into a DURABLE-TASK without changing its status.

(defmethod apply-event ((task durable-task) (event job-transition))
  (declare (ignore event))
  task)

(defmethod apply-event ((task durable-task) (event job-progress))
  (declare (ignore event))
  task)
