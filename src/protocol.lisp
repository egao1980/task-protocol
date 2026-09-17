(in-package #:task-protocol)

;;; Durable task model + deterministic replay. CLOS, not a workflow DSL.

(deftype task-status ()
  '(member :new :running :waiting :completed :failed :canceled))

(defvar *task* nil)
(defvar *journal* nil)
(defvar *consumed-steps* nil)
(defvar *id-counter* 0)
(defvar *redaction-policy* nil)

(defclass retry-policy ()
  ((max-attempts :initarg :max-attempts :accessor retry-policy-max-attempts
                 :initform 1)
   (backoff-seconds :initarg :backoff-seconds
                    :accessor retry-policy-backoff-seconds
                    :initform 0)
   (timeout :initarg :timeout :accessor retry-policy-timeout :initform nil)))

(defun make-retry-policy (&key (max-attempts 1) (backoff-seconds 0) timeout)
  (make-instance 'retry-policy
                 :max-attempts max-attempts
                 :backoff-seconds backoff-seconds
                 :timeout timeout))

(defclass durable-task ()
  ((id :initarg :id :accessor durable-task-id)
   (parent :initarg :parent :accessor durable-task-parent :initform nil)
   (status :initarg :status :accessor durable-task-status :initform :new)
   (retry-policy :initarg :retry-policy :accessor durable-task-retry-policy
                 :initform nil)
   (result :initarg :result :accessor durable-task-result :initform nil)
   (input :initarg :input :accessor durable-task-input :initform nil)
   (journal :initarg :journal :accessor durable-task-journal :initform nil)
   (children :initarg :children :accessor durable-task-children :initform nil)
   (child-ids :initarg :child-ids :accessor durable-task-child-ids :initform nil)
   (quorum :initarg :quorum :accessor durable-task-quorum :initform 1)
   (error :initarg :error :accessor durable-task-error :initform nil)
   (deadline :initarg :deadline :accessor durable-task-deadline :initform nil)
   (run-id :initarg :run-id :accessor durable-task-run-id :initform nil)
   (activation-id :initarg :activation-id :accessor durable-task-activation-id
                  :initform nil)
   (schema-version :initarg :schema-version :accessor durable-task-schema-version
                   :initform nil)
   (code-version :initarg :code-version :accessor durable-task-code-version
                 :initform nil)
   (config-version :initarg :config-version :accessor durable-task-config-version
                   :initform nil)
   (recorded-steps :initform (make-hash-table :test #'equal)
                   :accessor durable-task-recorded-steps)
   (effect-receipts :initform (make-hash-table :test #'equal)
                    :accessor durable-task-effect-receipts)))

(defun durable-task-p (x)
  (typep x 'durable-task))

(defun %next-task-id ()
  (format nil "task-~d-~d" (get-universal-time) (incf *id-counter*)))

(defun make-durable-task (&key id parent status retry-policy input journal
                            children quorum result
                            run-id activation-id
                            schema-version code-version config-version)
  (let ((task (make-instance 'durable-task
                             :id (or id (%next-task-id))
                             :parent parent
                             :status (or status :new)
                             :retry-policy retry-policy
                             :input input
                             :journal journal
                             :children (copy-list children)
                             :quorum (or quorum 1)
                             :result result
                             :run-id (%as-run-id run-id)
                             :activation-id (%as-activation-id activation-id)
                             :schema-version schema-version
                             :code-version code-version
                             :config-version config-version)))
    (check-type (durable-task-id task) string)
    (check-type (durable-task-status task) task-status)
    task))

(defgeneric append-event (journal event)
  (:documentation "Append EVENT to JOURNAL after redaction. → event."))

(defgeneric replay-journal (journal task)
  (:documentation "Apply journaled events to TASK. → task."))

(defgeneric journal-events (journal task)
  (:documentation "Oldest-first TASK-EVENT list for TASK."))

(defgeneric journal-task-ids (journal)
  (:documentation "Task id strings known to JOURNAL."))

(defgeneric import-events (journal events)
  (:documentation "Load EVENTS into JOURNAL without treating them as new work."))

(defgeneric journal-hash (journal task)
  (:documentation "Stable integer hash of TASK's serialized journal."))

(defgeneric compact-journal (journal task)
  (:documentation "Append a JOURNAL-SNAPSHOT and drop events before it."))

(defgeneric journal-redaction-policy (journal)
  (:method (journal) (declare (ignore journal)) *redaction-policy*))

(defgeneric journal-retention-policy (journal)
  (:method (journal) (declare (ignore journal)) nil))

(defgeneric apply-event (task event)
  (:documentation "Mutate TASK from EVENT."))

(defun %step-key (name idempotency-key &optional run-id activation-id)
  (let ((n (if (stringp name) name (string-downcase (string name))))
        (r (%identity-value run-id))
        (a (%identity-value activation-id)))
    (if (or r a)
        (list n idempotency-key r a)
        (cons n idempotency-key))))

(defun %receipt-key (idempotency-key &optional run-id activation-id)
  (let ((r (%identity-value run-id))
        (a (%identity-value activation-id)))
    (if (or r a)
        (list idempotency-key r a)
        idempotency-key)))

(defun %record-step (task event)
  (let ((run (event-run-id event))
        (act (event-activation-id event)))
    (setf (gethash (%step-key (step-name event) (step-idempotency-key event)
                              run act)
                   (durable-task-recorded-steps task))
          event)
    (when (step-name event)
      (setf (gethash (%step-key (step-name event) nil run act)
                     (durable-task-recorded-steps task))
            event))))

(defun %record-receipt (task event)
  (setf (gethash (%receipt-key (effect-receipt-idempotency-key event)
                               (event-run-id event)
                               (event-activation-id event))
                 (durable-task-effect-receipts task))
        event))

(defmethod apply-event ((task durable-task) (event task-started))
  (when (eq (durable-task-status task) :new)
    (setf (durable-task-status task) :running))
  task)

(defmethod apply-event ((task durable-task) (event step-completed))
  (%record-step task event)
  (when (eq (durable-task-status task) :new)
    (setf (durable-task-status task) :running))
  task)

(defmethod apply-event ((task durable-task) (event effect-receipt))
  (%record-receipt task event)
  task)

(defmethod apply-event ((task durable-task) (event timer-set))
  (setf (durable-task-status task) :waiting)
  task)

(defmethod apply-event ((task durable-task) (event timer-fired))
  (when (eq (durable-task-status task) :waiting)
    (setf (durable-task-status task) :running))
  task)

(defmethod apply-event ((task durable-task) (event child-spawned))
  (pushnew (event-child-id event) (durable-task-child-ids task) :test #'equal)
  task)

(defmethod apply-event ((task durable-task) (event children-joined))
  (when (eq (durable-task-status task) :waiting)
    (setf (durable-task-status task) :running))
  task)

(defmethod apply-event ((task durable-task) (event wait-input))
  (setf (durable-task-status task) :waiting)
  task)

(defmethod apply-event ((task durable-task) (event task-completed))
  (setf (durable-task-status task) :completed
        (durable-task-result task) (event-result event))
  task)

(defmethod apply-event ((task durable-task) (event task-failed))
  (setf (durable-task-status task)
        (if (eq (event-reason event) :canceled) :canceled :failed)
        (durable-task-error task) (event-reason event))
  task)

(defun %event-from-stored (plist default-type)
  (cond
    ((typep plist 'task-event) plist)
    ((and (consp plist) (keywordp (car plist)))
     (event-from-plist plist))
    (t (event-from-plist (append (list :type default-type) plist)))))

(defmethod apply-event ((task durable-task) (event journal-snapshot))
  (clrhash (durable-task-recorded-steps task))
  (clrhash (durable-task-effect-receipts task))
  (setf (durable-task-status task) (or (snapshot-status event) :running)
        (durable-task-result task) (snapshot-result event))
  (dolist (plist (snapshot-steps event))
    (%record-step task (%event-from-stored plist :step-completed)))
  (dolist (plist (snapshot-receipts event))
    (%record-receipt task (%event-from-stored plist :effect-receipt)))
  task)

(defun serializable-p (value)
  "T when VALUE can be journaled: numbers, strings, symbols, lists/vectors of
   those, hash-tables with string/keyword keys, pathnames (as namestrings)."
  (labels ((walk (x)
             (typecase x
               (null t)
               ((or number string symbol) t)
               (pathname t)
               (hash-table
                (let ((ok t))
                  (maphash (lambda (k v)
                             (unless (and (or (stringp k) (keywordp k))
                                          (walk v))
                               (setf ok nil)))
                           x)
                  ok))
               (cons (and (walk (car x)) (walk (cdr x))))
               ((and vector (not string)) (every #'walk x))
               (t nil))))
    (walk value)))

(defun canonicalize-value (value)
  "Return a serializable form of VALUE. Pathnames become namestrings.
   Non-serializable values signal TASK-SERIALIZATION-ERROR with USE-VALUE."
  (labels ((walk (x)
             (cond
               ((null x) nil)
               ((typep x '(or number string symbol)) x)
               ((pathnamep x) (namestring x))
               ((hash-table-p x)
                (unless (let ((ok t))
                          (maphash (lambda (k v)
                                     (declare (ignore v))
                                     (unless (or (stringp k) (keywordp k))
                                       (setf ok nil)))
                                   x)
                          ok)
                  (fail x))
                (let ((out (make-hash-table :test (hash-table-test x))))
                  (maphash (lambda (k v) (setf (gethash k out) (walk v))) x)
                  out))
               ((consp x)
                (cons (walk (car x)) (walk (cdr x))))
               ((and (vectorp x) (not (stringp x)))
                (map 'vector #'walk x))
               (t (fail x))))
           (fail (x)
             (restart-case
                 (error 'task-serialization-error
                        :value x
                        :task *task*
                        :message (format nil "not serializable: ~s" x))
               (use-value (replacement)
                 :report "Use a serializable substitute"
                 :interactive (lambda ()
                                (format *query-io* "Serializable value: ")
                                (force-output *query-io*)
                                (list (read *query-io*)))
                 (walk replacement)))))
    (walk value)))

(defun %journal (&optional journal)
  (or journal *journal*
      (and *task* (durable-task-journal *task*))
      (restart-case
          (error 'task-error :message "*journal* is nil — bind WITH-DURABLE-TASK")
        (use-value (supplied)
          :report "Use a supplied journal"
          supplied))))

(defun %current-task (&optional task)
  (or task *task*
      (restart-case
          (error 'task-error :message "*task* is nil — bind WITH-DURABLE-TASK")
        (use-value (supplied)
          :report "Use a supplied durable-task"
          supplied))))

(defun %ensure-running (task journal)
  (when (eq (durable-task-status task) :new)
    (append-event journal (make-instance 'task-started
                                         :task-id (durable-task-id task)))
    (setf (durable-task-status task) :running))
  task)

(defmacro with-durable-task ((task journal) &body body)
  "Bind *TASK* / *JOURNAL*, replay existing events, then evaluate BODY."
  (let ((task-var (gensym "TASK"))
        (journal-var (gensym "JOURNAL")))
    `(let* ((,journal-var ,journal)
            (,task-var ,task)
            (*journal* ,journal-var)
            (*task* ,task-var)
            (*consumed-steps* '()))
       (setf (durable-task-journal ,task-var) ,journal-var)
       (when (journal-events ,journal-var ,task-var)
         (replay-journal ,journal-var ,task-var))
       (%ensure-running ,task-var ,journal-var)
       ,@body)))

(defun %step-match (event name idempotency-key &optional run-id activation-id)
  (and (typep event 'step-completed)
       (equal (%identity-value run-id) (%identity-value (event-run-id event)))
       (equal (%identity-value activation-id)
              (%identity-value (event-activation-id event)))
       (or (and idempotency-key
                (equal idempotency-key (step-idempotency-key event)))
           (and (null idempotency-key)
                (equal name (step-name event))))))

(defun %find-recorded-step (task name idempotency-key
                            &optional run-id activation-id)
  (or (and idempotency-key
           (gethash (%step-key name idempotency-key run-id activation-id)
                    (durable-task-recorded-steps task)))
      (gethash (%step-key name nil run-id activation-id)
               (durable-task-recorded-steps task))))

(defun %same-identity-scope-p (event run-id activation-id)
  (and (equal (%identity-value run-id)
              (%identity-value (event-run-id event)))
       (equal (%identity-value activation-id)
              (%identity-value (event-activation-id event)))))

(defun %unconsumed-steps (task &optional run-id activation-id)
  (let ((seen (make-hash-table :test #'eq)))
    (maphash (lambda (k event)
               (declare (ignore k))
               (setf (gethash event seen) t))
             (durable-task-recorded-steps task))
    (let ((all '()))
      (maphash (lambda (event ignore)
                 (declare (ignore ignore))
                 (push event all))
               seen)
      (remove-if-not (lambda (event)
                       (%same-identity-scope-p event run-id activation-id))
                     (set-difference all *consumed-steps* :test #'eq)))))

(defun check-task-timeout (task &optional (now (get-universal-time)))
  (let ((deadline (or (durable-task-deadline task)
                      (and (durable-task-retry-policy task)
                           (retry-policy-timeout
                            (durable-task-retry-policy task))))))
    (when (and deadline (> now deadline))
      (restart-case
          (error 'task-timeout
                 :task task
                 :deadline deadline
                 :message (format nil "deadline ~s passed" deadline))
        (abort-task ()
          :report "Abort the durable task"
          (cancel-task task :timeout)
          nil)))))

(defun %resolve-run-id (task run-id)
  (%as-run-id (or run-id (durable-task-run-id task))))

(defun %resolve-activation-id (task activation-id)
  (%as-activation-id (or activation-id (durable-task-activation-id task))))

(defun %version-mismatch (event schema-version code-version config-version)
  (flet ((differs (recorded current)
           (and recorded current (not (equal recorded current)))))
    (cond
      ((differs (event-schema-version event) schema-version)
       (values (list :schema-version schema-version)
               (list :schema-version (event-schema-version event))
               "schema-version mismatch"))
      ((differs (event-code-version event) code-version)
       (values (list :code-version code-version)
               (list :code-version (event-code-version event))
               "code-version mismatch"))
      ((differs (event-config-version event) config-version)
       (values (list :config-version config-version)
               (list :config-version (event-config-version event))
               "config-version mismatch"))
      (t (values nil nil nil)))))

(defun %step-expected (name idempotency-key run-id activation-id)
  (list :step name
        :idempotency-key idempotency-key
        :run-id (%identity-value run-id)
        :activation-id (%identity-value activation-id)))

(defun call-with-durable-step (name idempotency-key retry-policy thunk
                               &key run-id activation-id
                                 schema-version code-version config-version)
  (let* ((task (%current-task))
         (journal (%journal))
         (name (if (stringp name) name (string-downcase (string name))))
         (policy (or retry-policy (durable-task-retry-policy task)))
         (max (if policy (retry-policy-max-attempts policy) 1))
         (run-id (%resolve-run-id task run-id))
         (activation-id (%resolve-activation-id task activation-id))
         (schema-version (or schema-version
                             (durable-task-schema-version task)
                             *schema-version*))
         (code-version (or code-version
                           (durable-task-code-version task)
                           *code-version*))
         (config-version (or config-version
                             (durable-task-config-version task)
                             *config-version*)))
    (check-task-timeout task)
    (let ((recorded (%find-recorded-step task name idempotency-key
                                         run-id activation-id)))
      (when recorded
        (when (and idempotency-key
                   (step-name recorded)
                   (not (equal name (step-name recorded))))
          (error 'task-replay-divergence
                 :task task
                 :journal-hash (journal-hash journal task)
                 :expected (%step-expected name idempotency-key
                                           run-id activation-id)
                 :actual (list :step (step-name recorded)
                               :idempotency-key (step-idempotency-key recorded))
                 :message "idempotency-key reused with a different step name"))
        (multiple-value-bind (expected actual message)
            (%version-mismatch recorded schema-version
                               code-version config-version)
          (when expected
            (restart-case
                (error 'task-replay-divergence
                       :task task
                       :journal-hash (journal-hash journal task)
                       :expected expected
                       :actual actual
                       :message message)
              (continue ()
                :report "Use the recorded result despite a version stamp mismatch"
                nil))))
        (push recorded *consumed-steps*)
        (return-from call-with-durable-step (step-result recorded))))
    (let ((orphans (%unconsumed-steps task run-id activation-id)))
      (when orphans
        (error 'task-replay-divergence
               :task task
               :journal-hash (journal-hash journal task)
               :expected (%step-expected name idempotency-key
                                         run-id activation-id)
               :actual (mapcar (lambda (e)
                                 (list :step (step-name e)
                                       :idempotency-key (step-idempotency-key e)
                                       :run-id (%identity-value (event-run-id e))
                                       :activation-id
                                       (%identity-value (event-activation-id e))))
                               orphans)
               :message "journal/code mismatch — unconsumed recorded steps remain")))
    (let ((attempts 0)
          (result nil))
      (tagbody
       :retry
         (incf attempts)
         (restart-case
             (progn
               (setf result (funcall thunk))
               (setf result (canonicalize-value result))
               (let ((event (make-instance 'step-completed
                                           :task-id (durable-task-id task)
                                           :name name
                                           :result result
                                           :idempotency-key idempotency-key
                                           :run-id run-id
                                           :activation-id activation-id
                                           :schema-version schema-version
                                           :code-version code-version
                                           :config-version config-version)))
                 (append-event journal event)
                 (apply-event task event)
                 (push event *consumed-steps*)
                 (return-from call-with-durable-step result)))
           (retry-step ()
             :report "Retry this step"
             (if (<= attempts max)
                 (go :retry)
                 (error 'task-error
                        :task task
                        :message (format nil "retry-step exhausted (~d)" max))))
           (skip-step ()
             :report "Skip this step and return NIL"
             (return-from call-with-durable-step nil))
           (abort-task ()
             :report "Abort the durable task"
             (cancel-task task :aborted)
             (return-from call-with-durable-step nil)))))))

(defmacro with-durable-step ((name &key idempotency-key retry-policy
                                   run-id activation-id
                                   schema-version code-version config-version)
                             &body body)
  "Execute BODY once and journal STEP-COMPLETED. On resume, return the
   recorded result without re-executing. Matched by NAME / IDEMPOTENCY-KEY
   and, when supplied, RUN-ID / ACTIVATION-ID."
  `(call-with-durable-step ,name ,idempotency-key ,retry-policy
                           (lambda () ,@body)
                           :run-id ,run-id
                           :activation-id ,activation-id
                           :schema-version ,schema-version
                           :code-version ,code-version
                           :config-version ,config-version))

(defun make-effect-receipt (&key name idempotency-key payload payload-hash
                              task-id run-id activation-id
                              schema-version code-version config-version)
  (let* ((canonical (and payload (canonicalize-value payload)))
         (hash (or payload-hash (and canonical (payload-hash canonical)))))
    (make-instance 'effect-receipt
                   :task-id task-id
                   :name name
                   :idempotency-key idempotency-key
                   :payload canonical
                   :payload-hash hash
                   :run-id (%as-run-id run-id)
                   :activation-id (%as-activation-id activation-id)
                   :schema-version schema-version
                   :code-version code-version
                   :config-version config-version)))

(defun find-effect-receipt (task idempotency-key &key run-id activation-id)
  "Return the journaled EFFECT-RECEIPT for IDEMPOTENCY-KEY in TASK's
   identity scope, or NIL."
  (let ((run-id (%resolve-run-id task run-id))
        (activation-id (%resolve-activation-id task activation-id)))
    (gethash (%receipt-key idempotency-key run-id activation-id)
             (durable-task-effect-receipts task))))

(defun record-effect-receipt (task receipt &key journal)
  "Append RECEIPT to TASK's journal. Same-scope idempotency-key returns the
   existing receipt (side effects are not the step return value)."
  (let* ((journal (%journal (or journal (durable-task-journal task))))
         (receipt (if (effect-receipt-p receipt)
                      receipt
                      (restart-case
                          (error 'task-error
                                 :task task
                                 :message (format nil "not an effect-receipt: ~s"
                                                  receipt))
                        (use-value (supplied)
                          :report "Use a supplied effect-receipt"
                          supplied)))))
    (check-type receipt effect-receipt)
    (unless (event-task-id receipt)
      (setf (event-task-id receipt) (durable-task-id task)))
    (unless (event-run-id receipt)
      (setf (event-run-id receipt)
            (%resolve-run-id task (event-run-id receipt))))
    (unless (event-activation-id receipt)
      (setf (event-activation-id receipt)
            (%resolve-activation-id task (event-activation-id receipt))))
    (let ((existing (find-effect-receipt
                     task
                     (effect-receipt-idempotency-key receipt)
                     :run-id (event-run-id receipt)
                     :activation-id (event-activation-id receipt))))
      (if existing
          existing
          (progn
            (append-event journal receipt)
            (apply-event task receipt)
            receipt)))))

(defun complete-task (task &optional result)
  (let ((journal (%journal (durable-task-journal task)))
        (value (if (serializable-p result)
                   (canonicalize-value result)
                   result)))
    (setf (durable-task-status task) :completed
          (durable-task-result task) value)
    (append-event journal (make-instance 'task-completed
                                         :task-id (durable-task-id task)
                                         :result value))
    task))

(defun fail-task (task &optional reason)
  (let ((journal (%journal (durable-task-journal task))))
    (setf (durable-task-status task) :failed
          (durable-task-error task) reason)
    (append-event journal (make-instance 'task-failed
                                         :task-id (durable-task-id task)
                                         :reason reason))
    task))

(defun cancel-task (task &optional reason)
  (let ((journal (%journal (durable-task-journal task))))
    (setf (durable-task-status task) :canceled
          (durable-task-error task) (or reason :canceled))
    (append-event journal (make-instance 'task-failed
                                         :task-id (durable-task-id task)
                                         :reason (or reason :canceled)))
    task))

(defun request-input (task &key prompt)
  (let ((journal (%journal (durable-task-journal task))))
    (append-event journal (make-instance 'wait-input
                                         :task-id (durable-task-id task)
                                         :prompt prompt))
    (setf (durable-task-status task) :waiting)
    task))

(defun %datetime-next-fire (spec last-fire)
  (let ((pkg (find-package '#:datetime-protocol)))
    (when pkg
      (let ((schedulep (find-symbol "SCHEDULEP" pkg))
            (next (find-symbol "NEXT-OCCURRENCE" pkg))
            (datep (find-symbol "DATEP" pkg))
            (date-year (find-symbol "DATE-YEAR" pkg))
            (date-month (find-symbol "DATE-MONTH" pkg))
            (date-day (find-symbol "DATE-DAY" pkg))
            (momentp (find-symbol "MOMENTP" pkg))
            (moment-date (find-symbol "MOMENT-DATE" pkg))
            (instantp (find-symbol "INSTANTP" pkg))
            (instant-seconds (find-symbol "INSTANT-SECONDS" pkg)))
        (when (and schedulep next (funcall schedulep spec))
          (let ((occ (funcall next spec last-fire)))
            (cond
              ((and instantp instant-seconds (funcall instantp occ))
               (+ 2208988800 (funcall instant-seconds occ)))
              ((and momentp moment-date datep date-year
                    (funcall momentp occ))
               (let ((d (funcall moment-date occ)))
                 (encode-universal-time 0 0 0
                                        (funcall date-day d)
                                        (funcall date-month d)
                                        (funcall date-year d))))
              ((and datep date-year (funcall datep occ))
               (encode-universal-time 0 0 0
                                      (funcall date-day occ)
                                      (funcall date-month occ)
                                      (funcall date-year occ)))
              ((integerp occ) occ)
              (t nil))))))))

(defun %normalize-dow (spec)
  (typecase spec
    (null spec)
    (keyword (position spec '(:monday :tuesday :wednesday :thursday
                              :friday :saturday :sunday)))
    (t spec)))

(defun %field-match (value spec)
  (cond
    ((or (null spec) (eq spec :*) (eq spec t)) t)
    ((and (integerp spec) (eql spec value)) t)
    ((and (consp spec) (not (keywordp (car spec)))
          (member value spec :test #'eql))
     t)
    (t nil)))

(defun %cron-match (spec ut)
  (multiple-value-bind (sec min hour date month year dow)
      (decode-universal-time ut 0)
    (declare (ignore sec year))
    (and (%field-match min (getf spec :minute))
         (%field-match hour (getf spec :hour))
         (%field-match date (getf spec :day-of-month))
         (%field-match month (getf spec :month))
         (%field-match dow (%normalize-dow (getf spec :day-of-week))))))

(defun compute-next-fire (spec last-fire)
  "Next fire time from cron-like SPEC plist, a function of last-fire, or a
   datetime-protocol schedule (soft-used when that system is loaded)."
  (cond
    ((functionp spec)
     (funcall spec last-fire))
    ((%datetime-next-fire spec last-fire))
    ((listp spec)
     (loop for ut from (+ last-fire 60)
             below (+ last-fire (* 366 24 60 60)) by 60
           when (%cron-match spec ut)
             return ut
           finally (error 'task-error
                          :message "no next fire within a year for schedule spec")))
    (t (error 'task-error
              :message (format nil "bad schedule spec: ~s" spec)))))

(defun schedule-wake (task time)
  (let ((journal (%journal (durable-task-journal task))))
    (check-type time integer)
    (append-event journal (make-instance 'timer-set
                                         :task-id (durable-task-id task)
                                         :time time))
    (setf (durable-task-status task) :waiting)
    time))

(defun schedule-recurring (task spec &key (now (get-universal-time)))
  (let* ((journal (%journal (durable-task-journal task)))
         (next (compute-next-fire spec now)))
    (append-event journal (make-instance 'timer-set
                                         :task-id (durable-task-id task)
                                         :time next
                                         :spec spec
                                         :recurring t))
    (setf (durable-task-status task) :waiting)
    next))

(defun %pending-timers (events)
  (let ((q '()))
    (dolist (e events q)
      (cond
        ((typep e 'timer-set)
         (setf q (append q (list e))))
        ((typep e 'timer-fired)
         (setf q (cdr q)))))))

(defun fire-due-timers (journal &optional now)
  "Append TIMER-FIRED for due TIMER-SET events. Recurring timers re-arm."
  (let ((now (or now (get-universal-time)))
        (fired '()))
    (dolist (id (journal-task-ids journal) (nreverse fired))
      (dolist (timer (%pending-timers (journal-events journal
                                                      (make-durable-task :id id))))
        (when (<= (timer-time timer) now)
          (let ((ev (make-instance 'timer-fired :task-id id :time now)))
            (append-event journal ev)
            (push ev fired)
            (when (timer-recurring-p timer)
              (append-event journal
                            (make-instance 'timer-set
                                           :task-id id
                                           :time (compute-next-fire
                                                  (timer-spec timer)
                                                  (timer-time timer))
                                           :spec (timer-spec timer)
                                           :recurring t)))))))))

(defun spawn-child-task (parent fn &key input)
  "Journal CHILD-SPAWNED and run FN with the child bound as *TASK*.
   On replay, reuse the recorded child-id and skip FN when the child completed."
  (let* ((journal (%journal (or (durable-task-journal parent) *journal*)))
         (existing (find-if (lambda (e)
                              (and (typep e 'child-spawned)
                                   (not (find (event-child-id e)
                                              (durable-task-children parent)
                                              :key (lambda (c)
                                                     (if (durable-task-p c)
                                                         (durable-task-id c)
                                                         c))
                                              :test #'equal))))
                            (journal-events journal parent)))
         (child (make-durable-task
                 :id (or (and existing (event-child-id existing))
                         (%next-task-id))
                 :parent parent
                 :input (or input (and existing (event-child-input existing)))
                 :journal journal)))
    (unless existing
      (append-event journal (make-instance 'child-spawned
                                           :task-id (durable-task-id parent)
                                           :child-id (durable-task-id child)
                                           :input input)))
    (setf (durable-task-children parent)
          (append (durable-task-children parent) (list child)))
    (replay-journal journal child)
    (when (and fn (not (member (durable-task-status child)
                               '(:completed :failed :canceled))))
      (let ((*task* child)
            (*journal* journal)
            (*consumed-steps* '()))
        (%ensure-running child journal)
        (let ((ok nil))
          (unwind-protect
               (progn
                 (setf (durable-task-result child)
                       (funcall fn (durable-task-input child)))
                 (complete-task child (durable-task-result child))
                 (setf ok t))
            (unless ok
              (unless (member (durable-task-status child)
                              '(:completed :failed :canceled))
                (fail-task child :spawn-error)))))))
    child))

(defun %child-terminal-p (child)
  (member (if (durable-task-p child)
              (durable-task-status child)
              :completed)
          '(:completed :failed :canceled)))

(defun %child-ok-p (child)
  (eq (if (durable-task-p child)
          (durable-task-status child)
          :completed)
      :completed))

(defun %child-result (child)
  (if (durable-task-p child)
      (durable-task-result child)
      child))

(defun %join-ready-p (children policy quorum)
  (ecase policy
    (:all (and children (every #'%child-terminal-p children)))
    (:any (some #'%child-ok-p children))
    (:quorum (>= (count-if #'%child-ok-p children) quorum))))

(defun %join-result (children policy)
  (ecase policy
    (:all (mapcar #'%child-result children))
    (:any (%child-result (find-if #'%child-ok-p children)))
    (:quorum (mapcar #'%child-result (remove-if-not #'%child-ok-p children)))))

(defun join-children (parent &key (policy :all))
  "Join PARENT's children. POLICY is :ALL, :ANY, or :QUORUM (uses
   DURABLE-TASK-QUORUM, or (:QUORUM N)). Journals CHILDREN-JOINED when ready."
  (let* ((journal (%journal (durable-task-journal parent)))
         (policy* (if (and (consp policy) (eq (car policy) :quorum))
                      :quorum
                      policy))
         (quorum (if (and (consp policy) (eq (car policy) :quorum))
                     (second policy)
                     (durable-task-quorum parent)))
         (children (durable-task-children parent)))
    (unless (member policy* '(:all :any :quorum))
      (error 'task-error
             :task parent
             :message (format nil "unknown join policy ~s" policy)))
    (unless (%join-ready-p children policy* quorum)
      (setf (durable-task-status parent) :waiting)
      (return-from join-children nil))
    (let ((result (%join-result children policy*)))
      (append-event journal (make-instance 'children-joined
                                           :task-id (durable-task-id parent)
                                           :policy policy*
                                           :result result))
      result)))
