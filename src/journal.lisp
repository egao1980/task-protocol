(in-package #:task-protocol)

;;; In-memory journal + compaction / redaction (A7 reuses REDACTION-POLICY).

(defparameter +default-sensitive-keys+
  '("password" "secret" "authorization" "api-key" "api_key"))

(defclass redaction-policy ()
  ((keys :initarg :keys :accessor redaction-policy-keys
         :initform +default-sensitive-keys+)))

(defun make-redaction-policy (&key (keys +default-sensitive-keys+))
  (make-instance 'redaction-policy
                 :keys (mapcar (lambda (k) (string-downcase (string k)))
                               keys)))

(defun %sensitive-key-p (key policy)
  (let ((norm (substitute #\_ #\- (string-downcase (string key)))))
    (some (lambda (pat)
            (let ((p (substitute #\_ #\- (string-downcase (string pat)))))
              (or (string= norm p)
                  (search p norm :test #'char=))))
          (redaction-policy-keys policy))))

(defun %secret-ref (key)
  (list :secret-ref (string-downcase (string key))))

(defun %already-secret-ref-p (value)
  (and (consp value)
       (eq (first value) :secret-ref)))

(defun %redact-value (policy key value)
  (cond
    ((%already-secret-ref-p value) value)
    ((%sensitive-key-p key policy) (%secret-ref key))
    (t (%redact-tree policy value))))

(defun %plist-p (list)
  (and (listp list)
       (evenp (length list))
       (loop for (k v) on list by #'cddr
             always (or (keywordp k) (stringp k) (symbolp k)))))

(defun %redact-tree (policy value)
  (cond
    ((hash-table-p value)
     (let ((out (make-hash-table :test (hash-table-test value))))
       (maphash (lambda (k v)
                  (setf (gethash k out) (%redact-value policy k v)))
                value)
       out))
    ((and (consp value) (%plist-p value)
          (or (keywordp (first value)) (stringp (first value))))
     (loop for (k v) on value by #'cddr
           collect k collect (%redact-value policy k v)))
    ((consp value)
     (cons (%redact-tree policy (car value))
           (%redact-tree policy (cdr value))))
    ((and (vectorp value) (not (stringp value)))
     (map 'vector (lambda (x) (%redact-tree policy x)) value))
    (t value)))

(defgeneric redact-event (policy event)
  (:documentation "Field-level redaction of EVENT payloads. Mutates EVENT.
   Sensitive keys become (:SECRET-REF name). A7 reuses this policy object."))

(defmethod redact-event ((policy null) event)
  event)

(defmethod redact-event ((policy redaction-policy) (event task-event))
  event)

(defmethod redact-event ((policy redaction-policy) (event step-completed))
  (setf (step-result event) (%redact-tree policy (step-result event)))
  event)

(defmethod redact-event ((policy redaction-policy) (event child-spawned))
  (setf (event-child-input event) (%redact-tree policy (event-child-input event)))
  event)

(defmethod redact-event ((policy redaction-policy) (event task-completed))
  (setf (event-result event) (%redact-tree policy (event-result event)))
  event)

(defmethod redact-event ((policy redaction-policy) (event children-joined))
  (setf (join-result event) (%redact-tree policy (join-result event)))
  event)

(defmethod redact-event ((policy redaction-policy) (event wait-input))
  (setf (wait-prompt event) (%redact-tree policy (wait-prompt event)))
  event)

(defmethod redact-event ((policy redaction-policy) (event journal-snapshot))
  (setf (snapshot-result event) (%redact-tree policy (snapshot-result event))
        (snapshot-steps event) (%redact-tree policy (snapshot-steps event)))
  event)

(defclass retention-policy ()
  ((max-age :initarg :max-age :accessor retention-policy-max-age :initform nil)
   (max-count :initarg :max-count :accessor retention-policy-max-count
              :initform nil)
   (per-class :initarg :per-class :accessor retention-policy-per-class
              :initform nil
              :documentation
              "Alist of event class or type keyword → (:max-age N :max-count N).")))

(defun make-retention-policy (&key max-age max-count per-class)
  (make-instance 'retention-policy
                 :max-age max-age
                 :max-count max-count
                 :per-class per-class))

(defun %class-retention (policy event)
  (let ((per (retention-policy-per-class policy))
        (type (event-type-keyword event))
        (class (class-name (class-of event))))
    (or (cdr (assoc type per))
        (cdr (assoc class per))
        (list :max-age (retention-policy-max-age policy)
              :max-count (retention-policy-max-count policy)))))

(defun %apply-retention (events policy &optional (now (get-universal-time)))
  (unless policy
    (return-from %apply-retention events))
  (let ((kept '())
        (counts (make-hash-table :test #'eq)))
    (dolist (event (reverse events))
      (let* ((rules (%class-retention policy event))
             (max-age (getf rules :max-age))
             (max-count (getf rules :max-count))
             (class (class-name (class-of event)))
             (n (gethash class counts 0))
             (age (and (event-timestamp event)
                       (- now (event-timestamp event)))))
        (cond
          ((and max-age age (> age max-age)))
          ((and max-count (>= n max-count)))
          (t
           (push event kept)
           (setf (gethash class counts) (1+ n))))))
    kept))

(defclass in-memory-journal ()
  ((events :initarg :events :accessor in-memory-journal-table
           :initform (make-hash-table :test #'equal)
           :documentation "task-id → oldest-first event list")
   (redaction-policy :initarg :redaction-policy
                     :accessor journal-redaction-policy
                     :initform (make-redaction-policy))
   (retention-policy :initarg :retention-policy
                     :accessor journal-retention-policy
                     :initform nil)))

(defun in-memory-journal-p (x)
  (typep x 'in-memory-journal))

(defun make-in-memory-journal (&key events redaction-policy retention-policy)
  (let ((journal (make-instance 'in-memory-journal
                                :redaction-policy (or redaction-policy
                                                      (make-redaction-policy))
                                :retention-policy retention-policy)))
    (when events
      (import-events journal events))
    journal))

(defun copy-in-memory-journal (journal)
  (let ((copy (make-in-memory-journal
               :redaction-policy (journal-redaction-policy journal)
               :retention-policy (journal-retention-policy journal))))
    (maphash (lambda (id evs)
               (setf (gethash id (in-memory-journal-table copy))
                     (mapcar #'copy-event evs)))
             (in-memory-journal-table journal))
    copy))

(defun %task-id (task)
  (if (durable-task-p task)
      (durable-task-id task)
      task))

(defmethod journal-events ((journal in-memory-journal) task)
  (copy-list (gethash (%task-id task) (in-memory-journal-table journal))))

(defmethod journal-task-ids ((journal in-memory-journal))
  (let ((ids '()))
    (maphash (lambda (k v)
               (declare (ignore v))
               (push k ids))
             (in-memory-journal-table journal))
    (nreverse ids)))

(defun %next-seq (journal task-id)
  (let ((events (gethash task-id (in-memory-journal-table journal))))
    (if events
        (1+ (or (event-seq (car (last events))) (length events)))
        1)))

(defmethod append-event :around (journal event)
  (let ((policy (journal-redaction-policy journal)))
    (when policy
      (redact-event policy event)))
  (call-next-method))

(defmethod append-event ((journal in-memory-journal) event)
  (let ((id (or (event-task-id event)
                (and *task* (durable-task-id *task*)))))
    (unless id
      (error 'task-error :message "event has no task-id"))
    (setf (event-task-id event) id)
    (unless (event-seq event)
      (setf (event-seq event) (%next-seq journal id)))
    (setf (gethash id (in-memory-journal-table journal))
          (append (gethash id (in-memory-journal-table journal))
                  (list event)))
    event))

(defmethod import-events ((journal in-memory-journal) events)
  (dolist (event events journal)
    (let* ((e (if (listp event) (event-from-plist event) (copy-event event)))
           (id (event-task-id e)))
      (unless (event-seq e)
        (setf (event-seq e) (%next-seq journal id)))
      (setf (gethash id (in-memory-journal-table journal))
            (append (gethash id (in-memory-journal-table journal))
                    (list e))))))

(defmethod journal-hash (journal task)
  (let ((s (with-output-to-string (o)
             (dolist (e (journal-events journal task))
               (prin1 (event-plist e) o)
               (terpri o)))))
    (let ((h 5381))
      (loop for c across s
            do (setf h (logand #xffffffff (+ (ash h 5) h (char-code c)))))
      h)))

(defmethod replay-journal (journal task)
  (clrhash (durable-task-recorded-steps task))
  (unless (member (durable-task-status task) '(:completed :failed :canceled))
    (setf (durable-task-status task) :new
          (durable-task-result task) nil
          (durable-task-error task) nil))
  (dolist (event (journal-events journal task) task)
    (apply-event task event)))

(defun %snapshot-steps (task events)
  (let ((seen (make-hash-table :test #'equal))
        (steps '()))
    (dolist (event events)
      (when (typep event 'journal-snapshot)
        (dolist (plist (snapshot-steps event))
          (let ((ev (if (typep plist 'step-completed)
                        plist
                        (event-from-plist
                         (if (getf plist :type)
                             plist
                             (append '(:type :step-completed) plist))))))
            (setf (gethash (%step-key (step-name ev) (step-idempotency-key ev))
                           seen)
                  ev))))
      (when (typep event 'step-completed)
        (setf (gethash (%step-key (step-name event) (step-idempotency-key event))
                       seen)
              event)))
    (maphash (lambda (k ev)
               (declare (ignore k))
               (push (list :type :step-completed
                           :name (step-name ev)
                           :result (step-result ev)
                           :idempotency-key (step-idempotency-key ev))
                     steps))
             seen)
    (or steps
        (let ((acc '()))
          (maphash (lambda (k ev)
                     (when (and (consp k) (null (cdr k)))
                       (push (list :type :step-completed
                                   :name (step-name ev)
                                   :result (step-result ev)
                                   :idempotency-key (step-idempotency-key ev))
                             acc)))
                   (durable-task-recorded-steps task))
          acc))))

(defmethod compact-journal ((journal in-memory-journal) task)
  (let* ((id (%task-id task))
         (all (gethash id (in-memory-journal-table journal)))
         (filtered (%apply-retention all (journal-retention-policy journal)))
         (snap (make-instance 'journal-snapshot
                              :task-id id
                              :status (durable-task-status task)
                              :result (durable-task-result task)
                              :steps (%snapshot-steps task filtered)
                              :event-count (length all))))
    (redact-event (journal-redaction-policy journal) snap)
    (setf (event-seq snap) (1+ (or (and (car (last filtered))
                                        (event-seq (car (last filtered))))
                                   0)))
    (setf (gethash id (in-memory-journal-table journal)) (list snap))
    snap))

(setf *redaction-policy* (make-redaction-policy))
