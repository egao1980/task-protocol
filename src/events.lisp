(in-package #:task-protocol)

;;; Event classes + sexp (plist) codec. Soft-use serdes-protocol / json-protocol
;;; when those packages are already loaded — core has no hard dep.

(defparameter *schema-version* "0.2.0"
  "Protocol schema version stamped on newly journaled events.")

(defvar *code-version* nil
  "Optional code-build stamp copied onto journaled events when bound.")

(defvar *config-version* nil
  "Optional config stamp copied onto journaled events when bound.")

(defvar *identity-counter* 0)

(defun %next-identity (prefix)
  (format nil "~a-~d-~d" prefix (get-universal-time) (incf *identity-counter*)))

(defun %normalize-identity-string (value prefix)
  (cond
    ((null value) (%next-identity prefix))
    ((stringp value) value)
    ((symbolp value) (string-downcase (string value)))
    (t (princ-to-string value))))

(defclass run-id ()
  ((value :initarg :value :reader run-id-value :initform nil)))

(defun run-id-p (x)
  (typep x 'run-id))

(defun make-run-id (&optional value)
  (let ((s (%normalize-identity-string value "run")))
    (check-type s string)
    (make-instance 'run-id :value s)))

(defclass activation-id ()
  ((value :initarg :value :reader activation-id-value :initform nil)
   (run-id :initarg :run-id :reader activation-id-run-id :initform nil)))

(defun activation-id-p (x)
  (typep x 'activation-id))

(defun make-activation-id (&optional value run-id)
  (let ((s (%normalize-identity-string value "activation")))
    (check-type s string)
    (make-instance 'activation-id :value s :run-id (%as-run-id run-id))))

(defun %identity-value (id)
  (typecase id
    (null nil)
    (run-id (run-id-value id))
    (activation-id (activation-id-value id))
    (string id)
    (symbol (string-downcase (string id)))
    (t (princ-to-string id))))

(defun %as-run-id (x)
  (cond
    ((null x) nil)
    ((run-id-p x) x)
    ((or (stringp x) (symbolp x)) (make-run-id x))
    (t
     (restart-case
         (error 'task-error
                :message (format nil "not a run-id: ~s" x))
       (use-value (supplied)
         :report "Use a supplied run-id"
         (%as-run-id supplied))))))

(defun %as-activation-id (x)
  (cond
    ((null x) nil)
    ((activation-id-p x) x)
    ((or (stringp x) (symbolp x)) (make-activation-id x))
    (t
     (restart-case
         (error 'task-error
                :message (format nil "not an activation-id: ~s" x))
       (use-value (supplied)
         :report "Use a supplied activation-id"
         (%as-activation-id supplied))))))

(defclass task-event ()
  ((task-id :initarg :task-id :accessor event-task-id :initform nil)
   (timestamp :initarg :timestamp :accessor event-timestamp
              :initform (get-universal-time))
   (seq :initarg :seq :accessor event-seq :initform nil)
   (run-id :initarg :run-id :accessor event-run-id :initform nil)
   (activation-id :initarg :activation-id :accessor event-activation-id
                  :initform nil)
   (schema-version :initarg :schema-version :accessor event-schema-version
                   :initform nil)
   (code-version :initarg :code-version :accessor event-code-version
                 :initform nil)
   (config-version :initarg :config-version :accessor event-config-version
                   :initform nil)))

(defclass step-completed (task-event)
  ((name :initarg :name :accessor step-name)
   (result :initarg :result :accessor step-result :initform nil)
   (idempotency-key :initarg :idempotency-key :accessor step-idempotency-key
                    :initform nil)))

(defclass effect-receipt (task-event)
  ((name :initarg :name :accessor effect-receipt-name :initform nil)
   (idempotency-key :initarg :idempotency-key
                    :accessor effect-receipt-idempotency-key
                    :initform nil)
   (payload-hash :initarg :payload-hash :accessor effect-receipt-payload-hash
                 :initform nil)
   (payload :initarg :payload :accessor effect-receipt-payload :initform nil)))

(defun effect-receipt-p (x)
  (typep x 'effect-receipt))

(defclass timer-set (task-event)
  ((time :initarg :time :accessor timer-time)
   (spec :initarg :spec :accessor timer-spec :initform nil)
   (recurring :initarg :recurring :accessor timer-recurring-p :initform nil)))

(defclass timer-fired (task-event)
  ((time :initarg :time :accessor timer-time :initform nil)))

(defclass child-spawned (task-event)
  ((child-id :initarg :child-id :accessor event-child-id)
   (input :initarg :input :accessor event-child-input :initform nil)))

(defclass children-joined (task-event)
  ((policy :initarg :policy :accessor join-policy :initform :all)
   (result :initarg :result :accessor join-result :initform nil)))

(defclass wait-input (task-event)
  ((prompt :initarg :prompt :accessor wait-prompt :initform nil)))

(defclass task-started (task-event) ())

(defclass task-completed (task-event)
  ((result :initarg :result :accessor event-result :initform nil)))

(defclass task-failed (task-event)
  ((reason :initarg :reason :accessor event-reason :initform nil)))

(defclass journal-snapshot (task-event)
  ((status :initarg :status :accessor snapshot-status :initform :running)
   (result :initarg :result :accessor snapshot-result :initform nil)
   (steps :initarg :steps :accessor snapshot-steps :initform nil)
   (receipts :initarg :receipts :accessor snapshot-receipts :initform nil)
   (event-count :initarg :event-count :accessor snapshot-event-count
                :initform 0)))

(defparameter *event-type-classes*
  '((:step-completed . step-completed)
    (:effect-receipt . effect-receipt)
    (:timer-set . timer-set)
    (:timer-fired . timer-fired)
    (:child-spawned . child-spawned)
    (:children-joined . children-joined)
    (:wait-input . wait-input)
    (:task-started . task-started)
    (:task-completed . task-completed)
    (:task-failed . task-failed)
    (:journal-snapshot . journal-snapshot)))

(defun event-type-keyword (event)
  (or (car (rassoc (class-name (class-of event)) *event-type-classes*))
      (intern (symbol-name (class-name (class-of event))) :keyword)))

(defun %class-for-type (type)
  (let* ((key (if (keywordp type)
                  type
                  (intern (string-upcase (string type)) :keyword)))
         (class (cdr (assoc key *event-type-classes*))))
    (or class
        (error 'task-error
               :message (format nil "unknown event type ~s" type)))))

(defun %tagged-hash (table)
  (let ((entries '()))
    (maphash (lambda (k v) (push (list k v) entries)) table)
    (list :%hash-table
          :test (hash-table-test table)
          :entries (nreverse entries))))

(defun %untag-value (value)
  (cond
    ((and (consp value) (eq (first value) :%hash-table))
     (let* ((test (or (getf (rest value) :test) 'eql))
            (entries (getf (rest value) :entries))
            (h (make-hash-table :test test)))
       (dolist (pair entries h)
         (setf (gethash (first pair) h) (%untag-value (second pair))))))
    ((consp value)
     (cons (%untag-value (car value)) (%untag-value (cdr value))))
    ((and (vectorp value) (not (stringp value)))
     (map 'vector #'%untag-value value))
    (t value)))

(defun %tag-value (value)
  (cond
    ((hash-table-p value) (%tagged-hash value))
    ((consp value)
     (cons (%tag-value (car value)) (%tag-value (cdr value))))
    ((and (vectorp value) (not (stringp value)))
     (map 'vector #'%tag-value value))
    (t value)))

(defgeneric event-plist (event)
  (:documentation "Serialize EVENT as a plist (sexp)."))

(defmethod event-plist ((event task-event))
  (list :type (event-type-keyword event)
        :task-id (event-task-id event)
        :timestamp (event-timestamp event)
        :seq (event-seq event)
        :run-id (%identity-value (event-run-id event))
        :activation-id (%identity-value (event-activation-id event))
        :schema-version (event-schema-version event)
        :code-version (event-code-version event)
        :config-version (event-config-version event)))

(defmethod event-plist :around ((event task-event))
  (let ((plist (call-next-method)))
    (loop for (k v) on plist by #'cddr
          collect k collect (%tag-value v))))

(defmethod event-plist ((event step-completed))
  (append (call-next-method)
          (list :name (step-name event)
                :result (step-result event)
                :idempotency-key (step-idempotency-key event))))

(defmethod event-plist ((event effect-receipt))
  (append (call-next-method)
          (list :name (effect-receipt-name event)
                :idempotency-key (effect-receipt-idempotency-key event)
                :payload-hash (effect-receipt-payload-hash event)
                :payload (effect-receipt-payload event))))

(defmethod event-plist ((event timer-set))
  (append (call-next-method)
          (list :time (timer-time event)
                :spec (timer-spec event)
                :recurring (timer-recurring-p event))))

(defmethod event-plist ((event timer-fired))
  (append (call-next-method)
          (list :time (timer-time event))))

(defmethod event-plist ((event child-spawned))
  (append (call-next-method)
          (list :child-id (event-child-id event)
                :input (event-child-input event))))

(defmethod event-plist ((event children-joined))
  (append (call-next-method)
          (list :policy (join-policy event)
                :result (join-result event))))

(defmethod event-plist ((event wait-input))
  (append (call-next-method)
          (list :prompt (wait-prompt event))))

(defmethod event-plist ((event task-completed))
  (append (call-next-method)
          (list :result (event-result event))))

(defmethod event-plist ((event task-failed))
  (append (call-next-method)
          (list :reason (event-reason event))))

(defmethod event-plist ((event journal-snapshot))
  (append (call-next-method)
          (list :status (snapshot-status event)
                :result (snapshot-result event)
                :steps (snapshot-steps event)
                :receipts (snapshot-receipts event)
                :event-count (snapshot-event-count event))))

(defun event-from-plist (plist)
  "Rehydrate a TASK-EVENT from a plist produced by EVENT-PLIST."
  (check-type plist list)
  (let* ((plist (loop for (k v) on plist by #'cddr
                      collect k collect (%untag-value v)))
         (type (getf plist :type))
         (class (%class-for-type type))
         (event (make-instance class
                               :task-id (getf plist :task-id)
                               :timestamp (getf plist :timestamp)
                               :seq (getf plist :seq)
                               :run-id (%as-run-id (getf plist :run-id))
                               :activation-id (%as-activation-id
                                               (getf plist :activation-id))
                               :schema-version (getf plist :schema-version)
                               :code-version (getf plist :code-version)
                               :config-version (getf plist :config-version))))
    (typecase event
      (step-completed
       (setf (step-name event) (getf plist :name)
             (step-result event) (getf plist :result)
             (step-idempotency-key event) (getf plist :idempotency-key)))
      (effect-receipt
       (setf (effect-receipt-name event) (getf plist :name)
             (effect-receipt-idempotency-key event)
             (getf plist :idempotency-key)
             (effect-receipt-payload-hash event) (getf plist :payload-hash)
             (effect-receipt-payload event) (getf plist :payload)))
      (timer-set
       (setf (timer-time event) (getf plist :time)
             (timer-spec event) (getf plist :spec)
             (timer-recurring-p event) (getf plist :recurring)))
      (timer-fired
       (setf (timer-time event) (getf plist :time)))
      (child-spawned
       (setf (event-child-id event) (getf plist :child-id)
             (event-child-input event) (getf plist :input)))
      (children-joined
       (setf (join-policy event) (or (getf plist :policy) :all)
             (join-result event) (getf plist :result)))
      (wait-input
       (setf (wait-prompt event) (getf plist :prompt)))
      (task-completed
       (setf (event-result event) (getf plist :result)))
      (task-failed
       (setf (event-reason event) (getf plist :reason)))
      (journal-snapshot
       (setf (snapshot-status event) (or (getf plist :status) :running)
             (snapshot-result event) (getf plist :result)
             (snapshot-steps event) (getf plist :steps)
             (snapshot-receipts event) (getf plist :receipts)
             (snapshot-event-count event) (or (getf plist :event-count) 0))))
    event))

(defun copy-event (event)
  (event-from-plist (event-plist event)))

(defun %prin1-safe (value)
  (with-standard-io-syntax
    (let ((*print-readably* nil)
          (*print-circle* t)
          (*print-pretty* nil)
          (*package* (find-package :cl)))
      (prin1-to-string value))))

(defun %read-safe (string)
  (with-standard-io-syntax
    (let ((*read-eval* nil)
          (*package* (find-package :cl)))
      (read-from-string string))))

(defun %find-exported (package-name symbol-name)
  (let ((pkg (find-package package-name)))
    (when pkg
      (let ((sym (find-symbol symbol-name pkg)))
        (when (and sym (fboundp sym))
          sym)))))

(defun encode-payload (value)
  "Encode VALUE as a string. Soft-use serdes-protocol / json-protocol if bound."
  (let ((serdes (%find-exported '#:serdes-protocol "ENCODE"))
        (json (%find-exported '#:json-protocol "ENCODE-JSON")))
    (or (when serdes
          (ignore-errors (funcall serdes value)))
        (when json
          (ignore-errors (funcall json value)))
        (%prin1-safe value))))

(defun %djb2 (string)
  (let ((h 5381))
    (loop for c across string
          do (setf h (logand #xffffffff (+ (ash h 5) h (char-code c)))))
    h))

(defun payload-hash (value)
  "Stable integer hash of VALUE via ENCODE-PAYLOAD."
  (%djb2 (encode-payload value)))

(defun decode-payload (string)
  "Decode a string from ENCODE-PAYLOAD. Soft-use serdes / json if bound."
  (let ((serdes (%find-exported '#:serdes-protocol "DECODE"))
        (json (%find-exported '#:json-protocol "PARSE-JSON")))
    (or (when serdes
          (ignore-errors (funcall serdes string)))
        (when json
          (ignore-errors (funcall json string)))
        (%read-safe string))))
