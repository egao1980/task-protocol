(in-package #:task-protocol)

;;; Versioned serialized event schema (0.2.1) + codec registration.
;;;
;;; Envelope keys: :type :codec :schema-version :task-id :timestamp :seq
;;; :run-id :activation-id :code-version :config-version + type-specific slots.
;;;
;;; Built-in codecs:
;;;   :sexp-plist  — keyword plist (EVENT-PLIST / EVENT-FROM-PLIST)
;;;   :json-object — hash-table object / vector array (json-protocol mapping)
;;;
;;; Consumers REGISTER-EVENT-CODEC a decoder. Vectors are never guessed as
;;; plists: #("a" "b") stays an array on encode and decode.

(defvar *default-event-codec* :sexp-plist
  "Codec used when DECODE-EVENT / ENCODE-EVENT cannot read one from DATA.")

(defvar *event-codecs* (make-hash-table :test #'equal)
  "Map (NAME . SCHEMA-VERSION) or NAME → EVENT-CODEC.")

(defclass event-codec ()
  ((name :initarg :name :reader event-codec-name)
   (schema-version :initarg :schema-version :reader event-codec-schema-version
                   :initform nil)
   (decode :initarg :decode :reader event-codec-decode)
   (encode :initarg :encode :reader event-codec-encode :initform nil)))

(defun event-codec-p (x)
  (typep x 'event-codec))

(defun %codec-key (name schema-version)
  (if schema-version
      (cons name schema-version)
      name))

(defun %normalize-codec-name (name)
  (cond
    ((null name) nil)
    ((keywordp name) name)
    ((symbolp name) (intern (symbol-name name) :keyword))
    ((stringp name) (intern (string-upcase name) :keyword))
    (t (intern (string-upcase (princ-to-string name)) :keyword))))

(defun register-event-codec (name decoder &key encoder schema-version)
  "Register DECODER (and optional ENCODER) for codec NAME.
   NAME is a keyword. SCHEMA-VERSION, when supplied, scopes the
   registration to that serialized schema; otherwise it applies to all
   versions. Replaces a prior registration with the same key. → EVENT-CODEC."
  (check-type decoder (or function symbol))
  (when encoder
    (check-type encoder (or function symbol)))
  (let* ((name (%normalize-codec-name name))
         (codec (make-instance 'event-codec
                               :name name
                               :schema-version schema-version
                               :decode decoder
                               :encode encoder)))
    (setf (gethash (%codec-key name schema-version) *event-codecs*) codec)
    codec))

(defun unregister-event-codec (name &key schema-version)
  "Remove the registration for NAME (and SCHEMA-VERSION when supplied).
   → the removed EVENT-CODEC, or NIL."
  (let ((key (%codec-key (%normalize-codec-name name) schema-version)))
    (prog1 (gethash key *event-codecs*)
      (remhash key *event-codecs*))))

(defun find-event-codec (name &key schema-version)
  "Return the EVENT-CODEC for NAME. Prefers a version-scoped registration,
   then an unversioned one. NIL when nothing is registered."
  (let ((name (%normalize-codec-name name)))
    (or (and schema-version
             (gethash (%codec-key name schema-version) *event-codecs*))
        (gethash (%codec-key name nil) *event-codecs*))))

(defun event-codec-names ()
  "Registered codec names (keywords), unsorted, duplicates removed."
  (let ((names '()))
    (maphash (lambda (k v)
               (declare (ignore k))
               (pushnew (event-codec-name v) names :test #'eq))
             *event-codecs*)
    names))

(defun %keyword-plist-p (value)
  "T when VALUE is an even-length list of keyword keys.
   String-key lists and vectors are not objects — do not guess."
  (and (consp value)
       (evenp (length value))
       (loop for (k nil) on value by #'cddr
             always (keywordp k))))

(defun %keywordize-key (key)
  (cond
    ((keywordp key) key)
    ((symbolp key) (intern (symbol-name key) :keyword))
    ((stringp key) (intern (string-upcase key) :keyword))
    (t (intern (string-upcase (princ-to-string key)) :keyword))))

(defun %json-key (key)
  (cond
    ((stringp key) key)
    ((keywordp key) (string-downcase (symbol-name key)))
    ((symbolp key) (string-downcase (symbol-name key)))
    (t (princ-to-string key))))

(defun %table-ref (table key)
  "Look up KEY in TABLE under keyword / string / case variants.
   → (values value present-p)."
  (let ((candidates (list key
                          (string key)
                          (string-downcase (string key))
                          (string-upcase (string key)))))
    (when (symbolp key)
      (push (intern (symbol-name key) :keyword) candidates))
    (dolist (k candidates (values nil nil))
      (multiple-value-bind (value present) (gethash k table)
        (when present
          (return (values value t)))))))

(defun %data-codec-name (data)
  (cond
    ((%keyword-plist-p data)
     (getf data :codec))
    ((hash-table-p data)
     (nth-value 0 (%table-ref data :codec)))
    (t nil)))

(defun %data-schema-version (data)
  (cond
    ((%keyword-plist-p data)
     (getf data :schema-version))
    ((hash-table-p data)
     (nth-value 0 (%table-ref data :schema-version)))
    (t nil)))

(defun %infer-codec (data)
  "Infer a codec from DATA's envelope. Never treats a vector as a plist."
  (or (%normalize-codec-name (%data-codec-name data))
      (cond
        ((%keyword-plist-p data) :sexp-plist)
        ((hash-table-p data) :json-object)
        (t nil))))

(defun %preserve-json-value (value)
  "Walk a json-protocol Lisp value: hash-table → object (keyword plist),
   vector → array (vector). Even-length string vectors stay vectors."
  (cond
    ((hash-table-p value)
     (let ((out '()))
       (maphash (lambda (k v)
                  (push (%preserve-json-value v) out)
                  (push (%keywordize-key k) out))
                value)
       out))
    ((and (vectorp value) (not (stringp value)))
     (map 'vector #'%preserve-json-value value))
    ((consp value)
     (cons (%preserve-json-value (car value))
           (%preserve-json-value (cdr value))))
    (t value)))

(defun %lisp-to-json-value (value)
  "Walk a Lisp value for the :json-object codec.
   Keyword plists and hash-tables are objects; vectors and non-keyword
   lists are arrays. #(\"a\" \"b\") and (\"a\" \"b\") stay arrays."
  (cond
    ((hash-table-p value)
     (let ((out (make-hash-table :test #'equal)))
       (maphash (lambda (k v)
                  (setf (gethash (%json-key k) out)
                        (%lisp-to-json-value v)))
                value)
       out))
    ((and (consp value) (eq (first value) :%hash-table))
     (%lisp-to-json-value (%untag-value value)))
    ((%keyword-plist-p value)
     (let ((out (make-hash-table :test #'equal)))
       (loop for (k v) on value by #'cddr
             do (setf (gethash (%json-key k) out)
                      (%lisp-to-json-value v)))
       out))
    ((and (vectorp value) (not (stringp value)))
     (map 'vector #'%lisp-to-json-value value))
    ((and (consp value) (null (cdr (last value))))
     (map 'vector #'%lisp-to-json-value value))
    (t value)))

(defun %encode-sexp-plist (event)
  (event-plist event))

(defun %decode-sexp-plist (data)
  (cond
    ((typep data 'task-event) (copy-event data))
    ((listp data) (event-from-plist data))
    (t
     (error 'task-error
            :message (format nil "sexp-plist codec expected a list, got ~s"
                             (type-of data))))))

(defun %encode-json-object (event)
  (let ((plist (event-plist event)))
    (%lisp-to-json-value
     (loop for (k v) on plist by #'cddr
           collect k
           collect (if (eq k :codec) :json-object v)))))

(defun %decode-json-object (data)
  (cond
    ((typep data 'task-event) (copy-event data))
    ((hash-table-p data)
     (event-from-plist (%preserve-json-value data)))
    ((%keyword-plist-p data)
     (event-from-plist data))
    (t
     (error 'task-error
            :message (format nil "json-object codec expected a hash-table, got ~s"
                             (type-of data))))))

(defun encode-event (event &key (codec *default-event-codec*))
  "Serialize EVENT with CODEC. Default :sexp-plist → EVENT-PLIST."
  (check-type event task-event)
  (let* ((name (%normalize-codec-name codec))
         (entry (find-event-codec name
                                  :schema-version (event-schema-version event))))
    (unless entry
      (restart-case
          (error 'task-unknown-codec
                 :codec name
                 :value event
                 :message (format nil "no encoder registered for ~s" name))
        (use-value (supplied)
          :report "Use a supplied encoding"
          (return-from encode-event supplied))))
    (let ((fn (event-codec-encode entry)))
      (unless fn
        (error 'task-error
               :message (format nil "codec ~s has no encoder" name)))
      (funcall fn event))))

(defun decode-event (data &key codec)
  "Rehydrate a TASK-EVENT from serialized DATA.
   CODEC, when supplied, selects the decoder. Otherwise the envelope
   :codec / \"codec\" field is used, then a shape default:
   keyword plist → :sexp-plist, hash-table → :json-object.
   Vectors are never treated as objects — unknown shape signals
   TASK-UNKNOWN-CODEC (USE-VALUE to supply an event or other data)."
  (when (typep data 'task-event)
    (return-from decode-event (copy-event data)))
  (let* ((name (%normalize-codec-name (or codec (%infer-codec data))))
         (version (%data-schema-version data))
         (entry (and name (find-event-codec name :schema-version version))))
    (unless entry
      (restart-case
          (error 'task-unknown-codec
                 :codec name
                 :value data
                 :message (if name
                              (format nil "no decoder registered for ~s" name)
                              "cannot infer event codec — not a plist or object"))
        (use-value (supplied)
          :report "Use a supplied event or serialized value"
          (return-from decode-event
            (if (typep supplied 'task-event)
                supplied
                (decode-event supplied :codec codec))))))
    (funcall (event-codec-decode entry) data)))

(register-event-codec :sexp-plist #'%decode-sexp-plist
                      :encoder #'%encode-sexp-plist)
(register-event-codec :json-object #'%decode-json-object
                      :encoder #'%encode-json-object)
