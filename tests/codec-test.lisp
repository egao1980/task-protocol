(in-package #:task-protocol/tests)

(defun %payload-equal (a b)
  "Structural equality that distinguishes JSON arrays (vectors) from
   objects (keyword plists / hash-tables)."
  (cond
    ((and (hash-table-p a) (hash-table-p b))
     (and (= (hash-table-count a) (hash-table-count b))
          (let ((ok t))
            (maphash (lambda (k v)
                       (multiple-value-bind (other present) (gethash k b)
                         (unless (and present (%payload-equal v other))
                           (setf ok nil))))
                     a)
            ok)))
    ((and (hash-table-p a)
          (consp b)
          (evenp (length b))
          (loop for (k nil) on b by #'cddr always (keywordp k)))
     (%payload-equal a
                     (let ((h (make-hash-table :test #'equal)))
                       (loop for (k v) on b by #'cddr
                             do (setf (gethash (string-downcase (string k)) h) v))
                       h)))
    ((and (hash-table-p b) (consp a))
     (%payload-equal b a))
    ((and (consp a) (consp b)
          (evenp (length a)) (evenp (length b))
          (loop for (k nil) on a by #'cddr always (keywordp k))
          (loop for (k nil) on b by #'cddr always (keywordp k)))
     (and (= (length a) (length b))
          (loop for (k v) on a by #'cddr
                always (%payload-equal v (getf b k)))))
    ((and (vectorp a) (not (stringp a))
          (vectorp b) (not (stringp b)))
     (and (= (length a) (length b))
          (loop for i from 0 below (length a)
                always (%payload-equal (aref a i) (aref b i)))))
    ((and (vectorp a) (not (stringp a)))
     nil)
    ((and (vectorp b) (not (stringp b)))
     nil)
    (t (equal a b))))

(defun %step-with-result (result)
  (make-instance 'task-protocol:step-completed
                 :task-id "t-codec"
                 :name "n"
                 :result result
                 :idempotency-key "k"
                 :seq 1
                 :timestamp 1
                 :run-id (task-protocol:make-run-id "run-c")
                 :activation-id (task-protocol:make-activation-id "act-c")
                 :schema-version "0.2.1"))

(defun %roundtrip (event &key (codec :sexp-plist))
  (task-protocol:decode-event (task-protocol:encode-event event :codec codec)
                              :codec codec))

(defparameter *nested-roundtrip-payloads*
  '(#("a" "b")
    #("a" "b" "c" "d")
    #()
    #("only")
    (:a "b")
    (:items #("a" "b") :obj (:k 1))
    (:deep #(#("a" "b") (:x 1) #(#("y" "z"))))
    (:mixed #("a" "b") :obj (:k #("c" "d")) :n 3)
    #("nest" #("a" "b") (:flag t))))

(defun %assert-nested-roundtrip (payload copy &key encoded schema-version)
  (when encoded
    (ok (hash-table-p encoded)))
  (ok (typep copy 'task-protocol:step-completed))
  (ok (%payload-equal payload (task-protocol:step-result copy)))
  (when (and (vectorp payload) (not (stringp payload)))
    (ok (vectorp (task-protocol:step-result copy)))
    (ok (not (consp (task-protocol:step-result copy)))))
  (when schema-version
    (ok (equal schema-version (task-protocol:event-schema-version copy))))
  (ok (equal "run-c" (task-protocol:run-id-value
                      (task-protocol:event-run-id copy))))
  (ok (equal "act-c" (task-protocol:activation-id-value
                      (task-protocol:event-activation-id copy)))))

(deftest sexp-plist-nested-roundtrip
  (dolist (payload *nested-roundtrip-payloads*)
    (testing (format nil "sexp-plist ~s" payload)
      (let* ((event (%step-with-result payload))
             (copy (%roundtrip event :codec :sexp-plist)))
        (%assert-nested-roundtrip payload copy)))))

(deftest json-object-nested-roundtrip
  (dolist (payload *nested-roundtrip-payloads*)
    (testing (format nil "json-object ~s" payload)
      (let* ((event (%step-with-result payload))
             (encoded (task-protocol:encode-event event :codec :json-object))
             (copy (task-protocol:decode-event encoded :codec :json-object)))
        (%assert-nested-roundtrip payload copy
                                  :encoded encoded
                                  :schema-version "0.2.1")))))

(deftest even-length-string-vector-is-not-a-plist
  (let* ((arr #("a" "b"))
         (event (%step-with-result arr))
         (sexp (task-protocol:decode-event
                (task-protocol:encode-event event :codec :sexp-plist)))
         (json (task-protocol:decode-event
                (task-protocol:encode-event event :codec :json-object)
                :codec :json-object))
         (raw-object (let ((h (make-hash-table :test #'equal)))
                       (setf (gethash "type" h) "step-completed"
                             (gethash "task-id" h) "t"
                             (gethash "name" h) "n"
                             (gethash "result" h) arr)
                       h))
         (from-raw (task-protocol:decode-event raw-object)))
    (ok (vectorp (task-protocol:step-result sexp)))
    (ok (equalp arr (task-protocol:step-result sexp)))
    (ok (not (consp (task-protocol:step-result sexp))))
    (ok (vectorp (task-protocol:step-result json)))
    (ok (equalp arr (task-protocol:step-result json)))
    (ok (not (consp (task-protocol:step-result json))))
    (ok (vectorp (task-protocol:step-result from-raw)))
    (ok (equalp arr (task-protocol:step-result from-raw)))))

(deftest json-object-vs-array-shapes
  (let ((object (let ((h (make-hash-table :test #'equal)))
                  (setf (gethash "a" h) "b")
                  h))
        (array #("a" "b")))
    (let* ((obj-event (%step-with-result object))
           (arr-event (%step-with-result array))
           (obj-enc (task-protocol:encode-event obj-event :codec :json-object))
           (arr-enc (task-protocol:encode-event arr-event :codec :json-object))
           (obj-dec (task-protocol:decode-event obj-enc :codec :json-object))
           (arr-dec (task-protocol:decode-event arr-enc :codec :json-object)))
      (ok (hash-table-p (gethash "result" obj-enc)))
      (ok (vectorp (gethash "result" arr-enc)))
      (ok (not (vectorp (gethash "result" obj-enc))))
      (ok (not (hash-table-p (gethash "result" arr-enc))))
      (ok (%payload-equal '(:a "b") (task-protocol:step-result obj-dec)))
      (ok (vectorp (task-protocol:step-result arr-dec)))
      (ok (equalp array (task-protocol:step-result arr-dec))))))

(deftest decode-event-does-not-guess-vector-plist
  (ok (signals (task-protocol:decode-event #("a" "b"))
               'task-protocol:task-unknown-codec))
  (ok (signals (task-protocol:event-from-plist #("a" "b"))
               'type-error)))

(deftest decode-event-use-value-restart
  (let* ((fallback (make-instance 'task-protocol:task-started
                                  :task-id "recovered"))
         (got nil))
    (handler-bind ((task-protocol:task-unknown-codec
                    (lambda (c)
                      (use-value fallback c))))
      (setf got (task-protocol:decode-event #("a" "b"))))
    (ok (typep got 'task-protocol:task-started))
    (ok (equal "recovered" (task-protocol:event-task-id got)))))

(deftest register-event-codec-is-used
  (let ((name (intern "H5-TEST-CODEC" :keyword))
        (seen nil))
    (unwind-protect
         (progn
           (task-protocol:register-event-codec
            name
            (lambda (data)
              (setf seen data)
              (make-instance 'task-protocol:task-started :task-id "reg"))
            :encoder (lambda (event)
                       (list :codec name
                             :task-id (task-protocol:event-task-id event))))
           (ok (find name (task-protocol:event-codec-names) :test #'eq))
           (let* ((event (make-instance 'task-protocol:task-started
                                        :task-id "reg"))
                  (encoded (task-protocol:encode-event event :codec name))
                  (decoded (task-protocol:decode-event encoded :codec name)))
             (ok (equal "reg" (getf encoded :task-id)))
             (ok (eq name (getf encoded :codec)))
             (ok (typep decoded 'task-protocol:task-started))
             (ok (equal encoded seen))))
      (task-protocol:unregister-event-codec name))))

(deftest unknown-codec-signals
  (ok (signals (task-protocol:encode-event
                (make-instance 'task-protocol:task-started :task-id "t")
                :codec :no-such-codec)
               'task-protocol:task-unknown-codec))
  (ok (signals (task-protocol:decode-event '(:type :task-started :codec :no-such-codec)
                                           :codec :no-such-codec)
               'task-protocol:task-unknown-codec)))

(deftest sexp-plist-envelope-is-versioned
  (let* ((event (%step-with-result '(:ok t)))
         (plist (task-protocol:encode-event event :codec :sexp-plist)))
    (ok (eq :sexp-plist (getf plist :codec)))
    (ok (eq :step-completed (getf plist :type)))
    (ok (equal "0.2.1" (getf plist :schema-version)))
    (let ((copy (task-protocol:event-from-plist plist)))
      (ok (typep copy 'task-protocol:step-completed))
      (ok (equal '(:ok t) (task-protocol:step-result copy))))))

(deftest import-events-accepts-json-object
  (let* ((journal (task-protocol:make-in-memory-journal))
         (event (%step-with-result #("a" "b")))
         (object (task-protocol:encode-event event :codec :json-object)))
    (task-protocol:import-events journal (list object))
    (let ((loaded (first (task-protocol:journal-events journal "t-codec"))))
      (ok (typep loaded 'task-protocol:step-completed))
      (ok (vectorp (task-protocol:step-result loaded)))
      (ok (equalp #("a" "b") (task-protocol:step-result loaded))))))

(deftest effect-receipt-json-object-roundtrip
  (let* ((receipt (task-protocol:make-effect-receipt
                   :name "write"
                   :idempotency-key "fx-1"
                   :payload (list :items #("a" "b") :ok t)
                   :task-id "t-fx"
                   :run-id (task-protocol:make-run-id "run-fx")
                   :activation-id (task-protocol:make-activation-id "act-fx")
                   :schema-version "0.2.1"))
         (copy (task-protocol:decode-event
                (task-protocol:encode-event receipt :codec :json-object)
                :codec :json-object)))
    (ok (task-protocol:effect-receipt-p copy))
    (ok (equal "write" (task-protocol:effect-receipt-name copy)))
    (ok (equal "fx-1" (task-protocol:effect-receipt-idempotency-key copy)))
    (ok (vectorp (getf (task-protocol:effect-receipt-payload copy) :items)))
    (ok (equalp #("a" "b")
                (getf (task-protocol:effect-receipt-payload copy) :items)))
    (ok (eq t (getf (task-protocol:effect-receipt-payload copy) :ok)))
    (ok (equal "run-fx" (task-protocol:run-id-value
                         (task-protocol:event-run-id copy))))))
