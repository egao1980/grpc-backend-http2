(in-package #:grpc-backend-http2)

;;; TLS accept loop over http-server-backend-http2 (zellerin http2).
;;; No h2c / :insecure. Unary + server-stream + client-stream + interleaved bidi.

(defclass http2-grpc-server (grpc-protocol:grpc-server)
  ((http-handler :initform nil :accessor http2-grpc-server-http-handler)))

(defclass http2-grpc-accept-stream (grpc-protocol:grpc-stream)
  ((h2 :initarg :h2 :accessor http2-accept-h2)
   (headers-sent-p :initform nil :accessor http2-accept-headers-sent-p)
   (finished-p :initform nil :accessor http2-accept-finished-p)
   (body-pos :initform 0 :accessor http2-accept-body-pos)
   (request-encoding :initarg :request-encoding :initform nil
                     :accessor http2-accept-request-encoding)
   (response-compression :initarg :response-compression :initform nil
                         :accessor http2-accept-response-compression)
   (status :initform :ok :accessor http2-accept-status)
   (status-message :initform nil :accessor http2-accept-status-message)))

(defvar *grpc-serve-classes-ready* nil)

(defun %core-sym (name)
  (let ((s (and (find-package :http2/core)
                (find-symbol (string name) :http2/core))))
    (unless (and s (or (fboundp s) (boundp s) (find-class s nil)))
      (error 'grpc-protocol:grpc-error
             :status :internal
             :message (format nil "http2/core:~A missing" name)))
    s))

(defun %optional-core-sym (name)
  (and (find-package :http2/core)
       (find-symbol (string name) :http2/core)))

(defun %credentials-plist (credentials)
  (cond
    ((and (consp credentials) (eq (car credentials) :ssl))
     (rest credentials))
    ((and (consp credentials) (keywordp (car credentials)))
     credentials)
    (t nil)))

(defun %tls-paths (credentials metadata)
  (let ((plist (append (%credentials-plist credentials) metadata)))
    (values (or (getf plist :cert)
                (getf plist :ssl-cert)
                (getf plist :certificate)
                (getf plist :certificate-file))
            (or (getf plist :key)
                (getf plist :ssl-key)
                (getf plist :private-key)
                (getf plist :private-key-file)))))

(defun %request-header (h2 name)
  (let ((ht (ignore-errors
              (http-server-backend-http2:http2-stream-request-headers h2))))
    (when (hash-table-p ht)
      (or (gethash name ht)
          (gethash (string-downcase name) ht)
          (gethash (string-upcase name) ht)))))

(defun %grpc-content-type-p (h2)
  (let ((ct (%request-header h2 "content-type")))
    (and ct (search "application/grpc" (string-downcase (princ-to-string ct))))))

(defun %h2-path (h2)
  (let ((fn (%optional-core-sym '#:get-path)))
    (or (and fn (funcall fn h2)) "/")))

(defun %h2-connection (h2)
  (funcall (%core-sym '#:get-connection) h2))

(defun %h2-body (h2)
  (let ((fn (%optional-core-sym '#:get-body)))
    (if (and fn (fboundp fn))
        (or (funcall fn h2)
            (make-array 0 :element-type '(unsigned-byte 8)))
        (make-array 0 :element-type '(unsigned-byte 8)))))

(defun %h2-flush (h2)
  (let ((flush (%optional-core-sym '#:flush-http2-data))
        (conn (ignore-errors (%h2-connection h2))))
    (when (and flush conn)
      (ignore-errors (funcall flush conn)))))

(defun %dispatched-p (h2)
  (and (slot-exists-p h2 'http-server-backend-http2::connect-dispatched-p)
       (slot-value h2 'http-server-backend-http2::connect-dispatched-p)))

(defun %mark-dispatched (h2)
  (when (slot-exists-p h2 'http-server-backend-http2::connect-dispatched-p)
    (setf (slot-value h2 'http-server-backend-http2::connect-dispatched-p) t))
  t)

(defun %wait-h2 (h2)
  (let ((lock (and (slot-exists-p h2 'accept-lock) (slot-value h2 'accept-lock)))
        (cv (and (slot-exists-p h2 'accept-cv) (slot-value h2 'accept-cv))))
    (if (and lock cv)
        (with-lock-held (lock)
          (unless (and (slot-exists-p h2 'peer-ended-p)
                       (slot-value h2 'peer-ended-p))
            (condition-wait cv lock :timeout 0.25)))
        (sleep 0.01))))

(defun %peer-ended-p (h2)
  (and (slot-exists-p h2 'peer-ended-p)
       (slot-value h2 'peer-ended-p)))

(defun %notify-h2 (h2)
  (let ((lock (and (slot-exists-p h2 'accept-lock) (slot-value h2 'accept-lock)))
        (cv (and (slot-exists-p h2 'accept-cv) (slot-value h2 'accept-cv))))
    (when (and lock cv)
      (with-lock-held (lock)
        (condition-notify cv)))))

(defun %port-from-url (url)
  (cond
    ((null url) nil)
    ((integerp url) url)
    (t
     (let* ((s (princ-to-string url))
            (colon (position #\: s :from-end t)))
       (when colon
         (parse-integer s :start (1+ colon) :junk-allowed t))))))

(defun %ensure-grpc-serve-classes ()
  "Subclass http2-connect-* after the H2 server classes exist."
  (http-server-backend-http2:ensure-http2-connect-classes)
  (when *grpc-serve-classes-ready*
    (return-from %ensure-grpc-serve-classes t))
  (let ((body (%core-sym '#:body-collecting-mixin))
        (apply-data (%core-sym '#:apply-data-frame))
        (process-end (%core-sym '#:process-end-headers))
        (peer-ends (%core-sym '#:peer-ends-http-stream))
        (parent-conn (find-class 'http-server-backend-http2:http2-connect-connection nil))
        (parent-stream (find-class 'http-server-backend-http2:http2-connect-stream nil)))
    (unless (and parent-conn parent-stream)
      (error 'grpc-protocol:grpc-error
             :status :internal
             :message "http-server-backend-http2 connect classes missing"))
    (unless (find-class 'grpc-http2-connection nil)
      (eval `(defclass grpc-http2-connection
                 (http-server-backend-http2:http2-connect-connection)
               ((grpc-accept :initarg :grpc-accept
                             :accessor grpc-http2-connection-accept)))))
    (unless (find-class 'grpc-http2-stream nil)
      (eval `(defclass grpc-http2-stream
                 (http-server-backend-http2:http2-connect-stream ,body)
               ((peer-ended-p :initform nil :accessor grpc-http2-stream-peer-ended-p)
                (accept-lock :initform (make-lock "grpc-accept")
                             :accessor grpc-http2-stream-lock)
                (accept-cv :initform (make-condition-variable :name "grpc-accept")
                           :accessor grpc-http2-stream-cv)))))
    (eval `(defmethod ,apply-data :after ((stream grpc-http2-stream) data start end)
             (declare (ignore data start end))
             (%notify-h2 stream)))
    (eval `(defmethod ,process-end :after (connection (stream grpc-http2-stream))
             (when (and (not (%dispatched-p stream))
                        (%grpc-content-type-p stream)
                        (functionp (ignore-errors
                                     (grpc-http2-connection-accept connection))))
               (%mark-dispatched stream)
               (let ((accept (grpc-http2-connection-accept connection)))
                 (make-thread (lambda ()
                                (handler-case (funcall accept stream)
                                  (error (e)
                                    (ignore-errors
                                      (let ((st (make-instance
                                                 'http2-grpc-accept-stream
                                                 :channel nil
                                                 :method (%h2-path stream)
                                                 :h2 stream)))
                                        (setf (http2-accept-status st) :internal
                                              (http2-accept-status-message st)
                                              (princ-to-string e))
                                        (%finish-accept st))))))
                              :name "grpc-accept")))))
    (eval `(defmethod ,peer-ends :around ((stream grpc-http2-stream))
             (when (slot-exists-p stream 'peer-ended-p)
               (setf (slot-value stream 'peer-ended-p) t))
             (%notify-h2 stream)
             (if (%dispatched-p stream)
                 nil
                 (call-next-method))))
    (setf *grpc-serve-classes-ready* t)
    t))

(defun %ensure-response-headers (stream)
  (unless (http2-accept-headers-sent-p stream)
    (let ((send (%core-sym '#:send-headers))
          (h2 (http2-accept-h2 stream))
          (comp (http2-accept-response-compression stream)))
      (funcall send h2
               (append '((:status "200")
                         ("content-type" "application/grpc"))
                       (when comp
                         (list (list "grpc-encoding"
                                     (string-downcase (symbol-name comp))))))
               :end-stream nil
               :end-headers t)
      (setf (http2-accept-headers-sent-p stream) t)
      (%h2-flush h2)))
  stream)

(defun %finish-accept (stream)
  (unless (http2-accept-finished-p stream)
    (setf (http2-accept-finished-p stream) t
          (grpc-protocol:grpc-stream-closed-p stream) t)
    (%ensure-response-headers stream)
    (let ((send (%core-sym '#:send-headers))
          (h2 (http2-accept-h2 stream))
          (code (grpc-status-code (http2-accept-status stream)))
          (msg (or (http2-accept-status-message stream) "")))
      (funcall send h2
               `(("grpc-status" ,(princ-to-string code))
                 ("grpc-message" ,msg))
               :end-stream t
               :end-headers t)
      (%h2-flush h2)))
  stream)

(defun %decode-accept-payload (payload compressed-p encoding)
  (cond
    ((not compressed-p) payload)
    ((or (null encoding) (eq encoding :identity))
     (error 'grpc-protocol:grpc-error
            :status :internal
            :message "compressed grpc frame without grpc-encoding"))
    (t (decompress-payload encoding payload))))

(defun %read-accept-frame (stream)
  (let ((h2 (http2-accept-h2 stream))
        (encoding (http2-accept-request-encoding stream)))
    (loop
      (multiple-value-bind (payload next compressed-p)
          (unframe-next (%h2-body h2) :start (http2-accept-body-pos stream))
        (when payload
          (setf (http2-accept-body-pos stream) next)
          (return (%decode-accept-payload payload compressed-p encoding)))
        (when (%peer-ended-p h2)
          (multiple-value-bind (payload2 next2 compressed2)
              (unframe-next (%h2-body h2) :start (http2-accept-body-pos stream))
            (when payload2
              (setf (http2-accept-body-pos stream) next2)
              (return (%decode-accept-payload payload2 compressed2 encoding))))
          (return :eof))
        (%wait-h2 h2)))))

(defmethod grpc-protocol:grpc-send ((stream http2-grpc-accept-stream) message &key end)
  (when (or (grpc-protocol:grpc-stream-closed-p stream)
            (http2-accept-finished-p stream))
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "accept stream is closed"))
  (when message
    (let* ((h2 (http2-accept-h2 stream))
           (conn (%h2-connection h2))
           (write (%core-sym '#:write-binary-payload))
           (framed (%frame-request (%message-octets message)
                                   (http2-accept-response-compression stream))))
      (%ensure-response-headers stream)
      (funcall write conn h2 framed :end-stream nil)
      (%h2-flush h2)))
  (when end
    (%finish-accept stream))
  message)

(defmethod grpc-protocol:grpc-recv ((stream http2-grpc-accept-stream) &key timeout)
  (declare (ignore timeout))
  (when (grpc-protocol:grpc-stream-closed-p stream)
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "accept stream is closed"))
  (%read-accept-frame stream))

(defmethod grpc-protocol:grpc-close ((stream http2-grpc-accept-stream) &key)
  (%finish-accept stream)
  stream)

(defun %dispatch-accept (handler stream)
  (let ((fn (grpc-protocol:grpc-method-handler-function handler)))
    (ecase (grpc-protocol:grpc-method-handler-kind handler)
      (:unary
       (let* ((req (grpc-protocol:grpc-recv stream))
              (res (funcall fn req stream)))
         (when (and res (not (eq res :eof)))
           (grpc-protocol:grpc-send stream res))))
      (:server-stream
       (funcall fn (grpc-protocol:grpc-recv stream) stream))
      ((:client-stream :bidi)
       (funcall fn stream)))))

(defun %handle-h2-accept (backend handlers h2)
  (declare (ignore backend))
  (let* ((method (%h2-path h2))
         (handler (grpc-protocol:find-grpc-method-handler handlers method))
         (enc (normalize-compression
               (parse-grpc-encoding (%request-header h2 "grpc-encoding"))))
         (stream (make-instance 'http2-grpc-accept-stream
                                :channel nil
                                :method method
                                :h2 h2
                                :request-encoding enc
                                :response-compression enc)))
    (handler-case
        (cond
          ((null handler)
           (setf (http2-accept-status stream) :unimplemented
                 (http2-accept-status-message stream)
                 (format nil "Method not found: ~A" method))
           (%finish-accept stream))
          (t
           (%dispatch-accept handler stream)
           (%finish-accept stream)))
      (grpc-protocol:grpc-error (c)
        (unless (http2-accept-finished-p stream)
          (setf (http2-accept-status stream)
                (or (grpc-protocol:grpc-error-status c) :internal)
                (http2-accept-status-message stream)
                (or (grpc-protocol:grpc-error-message c) (princ-to-string c)))
          (ignore-errors (%finish-accept stream))))
      (error (e)
        (unless (http2-accept-finished-p stream)
          (setf (http2-accept-status stream) :internal
                (http2-accept-status-message stream) (princ-to-string e))
          (ignore-errors (%finish-accept stream)))))))

(defun %dummy-clack (env)
  (declare (ignore env))
  '(415 (:content-type "text/plain") ("application/grpc required")))

(defmethod grpc-protocol:backend-grpc-serve
    ((backend http2-grpc-backend) handlers
     &key (host "127.0.0.1") port credentials metadata)
  (when (eq credentials :insecure)
    (error 'grpc-protocol:grpc-error
           :status :unimplemented
           :message "grpc-backend-http2 serve is TLS only (no h2c)"))
  (multiple-value-bind (cert key)
      (%tls-paths credentials metadata)
    (unless (and cert key)
      (error 'grpc-protocol:grpc-error
             :status :invalid-argument
             :message "grpc-serve requires TLS :cert and :key (or :ssl-cert / :ssl-key)"))
    (let ((cert* (probe-file cert))
          (key* (probe-file key)))
      (unless (and cert* key*)
        (error 'grpc-protocol:grpc-error
               :status :invalid-argument
               :message (format nil "TLS files not found: cert=~S key=~S" cert key)))
      (unless (http-server-backend-http2:http2-server-available-p)
        (error 'grpc-protocol:grpc-error
               :status :unimplemented
               :message "http2/server/threaded is not loadable"))
      (handler-case
          (%ensure-grpc-serve-classes)
        (error (e)
          (error 'grpc-protocol:grpc-error
                 :status :unimplemented
                 :message (format nil "http2 server classes unavailable: ~A" e))))
      (let* ((start (or (and (find-package :http2/server)
                             (find-symbol "START" :http2/server))
                        (error 'grpc-protocol:grpc-error
                               :status :internal
                               :message "http2/server:START missing")))
             (dispatcher-sym
               (or (and (find-package :http2/server)
                        (find-symbol "*VANILLA-SERVER-DISPATCHER*" :http2/server))
                   (and (find-package :http2/server/shared)
                        (find-symbol "*VANILLA-SERVER-DISPATCHER*"
                                     :http2/server/shared))))
             (dispatcher-class (and dispatcher-sym (symbol-value dispatcher-sym)))
             (accept (lambda (h2)
                       (%handle-h2-accept backend handlers h2))))
        (unless dispatcher-class
          (error 'grpc-protocol:grpc-error
                 :status :internal
                 :message "http2 vanilla dispatcher missing"))
        (multiple-value-bind (handler url)
            (funcall start (or port 0)
                     :host host
                     :dispatcher
                     (make-instance
                      dispatcher-class
                      :private-key-file (namestring key*)
                      :certificate-file (namestring cert*)
                      :connection-class 'grpc-http2-connection
                      :connection-args
                      (list :app #'%dummy-clack
                            :grpc-accept accept
                            :stream-class 'grpc-http2-stream)))
          (let ((server (make-instance 'http2-grpc-server
                                       :backend backend
                                       :host host
                                       :port (or (%port-from-url url) port)
                                       :credentials (or credentials :ssl)
                                       :handlers handlers)))
            (setf (http2-grpc-server-http-handler server) handler
                  (grpc-protocol:grpc-server-running-p server) t)
            server))))))

(defmethod grpc-protocol:backend-grpc-stop ((server http2-grpc-server) &key)
  (let ((stop (and (find-package :http2/server)
                   (find-symbol "STOP" :http2/server)))
        (handler (http2-grpc-server-http-handler server)))
    (when (and stop handler)
      (ignore-errors (funcall stop handler)))
    (setf (http2-grpc-server-http-handler server) nil
          (grpc-protocol:grpc-server-running-p server) nil)
    server))
