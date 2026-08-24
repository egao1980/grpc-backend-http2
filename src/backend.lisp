(in-package #:grpc-backend-http2)

(defclass http2-grpc-backend (grpc-protocol:grpc-backend) ())

(defclass http2-grpc-channel (grpc-protocol:grpc-channel)
  ((http-backend :initarg :http-backend :initform nil :accessor http2-channel-http-backend)
   (http-client :initarg :http-client :initform nil :accessor http2-channel-http-client)))

(defclass http2-grpc-stream (grpc-protocol:grpc-stream) ())

(defparameter *internal-metadata-keys*
  '(:response-class :http-backend :http-client)
  "Metadata keys consumed here — not sent as gRPC headers.")

(defun make-http2-grpc-backend ()
  (make-instance 'http2-grpc-backend))

(defun use-http2-grpc-backend ()
  (setf grpc-protocol:*grpc-backend* (make-http2-grpc-backend)))

(defun %message-octets (request)
  (cond
    ((and (vectorp request) (not (stringp request)))
     (coerce request '(vector (unsigned-byte 8))))
    (t
     (let ((encode (and (find-package '#:protobuf-protocol)
                        (find-symbol "ENCODE-TO-OCTETS" '#:protobuf-protocol))))
       (if (and encode (fboundp encode))
           (funcall encode request)
           (error 'grpc-protocol:grpc-error
                  :status :invalid-argument
                  :message "request must be octets or a proto message"))))))

(defun %decode (octets class)
  (let ((decode (and (find-package '#:protobuf-protocol)
                     (find-symbol "DECODE-OCTETS" '#:protobuf-protocol))))
    (if (and decode (fboundp decode))
        (funcall decode octets class)
        octets)))

(defun %normalize-method (method)
  (let ((s (string method)))
    (if (and (plusp (length s)) (char= (char s 0) #\/))
        s
        (concatenate 'string "/" s))))

(defun %call-url (channel method)
  (let* ((target (grpc-protocol:grpc-channel-target channel))
         (method* (%normalize-method method))
         (base (if (search "://" target)
                   (string-right-trim "/" target)
                   (format nil "https://~A" target))))
    (concatenate 'string base method*)))

(defun %metadata-headers (metadata)
  (loop for (k v) on metadata by #'cddr
        unless (member k *internal-metadata-keys* :test #'eq)
          collect (cons (string-downcase (string k)) (princ-to-string v))))

(defun %grpc-request-headers (metadata timeout)
  (let ((hdrs (append
               '(("content-type" . "application/grpc")
                 ("te" . "trailers")
                 ("user-agent" . "grpc-lisp/grpc-backend-http2"))
               (%metadata-headers metadata)))
        (to (format-grpc-timeout timeout)))
    (if to
        (cons (cons "grpc-timeout" to) hdrs)
        hdrs)))

(defun %ensure-http-backend (channel)
  (or (http2-channel-http-backend channel)
      http-protocol:*http-backend*
      (progn
        (handler-case (asdf:load-system "http-backend-async")
          (error (e)
            (error 'grpc-protocol:grpc-error
                   :status :internal
                   :message (format nil "failed to load http-backend-async: ~A" e))))
        (or http-protocol:*http-backend*
            (let ((sym (find-symbol "MAKE-ASYNC-BACKEND" "HTTP-BACKEND-ASYNC")))
              (unless (and sym (fboundp sym))
                (error 'grpc-protocol:grpc-error
                       :status :internal
                       :message "http-backend-async:MAKE-ASYNC-BACKEND missing"))
              (let ((b (funcall sym)))
                (setf http-protocol:*http-backend* b)
                b))))))

(defun %ensure-http-client (channel http)
  (or (http2-channel-http-client channel)
      http-protocol:*http-client*
      (http-protocol:make-http-client http :http-version :http/2)))

(defun %header (headers name)
  (cond
    ((null headers) nil)
    ((hash-table-p headers)
     (or (gethash (string-downcase name) headers)
         (gethash name headers)))
    (t (cdr (assoc name headers :test #'string-equal)))))

(defun %response-octets (body)
  (cond
    ((null body)
     (make-array 0 :element-type '(unsigned-byte 8)))
    ((and (vectorp body) (not (stringp body)))
     (coerce body '(vector (unsigned-byte 8))))
    (t
     (error 'grpc-protocol:grpc-error
            :status :internal
            :message "gRPC response body must be octets"))))

(defmethod grpc-protocol:backend-grpc-connect
    ((backend http2-grpc-backend) target &key credentials metadata)
  (when (eq credentials :insecure)
    (error 'grpc-protocol:grpc-error
           :status :unimplemented
           :message "grpc-backend-http2 wave-1 is TLS only (no h2c)"))
  (make-instance 'http2-grpc-channel
                 :target target
                 :backend backend
                 :credentials (or credentials :ssl)
                 :metadata metadata
                 :http-backend (getf metadata :http-backend)
                 :http-client (getf metadata :http-client)))

(defmethod grpc-protocol:backend-grpc-call
    ((channel http2-grpc-channel) method request &key timeout metadata)
  (when (grpc-protocol:grpc-channel-closed-p channel)
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "channel is closed"))
  (let* ((merged (append metadata (grpc-protocol:grpc-channel-metadata channel)))
         (http (%ensure-http-backend channel))
         (client (%ensure-http-client channel http))
         (url (%call-url channel method))
         (framed (frame-message (%message-octets request)))
         (req (http-protocol:make-http-request
               :method :post
               :url url
               :headers (%grpc-request-headers merged timeout)
               :content framed
               :http-version :http/2
               :force-binary t
               :accept-encoding nil
               :decompress nil))
         (res (http-protocol:send http client req))
         (status (grpc-status-keyword
                  (%header (http-protocol:response-headers res) "grpc-status")))
         (message (%header (http-protocol:response-headers res) "grpc-message")))
    (unless (eq status :ok)
      (error 'grpc-protocol:grpc-error
             :status (if (and (null (%header (http-protocol:response-headers res)
                                             "grpc-status"))
                              (not (= 200 (http-protocol:response-status res))))
                         :unknown
                         status)
             :message (or message
                          (format nil "grpc-status ~A (HTTP ~A)"
                                  status (http-protocol:response-status res)))
             :details res))
    (multiple-value-bind (octets)
        (unframe-message (%response-octets (http-protocol:response-body res)))
      (let ((class (getf merged :response-class)))
        (if class
            (%decode octets class)
            octets)))))

(defmethod grpc-protocol:backend-grpc-stream
    ((channel http2-grpc-channel) method &key metadata)
  (declare (ignore method metadata))
  (error 'grpc-protocol:grpc-error
         :status :unimplemented
         :message "grpc-backend-http2 wave-1 is unary only"))

(defmethod grpc-protocol:grpc-send ((stream http2-grpc-stream) message &key)
  (declare (ignore message))
  (error 'grpc-protocol:grpc-error
         :status :unimplemented
         :message "grpc-backend-http2 wave-1 is unary only"))

(defmethod grpc-protocol:grpc-recv ((stream http2-grpc-stream) &key timeout)
  (declare (ignore timeout))
  (error 'grpc-protocol:grpc-error
         :status :unimplemented
         :message "grpc-backend-http2 wave-1 is unary only"))

(eval-when (:load-toplevel :execute)
  (use-http2-grpc-backend))
