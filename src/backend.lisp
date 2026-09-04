(in-package #:grpc-backend-http2)

(defclass http2-grpc-backend (grpc-protocol:grpc-backend) ())

(defclass http2-grpc-channel (grpc-protocol:grpc-channel)
  ((http-backend :initarg :http-backend :initform nil :accessor http2-channel-http-backend)
   (http-client :initarg :http-client :initform nil :accessor http2-channel-http-client)))

;;; HTTP2-GRPC-STREAM is defined with stream slots below.

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

(defun %response-trailers (res)
  (let ((fn (find-symbol (string '#:response-trailers) :http-protocol)))
    (when (and fn (fboundp fn))
      (ignore-errors (funcall fn res)))))

(defun %grpc-wire-status (res)
  (or (%header (%response-trailers res) "grpc-status")
      (%header (http-protocol:response-headers res) "grpc-status")))

(defun %grpc-wire-message (res)
  (or (%header (%response-trailers res) "grpc-message")
      (%header (http-protocol:response-headers res) "grpc-message")))

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
         (status (grpc-status-keyword (%grpc-wire-status res)))
         (message (%grpc-wire-message res)))
    (unless (eq status :ok)
      (error 'grpc-protocol:grpc-error
             :status (if (and (null (%grpc-wire-status res))
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

(defclass http2-grpc-stream (grpc-protocol:grpc-stream)
  ((url :initarg :url :accessor http2-stream-url)
   (http-backend :initarg :http-backend :accessor http2-stream-http-backend)
   (http-client :initarg :http-client :accessor http2-stream-http-client)
   (metadata :initarg :metadata :accessor http2-stream-metadata)
   (timeout :initarg :timeout :accessor http2-stream-timeout :initform nil)
   (send-queue :initform nil :accessor http2-stream-send-queue)
   (http-started-p :initform nil :accessor http2-stream-http-started-p)
   (body :initform nil :accessor http2-stream-body)
   (body-pos :initform 0 :accessor http2-stream-body-pos)
   (response :initform nil :accessor http2-stream-response)
   (eof-p :initform nil :accessor http2-stream-eof-p)
   (half-closed-p :initform nil :accessor http2-stream-half-closed-p)))

(defmethod grpc-protocol:backend-grpc-stream
    ((channel http2-grpc-channel) method &key metadata)
  (when (grpc-protocol:grpc-channel-closed-p channel)
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "channel is closed"))
  (let* ((merged (append metadata (grpc-protocol:grpc-channel-metadata channel)))
         (http (%ensure-http-backend channel))
         (client (%ensure-http-client channel http)))
    (make-instance 'http2-grpc-stream
                   :channel channel
                   :method method
                   :url (%call-url channel method)
                   :http-backend http
                   :http-client client
                   :metadata merged
                   :timeout (getf merged :timeout))))

(defun %raise-if-bad-status (res &key (need-grpc-status nil))
  "On headers (NEED-GRPC-STATUS NIL) only fail non-200 HTTP.
   After END_STREAM, require grpc-status (headers or trailers)."
  (let ((wire (%grpc-wire-status res))
        (message (%grpc-wire-message res)))
    (when (and (null wire)
               (not (= 200 (http-protocol:response-status res))))
      (error 'grpc-protocol:grpc-error
             :status :unknown
             :message (format nil "HTTP ~A" (http-protocol:response-status res))
             :details res))
    (when wire
      (let ((status (grpc-status-keyword wire)))
        (unless (eq status :ok)
          (error 'grpc-protocol:grpc-error
                 :status status
                 :message (or message (format nil "grpc-status ~A" status))
                 :details res))))
    (when (and need-grpc-status (null wire))
      (error 'grpc-protocol:grpc-error
             :status :unknown
             :message "missing grpc-status trailer"
             :details res))))

(defun %flush-http2-stream (stream)
  "POST queued frames with :want-stream. Further SEND after this is unimplemented."
  (when (http2-stream-http-started-p stream)
    (return-from %flush-http2-stream stream))
  (let* ((payloads (nreverse (http2-stream-send-queue stream)))
         (body (concat-frames payloads))
         (req (http-protocol:make-http-request
               :method :post
               :url (http2-stream-url stream)
               :headers (%grpc-request-headers (http2-stream-metadata stream)
                                               (http2-stream-timeout stream))
               :content body
               :http-version :http/2
               :want-stream t
               :force-binary t
               :accept-encoding nil
               :decompress nil))
         (res (http-protocol:send (http2-stream-http-backend stream)
                                  (http2-stream-http-client stream)
                                  req)))
    (setf (http2-stream-send-queue stream) nil
          (http2-stream-http-started-p stream) t
          (http2-stream-response stream) res
          (http2-stream-body stream) (http-protocol:response-body res)
          (http2-stream-body-pos stream) 0)
    (%raise-if-bad-status res)
    stream))

(defun %read-one-frame (stream)
  (let ((body (http2-stream-body stream)))
    (cond
      ((null body)
       :eof)
      ((streamp body)
       (let ((hdr (make-array 5 :element-type '(unsigned-byte 8))))
         (let ((n (read-sequence hdr body)))
           (cond
             ((zerop n) :eof)
             ((< n 5)
              (error 'grpc-protocol:grpc-error
                     :status :internal
                     :message "short grpc frame header"))
             (t
              (when (plusp (logand (aref hdr 0) 1))
                (error 'grpc-protocol:grpc-error
                       :status :unimplemented
                       :message "compressed grpc frames are not supported"))
              (let* ((len (+ (ash (aref hdr 1) 24)
                             (ash (aref hdr 2) 16)
                             (ash (aref hdr 3) 8)
                             (aref hdr 4)))
                     (payload (make-array len :element-type '(unsigned-byte 8))))
                (unless (= len (read-sequence payload body))
                  (error 'grpc-protocol:grpc-error
                         :status :internal
                         :message "truncated grpc frame"))
                payload))))))
      (t
       (multiple-value-bind (payload next)
           (unframe-next body :start (http2-stream-body-pos stream))
         (cond
           ((null payload)
            (if (>= (http2-stream-body-pos stream) (length body))
                :eof
                (error 'grpc-protocol:grpc-error
                       :status :internal
                       :message "truncated grpc frame")))
           (t
            (setf (http2-stream-body-pos stream) next)
            payload)))))))

(defmethod grpc-protocol:grpc-send ((stream http2-grpc-stream) message &key)
  (when (or (grpc-protocol:grpc-stream-closed-p stream)
            (http2-stream-http-started-p stream))
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message (if (http2-stream-http-started-p stream)
                        "send after HTTP POST flush (interleaved bidi needs request streaming)"
                        "stream is closed")))
  (push (%message-octets message) (http2-stream-send-queue stream))
  message)

(defmethod grpc-protocol:grpc-recv ((stream http2-grpc-stream) &key timeout)
  (declare (ignore timeout))
  (when (grpc-protocol:grpc-stream-closed-p stream)
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "stream is closed"))
  (unless (http2-stream-http-started-p stream)
    (%flush-http2-stream stream))
  (if (http2-stream-eof-p stream)
      :eof
      (let ((frame (%read-one-frame stream)))
        (if (eq frame :eof)
            (progn
              (setf (http2-stream-eof-p stream) t)
              (let ((res (http2-stream-response stream)))
                (when res
                  (%raise-if-bad-status res :need-grpc-status t)))
              :eof)
            (let ((class (getf (http2-stream-metadata stream) :response-class)))
              (if class
                  (%decode frame class)
                  frame))))))

(defmethod grpc-protocol:grpc-close ((stream http2-grpc-stream) &key)
  (unless (http2-stream-http-started-p stream)
    (%flush-http2-stream stream))
  (setf (http2-stream-half-closed-p stream) t
        (grpc-protocol:grpc-stream-closed-p stream) t)
  (let ((body (http2-stream-body stream)))
    (when (streamp body)
      (ignore-errors (close body))))
  stream)

(eval-when (:load-toplevel :execute)
  (use-http2-grpc-backend))
