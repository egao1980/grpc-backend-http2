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
   (request-pipe :initform nil :accessor http2-stream-request-pipe)
   (http-started-p :initform nil :accessor http2-stream-http-started-p)
   (body :initform nil :accessor http2-stream-body)
   (body-pos :initform 0 :accessor http2-stream-body-pos)
   (response :initform nil :accessor http2-stream-response)
   (eof-p :initform nil :accessor http2-stream-eof-p)
   (half-closed-p :initform nil :accessor http2-stream-half-closed-p)
   (event-backend :initform nil :accessor http2-stream-event-backend)
   (event-loop :initform nil :accessor http2-stream-event-loop)
   (loop-thread :initform nil :accessor http2-stream-loop-thread)
   (owns-loop-p :initform nil :accessor http2-stream-owns-loop-p)))

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

(defun %pipe-sym (name)
  (let ((s (find-symbol (string name) :http-protocol)))
    (unless (and s (fboundp s))
      (error 'grpc-protocol:grpc-error
             :status :unimplemented
             :message "http-protocol http-body-pipe required (need ≥ 0.3.6)"))
    s))

(defun %ensure-pipe (stream)
  (or (http2-stream-request-pipe stream)
      (setf (http2-stream-request-pipe stream)
            (funcall (%pipe-sym '#:make-http-body-pipe)))))

(defun %write-pipe-frame (stream octets)
  (funcall (%pipe-sym '#:write-body-pipe)
           (%ensure-pipe stream)
           (frame-message octets)))

(defun %half-close (stream)
  (unless (http2-stream-half-closed-p stream)
    (setf (http2-stream-half-closed-p stream) t)
    (let ((pipe (http2-stream-request-pipe stream)))
      (when pipe
        (funcall (%pipe-sym '#:close-body-pipe) pipe))))
  stream)

(defun %async-http-p (http)
  (let ((pkg (find-package '#:http-backend-async)))
    (when pkg
      (let ((class (find-symbol "ASYNC-BACKEND" pkg)))
        (and class (find-class class nil) (typep http (find-class class)))))))

(defun %event-sym (name)
  (let ((pkg (find-package '#:event-protocol)))
    (and pkg (find-symbol (string name) pkg))))

(defun %ensure-loop (stream)
  "Bind/create an event loop for SEND-ASYNC. Real async HTTP needs a pump thread."
  (unless (%async-http-p (http2-stream-http-backend stream))
    (return-from %ensure-loop nil))
  (when (http2-stream-event-loop stream)
    (return-from %ensure-loop stream))
  (let* ((eb-sym (%event-sym '#:*event-backend*))
         (el-sym (%event-sym '#:*event-loop*))
         (eb (and eb-sym (symbol-value eb-sym)))
         (el (and el-sym (symbol-value el-sym)))
         (had (and eb el)))
    (unless had
      (let* ((maker-sym (let ((pkg (find-package '#:http-backend-async)))
                          (and pkg (find-symbol "*EVENT-BACKEND-MAKER*" pkg))))
             (maker (and maker-sym (symbol-value maker-sym)))
             (make-loop (%event-sym '#:make-event-loop)))
        (unless (and maker make-loop)
          (error 'grpc-protocol:grpc-error
                 :status :internal
                 :message "No event loop: bind event-protocol:*event-backend*/*event-loop* or http-backend-async:*event-backend-maker*"))
        (setf eb (funcall maker)
              el (funcall make-loop eb))))
    (setf (http2-stream-event-backend stream) eb
          (http2-stream-event-loop stream) el
          (http2-stream-owns-loop-p stream) (not had))
    (when (http2-stream-owns-loop-p stream)
      (let ((run (%event-sym '#:run)))
        (setf (http2-stream-loop-thread stream)
              (make-thread
               (lambda ()
                 (progv (list eb-sym el-sym) (list eb el)
                   (funcall run eb el :stop-when-idle nil)))
               :name "grpc-http2-loop"))))
    stream))

(defun %stop-loop (stream)
  (when (http2-stream-owns-loop-p stream)
    (let ((stop (%event-sym '#:stop))
          (eb (http2-stream-event-backend stream))
          (el (http2-stream-event-loop stream)))
      (when (and stop eb el)
        (ignore-errors (funcall stop eb el))))
    (let ((th (http2-stream-loop-thread stream)))
      (when (and th (thread-alive-p th))
        (ignore-errors (join-thread th))))
    (setf (http2-stream-loop-thread stream) nil
          (http2-stream-owns-loop-p stream) nil))
  stream)

(defun %with-stream-event-context (stream fn)
  (let ((eb (http2-stream-event-backend stream))
        (el (http2-stream-event-loop stream))
        (eb-sym (%event-sym '#:*event-backend*))
        (el-sym (%event-sym '#:*event-loop*)))
    (if (and eb el eb-sym el-sym)
        (progv (list eb-sym el-sym) (list eb el)
          (funcall fn))
        (funcall fn))))

(defun %start-http (stream)
  "Open POST with a body pipe via SEND-ASYNC (blocking SEND slurps :want-stream)."
  (when (http2-stream-http-started-p stream)
    (return-from %start-http stream))
  (let ((pipe (%ensure-pipe stream))
        (lock (make-lock "grpc-http2-headers"))
        (done nil)
        (err nil)
        (res nil)
        (deadline (+ (get-internal-real-time)
                     (* 30 internal-time-units-per-second))))
    (%ensure-loop stream)
    (%with-stream-event-context
     stream
     (lambda ()
       (let ((req (http-protocol:make-http-request
                   :method :post
                   :url (http2-stream-url stream)
                   :headers (%grpc-request-headers (http2-stream-metadata stream)
                                                   (http2-stream-timeout stream))
                   :content pipe
                   :http-version :http/2
                   :want-stream t
                   :force-binary t
                   :accept-encoding nil
                   :decompress nil)))
         (http-protocol:send-async
          (http2-stream-http-backend stream)
          (http2-stream-http-client stream)
          req
          :callback (lambda (r)
                      (with-lock-held (lock)
                        (setf res r done t)))
          :error-callback (lambda (e)
                            (with-lock-held (lock)
                              (setf err e done t)))))))
    (loop until (with-lock-held (lock) done)
          do (when (> (get-internal-real-time) deadline)
               (%stop-loop stream)
               (error 'grpc-protocol:grpc-error
                      :status :deadline-exceeded
                      :message "HTTP/2 gRPC headers timed out"))
             (sleep 0.01))
    (when err
      (%stop-loop stream)
      (error err))
    (setf (http2-stream-http-started-p stream) t
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

(defmethod grpc-protocol:grpc-send ((stream http2-grpc-stream) message &key end)
  (when (or (grpc-protocol:grpc-stream-closed-p stream)
            (http2-stream-half-closed-p stream))
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message (if (http2-stream-half-closed-p stream)
                        "send after client half-close"
                        "stream is closed")))
  (when message
    (%write-pipe-frame stream (%message-octets message)))
  (when end
    (%half-close stream))
  (unless (http2-stream-http-started-p stream)
    (%start-http stream))
  message)

(defmethod grpc-protocol:grpc-recv ((stream http2-grpc-stream) &key timeout)
  (declare (ignore timeout))
  (when (grpc-protocol:grpc-stream-closed-p stream)
    (error 'grpc-protocol:grpc-error
           :status :failed-precondition
           :message "stream is closed"))
  (unless (http2-stream-http-started-p stream)
    (%start-http stream))
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
  (ignore-errors (%half-close stream))
  (setf (grpc-protocol:grpc-stream-closed-p stream) t)
  (let ((body (http2-stream-body stream)))
    (when (streamp body)
      (ignore-errors (close body))))
  (%stop-loop stream)
  stream)

(eval-when (:load-toplevel :execute)
  (use-http2-grpc-backend))
