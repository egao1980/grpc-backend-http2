(in-package #:grpc-backend-http2/tests)

(defclass mock-http-backend (http-protocol:http-backend)
  ((last-request :initarg :last-request :initform nil :accessor mock-last-request)
   (response-status :initarg :response-status :initform 200 :accessor mock-response-status)
   (response-headers :initarg :response-headers
                     :initform (make-hash-table :test 'equal)
                     :accessor mock-response-headers)
   (response-body :initarg :response-body
                  :initform (make-array 0 :element-type '(unsigned-byte 8))
                  :accessor mock-response-body)
   (response-trailers :initarg :response-trailers :initform nil
                      :accessor mock-response-trailers))
  (:default-initargs :name "mock-grpc"))

(defmethod http-protocol:backend-http-versions ((backend mock-http-backend))
  (declare (ignore backend))
  '(:http/1.1 :http/2))

(defun %mock-response (backend)
  (let ((res (make-instance 'http-protocol:http-response
                            :status (mock-response-status backend)
                            :headers (mock-response-headers backend)
                            :body (mock-response-body backend)
                            :http-version :http/2)))
    (when (mock-response-trailers backend)
      (let ((writer (find-symbol (string '#:response-trailers) :http-protocol)))
        (when (and writer (fboundp writer))
          (funcall (fdefinition `(setf ,writer))
                   (mock-response-trailers backend) res))))
    res))

(defmethod http-protocol:send ((backend mock-http-backend) client request &key)
  (declare (ignore client))
  (setf (mock-last-request backend) request)
  (%mock-response backend))

(defmethod http-protocol:send-async ((backend mock-http-backend) client request
                                     &key callback error-callback)
  (declare (ignore client error-callback))
  (setf (mock-last-request backend) request)
  (when callback
    (funcall callback (%mock-response backend)))
  nil)

(defun %slurp-request-pipe (req)
  (let ((content (http-protocol:http-request-content req))
        (close (find-symbol (string '#:close-body-pipe) :http-protocol))
        (pipe-p (find-symbol (string '#:http-body-pipe-p) :http-protocol)))
    (cond
      ((and pipe-p (funcall pipe-p content))
       (funcall close content)
       (let ((buf (make-array 4096 :element-type '(unsigned-byte 8))))
         (subseq buf 0 (read-sequence buf content))))
      (t content))))

(defun %ok-headers (&optional (status "0"))
  (let ((ht (make-hash-table :test 'equal)))
    (setf (gethash "grpc-status" ht) status
          (gethash "content-type" ht) "application/grpc")
    ht))

(deftest backend-class
  (ok (typep (grpc-backend-http2:make-http2-grpc-backend)
             'grpc-backend-http2:http2-grpc-backend)))

(deftest auto-selects-backend
  (ok (typep grpc-protocol:*grpc-backend*
             'grpc-backend-http2:http2-grpc-backend)))

(deftest connect-tls-and-close
  (let ((ch (grpc-protocol:grpc-connect "example.test:443" :credentials :ssl)))
    (ok (typep ch 'grpc-backend-http2:http2-grpc-channel))
    (ok (equal "example.test:443" (grpc-protocol:grpc-channel-target ch)))
    (ok (eq :ssl (grpc-protocol:grpc-channel-credentials ch)))
    (grpc-protocol:grpc-close ch)
    (ok (grpc-protocol:grpc-channel-closed-p ch))
    (ok (signals (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #())
                 'grpc-protocol:grpc-error))))

(deftest insecure-unimplemented
  (ok (signals (grpc-protocol:grpc-connect "localhost:1" :credentials :insecure)
               'grpc-protocol:grpc-error)))

(deftest frame-unframe-roundtrip
  (let* ((payload #(1 2 3 4 5))
         (framed (grpc-backend-http2:frame-message payload)))
    (ok (= 10 (length framed)))
    (ok (zerop (aref framed 0)))
    (ok (= 5 (aref framed 4)))
    (multiple-value-bind (out compressed)
        (grpc-backend-http2:unframe-message framed)
      (ok (equalp payload out))
      (ok (not compressed)))))

(deftest unframe-empty
  (multiple-value-bind (out compressed)
      (grpc-backend-http2:unframe-message #())
    (ok (zerop (length out)))
    (ok (not compressed))))

(deftest unframe-short-errors
  (ok (signals (grpc-backend-http2:unframe-message #(0 0 0))
               'grpc-protocol:grpc-error)))

(deftest unframe-compressed-flag
  (let ((framed (grpc-backend-http2:frame-message #(1) t)))
    (multiple-value-bind (out compressed)
        (grpc-backend-http2:unframe-message framed)
      (ok (equalp #(1) out))
      (ok compressed))))

(deftest gzip-deflate-roundtrip
  (dolist (algo '(:gzip :deflate))
    (dolist (payload '(#() #(1 2 3 4 5) #(0 0 0 0)))
      (let* ((raw (coerce payload '(vector (unsigned-byte 8))))
             (c (grpc-backend-http2:compress-payload algo raw))
             (back (grpc-backend-http2:decompress-payload algo c)))
        (ok (equalp raw back)
            (format nil "~A ~S" algo payload))))))

(defun %hdr (req name)
  (cdr (assoc name (http-protocol:http-request-headers req) :test #'string-equal)))

(defun %gzip-headers ()
  (let ((ht (%ok-headers)))
    (setf (gethash "grpc-encoding" ht) "gzip")
    ht))

(deftest grpc-status-keyword-maps
  (ok (eq :ok (grpc-backend-http2:grpc-status-keyword 0)))
  (ok (eq :ok (grpc-backend-http2:grpc-status-keyword "0")))
  (ok (eq :unimplemented (grpc-backend-http2:grpc-status-keyword 12)))
  (ok (eq :unavailable (grpc-backend-http2:grpc-status-keyword "unavailable")))
  (ok (eq :unknown (grpc-backend-http2:grpc-status-keyword 99))))

(deftest format-grpc-timeout-units
  (ok (equal "5S" (grpc-backend-http2:format-grpc-timeout 5)))
  (ok (equal "1500m" (grpc-backend-http2:format-grpc-timeout 1.5)))
  (ok (null (grpc-backend-http2:format-grpc-timeout nil))))

(deftest unary-call-via-mock-http
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:frame-message #(9 8 7))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http))))
    (let ((out (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #(1 2 3)
                                        :timeout 2)))
      (ok (equalp #(9 8 7) out)))
    (let ((req (mock-last-request http)))
      (ok (eq :post (http-protocol:http-request-method req)))
      (ok (equal "https://example.test:443/pkg.Svc/Ping"
                 (http-protocol:http-request-url req)))
      (ok (eq :http/2 (http-protocol:http-request-http-version req)))
      (ok (equal "application/grpc"
                 (cdr (assoc "content-type" (http-protocol:http-request-headers req)
                             :test #'string-equal))))
      (ok (equal "trailers"
                 (cdr (assoc "te" (http-protocol:http-request-headers req)
                             :test #'string-equal))))
      (ok (equal "2S" (%hdr req "grpc-timeout")))
      (ok (equal "identity,gzip,deflate" (%hdr req "grpc-accept-encoding")))
      (ok (null (%hdr req "grpc-encoding")))
      (ok (equalp (grpc-backend-http2:frame-message #(1 2 3))
                  (http-protocol:http-request-content req))))))

(deftest unary-gzip-request-and-response
  (let* ((plain #(9 8 7))
         (gz (grpc-backend-http2:compress-payload :gzip plain))
         (http (make-instance 'mock-http-backend
                              :response-headers (%gzip-headers)
                              :response-body (grpc-backend-http2:frame-message gz t)))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http :compression :gzip))))
    (ok (equalp plain (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #(1 2 3))))
    (let ((req (mock-last-request http)))
      (ok (equal "gzip" (%hdr req "grpc-encoding")))
      (ok (equal "identity,gzip,deflate" (%hdr req "grpc-accept-encoding")))
      (ok (null (%hdr req "compression")))
      (ok (equalp (grpc-backend-http2:frame-message
                   (grpc-backend-http2:compress-payload :gzip #(1 2 3)) t)
                  (http-protocol:http-request-content req))))))

(deftest unary-compressed-without-encoding-errors
  (let* ((gz (grpc-backend-http2:compress-payload :gzip #(1)))
         (http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:frame-message gz t)))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http))))
    (ok (signals (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #())
                 'grpc-protocol:grpc-error))))

(deftest unary-unknown-encoding-errors
  (let* ((headers (%ok-headers))
         (gz (grpc-backend-http2:compress-payload :gzip #(1))))
    (setf (gethash "grpc-encoding" headers) "lz4")
    (let* ((http (make-instance 'mock-http-backend
                                :response-headers headers
                                :response-body (grpc-backend-http2:frame-message gz t)))
           (ch (grpc-protocol:grpc-connect
                "example.test:443" :credentials :ssl
                :metadata (list :http-backend http))))
      (ok (signals (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #())
                   'grpc-protocol:grpc-error)))))

(deftest unary-unsupported-request-compression
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:frame-message #(1))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http :compression :lz4))))
    (ok (signals (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #(1))
                 'grpc-protocol:grpc-error))))

(deftest stream-gzip-via-mock
  (let* ((a (grpc-backend-http2:compress-payload :gzip #(10)))
         (b (grpc-backend-http2:compress-payload :gzip #(11)))
         (http (make-instance 'mock-http-backend
                              :response-headers (%gzip-headers)
                              :response-body (concatenate
                                              '(vector (unsigned-byte 8))
                                              (grpc-backend-http2:frame-message a t)
                                              (grpc-backend-http2:frame-message b t))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http :compression :gzip)))
         (s (grpc-protocol:grpc-stream ch "/pkg.Svc/Watch")))
    (grpc-protocol:grpc-send s #(9))
    (ok (equalp #(10) (grpc-protocol:grpc-recv s)))
    (ok (equalp #(11) (grpc-protocol:grpc-recv s)))
    (ok (eq :eof (grpc-protocol:grpc-recv s)))
    (ok (equal "gzip" (%hdr (mock-last-request http) "grpc-encoding")))
    (ok (equalp (grpc-backend-http2:frame-message
                 (grpc-backend-http2:compress-payload :gzip #(9)) t)
                (%slurp-request-pipe (mock-last-request http))))))

(deftest unary-maps-grpc-status
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers "14")
                              :response-body #()))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http))))
    (ok (signals (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #())
                 'grpc-protocol:grpc-error))))

(deftest unary-status-from-trailers
  (let* ((tr (%ok-headers "0"))
         (http (make-instance 'mock-http-backend
                              :response-headers (make-hash-table :test 'equal)
                              :response-trailers tr
                              :response-body (grpc-backend-http2:frame-message #(1))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http))))
    (if (find-symbol (string '#:response-trailers) :http-protocol)
        (ok (equalp #(1) (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" #())))
        (skip "http-protocol has no response-trailers"))))

(deftest unframe-next-two-frames
  (let ((body (grpc-backend-http2:concat-frames '(#(1 2) #(3)))))
    (multiple-value-bind (a i) (grpc-backend-http2:unframe-next body)
      (ok (equalp #(1 2) a))
      (multiple-value-bind (b j) (grpc-backend-http2:unframe-next body :start i)
        (ok (equalp #(3) b))
        (ok (null (grpc-backend-http2:unframe-next body :start j)))))))

(deftest server-stream-via-mock
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:concat-frames
                                              '(#(10) #(11) #(12)))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http)))
         (s (grpc-protocol:grpc-stream ch "/pkg.Svc/Watch")))
    (ok (typep s 'grpc-backend-http2:http2-grpc-stream))
    (grpc-protocol:grpc-send s #(9))
    (ok (equalp #(10) (grpc-protocol:grpc-recv s)))
    (ok (equalp #(11) (grpc-protocol:grpc-recv s)))
    (ok (equalp #(12) (grpc-protocol:grpc-recv s)))
    (ok (eq :eof (grpc-protocol:grpc-recv s)))
    (ok (http-protocol:http-request-want-stream (mock-last-request http)))
    (ok (equalp (grpc-backend-http2:frame-message #(9))
                (%slurp-request-pipe (mock-last-request http))))))

(deftest bidi-send-then-recv
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:frame-message #(7))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http)))
         (s (grpc-protocol:grpc-stream ch "/pkg.Svc/Chat")))
    (grpc-protocol:grpc-send s #(1))
    (grpc-protocol:grpc-send s #(2))
    (ok (equalp #(7) (grpc-protocol:grpc-recv s)))
    (ok (eq :eof (grpc-protocol:grpc-recv s)))
    (ok (equalp (grpc-backend-http2:concat-frames '(#(1) #(2)))
                (%slurp-request-pipe (mock-last-request http))))))

(deftest bidi-send-after-recv
  "Interleaved send after first recv (http-body-pipe, not queued flush)."
  (let* ((http (make-instance 'mock-http-backend
                              :response-headers (%ok-headers)
                              :response-body (grpc-backend-http2:concat-frames
                                              '(#(7) #(8)))))
         (ch (grpc-protocol:grpc-connect
              "example.test:443" :credentials :ssl
              :metadata (list :http-backend http)))
         (s (grpc-protocol:grpc-stream ch "/pkg.Svc/Chat")))
    (grpc-protocol:grpc-send s #(1))
    (ok (equalp #(7) (grpc-protocol:grpc-recv s)))
    (ok (equalp #(2) (grpc-protocol:grpc-send s #(2))))
    (ok (equalp #(8) (grpc-protocol:grpc-recv s)))
    (grpc-protocol:grpc-send s nil :end t)
    (ok (eq :eof (grpc-protocol:grpc-recv s)))
    (ok (equalp (grpc-backend-http2:concat-frames '(#(1) #(2)))
                (%slurp-request-pipe (mock-last-request http))))))
