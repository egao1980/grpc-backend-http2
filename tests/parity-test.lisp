(in-package #:grpc-backend-http2/tests)

;;; Live Lisp → grpcio (parity/python/server.py). Gate: GRPC_PARITY_PEERS=1.
;;; Needs python + grpcio + grpcio-tools + the committed localhost TLS pair.

(defun %env-on-p (name)
  (let ((v (uiop:getenv name)))
    (and v (not (member (string-downcase v)
                        '("" "0" "false" "no")
                        :test #'string=)))))

(defun %python ()
  (or (uiop:getenv "GRPC_PARITY_PYTHON")
      (loop for cmd in '("python3" "python")
            when (zerop (nth-value 2 (uiop:run-program
                                      (list cmd "-c" "import grpc, grpc_tools")
                                      :ignore-error-status t
                                      :output nil
                                      :error-output nil)))
              return cmd)))

(defun %pb-varint (n)
  (let ((out (make-array 0 :element-type '(unsigned-byte 8)
                         :adjustable t :fill-pointer 0)))
    (loop
      (let ((b (logand n #x7f)))
        (setf n (ash n -7))
        (vector-push-extend (if (plusp n) (logior b #x80) b) out)
        (when (zerop n)
          (return out))))))

(defun %read-varint (octets start)
  (let ((n 0)
        (shift 0)
        (pos start))
    (loop
      (when (>= pos (length octets))
        (error "short protobuf varint"))
      (let ((b (aref octets pos)))
        (incf pos)
        (setf n (logior n (ash (logand b #x7f) shift)))
        (when (zerop (logand b #x80))
          (return (values n pos)))
        (incf shift 7)))))

(defun %pb-encode-bytes (octets)
  (let ((payload (coerce octets '(vector (unsigned-byte 8)))))
    (concatenate '(vector (unsigned-byte 8))
                 #(#x0a)
                 (%pb-varint (length payload))
                 payload)))

(defun %pb-decode-bytes (octets)
  (when (zerop (length octets))
    (return-from %pb-decode-bytes
      (make-array 0 :element-type '(unsigned-byte 8))))
  (unless (= #x0a (aref octets 0))
    (error "expected echo.Msg field 1"))
  (multiple-value-bind (len pos)
      (%read-varint octets 1)
    (subseq octets pos (+ pos len))))

(defun %read-listen-line (proc)
  (let ((out (uiop:process-info-output proc))
        (deadline (+ (get-internal-real-time)
                     (* 15 internal-time-units-per-second))))
    (loop
      (when (> (get-internal-real-time) deadline)
        (return nil))
      (when (listen out)
        (let ((line (read-line out nil nil)))
          (when (and line (search "LISTEN" line))
            (return line))))
      (unless (uiop:process-alive-p proc)
        (return nil))
      (sleep 0.05))))

(defun %bind-async-maker ()
  (unless (and (find-package :http-backend-async)
               (find-package :event-backend-libuv))
    (return-from %bind-async-maker nil))
  (let* ((maker (find-symbol "MAKE-LIBUV-BACKEND" :event-backend-libuv))
         (ensure (find-symbol "ENSURE-TLS" :http-backend-async))
         (eb (funcall maker)))
    (when (and ensure (fboundp ensure))
      (funcall ensure))
    (let ((ensure-h2 (find-symbol "ENSURE-HTTP2" :http-backend-async)))
      (when (and ensure-h2 (fboundp ensure-h2))
        (funcall ensure-h2)))
    (setf (symbol-value (find-symbol "*EVENT-BACKEND-MAKER*" :http-backend-async))
          (lambda () eb))
    (values (funcall (find-symbol "MAKE-ASYNC-BACKEND" :http-backend-async))
            eb)))

(defun %call-timeout (seconds thunk)
  (let ((done nil)
        (val nil)
        (err nil))
    (let ((th (bordeaux-threads:make-thread
               (lambda ()
                 (handler-case (setf val (funcall thunk) done t)
                   (error (e) (setf err e done t))))
               :name "grpc-parity-timeout")))
      (loop repeat (max 1 (round (* seconds 20)))
            until done
            do (sleep 0.05))
      (unless done
        (ignore-errors (bordeaux-threads:destroy-thread th))
        (error "grpcio parity timed out after ~A s" seconds))
      (when err (error err))
      val)))

(deftest grpcio-parity-live
  "Lisp unary / server-stream / interleaved bidi vs grpcio. GRPC_PARITY_PEERS=1."
  (cond
    ((not (%env-on-p "GRPC_PARITY_PEERS"))
     (skip "GRPC_PARITY_PEERS unset"))
    ((null (%python))
     (skip "python + grpcio + grpcio-tools missing"))
    ((not (and (find-package :http-backend-async)
               (find-package :event-backend-libuv)))
     (skip "http-backend-async/event-backend-libuv not loaded (scripts/ci/pre-test.lisp)"))
    ((let ((eh (find-symbol "ENSURE-HTTP2" :http-backend-async)))
       (not (and eh (fboundp eh) (funcall eh))))
     (skip "http2/client not loadable"))
    (t
     (let* ((root (asdf:system-source-directory "grpc-backend-http2"))
            (script (merge-pathnames "parity/python/server.py" root))
            (cert (merge-pathnames "parity/tls/cert.pem" root))
            (key (merge-pathnames "parity/tls/key.pem" root))
            (port (+ 18000 (random 1000)))
            (proc nil))
       (cond
         ((not (and (probe-file script) (probe-file cert) (probe-file key)))
          (skip "parity/python/server.py or TLS pair missing"))
         (t
          (unwind-protect
               (progn
                 (setf proc (uiop:launch-program
                             (list (%python)
                                   (uiop:native-namestring script)
                                   (princ-to-string port)
                                   (uiop:native-namestring cert)
                                   (uiop:native-namestring key))
                             :output :stream
                             :error-output :stream))
                 (cond
                   ((null (%read-listen-line proc))
                    (skip "grpcio server failed to start"))
                   (t
                    (let* ((http (%bind-async-maker))
                           (client (http-protocol:make-http-client
                                    http :http-version :http/2 :verify nil))
                           (target (format nil "localhost:~D" port))
                           (ch (grpc-protocol:grpc-connect
                                target :credentials :ssl
                                :metadata (list :http-backend http
                                                :http-client client))))
                      (unwind-protect
                           (progn
                             (testing "unary Ping"
                               (handler-case
                                   (let ((out (%call-timeout
                                               15
                                               (lambda ()
                                                 (grpc-protocol:grpc-call
                                                  ch "/echo.Echo/Ping"
                                                  (%pb-encode-bytes #(9 8 7))
                                                  :timeout 5)))))
                                     (ok (equalp #(9 8 7) (%pb-decode-bytes out))))
                                 (error (e)
                                   (ok nil (format nil "unary Ping: ~A" e)))))
                             (testing "server-stream Watch"
                               (handler-case
                                   (let ((msgs (%call-timeout
                                                20
                                                (lambda ()
                                                  (let ((s (grpc-protocol:grpc-stream
                                                            ch "/echo.Echo/Watch")))
                                                    (unwind-protect
                                                         (progn
                                                           (grpc-protocol:grpc-send
                                                            s (%pb-encode-bytes #(9)) :end t)
                                                           (list (grpc-protocol:grpc-recv s)
                                                                 (grpc-protocol:grpc-recv s)
                                                                 (grpc-protocol:grpc-recv s)
                                                                 (grpc-protocol:grpc-recv s)))
                                                      (grpc-protocol:grpc-close s)))))))
                                     (ok (equalp #(9 0) (%pb-decode-bytes (first msgs))))
                                     (ok (equalp #(9 1) (%pb-decode-bytes (second msgs))))
                                     (ok (equalp #(9 2) (%pb-decode-bytes (third msgs))))
                                     (ok (eq :eof (fourth msgs))))
                                 (error (e)
                                   (ok nil (format nil "Watch: ~A" e)))))
                             (testing "interleaved bidi Chat"
                               (handler-case
                                   (let ((msgs (%call-timeout
                                                20
                                                (lambda ()
                                                  (let ((s (grpc-protocol:grpc-stream
                                                            ch "/echo.Echo/Chat")))
                                                    (unwind-protect
                                                         (progn
                                                           (grpc-protocol:grpc-send
                                                            s (%pb-encode-bytes #(65)))
                                                           (let ((a (grpc-protocol:grpc-recv s)))
                                                             (grpc-protocol:grpc-send
                                                              s (%pb-encode-bytes #(66)))
                                                             (let ((b (grpc-protocol:grpc-recv s)))
                                                               (grpc-protocol:grpc-send s nil :end t)
                                                               (list a b (grpc-protocol:grpc-recv s)))))
                                                      (grpc-protocol:grpc-close s)))))))
                                     (ok (equalp #(65) (%pb-decode-bytes (first msgs))))
                                     (ok (equalp #(66) (%pb-decode-bytes (second msgs))))
                                     (ok (eq :eof (third msgs))))
                                 (error (e)
                                   (ok nil (format nil "Chat: ~A" e))))))
                        (grpc-protocol:grpc-close ch))))))
            (when proc
              (ignore-errors (uiop:terminate-process proc))
              (ignore-errors (uiop:wait-process proc))))))))))
