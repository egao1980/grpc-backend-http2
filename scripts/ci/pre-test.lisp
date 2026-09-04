;;;; Load live-parity extras before asdf:test-system (no recursive OPERATE).

(let ((v (string-downcase (or (uiop:getenv "GRPC_PARITY_PEERS") ""))))
  (unless (member v '("" "0" "false" "no") :test #'string=)
    (format t "~&; ci: pre-test GRPC_PARITY_PEERS — http-backend-async + libuv~%")
    (asdf:load-system "http-backend-async")
    (asdf:load-system "event-backend-libuv")
    (asdf:load-system "cl-stack-ssl")
    (asdf:load-system "http2/client")
    (let ((ensure (find-symbol "ENSURE-TLS" :http-backend-async)))
      (when (and ensure (fboundp ensure))
        (funcall ensure)))))
