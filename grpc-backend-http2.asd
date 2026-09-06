(defsystem "grpc-backend-http2"
  :version "0.4.0"
  :description "HTTP/2 (http-protocol) backend for grpc-protocol — client + TLS accept loop"
  :author "egao1980"
  :license "MIT"
  :depends-on ((:version "grpc-protocol" "0.2.0")
               (:version "http-protocol" "0.3.6")
               "http-encoding-chipz"
               "http-server-protocol"
               "http-server-backend-http2"
               "bordeaux-threads" "uiop")
  :properties (:cl-repo (:ci (:with ("dissect" "http-server-backend-http2" "http2"))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "framing")
               (:file "compression")
               (:file "backend")
               (:file "server"))
  :in-order-to ((test-op (test-op "grpc-backend-http2/tests"))))

(defsystem "grpc-backend-http2/tests"
  :depends-on ("grpc-backend-http2" "rove" "bordeaux-threads")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test")
               (:file "parity-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
