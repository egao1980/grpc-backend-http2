(defsystem "grpc-backend-http2"
  :version "0.1.0"
  :description "HTTP/2 (http-protocol) backend for grpc-protocol — Windows-safe unary"
  :author "egao1980"
  :license "MIT"
  :depends-on ("grpc-protocol" "http-protocol" "uiop")
  :properties (:cl-repo (:ci (:with ("dissect") :sources (("dissect" :ql)))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "framing")
               (:file "backend"))
  :in-order-to ((test-op (test-op "grpc-backend-http2/tests"))))

(defsystem "grpc-backend-http2/tests"
  :depends-on ("grpc-backend-http2" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
