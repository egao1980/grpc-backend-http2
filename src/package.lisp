(defpackage #:grpc-backend-http2
  (:use #:cl)
  (:import-from #:bordeaux-threads
                #:make-lock
                #:make-thread
                #:thread-alive-p
                #:join-thread
                #:with-lock-held)
  (:export #:http2-grpc-backend
           #:http2-grpc-channel
           #:http2-grpc-stream
           #:make-http2-grpc-backend
           #:use-http2-grpc-backend
           #:frame-message
           #:unframe-message
           #:unframe-next
           #:concat-frames
           #:compress-payload
           #:decompress-payload
           #:grpc-status-keyword
           #:format-grpc-timeout))

(in-package #:grpc-backend-http2)
