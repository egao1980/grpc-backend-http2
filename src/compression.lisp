(in-package #:grpc-backend-http2)

;;; grpc-encoding codecs = http-protocol content-coding (http-encoding-chipz).
;;; Wire is still gRPC message compression, not HTTP Content-Encoding.

(defun normalize-compression (value)
  "NIL / :identity / :none → NIL; else a content-coding keyword (or signal)."
  (cond
    ((or (null value) (eq value :none)) nil)
    (t
     (let ((c (http-protocol:normalize-content-coding value)))
       (cond
         ((or (null c) (eq c :identity)) nil)
         ((http-protocol:content-coding-supported-p c) c)
         (t
          (error 'grpc-protocol:grpc-error
                 :status :unimplemented
                 :message (format nil "unsupported grpc compression ~S" value))))))))

(defun parse-grpc-encoding (value)
  "grpc-encoding header → content-coding keyword or NIL."
  (http-protocol:normalize-content-coding
   (if (consp value) (car value) value)))

(defun %as-octets (octets)
  (if (and (vectorp octets) (not (stringp octets)))
      (coerce octets '(simple-array (unsigned-byte 8) (*)))
      octets))

(defun compress-payload (algorithm octets)
  (let ((algo (normalize-compression algorithm))
        (buf (%as-octets octets)))
    (if algo
        (http-protocol:encode-content-coding algo buf)
        (http-protocol:encode-content-coding :identity buf))))

(defun decompress-payload (algorithm octets)
  (let ((c (parse-grpc-encoding algorithm))
        (buf (%as-octets octets)))
    (cond
      ((or (null c) (eq c :identity))
       (http-protocol:decode-content-coding :identity buf))
      ((http-protocol:content-coding-supported-p c)
       (handler-case
           (http-protocol:decode-content-coding c buf)
         (http-protocol:unsupported-content-coding ()
           (error 'grpc-protocol:grpc-error
                  :status :unimplemented
                  :message (format nil "unsupported grpc-encoding ~A" algorithm)))
         (error (e)
           (error 'grpc-protocol:grpc-error
                  :status :internal
                  :message (format nil "grpc decompress ~S failed: ~A" c e)
                  :details e))))
      (t
       (error 'grpc-protocol:grpc-error
              :status :unimplemented
              :message (format nil "unsupported grpc-encoding ~A" algorithm))))))
