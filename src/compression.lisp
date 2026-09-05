(in-package #:grpc-backend-http2)

;;; Message codecs for grpc-encoding (NOT HTTP Content-Encoding).
;;; Same chipz/salza2 pairing as http-encoding-chipz.

;; salza2 mis-encodes zero-length input; use canonical empties.
(defparameter *empty-gzip*
  (coerce #(#x1f #x8b #x08 #x00 #x00 #x00 #x00 #x00 #x00 #xff
            #x03 #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x00)
          '(simple-array (unsigned-byte 8) (*))))

(defparameter *empty-zlib*
  (coerce #(#x78 #x9c #x03 #x00 #x00 #x00 #x00 #x01)
          '(simple-array (unsigned-byte 8) (*))))

(defun %as-octets (octets)
  (coerce octets '(simple-array (unsigned-byte 8) (*))))

(defun normalize-compression (value)
  "NIL / :identity / :none → NIL; :gzip / :deflate stay; else signal."
  (cond
    ((null value) nil)
    ((member value '(:identity :none) :test #'eq) nil)
    ((member value '(:gzip :deflate) :test #'eq) value)
    ((stringp value)
     (normalize-compression (intern (string-upcase value) :keyword)))
    (t
     (error 'grpc-protocol:grpc-error
            :status :unimplemented
            :message (format nil "unsupported grpc compression ~S" value)))))

(defun parse-grpc-encoding (value)
  "Header value → :gzip / :deflate / :identity / NIL. Unknown → string."
  (when value
    (let* ((raw (if (consp value) (car value) value))
           (s (string-downcase (string-trim '(#\Space #\Tab) (princ-to-string raw)))))
      (cond
        ((string= s "") nil)
        ((string= s "identity") :identity)
        ((string= s "gzip") :gzip)
        ((string= s "deflate") :deflate)
        (t s)))))

(defun compress-payload (algorithm octets)
  (let ((algo (normalize-compression algorithm))
        (buf (%as-octets octets)))
    (ecase algo
      ((nil) buf)
      (:gzip (if (zerop (length buf))
                 *empty-gzip*
                 (salza2:compress-data buf 'salza2:gzip-compressor)))
      (:deflate (if (zerop (length buf))
                    *empty-zlib*
                    (salza2:compress-data buf 'salza2:zlib-compressor))))))

(defun decompress-payload (algorithm octets)
  (let ((buf (%as-octets octets)))
    (etypecase algorithm
      (null buf)
      (keyword
       (ecase algorithm
         (:identity buf)
         (:gzip (chipz:decompress nil 'chipz:gzip buf))
         (:deflate
          (handler-case
              (chipz:decompress nil 'chipz:zlib buf)
            (error ()
              (chipz:decompress nil 'chipz:deflate buf))))))
      (string
       (error 'grpc-protocol:grpc-error
              :status :unimplemented
              :message (format nil "unsupported grpc-encoding ~A" algorithm))))))
