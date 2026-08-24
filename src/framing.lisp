(in-package #:grpc-backend-http2)

;;; Length-prefixed gRPC-over-HTTP/2 frame (gRPC HTTP/2 spec).
;;;   1-byte compressed-flag | 4-byte big-endian length | message octets
;;; Wave-1: uncompressed only (flag 0).

(defparameter *grpc-status-names*
  #(:ok :cancelled :unknown :invalid-argument :deadline-exceeded
    :not-found :already-exists :permission-denied :resource-exhausted
    :failed-precondition :aborted :out-of-range :unimplemented
    :internal :unavailable :data-loss :unauthenticated)
  "grpc-status wire integers 0..16 → keywords (no :grpc-status- prefix).")

(defun frame-message (octets &optional compressed)
  "Wrap OCTETS in a 5-byte gRPC prefix. COMPRESSED non-nil sets flag 1 (not decoded)."
  (let* ((payload (coerce octets '(vector (unsigned-byte 8))))
         (n (length payload))
         (out (make-array (+ 5 n) :element-type '(unsigned-byte 8))))
    (setf (aref out 0) (if compressed 1 0))
    (setf (aref out 1) (ldb (byte 8 24) n)
          (aref out 2) (ldb (byte 8 16) n)
          (aref out 3) (ldb (byte 8 8) n)
          (aref out 4) (ldb (byte 8 0) n))
    (replace out payload :start1 5)
    out))

(defun unframe-message (octets)
  "Return (values payload compressed-p) from a single gRPC frame.
   Empty/missing body → empty payload. Compressed frames signal :unimplemented."
  (let ((buf (cond
               ((null octets)
                (make-array 0 :element-type '(unsigned-byte 8)))
               ((and (vectorp octets) (not (stringp octets)))
                (coerce octets '(vector (unsigned-byte 8))))
               (t
                (error 'grpc-protocol:grpc-error
                       :status :internal
                       :message "gRPC response body must be octets")))))
    (when (zerop (length buf))
      (return-from unframe-message
        (values (make-array 0 :element-type '(unsigned-byte 8)) nil)))
    (when (< (length buf) 5)
      (error 'grpc-protocol:grpc-error
             :status :internal
             :message "short grpc frame"))
    (let* ((flag (aref buf 0))
           (n (+ (ash (aref buf 1) 24)
                 (ash (aref buf 2) 16)
                 (ash (aref buf 3) 8)
                 (aref buf 4)))
           (compressed (plusp (logand flag 1))))
      (when compressed
        (error 'grpc-protocol:grpc-error
               :status :unimplemented
               :message "compressed grpc frames are not supported"))
      (when (> (+ 5 n) (length buf))
        (error 'grpc-protocol:grpc-error
               :status :internal
               :message "truncated grpc frame"))
      (values (subseq buf 5 (+ 5 n)) nil))))

(defun grpc-status-keyword (value)
  "Coerce a grpc-status header/trailer (integer, digit string, or name) to a keyword."
  (cond
    ((null value) :unknown)
    ((keywordp value) value)
    ((integerp value)
     (if (<= 0 value 16)
         (aref *grpc-status-names* value)
         :unknown))
    ((stringp value)
     (let* ((s (string-trim '(#\Space #\Tab) value))
            (n (ignore-errors (parse-integer s :junk-allowed t))))
       (cond
         ((and n (every #'digit-char-p s))
          (grpc-status-keyword n))
         (t
          (let ((k (intern (string-upcase (substitute #\- #\_ s)) :keyword)))
            (if (find k *grpc-status-names* :test #'eq) k :unknown))))))
    (t :unknown)))

(defun format-grpc-timeout (timeout)
  "gRPC Timeout header: integer seconds → nS; ratio/float → milliseconds (n m)."
  (cond
    ((null timeout) nil)
    ((stringp timeout) timeout)
    ((integerp timeout) (format nil "~DS" timeout))
    ((realp timeout)
     (format nil "~Dm" (max 0 (round (* timeout 1000)))))
    (t nil)))
