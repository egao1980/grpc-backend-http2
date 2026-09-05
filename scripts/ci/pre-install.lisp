;;;; ci-base may already have http-protocol 0.3.2 on the ASDF path.
;;;; Body-pipe is 0.3.6 — force that tag from OCI before ensure-deps walks names.

(format t "~&; ci: pre-install http-protocol:0.3.6~%")
(funcall (find-symbol "ENSURE-SYSTEMS" :cl-repo)
         "http-protocol" :version "0.3.6" :force t)
