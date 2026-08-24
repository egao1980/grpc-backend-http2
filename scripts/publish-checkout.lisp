;;;; Publish this checkout to ghcr.io/egao1980/cl-systems via auto-package-spec.
;;;;
;;;; No Quicklisp. Packager + cl-oci-client come from OCI
;;;; (egao1980/cl-repository :latest, then first-party HTTP stack from cl-systems).
;;;; Env: GITHUB_ACTOR, GITHUB_TOKEN, PKG_SYSTEM, optional PKG_VERSION /
;;;; PACKAGER_VERSION / OCI_NAMESPACE.

(setf *debugger-hook*
      (lambda (c h)
        (declare (ignore h))
        (format *error-output* "~&UNHANDLED: ~A~%" c)
        (uiop:quit 1)))

#+sbcl (sb-ext:disable-debugger)

(setf asdf:*compile-file-failure-behaviour* :warn)

(defun %call-with-publish-muffles (fn)
  #+sbcl
  (handler-bind ((sb-ext:defconstant-uneql #'continue))
    (funcall fn))
  #-sbcl
  (funcall fn))

(%call-with-publish-muffles (lambda () (asdf:load-system "cl-repository-client")))

(cl-repo:add-registry "https://ghcr.io" :namespace "egao1980/cl-repository" :priority :prepend)
(cl-repo:add-registry "https://ghcr.io" :namespace "egao1980/cl-systems" :priority :append)

(let ((packager-ver (uiop:getenv "PACKAGER_VERSION")))
  (%call-with-publish-muffles
   (lambda ()
     (if (and packager-ver (plusp (length packager-ver)))
         (cl-repo:ensure-systems "cl-repository-packager"
                                 :version packager-ver
                                 :default-source :oci)
         (cl-repo:ensure-systems "cl-repository-packager" :default-source :oci))
     (cl-repo:ensure-systems "cl-oci-client" :default-source :oci))))

(cl-repository-client/asdf-integration:configure-asdf-source-registry)
(cl-repository-client/asdf-integration:load-system-init-files)

(%call-with-publish-muffles
 (lambda ()
   (when (asdf:find-system "cl-repository-packager" nil)
     (cl-repo:ensure-system-dependencies "cl-repository-packager"
                                         :also-tests nil
                                         :default-source :oci))
   (when (asdf:find-system "cl-oci-client" nil)
     (cl-repo:ensure-system-dependencies "cl-oci-client"
                                         :also-tests nil
                                         :default-source :oci))
   (asdf:load-system "cl-repository-packager")
   (asdf:load-system "cl-oci-client")))

(defun env (name &optional default)
  (or (uiop:getenv name) default))

(defun hide-bootstrap-from-source-layer (source-dir)
  "Move .cl-repository out of SOURCE-DIR after the packager is loaded.
   setup-client writes that tree into the checkout; packing it (packager
   0.16.0 does not exclude it) yields a ~10MB layer that fails extract
   with parse-integer junk in string \"orm, manifes\"."
  (let* ((root (uiop:ensure-directory-pathname source-dir))
         (bootstrap (merge-pathnames ".cl-repository/" root)))
    (when (uiop:directory-exists-p bootstrap)
      (let* ((stash-parent (uiop:ensure-directory-pathname
                            (or (uiop:getenv "RUNNER_TEMP")
                                (namestring (uiop:temporary-directory)))))
             (dest (merge-pathnames "cl-repository-bootstrap/" stash-parent)))
        (when (uiop:directory-exists-p dest)
          (uiop:delete-directory-tree dest :validate t :if-does-not-exist :ignore))
        (ensure-directories-exist stash-parent)
        (uiop:run-program (list "mv" (namestring bootstrap) (namestring dest))
                          :output t :error-output t)
        (format t "~&Hid .cl-repository from source layer -> ~a~%" dest)))))

(let* ((system-name (env "PKG_SYSTEM" "grpc-backend-http2"))
       (version (let ((v (env "PKG_VERSION")))
                  (if (and v (plusp (length v)))
                      v
                      (asdf:component-version (asdf:find-system system-name)))))
       (registry-url (env "OCI_REGISTRY" "ghcr.io"))
       (namespace (string-downcase (env "OCI_NAMESPACE" "egao1980/cl-systems")))
       (auth (cl-oci-client/auth:make-auth-config
              :username (env "GITHUB_ACTOR" "x-access-token")
              :password (or (env "GITHUB_TOKEN")
                            (error "GITHUB_TOKEN required"))))
       (reg (cl-oci-client/registry:make-registry
             (format nil "https://~a" registry-url) :auth auth))
       (spec (progn
               (hide-bootstrap-from-source-layer
                (asdf:system-source-directory system-name))
               (cl-repository-packager/asdf-plugin:auto-package-spec system-name)))
       (result nil))
  (setf (cl-repository-packager/build-matrix:package-spec-provides spec)
        (list system-name))
  (setf (cl-repository-packager/build-matrix:package-spec-version spec) version)
  (setf result (cl-repository-packager/build-matrix:build-package spec))
  (format t "~&Publishing ~a/~a:~a~%" namespace system-name version)
  (cl-repository-packager/publisher:publish-package
   reg namespace version result spec :skip-catalog t)
  (format t "~&Published ~a/~a:~a~%" namespace system-name version)
  (uiop:quit 0))
