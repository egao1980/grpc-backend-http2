# grpc-backend-http2

[`http-protocol`](https://github.com/egao1980/http-protocol) HTTP/2 backend for
[`grpc-protocol`](https://github.com/egao1980/grpc-protocol). Unary gRPC over
existing H2 (`http-backend-async`). Windows-safe — no C-core overlay.

Part of [cl-stack](https://github.com/egao1980/cl-stack) agent-wire
([brief](https://github.com/egao1980/cl-stack/blob/main/docs/capabilities/grpc.md)).

```lisp
(asdf:load-system "grpc-backend-http2")
;; soft-loads http-backend-async on the first call if *http-backend* is unbound

(let ((ch (grpc-protocol:grpc-connect "example.test:443" :credentials :ssl)))
  (unwind-protect
       (grpc-protocol:grpc-call ch "/pkg.Svc/Ping" octets)
    (grpc-protocol:grpc-close ch)))
```

Wave-1:

- Unary only. Streams signal `:unimplemented`.
- **TLS only.** `:insecure` / h2c is `:unimplemented` (`http-protocol` rejects
  `:http/2` on `http://`). Use [`grpc-backend-native`](https://github.com/egao1980/grpc-backend-native)
  for cleartext C-core on linux/darwin.
- Frame: `0x00` + u32be length + proto octets. `content-type: application/grpc`,
  `TE: trailers`. Status from `grpc-status` (headers or trailers folded by the
  HTTP backend).
- Needs [`http-protocol` ≥ 0.3.1](https://github.com/egao1980/http-protocol) so
  `TE: trailers` is not stripped on H2.

Do not load this together with `grpc-backend-native` unless you rebind
`grpc-protocol:*grpc-backend*` — last load wins.

CI: canned [`cl-repository`](https://github.com/egao1980/cl-repository) (`test-system.yml` / `setup-client` + `ci`). Deps from `ghcr.io/egao1980/cl-systems`.
(OCI only). Unit tests use a mock HTTP backend — no live server.

## License

MIT
