# grpc-backend-http2

[`http-protocol`](https://github.com/egao1980/http-protocol) HTTP/2 backend for
[`grpc-protocol`](https://github.com/egao1980/grpc-protocol). Unary + server-stream
+ **interleaved bidi** over [`http-backend-async`](https://github.com/egao1980/http-backend-async).
Windows-safe — no C-core overlay.

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

0.4.0:

- **TLS accept loop** (`grpc-serve`): unary + server-stream + client-stream +
  interleaved bidi over [`http-server-backend-http2`](https://github.com/egao1980/http-server-backend-http2).
  Register `grpc-method-handler`s; accept-side streams speak `grpc-send` /
  `grpc-recv`. Requires `:ssl :cert` / `:key` (or metadata `:ssl-cert` /
  `:ssl-key`). **No h2c / `:insecure`.**

```lisp
(grpc-protocol:grpc-serve
 (list (grpc-protocol:make-grpc-method-handler
        "/echo.Echo/Ping" (lambda (req stream) (declare (ignore stream)) req)))
 :credentials (list :ssl :cert #p"cert.pem" :key #p"key.pem")
 :port 8443)
```

0.3.1:

- **Compressed frames:** `:compression :gzip` / `:deflate` on `grpc-connect` /
  `grpc-call` / `grpc-stream` (or metadata `:compression`). Sets `grpc-encoding`
  and frame flag 1. Always sends `grpc-accept-encoding: identity,gzip,deflate`
  and decompresses inbound flag-1 frames via
  `http-protocol:encode/decode-content-coding` (`http-encoding-chipz`).
  **Not** HTTP `Content-Encoding` (`:accept-encoding nil :decompress nil` still).
- Unary + **server-stream / bidi**. Request body is an `http-body-pipe`;
  `grpc-send` writes DATA after the POST is open (`send-async`, not blocking
  `send` — that slurps `:want-stream`).
- `grpc-send` `&key end` half-closes the client (H2 END_STREAM). Needed for
  unary-request server-stream (grpcio `Watch` waits for END_STREAM).
- **TLS only.** `:insecure` / h2c is `:unimplemented`. Use
  [`grpc-backend-native`](https://github.com/egao1980/grpc-backend-native)
  for cleartext C-core on linux/darwin.
- Frame: flag + u32be length + proto octets. `content-type: application/grpc`,
  `TE: trailers`. Status from `grpc-status` (headers or trailers).
- Needs [`http-protocol` ≥ 0.3.6](https://github.com/egao1980/http-protocol)
  (`http-body-pipe`) and [`http-backend-async` ≥ 0.2.7](https://github.com/egao1980/http-backend-async).
- Live grpcio: `GRPC_PARITY_PEERS=1` + `parity/python/server.py` (unary Ping,
  Watch, interleaved Chat). CI job `grpcio` on ubuntu/macos/**windows**.
  Do not treat streams as portable until that job is green.

Do not load this together with `grpc-backend-native` unless you rebind
`grpc-protocol:*grpc-backend*` — last load wins.

Bidi needs `http-backend-async:*event-backend-maker*` (or a bound
`event-protocol` loop). The stream owns a pump thread when it creates the loop.

CI: canned [`cl-repository`](https://github.com/egao1980/cl-repository) unit
tests (mock HTTP). Live job installs grpcio via uv and sets `GRPC_PARITY_PEERS=1`.

## License

MIT
