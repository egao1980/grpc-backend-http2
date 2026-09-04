#!/usr/bin/env python3
"""Tiny grpcio echo: unary + server-stream + bidi. TLS if cert/key given."""

from __future__ import annotations

import sys
from concurrent import futures

import grpc

_PROTO = """
syntax = "proto3";
package echo;
service Echo {
  rpc Ping (Msg) returns (Msg);
  rpc Watch (Msg) returns (stream Msg);
  rpc Chat (stream Msg) returns (stream Msg);
}
message Msg { bytes body = 1; }
"""


def _load():
    from grpc_tools import protoc
    import importlib.util
    import os
    import tempfile

    work = tempfile.mkdtemp()
    proto = os.path.join(work, "echo.proto")
    with open(proto, "w", encoding="utf-8") as fh:
        fh.write(_PROTO)
    protoc.main(["", f"-I{work}", f"--python_out={work}", f"--grpc_python_out={work}", proto])
    sys.path.insert(0, work)
    spec = importlib.util.spec_from_file_location("echo_pb2", os.path.join(work, "echo_pb2.py"))
    pb2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pb2)
    spec2 = importlib.util.spec_from_file_location(
        "echo_pb2_grpc", os.path.join(work, "echo_pb2_grpc.py")
    )
    grpc_mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(grpc_mod)
    return pb2, grpc_mod


def main() -> None:
    port = int(sys.argv[1])
    cert = sys.argv[2] if len(sys.argv) > 2 else ""
    key = sys.argv[3] if len(sys.argv) > 3 else ""
    pb2, grpc_mod = _load()

    class Servicer(grpc_mod.EchoServicer):
        def Ping(self, request, context):
            return pb2.Msg(body=request.body)

        def Watch(self, request, context):
            for i in range(3):
                yield pb2.Msg(body=request.body + bytes([i]))

        def Chat(self, request_iterator, context):
            for req in request_iterator:
                yield pb2.Msg(body=req.body)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    grpc_mod.add_EchoServicer_to_server(Servicer(), server)
    if cert and key:
        with open(cert, "rb") as cf, open(key, "rb") as kf:
            creds = grpc.ssl_server_credentials(((kf.read(), cf.read()),))
        server.add_secure_port(f"127.0.0.1:{port}", creds)
    else:
        server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    print(f"LISTEN {port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
