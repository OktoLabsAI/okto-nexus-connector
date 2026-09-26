"""Synchronous IPC client used by the CLI (plan C02.4).

Closing a CLI or log follower simply closes its own IPC connection; the
daemon and unrelated sessions keep running (TC-09). The client never holds
secrets beyond the per-user IPC token.
"""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Iterator

from ..errors import ConnectorError

_TIMEOUT = 30.0
_CONNECT_TIMEOUT = 5.0
_STREAM_POLL_SECONDS = 1.0


class IPCClient:
    def __init__(self, transport: str, address: str, token: str):
        self.transport = transport
        self.address = address
        self.token = token
        self._socket: socket.socket | None = None
        self._buffer = b""
        self._closed = False

    def __enter__(self) -> "IPCClient":
        self.connect()
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def connect(self) -> None:
        if self._socket is not None:
            return
        if self.transport == "unix":
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            target = self.address
        else:
            host, _, port = self.address.partition(":")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target = (host, int(port))
        sock.settimeout(_CONNECT_TIMEOUT)
        try:
            sock.connect(target)
        except OSError as exc:
            sock.close()
            raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_connect",
                                 f"cannot reach daemon: {exc}",
                                 retry_safe=True) from exc
        sock.settimeout(_STREAM_POLL_SECONDS)
        self._socket = sock
        self._send({"op": "hello", "token": self.token, "version": 1})
        hello = self._read_object()
        if not hello.get("ok"):
            raise ConnectorError("AGENT_AUTH_REQUIRED", "ipc_hello",
                                 "daemon rejected IPC authentication")

    def close(self) -> None:
        self._closed = True
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None

    def call(self, op: str, params: dict[str, object] | None = None
             ) -> dict[str, object]:
        """One request → one terminal response."""
        responses = list(self.stream(op, params))
        if not responses:
            raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_call",
                                 "daemon closed the connection",
                                 retry_safe=True)
        return responses[-1]

    def stream(self, op: str, params: dict[str, object] | None = None
               ) -> Iterator[dict[str, object]]:
        """Yield streamed records and the terminal response."""
        self.connect()
        assert self._socket is not None
        self._send({"op": op, "seq": _next_seq(), "params": params or {}})
        while True:
            payload = self._read_object()
            yield payload
            if payload.get("done"):
                return

    def _send(self, payload: dict[str, object]) -> None:
        assert self._socket is not None
        blob = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._socket.sendall(blob + b"\n")

    def _read_object(self) -> dict[str, object]:
        assert self._socket is not None
        while b"\n" not in self._buffer:
            if self._closed or self._socket is None:
                raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_read",
                                     "client closed",
                                     retry_safe=True)
            try:
                chunk = self._socket.recv(65536)
            except socket.timeout:
                continue  # poll the closed flag without blocking forever
            if not chunk:
                raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_read",
                                     "daemon closed the connection",
                                     retry_safe=True)
            self._buffer += chunk
            if len(self._buffer) > 1024 * 1024:
                raise ConnectorError("CAPACITY_EXCEEDED", "ipc_read")
        line, _, self._buffer = self._buffer.partition(b"\n")
        payload = json.loads(line.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ConnectorError("VALIDATION_ERROR", "ipc_read")
        return payload


_seq = 0


def _next_seq() -> int:
    global _seq
    _seq += 1
    return _seq


def connect_readiness(readiness, token: str) -> IPCClient:
    client = IPCClient(readiness.transport, readiness.address, token)
    client.connect()
    return client


def ping(readiness, token: str, *, timeout: float = 5.0) -> bool:
    try:
        with connect_readiness(readiness, token) as client:
            response = client.call("ping")
            return bool(response.get("ok"))
    except (ConnectorError, OSError):
        return False
