"""Authenticated local IPC server (plan C02.2/C02.4).

Transport: an AF_UNIX stream socket with per-user permissions where
supported (non-Windows platforms with AF_UNIX, including macOS), otherwise a loopback-only
TCP socket. The loopback variant is bound to 127.0.0.1, uses an ephemeral
port published only in the per-user readiness file and requires a 256-bit
connection token stored with user-only permissions — qualified as
"private, authenticated" per the plan, never exposed to external
interfaces. Every connection must present a valid ``hello`` before any
operation is dispatched; unknown peers are refused before effects.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket as socket_module
import sys
from pathlib import Path

from ..errors import ConnectorError
from ..redaction import redact_mapping
from .protocol import (MAX_FRAME_BYTES, decode_message, encode_message,
                       parse_request, response_error, response_ok)

logger = logging.getLogger(__name__)

_HAS_UNIX = (sys.platform != "win32"
             and hasattr(socket_module, "AF_UNIX"))
_IDLE_TIMEOUT = 300.0
_AUTH_TIMEOUT = 15.0
_MAX_CONNECTIONS = 64


class IPCServer:
    """Runs one transport and dispatches authenticated requests."""

    def __init__(self, dispatcher, *, redaction_secrets=()):
        self._dispatcher = dispatcher
        self._secrets = list(redaction_secrets)
        self._server: asyncio.AbstractServer | None = None
        self.transport = ""
        self.address = ""
        self._connections: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task] = set()
        self._token = ""
        self._closing = False

    async def start(self, run_dir: Path, token: str) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        self._token = token
        if _HAS_UNIX:
            path = run_dir / "ipc.sock"
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            self._server = await asyncio.start_unix_server(
                self._spawn_client, path=str(path))
            os.chmod(path, 0o600)
            self.transport = "unix"
            self.address = str(path)
            return
        self._server = await asyncio.start_server(
            self._spawn_client, host="127.0.0.1", port=0, backlog=16)
        socket = self._server.sockets[0] if self._server.sockets else None
        if socket is None:
            raise ConnectorError("DAEMON_UNAVAILABLE", "ipc_bind",
                                 "loopback bind failed")
        self.transport = "loopback"
        self.address = f"127.0.0.1:{socket.getsockname()[1]}"

    def _spawn_client(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        if self._closing:
            writer.close()
            return
        token = self._token
        task = asyncio.create_task(
            self._client(reader, writer, token))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def stop(self) -> None:
        self._closing = True
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for writer in list(self._connections):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        self._connections.clear()

    @property
    def active_connections(self) -> int:
        return len(self._connections)

    async def _client(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter, token: str) -> None:
        if len(self._connections) >= _MAX_CONNECTIONS:
            writer.close()
            return
        self._connections.add(writer)
        peer = writer.get_extra_info("peername")
        try:
            # Authentication happens before any dispatch (TC-08).
            try:
                first = await asyncio.wait_for(
                    reader.readline(), _AUTH_TIMEOUT)
            except asyncio.TimeoutError:
                return
            hello = _safe_decode(first)
            if (not isinstance(hello, dict)
                    or hello.get("op") != "hello"
                    or hello.get("token") != token
                    or int(hello.get("version", 0)) != 1):
                await _send(writer, {
                    "ok": False,
                    "error": {"code": "AGENT_AUTH_REQUIRED",
                              "stage": "ipc_hello",
                              "message": "authentication required"}})
                return
            await _send(writer, {"ok": True, "result": {"ready": True}})
            while not self._closing:
                try:
                    line = await asyncio.wait_for(
                        reader.readline(), _IDLE_TIMEOUT)
                except asyncio.TimeoutError:
                    break
                if not line:
                    break
                if len(line) > MAX_FRAME_BYTES:
                    break
                payload = _safe_decode(line)
                if payload is None:
                    await _send(writer, response_error(0, ConnectorError(
                        "VALIDATION_ERROR", "ipc_frame", "bad frame")))
                    continue
                try:
                    request = parse_request(payload)
                except ConnectorError as error:
                    await _send(writer, response_error(0, error))
                    continue
                async for message in self._dispatch(request):
                    await _send(writer, message)
        finally:
            self._connections.discard(writer)
            writer.close()

    async def _dispatch(self, request):
        try:
            async for message in self._dispatcher(request):
                yield message
        except ConnectorError as error:
            yield response_error(request.seq, error)
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            logger.exception("internal error handling op %r", request.op)
            yield response_error(request.seq, ConnectorError(
                "UNKNOWN", request.op,
                "internal error; see daemon log"))


def _safe_decode(line: bytes) -> dict[str, object] | None:
    try:
        return decode_message(line)
    except ConnectorError:
        return None


async def _send(writer: asyncio.StreamWriter, payload: dict[str, object]) -> None:
    safe = redact_mapping(payload)
    try:
        writer.write(encode_message(safe))
        await writer.drain()
    except (ConnectionError, OSError):
        pass


def redact_response(payload: dict[str, object],
                    secrets=()) -> dict[str, object]:
    return redact_mapping(payload, secrets)  # type: ignore[return-value]
