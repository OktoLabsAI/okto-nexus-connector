"""IPC framing: newline-delimited, strictly-bounded JSON envelopes.

Every connection starts with an authenticated ``hello`` before any effect.
Requests carry a monotonically increasing ``seq``; responses reference it.
Streaming operations push intermediate records and finish with a terminal
response carrying ``"done": true``. All frames pass through redaction
before they touch the wire.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from ..errors import ConnectorError

MAX_FRAME_BYTES = 1024 * 1024
SUPPORTED_VERSION = 1


@dataclass(frozen=True, slots=True)
class Request:
    op: str
    seq: int
    params: dict[str, object]


def encode_message(payload: dict[str, object]) -> bytes:
    blob = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    data = blob.encode("utf-8")
    if len(data) + 1 > MAX_FRAME_BYTES:
        raise ConnectorError("CAPACITY_EXCEEDED", "ipc_encode")
    return data + b"\n"


def decode_message(line: bytes) -> dict[str, object]:
    if not line.endswith(b"\n"):
        raise ConnectorError("VALIDATION_ERROR", "ipc_decode",
                             "unterminated frame")
    if len(line) > MAX_FRAME_BYTES:
        raise ConnectorError("CAPACITY_EXCEEDED", "ipc_decode")
    try:
        payload = json.loads(line[:-1].decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise ConnectorError("VALIDATION_ERROR", "ipc_decode",
                             f"bad frame: {exc}") from exc
    if not isinstance(payload, dict):
        raise ConnectorError("VALIDATION_ERROR", "ipc_decode",
                             "frame must be an object")
    return payload


def parse_request(payload: dict[str, object]) -> Request:
    op = payload.get("op")
    seq = payload.get("seq")
    params = payload.get("params", {})
    if not isinstance(op, str) or not op or any(c in op for c in "\r\n\x00"):
        raise ConnectorError("VALIDATION_ERROR", "ipc_request")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        raise ConnectorError("VALIDATION_ERROR", "ipc_request")
    if not isinstance(params, dict):
        raise ConnectorError("VALIDATION_ERROR", "ipc_request")
    return Request(op, seq, params)


def response_ok(seq: int, result: dict[str, object], *,
                done: bool = True) -> dict[str, object]:
    return {"ok": True, "seq": seq, "result": result, "done": done}


def response_stream(seq: int, record: dict[str, object]) -> dict[str, object]:
    return {"ok": True, "seq": seq, "record": record, "done": False}


def response_error(seq: int, error) -> dict[str, object]:
    from ..errors import ConnectorError
    if isinstance(error, ConnectorError):
        payload = error.to_json()
    else:
        payload = {"code": "UNKNOWN", "stage": "ipc", "message": type(
            error).__name__}
    return {"ok": False, "seq": seq, "error": payload, "done": True}
