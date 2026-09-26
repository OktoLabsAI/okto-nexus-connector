"""Unit: IPC framing and envelope validation."""

from __future__ import annotations

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.ipc.protocol import (
    decode_message, encode_message, parse_request, response_error,
    response_ok,
)


def test_roundtrip():
    frame = encode_message({"op": "ping", "seq": 1, "params": {}})
    assert frame.endswith(b"\n")
    assert decode_message(frame) == {"op": "ping", "seq": 1, "params": {}}


def test_unterminated_rejected():
    with pytest.raises(ConnectorError):
        decode_message(b'{"op":"x"}')


def test_non_object_rejected():
    with pytest.raises(ConnectorError):
        decode_message(encode_message_raw(b'[1,2]\n'))


def encode_message_raw(blob: bytes) -> bytes:
    return blob


def test_bad_request_shapes():
    with pytest.raises(ConnectorError):
        parse_request({"op": "", "seq": 1})
    with pytest.raises(ConnectorError):
        parse_request({"op": "x", "seq": -1})
    with pytest.raises(ConnectorError):
        parse_request({"op": "x", "seq": 1, "params": []})
    request = parse_request({"op": "status", "seq": 7})
    assert request.op == "status" and request.seq == 7


def test_error_envelope_shape():
    from okto_nexus_connector.errors import ConnectorError as CE
    payload = response_error(3, CE("AGENT_AUTH_REQUIRED", "ipc_hello",
                                   "nope"))
    assert payload["ok"] is False and payload["seq"] == 3
    assert payload["error"]["code"] == "AGENT_AUTH_REQUIRED"
    ok = response_ok(3, {"x": 1})
    assert ok["done"] is True
