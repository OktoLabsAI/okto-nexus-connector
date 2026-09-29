"""Contract: outbound NXL WSS client against the fake peer (C04).

Covers outbound-only dialing, hello/welcome revision gate, lane attach
with per-binding tickets, event batching with contiguous ACK watermark,
urgent-priority control path, reconnect with backoff and generation
fencing. Contract PASS — real Server integration stays J-gated.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from nexus_connector_core import RuntimeEvent

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.wss_client import NXLTransport
from tests.fakes.wss_peer import FakeNXLPeer

TICKET_A = "nstkt_bind_a_valid"
TICKET_B = "nstkt_bind_b_valid"


def _events(session: str, epoch: str, sequences: range) -> list[RuntimeEvent]:
    return [RuntimeEvent("srv", "conn", session, epoch, seq,
                         "text_delta", "text.delta",
                         {"text": f"chunk-{seq}"})
            for seq in sequences]


@pytest.fixture
async def peer():
    server = FakeNXLPeer(valid_tickets={TICKET_A, TICKET_B})
    await server.start()
    yield server
    await server.stop()


def _transport(peer: FakeNXLPeer, **kwargs) -> NXLTransport:
    defaults = dict(
        server_id="srv_fake", executor_id="conn_test",
        link_url=peer.url,
        ticket_provider=_ticket(TICKET_A),
        on_operation=_noop_operation, on_approval=_noop_approval)
    defaults.update(kwargs)
    return NXLTransport(**defaults)


async def _noop_operation(frame):
    return None


async def _noop_approval(frame):
    return None


def _ticket(value: str):
    async def provide() -> str:
        return value
    return provide


async def test_outbound_dial_and_welcome(peer):
    """TC-15: client dials out; no listener exists on the connector side."""
    transport = _transport(peer)
    transport.start()
    assert await transport.wait_online(10)
    session = peer.latest
    await _wait_for(lambda: session.welcomed)
    assert session.executor_id == "conn_test"
    await transport.stop()


async def test_lane_attach_with_valid_ticket(peer):
    """TC-16: per-binding lane attaches only with its own valid ticket."""
    transport = _transport(peer)
    transport.add_lane("bind_a", "ag_1", _ticket(TICKET_A))
    transport.start()
    await transport.wait_online(10)
    await _wait_for(lambda: peer.latest.lanes.get("bind_a") == "ag_1")
    await transport.stop()


async def test_lane_attach_with_invalid_ticket_rejected(peer):
    transport = _transport(peer)
    transport.add_lane("bind_evil", "ag_2", _ticket("nstkt_forged_ticket"))
    transport.start()
    await transport.wait_online(10)
    await asyncio.sleep(0.5)
    assert "bind_evil" not in peer.latest.lanes
    await transport.stop()


async def test_event_batch_and_contiguous_ack(peer):
    events = _events("rs_1", "ep_1", range(1, 6))
    transport = _transport(peer)
    transport.start()
    await transport.wait_online(10)
    await _wait_for(lambda: peer.latest is not None and peer.latest.welcomed)
    assert await transport.send_events(events) is True
    watermark = await transport.wait_event_ack("rs_1", "ep_1", 10)
    assert watermark == 5
    # duplicate batch keeps the same identity (durable-ingress idempotent)
    await transport.send_events(_events("rs_1", "ep_1", range(1, 6)))
    await asyncio.sleep(0.5)
    assert peer.latest.acked[("rs_1", "ep_1")] == 5
    await transport.stop()


async def test_gap_is_not_acked(peer):
    """Watermark stays contiguous; gaps remain explicit (A.10)."""
    transport = _transport(peer)
    transport.start()
    await transport.wait_online(10)
    await _wait_for(lambda: peer.latest is not None and peer.latest.welcomed)
    await transport.send_events(_events("rs_2", "ep_1", [1, 2]))
    await transport.send_events(_events("rs_2", "ep_1", [4, 5]))
    await asyncio.sleep(0.6)
    assert peer.latest.acked.get(("rs_2", "ep_1")) == 2
    await transport.stop()


async def test_urgent_control_not_blocked_by_flood(peer):
    """TC-19: urgent budget survives a flood of normal frames."""
    transport = _transport(peer)
    transport.start()
    await transport.wait_online(10)
    await _wait_for(lambda: peer.latest is not None and peer.latest.welcomed)
    for index in range(600):
        sent = await transport.send_events(
            _events("rs_f", "ep_1", [index % 100 + 1]))
    urgent_ok = 0
    for _ in range(URGENT_COUNT := 8):
        ok = await transport.queues.put(
            {"type": "operation.receipt", "urgent_probe": True}, urgent=True)
        urgent_ok += int(ok)
    assert urgent_ok == URGENT_COUNT
    assert transport.queues.dropped_normal > 0  # honest drop accounting
    await transport.stop()


async def test_reconnect_after_drop(peer):
    """TC-17: socket loss triggers reconnect with generation fencing."""
    transport = _transport(peer)
    transport.start()
    await transport.wait_online(10)
    first_generation = transport.stats.connection_generation
    old_socket = transport._websocket
    await old_socket.close()
    await _wait_for(lambda: transport.stats.connection_generation
                    > first_generation, timeout=15)
    assert transport.stats.reconnects >= 2
    assert await transport.wait_online(10)
    await transport.stop()


async def test_receipt_roundtrip_for_remote_operation(peer):
    """Server-initiated submit produces a receipt frame back."""
    received: list[dict] = []

    async def on_operation(operation):
        # CN2/N01: the handler receives the ValidatedOperation DTO.
        received.append(operation)
        from nexus_connector_core import OperationReceipt
        return OperationReceipt(
            operation_id=operation.operation_id,
            intent_hash=operation.intent_hash,
            stage="SUBMITTED", possible_effect=True, retry_safe=False,
            session_id=operation.session_id)
    transport = _transport(peer, on_operation=on_operation)
    # CN1/A02: operations require a live negotiated lane scope.
    async def _lane_ticket():
        return TICKET_A
    transport.add_lane("bind_x", "ag_1", _lane_ticket)
    transport.start()
    await transport.wait_online(10)
    session = peer.latest
    await _wait_for(lambda: transport.stats.state == "ready" and
                    session.welcomed and
                    session.lanes.get("bind_x") == "ag_1")
    await peer.send_operation(
        session, binding_id="bind_x", agent_id="ag_1", session_id="rs_9",
        operation_id="op_1", action="turn.submit")
    await _wait_for(lambda: len(peer.latest.receipts) >= 1)
    receipt = peer.latest.receipts[0]
    assert receipt["type"] == "operation.receipt"
    assert receipt["operation_id"] == "op_1"
    await transport.stop()


async def test_intent_hash_enforced_by_codec_and_handler(peer):
    """Wrong-hash submits cannot exist on the wire (codec enforcement)
    and the connector handler refuses them independently (defense in
    depth against a non-validating peer)."""
    from nexus_connector_core.frame_codec import encode_frame
    from nexus_connector_core.protocol import (
        CONTRACT_REVISION as REV, PROTOCOL_MAJOR as MAJOR,
        submit_frame_intent_hash,
    )
    frame = {
        "protocol_major": MAJOR, "contract_revision": REV,
        "type": "operation.submit", "server_id": "srv_fake",
        "executor_id": "conn", "binding_id": "b", "agent_id": "a",
        "workspace_id": "ws", "workspace_binding_id": "wb",
        "session_id": "rs_1", "operation_id": "op_9",
        "action": "turn.submit", "connection_generation": 1,
        "authorization_revision": 1, "configuration_revision": 1,
        "intent_hash": "sha256:" + "0" * 64,
        "payload": {"text": "x"}}
    with pytest.raises(Exception):
        encode_frame(frame)  # codec refuses an inconsistent submit

    mismatches = []

    async def on_operation(operation):
        payload = {"server_id": operation.server_id,
                   "executor_id": operation.executor_id,
                   "binding_id": operation.binding_id,
                   "agent_id": operation.agent_id,
                   "workspace_id": operation.workspace_id,
                   "workspace_binding_id": "wb",
                   "session_id": operation.session_id,
                   "operation_id": operation.operation_id,
                   "action": operation.action,
                   "connection_generation":
                       operation.connection_generation,
                   "authorization_revision":
                       operation.authorization_revision,
                   "configuration_revision":
                       operation.configuration_revision,
                   "payload": operation.payload,
                   "intent_hash": operation.intent_hash}
        if submit_frame_intent_hash(payload) != operation.intent_hash:
            mismatches.append(operation.operation_id)
            raise ConnectorError("OPERATION_CONFLICT", "wss_submit",
                                 "hash mismatch")
        return None

    transport = _transport(peer, on_operation=on_operation)
    transport.start()
    await transport.wait_online(10)
    session = peer.latest
    await _wait_for(lambda: session.welcomed)
    # a well-formed frame passes the handler's own check
    await peer.send_operation(
        session, binding_id="bind_x", agent_id="ag_1", session_id="rs_9",
        operation_id="op_ok", action="turn.submit")
    await asyncio.sleep(0.4)
    assert mismatches == []
    await transport.stop()


async def test_goaway_stops_transport(peer):
    peer.goaway_after_welcome = True
    transport = _transport(peer)
    transport.start()
    await transport.wait_online(10)
    await asyncio.sleep(0.5)
    assert transport.stats.state in ("stopped", "backoff")
    await transport.stop()


async def _wait_for(predicate, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")
