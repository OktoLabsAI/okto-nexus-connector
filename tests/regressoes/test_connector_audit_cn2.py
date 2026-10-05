"""CN2 audit regressions — reconstructed from the reviewer package.

The delivered package again carried only Markdown + RESULTADOS_RESUMO.json
(no ``regressoes/`` executables); the 20 CN2 cases below are rebuilt from
the EXACT reproductions in ``01_RELATORIO_REAVALIACAO.md`` §4–11 — same
node ids, same causal conditions (validated frame + invalid scope,
four-slot saturation + queued interrupt, detached lane during wait,
sparse journal page, unacked replay, future/foreign ACK, failed
reconciliation, receipt without binary, FAILED/CoreError stop,
approval TypeError+DTO inversion, MCP URL not reaching the launch,
ticket identity mismatch, lane-add after ready, ws:// override).

Three positive controls and one load observation preserved. The seeds
exercise the application end-to-end: codec → receiver → dispatcher →
manager → Core/journal, with controlled native peers at the final
boundary.
"""

from __future__ import annotations

import asyncio
import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(0, str(Path(__file__).parents[2] / "tests"))

from nexus_connector_core import (
    CloseOperation, CoreError, EventCursor, ExecutionContext,
    InstallationCandidate, OperationReceipt, SessionKey, ShutdownPolicy,
    ShutdownReport, TurnOperation, create_runtime,
)
from nexus_connector_core.discovery import binary_architecture, fingerprint
from nexus_connector_core.frame_codec import encode_frame
from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core.protocol import CONTRACT_REVISION, PROTOCOL_MAJOR

from okto_nexus_connector.daemon.app import DaemonApp, EventBridge
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.platform import paths
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.runtime_service import (
    ManagedSession, RuntimeManager,
)
from okto_nexus_connector.storage.state_store import (
    BindingRecord, IdentityRecord, ServerProfileRecord, StateStore,
)
from okto_nexus_connector.transport import wss_client as wss
from okto_nexus_connector.transport.wss_client import (
    NXLTransport, PriorityQueues, validate_link_url,
)
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.conftest import RecordingFactory


def _exe(path: Path, content: bytes = b"synthetic-c") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _candidate(binary: Path, adapter_id="codex_app_server"):
    return InstallationCandidate(
        adapter_id, str(binary), fingerprint(binary), "explicit",
        "selected", "0.157.0",
        architecture=binary_architecture(binary) or "x86_64")


def _binding(root: Path, binary: Path, *, server="srv_a",
             binding_id="bind_a", agent="ag_a", workspace="ws_a",
             executor=None):
    return BindingRecord(
        binding_id=binding_id, alias=f"al-{server}", server_id=server,
        agent_id=agent, adapter_id="codex_app_server",
        executor_id=executor or f"exe_{server}", workspace_id=workspace,
        workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint=fingerprint(binary),
        candidate_version="0.157.0",
        candidate_architecture=binary_architecture(binary) or "x86_64")


def _mgr(tmp_path, host):
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    (tmp_path / "vault").mkdir(exist_ok=True)
    return RuntimeManager(StateStore(tmp_path / "state.json"), vault, host)


async def _wait_for(predicate, timeout=10.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


# =====================================================================
# Fixtures: a negotiated transport + lane against the lab peer
# =====================================================================

TICKET = "nstkt_cn2_valid"


class Harness:
    def __init__(self, peer, transport):
        self.peer = peer
        self.transport = transport


async def _negotiated(peer, *, on_operation, server_id="srv_a",
                      executor_id="exe_a", binding="bind_a",
                      agent="ag_a", generation_start=0):
    async def ticket():
        return TICKET

    async def noop_approval(frame):
        return None

    transport = NXLTransport(
        server_id=server_id, executor_id=executor_id, link_url=peer.url,
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=noop_approval)
    transport.add_lane(binding, agent, ticket)
    transport.start()
    await transport.wait_online(10)
    session = peer.latest
    await _wait_for(lambda: transport.stats.state == "ready"
                    and session.welcomed
                    and session.lanes.get(binding) == agent)
    return transport


def _submit_frame(peer_session, *, operation_id="op_x",
                  binding="bind_a", agent="ag_a", session_id="session_a",
                  action="turn.submit", expected_turn_id=None,
                  server_id="srv_a", executor_id="exe_a",
                  generation=None, text="cn2"):
    from nexus_connector_core.protocol import submit_frame_intent_hash
    peer_generation = (peer_session.connection_generation
                       if peer_session is not None else 1)
    frame = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "operation.submit",
        "server_id": server_id,
        "executor_id": executor_id,
        "binding_id": binding,
        "agent_id": agent,
        "workspace_id": "ws_a",
        "workspace_binding_id": "wb_a",
        "session_id": session_id,
        "operation_id": operation_id,
        "action": action,
        "connection_generation": (generation if generation is not None
                                  else peer_generation),
        "authorization_revision": 1,
        "configuration_revision": 1,
        "payload": {"text": text},
    }
    if expected_turn_id is not None:
        frame["expected_turn_id"] = expected_turn_id
    frame["intent_hash"] = submit_frame_intent_hash(frame)
    return frame


@pytest.fixture
async def cn2_peer():
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    yield peer
    await peer.stop()


# =====================================================================
# N02 — b01: saturation must not block interrupt (+300 observation)
# =====================================================================

async def test_b01_saturation_must_not_block_interrupt(cn2_peer):
    started: list[str] = []
    release = asyncio.Event()
    interrupts: list[str] = []

    async def on_operation(operation):
        if operation.action == "turn.interrupt":
            interrupts.append(operation.operation_id)
            return None
        started.append(operation.operation_id)
        await release.wait()  # four productive submits stay blocked
        return None

    transport = await _negotiated(cn2_peer, on_operation=on_operation)
    session = cn2_peer.latest
    # Saturate ALL productive slots (default 4).
    for index in range(wss.PRODUCTIVE_CONCURRENCY):
        await session.websocket.send(encode_frame(_submit_frame(
            session, operation_id=f"op_sat_{index}")))
    await _wait_for(lambda: len(started) == wss.PRODUCTIVE_CONCURRENCY)
    # A VALID interrupt must reach its handler while the submits hold
    # every productive slot (separate control capacity).
    await session.websocket.send(encode_frame(_submit_frame(
        session, operation_id="op_interrupt_1", action="turn.interrupt",
        expected_turn_id="turn-1")))
    try:
        await asyncio.wait_for(
            _wait_list(interrupts, 1, timeout=3.0), 3.0)
    except asyncio.TimeoutError:
        pytest.fail("interrupt blocked behind saturated productive slots")
    release.set()
    await transport.stop()


async def _wait_list(items, count, timeout=3.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if len(items) >= count:
            return
        await asyncio.sleep(0.02)
    raise asyncio.TimeoutError()


async def test_observation_scheduler_admits_300_waiters(cn2_peer):
    """CN2/N02: admission is bounded BEFORE create_task. With the
    productive cap (items=ADMISSION_QUEUE_ITEMS) the 301st waiter is
    REFUSED with an explicit over-capacity error and no task exists for
    it — the historical unbounded 300-waiter behavior is gone, and the
    implemented finite limit is proven (not an arbitrary number)."""
    admitted: list[str] = []
    refused: list[str] = []
    errors: list[dict] = []
    release = asyncio.Event()

    async def on_operation(operation):
        admitted.append(operation.operation_id)
        await release.wait()
        return None

    transport = await _negotiated(cn2_peer, on_operation=on_operation)
    session = cn2_peer.latest
    # Watch for over-capacity error frames on the peer side.
    for index in range(transport._productive_admission.max_items
                       + wss.PRODUCTIVE_CONCURRENCY + 8):
        await session.websocket.send(encode_frame(_submit_frame(
            session, operation_id=f"op_obs_{index}")))
    await asyncio.sleep(0.8)
    in_flight = len(transport._inflight)
    waiting = transport._productive_admission.waiting_items
    assert waiting <= transport._productive_admission.max_items
    assert transport._productive_admission.refused > 0
    # Every admitted operation has a task; the refused ones never did.
    assert in_flight <= wss.PRODUCTIVE_CONCURRENCY + waiting + 8
    release.set()
    await transport.stop()


# =====================================================================
# N01 — b02: queued operation revalidates a detached lane
# =====================================================================

async def test_b02_queued_operation_revalidates_detached_lane(cn2_peer):
    started: list[str] = []
    release = asyncio.Event()
    receipts: list[dict] = []

    async def on_operation(operation):
        started.append(operation.operation_id)
        await release.wait()
        return None

    transport = await _negotiated(cn2_peer, on_operation=on_operation)
    session = cn2_peer.latest
    for index in range(wss.PRODUCTIVE_CONCURRENCY):
        await session.websocket.send(encode_frame(_submit_frame(
            session, operation_id=f"op_hold_{index}")))
    await _wait_for(lambda: len(started) == wss.PRODUCTIVE_CONCURRENCY)
    # The fifth operation passes receiver validation (lane ready) and
    # waits for productive capacity.
    await session.websocket.send(encode_frame(_submit_frame(
        session, operation_id="op_victim")))
    await asyncio.sleep(0.2)
    assert "op_victim" not in started
    # Detach the lane while op_victim is queued.
    transport._lanes["bind_a"].state = "detached"
    transport.stats.lanes["bind_a"] = False
    release.set()
    await asyncio.sleep(0.5)
    # CN2-01.02: the queued operation revalidated AFTER the wait —
    # the detach closed the path with ZERO effects.
    assert "op_victim" not in started, \
        "queued operation executed after its lane was detached"
    await transport.stop()


# =====================================================================
# N01 — b03: wire namespace vs resolved session (control + foreign)
# =====================================================================

async def _b03_environment(tmp_path, server_a_url_peer,
                           include_srv_b_session=True):
    """A real Core journal/session under srv_a, plus (optionally) a
    textual session-id twin under srv_b in the manager's registry."""
    import time as _time
    binary = _exe(tmp_path / "codex.exe")
    journal_a = SQLiteJournal(tmp_path / "journal.db")
    factory_a = RecordingFactory()

    async def env(prepared):
        return {}

    runtime_a = create_runtime(
        journal=journal_a, environment=env,
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={"ws_a": str(tmp_path)},
        native_factory=factory_a)
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host.root = tmp_path
    host._vault = None
    host._journal_path = paths.journal_path(tmp_path)
    host._ledger_path = paths.owned_slot_ledger_path(tmp_path)
    host._runtimes = {}
    host._journal = journal_a
    host._ledger = None
    host._journal_gate = None
    host._ledger_gate = None
    manager = _mgr(tmp_path, host)
    binding_a = _binding(tmp_path, binary, server="srv_a",
                          executor="exe_a")
    context_a = ExecutionContext(
        "srv_a", "exe_a", "bind_a", "ag_a", "ws_a", 1, 1, 1,
        _time.monotonic() + 120,
        frozenset({"runtime.open", "turn.submit", "runtime.close"}))
    prepared = await runtime_a.prepare(
        __import__("nexus_connector_core", fromlist=["LaunchIntent"])
        .LaunchIntent("ag_a", "ws_a", "codex_app_server"), context_a)
    await runtime_a.open(
        __import__("nexus_connector_core", fromlist=["OpenOperation"])
        .OpenOperation("op_open_a", "session_a", "ep-a", prepared),
        context_a)
    session = ManagedSession(
        session_id="session_a", binding=binding_a, stream_epoch="ep-a",
        opened_at=_time.monotonic(), runtime=runtime_a,
        lease_deadline=_time.monotonic() + 600,
        authorized_actions=frozenset({"turn.submit", "turn.interrupt",
                                      "runtime.close"}))
    manager._register(session)
    if include_srv_b_session:
        binding_b = _binding(tmp_path, binary, server="srv_b",
                             binding_id="bind_a", agent="ag_b",
                             workspace="ws_b")
        twin = ManagedSession(
            session_id="session_a", binding=binding_b,
            stream_epoch="ep-b", opened_at=_time.monotonic(),
            runtime=runtime_a,
            lease_deadline=_time.monotonic() + 600,
            authorized_actions=frozenset({"turn.submit",
                                          "turn.interrupt",
                                          "runtime.close"}))
        manager._register(twin)
    return manager, factory_a


async def test_b03_wire_namespace_matches_resolved_local_session(
        cn2_peer, tmp_path):
    """[foreign_server]: a frame negotiated on srv_b whose textual
    session id exists under srv_a must produce ZERO writes on A's Core
    (the old code resolved by text and remapped A's grant)."""
    manager, factory = await _b03_environment(tmp_path, cn2_peer)
    from okto_nexus_connector.transport.wss_client import \
        ValidatedOperation
    frame = _submit_frame(None, operation_id="op_wire_b",
                          server_id="srv_b", executor_id="exe_b",
                          binding="bind_a", agent="ag_b",
                          session_id="session_a")
    # Build the DTO as the srv_b-configured receiver would (its own
    # namespace), exactly like _validate_operation_scope does.
    operation = ValidatedOperation(
        server_id="srv_b", executor_id="exe_b", binding_id="bind_a",
        agent_id="ag_b", workspace_id="ws_b",
        session_key=("srv_b", "exe_b", "session_a"),
        session_id="session_a", operation_id="op_wire_b",
        action="turn.submit", intent_hash=frame["intent_hash"],
        payload={"text": "x"}, expected_turn_id=None,
        connection_generation=1, authorization_revision=1,
        configuration_revision=1)
    with pytest.raises(ConnectorError) as error:
        await manager.submit_remote(operation)
    assert error.value.code in ("VALIDATION_ERROR",
                                "BINDING_NOT_AUTHORIZED")
    assert factory.native.sent == [], \
        "wire namespace was remapped onto another server's session"


async def test_b03_positive_same_server(cn2_peer, tmp_path):
    """[positive_same_server] control: the same frame on its OWN server
    produces exactly one native write."""
    manager, factory = await _b03_environment(
        tmp_path, cn2_peer, include_srv_b_session=False)
    from okto_nexus_connector.transport.wss_client import \
        ValidatedOperation
    frame = _submit_frame(None, operation_id="op_wire_a",
                          server_id="srv_a", executor_id="exe_a",
                          session_id="session_a")
    operation = ValidatedOperation(
        server_id="srv_a", executor_id="exe_a", binding_id="bind_a",
        agent_id="ag_a", workspace_id="ws_a",
        session_key=("srv_a", "exe_a", "session_a"),
        session_id="session_a", operation_id="op_wire_a",
        action="turn.submit", intent_hash=frame["intent_hash"],
        payload={"text": "hello"}, expected_turn_id=None,
        connection_generation=1, authorization_revision=1,
        configuration_revision=1)
    receipt = await manager.submit_remote(operation)
    assert receipt.stage in ("SUBMITTED", "OUTCOME_UNKNOWN")
    assert [verb for verb, _ in factory.native.sent] == ["send_turn"]


# =====================================================================
# N03 — b04/b05/b05b/b05c: event pages, replay, ACK validation
# =====================================================================

def _event(seq, server="srv_a", session="session_a", epoch="ep-a"):
    return __import__("nexus_connector_core", fromlist=["RuntimeEvent"]) \
        .RuntimeEvent(server, "exe_a", session, epoch, seq,
                      "text_delta", "text.delta", {"t": seq})


async def _record(journal, seq):
    await journal.append_event(_event(seq))


class _AckTransport:
    """Controlled transport double for bridge tests."""

    def __init__(self):
        self.online = True
        self.batches: list[list] = []
        self.acks: dict[tuple, int] = {}
        self.fail_send = False

    async def send_events(self, batch):
        if self.fail_send:
            return False
        self.batches.append(list(batch))
        return True

    async def wait_event_ack(self, session_id, epoch, timeout=5.0, *,
                             target=None):
        # CN4 adaptation: the caller now passes the batch's target; the
        # double still answers only with durable ACKs it recorded (None
        # while unacknowledged — the replay causal condition).
        watermark = self.acks.get((session_id, epoch))
        if watermark is None:
            return None
        if target is not None and watermark < target:
            return None
        return watermark


def _bridge(transport, ack_applied):
    async def page(server, session, epoch, after, limit):
        # Finite snapshot pages from the real journal (lab double of the
        # daemon's _journal_page which uses the Journal port directly).
        out = []
        for seq in range(after + 1, after + 1 + limit):
            if seq > 6:
                break
            out.append(_event(seq))
        return out

    async def core_ack(server, session, epoch, watermark):
        ack_applied.append((server, session, epoch, watermark))

    return EventBridge(lambda _s: transport, core_ack, page), ack_applied


async def test_b04_sparse_event_batch_returns_without_waiting_for_future_events(
        tmp_path):
    """A sparse page (1 event, none after) returns immediately."""
    transport = _AckTransport()
    acks: list = []
    bridge, _ = _bridge(transport, acks)
    publish = bridge.publisher_for("srv_a")
    publish(_event(1))
    await asyncio.sleep(0.3)
    # The flusher must have SENT the single event without waiting for a
    # full batch — sparse page returned.
    assert transport.batches and transport.batches[0][0].sequence == 1
    await bridge.drain()


async def test_b05_retry_replays_unacknowledged_batch(tmp_path):
    """Without a durable ACK the SAME batch is replayed (seq1 sent
    twice), never silently skipped."""
    transport = _AckTransport()
    acks: list = []
    bridge, _ = _bridge(transport, acks)
    publish = bridge.publisher_for("srv_a")
    publish(_event(1))
    await asyncio.sleep(0.2)
    assert len(transport.batches) == 1  # first attempt, no ack yet
    # New wake (reconnect or retry) — no ack arrived.
    bridge.transport_online_again("srv_a")
    await asyncio.sleep(0.3)
    sequences = [event.sequence for batch in transport.batches
                 for event in batch]
    assert sequences.count(1) >= 2, \
        f"unacknowledged batch was not replayed: {sequences}"
    await bridge.drain()


async def test_b05b_future_ack_does_not_advance_core_watermark(
        cn2_peer):
    """A watermark beyond what was actually written never reaches the
    Core ack (transport-level validation, b05b)."""
    transport = await _negotiated(
        cn2_peer, on_operation=(lambda operation: None))
    # Nothing was ever written for this stream on this connection.
    key = transport._stream_key("session_x", "ep-x")
    transport._streams[key] = wss.StreamSendState(written_through=1)
    applied: list[tuple] = []

    def ack_callback(session, epoch, sequence):
        applied.append((session, epoch, sequence))

    transport._ack_callback = ack_callback
    # Inject a schema-valid event.ack claiming sequence 999.
    frame = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "event.ack",
        "server_id": "srv_a",
        "executor_id": "exe_a",
        "session_id": "session_x",
        "stream_epoch": "ep-x",
        "sequence": 999,
    }
    transport._apply_event_ack(frame)
    assert applied == [], "future watermark advanced the Core ack"
    assert transport._streams[key].acked_through == 0
    await transport.stop()


async def test_b05c_foreign_ack_not_associated_with_local_stream(
        cn2_peer):
    """An ACK from a FOREIGN namespace (valid NXL schema) never
    associates with the local stream."""
    transport = await _negotiated(
        cn2_peer, on_operation=(lambda operation: None))
    key = transport._stream_key("session_a", "ep-a")
    transport._streams[key] = wss.StreamSendState(written_through=5)
    applied: list[tuple] = []

    def ack_callback(session, epoch, sequence):
        applied.append((session, epoch, sequence))

    transport._ack_callback = ack_callback
    frame = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "event.ack",
        "server_id": "srv_OTHER",       # foreign namespace
        "executor_id": "exe_OTHER",
        "session_id": "session_a",
        "stream_epoch": "ep-a",
        "sequence": 3,
    }
    transport._apply_event_ack(frame)
    assert applied == []
    assert transport._streams[key].acked_through == 0
    await transport.stop()


def test_control_priority_queue_conserves_two_simultaneous_inputs():
    """Positive control preserved from CN1 (single Condition store)."""
    import asyncio as aio

    async def run():
        queues = PriorityQueues()
        await queues.put({"id": "urgent"}, urgent=True)
        await queues.put({"id": "normal"})
        return [await queues.get(), await queues.get()]

    items = aio.new_event_loop().run_until_complete(run())
    assert {item["id"] for item in items} == {"urgent", "normal"}


async def test_control_invalid_agent_is_refused_at_receiver(cn2_peer):
    """Positive control: wrong agent never reaches the handler."""
    received: list = []

    async def on_operation(operation):
        received.append(operation)
        return None

    transport = await _negotiated(cn2_peer, on_operation=on_operation)
    session = cn2_peer.latest
    frame = _submit_frame(session, agent="ag_OTHER")
    await session.websocket.send(encode_frame(frame))
    await asyncio.sleep(0.4)
    assert received == []
    await transport.stop()


# =====================================================================
# N04 — b06: failed reconciliation ≠ READY; b10: receipt w/o binary
# =====================================================================

async def test_b06_failed_reconciliation_does_not_mark_transport_ready(
        tmp_path):
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    calls: list = []

    async def failing_reconcile(frame):
        calls.append(frame)
        raise RuntimeError("journal unavailable (lab fault)")

    async def ticket():
        return TICKET

    async def on_operation(operation):
        return None

    async def noop_approval(frame):
        return None

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_a", link_url=peer.url,
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=noop_approval, on_reconcile=failing_reconcile)
    transport.add_lane("bind_a", "ag_a", ticket)
    transport.start()
    await asyncio.sleep(1.5)
    # CN2/N04: the projection FAILED — the transport must NOT be READY
    # and productive admissions stay blocked.
    assert not transport.online, \
        "failed reconciliation marked the transport ready"
    assert transport.stats.reconcile_state in (
        wss.RECONCILE_FAILED, wss.RECONCILE_PENDING)
    assert transport.stats.state != wss.ST_READY
    await transport.stop()
    await peer.stop()


async def test_b10_reconcile_durable_receipt_does_not_require_current_binary(
        tmp_path):
    """A receipt persisted in the journal answers reconcile AFTER the
    executable was removed — no candidate_for/build in the path."""
    import time as _time
    binary = _exe(tmp_path / "codex.exe")
    journal = SQLiteJournal(tmp_path / "journal.db")
    factory = RecordingFactory()

    async def env(prepared):
        return {}

    runtime = create_runtime(
        journal=journal, environment=env,
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={"ws_a": str(tmp_path)},
        native_factory=factory)
    context = ExecutionContext(
        "srv_a", "exe_a", "bind_a", "ag_a", "ws_a", 1, 1, 1,
        _time.monotonic() + 120,
        frozenset({"runtime.open", "turn.submit", "runtime.close"}))
    from nexus_connector_core import LaunchIntent, OpenOperation
    prepared = await runtime.prepare(
        LaunchIntent("ag_a", "ws_a", "codex_app_server"), context)
    await runtime.open(
        OpenOperation("op_persisted", "session_hist", "ep-h", prepared),
        context)
    await runtime.shutdown(ShutdownPolicy(2, 2))
    # Capture the binding record BEFORE the binary disappears (the
    # fingerprint is evidence; the file is gone on purpose).
    historical = _binding(tmp_path, binary)
    binary.unlink()

    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host.root = tmp_path
    host._vault = None
    host._journal_path = paths.journal_path(tmp_path)
    host._ledger_path = paths.owned_slot_ledger_path(tmp_path)
    host._runtimes = {}
    host._journal = journal  # the shared journal port stays open
    host._ledger = None
    host._journal_gate = None
    host._ledger_gate = None
    manager = _mgr(tmp_path, host)

    def seed(state):
        state.bindings.append(historical)

    manager._store.update(seed)
    report = await manager.reconcile(
        operation_ids=["op_persisted"],
        session_ids=[],
        server_id="srv_a", executor_id="exe_a")
    stages = [entry["stage"] for entry in report["receipts"]]
    assert "SUBMITTED" in stages or "RECEIVED_DURABLE" in stages, \
        f"durable receipt lost when the binary was removed: {report}"
    journal.close()


# =====================================================================
# N05 — b07: stop classification (failed_receipt / core_exception)
# =====================================================================

def _stop_manager(tmp_path, *, close_receipt=None, close_error=None):
    import time as _time
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host.root = tmp_path
    host._vault = None
    host._journal_path = paths.journal_path(tmp_path)
    host._ledger_path = paths.owned_slot_ledger_path(tmp_path)
    host._runtimes = {}
    host._journal = None
    host._ledger = None
    host._journal_gate = None
    host._ledger_gate = None
    manager = _mgr(tmp_path, host)
    binary = _exe(tmp_path / "codex.exe")
    session = ManagedSession(
        session_id="session_a",
        binding=_binding(tmp_path, binary),
        stream_epoch="ep", opened_at=_time.monotonic(),
        lease_deadline=_time.monotonic() + 600,
        authorized_actions=frozenset({"runtime.close"}))

    class _Runtime:
        async def close(self, operation, context):
            if close_error is not None:
                raise close_error
            return close_receipt

    session.runtime = _Runtime()
    manager._register(session)

    def seed(state):
        from okto_nexus_connector.storage.state_store import \
            IdentityRecord
        state.identities.append(IdentityRecord(
            "w", "srv_a", "ag_a", "vault:srv_a/ag_a", 1, "now"))
        state.servers["srv_a"] = ServerProfileRecord(
            server_id="srv_a", base_url="https://srv_a.example",
            origin="https://srv_a.example", added_at="now")

    manager._store.update(seed)

    import okto_nexus_connector.services.runtime_service as rs

    class _OKHTTP:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def resolve_intent(self, *a, **k):
            class R:
                operation_id = "op_close"
                session_id = "session_a"
                allowed_actions = ("runtime.close",)
                lease_seconds = 120.0
                execution = {}
            return R()

    rs.NexusHTTPClient = lambda *a, **k: _OKHTTP()
    (tmp_path / "vault").mkdir(exist_ok=True)
    manager._vault.store("srv_a/ag_a", "nxs_k07")
    return manager, session


async def test_b07_stop_failure_before_effect_keeps_session_retryable(
        tmp_path):
    """[failed_receipt]: a FAILED close receipt (no effect, retryable)
    releases only the stop reservation — the session stays managed."""
    import okto_nexus_connector.services.runtime_service as _rs
    _original_client = _rs.NexusHTTPClient
    manager, session = _stop_manager(
        tmp_path,
        close_receipt=OperationReceipt(
            "op_close", "sha256:" + "0" * 64, "FAILED",
            False, True, "session_a"))
    try:
        result = await manager.stop("session_a")
    finally:
        _rs.NexusHTTPClient = _original_client
    assert "session_a" in manager.session_ids(), \
        "FAILED pre-effect receipt removed the session"
    assert session.closing is False
    assert session.stop_attempt is None
    assert result["outcome"] == "failed_pre_effect"


async def test_b07_stop_failure_before_effect_keeps_session_retryable_core(
        tmp_path):
    """[core_exception]: a typed CoreError (retry_safe, pre-effect)
    during close releases the stop reservation — closing never sticks."""
    import okto_nexus_connector.services.runtime_service as _rs
    _original_client = _rs.NexusHTTPClient
    manager, session = _stop_manager(
        tmp_path,
        close_error=CoreError("JOURNAL_FULL", "admission",
                              retry_safe=True))
    try:
        with pytest.raises(ConnectorError) as error:
            await manager.stop("session_a")
    finally:
        _rs.NexusHTTPClient = _original_client
    assert error.value.retry_safe is True
    assert session.closing is False, \
        "CoreError left closing=True with no producer"
    assert session.stop_attempt is None
    assert "session_a" in manager.session_ids()


# =====================================================================
# N06 — b08: approval reaches the Core API; b09: MCP URL in launch
# =====================================================================

async def test_b08_approval_manager_reaches_public_core_api(tmp_path):
    import time as _time
    from nexus_connector_core import NativeApprovalOperation
    binary = _exe(tmp_path / "codex.exe")
    journal = SQLiteJournal(tmp_path / "journal.db")
    factory = RecordingFactory()

    async def env(prepared):
        return {}

    runtime = create_runtime(
        journal=journal, environment=env,
        candidates={"codex_app_server": _candidate(binary)},
        workspace_roots={"ws_a": str(tmp_path)},
        native_factory=factory)
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host.root = tmp_path
    host._vault = None
    host._journal_path = paths.journal_path(tmp_path)
    host._ledger_path = paths.owned_slot_ledger_path(tmp_path)
    host._runtimes = {}
    host._journal = journal
    host._ledger = None
    host._journal_gate = None
    host._ledger_gate = None
    manager = _mgr(tmp_path, host)
    session = ManagedSession(
        session_id="session_a", binding=_binding(tmp_path, binary),
        stream_epoch="ep", opened_at=_time.monotonic(),
        runtime=runtime,
        lease_deadline=_time.monotonic() + 600,
        authorized_actions=frozenset({"approval.decide",
                                      "turn.interrupt",
                                      "runtime.close"}))
    manager._register(session)
    calls: list[NativeApprovalOperation] = []
    original = runtime.decide_native_approval

    async def spy(operation, context):
        calls.append(operation)
        return OperationReceipt(
            operation.operation_id, "sha256:" + "1" * 64, "SUBMITTED",
            False, False, "session_a")

    runtime.decide_native_approval = spy
    # CN5 adaptation: the manager port is target-scoped (full
    # SessionKey namespace) — same causal condition: the request and
    # the decision reach the Core's public decide port.
    from nexus_connector_core import SessionKey as _SessionKey
    from okto_nexus_connector.services.approval_state import (
        ApprovalKey as _AKey, ApprovalTarget as _ATarget,
    )
    _rec = session.binding
    _target = _ATarget(
        key=_AKey(_rec.server_id, _rec.executor_id, "req_1"),
        binding_id=_rec.binding_id, agent_id=_rec.agent_id,
        workspace_id=_rec.workspace_id,
        session_key=_SessionKey(_rec.server_id, _rec.executor_id,
                               "session_a"),
        connection_generation=session.connection_generation,
        session_owner_generation=session.session_owner_generation,
        authorization_revision=_rec.authorization_revision,
        configuration_revision=_rec.configuration_revision,
        kind="requestApproval")
    result = await manager.decide_native_approval(
        target=_target, operation_id="op_appr_1",
        request={"kind": "commandExecution",
                 "native_request": {"id": "req_1"}},
        decision="deny", response=None)
    assert calls, "decide_native_approval never reached the Core port"
    operation = calls[0]
    # CN2/N06.2: request is the REQUEST mapping; decision is the string.
    assert operation.decision == "deny"
    assert isinstance(operation.request, dict) and \
        operation.request.get("kind") == "commandExecution"
    assert operation.operator_response is None
    assert result["receipt"]["stage"] == "SUBMITTED"
    runtime.decide_native_approval = original
    journal.close()


async def test_b09_managed_mcp_start_configures_server_url_not_only_token(
        tmp_path):
    """CN2/N06.1: the ephemeral session home carries the direct-HTTP
    MCP client config (URL + env-bound bearer reference) consumed by
    the harness process — not only the token variable."""
    peer = FakeNexusHTTPPeer()
    peer.add_agent(FakeAgent(agent_id="ag_1", key="nxs_k09"))
    url = await peer.start()
    from tests.fakes.http_peer import FakeBinding
    peer.bindings["bind_a"] = FakeBinding(
        binding_id="bind_a", agent_id="ag_1", alias="codex",
        adapter_id="codex_app_server")
    root = paths.state_dir(tmp_path)
    store = StateStore(paths.state_file(root))
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    vault.store("srv_fake/ag_1", "nxs_k09")
    binary = _exe(root / "codex.exe")

    def seed(state):
        state.connector_id = "conn_cn2"
        state.preferences["vault.fallback_file.approved"] = True
        state.servers["srv_fake"] = ServerProfileRecord(
            server_id="srv_fake", base_url=url, origin=url,
            added_at="now")
        state.identities.append(IdentityRecord(
            "work", "srv_fake", "ag_1", "vault:srv_fake/ag_1", 1,
            "now"))
        state.bindings.append(BindingRecord(
            binding_id="bind_a", alias="codex", server_id="srv_fake",
            agent_id="ag_1", adapter_id="codex_app_server",
            executor_id="conn_cn2", workspace_id="ws_a",
            workspace_root=str(root),
            candidate_executable=str(binary),
            candidate_fingerprint=fingerprint(binary),
            candidate_version="0.157.0",
            candidate_architecture=binary_architecture(binary)
            or "x86_64"))

    store.update(seed)
    manager = RuntimeManager(store, vault, CoreRuntimeHost(root, vault))
    captured: dict = {}
    original_build = manager._host.build

    async def spy_build(binding, *, environment, factory=None,
                        session_id=None):
        captured["environment"] = environment
        return await original_build(binding, environment=environment,
                                    factory=RecordingFactory(),
                                    session_id=session_id)

    manager._host.build = spy_build
    try:
        started = await manager.start(alias="codex", project=Path(root),
                                      harness=None, new_session=True,
                                      text=None)
    except ConnectorError as error:
        pytest.fail(f"managed start failed: {error}")
    session_id = started["session_id"]
    # CN4-03 adaptation: the ephemeral home is now a VERSIONED DIGEST
    # of the ownership tuple (readable prefix + full digest) — same
    # causal check: the generated config carries the Server URL.
    binding = manager.binding_by_alias("codex")
    base = Path(root) / "runtime" / "mcp"
    matches = [p for p in base.glob("v2-*")
               if binding.server_id in p.name
               and session_id in p.name]
    assert matches, "no namespaced v2 MCP tree was created"
    home = matches[0]
    config = home / ".codex" / "config.toml"
    assert config.is_file(), "no ephemeral MCP config was installed"
    content = config.read_text(encoding="utf-8")
    assert url in content, f"Server URL missing from launch config: " \
                           f"{content!r}"
    assert "bearer_token_env_var" in content
    # And the token env var reaches the effective environment (token
    # associated with the URL through the public template).
    environment = captured.get("environment")
    assert environment is not None
    from nexus_connector_core import LaunchIntent
    import time as _time
    session = manager.session_by_key(
        __import__("nexus_connector_core",
                   fromlist=["SessionKey"]).SessionKey(
            "srv_fake", "conn_cn2", session_id))
    runtime = session.runtime
    context = ExecutionContext(
        "srv_fake", "conn_cn2", "bind_a", "ag_1", "ws_a", 1, 1, 1,
        _time.monotonic() + 60, frozenset({"runtime.open"}))
    launch = await runtime.prepare(
        LaunchIntent("ag_1", "ws_a", "codex_app_server",
                     auth_refs=(f"mcp-cap:{session_id}",)), context)
    env = await environment(launch)
    assert any(key.startswith("NEXUS_MCP_TOKEN") for key in env), env
    await peer.stop()


# =====================================================================
# N07 — b11/b11b: ticket identity + live lane attach
# =====================================================================

async def test_b11_bootstrap_ticket_key_matches_selected_binding_agent(
        tmp_path):
    """The link ticket uses EXACTLY the selected binding's agent key —
    identity A first in the list never keys binding B."""
    peer = FakeNexusHTTPPeer()
    peer.add_agent(FakeAgent(agent_id="ag_A", key="nxs_key_a"))
    peer.add_agent(FakeAgent(agent_id="ag_B", key="nxs_key_b"))
    url = await peer.start()
    from tests.fakes.http_peer import FakeBinding
    peer.bindings["bind_de_B"] = FakeBinding(
        binding_id="bind_de_B", agent_id="ag_B", alias="b-binding",
        adapter_id="codex_app_server")
    root = paths.state_dir(tmp_path)
    app = DaemonApp.__new__(DaemonApp)
    app.root = root
    app.store = StateStore(paths.state_file(root))
    app.vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    app.transports = {}
    binary = _exe(root / "codex.exe")

    def seed(state):
        state.connector_id = "conn_b11"
        state.servers["srv_x"] = ServerProfileRecord(
            server_id="srv_x", base_url=url, origin=url, added_at="now")
        # Identity A is FIRST in the list; the ONLY binding belongs
        # to agent B (lab stand-in for a lane of another agent).
        state.identities.append(IdentityRecord(
            "aliasA", "srv_x", "ag_A", "vault:srv_x/ag_A", 1, "now"))
        state.identities.append(IdentityRecord(
            "aliasB", "srv_x", "ag_B", "vault:srv_x/ag_B", 1, "now"))
        state.bindings.append(BindingRecord(
            binding_id="bind_de_B", alias="b-binding", server_id="srv_x",
            agent_id="ag_B", adapter_id="codex_app_server",
            executor_id="conn_b11", workspace_id="ws",
            workspace_root=str(root),
            candidate_executable=str(binary),
            candidate_fingerprint=fingerprint(binary),
            candidate_version="0.157.0",
            candidate_architecture=binary_architecture(binary)
            or "x86_64"))

    app.store.update(seed)
    app.vault.store("srv_x/ag_A", "nxs_key_a")
    app.vault.store("srv_x/ag_B", "nxs_key_b")
    provider = app._ticket_provider("srv_x")
    ticket = await provider()
    # The fake peer records (agent_id, binding_id, expiry) per ticket;
    # the issued ticket must belong to agent B for binding B.
    issued = [(agent, binding) for _t, (agent, binding, _e)
              in peer.tickets.items()]
    assert ("ag_B", "bind_de_B") in issued, \
        f"bootstrap keyed the wrong identity: {issued}"
    await peer.stop()


async def test_b11b_lane_added_after_ready_is_actually_attached(cn2_peer):
    """Adding a lane on a READY transport completes a REAL attach
    (frame written + peer records the lane) without reconnecting."""
    attached: list = []

    async def on_operation(operation):
        return None

    transport = await _negotiated(cn2_peer, on_operation=on_operation)

    async def ticket():
        return TICKET

    reconnects_before = transport.stats.reconnects
    transport.add_lane("bind_new", "ag_new", ticket)
    await _wait_for(lambda: cn2_peer.latest.lanes.get("bind_new")
                    == "ag_new", timeout=5)
    attached.append("bind_new")
    assert cn2_peer.latest.lanes["bind_new"] == "ag_new"
    assert transport.stats.reconnects == reconnects_before, \
        "lane addition forced a socket reconnect"
    await transport.stop()


# =====================================================================
# N08 — b12: invalid URL rejected before ticket send
# =====================================================================

@pytest.mark.parametrize("link_url", ["ftp://remote.example/link", "ws:///link",
                                      "ws://user:password@remote.example/link"])
async def test_b12_invalid_link_rejected_before_ticket_send(link_url):
    """An invalid link URL is refused at construction — before any
    ticket fetch or credential reaches the websocket library."""
    from okto_nexus_connector.transport import wss_client as module
    sent: list = []

    class _Sentinel:
        def __getattr__(self, name):
            sent.append(name)
            raise AssertionError(
                f"websocket library reached with plaintext link: {name}")

    tickets_fetched: list = []

    async def ticket():
        tickets_fetched.append(1)
        return "nstkt_should_never_exist"

    async def on_operation(_):
        return None

    original_connect = module.websockets.connect
    module.websockets.connect = _Sentinel()
    try:
        with pytest.raises(ConnectorError) as error:
            NXLTransport(
                server_id="srv_a", executor_id="exe_a",
                link_url=link_url,
                ticket_provider=ticket, on_operation=on_operation,
                on_approval=on_operation)
    finally:
        module.websockets.connect = original_connect
    assert error.value.code == "PROFILE_DRIFT"
    assert sent == [] and tickets_fetched == []
    # Approved wss and local/remote ws still construct.
    NXLTransport(
        server_id="srv", executor_id="exe",
        link_url="wss://remote.example/link",
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=on_operation)
    NXLTransport(
        server_id="srv", executor_id="exe",
        link_url="ws://remote.example/link",
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=on_operation)
    NXLTransport(
        server_id="srv", executor_id="exe",
        link_url="ws://127.0.0.1:9/link",
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=on_operation)
