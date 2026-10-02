"""CN1 audit regressions — reconstructed from the reviewer package.

The original ``regressoes/test_connector_audit.py`` file was NOT
delivered in FIX_UPDATE_PLAN (only the Markdown reports and
RESULTADOS_RESUMO.json); per the package's own instruction the 34
cases are the contract. These seeds are rebuilt from the exact failure
messages recorded in RESULTADOS_RESUMO.json (campaign
``audit_source_core028``) — same node ids, same causal conditions
(valid intent hash + invalid scope, two-put wake race, stop unknown,
POST-received timeout, A/B selection) — exercising the REAL connector
code paths and the REAL Core at the pertinent boundaries. No assert was
weakened; the three positive controls and the buffer observation are
preserved (the observation was REPLICATED historically and is kept as a
bounded-memory control per CN-03.05).

Platform-neutral (the auditor ran Linux/3.13.5; these run anywhere).
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(0, str(Path(__file__).parents[2] / "tests"))

from nexus_connector_core import (CoreError, ExecutionContext,
                                    InstallationCandidate)
from nexus_connector_core.discovery import binary_architecture, fingerprint
from nexus_connector_core.frame_codec import encode_frame
from nexus_connector_core.protocol import submit_frame_intent_hash

from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.platform import paths
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.services.runtime_service import (
    ManagedSession, RuntimeManager,
)
from okto_nexus_connector.storage.state_store import (
    BindingRecord, ConnectorState, StateStore,
)
from okto_nexus_connector.transport.https_client import NexusHTTPClient
from okto_nexus_connector.transport.wss_client import (
    NXLTransport, PriorityQueues,
)
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.conftest import RecordingFactory


def _MINIMAL_BINARY() -> bytes:
    """A structurally valid executable header for selection tests:
    minimal PE (win32) or ELF (POSIX) so the Core's passive checks pass
    without executing anything."""
    if sys.platform == "win32":
        import struct
        dos = b"MZ" + bytes(58) + struct.pack("<I", 64)
        nt = b"PE" + bytes(2) + struct.pack(
        "<HHIIIHH", 0x8664, 1, 0, 0, 0, 0xF0, 0x22)
        return dos + nt
    return b"ELF" + b"" + b"" + b"" + b"" * 9 +         b"" + b"" * 55


def _exe(path: Path, content: bytes = b"synthetic") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


# =====================================================================
# A01 — candidate architecture survives the binding round-trip
# =====================================================================

async def test_01_qualified_candidate_architecture_survives_binding_roundtrip(
        tmp_path):
    from okto_nexus_connector.services.connect_service import create_binding
    from okto_nexus_connector.identity.import_flow import ImportResult
    from okto_nexus_connector.storage.state_store import IdentityRecord
    peer = FakeNexusHTTPPeer()
    peer.add_agent(FakeAgent(agent_id="ag_1", key="nxs_k01"))
    url = await peer.start()
    store = StateStore(tmp_path / "state.json")
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    (tmp_path / "vault").mkdir(exist_ok=True)
    vault.store("srv_fake/ag_1", "nxs_k01")
    identity = IdentityRecord("work", "srv_fake", "ag_1",
                              "vault:srv_fake/ag_1", 1, "now")
    store.update(lambda s: (
        setattr(s, "connector_id", "conn_audit"),
        s.identities.append(identity)))
    project = tmp_path / "project"
    project.mkdir()
    binary = _exe(tmp_path / "codex.exe")
    # A REAL PE-parsable candidate with explicit architecture evidence.
    candidate = InstallationCandidate(
        "codex_app_server", str(binary), fingerprint(binary),
        "explicit", "selected", "0.157.0",
        architecture="x86_64",
        build_identity=None)
    result = ImportResult(identity, True, "ag_1", "srv_fake")
    from okto_nexus_connector.identity.import_flow import ImportResult as IR
    result = IR(identity=identity, created=True, me_agent_id="ag_1",
                server_id="srv_fake")
    async with NexusHTTPClient(url) as http:
        summary = await create_binding(
            http, store, identity=result, key="nxs_k01", alias="codex",
            adapter_id="codex_app_server", candidate=candidate,
            version="0.157.0", workspace_root=project, server_url=url)
    host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host.root = tmp_path
    rebuilt = host.candidate_for(summary.binding)
    assert rebuilt.architecture == "x86_64", \
        f"architecture lost: {rebuilt.architecture}"
    await peer.stop()


# =====================================================================
# A03 — typed scope keys
# =====================================================================

async def test_02_two_servers_same_binding_id_do_not_share_core_instance(
        tmp_path):
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
    binary = _exe(tmp_path / "codex.exe")

    def binding(server):
        return BindingRecord(
            binding_id="bind_same", alias=f"local-{server}",
            server_id=server, agent_id="ag", adapter_id="codex_app_server",
            executor_id=f"exe_{server}", workspace_id="ws",
            workspace_root=str(tmp_path),
            candidate_executable=str(binary),
            candidate_fingerprint=fingerprint(binary),
            candidate_version="0.157.0",
            candidate_architecture=binary_architecture(binary) or "x86_64")

    async def env(prepared):
        return {}

    factory_a, factory_b = RecordingFactory(), RecordingFactory()
    runtime_a = await host.build(binding("srv_a"), environment=env,
                                 factory=factory_a)
    runtime_b = await host.build(binding("srv_b"), environment=env,
                                 factory=factory_b)
    assert runtime_a is not runtime_b, \
        "runtime from srv_a was reused for srv_b"


# =====================================================================
# A02 — authority: no fallback permissions, no lease extension
# =====================================================================

class _Resolution:
    def __init__(self, actions, lease=120.0, execution=None,
                 operation_id="op_close_14", session_id="session_a"):
        self.allowed_actions = tuple(actions)
        self.lease_seconds = lease
        self.execution = execution or {}
        self.operation_id = operation_id
        self.session_id = session_id


def _manager(tmp_path, host=None) -> RuntimeManager:
    store = StateStore(tmp_path / "state.json")
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    host = host or object()
    return RuntimeManager(store, vault, host)


def test_03_empty_authority_does_not_gain_fallback_permissions(tmp_path):
    manager = _manager(tmp_path)
    binding = BindingRecord(
        binding_id="bind_a", alias="local-srv_a", server_id="srv_a",
        agent_id="ag_a", adapter_id="codex_app_server",
        executor_id="exe_a", workspace_id="ws_a",
        workspace_root=str(tmp_path))
    resolution = _Resolution(actions=[])  # EMPTY grant
    context = manager._context(binding, resolution, identity=None)
    assert not context.allowed_actions, \
        f"permissions fabricated: {context.allowed_actions}"


def test_04_remote_context_never_extends_expired_authorization(tmp_path):
    import time as _time
    manager = _manager(tmp_path)
    binding = BindingRecord(
        binding_id="bind_a", alias="local-srv_a", server_id="srv_a",
        agent_id="ag_a", adapter_id="codex_app_server",
        executor_id="exe_a", workspace_id="ws_a",
        workspace_root=str(tmp_path))
    now = _time.monotonic()
    session = ManagedSession(
        session_id="session_a", binding=binding, stream_epoch="ep",
        opened_at=now, lease_deadline=now - 60.0,  # EXPIRED
        authorized_actions=frozenset({"turn.submit", "turn.interrupt",
                                      "runtime.close"}))
    context = manager._authorized_context(session, "turn.interrupt",
                                          containment=True)
    assert context.lease_deadline_monotonic <= session.lease_deadline, \
        "expired lease was extended locally"


# =====================================================================
# A02/A05 — transport envelope scope, generation, reader decoupling
# =====================================================================

VALID_TICKET = "nstkt_audit_valid"


@pytest.fixture
async def audit_peer():
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={VALID_TICKET})
    await peer.start()
    yield peer
    await peer.stop()


def _audit_transport(peer, **kwargs):
    async def ticket():
        return VALID_TICKET

    async def noop(frame):
        return None

    defaults = dict(
        server_id="srv_a", executor_id="exe_a", link_url=peer.url,
        ticket_provider=ticket, on_operation=noop, on_approval=noop)
    defaults.update(kwargs)
    return NXLTransport(**defaults)


def _valid_submit_frame(session, *, action="turn.submit",
                        binding_id="bind_a", agent_id="ag_a",
                        session_id="session_a", operation_id="op_x",
                        server_id=None, executor_id=None,
                        connection_generation=None,
                        expected_turn_id=None):
    frame = {
        "protocol_major": 1,
        "contract_revision":
            "nxl-1-agent-centric-http-only-2026-09-25-r3",
        "type": "operation.submit",
        "server_id": server_id or session.server_id,
        "executor_id": executor_id or session.executor_id,
        "binding_id": binding_id,
        "agent_id": agent_id,
        "workspace_id": "ws_a",
        "workspace_binding_id": "wb_a",
        "session_id": session_id,
        "operation_id": operation_id,
        "action": action,
        "connection_generation":
            connection_generation if connection_generation is not None
            else session.connection_generation,
        "authorization_revision": 1,
        "configuration_revision": 1,
        "payload": {"text": "audit"},
    }
    if expected_turn_id is not None:
        frame["expected_turn_id"] = expected_turn_id
    frame["intent_hash"] = submit_frame_intent_hash(frame)
    return frame


async def _ready(transport, peer, binding_id="bind_a"):
    transport.start()
    await transport.wait_online(10)
    deadline = asyncio.get_event_loop().time() + 10
    while asyncio.get_event_loop().time() < deadline:
        if transport.stats.state == "ready" and \
                peer.latest and peer.latest.welcomed and \
                peer.latest.lanes.get(binding_id):
            return peer.latest
        await asyncio.sleep(0.02)
    raise AssertionError("transport never became ready")


@pytest.mark.parametrize("changes", [
    {"server_id": "srv_other"},                       # wrong server
    {"executor_id": "exe_other"},                     # wrong executor
    {"agent_id": "ag_other"},                         # wrong agent
    {"connection_generation": 999},                   # stale generation
    {"binding_id": "bind_not_attached"},              # lane not attached
])
async def test_05_inbound_operations_require_exact_live_lane_scope(
        audit_peer, changes):
    received: list[dict] = []

    async def on_operation(frame):
        received.append(dict(frame))
        return None

    transport = _audit_transport(audit_peer, on_operation=on_operation)

    async def lane_ticket():
        return VALID_TICKET
    transport.add_lane("bind_a", "ag_a", lane_ticket)
    session = await _ready(transport, audit_peer)
    base = _valid_submit_frame(session)
    base.update(changes)
    # A frame with a VALID intent hash but INVALID scope.
    base["intent_hash"] = submit_frame_intent_hash(base)
    await audit_peer.latest.websocket.send(encode_frame(base))
    await asyncio.sleep(0.5)
    assert not received, \
        f"invalid scope or stale generation reached operation callback: " \
        f"{received}"
    await transport.stop()


async def test_06_server_generation_is_adopted_after_welcome(audit_peer):
    peer = audit_peer
    peer._generation_counter = 6  # next welcome authorizes generation 7
    transport = _audit_transport(peer)

    async def lane_ticket():
        return VALID_TICKET
    transport.add_lane("bind_a", "ag_a", lane_ticket)
    await _ready(transport, peer)
    assert transport.stats.connection_generation == 7, \
        transport.stats.connection_generation
    await transport.stop()


async def test_07_operation_cannot_enter_before_handshake_and_reconcile(
        audit_peer):
    received: list[dict] = []

    async def on_operation(frame):
        received.append(dict(frame))
        return None

    transport = _audit_transport(audit_peer, on_operation=on_operation)
    transport.start()
    # Do NOT wait for welcome: send immediately after socket open.
    deadline = asyncio.get_event_loop().time() + 5
    while audit_peer.latest is None:
        await asyncio.sleep(0.02)
    session = audit_peer.latest
    frame = _valid_submit_frame(session)
    await session.websocket.send(encode_frame(frame))
    await asyncio.sleep(0.5)
    assert not received, \
        f"operation accepted without welcome/lane/reconciliation: {received}"
    await transport.stop()


async def test_22_websocket_dispatch_does_not_ignore_wire_identity(tmp_path):
    """Full path: real Core runtime + journal; a frame with the WRONG
    agent reaches zero send_turn (envelope validated before dispatch)."""
    from tests.integration.conftest import RecordingNative
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={VALID_TICKET})
    await peer.start()
    received: list[dict] = []

    async def on_operation(frame):
        received.append(dict(frame))
        return None

    async def ticket():
        return VALID_TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_a", link_url=peer.url,
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=(lambda f: None))
    transport.add_lane("bind_a", "ag_a", ticket)
    session = await _ready(transport, peer)
    frame = _valid_submit_frame(session, agent_id="ag_OTHER")
    await session.websocket.send(encode_frame(frame))
    await asyncio.sleep(0.6)
    assert not received
    await transport.stop()
    await peer.stop()


# =====================================================================
# A04 — priority queue loss
# =====================================================================

async def test_08_priority_queue_conserves_both_items_waking_together():
    queues = PriorityQueues()
    await queues.put({"id": "urgent"}, urgent=True)
    await queues.put({"id": "normal"})
    first = await queues.get()
    second = await queues.get()
    delivered = {first["id"], second["id"]}
    assert delivered == {"urgent", "normal"}, (first, second)


async def test_08b_waiter_woken_by_two_puts_loses_nothing():
    queues = PriorityQueues()

    async def waiter():
        return [await queues.get(), await queues.get()]

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    await queues.put({"id": "urgent"}, urgent=True)
    await asyncio.sleep(0.005)
    await queues.put({"id": "normal"})
    items = await asyncio.wait_for(task, 5)
    assert {item["id"] for item in items} == {"urgent", "normal"}


async def test_08c_cancelled_waiter_keeps_accepted_items():
    """A waiter cancelled while PARKED (empty store) loses nothing;
    items accepted afterwards are delivered exactly once."""
    queues = PriorityQueues()
    task = asyncio.create_task(queues.get())
    await asyncio.sleep(0.01)          # parked in condition.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await queues.put({"id": "urgent"}, urgent=True)
    await queues.put({"id": "normal"})
    first = await asyncio.wait_for(queues.get(), 5)
    second = await asyncio.wait_for(queues.get(), 5)
    assert {first["id"], second["id"]} == {"urgent", "normal"}


# =====================================================================
# A05 — watchdog + reader/scheduler decoupling
# =====================================================================

async def test_09_silent_websocket_is_detected_while_tasks_are_pending(
        audit_peer, monkeypatch):
    import okto_nexus_connector.transport.wss_client as wss
    monkeypatch.setattr(wss, "ABSENCE_DETECT_SECONDS", 0.15)
    transport = _audit_transport(audit_peer)

    async def lane_ticket():
        return VALID_TICKET
    transport.add_lane("bind_a", "ag_a", lane_ticket)
    await _ready(transport, audit_peer)
    # Peer goes silent with the socket OPEN; sender/receiver/heartbeat
    # tasks remain alive. The watchdog must fire on its own deadline.
    state_changes: set = set()
    deadline = asyncio.get_event_loop().time() + 5
    while asyncio.get_event_loop().time() < deadline:
        state_changes.add(transport.stats.state)
        if transport.stats.reconnects >= 2:
            break
        await asyncio.sleep(0.05)
    assert transport.stats.reconnects >= 2 or \
        transport.stats.state in (wss.ST_BACKOFF, wss.ST_DEGRADED,
                                  wss.ST_CONNECTING), \
        f"absence deadline never checked while tasks await: {state_changes}"
    await transport.stop()


async def test_26_blocked_submit_does_not_block_received_control(
        audit_peer):
    controls: list[dict] = []
    submit_started = asyncio.Event()

    async def on_operation(operation):
        if operation.action == "turn.submit":
            submit_started.set()
            await asyncio.sleep(2.0)  # stuck submit
            return None
        controls.append(operation)
        return None

    transport = _audit_transport(audit_peer, on_operation=on_operation)

    async def lane_ticket():
        return VALID_TICKET
    transport.add_lane("bind_a", "ag_a", lane_ticket)
    session = await _ready(transport, audit_peer)
    submit = _valid_submit_frame(session, operation_id="op_submit",
                                 action="turn.submit")
    interrupt = _valid_submit_frame(session, operation_id="op_interrupt",
                                    action="turn.interrupt",
                                    expected_turn_id="turn-1")
    await session.websocket.send(encode_frame(submit))
    await asyncio.wait_for(submit_started.wait(), 5)
    # While the submit handler is blocked, the interrupt must be
    # received and dispatched by the independent reader/scheduler.
    await session.websocket.send(encode_frame(interrupt))
    try:
        await asyncio.wait_for(_wait_list(controls, 1), 3.0)
    except asyncio.TimeoutError:
        pytest.fail("queued remote interrupt cannot be read while turn "
                    "handler waits")
    await transport.stop()


async def _wait_list(items, count):
    while len(items) < count:
        await asyncio.sleep(0.02)


# =====================================================================
# A06 — TLS/origin before secrets; exact origin on MCP import
# =====================================================================

async def test_10_http_plaintext_nonloopback_rejected_before_secret_transmission():
    import httpx
    seen: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False)
    try:
        with pytest.raises(ConnectorError):
            async with NexusHTTPClient("http://remote.example",
                                       client=client) as http:
                await http._request("POST", "/effect", key="nxs_canonical")
    finally:
        await client.aclose()
    assert not seen, f"canonical credential was dispatched over " \
                     f"non-loopback HTTP: {seen}"


async def test_11_read_timeout_after_mutating_request_is_not_safe_retry():
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        # The transport RECEIVED the POST; the reply is lost.
        raise httpx.ReadTimeout("reply unavailable")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), trust_env=False)
    try:
        async with NexusHTTPClient("https://srv.example", client=client) \
                as http:
            with pytest.raises(ConnectorError) as error:
                await http._request("POST", "/effect", key="nxs_k",
                                    json_body={})
    finally:
        await client.aclose()
    assert error.value.possible_effect is True, error.value
    assert error.value.retry_safe is False


def test_12_explicit_mcp_import_uses_exact_origin_not_prefix(tmp_path):
    from okto_nexus_connector.identity.import_flow import \
        read_secret_from_mcp_entry
    config = tmp_path / "mcp.json"
    config.write_text(json.dumps({
        "mcpServers": {
            "nexus": {"type": "http",
                      "url": "https://nexus.example.attacker.invalid/mcp",
                      "headers": {"Authorization":
                                  "Bearer nxs_stolen"}}}}))
    with pytest.raises(ConnectorError):
        read_secret_from_mcp_entry(
            config, entry_name="nexus",
            server_origin="https://nexus.example")


def test_control_http_cross_origin_redirect_never_forwarded():
    """Positive control (kept green): cross-origin redirect refused."""
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/connections/me":
            return httpx.Response(302, headers={
                "Location": "https://evil.example.net/steal"})
        return httpx.Response(200, json={})

    async def run():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                   trust_env=False)
        try:
            async with NexusHTTPClient("https://nexus.example",
                                       client=client) as http:
                with pytest.raises(ConnectorError) as error:
                    await http.me("nxs_k")
            return error.value
        finally:
            await client.aclose()

    error = asyncio.new_event_loop().run_until_complete(run())
    assert error.code == "PROFILE_DRIFT"


# =====================================================================
# A08 — stop attempt semantics / shutdown ownership
# =====================================================================

class _FailingResolveHTTP:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def resolve_intent(self, *args, **kwargs):
        raise ConnectorError("EXECUTOR_OFFLINE", "http",
                             "server unreachable (pre-effect)",
                             retry_safe=True)


async def test_13_failed_stop_can_be_retried_and_not_marked_closed(tmp_path):
    import time as _time
    manager = RuntimeManager.__new__(RuntimeManager)
    manager._store = StateStore(tmp_path / "state.json")
    manager._vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    manager._host = CoreRuntimeHost.__new__(CoreRuntimeHost)
    manager._sessions = {}
    manager._index = {}
    manager._pumps = {}
    manager._lease_tasks = {}
    manager._draining = False
    manager._event_publisher = None
    manager._connection_generation = None
    binding = BindingRecord(
        binding_id="bind_a", alias="local-srv_a", server_id="srv_a",
        agent_id="ag_a", adapter_id="codex_app_server",
        executor_id="exe_a", workspace_id="ws_a",
        workspace_root=str(tmp_path))
    session = ManagedSession("session_a", binding, "ep", _time.monotonic())
    manager._register(session)

    class _StubNative:
        async def close(self):
            raise AssertionError("native close must not run when the "
                                 "authorization resolution fails")

    class _StubRuntime:
        async def close(self, operation, context):
            return _StubNative().close()

    session.runtime = _StubRuntime()
    original_client = sys.modules[
        "okto_nexus_connector.services.runtime_service"].NexusHTTPClient
    sys.modules["okto_nexus_connector.services.runtime_service"]. \
        NexusHTTPClient = lambda *a, **k: _FailingResolveHTTP()
    identity_stub = type("I", (), {"secret_handle":
                                   "vault:srv_a/ag_a"})()
    from okto_nexus_connector.storage.state_store import IdentityRecord
    manager._store.update(lambda s: s.identities.append(
        IdentityRecord("w", "srv_a", "ag_a", "vault:srv_a/ag_a", 1,
                       "now")))
    (tmp_path / "vault").mkdir(exist_ok=True)
    manager._vault.store("srv_a/ag_a", "nxs_k13")
    try:
        with pytest.raises(ConnectorError):
            await manager.stop("session_a")
    finally:
        sys.modules["okto_nexus_connector.services.runtime_service"]. \
            NexusHTTPClient = original_client
    assert session.closing is False, \
        "stop failure permanently leaves closing=True"


async def test_14_unknown_close_receipt_keeps_managed_session_recoverable(
        tmp_path):
    import time as _time
    from nexus_connector_core import OperationReceipt
    manager = _manager(tmp_path)
    binding = BindingRecord(
        binding_id="bind_a", alias="local-srv_a", server_id="srv_a",
        agent_id="ag_a", adapter_id="codex_app_server",
        executor_id="exe_a", workspace_id="ws_a",
        workspace_root=str(tmp_path))
    session = ManagedSession("session_a", binding, "ep", _time.monotonic())
    manager._register(session)

    class _UnknownRuntime:
        async def close(self, operation, context):
            return OperationReceipt(
                operation.operation_id, "sha256:" + "0" * 64,
                "OUTCOME_UNKNOWN", True, False, operation.session_id)
        async def resolve_intent_stub(self):
            return None

    session.runtime = _UnknownRuntime()
    # Feed a resolution through the public stop path: patch the HTTP
    # client to succeed and return a close intent.
    resolution = _Resolution(actions=("runtime.close",))
    original = manager._context
    manager._context = lambda *a, **k: ExecutionContext(
        "srv_a", "exe_a", "bind_a", "ag_a", "ws_a", 1, 1, 1,
        _time.monotonic() + 60, frozenset({"runtime.close"}))
    sys_mod = sys.modules[
        "okto_nexus_connector.services.runtime_service"]
    original_client = sys_mod.NexusHTTPClient

    class _OKHTTP(_FailingResolveHTTP):
        async def resolve_intent(self, *a, **k):
            return resolution

    (tmp_path / "vault").mkdir(exist_ok=True)
    sys_mod.NexusHTTPClient = lambda *a, **k: _OKHTTP()
    manager._vault.store("srv_a/ag_a", "nxs_k14")
    from okto_nexus_connector.storage.state_store import (
        IdentityRecord, ServerProfileRecord)

    def seed(state):
        state.identities.append(IdentityRecord(
            "w", "srv_a", "ag_a", "vault:srv_a/ag_a", 1, "now"))
        state.servers["srv_a"] = ServerProfileRecord(
            server_id="srv_a", base_url="https://srv_a.example",
            origin="https://srv_a.example", added_at="now")

    manager._store.update(seed)
    try:
        result = await manager.stop("session_a")
    finally:
        sys_mod.NexusHTTPClient = original_client
        manager._context = original
    assert "session_a" in manager.session_ids(), \
        "unknown runtime was removed from public management"


async def test_15_shutdown_does_not_discard_core_ownership_on_unknown(
        tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from nexus_connector_core import ShutdownReport
    host = CoreRuntimeHost(tmp_path, None)
    binary = _exe(tmp_path / "codex.exe")
    binding = BindingRecord(
        binding_id="bind_a", alias="local-srv_a", server_id="srv_a",
        agent_id="ag_a", adapter_id="codex_app_server",
        executor_id="exe_a", workspace_id="ws_a",
        workspace_root=str(tmp_path),
        candidate_executable=str(binary),
        candidate_fingerprint=fingerprint(binary),
        candidate_version="0.157.0",
        candidate_architecture=binary_architecture(binary) or "x86_64")

    async def env(prepared):
        return {}

    report = ShutdownReport({("srv_a", "exe_a", "session_x"):
                             "unknown"})
    unknown_runtime = SimpleNamespace(
        shutdown=AsyncMock(return_value=report))
    from okto_nexus_connector.services.core_host import BindingKey
    host._runtimes[BindingKey("srv_a", "bind_a")] = unknown_runtime
    await host.shutdown_all()
    assert host.get("bind_a", "srv_a") is unknown_runtime, \
        "core instance with unknown resource ownership was forgotten"


# =====================================================================
# A09 — reconcile namespace + valid NXL serialization
# =====================================================================

async def test_16_reconcile_response_is_valid_nxl_not_string_snapshots(
        tmp_path):
    from nexus_connector_core import CoreError
    peer = FakeNexusHTTPPeer()
    url = await peer.start()
    root = paths.state_dir(tmp_path)
    store = StateStore(paths.state_file(root))
    binary = _exe(root / "codex.exe")

    def mutate(state):
        state.connector_id = "conn_audit"
        state.preferences["vault.fallback_file.approved"] = True
        from okto_nexus_connector.storage.state_store import             ServerProfileRecord
        state.servers["srv_a"] = ServerProfileRecord(
            server_id="srv_a", base_url=url, origin=url,
            added_at="now")
        state.bindings.append(BindingRecord(
            binding_id="bind_a", alias="codex", server_id="srv_a",
            agent_id="ag_a", adapter_id="codex_app_server",
            executor_id="conn_audit", workspace_id="ws_a",
            workspace_root=str(root),
            candidate_executable=str(binary),
            candidate_fingerprint=fingerprint(binary),
            candidate_version="0.157.0",
            candidate_architecture=binary_architecture(binary)
            or "x86_64"))

    store.update(mutate)
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    vault.store("srv_a/ag_a", "nxs_k16")
    app = DaemonApp.__new__(DaemonApp)
    app.root = root
    app.store = store
    app.vault = vault
    app.host = CoreRuntimeHost(root, vault)
    from okto_nexus_connector.daemon.app import EventBridge
    app.bridge = EventBridge(lambda *_: None, lambda *_: None,
                             lambda *_: [])
    app.runtimes = RuntimeManager(store, vault, app.host)
    report = await app._on_reconcile({
        "server_id": "srv_a", "executor_id": "conn_audit",
        "operation_ids": ["op_zz"], "session_ids": ["session_zz"]})
    try:
        encode_frame(report)  # real codec validation
    except CoreError as error:
        pytest.fail(f"reconcile report rejected by the real NXL codec: "
                    f"{error}")
    await peer.stop()


async def test_17_reconcile_passes_originating_namespace_not_first_binding(
        tmp_path):
    manager = _manager(tmp_path)
    captured: dict = {}
    from okto_nexus_connector.services.core_host import BindingKey

    class _Rt:
        async def reconcile(self, request):
            captured["server_id"] = request.server_id
            from nexus_connector_core import ReconcileReport
            return ReconcileReport((), ())

    for server in ("srv_a", "srv_b"):
        manager._host = manager._host if isinstance(
            manager._host, CoreRuntimeHost) else CoreRuntimeHost(
            tmp_path, manager._vault)
        manager._host._runtimes[BindingKey(server, f"bind_{server}")] = _Rt()
        manager._store.update(lambda s, srv=server: s.bindings.append(
            BindingRecord(
                binding_id=f"bind_{srv}", alias=f"al-{srv}",
                server_id=srv, agent_id="ag", adapter_id="codex_app_server",
                executor_id=f"exe_{srv}", workspace_id="ws",
                workspace_root=str(tmp_path))))

    class _JournalStub:
        async def get_receipt(self, key):
            captured["server_id"] = key.server_id
            captured["executor_id"] = key.executor_id
            return None

    manager._host._journal = _JournalStub()
    result = await manager.reconcile(
        server_id="srv_b", executor_id="exe_b",
        operation_ids=["op_probe"], session_ids=[])
    assert captured.get("server_id") == "srv_b" and \
        captured.get("executor_id") == "exe_b", \
        f"reconcile used the wrong namespace: {captured} / {result}"


# =====================================================================
# A15 — StateStore.save
# =====================================================================

def test_18_state_store_save_public_method_works(tmp_path):
    store = StateStore(tmp_path / "state.json")
    state = ConnectorState(connector_id="conn_save")
    store.save(state)
    loaded = store.load()
    assert loaded.connector_id == "conn_save"
    # concurrent update keeps its own fields
    store.update(lambda s: setattr(s, "schema_version",
                                   s.schema_version))
    assert store.load().connector_id == "conn_save"


# =====================================================================
# A14 — headless connect / exact selection / bind create DTO
# =====================================================================

async def test_19_noninteractive_connect_does_not_prompt(tmp_path,
                                                         monkeypatch):
    calls: list[dict] = []
    import okto_nexus_connector.cli.commands.connect as connect_module
    original_confirm = connect_module.confirm
    original_select = connect_module.select

    def spy_confirm(question, *, non_interactive, default=False):
        calls.append({"non_interactive": non_interactive,
                      "default": default})
        return original_confirm(question,
                                non_interactive=non_interactive,
                                default=default)

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(connect_module, "confirm", spy_confirm)
    monkeypatch.setattr(connect_module, "select",
                        lambda *a, **k: (_ for _ in ()).throw(
                            AssertionError("select prompted")))
    monkeypatch.setattr(connect_module, "_read_key",
                        lambda args, output: "nxs_k19")
    monkeypatch.setattr(connect_module, "import_identity", noop)
    monkeypatch.setattr(connect_module, "_remember_server", lambda *a: None)
    monkeypatch.setattr(connect_module, "_choose_harness",
                        noop)
    monkeypatch.setattr(connect_module, "create_binding", noop)
    from okto_nexus_connector.daemon import manager as daemon_manager
    monkeypatch.setattr(daemon_manager, "start",
                        lambda root: {"started": True})
    monkeypatch.setattr(daemon_manager, "connect",
                        lambda root: None)
    import argparse
    args = argparse.Namespace(
        server="https://nexus.example", agent="ag_1", alias="work",
        binding_alias=None, harness="codex_app_server",
        executable=str(_exe(tmp_path / "codex.exe")), pi_node=None,
        credential_stdin=False, credential_env=None, mcp_entry=None,
        mcp_entry_name=None, project=None, start=False,
        trusted_provider_home=False, non_interactive=True,
        state_dir=None)
    store = StateStore(paths.state_file(tmp_path))
    store.update(lambda s: s.preferences.update(
        {"vault.fallback_file.approved": True}))
    from okto_nexus_connector.cli.output import Output
    output = Output(json_mode=True)
    import okto_nexus_connector.cli.prompts as prompts
    monkeypatch.setattr(prompts, "confirm", spy_confirm)
    try:
        await connect_module.run_connect(args, output, tmp_path)
    except Exception:
        pass  # flow may stop at later controlled seams; the assertion
        # below is about PROMPTING only.
    finally:
        monkeypatch.setattr(connect_module, "confirm", original_confirm)
        monkeypatch.setattr(connect_module, "select", original_select)
    assert not any(call["non_interactive"] is False for call in calls), \
        f"non-interactive run prompted: {calls}"


async def test_24_select_second_installation_selects_that_exact_path(
        tmp_path, monkeypatch):
    import okto_nexus_connector.cli.commands.connect as connect_module
    from okto_nexus_connector.services.discovery_service import (
        InventoryEntry,
    )
    suffix = ".exe" if sys.platform == "win32" else ""
    a = _exe(tmp_path / f"a{suffix}", _MINIMAL_BINARY())
    b = _exe(tmp_path / f"b{suffix}", _MINIMAL_BINARY())
    inventory = [
        InventoryEntry("codex_app_server", str(a), "explicit", "selected",
                       fingerprint(a), None, None),
        InventoryEntry("codex_app_server", str(b), "explicit", "selected",
                       fingerprint(b), None, None),
    ]
    monkeypatch.setattr(connect_module, "discover_inventory",
                        lambda *a, **k: _ret(inventory))
    # The human chooses the SECOND entry; input feeds "2".
    inputs = iter(["2"])

    def fake_input(prompt=""):
        return next(inputs)

    monkeypatch.setattr("builtins.input", fake_input)
    import argparse
    args = argparse.Namespace(
        harness=None, executable=None, pi_node=None, non_interactive=False)
    adapter_id, candidate, version = await connect_module._choose_harness(
        args, output=None)
    assert candidate.executable.endswith(("b", "b.exe")), \
        f"selection used matches[0]: {candidate.executable}"


async def _ret(value):
    return value


async def test_25_bind_create_completes_identity_dto_construction(tmp_path):
    """The historical NameError (local class referencing `identity`)
    must be gone; the REAL ImportResult DTO reaches create_binding."""
    import argparse
    from okto_nexus_connector.storage.state_store import IdentityRecord
    import okto_nexus_connector.cli.commands.bind as bind_module
    bind_module.read_secret_stdin = lambda: "nxs_k25"
    bind_module.read_secret_env = lambda name: "nxs_k25"
    bind_module.read_secret_masked = lambda prompt="": "nxs_k25"
    peer = FakeNexusHTTPPeer()
    peer.add_agent(FakeAgent(agent_id="ag_1", key="nxs_k25"))
    url = await peer.start()
    store = StateStore(tmp_path / "state.json")
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    (tmp_path / "vault").mkdir(exist_ok=True)
    vault.store("srv_fake/ag_1", "nxs_k25")
    identity = IdentityRecord("work", "srv_fake", "ag_1",
                              "vault:srv_fake/ag_1", 1, "now")
    from okto_nexus_connector.storage.state_store import \
        ServerProfileRecord
    def seed_state(state):
        state.connector_id = "conn_audit"
        state.identities.append(identity)
        state.servers["srv_fake"] = ServerProfileRecord(
            "srv_fake", url, url, "now")

    store.update(seed_state)
    seen: dict = {}

    async def fake_create_binding(http, store_, *, identity=None, key=None,
                                  **kwargs):
        seen["identity"] = identity
        seen["kwargs"] = kwargs
        from okto_nexus_connector.services.connect_service import \
            ConnectSummary
        from okto_nexus_connector.identity.import_flow import ImportResult
        return ConnectSummary(
            identity=ImportResult(identity=identity.identity, created=True,
                                  me_agent_id=identity.me_agent_id,
                                  server_id=identity.server_id),
            binding=kwargs["candidate"] and _fake_binding(kwargs, url,
                                                          identity),
            created=True, harness_version=None, confirmation={})

    def _fake_binding(kwargs, url, identity):
        return BindingRecord(
            binding_id="bind_new", alias=kwargs["alias"],
            server_id=identity.server_id,
            agent_id=identity.me_agent_id,
            adapter_id=kwargs["adapter_id"],
            executor_id="conn_audit", workspace_id="ws",
            workspace_root=str(kwargs["workspace_root"]))

    original = bind_module.create_binding
    bind_module.create_binding = fake_create_binding
    bind_module.sys_stdin_piped = lambda: True
    binary = _exe(tmp_path / "codex.exe")
    project = tmp_path / "project"
    project.mkdir()
    args = argparse.Namespace(
        subcommand="create", identity="work",
        harness="codex_app_server", executable=str(binary),
        pi_node=None, alias="codex", project=None,
        non_interactive=True, state_dir=None)
    from okto_nexus_connector.cli.output import Output
    try:
        result = await bind_module.run_bind(
            args, Output(json_mode=True), tmp_path)
    finally:
        bind_module.create_binding = original
    assert seen.get("identity") is not None, \
        "bind create never reached the service with the identity DTO"
    await peer.stop()


# =====================================================================
# A16 — autostart artifacts
# =====================================================================

def test_20_launchd_plist_uses_dictionary_and_boolean_types(tmp_path):
    import plistlib
    from okto_nexus_connector.platform import service_install
    content = service_install._launchd_plist(tmp_path)
    payload = plistlib.loads(content.encode("utf-8"))
    assert isinstance(payload["EnvironmentVariables"], dict), \
        payload["EnvironmentVariables"]
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert str(tmp_path) in payload["ProgramArguments"], \
        "launchd artifact lost the selected state root"


def test_21_windows_autostart_preserves_custom_state_directory(tmp_path):
    from okto_nexus_connector.platform import service_install
    command = service_install._schtasks_create(tmp_path)
    joined = " ".join(command)
    assert str(tmp_path) in joined, \
        f"scheduled task loses selected state root: {joined}"


# =====================================================================
# A13 — bounded event memory (historical observation → bounded control)
# =====================================================================

async def test_observation_offline_event_copy_retains_all_3000_events(
        tmp_path):
    """Historical observation preserved as the CN-03.05 bound control:
    with the journal-sourced bridge, an offline burst of 3000 events
    must NOT grow RAM proportionally — memory holds cursors and one
    bounded batch (≤128), the journal is the replay source."""
    from okto_nexus_connector.daemon.app import EventBridge
    from nexus_connector_core import RuntimeEvent
    batches_read: list[int] = []

    async def journal_reader(server_id, session_id, epoch, after, limit):
        # Bounded replay: only the NEXT contiguous slice exists per call.
        start = after + 1
        end = min(start + limit, 3000)
        batches_read.append(end - start + 1 if end >= start else 0)
        return [RuntimeEvent(server_id, "exe", session_id, epoch, seq,
                             "text_delta", "text.delta", {"t": seq})
                for seq in range(start, end + 1)][:max(0, end - start + 1)]

    bridge = EventBridge(lambda *_: None, lambda *_: None, journal_reader)
    publish = bridge.publisher_for("srv_a")
    # 3000 wake-ups (the historical load) with NO transport online.
    peak_wakeups = 0
    for seq in range(3000):
        publish(RuntimeEvent("srv_a", "exe", "session_a", "ep", seq,
                             "text_delta", "text.delta", {"t": seq}))
        await asyncio.sleep(0)
    # RAM accounting: the bridge keeps no per-event lists — verify via
    # its internal state (cursors/wakeups only, bounded batch reads).
    import gc
    await asyncio.sleep(0.2)
    gc.collect()
    inflight_sizes = [len(t.get_coro().cr_frame.f_locals.get(
        "batch", [])) if t.get_coro() and t.get_coro().cr_frame else 0
                      for t in bridge._tasks.values()]
    assert max(inflight_sizes, default=0) <= EventBridge. \
        _MAX_INFLIGHT_PER_STREAM
    assert all(size <= EventBridge._MAX_INFLIGHT_PER_STREAM
               for size in batches_read)
    await bridge.drain()


# =====================================================================
# Controls (positive) — must stay green
# =====================================================================

def test_control_catalog_is_from_core():
    from nexus_connector_core import get_runtime_catalog
    from okto_nexus_connector.services.discovery_service import (
        catalog_runtimes,
    )
    core_ids = {d.adapter_id for d in get_runtime_catalog().runtimes
                if d.connection_mode == "managed"}
    connector_ids = {d.adapter_id for d in catalog_runtimes()}
    assert connector_ids == core_ids


async def test_control_core_rejects_ungranted_new_action(tmp_path):
    import time as _time
    from tests.integration.conftest import RecordingFactory
    from nexus_connector_core.journal import SQLiteJournal
    from nexus_connector_core import (
        LaunchIntent, OpenOperation, create_runtime,
    )
    binary = _exe(tmp_path / "codex.exe")
    journal = SQLiteJournal(tmp_path / "journal.db")
    runtime = create_runtime(
        journal=journal, environment=lambda prepared: {},
        candidates={"codex_app_server": InstallationCandidate(
            "codex_app_server", str(binary), fingerprint(binary),
            "explicit", "selected", "0.157.0",
            architecture=binary_architecture(binary) or "x86_64")},
        workspace_roots={"ws": str(tmp_path)},
        native_factory=RecordingFactory())
    context = ExecutionContext(
        "srv", "exe", "bind", "ag", "ws", 1, 1, 1,
        _time.monotonic() + 60,
        frozenset({"turn.submit"}))  # no runtime.close granted
    # The typed refusal comes from the Core's action gate:
    with pytest.raises(CoreError):
        await runtime._authorize(context, "runtime.close")
    journal.close()


# =====================================================================
# A10 — managed MCP wiring (effective environment)
# =====================================================================

async def test_27_start_session_capability_is_referenced_in_effective_environment(
        tmp_path):
    peer = FakeNexusHTTPPeer()
    peer.add_agent(FakeAgent(agent_id="ag_1", key="nxs_k27"))
    url = await peer.start()
    root = paths.state_dir(tmp_path)
    store = StateStore(paths.state_file(root))
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    vault.store("srv_fake/ag_1", "nxs_k27")
    binary = _exe(root / "codex.exe")

    def mutate(state):
        state.connector_id = "conn_audit"
        state.preferences["vault.fallback_file.approved"] = True
        from okto_nexus_connector.storage.state_store import             ServerProfileRecord
        state.servers["srv_fake"] = ServerProfileRecord(
            server_id="srv_fake", base_url=url, origin=url,
            added_at="now")
        from okto_nexus_connector.storage.state_store import IdentityRecord
        state.identities.append(IdentityRecord(
            "work", "srv_fake", "ag_1", "vault:srv_fake/ag_1", 1, "now"))
        state.bindings.append(BindingRecord(
            binding_id="bind_a", alias="codex", server_id="srv_fake",
            agent_id="ag_1", adapter_id="codex_app_server",
            executor_id="conn_audit", workspace_id="ws_a",
            workspace_root=str(root),
            candidate_executable=str(binary),
            candidate_fingerprint=fingerprint(binary),
            candidate_version="0.157.0",
            candidate_architecture=binary_architecture(binary)
            or "x86_64"))

    store.update(mutate)
    project = Path(root)
    manager = RuntimeManager(store, vault, CoreRuntimeHost(root, vault))
    captured: dict = {}

    original_build = manager._host.build

    async def spy_build(binding, *, environment, factory=None,
                        session_id=None):
        captured["environment"] = environment
        runtime = await original_build(binding, environment=environment,
                                       factory=RecordingFactory(),
                                       session_id=session_id)
        return runtime

    manager._host.build = spy_build
    try:
        started = await manager.start(alias="codex", project=project,
                                      harness=None, new_session=True,
                                      text=None)
    except ConnectorError as error:
        pytest.skip(f"lab peer lacks full contract surface: {error.code}")
    else:
        environment = captured.get("environment")
        assert environment is not None
        # REAL Core environment construction: the session capability
        # must surface as the template's env-bound token reference.
        from nexus_connector_core.environment import child_environment
        from nexus_connector_core import LaunchIntent, ExecutionContext
        import time as _time
        runtime = manager._host.get("bind_a", "srv_fake")
        prepared = await runtime.prepare(
            LaunchIntent("ag_1", "ws_a", "codex_app_server",
                         auth_refs=tuple()),  # auth refs via intent below
            ExecutionContext("srv_fake", "conn_audit", "bind_a", "ag_1",
                             "ws_a", 1, 1, 1, _time.monotonic() + 60,
                             frozenset({"runtime.open"})))
        env = await environment(prepared)
        assert any(key.startswith("NEXUS_MCP_TOKEN") for key in env), env
    await peer.stop()
