"""CN3 audit regressions — reconstructed from the reviewer package.

The package again shipped no ``regressoes/`` executables (Markdown +
RESULTADOS_RESUMO.json only); the 12 CN3 cases below are rebuilt from the
EXACT reproductions in ``01_RELATORIO_REAVALIACAO.md`` §4–10 — same node
ids and causal conditions: full pipeline with diverging
configuration/owner/connection generations (D01), foreign-namespace
reconcile disclosure (D02), admission byte leak (D03), cold-host history
(D04), duplicate-valid-ACK replay wait (D05), lane revision change during
queue (D06), ephemeral MCP home collision across namespaces (D07), plus
the three controls (valid pipeline, tampered hash refused by the codec,
own-server reconcile).

The seeds traverse codec → receiver → dispatcher → manager → Core
kernel/journal with a lab native peer at the final boundary.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(0, str(Path(__file__).parents[2] / "tests"))

from nexus_connector_core import (
    CloseOperation, CoreError, EventCursor, ExecutionContext,
    InstallationCandidate, LaunchIntent, OpenOperation, OperationReceipt,
    SessionKey, ShutdownPolicy, TurnOperation, create_runtime,
)
from nexus_connector_core.discovery import binary_architecture, fingerprint
from nexus_connector_core.frame_codec import encode_frame
from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core.protocol import (
    CONTRACT_REVISION, PROTOCOL_MAJOR, submit_frame_intent_hash,
)

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
    NXLTransport, ValidatedOperation,
)
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.conftest import RecordingFactory

TICKET = "nstkt_cn3_valid"


def _exe(path: Path, content: bytes = b"synthetic-c3") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _candidate(binary: Path):
    return InstallationCandidate(
        "codex_app_server", str(binary), fingerprint(binary), "explicit",
        "selected", "0.157.0",
        architecture=binary_architecture(binary) or "x86_64")


def _binding(root: Path, binary: Path, *, server="srv_a",
             binding_id="bind_a", agent="ag_a", workspace="ws_a",
             executor="exe_a"):
    return BindingRecord(
        binding_id=binding_id, alias=f"al-{server}", server_id=server,
        agent_id=agent, adapter_id="codex_app_server",
        executor_id=executor, workspace_id=workspace,
        workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint=fingerprint(binary),
        candidate_version="0.157.0",
        candidate_architecture=binary_architecture(binary) or "x86_64")


async def _core_session(tmp_path, *, generation=3, owner=1,
                        config=1, auth=1, server="srv_a",
                        executor="exe_a", session_id="session_a"):
    """A REAL Core session opened under the given authorized snapshot."""
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
    manager = RuntimeManager.__new__(RuntimeManager)
    manager._store = StateStore(tmp_path / "state.json")
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    (tmp_path / "vault").mkdir(exist_ok=True)
    manager._vault = vault
    manager._host = host
    manager._sessions = {}
    manager._index = {}
    manager._pumps = {}
    manager._lease_tasks = {}
    manager._draining = False
    manager._event_publisher = None
    manager._connection_generation = None
    binding = _binding(tmp_path, binary, server=server,
                       executor=executor)
    binding.authorization_revision = auth
    binding.configuration_revision = config
    context = ExecutionContext(
        server, executor, binding.binding_id, binding.agent_id,
        binding.workspace_id, auth, config, generation,
        time.monotonic() + 90,
        frozenset({"runtime.open", "turn.submit", "turn.interrupt",
                   "runtime.close"}), session_owner_generation=owner)
    prepared = await runtime.prepare(
        LaunchIntent(binding.agent_id, binding.workspace_id,
                     "codex_app_server"), context)
    await runtime.open(
        OpenOperation("op_open_d01", session_id, "ep-d01", prepared),
        context)
    session = ManagedSession(
        session_id=session_id, binding=binding, stream_epoch="ep-d01",
        opened_at=time.monotonic(), runtime=runtime,
        lease_deadline=time.monotonic() + 90,
        connection_generation=generation,
        session_owner_generation=owner,
        authorized_actions=frozenset({"turn.submit", "turn.interrupt",
                                      "runtime.close"}))
    manager._register(session)

    def _seed(state):
        state.identities.append(IdentityRecord(
            "w", server, binding.agent_id,
            f"vault:{server}/{binding.agent_id}", 1, "now"))
        state.servers[server] = ServerProfileRecord(
            server_id=server, base_url=f"https://{server}.example",
            origin=f"https://{server}.example", added_at="now")
        state.bindings.append(binding)

    (tmp_path / "vault").mkdir(exist_ok=True)
    vault.store(f"{server}/{binding.agent_id}", "nxs_cn3")
    manager._store.update(_seed)
    return manager, factory, journal


def _operation(server="srv_a", executor="exe_a", binding="bind_a",
               agent="ag_a", workspace="ws_a", session="session_a",
               operation_id="op_d01", action="turn.submit",
               generation=3, owner=1, auth_rev=1, config_rev=1,
               text="cn3", expected_turn_id=None, intent_hash=None):
    frame_payload = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "operation.submit",
        "server_id": server, "executor_id": executor,
        "binding_id": binding, "agent_id": agent,
        "workspace_id": workspace, "workspace_binding_id": "wb_a",
        "session_id": session, "operation_id": operation_id,
        "action": action, "connection_generation": generation,
        "authorization_revision": auth_rev,
        "configuration_revision": config_rev,
        "session_owner_generation": owner,
        "payload": {"text": text},
    }
    if expected_turn_id is not None:
        frame_payload["expected_turn_id"] = expected_turn_id
    frame_payload["intent_hash"] = intent_hash if intent_hash else \
        submit_frame_intent_hash(frame_payload)
    return ValidatedOperation(
        server_id=server, executor_id=executor, binding_id=binding,
        agent_id=agent, workspace_id=workspace,
        session_key=(server, executor, session), session_id=session,
        operation_id=operation_id, action=action,
        intent_hash=frame_payload["intent_hash"],
        payload={"text": text}, expected_turn_id=expected_turn_id,
        connection_generation=generation,
        authorization_revision=auth_rev,
        configuration_revision=config_rev,
        session_owner_generation=owner,
        workspace_binding_id="wb_a")


# =====================================================================
# D01 — full pipeline preserves intent and grant (4 variants)
# =====================================================================

_VARIANTS = {
    "valid": dict(),
    "hash_changed": dict(),
    "configuration_changed": dict(config_rev=99),
    "owner_changed": dict(owner=99),
    "generation_changed": dict(generation=5),
}


@pytest.mark.parametrize("variant", list(_VARIANTS))
async def test_d01_full_pipeline_preserves_intent_and_grant(
        tmp_path, variant):
    overrides = _VARIANTS[variant]
    manager, factory, journal = await _core_session(tmp_path)
    try:
        if variant == "hash_changed":
            # Reviewer control: the REAL codec refuses a tampered
            # hash on the wire, before any dispatcher.
            payload = {
                "protocol_major": PROTOCOL_MAJOR,
                "contract_revision": CONTRACT_REVISION,
                "type": "operation.submit",
                "server_id": "srv_a", "executor_id": "exe_a",
                "binding_id": "bind_a", "agent_id": "ag_a",
                "workspace_id": "ws_a",
                "workspace_binding_id": "wb_a",
                "session_id": "session_a",
                "operation_id": "op_bad",
                "action": "turn.submit",
                "connection_generation": 3,
                "authorization_revision": 1,
                "configuration_revision": 1,
                "session_owner_generation": 1,
                "payload": {"text": "tampered"},
                "intent_hash": "sha256:" + "0" * 64,
            }
            with pytest.raises(CoreError):
                encode_frame(payload)
            assert factory.native.sent == []
            return
        operation = _operation(**overrides)
        if variant == "valid":
            receipt = await manager.submit_remote(operation)
            assert receipt.stage in ("SUBMITTED", "OUTCOME_UNKNOWN")
            verbs = [verb for verb, _ in factory.native.sent]
            assert verbs == ["send_turn"], verbs
        else:
            with pytest.raises(ConnectorError) as error:
                await manager.submit_remote(operation)
            assert error.value.code in ("STALE_GENERATION",
                                        "BINDING_NOT_AUTHORIZED")
            assert factory.native.sent == [], \
                f"{variant}: envelope divergence reached the Core"
    finally:
        await manager.shutdown()
        journal.close()


# =====================================================================
# D02 — foreign reconcile does not expose another Server's receipt
# =====================================================================

async def test_d02_foreign_reconcile_does_not_expose_receipt(tmp_path):
    """Server B's channel must never read or return Server A's journal
    namespace, however valid the frame claims to be."""
    peer_b = FakeNXLPeer(server_id="srv_b", valid_tickets={TICKET})
    await peer_b.start()
    # Real journal with srv_a's durable receipt.
    manager, factory, journal = await _core_session(
        tmp_path, server="srv_a", executor="exe_a",
        session_id="session_hist")
    await manager.shutdown()
    try:
        # A transport whose AUTHENTICATED channel is srv_b receives a
        # reconcile.request claiming srv_a's namespace.
        reconciled: list[dict] = []

        async def on_reconcile(envelope):
            reconciled.append(dict(envelope))
            return None

        async def ticket():
            return TICKET

        async def on_operation(_):
            return None

        transport = NXLTransport(
            server_id="srv_b", executor_id="exe_b",
            link_url=peer_b.url, ticket_provider=ticket,
            on_operation=on_operation, on_approval=on_operation,
            on_reconcile=on_reconcile)
        transport.add_lane("bind_b", "ag_b", ticket)
        transport.start()
        await transport.wait_online(10)
        session = peer_b.latest
        from tests.fakes.wss_peer import FakeNXLPeer as _P
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and (
                transport.stats.state != "ready"):
            await asyncio.sleep(0.05)
        assert transport.stats.state == "ready"
        # Foreign reconcile.request through the real receiver.
        frame = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "reconcile.request",
            "server_id": "srv_a",       # FOREIGN to this channel
            "executor_id": "exe_a",
            "operation_ids": ["op_open_d01"],
            "session_ids": [],
        }
        await session.websocket.send(encode_frame(frame))
        await asyncio.sleep(0.6)
        # CN3-02: the foreign request was REFUSED at the channel;
        # every handler invocation carries the CHANNEL namespace
        # (handshake projections), never srv_a / the foreign id.
        assert all(entry.get("server_id") == "srv_b"
                   and entry.get("executor_id") == "exe_b"
                   for entry in reconciled), reconciled
        assert all("op_open_d01" not in
                   (entry.get("operation_ids") or [])
                   for entry in reconciled), reconciled
        assert all("srv_a" not in json.dumps(entry, default=str)
                   for entry in reconciled)
        await transport.stop()
    finally:
        journal.close()
        await peer_b.stop()


async def test_d02_control_own_server_reconcile_works(tmp_path):
    """Control (D08 in the report): the OWN namespace reconciles fine."""
    manager, factory, journal = await _core_session(
        tmp_path, server="srv_a", executor="exe_a",
        session_id="session_hist")
    try:
        report = await manager.reconcile(
            operation_ids=["op_open_d01"], session_ids=[],
            server_id="srv_a", executor_id="exe_a")
        stages = [entry["stage"] for entry in report["receipts"]]
        assert stages, "own-namespace reconcile lost the receipt"
        assert stages[0] in ("SUBMITTED", "RECEIVED_DURABLE",
                             "OUTCOME_UNKNOWN")
    finally:
        await manager.shutdown()
        journal.close()


# =====================================================================
# D03 — admission bytes released exactly
# =====================================================================

async def test_d03_admission_bytes_released_exactly(cn2_peer=None):
    """Sequenced completed operations leave ZERO residual bytes/items."""
    from okto_nexus_connector.transport.wss_client import _Admission
    admission = _Admission(items=8, max_bytes=64 * 1024)
    # Frames with DELIBERATELY different serializations (long binding
    # ids / optional fields) complete one at a time.
    for index in range(12):
        frame = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "operation.submit",
            "server_id": "srv_a",
            "executor_id": "exe_a",
            "binding_id": "bind_" + "x" * 40 + f"_{index}",
            "agent_id": "ag_a",
            "workspace_id": "ws_a",
            "workspace_binding_id": "wb_" + "y" * 50,
            "session_id": "session_a",
            "operation_id": f"op_d03_{index}",
            "action": "turn.submit",
            "connection_generation": 3,
            "authorization_revision": 1,
            "configuration_revision": 1,
            "session_owner_generation": 1,
            "payload": {"text": "t" * 30},
        }
        frame["intent_hash"] = submit_frame_intent_hash(frame)
        reservation = admission.try_reserve(frame)
        assert reservation is not None
        await asyncio.sleep(0)  # work completes before the next
        admission.release(reservation)
    assert admission.waiting_items == 0
    assert admission.waiting_bytes == 0, \
        f"residual bytes after all operations: {admission.waiting_bytes}"


async def test_d03b_double_release_is_detected():
    from okto_nexus_connector.transport.wss_client import _Admission
    admission = _Admission()
    frame = {"type": "x", "payload": "y" * 100}
    reservation = admission.try_reserve(frame)
    admission.release(reservation)
    with pytest.raises(RuntimeError):
        admission.release(reservation)


# =====================================================================
# D04 — restart reads durable receipt without prior runtime
# =====================================================================

async def test_d04_restart_reads_durable_receipt_without_prior_runtime(
        tmp_path):
    """A BRAND-NEW host process (journal closed, nothing composed)
    recovers the journal and answers the durable receipt — a cold start
    is state to RECOVER, never a false empty."""
    binary = _exe(tmp_path / "codex.exe")
    journal = SQLiteJournal(paths.journal_path(tmp_path))
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
        time.monotonic() + 120,
        frozenset({"runtime.open", "turn.submit", "runtime.close"}))
    prepared = await runtime.prepare(
        LaunchIntent("ag_a", "ws_a", "codex_app_server"), context)
    await runtime.open(
        OpenOperation("op_durable", "session_cold", "ep-c", prepared),
        context)
    await runtime.shutdown(ShutdownPolicy(2, 2))
    journal.close()  # the FIRST host dies completely.

    # Second host: same directory, NOTHING composed, journal closed.
    host2 = CoreRuntimeHost.__new__(CoreRuntimeHost)
    host2.root = tmp_path
    host2._vault = None
    host2._journal_path = paths.journal_path(tmp_path)
    host2._ledger_path = paths.owned_slot_ledger_path(tmp_path)
    host2._runtimes = {}
    host2._journal = None     # cold: not open yet
    host2._ledger = None
    host2._journal_gate = None
    host2._ledger_gate = None
    vault = RestrictedFileVault(tmp_path / "vault", approved=True)
    manager2 = RuntimeManager(
        StateStore(tmp_path / "state.json"), vault, host2)
    manager2._store.update(lambda s: s.bindings.append(
        _binding(tmp_path, binary)))
    report = await manager2.reconcile(
        operation_ids=["op_durable"], session_ids=[],
        server_id="srv_a", executor_id="exe_a")
    stages = [entry["stage"] for entry in report["receipts"]]
    assert stages and stages[0] in ("SUBMITTED", "RECEIVED_DURABLE",
                                    "OUTCOME_UNKNOWN"), \
        f"cold host returned false-empty history: {report}"
    await host2.shutdown_all()


# =====================================================================
# D05 — duplicate valid ACK satisfies the replay waiter
# =====================================================================

class _AckPeerTransport:
    def __init__(self):
        self.online = True
        self.batches: list = []
        self.acks: dict[tuple, int] = {}
        self.acked_sequences: list[tuple] = []

    async def send_events(self, batch):
        self.batches.append(list(batch))
        return True

    async def wait_event_ack(self, session_id, epoch, timeout=5.0,
                             target=None):
        watermark = self.acks.get((session_id, epoch), 0)
        if target is not None and watermark < target:
            return None
        return watermark or None


async def test_d05_duplicate_valid_ack_satisfies_replay_waiter():
    """Replaying seq1 after ACK1 was already validated must satisfy the
    wait for it (idempotent); a NEW batch to seq2 still waits for ACK2."""
    from okto_nexus_connector.transport.wss_client import (
        NXLTransport, StreamSendState,
    )
    from okto_nexus_connector.transport import wss_client as module

    transport = NXLTransport.__new__(NXLTransport)
    transport.server_id = "srv_a"
    transport.executor_id = "exe_a"
    transport._connection_serial = 1
    transport._streams = {}
    transport._ack_waiters = {}
    key = transport._stream_key("session_a", "ep-a")
    transport._streams[key] = StreamSendState(written_through=1,
                                              acked_through=1)
    # The Server repeats ACK1 (duplicate of the validated watermark).
    frame = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "event.ack",
        "server_id": "srv_a", "executor_id": "exe_a",
        "session_id": "session_a", "stream_epoch": "ep-a",
        "sequence": 1,
    }
    transport._apply_event_ack(frame)
    # D05: the replay wait for seq1 returns 1 — not a timeout.
    result = await transport.wait_event_ack(
        "session_a", "ep-a", timeout=1.0, target=1)
    assert result == 1, f"duplicate valid ACK did not satisfy: {result}"
    # Control: a wait for seq2 does NOT accept the stale ACK1.
    result2 = await transport.wait_event_ack(
        "session_a", "ep-a", timeout=0.3, target=2)
    assert result2 is None, \
        f"old watermark satisfied a newer target: {result2}"


# =====================================================================
# D06 — lane revision change invalidates the queued operation
# =====================================================================

async def test_d06_lane_revision_change_invalidates_queued_operation(
        tmp_path):
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    started: list[str] = []
    release = asyncio.Event()

    async def on_operation(operation):
        started.append(operation.operation_id)
        await release.wait()
        return None

    async def ticket():
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_a", link_url=peer.url,
        ticket_provider=ticket, on_operation=on_operation,
        on_approval=on_operation)
    transport.add_lane("bind_a", "ag_a", ticket)
    transport.start()
    await transport.wait_online(10)
    session = peer.latest
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and transport.stats.state != "ready":
        await asyncio.sleep(0.05)
    # Saturate the productive slots.
    for index in range(wss.PRODUCTIVE_CONCURRENCY):
        frame = _d06_frame(session, f"op_hold_{index}")
        await session.websocket.send(encode_frame(frame))
    await _wait_list(started, wss.PRODUCTIVE_CONCURRENCY)
    # Fifth op queues under authorization_revision 1.
    frame = _d06_frame(session, "op_victim")
    await session.websocket.send(encode_frame(frame))
    await asyncio.sleep(0.2)
    assert "op_victim" not in started
    # The lane's authorization revision ROTATES 1 → 2 while queued
    # (same transition reload/rotation uses).
    transport._lanes["bind_a"].authorization_revision = 2
    release.set()
    await asyncio.sleep(0.5)
    assert "op_victim" not in started, \
        "operation admitted under revision 1 executed after the lane " \
        "advanced to revision 2"
    await transport.stop()
    await peer.stop()


def _d06_frame(peer_session, operation_id):
    frame = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "operation.submit",
        "server_id": "srv_a", "executor_id": "exe_a",
        "binding_id": "bind_a", "agent_id": "ag_a",
        "workspace_id": "ws_a", "workspace_binding_id": "wb_a",
        "session_id": "session_a", "operation_id": operation_id,
        "action": "turn.submit",
        "connection_generation": peer_session.connection_generation,
        "authorization_revision": 1,
        "configuration_revision": 1,
        "session_owner_generation": 1,
        "payload": {"text": "d06"},
    }
    frame["intent_hash"] = submit_frame_intent_hash(frame)
    return frame


# =====================================================================
# D07 — ephemeral MCP home is scoped by server and binding
# =====================================================================

async def test_d07_ephemeral_mcp_home_is_scoped_by_server_and_binding(
        tmp_path):
    """Two bindings from DIFFERENT Servers with the SAME textual
    session_id get SEPARATE config trees; the first stays intact after
    the second is created."""
    from nexus_connector_core.harness_config import harness_http_template
    from okto_nexus_connector.services.runtime_service import (
        RuntimeManager,
    )
    root = paths.state_dir(tmp_path)
    binary = _exe(root / "codex.exe")
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    store = StateStore(paths.state_file(root))

    def seed(state):
        state.connector_id = "exe_host"
        state.preferences["vault.fallback_file.approved"] = True
        for server, url in (("srv_a", "https://a.example"),
                            ("srv_b", "https://b.example")):
            state.servers[server] = ServerProfileRecord(
                server_id=server, base_url=url, origin=url,
                added_at="now")
        state.bindings.append(_binding(root, binary, server="srv_a",
                                       executor="exe_host"))
        state.bindings.append(_binding(root, binary, server="srv_b",
                                       executor="exe_host"))

    store.update(seed)
    manager = RuntimeManager(store, vault, CoreRuntimeHost(root, vault))
    manager._vault = vault

    def template_for(server, url):
        return harness_http_template(
            "codex_app_server", url + "/mcp", "mcp-cap:same_session",
            entry_name="nexus", harness_is_local=False,
            approved_origins={url}, format_qualified=True)

    home_a = manager._mcp_session_home(
        _binding(root, binary, server="srv_a", executor="exe_host"),
        "same_session", [template_for("srv_a", "https://a.example")])
    config_a = (home_a / ".codex" / "config.toml").read_text("utf-8")
    assert "https://a.example/mcp" in config_a
    home_b = manager._mcp_session_home(
        _binding(root, binary, server="srv_b", executor="exe_host"),
        "same_session", [template_for("srv_b", "https://b.example")])
    config_b = (home_b / ".codex" / "config.toml").read_text("utf-8")
    assert "https://b.example/mcp" in config_b
    # D07: A's config is UNCHANGED after B's creation; the trees are
    # distinct and namespaced.
    assert home_a != home_b
    assert (home_a / ".codex" / "config.toml").read_text("utf-8") == \
        config_a, "namespace B overwrote namespace A's MCP config"
    assert "srv_a" in str(home_a) and "srv_b" in str(home_b)


# =====================================================================
# Controls already passing (preserved from the campaign)
# =====================================================================

async def test_control_codec_refuses_tampered_hash(tmp_path):
    """ACN3-E02 control: tampered hash is refused by the REAL codec
    before the dispatcher - zero native writes, no manager dispatch."""
    payload = {
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "operation.submit",
        "server_id": "srv_a", "executor_id": "exe_a",
        "binding_id": "bind_a", "agent_id": "ag_a",
        "workspace_id": "ws_a", "workspace_binding_id": "wb_a",
        "session_id": "session_a", "operation_id": "op_ctrl",
        "action": "turn.submit", "connection_generation": 3,
        "authorization_revision": 1, "configuration_revision": 1,
        "session_owner_generation": 1,
        "payload": {"text": "x"},
        "intent_hash": "sha256:" + "0" * 64,
    }
    with pytest.raises(CoreError):
        encode_frame(payload)


async def _wait_list(items, count, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(items) >= count:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"only {len(items)}/{count} in time")
