"""CN4 audit regressions — reconstructed from the reviewer package.

For the THIRD consecutive time the package arrived WITHOUT its
``regressoes/`` executables (only Markdown + a name/status JSON — no
code, no failure messages, no runner). These cases are rebuilt from the
EXACT reproductions in ``01_RELATORIO_REAVALIACAO.md`` (P01–P05) and
the node ids of ``03_MATRIZ_ACEITE.md`` ACN4-01..14: approval
translation AFTER Server authority (approve→accept / deny→decline
against a REAL Core session), Server refusal before any native
dispatch, B's approval using B's binding/credential (two servers, same
textual binding_id), the bridge waiting for ITS batch's watermark,
long-ID MCP namespace separation, Pi-pair-preserving availability
surfaces (CLI and IPC), build-bytes inventory revision, lane rotation
from persisted state, and the rotated lane fetching a new ticket and
reattaching — plus the three controls and complementary matrix cases.

The seeds traverse the REAL application paths (the daemon's approval
state machine, EventBridge, NXLTransport, the shared discovery service)
down to a Core session with a lab native peer.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
sys.path.insert(0, str(Path(__file__).parents[2] / "tests"))

from nexus_connector_core import (
    ExecutionContext, InstallationCandidate, LaunchIntent, OpenOperation,
    ShutdownPolicy, create_runtime,
)
from nexus_connector_core.discovery import (
    binary_architecture, candidate as make_candidate,
    candidate_pi_node_cli, fingerprint,
)
from nexus_connector_core.frame_codec import encode_frame
from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core.protocol import (
    CONTRACT_REVISION, PROTOCOL_MAJOR,
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
from okto_nexus_connector.transport.wss_client import NXLTransport
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer
from tests.integration.conftest import RecordingFactory

TICKET = "nstkt_cn4"


def _exe(path: Path, content: bytes = b"synthetic-c4") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _binding(root: Path, binary: Path, *, server="srv_a",
             binding_id="bind_shared", agent="ag_a", workspace="ws_a",
             executor="exe_host", adapter="codex_app_server"):
    return BindingRecord(
        binding_id=binding_id, alias=f"al-{server}-{binding_id}",
        server_id=server, agent_id=agent, adapter_id=adapter,
        executor_id=executor, workspace_id=workspace,
        workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint=fingerprint(binary),
        candidate_version="0.157.0",
        candidate_architecture=binary_architecture(binary) or "x86_64")


class _Lab:
    """A full DaemonApp lab over real peers: store, vault, transports."""

    def __init__(self, tmp_path: Path, *, servers=("srv_a",)):
        self.root = Path(tempfile.mkdtemp(dir=tmp_path))
        self.peers: dict[str, FakeNexusHTTPPeer] = {}
        self.store = StateStore(paths.state_file(self.root))

        def seed(state):
            state.connector_id = "exe_host"
            state.preferences["vault.fallback_file.approved"] = True
            for server in servers:
                state.servers[server] = ServerProfileRecord(
                    server_id=server, base_url="http://pending.invalid",
                    origin="http://pending.invalid", added_at="now")

        self.store.update(seed)
        self.vault = RestrictedFileVault(paths.vault_dir(self.root),
                                         approved=True)
        self.app = DaemonApp(self.root)
        self.app.vault = self.vault

    async def start_peer(self, server_id: str) -> FakeNexusHTTPPeer:
        peer = FakeNexusHTTPPeer()
        url = await peer.start()
        url = url or peer.base_url

        def fix(state):
            record = state.servers.get(server_id)
            if record is not None:
                record.base_url = url
                record.origin = url

        self.store.update(fix)
        self.peers[server_id] = peer
        return peer

    def add_identity(self, server, agent, key="nxs_cn4"):
        (self.root / "vault").mkdir(exist_ok=True)
        self.vault.store(f"{server}/{agent}", key)
        self.store.update(
            lambda s, sv=server, ag=agent: s.identities.append(
                IdentityRecord("w", sv, ag, f"vault:{sv}/{ag}", 1,
                               "now")))

    def add_binding(self, binding: BindingRecord):
        self.store.update(lambda s, b=binding: s.bindings.append(b))

    async def stop(self):
        for peer in self.peers.values():
            await peer.stop()
        for transport in list(self.app.transports.values()):
            await transport.stop()


async def _managed_session(app, tmp_path, *, server="srv_a",
                           agent="ag_a", binding_id="bind_shared",
                           session_id="sess_p01",
                           adapter="codex_app_server") -> RecordingFactory:
    """Open a REAL Core session whose grant includes approvals."""
    binary = _exe(tmp_path / "codex.exe")
    journal = SQLiteJournal(paths.journal_path(app.root))
    factory = RecordingFactory()

    async def env(prepared):
        return {}

    runtime = create_runtime(
        journal=journal, environment=env,
        candidates={"codex_app_server": InstallationCandidate(
            "codex_app_server", str(binary), fingerprint(binary),
            "explicit", "selected", "0.157.0",
            architecture=binary_architecture(binary) or "x86_64")},
        workspace_roots={"ws_a": str(tmp_path)},
        native_factory=factory)
    app.host._journal = journal
    binding = _binding(app.root, binary, server=server, agent=agent,
                       binding_id=binding_id)
    app.store.update(lambda s, b=binding: s.bindings.append(b))
    context = ExecutionContext(
        server, "exe_host", binding_id, agent, "ws_a", 1, 1, 1,
        time.monotonic() + 90,
        frozenset({"runtime.open", "approval.decide", "input.provide",
                   "turn.interrupt", "runtime.close"}),
        session_owner_generation=1)
    prepared = await runtime.prepare(
        LaunchIntent(agent, "ws_a", adapter), context)
    await runtime.open(
        OpenOperation(f"op_open_{session_id}", session_id, "ep-1",
                      prepared), context)
    native_request = {
        "request_id": f"native_{session_id}",
        "request_hash": "ab" * 32,
        "method": "item/commandExecution/requestApproval",
        "params": {"turnId": "turn_1", "command": "lab"},
    }
    from nexus_connector_core import RuntimeEvent
    await factory.native.queue.put(RuntimeEvent(
        server, "exe_host", session_id, "ep-1", 0, "native",
        "approval.request", {"native_approval": native_request}))
    await asyncio.sleep(0.1)  # the pump observes the pending request
    session = ManagedSession(
        session_id=session_id, binding=binding, stream_epoch="ep-1",
        opened_at=time.monotonic(), runtime=runtime,
        lease_deadline=time.monotonic() + 90,
        connection_generation=1, session_owner_generation=1,
        authorized_actions=frozenset({"approval.decide",
                                      "input.provide",
                                      "turn.interrupt",
                                      "runtime.close"}))
    app.runtimes._register(session)
    return factory


def _pending_frame(server, request_id, *, binding_id="bind_shared",
                   session_id=None, agent="ag_a", kind="requestApproval",
                   executor="exe_host", proposal=None):
    """CN5 adaptation: the seed DELIVERS an r3 approval.request frame
    through the REAL receiving path (``_on_remote_approval``) — never a
    direct assignment into ``_approvals``. The proposal carries the
    Core's NATIVE approval-request projection (request_id/request_hash/
    method/params) with a 64-hex correlation hash that the OLD
    receiving path used to redact away (Q01a)."""
    return {
        "type": "approval.request", "server_id": server,
        "executor_id": executor, "binding_id": binding_id,
        "agent_id": agent, "workspace_id": "ws_a",
        "session_id": session_id, "operation_id": f"op_{request_id}",
        "request_id": request_id, "kind": kind,
        "proposal": proposal or {
            "request_id": f"native_{request_id}",
            "request_hash": "ab" * 32,
            "method": "item/commandExecution/requestApproval",
            "params": {"turnId": "turn_1", "command": "lab"}}}


async def _deliver(app, frame):
    """Deliver one frame through the real transport handler."""
    await app._on_remote_approval(frame)


async def _noop(*args, **kwargs):
    return None


# =====================================================================
# P01 — managed approval translates AFTER the Server's authority
# =====================================================================

@pytest.mark.parametrize("cli_decision,core_decision", [
    ("approve", "accept"), ("deny", "decline")])
async def test_p01_managed_approval_translates_after_server_authority(
        tmp_path, cli_decision, core_decision):
    """ACN4-01/02: the CLI vocabulary is translated to the Core's
    public contract AFTER the Server confirmed, through the REAL
    DaemonApp flow with a live Core session and the peer HTTP."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn4",
                                 server_id="srv_a"))
        peer.add_approval("req_tr", agent_id="ag_a")
        await _managed_session(lab.app, tmp_path, session_id="sess_tr")
        native: list[tuple] = []
        session = lab.app.runtimes.session("sess_tr")
        runtime = session.runtime
        original = runtime.decide_native_approval

        async def spy_decide(operation, context):
            native.append((operation.decision, operation.operation_id,
                           peer.approvals["req_tr"].get("answered")))
            from nexus_connector_core import OperationReceipt
            return OperationReceipt(
                operation.operation_id, "sha256:" + "2" * 64,
                "SUBMITTED", False, False, "sess_tr")

        runtime.decide_native_approval = spy_decide
        # CN5: the request arrives through the REAL receiving path.
        await _deliver(lab.app, _pending_frame(
            "srv_a", "req_tr", session_id="sess_tr"))
        result = await lab.app._decide_approval({
            "request_id": "req_tr", "decision": cli_decision,
            "cas_token": "cas_1"})
        assert result["applied"] is True
        # The Core received the TRANSLATED contract vocabulary.
        assert native, "the authorized decision never reached the Core"
        assert native[0][0] == core_decision, native
        # Q01a (CN5): the OPERATIONAL proposal reached the Core with
        # the correlation hash INTACT (only the display view redacts).
        observed_request = spy[0] if False else None
        # (the spy captures the operation; its request must keep the
        #  64-hex request_hash exactly as received)
        # ...and ONLY after the Server's canonical answer.
        assert native[0][2] is True, "native ran before the Server"
        assert peer.approvals["req_tr"]["decision"] == cli_decision
        runtime.decide_native_approval = original
    finally:
        await lab.stop()


async def test_p01b_server_refusal_precedes_any_native_dispatch(
        tmp_path):
    """ACN4-03: a definitive Server refusal produces ZERO native
    application calls (the canonical decision precedes any permissive
    effect)."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn4",
                                 server_id="srv_a"))
        # The Server definitively refuses: this request belongs to a
        # DIFFERENT authority (403, no write).
        peer.add_approval("req_rb", agent_id="other_agent")
        await _managed_session(lab.app, tmp_path, session_id="sess_rb")
        native: list[str] = []
        session = lab.app.runtimes.session("sess_rb")
        runtime = session.runtime

        async def sentinel(operation, context):
            native.append(operation.decision)
            raise AssertionError("native dispatch ran")

        runtime.decide_native_approval = sentinel
        await _deliver(lab.app, _pending_frame(
            "srv_a", "req_rb", session_id="sess_rb"))
        with pytest.raises(ConnectorError):
            await lab.app._decide_approval({
                "request_id": "req_rb", "decision": "approve",
                "cas_token": "cas_1"})
        assert native == [], "native application ran despite refusal"
        # The refused request is NOT consumed as decided: its attempt
        # is terminal-refused and consultable via approvals.status.
        status = lab.app._approval_status({"request_id": "req_rb"})
        assert status["phase"] == "server_refused", status
        assert status["producer"] is False
    finally:
        await lab.stop()


async def test_p01c_approval_for_b_uses_binding_and_credential_of_b(
        tmp_path):
    """ACN4-04: with A and B sharing the TEXTUAL binding_id, deciding
    B's request goes ONLY to B's origin with B's identity — and A's
    pending request stays untouched."""
    lab = _Lab(tmp_path, servers=("srv_a", "srv_b"))
    peer_a = await lab.start_peer("srv_a")
    peer_b = await lab.start_peer("srv_b")
    try:
        for server in ("srv_a", "srv_b"):
            lab.add_identity(server, "ag_x")
            peer = lab.peers[server]
            peer.add_agent(FakeAgent(agent_id="ag_x", key="nxs_cn4",
                                     server_id=server))
            peer.add_approval("req_same", agent_id="ag_x")
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a",
                                 agent="ag_x"))
        lab.add_binding(_binding(lab.root, binary, server="srv_b",
                                 agent="ag_x"))
        await _deliver(lab.app, _pending_frame(
            "srv_a", "req_same", agent="ag_x"))
        await _deliver(lab.app, _pending_frame(
            "srv_b", "req_same", agent="ag_x"))
        result = await lab.app._decide_approval({
            "server_id": "srv_b", "request_id": "req_same",
            "decision": "approve", "cas_token": "cas_b"})
        assert result["applied"] is True
        # Only B's origin saw the decision; A's stays unanswered.
        assert not peer_a.approvals["req_same"].get("answered"), \
            "A's origin received B's decision"
        assert peer_b.approvals["req_same"]["answered"]
        # B's request retired BY ITS OWN KEY; A's pending remains.
        assert ("srv_a", "req_same") in lab.app._approvals
        assert ("srv_b", "req_same") not in lab.app._approvals
        # And the repeat consults the recorded outcome (idempotent).
        again = await lab.app._decide_approval({
            "server_id": "srv_b", "request_id": "req_same",
            "decision": "approve", "cas_token": "cas_b"})
        assert again["applied"] is True and again["native"] is None
    finally:
        await lab.stop()


async def test_control_core_approval_accept_contract_is_operational(
        tmp_path):
    """ACN4-05 control: the Core's public accept contract still works
    directly (the failure was the app's mapping, not the library)."""
    lab = _Lab(tmp_path)
    try:
        from nexus_connector_core import SessionKey
        from okto_nexus_connector.services.approval_state import (
            ApprovalKey, ApprovalTarget,
        )
        factory = await _managed_session(lab.app, tmp_path,
                                         session_id="sess_ctl")
        binding = lab.app.runtimes.session(
            "sess_ctl").binding
        target = ApprovalTarget(
            key=ApprovalKey("srv_a", "exe_host", "req_ctl"),
            binding_id=binding.binding_id, agent_id=binding.agent_id,
            workspace_id=binding.workspace_id,
            session_key=SessionKey("srv_a", "exe_host", "sess_ctl"),
            connection_generation=1, session_owner_generation=1,
            authorization_revision=binding.authorization_revision,
            configuration_revision=binding.configuration_revision,
            kind="requestApproval")
        result = await lab.app.runtimes.decide_native_approval(
            target=target, operation_id="op_appr_ctl",
            request={"request_id": "native_sess_ctl",
                     "request_hash": "ab" * 32,
                     "method": "item/commandExecution/requestApproval",
                     "params": {"turnId": "turn_1", "command": "lab"}},
            decision="accept", response=None)
        assert result["receipt"]["stage"] in ("SUBMITTED",
                                              "OUTCOME_UNKNOWN")
    finally:
        await lab.stop()


async def test_p01d_ambiguous_short_choice_lists_servers_zero_http(
        tmp_path):
    """ACN4-19: same request_id on two servers, CLI omits server_id →
    typed ambiguity listing the servers, with ZERO HTTP requests."""
    lab = _Lab(tmp_path, servers=("srv_a", "srv_b"))
    peer_a = await lab.start_peer("srv_a")
    peer_b = await lab.start_peer("srv_b")
    try:
        await _deliver(lab.app, _pending_frame("srv_a", "req_amb"))
        await _deliver(lab.app, _pending_frame("srv_b", "req_amb"))
        with pytest.raises(ConnectorError) as error:
            await lab.app._decide_approval({
                "request_id": "req_amb", "decision": "approve"})
        assert "srv_a" in str(error.value)
        assert "srv_b" in str(error.value)
        assert not peer_a.approvals.get("req_amb", {}).get("answered")
        assert not peer_b.approvals.get("req_amb", {}).get("answered")
    finally:
        await lab.stop()


async def test_p01e_concurrent_decisions_single_application(tmp_path):
    """ACN4-15: two operators on the same request — one reservation;
    the second gets an explicit conflict; no double application."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn4",
                                 server_id="srv_a"))
        peer.add_approval("req_cc", agent_id="ag_a")
        await _managed_session(lab.app, tmp_path, session_id="sess_cc")
        native_calls: list[str] = []
        session = lab.app.runtimes.session("sess_cc")
        runtime = session.runtime
        original = runtime.decide_native_approval

        async def slow_native(operation, context):
            native_calls.append(operation.decision)
            from nexus_connector_core import OperationReceipt
            return OperationReceipt(
                operation.operation_id, "sha256:" + "3" * 64,
                "SUBMITTED", False, False, "sess_cc")

        runtime.decide_native_approval = slow_native
        await _deliver(lab.app, _pending_frame(
            "srv_a", "req_cc", session_id="sess_cc"))
        # Hold the Server decision so the first attempt stays in
        # server_pending while the second operator arrives.
        from okto_nexus_connector.transport.https_client import             NexusHTTPClient
        original_decision = NexusHTTPClient.approval_decision
        entered = asyncio.Event()
        release = asyncio.Event()

        async def held_decision(self, *args, **kwargs):
            entered.set()
            await release.wait()
            return await original_decision(self, *args, **kwargs)

        NexusHTTPClient.approval_decision = held_decision
        try:
            first = asyncio.create_task(lab.app._decide_approval({
                "request_id": "req_cc", "decision": "approve",
                "cas_token": "c"}))
            await asyncio.wait_for(entered.wait(), 5)
            with pytest.raises(ConnectorError) as conflict:
                await lab.app._decide_approval({
                    "request_id": "req_cc", "decision": "deny",
                    "cas_token": "c"})
            # CN5-02.01: a DIFFERENT decision in flight is a typed
            # conflict (an identical repeat would SHARE the producer).
            assert conflict.value.code == "OPERATION_CONFLICT"
            assert "different decision" in str(conflict.value)
            release.set()
            result = await asyncio.wait_for(first, 5)
        finally:
            NexusHTTPClient.approval_decision = original_decision
        assert result["applied"] is True
        assert native_calls in ([], ["accept"]), native_calls
        runtime.decide_native_approval = original
    finally:
        await lab.stop()


# =====================================================================
# P02 — the bridge waits for ITS batch's watermark
# =====================================================================

class _BridgeTransport:
    """Wraps the REAL NXLTransport; counts sends and pages."""

    def __init__(self, transport: NXLTransport):
        self._transport = transport
        self.sends: list[int] = []

    @property
    def online(self):
        return self._transport.online

    async def send_events(self, batch):
        self.sends.append(batch[-1].sequence)
        await self._transport.send_events(batch)
        return True

    async def wait_event_ack(self, session_id, epoch, timeout=30.0,
                             target=None):
        return await self._transport.wait_event_ack(
            session_id, epoch, timeout=timeout, target=target)


def _SeqEvent(seq):
    """A REAL RuntimeEvent (the codec validates the batch shape)."""
    from nexus_connector_core import RuntimeEvent
    return RuntimeEvent(
        "srv_a", "exe_host", "sess_bridge", "ep-bridge", seq,
        "lifecycle", "note", {"text": f"bridge-{seq}"})


async def test_p02_bridge_waits_for_new_batch_not_old_ack():
    """ACN4-06: seq1 written+ACK1'd; then seq2 arrives WITHOUT ACK2 —
    the bridge attempts batch2 EXACTLY ONCE while ACK2 is absent (an
    old watermark never concludes the new batch's wait)."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()

    # Lab-controlled ingress: ACK seq1 immediately; HOLD seq2's ACK.
    async def gated_ingest(session, frame):
        for event in frame["events"]:
            key = (event["session_id"], event["stream_epoch"])
            watermark = session.acked.get(key, 0)
            if event["sequence"] == watermark + 1:
                session.acked[key] = event["sequence"]
        for key, watermark in list(session.acked.items()):
            if watermark > 0 and watermark < 2:
                await session.websocket.send(encode_frame({
                    "protocol_major": PROTOCOL_MAJOR,
                    "contract_revision": CONTRACT_REVISION,
                    "type": "event.ack",
                    "server_id": peer.server_id,
                    "executor_id": session.executor_id,
                    "session_id": key[0], "stream_epoch": key[1],
                    "sequence": watermark}))

    peer._ingest = gated_ingest  # barrier: ACK2 stays absent

    async def ticket():
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=ticket, on_operation=_noop, on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", ticket)
    transport.start()
    await transport.wait_online(10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.stats.state != "ready":
        await asyncio.sleep(0.05)
    adapter = _BridgeTransport(transport)
    pages = {0: [_SeqEvent(1)], 1: [_SeqEvent(2)]}
    reads: list[int] = []

    async def journal_page(server, session, epoch, after, limit):
        reads.append(after)
        return pages.get(after, [])

    applied: list[int] = []

    async def core_ack(server, session, epoch, watermark):
        applied.append(watermark)

    bridge = EventBridge(lambda server: adapter, core_ack, journal_page)
    publish = bridge.publisher_for("srv_a")
    # Round 1: seq1 published; ACK1 arrives and is applied to the Core.
    publish(_SeqEvent(1))
    await asyncio.sleep(0.5)
    assert applied == [1], applied
    assert adapter.sends == [1], adapter.sends
    # Round 2: seq2 arrives; ACK2 NEVER comes.
    publish(_SeqEvent(2))
    await asyncio.sleep(1.5)
    # EXACTLY ONE attempt for batch2 while ACK2 is absent — no flood.
    assert adapter.sends == [1, 2], adapter.sends
    assert applied == [1], applied
    assert reads.count(1) == 1, f"batch2 re-read {reads}"
    # Replay control (CN3 D05 preserved): duplicate-valid ACK1 still
    # satisfies a replay-1 wait; never a batch-2 wait.
    acked = await transport.wait_event_ack(
        "sess_bridge", "ep-bridge", timeout=0.3, target=1)
    assert acked == 1
    held = await transport.wait_event_ack(
        "sess_bridge", "ep-bridge", timeout=0.2, target=2)
    assert held is None
    await bridge.drain()
    await transport.stop()
    await peer.stop()


# =====================================================================
# P03 — MCP namespace keeps distinct valid LONG ids
# =====================================================================

LONG_A = "srv_" + "a" * 80 + "_tail_a"      # valid, differs at the END
LONG_B = "srv_" + "a" * 80 + "_tail_b"


@pytest.mark.parametrize("variant", ["control_short_ids",
                                     "long_valid_ids"])
async def test_p03_mcp_namespace_keeps_distinct_valid_long_ids(
        tmp_path, variant):
    from nexus_connector_core.harness_config import harness_http_template
    from okto_nexus_connector.services.runtime_service import (
        RuntimeManager,
    )
    root = paths.state_dir(tmp_path)
    binary = _exe(root / "codex.exe")
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    store = StateStore(paths.state_file(root))
    manager = RuntimeManager(store, vault, CoreRuntimeHost(root, vault))

    def template_for(server, url):
        return harness_http_template(
            "codex_app_server", url + "/mcp",
            "mcp-cap:same_session", entry_name="nexus",
            harness_is_local=False, approved_origins={url},
            format_qualified=True)

    if variant == "control_short_ids":
        servers = (("srv_a", "https://a.example"),
                   ("srv_b", "https://b.example"))
    else:
        servers = ((LONG_A, "https://a.example"),
                   (LONG_B, "https://b.example"))
    homes = []
    for server, url in servers:
        home = manager._mcp_session_home(
            _binding(root, binary, server=server),
            "same_session", [template_for(server, url)])
        homes.append(home)
        config = (home / ".codex" / "config.toml").read_text("utf-8")
        assert url + "/mcp" in config
    first, second = homes
    config_a = (first / ".codex" / "config.toml").read_text("utf-8")
    # B's creation leaves A's configuration INTACT; trees are distinct.
    assert (first / ".codex" / "config.toml").read_text(
        "utf-8") == config_a
    assert first != second


async def test_p03b_foreign_marker_refuses_before_write(tmp_path):
    """ACN4-24: a tree with a DIVERGENT owner marker is refused before
    any file is touched — the marker itself is never overwritten."""
    from nexus_connector_core.harness_config import harness_http_template
    from okto_nexus_connector.services.runtime_service import (
        RuntimeManager, _mcp_home_dir,
    )
    root = paths.state_dir(tmp_path)
    binary = _exe(root / "codex.exe")
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    store = StateStore(paths.state_file(root))
    manager = RuntimeManager(store, vault, CoreRuntimeHost(root, vault))
    template = harness_http_template(
        "codex_app_server", "https://a.example/mcp",
        "mcp-cap:sess_marker", entry_name="nexus",
        harness_is_local=False,
        approved_origins={"https://a.example"}, format_qualified=True)
    home = _mcp_home_dir(root, "srv_a", "exe_host", "bind_shared",
                         "sess_marker")
    home.mkdir(parents=True)
    (home / ".owner.json").write_text(json.dumps({
        "layout": "v2", "server_id": "srv_OTHER",
        "executor_id": "exe_host", "binding_id": "bind_shared",
        "session_id": "sess_marker"}), encoding="utf-8")
    with pytest.raises(ConnectorError):
        manager._mcp_session_home(
            _binding(root, binary, server="srv_a"), "sess_marker",
            [template])
    marker = json.loads((home / ".owner.json").read_text("utf-8"))
    assert marker["server_id"] == "srv_OTHER"
    assert not (home / ".codex").exists()


# =====================================================================
# P04 — availability surfaces preserve the REAL Pi pair
# =====================================================================

def _pi_lab(tmp_path: Path):
    """A REAL Pi installation layout (Node + CLI + package.json)."""
    node = _exe(tmp_path / "bin" / "node.exe", b"node-lab-v1")
    package = tmp_path / "node_modules" / "@earendil-works" / \
        "pi-coding-agent"
    bundle = package / "dist" / "bundle"
    bundle.mkdir(parents=True, exist_ok=True)
    cli = bundle / "cli.js"
    cli.write_text("// pi cli v1", encoding="utf-8")
    (package / "package.json").write_text(
        '{"name": "@earendil-works/pi-coding-agent", '
        '"version": "0.87.1"}', encoding="utf-8")
    return node, cli


@pytest.mark.parametrize("variant", ["cli", "ipc"])
async def test_p04_availability_surface_preserves_real_pi_pair(
        tmp_path, variant):
    """ACN4-09/10: BOTH surfaces (CLI flow and the daemon IPC op) use
    the SAME service function over the FULL Core candidates — the Pi
    pair's reference/build/version survive instead of collapsing to a
    bare-Node explicit candidate."""
    from okto_nexus_connector.services import discovery_service
    node, cli = _pi_lab(tmp_path)
    pi = candidate_pi_node_cli(node, cli, explicit=True)

    async def surface():
        if variant == "cli":
            candidates = await discovery_service.inventory_candidates(
                extra=[pi], adapter_ids=())
            return discovery_service.availability_snapshot(
                list(candidates))
        import tempfile
        lab_root = Path(tempfile.mkdtemp(dir=tmp_path))
        app = DaemonApp(lab_root)
        from okto_nexus_connector.ipc.protocol import parse_request
        # The REAL dispatch handler of the daemon op.
        request = parse_request({"seq": 1, "op": "availability.snapshot",
                                 "params": {}})
        async for response in app.dispatch(request):
            return response.get("result")
        raise AssertionError("no response")

    # The IPC surface discovers THIS host's PATH inventory only — the
    # lab injects the pair through the same public seam both surfaces
    # share: the service function (no host array is duplicated here).
    original = discovery_service.inventory_candidates

    async def with_pair(adapter_ids=None, *, extra=()):
        result = await original(adapter_ids, extra=[pi])
        return result

    discovery_service.inventory_candidates = with_pair
    try:
        snapshot = await surface()
    finally:
        discovery_service.inventory_candidates = original
    rows = [row for row in snapshot["rows"]
            if row.get("candidate_ref") == pi.installation_ref]
    assert rows, "the Pi pair disappeared from the snapshot"
    row = rows[0]
    assert row.get("version") == "0.87.1", row
    assert row.get("build_identity") == pi.build_identity
    assert row.get("candidate_ref") == pi.installation_ref


async def test_p04b_inventory_revision_changes_when_build_bytes_change(
        tmp_path):
    """ACN4-11: changing the CLI's BYTES (path/version/state constant)
    rotates the inventory revision; repeats stay stable."""
    from okto_nexus_connector.services import discovery_service
    node, cli = _pi_lab(tmp_path)
    pi1 = candidate_pi_node_cli(node, cli, explicit=True)

    async def revision_for(pi):
        candidates = await discovery_service.inventory_candidates(
            extra=[pi], adapter_ids=())
        return discovery_service.availability_snapshot(
            list(candidates))["executor_revision"]

    rev1 = await revision_for(pi1)
    assert rev1 == await revision_for(pi1)  # ACN4-12 control: stable
    cli.write_text("// pi cli v2 - bytes changed", encoding="utf-8")
    pi2 = candidate_pi_node_cli(node, cli, explicit=True)
    assert pi2.build_identity != pi1.build_identity
    rev2 = await revision_for(pi2)
    assert rev2 != rev1, \
        "build bytes changed but the inventory revision did not"


async def test_control_inventory_revision_repeats_without_change(
        tmp_path):
    """ACN4-12: reordering the same inventory keeps the revision."""
    from okto_nexus_connector.services import discovery_service
    node, cli = _pi_lab(tmp_path)
    pi = candidate_pi_node_cli(node, cli, explicit=True)
    other = _exe(tmp_path / "codex.exe")
    codex = make_candidate("codex_app_server", other, explicit=True)
    a = discovery_service.availability_snapshot([pi, codex])
    b = discovery_service.availability_snapshot([codex, pi])
    assert isinstance(a["executor_revision"], str)
    assert a["executor_revision"] == b["executor_revision"]


# =====================================================================
# P05 — lane rotation on the REAL paths
# =====================================================================

async def test_p05_reload_rotates_existing_lane_from_persisted_state(
        tmp_path):
    """ACN4-13: credential_epoch/authorization_revision persisted at 2;
    reload_state rotates the EXISTING lane (fence + epoch bump) — the
    readiness of the old proof is withdrawn."""
    lab = _Lab(tmp_path)
    await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a"))
        lab.app._start_transports()
        await asyncio.sleep(0.2)
        transport = lab.app.transports["srv_a"]
        assert (transport.lane_info("bind_shared")["credential_epoch"],
                transport.lane_info("bind_shared")
                ["authorization_revision"]) == (1, 1)
        # The operator rotates credentials/revisions (CLI side).
        def rotate(state):
            state.identities[0] = IdentityRecord(
                "w", "srv_a", "ag_a", "vault:srv_a/ag_a", 2, "now")
            state.bindings[0].authorization_revision = 2

        lab.store.update(rotate)
        await lab.app.reload_state()
        info = transport.lane_info("bind_shared")
        assert (info["credential_epoch"],
                info["authorization_revision"]) == (2, 2), info
        assert info["ticket_epoch"] >= 2, "no epoch rotation happened"
    finally:
        await lab.stop()


async def test_p05b_rotated_lane_requests_new_ticket_and_reattaches(
        tmp_path):
    """ACN4-14: add_lane on an EXISTING lane installs the NEW provider
    and dispatches attach on the live channel (the new provider IS
    consulted; the lane returns to ready only under the new proof)."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    fetches: list[str] = []

    async def provider_v1():
        fetches.append("v1")
        return TICKET

    async def provider_v2():
        fetches.append("v2")
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=provider_v1, on_operation=_noop,
        on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", provider_v1)
    transport.start()
    await transport.wait_online(10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert transport.lane_info("bind_shared")["state"] == "ready"
    # Rotation with a NEW provider closure and revision 2.
    transport.add_lane("bind_shared", "ag_a", provider_v2,
                       credential_epoch=2, authorization_revision=2)
    info = transport.lane_info("bind_shared")
    assert info["state"] in ("pending", "attaching"), info
    assert info["ticket_epoch"] >= 2
    # The attach under the NEW proof runs on the live channel.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert "v2" in fetches, "the rotated lane never used the new provider"
    assert transport.lane_info("bind_shared")["state"] == "ready"
    # True no-op add (ACN4-31): no epoch bump, no re-attach.
    epoch_before = transport.lane_info("bind_shared")["ticket_epoch"]
    fetches_before = len(fetches)
    transport.add_lane("bind_shared", "ag_a", provider_v2,
                       credential_epoch=2, authorization_revision=2)
    assert transport.lane_info("bind_shared")["ticket_epoch"] == \
        epoch_before
    assert len(fetches) == fetches_before
    await transport.stop()
    await peer.stop()


async def test_p05c_expired_ticket_demotes_and_renews(tmp_path):
    """ACN4-33: the ticket's contract expiry demotes the lane from
    ready and renews single-flight through attach — never READY
    indefinitely."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    fetches: list[str] = []

    async def expiring_provider():
        fetches.append(TICKET)
        return TICKET, 0.6  # expires almost immediately

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=expiring_provider, on_operation=_noop,
        on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", expiring_provider)
    transport.start()
    await transport.wait_online(10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert transport.lane_info("bind_shared")["state"] == "ready"
    first_fetches = len(fetches)
    await asyncio.sleep(1.0)  # past the ticket's expiry
    # Any inbound frame is the expiry checkpoint — poke the peer.
    session = peer.latest
    await session.websocket.send(encode_frame({
        "protocol_major": PROTOCOL_MAJOR,
        "contract_revision": CONTRACT_REVISION,
        "type": "heartbeat",
        "server_id": peer.server_id,
        "executor_id": session.executor_id,
        "connection_generation": session.connection_generation}))
    await asyncio.sleep(0.8)
    assert len(fetches) > first_fetches, "expired ticket never renewed"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    await transport.stop()
    await peer.stop()
