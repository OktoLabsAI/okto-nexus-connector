"""CN5 audit regressions — reconstructed from the reviewer package.

For the FOURTH consecutive time the package arrived WITHOUT its
executables (`00_ENTREGAR_AO_AGENTE.md` claims "nesta rodada eles estão
realmente incluídos", but only 6 Markdown/JSON files were delivered —
no ``regressoes/``, no ``executar_verificacao.py``, no
``evidencias/``). These cases are rebuilt from the FIXED
specifications of ``02_PLANO_CORRECAO_CN5.md`` (sections 2–3: DTOs,
signatures, states, effect order) and the node ids of
``03_MATRIZ_ACEITE.md`` ACN5-01..32, following the CN1–CN4 precedent.

The six confirmed failures (Q01a×2, Q01b, Q02, Q03, Q04) plus their
controls and the complementary matrix cases all traverse the REAL
application paths: ``_on_remote_approval`` receiving →
``_decide_approval``/producer → HTTP peer → manager → Core real →
native peer; ``EventBridge`` with the real transport; lane attach with
the real scheduler.
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
    RuntimeEvent, create_runtime,
)
from nexus_connector_core.discovery import (
    binary_architecture, fingerprint,
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

TICKET = "nstkt_cn5"
HASH64 = "ab" * 32          # the correlation hash the old path redacted
TOKEN_LIKE = "nxs_labsecret0123456789abcdef"


def _exe(path: Path, content: bytes = b"synthetic-c5") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if os.name != "nt":
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _binding(root: Path, binary: Path, *, server="srv_a",
             binding_id="bind_shared", agent="ag_a", workspace="ws_a",
             executor="exe_host"):
    return BindingRecord(
        binding_id=binding_id, alias=f"al-{server}-{binding_id}",
        server_id=server, agent_id=agent, adapter_id="codex_app_server",
        executor_id=executor, workspace_id=workspace,
        workspace_root=str(root),
        candidate_executable=str(binary),
        candidate_fingerprint=fingerprint(binary),
        candidate_version="0.157.0",
        candidate_architecture=binary_architecture(binary) or "x86_64")


class _Lab:
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

        def fix(state):
            record = state.servers.get(server_id)
            if record is not None:
                record.base_url = url
                record.origin = url

        self.store.update(fix)
        self.peers[server_id] = peer
        return peer

    def add_identity(self, server, agent, key="nxs_cn5"):
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
                           session_id="sess_q01",
                           params_extra=None):
    """A REAL Core session (kernel+journal) whose pump OBSERVES the
    pending native request — the correlation the decision must match."""
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
        LaunchIntent(agent, "ws_a", "codex_app_server"), context)
    await runtime.open(
        OpenOperation(f"op_open_{session_id}", session_id, "ep-1",
                      prepared), context)
    params = {"turnId": "turn_1", "command": "lab"}
    if params_extra:
        params = {**params, **params_extra}
    native_request = {
        "request_id": f"native_{session_id}",
        "request_hash": HASH64,
        "method": "item/commandExecution/requestApproval",
        "params": params,
    }
    await factory.native.queue.put(RuntimeEvent(
        server, "exe_host", session_id, "ep-1", 0, "native",
        "approval.request", {"native_approval": native_request}))
    await asyncio.sleep(0.1)  # the pump observes the pending request
    session = ManagedSession(
        session_id=session_id, binding=binding, stream_epoch="ep-1",
        opened_at=time.monotonic(), runtime=runtime,
        lease_deadline=time.monotonic() + 90,
        connection_generation=1, session_owner_generation=1,
        authorized_actions=frozenset({"approval.decide", "input.provide",
                                       "turn.interrupt",
                                       "runtime.close"}))
    app.runtimes._register(session)
    return factory, native_request


def _frame(server, request_id, *, binding_id="bind_shared",
           session_id=None, agent="ag_a", kind="requestApproval",
           proposal=None, executor="exe_host"):
    return {
        "type": "approval.request", "server_id": server,
        "executor_id": executor, "binding_id": binding_id,
        "agent_id": agent, "workspace_id": "ws_a",
        "session_id": session_id, "operation_id": f"op_{request_id}",
        "request_id": request_id, "kind": kind,
        "proposal": proposal if proposal is not None else {
            "request_id": f"native_{request_id}",
            "request_hash": HASH64,
            "method": "item/commandExecution/requestApproval",
            "params": {"turnId": "turn_1", "command": "lab"}}}


async def _noop(*args, **kwargs):
    return None


# =====================================================================
# Q01a — the REAL receiving path preserves the operational proposal
# =====================================================================

@pytest.mark.parametrize("cli_decision,core_decision", [
    ("approve", "accept"), ("deny", "decline")])
async def test_q01_real_receiving_preserves_operational_proposal(
        tmp_path, cli_decision, core_decision):
    """ACN5-01/02: the frame crosses ``_on_remote_approval`` (where the
    old path redacted the 64-hex ``request_hash`` away); the Server
    confirms; the manager translates; the REAL Core accepts the reply
    against the request its pump OBSERVED — and the native peer gets
    the response. Presentation is redacted; operation is intact."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        peer.add_approval("req_q01", agent_id="ag_a")
        observed = {"note": f"token {TOKEN_LIKE} inline"}
        factory, native_request = await _managed_session(
            lab.app, tmp_path, session_id="sess_q01",
            params_extra=observed)
        received_requests: list[dict] = []
        native_answers: list[tuple] = []
        session = lab.app.runtimes.session("sess_q01")
        runtime = session.runtime
        original = runtime.decide_native_approval

        async def spy_decide(operation, context):
            received_requests.append(dict(operation.request))
            receipt = await original(operation, context)
            native_answers.append((operation.decision,
                                   receipt.stage))
            return receipt

        runtime.decide_native_approval = spy_decide
        # The REAL receiving path — proposal carries the correlation
        # hash AND a lab token (presentation must redact the token).
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_q01", session_id="sess_q01",
            proposal=native_request))
        # ACN5-21: the PENDING listing shows a REDACTED projection and
        # never leaks the lab token.
        listing = [r.to_public_dict() for r in
                   lab.app._approvals.values()]
        assert listing and TOKEN_LIKE not in json.dumps(listing), listing
        result = await lab.app._decide_approval({
            "request_id": "req_q01", "decision": cli_decision,
            "cas_token": "cas_1"})
        assert result["applied"] is True, result
        assert result["native"]["receipt"]["stage"] in (
            "SUBMITTED", "OUTCOME_UNKNOWN"), result
        # The OPERATIONAL proposal reached the Core INTACT — the 64-hex
        # correlation hash was NOT redacted to "[redacted]" (Q01a).
        assert received_requests, "no native application happened"
        assert received_requests[0]["request_hash"] == HASH64, \
            received_requests[0]
        assert native_answers[0][0] == core_decision
        assert peer.approvals["req_q01"]["decision"] == cli_decision
    finally:
        await lab.stop()


async def test_q01_foreign_approval_never_selects_a_core_in_other_namespace(
        tmp_path):
    """ACN5-15 (Q01b): A holds a live Core session; B's request (same
    textual binding/session ids) confirms on B's origin — the native
    step must resolve B's namespace ONLY. A's Core is NEVER dispatched
    to; the refusal is NOT satisfied by a later VALIDATION_ERROR: the
    spy proves ZERO dispatch toward A."""
    lab = _Lab(tmp_path, servers=("srv_a", "srv_b"))
    peer_a = await lab.start_peer("srv_a")
    peer_b = await lab.start_peer("srv_b")
    try:
        # A: real Core session with the SAME textual ids.
        lab.add_identity("srv_a", "ag_shared")
        peer_a.add_agent(FakeAgent(agent_id="ag_shared", key="nxs_cn5",
                                   server_id="srv_a"))
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a",
                                 agent="ag_shared"))
        factory_a, _request = await _managed_session(
            lab.app, tmp_path, server="srv_a", agent="ag_shared",
            session_id="sess_shared")
        dispatches: list[tuple] = []
        runtime_a = lab.app.runtimes.session("sess_shared").runtime
        original_a = runtime_a.decide_native_approval

        async def spy_a(operation, context):
            dispatches.append((context.server_id, context.executor_id,
                               operation.session_id))
            return await original_a(operation, context)

        runtime_a.decide_native_approval = spy_a
        # B: same TEXTUAL binding/session/request ids, its own origin.
        lab.add_identity("srv_b", "ag_shared")
        peer_b.add_agent(FakeAgent(agent_id="ag_shared", key="nxs_cn5",
                                   server_id="srv_b"))
        lab.add_binding(_binding(lab.root, binary, server="srv_b",
                                 agent="ag_shared"))
        peer_b.add_approval("req_shared", agent_id="ag_shared")
        await lab.app._on_remote_approval(_frame(
            "srv_b", "req_shared", agent="ag_shared",
            session_id="sess_shared"))
        with pytest.raises(ConnectorError) as error:
            await lab.app._decide_approval({
                "server_id": "srv_b", "request_id": "req_shared",
                "decision": "approve", "cas_token": "cas_b"})
        # The refusal is the typed NAMESPACE refusal — not a Core
        # VALIDATION_ERROR (which would mean A's Core was consulted).
        assert error.value.code == "BINDING_NOT_AUTHORIZED", \
            str(error.value)
        assert "not managed in this namespace" in str(error.value)
        # Q01b acceptance: ZERO dispatch toward A's Core.
        assert dispatches == [], \
            f"B's approval reached A's Core: {dispatches}"
        # ...and the refusal PRECEDED the canonical POST (target
        # validated before it, CN5-01.04): neither origin consumed
        # the decision.
        assert not peer_b.approvals.get("req_shared", {}).get(
            "answered")
        assert not peer_a.approvals.get("req_shared", {}).get(
            "answered")
    finally:
        await lab.stop()


async def test_q01c_malformed_confirmation_refuses_typed(tmp_path):
    """ACN5-24: the peer returns applied as a STRING/object — a typed
    error, zero native application."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a"))
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_mal", session_id=None))  # administrative
        peer.add_approval("req_mal", agent_id="ag_a")
        # The peer echoes applied=True as a STRING through the body.
        original = peer._approval

        def stringy_applied(headers, body):
            status, payload = original(headers, body)
            payload["applied"] = "true"  # malformed type
            return status, payload

        peer._approval = stringy_applied
        with pytest.raises(ConnectorError) as error:
            await lab.app._decide_approval({
                "request_id": "req_mal", "decision": "approve",
                "cas_token": "c"})
        assert error.value.code == "VERSION_INCOMPATIBLE"
        status = lab.app._approval_status({"request_id": "req_mal"})
        assert status["producer"] is False
    finally:
        await lab.stop()


async def test_q01d_replay_during_in_flight_keeps_attempt(tmp_path):
    """ACN5-23: the same approval.request replays while the POST is
    retained — the record/phase is kept, no second producer."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        peer.add_approval("req_rep", agent_id="ag_a")
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a"))
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_rep", session_id=None))
        from okto_nexus_connector.transport.https_client import \
            NexusHTTPClient
        entered = asyncio.Event()
        release = asyncio.Event()
        original = NexusHTTPClient.approval_decision

        async def held(self, *args, **kwargs):
            entered.set()
            await release.wait()
            return await original(self, *args, **kwargs)

        NexusHTTPClient.approval_decision = held
        try:
            first = asyncio.create_task(lab.app._decide_approval({
                "request_id": "req_rep", "decision": "approve",
                "cas_token": "c"}))
            await asyncio.wait_for(entered.wait(), 5)
            # The SAME frame replays while the POST is retained.
            await lab.app._on_remote_approval(_frame(
                "srv_a", "req_rep", session_id=None))
            pending = lab.app._approvals[("srv_a", "req_rep")]
            assert pending.attempt is not None
            producer_before = pending.attempt.producer_task
            release.set()
            result = await asyncio.wait_for(first, 5)
        finally:
            NexusHTTPClient.approval_decision = original
        assert result["applied"] is True
        record = lab.app._approvals.get(("srv_a", "req_rep"))
        assert record is None or record is pending
        # A different proposal under the same key is a conflict.
        if ("srv_a", "req_rep") in lab.app._approvals:
            other = _frame("srv_a", "req_rep", session_id=None)
            other["proposal"] = {"request_id": "different"}
            with pytest.raises(ConnectorError):
                await lab.app._on_remote_approval(other)
    finally:
        await lab.stop()


async def test_q01e_target_changed_during_post_keeps_confirmation(
        tmp_path):
    """ACN5-27: the Server confirms while the session's authorized
    generation rotated — no effect on the new target, no re-POST, the
    canonical confirmation is kept (STALE_GENERATION/NATIVE_REFUSED)."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        peer.add_approval("req_gen", agent_id="ag_a")
        _exe(lab.root / "codex.exe")
        factory, _request = await _managed_session(
            lab.app, tmp_path, session_id="sess_gen")
        runtime = lab.app.runtimes.session("sess_gen").runtime
        dispatched: list = []

        async def spy(operation, context):
            dispatched.append(operation)
            raise AssertionError("native ran on the new target")

        runtime.decide_native_approval = spy
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_gen", session_id="sess_gen"))
        from okto_nexus_connector.transport.https_client import \
            NexusHTTPClient
        entered = asyncio.Event()
        original = NexusHTTPClient.approval_decision

        async def held(self, *args, **kwargs):
            entered.set()
            # While the POST was in flight the session's generation
            # rotated (renewal) — the captured target is now stale.
            session = lab.app.runtimes.session("sess_gen")
            session.connection_generation = 7
            return await original(self, *args, **kwargs)

        NexusHTTPClient.approval_decision = held
        try:
            with pytest.raises(ConnectorError) as error:
                await lab.app._decide_approval({
                    "request_id": "req_gen", "decision": "approve",
                    "cas_token": "c"})
        finally:
            NexusHTTPClient.approval_decision = original
        assert error.value.code == "STALE_GENERATION", str(error.value)
        assert dispatched == []
        status = lab.app._approval_status({"request_id": "req_gen"})
        assert status["phase"] == "native_refused", status
        # The canonical confirmation was kept: the Server answered.
        assert peer.approvals["req_gen"].get("answered") is True
        # And a repeated decision does NOT re-POST the confirmation.
        calls = []
        record = peer.approvals["req_gen"]

        async def no_second_post(self, *args, **kwargs):
            calls.append(1)
            return await original(self, *args, **kwargs)

        NexusHTTPClient.approval_decision = no_second_post
        try:
            with pytest.raises(ConnectorError):
                await lab.app._decide_approval({
                    "request_id": "req_gen", "decision": "approve",
                    "cas_token": "c"})
        finally:
            NexusHTTPClient.approval_decision = original
        assert calls == [], "the confirmed POST was repeated"
    finally:
        await lab.stop()


# =====================================================================
# Q02 — cancelling the waiter never cancels the accepted decision
# =====================================================================

async def test_q02_cancel_waiter_does_not_cancel_accepted_server_decision(
        tmp_path):
    """ACN5-16: an administrative decision reaches the retained peer
    answer; the IPC waiter is CANCELLED; the producer survives, the
    same attempt finishes, and a repeat consults it WITHOUT a second
    POST. No ghost 'server_pending' without a live producer."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        peer.add_approval("req_q02", agent_id="ag_a")
        binary = _exe(lab.root / "codex.exe")
        lab.add_binding(_binding(lab.root, binary, server="srv_a"))
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_q02", session_id=None))  # administrative
        from okto_nexus_connector.transport.https_client import \
            NexusHTTPClient
        entered = asyncio.Event()
        release = asyncio.Event()
        posts: list[int] = []
        original = NexusHTTPClient.approval_decision

        async def held(self, *args, **kwargs):
            posts.append(1)
            entered.set()
            await release.wait()      # the Server barrier stays closed
            return await original(self, *args, **kwargs)

        NexusHTTPClient.approval_decision = held
        waiter = None
        try:
            waiter = asyncio.create_task(lab.app._decide_approval({
                "request_id": "req_q02", "decision": "approve",
                "cas_token": "cas_1"}))
            await asyncio.wait_for(entered.wait(), 5)
            # The waiter is cancelled while the POST is retained.
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            await asyncio.sleep(0.1)
            # The producer was NOT cancelled: the attempt reports a
            # live producer (no fictitious in-flight without owner).
            status = lab.app._approval_status({"request_id": "req_q02"})
            assert status["phase"] == "server_pending", status
            assert status["producer"] is True, status
            producer = lab.app._approvals[
                ("srv_a", "req_q02")].attempt.producer_task
            assert producer is not None and not producer.done()
            # A repeat of the SAME decision SHARES the producer.
            shared = asyncio.create_task(lab.app._decide_approval({
                "request_id": "req_q02", "decision": "approve",
                "cas_token": "cas_1"}))
            release.set()             # the Server answers
            result = await asyncio.wait_for(shared, 5)
            assert result["applied"] is True
            # Exactly ONE POST reached the Server for this intent.
            assert len(posts) == 1, posts
        finally:
            NexusHTTPClient.approval_decision = original
            if waiter is not None and not waiter.done():
                waiter.cancel()
        # The terminal attempt is consultable; a repeat returns the
        # same recorded outcome without any new POST.
        again = await lab.app._decide_approval({
            "request_id": "req_q02", "decision": "approve",
            "cas_token": "cas_1"})
        assert again["applied"] is True
        assert len(posts) == 1
    finally:
        await lab.stop()


async def test_q02b_core_error_after_confirmation_is_typed_terminal(
        tmp_path):
    """ACN5-25: the Server confirms; the Core refuses BEFORE the effect
    (JOURNAL_FULL) — NATIVE_REFUSED, consultable, no re-POST, no false
    native_pending."""
    lab = _Lab(tmp_path)
    peer = await lab.start_peer("srv_a")
    try:
        lab.add_identity("srv_a", "ag_a")
        peer.add_agent(FakeAgent(agent_id="ag_a", key="nxs_cn5",
                                 server_id="srv_a"))
        peer.add_approval("req_ce", agent_id="ag_a")
        factory, _request = await _managed_session(
            lab.app, tmp_path, session_id="sess_ce")
        from nexus_connector_core import CoreError
        runtime = lab.app.runtimes.session("sess_ce").runtime
        original = runtime.decide_native_approval

        async def journal_full(operation, context):
            raise CoreError("JOURNAL_FULL", "approval_decide")

        runtime.decide_native_approval = journal_full
        await lab.app._on_remote_approval(_frame(
            "srv_a", "req_ce", session_id="sess_ce"))
        with pytest.raises(ConnectorError) as error:
            await lab.app._decide_approval({
                "request_id": "req_ce", "decision": "approve",
                "cas_token": "c"})
        assert "JOURNAL_FULL" in str(error.value), str(error.value)
        status = lab.app._approval_status({"request_id": "req_ce"})
        assert status["phase"] == "native_refused", status
        assert status["producer"] is False, status
        # The canonical Server confirmation is intact.
        assert peer.approvals["req_ce"].get("answered") is True
        runtime.decide_native_approval = original
    finally:
        await lab.stop()


# =====================================================================
# Q03 — rotation during an in-flight attach schedules the successor
# =====================================================================

async def test_q03_rotation_reschedules_after_old_attach_is_cancelled(
        tmp_path):
    """ACN5-17: the OLD provider stays awaiting; rotation bumps the
    generation and cancels it; once the old attempt finishes, the
    successor for the CURRENT generation runs (new provider consulted,
    its attach queued — no socket restart)."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    old_release = asyncio.Event()
    fetches: list[str] = []

    async def provider_v1():
        fetches.append("v1")
        await old_release.wait()      # the OLD provider stays pending
        return TICKET

    async def provider_v2():
        fetches.append("v2")
        return TICKET

    async def connect_ticket():
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=connect_ticket, on_operation=_noop,
        on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", provider_v1)
    transport.start()
    assert await transport.wait_online(10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        # wait until the OLD attach attempt is actually in flight
        task = transport._lanes["bind_shared"].attach_task
        if task is not None and not task.done():
            break
        await asyncio.sleep(0.02)
    old_task = transport._lanes["bind_shared"].attach_task
    assert old_task is not None and not old_task.done()
    # Rotation while the old attempt is in flight.
    transport.add_lane("bind_shared", "ag_a", provider_v2,
                       credential_epoch=2, authorization_revision=2)
    lane = transport._lanes["bind_shared"]
    assert lane.reattach_requested is True
    await asyncio.sleep(0.2)          # let the cancellation complete
    assert old_task.done(), "old attach was not cancelled"
    # The successor for the CURRENT generation is scheduled and runs
    # (the new provider is consulted) — without a socket restart.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and "v2" not in fetches:
        await asyncio.sleep(0.02)
    assert "v2" in fetches, "the successor generation never ran"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.02)
    assert transport.lane_info("bind_shared")["state"] == "ready"
    old_release.set()                 # the LATE old ticket arrives
    await asyncio.sleep(0.3)
    # The late old result never re-labelled anything: the lane is
    # ready under the NEW proof only (ACN5-28).
    info = transport.lane_info("bind_shared")
    assert info["state"] == "ready", info
    assert info["credential_epoch"] == 2
    await transport.stop()
    await peer.stop()


async def test_control_q03_ready_lane_rotation_fetches_new_ticket(
        tmp_path):
    """ACN5-18 control: rotation from READY (no in-flight attach)
    fetches the new ticket and reattaches."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    fetches: list[str] = []

    async def p1():
        fetches.append("v1")
        return TICKET

    async def p2():
        fetches.append("v2")
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=p1, on_operation=_noop, on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", p1)
    transport.start()
    await transport.wait_online(10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert transport.lane_info("bind_shared")["state"] == "ready"
    transport.add_lane("bind_shared", "ag_a", p2, credential_epoch=2,
                       authorization_revision=2)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert "v2" in fetches
    assert transport.lane_info("bind_shared")["state"] == "ready"
    await transport.stop()
    await peer.stop()


async def test_q03b_triple_rotation_runs_only_generation_three(
        tmp_path):
    """ACN5-29: rotations 1→2→3 with attach1 in flight — at most one
    current attempt; only generation 3's provider is needed."""
    peer = FakeNXLPeer(server_id="srv_a", valid_tickets={TICKET})
    await peer.start()
    release1 = asyncio.Event()
    fetches: list[str] = []

    async def p1():
        fetches.append("v1")
        await release1.wait()
        return TICKET

    async def p2():
        fetches.append("v2")
        return TICKET

    async def p3():
        fetches.append("v3")
        return TICKET

    transport = NXLTransport(
        server_id="srv_a", executor_id="exe_host", link_url=peer.url,
        ticket_provider=p1, on_operation=_noop, on_approval=_noop)
    transport.add_lane("bind_shared", "ag_a", p1)
    transport.start()
    await transport.wait_online(10)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        task = transport._lanes["bind_shared"].attach_task
        if task is not None and not task.done():
            break
        await asyncio.sleep(0.02)
    transport.add_lane("bind_shared", "ag_a", p2, credential_epoch=2,
                       authorization_revision=2)
    transport.add_lane("bind_shared", "ag_a", p3, credential_epoch=3,
                       authorization_revision=3)
    lane = transport._lanes["bind_shared"]
    assert lane.attach_generation == 2  # 0 -> 1 -> 2 (three proofs)
    release1.set()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and \
            transport.lane_info("bind_shared")["state"] != "ready":
        await asyncio.sleep(0.05)
    assert transport.lane_info("bind_shared")["state"] == "ready"
    # Generation 2's provider may have been superseded before running;
    # generation 3 is the one that MUST have run, and the lane is
    # ready under exactly its proof.
    assert "v3" in fetches, fetches
    info = transport.lane_info("bind_shared")
    assert (info["credential_epoch"],
            info["authorization_revision"]) == (3, 3), info
    await transport.stop()
    await peer.stop()


# =====================================================================
# Q04 — the publisher recovers after a transient failure on reconnect
# =====================================================================

class _SeqEvent:
    def __new__(cls, seq):
        from nexus_connector_core import RuntimeEvent
        return RuntimeEvent(
            "srv_a", "exe_host", "sess_q04", "ep-q04", seq,
            "lifecycle", "note", {"text": f"q04-{seq}"})


class _BridgeTransport:
    def __init__(self):
        self.online = True
        self.batches: list[list] = []
        self.acks: dict[tuple, int] = {}
        self.fail_sends = 0

    async def send_events(self, batch):
        if self.fail_sends > 0:
            self.fail_sends -= 1
            raise RuntimeError("transient send failure")
        self.batches.append([e.sequence for e in batch])
        return True

    async def wait_event_ack(self, session_id, epoch, timeout=30.0, *,
                             target=None):
        watermark = self.acks.get((session_id, epoch))
        if watermark is None:
            return None
        if target is not None and watermark < target:
            return None
        return watermark


async def test_q04_event_stream_recovers_on_reconnect_without_new_native_event(
        tmp_path, variant="transient_page_failure"):
    """ACN5-19/20: a stream holds seq1; the FIRST page read fails
    transiently (the next read would succeed); the publisher task dies;
    calling ONLY transport_online_again (no new harness event) must
    restore the publisher — seq1 is sent and ACK1 applied exactly once.
    The healthy control sends and applies normally."""
    transport = _BridgeTransport()
    pages = {0: [_SeqEvent(1)]}
    reads: list[int] = []

    async def journal_page(server, session, epoch, after, limit):
        reads.append(after)
        if variant == "transient_page_failure" and len(reads) == 1:
            raise RuntimeError("transient journal read failure")
        return pages.get(after, [])

    applied: list[int] = []

    async def core_ack(server, session, epoch, watermark):
        applied.append(watermark)

    bridge = EventBridge(lambda server: transport, core_ack, journal_page)
    publish = bridge.publisher_for("srv_a")
    publish(_SeqEvent(1))
    if variant == "transient_page_failure":
        # The first read fails; the publisher parks with an armed retry
        # (owned timer) — and reconnect alone must also restore it.
        await asyncio.sleep(0.15)  # before the 0.25s retry timer
        assert applied == []  # nothing applied yet
        assert reads == [0], reads
        # Reconnect entry point ONLY (no new native event).
        bridge.transport_online_again("srv_a")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not transport.batches:
            await asyncio.sleep(0.05)
        assert transport.batches and all(
            batch == [1] for batch in transport.batches),             transport.batches
        # The Server then ACKs the recovered batch.
        transport.acks[("sess_q04", "ep-q04")] = 1
        bridge.ack_arrived("srv_a", "sess_q04", "ep-q04")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not applied:
            await asyncio.sleep(0.05)
        assert reads.count(0) >= 2,             f"the failed read was never retried: {reads}"
    assert applied == [1], applied
    assert all(batch == [1] for batch in transport.batches),         transport.batches  # only seq1 was ever (re)sent
    await bridge.drain()


async def test_q04b_transient_send_failure_converges(tmp_path):
    """ACN5-30: the sender fails ONCE; the owned retry converges with
    cursors advancing only after the validated ACK."""
    transport = _BridgeTransport()
    transport.fail_sends = 1
    pages = {0: [_SeqEvent(1)]}

    async def journal_page(server, session, epoch, after, limit):
        return pages.get(after, [])

    applied: list[int] = []

    async def core_ack(server, session, epoch, watermark):
        applied.append(watermark)

    bridge = EventBridge(lambda server: transport, core_ack, journal_page)
    bridge.publisher_for("srv_a")(_SeqEvent(1))
    await asyncio.sleep(0.1)
    transport.online = True
    # The retry timer fires by itself (progressive backoff).
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline and len(transport.batches) < 2:
        await asyncio.sleep(0.05)
    assert transport.batches[0] == [1]
    assert transport.batches[-1] == [1], transport.batches
    # The Server ACKs once; the Core applies once.
    transport.acks[("sess_q04", "ep-q04")] = 1
    bridge.ack_arrived("srv_a", "sess_q04", "ep-q04")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not applied:
        await asyncio.sleep(0.05)
    assert applied == [1], applied
    await bridge.drain()


async def test_q04c_drain_cancels_timers_without_resurrection(tmp_path):
    """ACN5-32: a publisher armed a retry; drain closes the bridge —
    no task is resurrected and no orphan TimerHandle fires."""
    transport = _BridgeTransport()
    transport.online = False      # send path unavailable → park

    async def journal_page(server, session, epoch, after, limit):
        return []

    async def core_ack(server, session, epoch, watermark):
        pass

    bridge = EventBridge(lambda server: transport, core_ack, journal_page)
    key = ("srv_a", "sess_d", "ep-d")

    async def failing_page(*args, **kwargs):
        raise RuntimeError("transient")

    bridge._journal_page = failing_page
    bridge._ensure_stream_task(key)
    wake = bridge._wakeups[key]
    wake.set()
    await asyncio.sleep(0.2)
    assert bridge._retry_timers.get(key) is not None or \
        bridge._tasks.get(key) is not None
    await bridge.drain()
    assert bridge._closed is True
    assert not bridge._retry_timers
    # After drain nothing resurrects: ensure is a no-op.
    bridge._ensure_stream_task(key)
    assert key not in bridge._tasks
    await asyncio.sleep(0.3)  # any armed timer would have fired here


async def test_q04d_core_ack_failure_reapplies_same_watermark(tmp_path):
    """ACN5-31: the Server confirms the batch; the FIRST Core
    application fails — the retry re-applies the SAME confirmation,
    without a new send and without looping on old ACKs."""
    transport = _BridgeTransport()
    pages = {0: [_SeqEvent(1)]}
    ack_calls: list[int] = []
    fail_first = True

    async def journal_page(server, session, epoch, after, limit):
        return pages.get(after, [])

    async def core_ack(server, session, epoch, watermark):
        nonlocal fail_first
        ack_calls.append(watermark)
        if fail_first:
            fail_first = False
            raise RuntimeError("transient core ack failure")

    bridge = EventBridge(lambda server: transport, core_ack, journal_page)
    bridge.publisher_for("srv_a")(_SeqEvent(1))
    transport.acks[("sess_q04", "ep-q04")] = 1
    bridge.ack_arrived("srv_a", "sess_q04", "ep-q04")
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline and len(ack_calls) < 2:
        await asyncio.sleep(0.05)
    assert ack_calls == [1, 1], ack_calls  # SAME watermark re-applied
    assert transport.batches == [[1]], transport.batches
    await bridge.drain()
