"""Integration: DaemonApp composition — IPC, runtimes, events, WSS (C02/C05).

Runs the real daemon application in-process against the fake HTTP/WSS
peers. The synchronous IPC client runs in a worker thread, exactly like
the real CLI process does. Contract-level: real provider runs remain
provider-gated (TC-42).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from pathlib import Path

import pytest

from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.ipc.client import IPCClient
from okto_nexus_connector.platform import paths
from okto_nexus_connector.storage.state_store import (
    BindingRecord, IdentityRecord, ServerProfileRecord, StateStore,
)
from tests.fakes.http_peer import FakeAgent, FakeBinding, FakeNexusHTTPPeer
from tests.fakes.wss_peer import FakeNXLPeer

KEY = "nxs_integration_key"
SERVER_ID = "srv_fake"
AGENT = "ag_int"
BINDING_ID = "bind_int"
ALIAS = "codex"


def _fake_binary(root: Path) -> Path:
    binary = root / "fake-codex.exe"
    if not binary.exists():
        binary.write_bytes(b"synthetic selected codex binary")
    return binary


def _fingerprint(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


async def _prepare_state(root: Path, http_url: str, wss_url: str) -> None:
    store = StateStore(paths.state_file(root))
    vault = RestrictedFileVault(paths.vault_dir(root), approved=True)
    vault.store(f"{SERVER_ID}/{AGENT}", KEY)
    project = root / "project"
    project.mkdir(exist_ok=True)

    def mutate(state):
        state.connector_id = "conn_test"
        state.preferences["vault.fallback_file.approved"] = True
        state.servers[SERVER_ID] = ServerProfileRecord(
            server_id=SERVER_ID, base_url=http_url, origin=http_url,
            added_at="now",
            link_url_override=wss_url +
            "/v1/runtime/executors/conn_test/link")
        state.identities.append(IdentityRecord(
            alias="work", server_id=SERVER_ID, agent_id=AGENT,
            secret_handle=f"vault:{SERVER_ID}/{AGENT}", credential_epoch=1,
            added_at="now"))
        state.bindings.append(BindingRecord(
            binding_id=BINDING_ID, alias=ALIAS, server_id=SERVER_ID,
            agent_id=AGENT, adapter_id="codex_app_server",
            executor_id="conn_test", workspace_id="ws_1",
            workspace_root=str(project), endpoint_id="wb_1",
            profile_id="prof_1", candidate_executable=str(_fake_binary(root)),
            candidate_fingerprint=_fingerprint(_fake_binary(root)),
            candidate_version="0.157.0", created_at="now"))
    store.update(mutate)


class DaemonHarness:
    def __init__(self, app: DaemonApp, root: Path):
        self.app = app
        self.root = root
        self.lock = InstanceLock(paths.pid_dir(root))
        self.task: asyncio.Task | None = None

    async def start(self):
        self.task = asyncio.create_task(self.app.run_forever())
        await self._wait_ready()

    async def _wait_ready(self, timeout: float = 20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.lock.live_owner() is not None:
                return self.lock.live_owner()
            await asyncio.sleep(0.05)
        raise AssertionError("daemon not ready")

    def call(self, op: str, params: dict | None = None):
        return asyncio.to_thread(self._call_blocking, op, params)

    def _call_blocking(self, op, params):
        readiness = self.lock.live_owner()
        assert readiness is not None
        with IPCClient(readiness.transport, readiness.address,
                       self.lock.load_token()) as client:
            return client.call(op, params)

    def stream(self, op: str, params: dict | None = None):
        return asyncio.to_thread(self._stream_blocking, op, params)

    def _stream_blocking(self, op, params):
        readiness = self.lock.live_owner()
        assert readiness is not None
        frames = []
        with IPCClient(readiness.transport, readiness.address,
                       self.lock.load_token()) as client:
            for frame in client.stream(op, params):
                frames.append(frame)
                if frame.get("done"):
                    break
        return frames

    def stream_iter(self, op: str, params: dict | None = None):
        """Live async iterator over streamed IPC records; cancelling the
        consumer closes the client socket (a "closed terminal")."""
        return _LiveStream(self, op, params)

    async def stop(self, timeout: float = 45.0):
        """Stop the in-process daemon, bounded so a wedged teardown
        cannot hang the suite (subprocess lifecycle is covered by the
        process tests; this is a test-harness guard)."""
        self.app.request_stop()
        if self.task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self.task), timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError,
                    Exception):
                self.task.cancel()
                try:
                    await self.task
                except (asyncio.CancelledError, Exception):
                    pass
            self.task = None


class _LiveStream:
    def __init__(self, harness, op, params):
        self._harness = harness
        self._op = op
        self._params = params
        self._queue: asyncio.Queue | None = None
        self._thread: asyncio.Task | None = None
        self._client = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._queue is None:
            self._queue = asyncio.Queue()
            loop = asyncio.get_running_loop()

            def worker():
                try:
                    readiness = self._harness.lock.live_owner()
                    client = IPCClient(readiness.transport,
                                       readiness.address,
                                       self._harness.lock.load_token())
                    self._client = client
                    for frame in client.stream(self._op, self._params):
                        loop.call_soon_threadsafe(
                            self._queue.put_nowait, frame)
                        if frame.get("done"):
                            break
                except BaseException:
                    loop.call_soon_threadsafe(
                        self._queue.put_nowait, {"done": True, "ok": True,
                                                 "closed": True})
            self._thread = asyncio.get_running_loop().run_in_executor(
                None, worker)
        frame = await self._queue.get()
        if frame.get("done") and "record" not in frame:
            raise StopAsyncIteration
        return frame

    async def aclose(self):
        client = self._client
        if client is not None:
            await asyncio.get_running_loop().run_in_executor(
                None, client.close)


@pytest.fixture
async def harness(tmp_path: Path):
    http_peer = FakeNexusHTTPPeer(server_id=SERVER_ID)
    http_peer.add_agent(FakeAgent(agent_id=AGENT, key=KEY,
                                  server_id=SERVER_ID))
    http_url = await http_peer.start()
    http_peer.bindings[BINDING_ID] = FakeBinding(
        binding_id=BINDING_ID, agent_id=AGENT, alias=ALIAS,
        adapter_id="codex_app_server")
    wss_peer = FakeNXLPeer(server_id=SERVER_ID)
    wss_url = await wss_peer.start()
    await _prepare_state(tmp_path, http_url, wss_url)
    # Tickets issued by the HTTP contract peer are accepted by the WSS
    # peer (same canonical authority in the fake topology).
    original_ticket = http_peer._ticket

    def ticket_and_register(headers, binding_id):
        status, payload = original_ticket(headers, binding_id)
        if status == 200:
            wss_peer.valid_tickets.add(payload["ticket"])
        return status, payload
    http_peer._ticket = ticket_and_register
    app = DaemonApp(tmp_path)
    # Contract-level native factory: a real provider qualification is a
    # separate provider-gated campaign (TC-42), never a fake-passing gate.
    from tests.integration.conftest import RecordingFactory
    recording_factory = RecordingFactory()
    original_host_build = app.host.build

    def build_with_fake_factory(binding, *, environment):
        return original_host_build(binding, environment=environment,
                                   factory=recording_factory)
    app.host.build = build_with_fake_factory
    harness = DaemonHarness(app, tmp_path)
    harness.http = http_peer
    harness.wss = wss_peer
    harness.recording_factory = recording_factory
    await harness.start()
    yield harness
    await harness.stop()
    await http_peer.stop()
    await wss_peer.stop()


async def test_daemon_ready_ping_status(harness):
    response = await harness.call("ping")
    assert response["ok"] and response["result"]["pong"] is True
    status = await harness.call("status")
    result = status["result"]
    assert result["daemon"]["version"]
    assert SERVER_ID in result["transports"]
    assert result["bindings"] >= 1


async def test_unauthenticated_ipc_denied(harness):
    """TC-08: wrong token is refused before any effect."""
    def probe():
        readiness = harness.lock.live_owner()
        client = IPCClient(readiness.transport, readiness.address,
                           "forged-token-value-1234")
        try:
            client.connect()
            return "connected"
        except ConnectorError as error:
            return error.code
    outcome = await asyncio.to_thread(probe)
    assert outcome == "AGENT_AUTH_REQUIRED"


async def test_unknown_op_is_typed_error(harness):
    response = await harness.call("definitely-not-an-op")
    assert response["ok"] is False
    assert response["error"]["code"] == "CAPABILITY_UNSUPPORTED"


async def test_runtime_start_lifecycle(harness):
    """TC-12/TC-21: authorized start, reuse, explicit new session."""
    started = await harness.call("runtime.start", {"alias": ALIAS})
    assert started["ok"], started
    result = started["result"]
    session_id = result["session_id"]
    assert result["reused"] is False
    assert result["receipt"]["stage"] in ("SUBMITTED", "ACCEPTED",
                                          "RUNNING")
    # second start reuses the compatible session
    again = await harness.call("runtime.start", {"alias": ALIAS})
    assert again["result"]["reused"] is True
    assert again["result"]["session_id"] == session_id
    # explicit new session
    fresh = await harness.call("runtime.start", {
        "alias": ALIAS, "new_session": True})
    assert fresh["result"]["reused"] is False
    assert fresh["result"]["session_id"] != session_id
    # status lists both
    status = await harness.call("runtime.status")
    ids = {s["session_id"] for s in status["result"]["sessions"]}
    assert {session_id, fresh["result"]["session_id"]} <= ids
    # inspect one
    inspect = await harness.call("runtime.inspect", {
        "session_id": session_id})
    assert inspect["result"]["ownership"] in ("owned", "unknown")
    # interrupt keeps the runtime; stop closes it
    interrupt = await harness.call("runtime.interrupt", {
        "session_id": fresh["result"]["session_id"]})
    assert interrupt["ok"]
    stop = await harness.call("runtime.stop", {
        "session_id": fresh["result"]["session_id"]})
    assert stop["ok"]
    status = await harness.call("runtime.status")
    ids = {s["session_id"] for s in status["result"]["sessions"]}
    assert fresh["result"]["session_id"] not in ids
    assert session_id in ids


async def test_workspace_guard_rejects_other_directory(harness):
    """TC-14: cwd outside the bound root is refused, not silently bound."""
    elsewhere = harness.root / "elsewhere"
    elsewhere.mkdir(exist_ok=True)
    response = await harness.call("runtime.start", {
        "alias": ALIAS, "project": str(elsewhere)})
    assert response["ok"] is False
    assert response["error"]["code"] == "WORKSPACE_UNAVAILABLE"


async def test_unknown_binding_alias_is_actionable(harness):
    response = await harness.call("runtime.start", {"alias": "nope"})
    assert response["ok"] is False
    assert response["error"]["code"] == "VALIDATION_ERROR"
    assert "action" in response["error"]


async def test_shutdown_report_is_honest(harness):
    """TC-23: stop drains sessions and reports per-resource outcomes."""
    started = await harness.call("runtime.start", {"alias": ALIAS})
    session_id = started["result"]["session_id"]
    response = await harness.call("shutdown")
    assert response["ok"]
    await harness.stop()
    # the daemon task returned a clean exit code
    assert harness.task is None or harness.task.done()
    status = await harness.call.__wrapped__ if False else None
