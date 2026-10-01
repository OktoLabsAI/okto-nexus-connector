"""Startup ownership, durable inventory ordering and honest cold recovery."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from nexus_connector_core import OperationKey, R4_PREVIEW_REVISION, SNAPSHOT_FORMAT_VERSION, __version__

from okto_nexus_connector.daemon.r4_control import R4DaemonControl
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.storage.state_store import (
    ConnectorState, ExecutionExecutorRecord, IdentityRecord, ServerProfileRecord, StateStore, state_from_json,
)
from okto_nexus_connector.transport.https_client import (
    ExecutorRegistration, InventoryAccepted, MeInfo, MANAGEMENT_REVISION, NexusHTTPClient, R4ProtocolInfo,
)


class Vault:
    reads = 0
    def resolve(self, handle):
        assert handle == 'vault:selected'
        self.reads += 1
        return 'synthetic-canonical-secret'


@pytest.fixture
def control(tmp_path):
    store = StateStore(tmp_path / 'state.json')
    state = ConnectorState(connector_id='connector')
    state.servers['srv'] = ServerProfileRecord('srv', 'https://nexus.test', 'https://nexus.test', 'now')
    state.identities.append(IdentityRecord('selected', 'srv', 'agent', 'vault:selected', 1, 'now'))
    state.execution_executors.append(ExecutionExecutorRecord('srv', 'connector', 'agent', 'registration', 'Host'))
    store.save(state)
    vault = Vault()
    host = CoreRuntimeHost(tmp_path, vault)
    class Peer:
        ready = True
        calls = 0
        publications = []
        sockets = []
        lost = False
        barrier = None
        after_discovery = None
        async def r4_protocol(self):
            return R4ProtocolInfo(MANAGEMENT_REVISION, SNAPSHOT_FORMAT_VERSION, self.ready)
        async def me(self, key):
            assert key == 'synthetic-canonical-secret'
            return MeInfo('srv', 'agent', 'Agent', (), 2, 3, 1)
        async def register_executor(self, key, **body):
            self.calls += 1
            assert body['client_intent_id'] == 'registration'
            return ExecutorRegistration('srv', 'executor', 'connector', 'AWAITING_INVENTORY',
                'nxt4_synthetic_' + str(self.calls), 600, 'agent', 1, 2)
        async def publish_inventory(self, ticket, *, executor_id, snapshot):
            assert ticket == 'nxt4_synthetic_' + str(self.calls)
            assert store.load().execution_executors[0].inventory_publication_sequence == snapshot['publication_sequence']
            self.publications.append(snapshot)
            if self.barrier:
                entered, release = self.barrier
                entered.set()
                await release.wait()
            if self.lost:
                self.lost = False
                raise ConnectorError('OUTCOME_UNKNOWN', 'inventory', 'The reply was lost.')
            return InventoryAccepted('srv', executor_id, snapshot['publication_sequence'], snapshot['inventory_revision'], 120000)
        async def discover(self):
            if self.after_discovery:
                self.after_discovery()
            return []
        async def connect(self, url, ticket, **params):
            assert url == 'wss://nexus.test/v1/runtime/executors/executor/link'
            request = dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION,
                server_id='srv', executor_id='executor', connection_id='link', connection_generation=1,
                reconcile_id='reconcile', cursor=None, operation_ids=[], session_ids=[])
            self.report = await params['report_reconciliation'](request)
            socket = SimpleNamespace(online=True, state=SimpleNamespace(control_ready=True,
                connection_generation=len(self.sockets) + 1, connection_id='link_' + str(len(self.sockets) + 1)))
            async def close(): socket.online = False
            socket.close = close
            self.sockets.append(socket)
            return socket
    peer = Peer()
    @asynccontextmanager
    async def http(url):
        assert url == 'https://nexus.test'
        yield peer
    owner = R4DaemonControl(store, vault, host, 'srv', http_factory=http, connect=peer.connect,
        discover=peer.discover, retry_delays=(0.01,), poll_seconds=0.01)
    return owner, peer, store, host, vault


async def eventually(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.005)


async def dispose(owner, host):
    await owner.stop()
    await host.shutdown_all()


async def test_startup_reconnect_and_reboot_keep_executor_and_sequence(control):
    owner, peer, store, host, vault = control
    owner.start()
    try:
        await eventually(lambda: owner.status()['control_ready'])
        assert not owner.status()['execution_ready'] and peer.report['complete'] is True
        assert store.load().execution_executors[0].executor_id == 'executor'
        peer.sockets[0].online = False
        await eventually(lambda: len(peer.sockets) == 2 and owner.status()['control_ready'])
        assert [p['publication_sequence'] for p in peer.publications] == [1, 2]
        assert 'nxt4_' not in store.path.read_text() and 'secret' not in str(owner.status())
        await owner.stop()
        reboot = R4DaemonControl(store, vault, host, 'srv', http_factory=owner.http_factory,
            connect=peer.connect, discover=peer.discover, retry_delays=(0.01,), poll_seconds=0.01)
        reboot.start()
        try:
            await eventually(lambda: reboot.status()['control_ready'])
            assert peer.publications[-1]['publication_sequence'] == 3
            assert peer.publications[-1]['producer_instance_id'] != peer.publications[0]['producer_instance_id']
        finally:
            await reboot.stop()
    finally:
        await dispose(owner, host)


async def test_lost_publication_reply_never_reuses_sequence(control):
    owner, peer, store, host, _ = control
    peer.lost = True
    owner.start()
    try:
        await eventually(lambda: owner.status()['control_ready'])
        assert [p['publication_sequence'] for p in peer.publications] == [1, 2]
        assert peer.calls == 2 and len(peer.sockets) == 2 and not peer.sockets[0].online
    finally:
        await dispose(owner, host)


async def test_unqualified_server_publishes_inventory_without_opening_socket(control):
    owner, peer, _, host, _ = control
    peer.ready = False
    owner.start()
    try:
        await eventually(lambda: owner.error_code == 'VERSION_INCOMPATIBLE')
        assert peer.publications and not peer.sockets and not owner.status()['control_ready']
    finally:
        await dispose(owner, host)


@pytest.mark.parametrize('change', ['foreign_origin', 'wrong_executor', 'revoked', 'ambiguous'])
async def test_invalid_local_selection_sends_no_credentials(control, change):
    owner, peer, store, host, vault = control
    def alter(state):
        if change == 'foreign_origin': state.servers['srv'].link_url_override = 'wss://foreign.test/link'
        elif change == 'wrong_executor':
            state.execution_executors[0].executor_id = 'executor'
            state.servers['srv'].link_url_override = 'wss://nexus.test/v1/runtime/executors/other/link'
        elif change == 'revoked': state.identities[0].revoked = True
        else: state.identities.append(replace(state.identities[0], alias='duplicate'))
    store.update(alter)
    owner.start()
    try:
        await eventually(lambda: owner.error_code is not None)
        assert vault.reads == 0 and peer.calls == 0 and not peer.publications
    finally:
        await dispose(owner, host)


async def test_local_change_during_discovery_fences_publication(control):
    owner, peer, store, host, _ = control
    peer.after_discovery = lambda: store.update(lambda state: setattr(state.identities[0], 'revoked', True))
    owner.start()
    try:
        await eventually(lambda: owner.error_code is not None)
        assert not peer.publications and all(not socket.online for socket in peer.sockets)
        assert store.load().execution_executors[0].inventory_publication_sequence == 0
    finally:
        await dispose(owner, host)


@pytest.mark.parametrize('history', ['claim', 'journal_slot', 'ledger_slot', 'journal_failure'])
async def test_nonempty_or_unavailable_history_cannot_be_reported_empty(control, history):
    owner, peer, _, host, _ = control
    key = OperationKey('srv', 'executor', 'opening')
    journal = await host.ensure_history_journal()
    if history == 'claim':
        await journal.admit(key, 'digest', 'session', claim_session=True)
    elif history == 'journal_slot':
        await journal.admit(key, 'digest', 'session', claim_session=True, effect_imminent=True)
        await journal.reserve_owned_slot(key, 'session')
    elif history == 'ledger_slot':
        await (await host.ensure_ledger()).reserve_owned_slot(key, 'session')
    else:
        async def fail(*args, **kwargs): raise OSError('synthetic-secret must not escape')
        journal.claimed_sessions = fail
    owner.start()
    try:
        await eventually(lambda: owner.phase == 'RECOVERING' and owner.error_code is not None)
        assert all(not socket.online for socket in peer.sockets) and not owner.status()['control_ready']
        if history == 'claim':
            assert peer.report['claims'][0]['state'] == 'UNKNOWN'
        assert 'synthetic-secret' not in str(owner.status())
    finally:
        await dispose(owner, host)


async def test_cancelled_stop_waiter_does_not_abandon_inventory_producer(control):
    owner, peer, _, host, _ = control
    entered, release = asyncio.Event(), asyncio.Event()
    peer.barrier = entered, release
    owner.start()
    await asyncio.wait_for(entered.wait(), 3)
    waiter = asyncio.create_task(owner.stop())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError): await waiter
    assert not owner._task.done() and all(not socket.online for socket in peer.sockets)
    release.set()
    await dispose(owner, host)
    assert owner._task.done() and owner.phase == 'STOPPED' and all(not socket.online for socket in peer.sockets)


def test_schema_five_upgrade_keeps_registration_and_starts_sequence_at_zero():
    state = state_from_json({'schema_version': 5, 'execution_executors': [dict(
        server_id='srv', connector_id='connector', registration_agent_id='agent', client_intent_id='intent',
        label='Host', executor_id='executor', state='REGISTERED')]})
    assert state.schema_version == 9
    assert state.execution_executors[0].executor_id == 'executor'
    assert state.execution_executors[0].inventory_publication_sequence == 0


@pytest.mark.parametrize('field,value', [('operation_ids', ['op']), ('session_ids', ['session']),
                                      ('stream_watermarks', [{'stream_epoch': 'stream'}]), ('cursor', 'page')])
async def test_requested_recovery_facts_never_become_an_empty_report(control, field, value):
    owner, _, _, host, _ = control
    owner.executor_id = 'executor'
    request = dict(server_id='srv', executor_id='executor', operation_ids=[], session_ids=[],
                   stream_watermarks=[], cursor=None, connection_id='connection', connection_generation=1, reconcile_id='reconcile')
    request[field] = value
    try:
        with pytest.raises(ConnectorError) as refused:
            await owner._reconcile(request)
        assert refused.value.code == ('STALE_GENERATION' if field == 'cursor' else 'RECONCILIATION_REQUIRED')
    finally:
        await dispose(owner, host)


@pytest.mark.parametrize('change', ['valid', 'not_ready', 'core', 'wire', 'format', 'bool_major', 'bool_ready'])
async def test_public_protocol_is_checked_without_credentials(change):
    payload = dict(management_revision=MANAGEMENT_REVISION, protocol_major=1, core_version=__version__,
        executor_snapshot_format=SNAPSHOT_FORMAT_VERSION, nxl_accepted=[R4_PREVIEW_REVISION], remote_execution_ready=True)
    if change == 'not_ready': payload.update(nxl_accepted=[], remote_execution_ready=False)
    elif change == 'core': payload['core_version'] = '0.0.0'
    elif change == 'wire': payload['nxl_accepted'] = ['old-wire']
    elif change == 'format': payload['executor_snapshot_format'] = 1
    elif change == 'bool_major': payload['protocol_major'] = True
    elif change == 'bool_ready': payload['remote_execution_ready'] = 1
    def reply(request):
        assert request.url.path == '/v1/connections/protocol' and 'authorization' not in request.headers
        return httpx.Response(200, json=payload, headers={'X-Nexus-Connections-Revision': MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as raw:
        async with NexusHTTPClient('https://nexus.test', client=raw) as http:
            if change in ('valid', 'not_ready'):
                assert (await http.r4_protocol()).remote_execution_ready == (change == 'valid')
            else:
                with pytest.raises(ConnectorError) as error: await http.r4_protocol()
                assert error.value.code == 'VERSION_INCOMPATIBLE'


async def test_claim_history_is_paged_without_inventing_release_proofs(control):
    from nexus_connector_core import OperationKey
    from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
    owner, _, store, host, _ = control
    journal = await host.ensure_history_journal()
    try:
        for index in range(257):
            await journal.admit(OperationKey("srv","executor",f"open-{index}"),"digest",
                                f"session-{index}",claim_session=True,
                                connection_generation=1,session_owner_generation=1)
        reporter = R4ReconciliationReporter(store,host,"srv","executor")
        request = dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,
            server_id="srv",executor_id="executor",connection_id="connection",connection_generation=2,
            reconcile_id="cycle",cursor=None,operation_ids=[],session_ids=[],stream_watermarks=[])
        reports = []
        for _ in range(3):
            report = await reporter.report(request)
            reports.append(report)
            request = {**request,"cursor":report["next_cursor"]}
        assert [len(r["claims"]) for r in reports] == [128,128,1]
        assert [r["complete"] for r in reports] == [False,False,True]
        assert len({c["session_id"] for r in reports for c in r["claims"]}) == 257
        assert all(c["state"] == "UNKNOWN" for r in reports for c in r["claims"])
        assert all(not r["stream_watermarks"] for r in reports)
        assert reporter.blocked
        with pytest.raises(ConnectorError) as refused:
            await reporter.report({**request,"cursor":reports[0]["next_cursor"]})
        assert refused.value.code == "STALE_GENERATION"
    finally:
        await dispose(owner,host)


async def test_realization_outlives_cancelled_ipc_waiter_and_fences_bootstrap_rotation(control, monkeypatch):
    from okto_nexus_connector.services.executor_onboarding import ExecutorOnboarding
    owner, peer, store, host, _ = control
    entered, release = asyncio.Event(), asyncio.Event()
    applications = []
    async def realize(self, *, bootstrap, require_current, **params):
        entered.set()
        await release.wait()
        await require_current()
        applications.append(bootstrap.ticket)
        return {"realization_ref": "opaque"}
    monkeypatch.setattr(ExecutorOnboarding, "realize", realize)
    owner.start()
    try:
        await eventually(lambda: owner.status()["control_ready"])
        observer = asyncio.create_task(owner.realize())
        await asyncio.wait_for(entered.wait(), 3)
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        assert len(owner._onboarding_tasks) == 1
        peer.sockets[0].online = False
        await eventually(lambda: owner.phase == "REGISTERING")
        assert peer.calls == 1 and not applications
        release.set()
        await eventually(lambda: peer.calls == 2 and owner.status()["control_ready"])
        assert applications == ["nxt4_synthetic_1"]
        assert not owner._onboarding_tasks
    finally:
        release.set()
        await dispose(owner, host)
