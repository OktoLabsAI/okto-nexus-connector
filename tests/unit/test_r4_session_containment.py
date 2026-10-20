"""A correlated denied lease retains only its session's containment producer."""
import asyncio
from dataclasses import replace

import pytest
from nexus_connector_core import CoreError, SessionKey, r4_submit_intent_hash

from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_execution import observed, operation, Native
from tests.unit.test_r4_lease_renewal import setup, renewal_entered, LeaseClock
from okto_nexus_connector.services.r4_session_release import R4SessionReleaseReporter
from okto_nexus_connector.storage.r4_session_releases import R4SessionReleaseStore
from okto_nexus_connector.errors import ConnectorError
from nexus_connector_core.session_release import SESSION_RELEASE_REVISION, session_release_accepted


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('refusal', ['scoped', 'expired_scoped', 'revalidation_scoped'])
async def test_scoped_lease_failure_retries_failed_stop_without_closing_sibling(lifecycle, monkeypatch, refusal):
    factory = lifecycle[4]
    async def independent_peer(prepared, session_id, context, *, stream_epoch):
        factory.native = Native()
        factory.opened.append(prepared)
        return factory.native
    monkeypatch.setattr(factory, 'open', independent_peer)
    renewal_gate = asyncio.Event()
    daemon, store, frame, native, published, session, renewing, allow_denial, _ = await setup(
        lifecycle, monkeypatch, hold=True, refuse=refusal, clock=LeaseClock(), renewal_gate=renewal_gate)
    first_peer = native.native
    original_stop = session.runtime.shutdown
    entered, release = asyncio.Event(), asyncio.Event()
    attempts = []
    async def stop(policy):
        attempts.append(policy)
        if len(attempts) == 1:
            entered.set()
            await release.wait()
            raise CoreError('RECONCILIATION_REQUIRED', 'delayed_session_stop')
        return await original_stop(policy)
    monkeypatch.setattr(session.runtime, 'shutdown', stop)
    try:
        await daemon.connection.emit(operation(frame, 'turn.submit', {'text': 'Work to contain'},
                                                operation_id='original-work'))
        assert (await observed(published, daemon.owner))['operation_id'] == 'original-work'
        sibling = dict(frame, session_id='sibling', operation_id='sibling-open')
        sibling['intent_hash'] = r4_submit_intent_hash(sibling)
        await daemon.connection.emit(sibling)
        receipt = await observed(published, daemon.owner)
        assert receipt['operation_id'] == 'sibling-open'
        sibling_peer = native.native
        # Establish both executions before holding a renewal. Otherwise a
        # slower host can correctly block original-work behind the held lease.
        renewal_gate.set()
        await renewal_entered(daemon, renewing)
        await daemon.connection.emit(operation(frame, 'turn.submit', {'text':'Queued before lease failure'},
                                               operation_id='queued-work'))
        allow_denial.set()
        await asyncio.wait_for(entered.wait(), 3)
        queued = await observed(published, daemon.owner)
        assert queued['operation_id'] == 'queued-work' and queued['stage'] == 'FAILED'
        assert not queued['possible_effect'] and queued['retry_safe']
        assert queued['error_code'] == ('LEASE_EXPIRED' if refusal == 'expired_scoped' else 'LEASE_REVALIDATION_REQUIRED')
        assert session.stopped.is_set() and not session.contained
        assert daemon.ready and daemon.owner.failure is None and daemon.connection.online
        ledger = await daemon.host.ensure_ledger()
        slots = await ledger.owned_slot_page()
        assert {slot.key.session_id for slot in slots.reservations} == {frame['session_id'], 'sibling'}
        reporter = R4SessionReleaseReporter(store, daemon.host)
        scope = {k: frame[k] for k in ('server_id','executor_id','binding_id','agent_id','workspace_id',
            'workspace_binding_id','session_id','session_owner_generation','authorization_revision',
            'configuration_revision','binding_revision','credential_epoch')}
        args = dict(scope=scope, connection_id=frame['connection_id'],
                    connection_generation=frame['connection_generation'])
        with pytest.raises(CoreError, match='RECONCILIATION_REQUIRED'):
            await reporter.report(**args)
        turn = operation(sibling, 'turn.submit', {'text': 'Healthy sibling work'})
        await daemon.connection.emit(turn)
        assert (await observed(published, daemon.owner))['operation_id'] == 'turn.submit'
        assert sibling_peer.sent == [('send_turn', {'text': 'Healthy sibling work'}, 'turn.submit', None)]
        assert not first_peer.stopped
        assert len(attempts) == 1
        release.set()
        async with asyncio.timeout(5):
            while session.containment_error is None:
                await asyncio.sleep(.01)
        assert not session.contained and not first_peer.stopped and daemon.ready
        assert len((await ledger.owned_slot_page()).reservations) == 2
        await asyncio.wait_for(asyncio.shield(session.renewal), 8)
        if daemon.owner.failure is not None:
            raise daemon.owner.failure
        assert session.contained and session.containment_error is None and len(attempts) == 2, dict(
            attempts=len(attempts), owner_failure=repr(daemon.owner.failure),
            connection_failure=repr(getattr(daemon.connection, 'failure', None)),
            online=daemon.connection.online, stopping=daemon.owner._stopping)
        assert first_peer.stopped and not sibling_peer.stopped and daemon.ready
        assert {slot.key.session_id for slot in (await ledger.owned_slot_page()).reservations} == {'sibling'}
        assert len(native.opened) == 2
        snapshot = await session.runtime.inspect(SessionKey(frame['server_id'], frame['executor_id'], frame['session_id']))
        assert snapshot.ownership == 'released'
        journal = await daemon.host.ensure_history_journal()
        assert await journal.get_session_claim(SessionKey(frame['server_id'], frame['executor_id'], 'absent')) is None
        async def no_global_scan(*args, **kwargs):
            raise AssertionError('A scoped release must not scan all session claims.')
        monkeypatch.setattr(journal, 'claimed_sessions', no_global_scan)
        async with asyncio.timeout(5):
            while True:
                try:
                    report = await reporter.report(**args)
                    break
                except CoreError as error:
                    assert error.code in ('RECONCILIATION_REQUIRED', 'EVENT_GAP')
                    await asyncio.sleep(.05)
        assert report['scope'] == scope and report['opening_operation_id'] == frame['operation_id']
        assert report['receipt_count'] == 3  # Opening, original work and durable queued refusal.
        assert await reporter.report(**args) == report
        with pytest.raises(CoreError, match='SCOPE_MISMATCH'):
            await reporter.report(**dict(args, scope=dict(scope, binding_id='foreign')))
        read_progress = reporter.events.read
        monkeypatch.setattr(reporter.events, 'read', lambda scope: dict(read_progress(scope), core_applied=-1))
        with pytest.raises(CoreError, match='EVENT_GAP'):
            await reporter.report(**args)
        assert daemon.ready and daemon.connection.online and not sibling_peer.stopped
        # A real interrupted-turn fact may arrive. No synthetic close or
        # successful business completion is produced by lease containment.
        while not published.empty():
            fact = published.get_nowait()
            assert fact['operation_id'] == 'original-work' and fact['stage'] != 'SUCCEEDED'
    finally:
        allow_denial.set()
        release.set()


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('shutdown_before_ack', [False, True])
async def test_release_publication_is_durable_and_does_not_occupy_sibling_execution(
        lifecycle, monkeypatch, shutdown_before_ack):
    daemon, _, store, frame, factory, _, _, _ = lifecycle
    daemon.connection.state = replace(daemon.connection.state, control_capabilities=(SESSION_RELEASE_REVISION,))
    async def independent_peer(prepared, session_id, context, *, stream_epoch):
        factory.native = Native()
        factory.opened.append(prepared)
        return factory.native
    monkeypatch.setattr(factory, 'open', independent_peer)
    sent, permit_reply = asyncio.Queue(), asyncio.Event()
    outbox = R4SessionReleaseStore.for_state(store)
    async def held_publish(request, *, stop_event):
        identity = {k: request[k] for k in ('scope','connection_id','connection_generation')}
        assert outbox.pending(**identity) == request  # Already durable before transport.
        await sent.put(request)
        if shutdown_before_ack:
            await stop_event.wait()
            raise ConnectorError('PUBLICATION_INTERRUPTED', 'test', 'Owner stopped.')
        await permit_reply.wait()
        return session_release_accepted(request)
    daemon.connection.publish_session_release = held_publish
    daemon, _, frame, _, published, session, renewing, allow_denial, _ = await setup(
        lifecycle, monkeypatch, hold=True, refuse='scoped', clock=LeaseClock())
    first_peer = factory.native
    shutdowns = []
    original_shutdown = session.runtime.shutdown
    async def counted_shutdown(policy):
        shutdowns.append(policy)
        return await original_shutdown(policy)
    monkeypatch.setattr(session.runtime, 'shutdown', counted_shutdown)
    try:
        sibling = dict(frame, session_id='sibling', operation_id='sibling-open')
        await daemon.connection.emit(sibling)
        assert (await observed(published, daemon.owner))['operation_id'] == 'sibling-open'
        sibling_peer = factory.native
        await renewal_entered(daemon, renewing)
        allow_denial.set()
        report = await asyncio.wait_for(sent.get(), 8)
        assert session.contained and not session.release_accepted and first_peer.stopped
        assert not sibling_peer.stopped and daemon.ready and len(shutdowns) == 1
        turn = operation(sibling, 'turn.submit', {'text': 'Work while the other release is unacknowledged'})
        await daemon.connection.emit(turn)
        assert (await observed(published, daemon.owner))['operation_id'] == 'turn.submit'
        assert len(sibling_peer.sent) == 1 and len(factory.opened) == 2
        if shutdown_before_ack:
            await asyncio.wait_for(daemon.owner.stop(), 3)
            assert not session.release_accepted
            assert R4SessionReleaseStore(outbox.path).pending(**{k: report[k] for k in
                ('scope','connection_id','connection_generation')}) == report
        else:
            permit_reply.set()
            await asyncio.wait_for(asyncio.shield(session.renewal), 3)
            assert session.release_accepted and session.containment_error is None
            assert outbox.pending(**{k: report[k] for k in
                ('scope','connection_id','connection_generation')}) is None
        assert daemon.connection.online and daemon.owner.failure is None and len(shutdowns) == 1
    finally:
        allow_denial.set()
        permit_reply.set()


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('fail_commit', [False, True])
async def test_new_channel_reconciles_original_release_without_replaying_native_execution(
        lifecycle, monkeypatch, fail_commit):
    import sqlite3
    from nexus_connector_core import SNAPSHOT_FORMAT_VERSION
    from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
    from okto_nexus_connector.transport.wss_r4 import negotiate_r4_control
    from tests.contract.test_wss_r4 import Peer, MANAGEMENT
    daemon, _, store, frame, factory, _, _, _ = lifecycle
    daemon.connection.state = replace(daemon.connection.state, control_capabilities=(SESSION_RELEASE_REVISION,))
    sent = asyncio.Queue()
    async def unacknowledged(request, *, stop_event):
        await sent.put(request)
        await stop_event.wait()
        raise ConnectorError('PUBLICATION_INTERRUPTED', 'test', 'Connection owner stopped.')
    daemon.connection.publish_session_release = unacknowledged
    daemon, _, _, _, _, session, renewing, allow_denial, _ = await setup(
        lifecycle, monkeypatch, hold=True, refuse='scoped', clock=LeaseClock())
    try:
        await renewal_entered(daemon, renewing)
        allow_denial.set()
        original = await asyncio.wait_for(sent.get(), 8)
        await asyncio.wait_for(daemon.owner.stop(), 3)
        assert session.contained and not session.release_accepted and factory.native.stopped
        outbox = R4SessionReleaseStore.for_state(store)
        identity = {k: original[k] for k in ('scope','connection_id','connection_generation')}
        assert outbox.pending(**identity) == original
        await daemon.close()
        if fail_commit:
            with sqlite3.connect(outbox.path) as conn:
                conn.execute("CREATE TRIGGER fail_recovery BEFORE UPDATE ON releases BEGIN SELECT RAISE(ABORT,'fault'); END")
        for attempt in range(2 if fail_commit else 1):
            reporter = R4ReconciliationReporter(store, daemon.host, frame['server_id'], frame['executor_id'])
            peer = Peer()
            peer.connection = dict(connection_id=f'replacement-{attempt}',
                connection_generation=frame['connection_generation'] + attempt + 1)
            coroutine = negotiate_r4_control(peer, server_id=frame['server_id'], executor_id=frame['executor_id'],
                management_revision=MANAGEMENT, snapshot_format=SNAPSHOT_FORMAT_VERSION,
                boot_id=f'boot-{attempt}', report_reconciliation=reporter.report,
                reconciliation_accepted=reporter.accepted)
            if fail_commit and attempt == 0:
                with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
                    await asyncio.wait_for(coroutine, 8)
                assert outbox.pending(**identity) == original
                with sqlite3.connect(outbox.path) as conn:
                    assert conn.execute('SELECT recovery FROM releases').fetchone() == (None,)
                    conn.execute('DROP TRIGGER fail_recovery')
            else:
                state = await asyncio.wait_for(coroutine, 8)
                assert state.control_ready and state.connection_generation > original['connection_generation']
        assert R4SessionReleaseStore(outbox.path).pending(**identity) is None
        assert len(factory.opened) == 1 and factory.native.sent == []
        assert not session.release_accepted  # No invented old-channel live ACK.
        with sqlite3.connect(outbox.path) as conn:
            row = conn.execute('SELECT request,recovery FROM releases').fetchone()
        import json
        assert json.loads(row[0]) == original and json.loads(row[1])['ack']['connection_id'] == state.connection_id
    finally:
        allow_denial.set()
