"""Durable observations survive lost replies without native execution replay."""
import asyncio
import sqlite3

import pytest
from nexus_connector_core import CoreError
from nexus_connector_core.session_release import session_release_accepted, session_release_deferred
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.r4_session_releases import R4SessionReleaseStore
from okto_nexus_connector.services.r4_session_release import R4SessionReleasePublisher,retire_accepted_event
from tests.contract.test_session_release import request


def identity(frame):
    return {k: frame[k] for k in ('scope', 'connection_id', 'connection_generation')}


def test_reconstruction_preserves_unknown_attempt_until_correlated_deferral(tmp_path):
    path = tmp_path / 'releases.sqlite3'
    original = request()
    newer = dict(original, release_id='newer', sequence=3)
    outbox = R4SessionReleaseStore(path)
    assert outbox.prepare(original) == original
    outbox = R4SessionReleaseStore(path)
    assert outbox.pending(**identity(original)) == original
    assert outbox.prepare(newer) == original
    with pytest.raises(ConnectorError, match='SCOPE_MISMATCH'):
        outbox.acknowledge(session_release_accepted(newer))
    with pytest.raises(ConnectorError, match='SCOPE_MISMATCH'):
        outbox.prepare(dict(original, sequence=4))
    outbox.acknowledge(session_release_deferred(original))
    assert outbox.prepare(newer) == newer
    # Old duplicates cannot settle the new observation or erase its history.
    outbox.acknowledge(session_release_deferred(original))
    assert outbox.pending(**identity(newer)) == newer
    outbox.acknowledge(session_release_accepted(newer))
    outbox.acknowledge(session_release_accepted(newer))
    assert outbox.pending(**identity(newer)) is None
    assert outbox.prepare(original) == newer
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT release_id,state FROM releases ORDER BY release_id').fetchall() == [
            ('newer', 'ACCEPTED'), ('original', 'DEFERRED')]


@pytest.mark.parametrize('field', ['scope', 'connection_id', 'connection_generation'])
def test_pending_report_is_never_adopted_by_another_scope_or_channel(tmp_path, field):
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    original = request()
    outbox.prepare(original)
    changed = identity(original)
    if field == 'scope': changed[field] = dict(changed[field], session_id='other')
    elif field == 'connection_generation': changed[field] += 1
    else: changed[field] = 'other'
    assert outbox.pending(**changed) is None
    assert outbox.pending(**identity(original)) == original


async def test_accepted_release_retires_event_stream_idempotently_after_interrupted_marker(tmp_path,monkeypatch):
    from nexus_connector_core import OperationKey, SessionKey
    from nexus_connector_core.journal import SQLiteJournal
    from okto_nexus_connector.storage.state_store import StateStore
    from okto_nexus_connector.storage.r4_events import R4EventStore,FIELDS

    state=StateStore(tmp_path/'state.json')
    outbox=R4SessionReleaseStore.for_state(state)
    events=R4EventStore.for_state(state)
    accepted=request()
    journal=SQLiteJournal(tmp_path/'journal.db')
    await journal.admit(OperationKey('srv','exe',accepted['opening_operation_id']),
                        'intent',accepted['scope']['session_id'],claim_session=True)
    scope={k:accepted['scope'][k] for k in FIELDS if k!='stream_epoch'} | dict(stream_epoch=accepted['stream_epoch'])
    events.register(scope)
    events.advance(scope,remote_acked=accepted['sequence'])
    events.advance(scope,core_applied=accepted['sequence'])
    outbox.prepare(accepted)
    outbox.acknowledge(session_release_accepted(accepted))
    original=R4SessionReleaseStore.mark_checkpoints_retired
    failed=False
    def interrupted(self,frame):
        nonlocal failed
        if not failed:
            failed=True
            raise OSError('Injected marker interruption after event checkpoint.')
        return original(self,frame)
    monkeypatch.setattr(R4SessionReleaseStore,'mark_checkpoints_retired',interrupted)
    with pytest.raises(OSError):
        await retire_accepted_event(state,accepted,journal)
    assert events.page('srv','exe')[0]==[]
    assert events.read(scope)['core_applied']==accepted['sequence']
    assert (await journal.claimed_sessions('srv','exe',pending_only=True)).claims==()
    assert len(outbox.accepted_event_page('srv','exe'))==1
    await retire_accepted_event(state,accepted,journal)
    assert outbox.accepted_event_page('srv','exe')==[]
    assert (await journal.get_session_claim(SessionKey('srv','exe',accepted['scope']['session_id']))) is not None
    journal.close()


async def test_reconciliation_reports_pending_claims_without_replaying_accepted_history(tmp_path):
    from nexus_connector_core import OperationKey,R4_PREVIEW_REVISION,SessionKey
    from okto_nexus_connector.services.core_host import CoreRuntimeHost
    from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
    from okto_nexus_connector.storage.state_store import StateStore,ConnectorState

    store=StateStore(tmp_path/'state.json')
    store.save(ConnectorState(connector_id='connector'))
    host=CoreRuntimeHost(tmp_path,None)
    try:
        journal=await host.ensure_history_journal()
        await journal.admit(OperationKey('srv','exe','open'),'intent','session',claim_session=True)
        await journal.retire_reconciliation_claim(SessionKey('srv','exe','session'),
            operation_id='open',release_id='accepted',release_digest='a'*64)
        reporter=R4ReconciliationReporter(store,host,'srv','exe')
        frame=dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,
            server_id='srv',executor_id='exe',connection_id='replacement',
            connection_generation=2,reconcile_id='cycle',cursor=None,
            operation_ids=[],session_ids=[],stream_watermarks=[])
        report=await reporter.report(frame)
        assert report['complete'] and report['claims']==[] and report['ownership_facts']==[]
        assert len((await journal.claimed_sessions('srv','exe')).claims)==1
    finally:
        await host.shutdown_all()


async def test_reconciliation_claim_pages_ignore_1k_10k_100k_terminal_history(tmp_path):
    from nexus_connector_core import R4_PREVIEW_REVISION
    from okto_nexus_connector.services.core_host import CoreRuntimeHost
    from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
    from okto_nexus_connector.storage.state_store import StateStore,ConnectorState

    store=StateStore(tmp_path/'state.json')
    store.save(ConnectorState(connector_id='connector'))
    host=CoreRuntimeHost(tmp_path,None)
    try:
        journal=await host.ensure_history_journal()
        previous=0
        for total in (1_000,10_000,100_000):
            for start in range(previous,total,10_000):
                stop=min(start+10_000,total)
                def seed(db):
                    db.execute('BEGIN IMMEDIATE')
                    try:
                        db.executemany('INSERT INTO session_claims('
                            'server_id,executor_id,session_id,operation_id,reconciliation_pending,'
                            'release_id,release_digest) VALUES (?,?,?,?,?,?,?)',(
                            ('srv','exe',f'session-{i:06d}',f'open-{i:06d}',int(i<3),
                             None if i<3 else f'release-{i}',None if i<3 else 'a'*64)
                            for i in range(start,stop)))
                        db.execute('COMMIT')
                    except BaseException:
                        db.execute('ROLLBACK')
                        raise
                journal._run_sync(seed,timeout=60)
            previous=total
            reporter=R4ReconciliationReporter(store,host,'srv','exe')
            frame=dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,
                server_id='srv',executor_id='exe',connection_id='replacement',
                connection_generation=2,reconcile_id=f'cycle-{total}',cursor=None,
                operation_ids=[],session_ids=[],stream_watermarks=[])
            report=await reporter.report(frame)
            assert report['complete'] and len(report['claims'])==3
            assert [claim['session_id'] for claim in report['claims']] == [
                'session-000000','session-000001','session-000002']
    finally:
        await host.shutdown_all()


@pytest.mark.parametrize('tamper', ['digest', 'identity', 'state', 'reply'])
def test_corrupted_publication_cannot_become_release_evidence(tmp_path, tamper):
    path = tmp_path / 'releases.sqlite3'
    outbox = R4SessionReleaseStore(path)
    original = request()
    outbox.prepare(original)
    with sqlite3.connect(path) as conn:
        if tamper == 'digest': conn.execute("UPDATE releases SET digest='bad'")
        elif tamper == 'identity': conn.execute("UPDATE releases SET release_id='other'")
        elif tamper == 'state': conn.execute("UPDATE releases SET reply=?", (b'{}',))
        else:
            from nexus_connector_core.session_release import encode_session_release
            conn.execute('UPDATE releases SET reply=?', (encode_session_release(session_release_accepted(original)),))
    with pytest.raises((ConnectorError, CoreError)) as caught:
        outbox.pending(**identity(original))
    assert getattr(caught.value, 'code', None) in ('JOURNAL_UNAVAILABLE', 'VALIDATION_ERROR')


async def test_publisher_reconstructs_original_after_lost_ack_then_rebuilds_only_after_deferral(tmp_path):
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    original, stop = request(), asyncio.Event()
    reports, sends = [], []
    class Reporter:
        async def report(self, **kwargs):
            reports.append(kwargs)
            return original if len(reports) == 1 else dict(original, release_id='advanced', sequence=3)
    async def lost(frame, *, stop_event):
        sends.append(frame)
        raise ConnectorError('CONTROL_DISCONNECTED', 'test', 'Lost acknowledgment.')
    with pytest.raises(ConnectorError, match='CONTROL_DISCONNECTED'):
        await R4SessionReleasePublisher(outbox, Reporter(), lost).run(**identity(original), stop_event=stop)
    assert len(reports) == 1 and sends == [original]
    async def recovered(frame, *, stop_event):
        sends.append(frame)
        return session_release_deferred(frame) if frame == original else session_release_accepted(frame)
    reconstructed = R4SessionReleaseStore(outbox.path)
    assert await R4SessionReleasePublisher(reconstructed, Reporter(), recovered).run(
        **identity(original), stop_event=stop)
    assert sends[:2] == [original, original] and len(sends) == 3 and len(reports) == 2
    assert sends[-1]['release_id'] == 'advanced'
    assert reconstructed.pending(**identity(original)) is None


async def test_shutdown_after_report_commit_preserves_exact_request_without_sending(tmp_path):
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    original, stop = request(), asyncio.Event()
    class Reporter:
        async def report(self, **kwargs):
            stop.set()
            return original
    async def forbidden(*args, **kwargs):
        raise AssertionError('No send is allowed after owner shutdown.')
    assert not await R4SessionReleasePublisher(outbox, Reporter(), forbidden).run(
        **identity(original), stop_event=stop)
    assert R4SessionReleaseStore(outbox.path).pending(**identity(original)) == original


async def test_failed_local_ack_commit_replays_original_already_accepted_report(tmp_path):
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    original, stop, sends = request(), asyncio.Event(), []
    outbox.prepare(original)
    with sqlite3.connect(outbox.path) as conn:
        conn.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON releases BEGIN SELECT RAISE(ABORT,'fault'); END")
    class Reporter:
        async def report(self, **kwargs):
            raise AssertionError('An unknown ACK commit cannot rebuild the report.')
    async def server_accepts(frame, *, stop_event):
        sends.append(frame)
        return session_release_accepted(frame)
    with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
        await R4SessionReleasePublisher(outbox, Reporter(), server_accepts).run(
            **identity(original), stop_event=stop)
    assert outbox.pending(**identity(original)) == original
    with sqlite3.connect(outbox.path) as conn:
        conn.execute('DROP TRIGGER fail_ack')
    rebuilt = R4SessionReleaseStore(outbox.path)
    assert await R4SessionReleasePublisher(rebuilt, Reporter(), server_accepts).run(
        **identity(original), stop_event=stop)
    assert sends == [original, original] and rebuilt.pending(**identity(original)) is None
