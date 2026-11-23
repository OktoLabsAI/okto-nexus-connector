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


def test_private_codex_witness_stays_separate_from_the_release_report(tmp_path):
    path = tmp_path / 'releases.sqlite3'
    original = request()
    outbox = R4SessionReleaseStore(path)
    assert outbox.prepare(original) == original
    witness = {'scope': original['scope'], 'rollout': {'sha256': 'a' * 64}}
    outbox.record_saved_codex_context(witness)
    rebuilt = R4SessionReleaseStore(path)
    rebuilt.record_saved_codex_context(witness)
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        rebuilt.record_saved_codex_context(witness | {'rollout': {'sha256': 'b' * 64}})
    assert rebuilt.pending(**identity(original)) == original
    rebuilt.acknowledge(session_release_accepted(original))
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT COUNT(*) FROM saved_codex_contexts').fetchone() == (1,)
        assert conn.execute('PRAGMA user_version').fetchone() == (6,)


async def test_saved_codex_grant_fences_two_successors_and_stages_only_the_owned_file(tmp_path, monkeypatch):
    from dataclasses import asdict, replace
    import json
    import sys
    from types import SimpleNamespace
    from nexus_connector_core import InstallationCandidate, LaunchIntent, PreparedLaunch
    from nexus_connector_core.native.codex_rollout import observe_rollout, saved_launch_configuration
    from okto_nexus_connector.services import r4_saved_codex

    home = tmp_path / 'source-profile'
    path = home / 'sessions' / 'rollout-saved.jsonl'
    path.parent.mkdir(parents=True)
    thread = 'bdfe5b81-54cf-43cf-8001-5de670d3dd69'
    header = dict(type='session_meta', payload=dict(id=thread, cwd=str(tmp_path)))
    path.write_text(json.dumps(header) + '\n{"type":"response_item"}\n', encoding='utf-8')
    (home / 'auth.json').write_text('fixture-only', encoding='utf-8')
    rollout = observe_rollout(str(home), str(path), thread, str(tmp_path))
    assert rollout is not None
    original = request()
    scope = original['scope']
    candidate = InstallationCandidate('codex_app_server', 'test-bin', 'fingerprint', 'explicit', 'selected',
                                      version='fixture', architecture='fixture')
    monkeypatch.setattr(r4_saved_codex, 'CODEX_SAVED_HISTORY_BUILD',
                        ('fixture', sys.platform, 'fixture', 'fingerprint'))
    prepared = PreparedLaunch(LaunchIntent(scope['agent_id'], scope['workspace_id'], 'codex_app_server'),
        candidate, ('test-bin',), str(tmp_path), str(tmp_path), 'workspace', 'old-profile', ('mcp-cap:old',))
    changed_capability = replace(prepared, profile_fingerprint='new-profile', secret_refs=('mcp-cap:new',))
    source = dict(close_operation_id='close', resource_release_digest=original['proof_digest'])
    witness = dict(source, scope=scope, rollout=asdict(rollout), workspace_root=str(tmp_path),
                   opening_profile=saved_launch_configuration(prepared))
    store = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    store.record_saved_codex_context(witness)

    def opening(session):
        frame = dict(scope, session_id=session, operation_id='open-' + session)
        return r4_saved_codex.SavedCodexOpening(R4SessionReleaseStore(store.path), frame, source, scope,
                                               require_current=lambda: None)

    left, right = opening('first'), opening('second')
    context = SimpleNamespace(**scope)
    outcomes = await asyncio.gather(left.grant(changed_capability, 'first', context),
        right.grant(changed_capability, 'second', context), return_exceptions=True)
    winners = [(candidate, grant) for candidate, grant in zip((left, right), outcomes)
               if not isinstance(grant, BaseException)]
    assert len(winners) == 1
    assert [value.code for value in outcomes if isinstance(value, CoreError)] == ['OPERATION_CONFLICT']
    winner, grant = winners[0]
    restored = opening(grant.session_id)
    assert await restored.grant(changed_capability, grant.session_id, context) == grant

    target = tmp_path / 'owned-successor'
    target.mkdir()
    owned = SimpleNamespace(home=target, require_current=lambda: None)

    async def environment(_):
        return {'CODEX_HOME': str(target)}

    with pytest.raises(CoreError, match='PROFILE_DRIFT'):
        await winner.environment(environment, changed_capability, None)
    await winner.environment(environment, changed_capability, owned)
    assert (target / 'sessions' / path.name).read_bytes() == path.read_bytes()
    assert not (target / 'auth.json').exists()
    with path.open('ab') as changed:
        changed.write(b'{"type":"file_changed"}\n')
    with pytest.raises(CoreError, match='PROFILE_DRIFT'):
        await restored.grant(changed_capability, grant.session_id, context)


async def test_saved_claude_grant_rechecks_private_proof_and_fences_one_successor(tmp_path, monkeypatch):
    from dataclasses import asdict, replace
    from types import SimpleNamespace
    from nexus_connector_core import InstallationCandidate, LaunchIntent, PreparedLaunch
    from nexus_connector_core.native.claude_saved_history import ClaudeSavedHistory
    from nexus_connector_core.native.codex_rollout import saved_launch_configuration
    from nexus_connector_core.saved_context_link import saved_native_session_digest
    from okto_nexus_connector.services import r4_saved_claude

    original = request()
    scope = original['scope']
    native_uuid = '51131bb3-7843-4c0e-a09b-83ec580e7c75'
    history = ClaudeSavedHistory(native_uuid, str(tmp_path/'profile'), str(tmp_path),
                                (1, 1), (1, 2), (1, 3), 100, 'a' * 64)
    profile = {'state_home': history.state_home, 'settings_sha256': 'a' * 64}
    monkeypatch.setattr(r4_saved_claude, 'observe_claude_resume_profile', lambda *_: profile)
    monkeypatch.setattr(r4_saved_claude, 'observe_claude_saved_history', lambda *_: history)
    monkeypatch.setattr(r4_saved_claude, 'reviewed_claude_terminal_candidate', lambda candidate: True)
    candidate = InstallationCandidate('claude_stream', 'test-bin', 'fingerprint', 'explicit', 'selected',
                                      version='fixture', architecture='fixture')
    prepared = PreparedLaunch(LaunchIntent(scope['agent_id'], scope['workspace_id'], 'claude_stream'),
        candidate, ('test-bin',), str(tmp_path), str(tmp_path), 'workspace', 'old-profile', ('mcp-cap:old',))
    opening_profile = dict(saved_launch_configuration(prepared), claude_resume_profile=profile)
    source = dict(close_operation_id='close', resource_release_digest=original['proof_digest'])
    store = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    store.record_saved_context('claude_stream', dict(source, scope=scope, history=asdict(history),
                                                    opening_profile=opening_profile))
    source['native_session_digest'] = saved_native_session_digest(native_uuid)
    assert r4_saved_claude.probe_saved_claude(store, dict(scope=scope, source=source))
    assert not r4_saved_claude.probe_saved_claude(store,
        dict(scope=scope, source=source | {'native_session_digest': 'sha256:' + 'b' * 64}))
    assert store.saved_codex_context(scope, source) is None

    frames = [dict(scope, session_id=session, operation_id='open-' + session) for session in ('a', 'b')]
    def require_current():
        pass

    async def grant(frame, approved=profile):
        opening = r4_saved_claude.SavedClaudeOpening(R4SessionReleaseStore(store.path), frame, source, scope,
                                                   require_current=require_current)
        target = replace(prepared, profile_fingerprint='new-profile', secret_refs=('mcp-cap:new',))
        return await opening.grant(target, frame['session_id'], SimpleNamespace(**scope),
                                   profile=approved, require_profile_current=require_current)

    with pytest.raises(CoreError, match='PROFILE_DRIFT'):
        await grant(frames[0], profile | {'settings_sha256': 'd' * 64})
    outcomes = await asyncio.gather(*(grant(frame) for frame in frames), return_exceptions=True)
    winners = [value for value in outcomes if not isinstance(value, BaseException)]
    assert len(winners) == 1 and winners[0].history == history
    assert [value.code for value in outcomes if isinstance(value, CoreError)] == ['OPERATION_CONFLICT']
    winner = next(frame for frame in frames if frame['session_id'] == winners[0].session_id)
    assert await grant(winner) == winners[0]

    monkeypatch.setattr(r4_saved_claude, 'observe_claude_saved_history', lambda *_: replace(history, byte_size=200))
    assert not r4_saved_claude.probe_saved_claude(store, dict(scope=scope, source=source))
    with pytest.raises(CoreError, match='PROFILE_DRIFT'):
        await grant(winner)


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
