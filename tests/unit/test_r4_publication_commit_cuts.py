"""T24: publication commits preserve provenance and never authorize replay."""
from contextlib import contextmanager
import hashlib

import pytest
from nexus_connector_core.protocol import canonical_json

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.r4_publications import R4PublicationStore
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_publications import receipt
from tests.unit.test_r4_execution import failed
from tests.unit.test_r4_daemon_execution import lifecycle
from okto_nexus_connector.services.r4_publications import recover_publications


def binding_for(frame):
    source = {k: frame[k] for k in ('protocol_major', 'contract_revision', 'server_id', 'executor_id',
        'operation_id', 'binding_id', 'agent_id', 'session_id', 'intent_hash', 'connection_id', 'connection_generation')}
    body = dict(schema_version=1, action=frame['action'], core_intent_hash='sha256:' + 'f' * 64, source=source)
    return body | {'digest': 'sha256:' + hashlib.sha256(canonical_json(body)).hexdigest()}


@pytest.mark.parametrize('boundary', ['rollback', 'lost_reply'])
@pytest.mark.parametrize('stage', ['reserve', 'bind', 'record', 'acknowledge'])
def test_publication_commit_cut_retains_exact_original_obligation(selection, monkeypatch, boundary, stage):
    state, _, _, frame, *_ = selection
    store = R4PublicationStore.for_state(state)
    binding, fact = binding_for(frame), receipt(frame)
    if stage != 'reserve':
        store.reserve(frame)
    if stage in ('record', 'acknowledge'):
        store.bind(binding, stream_epoch='original-stream')
    if stage == 'acknowledge':
        store.record(fact)
    transaction = store._transaction
    fired = []
    @contextmanager
    def cut():
        with transaction() as conn:
            yield conn
            if boundary == 'rollback':
                fired.append(stage)
                raise OSError('Publication writes rolled back')
        fired.append(stage)
        raise OSError('Publication committed; its confirmation was lost')
    monkeypatch.setattr(store, '_transaction', cut)
    def action(target):
        if stage == 'reserve':
            return target.reserve(frame)
        if stage == 'bind':
            return target.bind(binding, stream_epoch='original-stream')
        return getattr(target, stage)(fact)
    with pytest.raises((ConnectorError, OSError)):
        action(store)
    assert fired == [stage]
    reopened = R4PublicationStore(store.path)
    saved = reopened.lookup('srv', 'exe', [frame['operation_id']])
    if stage == 'reserve' and boundary == 'rollback':
        assert saved == {}
        reopened.reserve(frame)
    elif stage == 'reserve':
        assert saved[frame['operation_id']]['receipt'] is None
        with pytest.raises(ConnectorError, match='RECONCILIATION_REQUIRED'):
            reopened.reserve(frame)
    else:
        row = saved[frame['operation_id']]
        assert row['metadata']['intent_hash'] == frame['intent_hash']
        if stage == 'bind':
            assert row['binding'] == (None if boundary == 'rollback' else binding)
            assert row['metadata'].get('stream_epoch') == (None if boundary == 'rollback' else 'original-stream')
        elif stage == 'record':
            assert row['receipt'] == (None if boundary == 'rollback' else fact)
        else:
            assert row['acknowledged'] == (boundary == 'lost_reply')
        action(reopened)
        action(reopened)
    reopened.bind(binding, stream_epoch='original-stream')
    reopened.record(fact)
    reopened.acknowledge(fact)
    final = reopened.lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]
    assert final['binding'] == binding and final['receipt'] == fact and final['acknowledged']
    assert not reopened.pending('srv', 'exe')
    assert reopened.ready('srv', 'exe') == []
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        reopened.reserve(frame)


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('boundary', ['rollback', 'lost_reply'])
@pytest.mark.parametrize('stage', ['reserve', 'bind', 'record', 'acknowledge'])
async def test_live_publication_cut_recovers_only_facts_without_native_replay(lifecycle, monkeypatch, boundary, stage):
    owner, http, state, frame, native, _, published, _ = lifecycle
    from okto_nexus_connector.platform.paths import state_dir
    # Production creates its private state root before constructing the host.
    # A pre-reserve cut must not rely on runtime acquisition creating it later.
    state_dir(owner.host.root)
    await owner.sync()
    store = owner.owner.publications
    transaction = store._transaction
    fired = []
    def snapshot(conn):
        row = conn.execute('SELECT projection_binding,receipt,acknowledged FROM publications '
            'WHERE server_id=? AND executor_id=? AND operation_id=?',
            (frame['server_id'], frame['executor_id'], frame['operation_id'])).fetchone()
        return tuple(row) if row is not None else None
    def changed(before, after):
        if stage == 'reserve':
            return before is None and after is not None
        if before is None or after is None:
            return False
        index = {'bind': 0, 'record': 1, 'acknowledge': 2}[stage]
        return before[index] != after[index]
    @contextmanager
    def cut():
        hit = False
        with transaction() as conn:
            before = snapshot(conn)
            yield conn
            hit = not fired and changed(before, snapshot(conn))
            if hit:
                fired.append(stage)
                if boundary == 'rollback':
                    raise OSError('Publication transaction rolled back during live execution')
        if hit and boundary == 'lost_reply':
            raise OSError('Publication commit result lost during live execution')
    monkeypatch.setattr(store, '_transaction', cut)
    await owner.connection.emit(frame)
    await failed(owner.owner)
    assert fired == [stage]
    expected_opens = int(stage in ('record', 'acknowledge'))
    assert len(native.opened) == expected_opens and not native.native.sent
    retained = R4PublicationStore.for_state(state)
    async def current():
        pass
    await recover_publications(state, owner.vault, http, server_id='srv', executor_id='exe',
        require_current=current, journal=await owner.host.ensure_history_journal())
    assert len(native.opened) == expected_opens and not native.native.sent
    if expected_opens:
        assert not retained.pending('srv', 'exe')
        facts = []
        while not published.empty():
            facts.append(published.get_nowait())
        assert facts and all(fact == facts[0] for fact in facts)
        assert facts[0]['operation_id'] == frame['operation_id']
        assert facts[0]['intent_hash'] == frame['intent_hash'] and facts[0]['stage'] == 'SUBMITTED'
    else:
        assert published.empty()
        assert retained.pending('srv', 'exe') == (stage != 'reserve' or boundary == 'lost_reply')
        assert retained.ready('srv', 'exe') == []
