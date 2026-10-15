"""Preopening recovery preserves its original association across each commit."""
from contextlib import contextmanager

import pytest
from nexus_connector_core import CoreError, OperationKey
from nexus_connector_core.journal import open_journal
from nexus_connector_core.preopen_refusal_r4 import prepare_r4_preopen_refusal, persist_r4_preopen_refusal

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.r4_publications import R4PublicationStore
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_publication_commit_cuts import binding_for
from tests.unit.test_r4_publications import receipt


def prepare(selection, bound):
    state, _, _, frame, *_ = selection
    store = R4PublicationStore.for_state(state)
    store.reserve(frame)
    binding = binding_for(frame)
    if bound:
        store.bind(binding, stream_epoch='original-stream')
    refusal = prepare_r4_preopen_refusal(binding['source'],
        session_owner_generation=frame['session_owner_generation'], prior_binding=binding if bound else None,
        recovery={'connection_id': 'replacement', 'connection_generation': frame['connection_generation'] + 1})
    return store, frame, refusal


@pytest.mark.parametrize('bound', [False, True])
@pytest.mark.parametrize('stage', ['prepare_preopen', 'fence_preopen', 'acknowledge'])
@pytest.mark.parametrize('boundary', ['rollback', 'lost_reply'])
async def test_preopen_store_commit_cuts_preserve_history_and_exact_ack(selection, tmp_path, monkeypatch, bound, stage, boundary):
    store, frame, refusal = prepare(selection, bound)
    journal = await open_journal(tmp_path / 'preopen-core.db')
    try:
        if stage != 'prepare_preopen':
            store.prepare_preopen(refusal)
        fact = await persist_r4_preopen_refusal(journal, refusal)
        projected = store.fence_preopen(refusal, fact) if stage == 'acknowledge' else None
        transaction = store._transaction
        @contextmanager
        def cut():
            with transaction() as conn:
                yield conn
                if boundary == 'rollback':
                    raise OSError('Synthetic uncommitted write')
            raise OSError('Synthetic committed reply loss')
        monkeypatch.setattr(store, '_transaction', cut)
        arguments = {'prepare_preopen': (refusal,), 'fence_preopen': (refusal, fact), 'acknowledge': (projected,)}
        with pytest.raises((OSError, ConnectorError)):
            getattr(store, stage)(*arguments[stage])
        reopened = R4PublicationStore(store.path)
        reopened.prepare_preopen(refusal)
        frame = reopened.fence_preopen(refusal, fact)
        assert reopened.ready('srv', 'exe') == []  # Needs authenticated reconciliation, not normal receipt HTTP.
        assert not reopened.pending_unfenced('srv', 'exe')
        row = reopened.lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]
        assert row['preopen_refusal']['prior_binding'] == refusal['prior_binding']
        assert row['preopen_state'] == 'FENCED' and row['binding'] == refusal['binding']
        assert row['metadata'].get('stream_epoch') == ('original-stream' if bound else None)
        with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
            reopened.bind(binding_for({**frame, 'action': 'runtime.open'}))
        reopened.acknowledge(frame)
        assert not reopened.pending('srv', 'exe')
        assert reopened.preopen_page('srv', 'exe', generation=100) == []
    finally:
        await journal.aclose()


async def test_native_journal_winner_retains_original_binding_and_receipt(selection, tmp_path):
    store, frame, refusal = prepare(selection, True)
    store.prepare_preopen(refusal)
    journal = await open_journal(tmp_path / 'preopen-core.db')
    try:
        key = OperationKey('srv', 'exe', frame['operation_id'])
        await journal.admit(key, refusal['prior_binding']['core_intent_hash'], frame['session_id'], effect_imminent=True)
        with pytest.raises(CoreError, match='OPERATION_CONFLICT'):
            await persist_r4_preopen_refusal(journal, refusal)
        store.conflict_preopen(refusal)
        store.record(receipt(frame))
        row = store.lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]
        assert row['binding'] == refusal['prior_binding']
        assert row['preopen_state'] == 'CONFLICT' and row['receipt']['possible_effect']
        assert store.preopen_page('srv', 'exe', generation=100) == []
        assert store.ready('srv', 'exe') == [row['receipt']]
    finally:
        await journal.aclose()


def test_prepared_refusal_blocks_late_binding_without_claiming_it_is_fenced(selection):
    store, frame, refusal = prepare(selection, False)
    store.prepare_preopen(refusal)
    assert store.pending_unfenced('srv', 'exe') and store.pending('srv', 'exe')
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        store.bind(binding_for(frame))
    assert store.lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]['receipt'] is None
