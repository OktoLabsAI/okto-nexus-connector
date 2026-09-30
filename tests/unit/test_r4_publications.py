"""Publication recovery never enters the native execution path."""
import asyncio
import sqlite3

import pytest
from nexus_connector_core import r4_submit_intent_hash

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.r4_publications import recover_publications
from okto_nexus_connector.storage.r4_publications import R4PublicationStore
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, failed, observed, operation
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_daemon_control import control


def receipt(frame):
    return {k: frame[k] for k in ('protocol_major', 'contract_revision', 'server_id', 'executor_id',
        'operation_id', 'binding_id', 'agent_id', 'session_id', 'intent_hash', 'connection_id',
        'connection_generation')} | dict(type='operation.receipt', receipt_revision=1,
                                        stage='SUBMITTED', possible_effect=True, retry_safe=False)


def test_pending_restart_scope_capacity_and_exact_ack(selection):
    state, _, _, frame, *_ = selection
    store = R4PublicationStore(R4PublicationStore.for_state(state).path, capacity=1)
    store.reserve(frame)
    reopened = R4PublicationStore(store.path, capacity=1)
    assert reopened.pending('srv', 'exe') and not reopened.pending('other', 'exe')
    assert reopened.ready('srv', 'exe') == []
    with pytest.raises(ConnectorError, match='RECONCILIATION_REQUIRED'):
        reopened.reserve(frame)
    changed = dict(frame, operation_id='another')
    changed['intent_hash'] = r4_submit_intent_hash(changed)
    with pytest.raises(ConnectorError, match='CAPACITY_EXCEEDED'):
        reopened.reserve(changed)
    value = receipt(frame)
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        reopened.record(dict(value, connection_generation=2))
    reopened.record(value)
    assert reopened.ready('srv', 'exe') == [value]
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        reopened.acknowledge(dict(value, stage='SUCCEEDED'))
    assert reopened.pending('srv', 'exe')
    reopened.acknowledge(value)
    assert not reopened.pending('srv', 'exe')
    reopened.reserve(changed)


def test_payload_is_not_persisted_and_receipt_corruption_fails_closed(selection):
    state, _, _, opening, *_ = selection
    frame = operation(opening, 'turn.submit', {'text': 'PRIVATE_OPERATOR_TEXT_123456'})
    frame['intent_hash'] = r4_submit_intent_hash(frame)
    store = R4PublicationStore.for_state(state)
    store.reserve(frame)
    store.record(receipt(frame))
    assert b'PRIVATE_OPERATOR_TEXT_123456' not in store.path.read_bytes()
    with sqlite3.connect(store.path) as conn:
        conn.execute("UPDATE publications SET digest='corrupted'")
    with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
        store.ready('srv', 'exe')
    assert store.pending('srv', 'exe')


def test_confirmed_history_advances_and_old_ack_cannot_erase_new_fact(selection):
    state, _, _, frame, *_ = selection
    store = R4PublicationStore.for_state(state)
    store.reserve(frame)
    first = receipt(frame)
    store.record(first)
    store.acknowledge(first)
    saved = R4PublicationStore(store.path).lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]
    assert saved['acknowledged'] and saved['receipt'] == first
    assert not store.ready('srv', 'exe') and not store.pending('srv', 'exe')
    second = dict(first, receipt_revision=2, stage='RUNNING')
    store.record(second)
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        store.acknowledge(first)
    assert store.ready('srv', 'exe') == [second]
    store.acknowledge(second)
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        store.record(dict(first, receipt_revision=3))
    terminal = dict(second, receipt_revision=3, stage='SUCCEEDED')
    store.record(terminal)
    store.acknowledge(terminal)
    assert store.lookup('srv', 'exe', [frame['operation_id']])[frame['operation_id']]['receipt'] == terminal
    assert store.lookup('foreign', 'exe', [frame['operation_id']]) == {}
    with pytest.raises(ConnectorError, match='OPERATION_CONFLICT'):
        store.reserve(frame)


def test_corrupted_confirmed_metadata_is_not_a_release_proof(selection):
    state, _, _, frame, *_ = selection
    store = R4PublicationStore.for_state(state)
    store.reserve(frame)
    store.record(receipt(frame))
    store.acknowledge(receipt(frame))
    with sqlite3.connect(store.path) as conn:
        conn.execute("UPDATE publications SET metadata=json_set(metadata,'$.session_owner_generation',2)")
    with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
        store.session_history('srv', 'exe', frame['session_id'])


async def test_publication_failure_leaves_exact_durable_receipt(execution):
    owner, connection, native, _, opening = execution
    sent = []
    async def unavailable(frame):
        sent.append(frame)
        raise OSError('The response was lost.')
    owner.publish_receipt = unavailable
    await connection.emit(opening)
    await failed(owner)
    reopened = R4PublicationStore.for_state(owner.store)
    assert reopened.ready(opening['server_id'], opening['executor_id']) == sent
    assert len(native.opened) == 1


async def test_failed_reservation_never_reaches_native_open(execution, monkeypatch):
    owner, connection, native, _, opening = execution
    def unavailable(frame):
        raise ConnectorError('JOURNAL_UNAVAILABLE', 'test', 'The disk is unavailable.')
    monkeypatch.setattr(owner.publications, 'reserve', unavailable)
    await connection.emit(opening)
    await failed(owner)
    assert not native.opened and connection.lease_count == 0


async def test_failed_binding_commit_never_reaches_native_open(execution, monkeypatch):
    owner, connection, native, _, opening = execution
    def unavailable(binding, **options):
        raise ConnectorError('JOURNAL_UNAVAILABLE', 'test', 'The disk is unavailable.')
    monkeypatch.setattr(owner.publications, 'bind', unavailable)
    await connection.emit(opening)
    await failed(owner)
    assert not native.opened
    assert owner.publications.pending(opening['server_id'], opening['executor_id'])


def test_schema_one_pending_receipt_migrates_without_inventing_hash_binding(selection):
    from nexus_connector_core.protocol import canonical_json
    from okto_nexus_connector.storage.r4_publications import _PROVENANCE
    state, _, _, opening, *_ = selection
    store = R4PublicationStore.for_state(state)
    with sqlite3.connect(store.path) as conn:
        conn.execute('CREATE TABLE publications (server_id TEXT,executor_id TEXT,operation_id TEXT,'
                     'metadata TEXT,receipt TEXT,digest TEXT,PRIMARY KEY(server_id,executor_id,operation_id))')
        conn.execute('PRAGMA user_version=1')
        conn.execute('INSERT INTO publications(server_id,executor_id,operation_id,metadata) VALUES (?,?,?,?)',
                     (opening['server_id'], opening['executor_id'], opening['operation_id'],
                      canonical_json({k: opening[k] for k in (*_PROVENANCE, 'action')}).decode()))
    assert store.pending(opening['server_id'], opening['executor_id'])
    assert store.unprojected(opening['server_id'], opening['executor_id']) == []
    store.record(receipt(opening))
    assert store.ready(opening['server_id'], opening['executor_id']) == [receipt(opening)]


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_recover_after_core_commit_before_wire_projection(lifecycle, monkeypatch):
    owner, http, state, frame, native, _, published, _ = lifecycle
    await owner.sync()
    def failed_projection_write(frame):
        raise OSError('The process lost the publication write.')
    monkeypatch.setattr(owner.owner.publications, 'record', failed_projection_write)
    await owner.connection.emit(frame)
    await failed(owner.owner)
    journal = R4PublicationStore.for_state(state)
    assert not journal.ready('srv', 'exe') and len(journal.unprojected('srv', 'exe')) == 1
    assert len(native.opened) == 1 and published.empty()
    async def current():
        pass
    await recover_publications(state, owner.vault, http, server_id='srv', executor_id='exe',
        require_current=current, journal=await owner.host.ensure_history_journal())
    recovered = published.get_nowait()
    assert recovered['operation_id'] == frame['operation_id'] and recovered['stage'] == 'SUBMITTED'
    assert recovered['intent_hash'] == frame['intent_hash']
    assert len(native.opened) == 1 and not journal.pending('srv', 'exe')


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_binding_without_core_admission_stays_pending(lifecycle, monkeypatch):
    owner, http, state, frame, native, _, published, keys = lifecycle
    await owner.sync()
    persist = owner.owner.publications.bind
    def interrupted(binding, **options):
        persist(binding, **options)
        raise OSError('The process stopped before Core admission.')
    monkeypatch.setattr(owner.owner.publications, 'bind', interrupted)
    await owner.connection.emit(frame)
    await failed(owner.owner)
    journal = R4PublicationStore.for_state(state)
    assert len(journal.unprojected('srv', 'exe')) == 1
    before = len(keys)
    async def current():
        pass
    assert await recover_publications(state, owner.vault, http, server_id='srv', executor_id='exe',
        require_current=current, journal=await owner.host.ensure_history_journal()) == {}
    assert len(keys) == before and not native.opened and published.empty()
    assert journal.pending('srv', 'exe') and not journal.ready('srv', 'exe')


async def test_failed_receipt_write_retains_reservation_and_never_publishes(execution, monkeypatch):
    owner, connection, native, published, opening = execution
    def unavailable(frame):
        raise ConnectorError('JOURNAL_UNAVAILABLE', 'test', 'The disk is unavailable.')
    monkeypatch.setattr(owner.publications, 'record', unavailable)
    await connection.emit(opening)
    await failed(owner)
    assert len(native.opened) == 1 and published.empty()
    reopened = R4PublicationStore.for_state(owner.store)
    assert reopened.pending(opening['server_id'], opening['executor_id'])
    assert reopened.ready(opening['server_id'], opening['executor_id']) == []


async def test_cancelled_shutdown_observer_retains_publication_commit(execution, monkeypatch):
    import threading
    owner, connection, native, published, opening = execution
    entered, release = threading.Event(), threading.Event()
    record = owner.publications.record
    def blocked(frame):
        record(frame)
        entered.set()
        assert release.wait(10)
    monkeypatch.setattr(owner.publications, 'record', blocked)
    try:
        await connection.emit(opening)
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(.01)
        stopping = asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert owner.pending_count == 1 and len(connection.reservations) == 1
        assert published.empty() and len(native.opened) == 1
        release.set()
        assert (await observed(published, owner))['operation_id'] == opening['operation_id']
        await owner.stop()
        assert not owner.publications.pending(opening['server_id'], opening['executor_id'])
    finally:
        release.set()


async def test_startup_recovers_before_connect_and_pending_reservation_blocks_empty(control, selection):
    from dataclasses import replace
    from okto_nexus_connector.services.execution_selection import acknowledge_execution_binding
    from okto_nexus_connector.transport.https_client import R4BindingTicket
    owner, peer, state, host, _ = control
    _, _, binding, opening, *_ = selection
    acknowledge_execution_binding(state, binding=binding)
    register = peer.register_executor
    async def registered(*args, **kwargs):
        return replace(await register(*args, **kwargs), executor_id='exe')
    peer.register_executor = registered
    journal = R4PublicationStore.for_state(state)
    journal.reserve(opening)
    owner.executor_id = 'exe'
    request = dict(server_id='srv', executor_id='exe', operation_ids=[], session_ids=[], cursor=None)
    with pytest.raises(ConnectorError, match='RECONCILIATION_REQUIRED'):
        await owner._reconcile(request)
    journal.record(receipt(opening))
    peer.origin = peer.base_url = 'https://nexus.test'
    posted = []
    async def ticket(key, **kwargs):
        return R4BindingTicket('ticket-id', 'nxt4_receipt', 'exe', binding.binding_id, 'agent',
                              600, 1, 2, tuple(kwargs['scopes']))
    async def publish(ticket, *, frame):
        posted.append(frame)
    async def connect(*args, **kwargs):
        assert posted == [receipt(opening)] and not journal.pending('srv', 'exe')
        raise ConnectorError('CONTROL_DISCONNECTED', 'test', 'The test ends before connecting.')
    peer.request_r4_binding_ticket = ticket
    peer.publish_operation_receipt = publish
    owner.connect = connect
    try:
        with pytest.raises(ConnectorError, match='CONTROL_DISCONNECTED'):
            await owner._attempt()
    finally:
        await host.shutdown_all()


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_recovery_republishes_same_fact_and_reuses_authority_for_attach(lifecycle):
    owner, http, store, frame, native, attached, published, keys = lifecycle
    journal = R4PublicationStore.for_state(store)
    value = receipt(frame)
    journal.reserve(frame)
    journal.record(value)
    async def current():
        pass
    authorities = await recover_publications(store, owner.vault, http,
        server_id='srv', executor_id='exe', require_current=current)
    assert await published.get() == value and not journal.pending('srv', 'exe')
    assert not native.opened and not attached
    owner.recovered_lanes = authorities
    await owner.sync()
    assert len(attached) == 1 and keys == ['agent-key']
    assert not native.opened


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('fault', ['lost_ack', 'revocation', 'corrupt_ticket'])
async def test_recovery_failure_preserves_obligation_and_never_opens(lifecycle, fault):
    owner, http, state, frame, native, attached, published, _ = lifecycle
    journal = R4PublicationStore.for_state(state)
    value = receipt(frame)
    journal.reserve(frame)
    journal.record(value)
    if fault == 'lost_ack':
        async def lost(ticket, *, frame):
            raise OSError('The response was lost.')
        http.publish_operation_receipt = lost
    elif fault == 'revocation':
        http.after_ticket = lambda: state.update(lambda s: setattr(s.execution_bindings[0], 'state', 'REVOKED'))
    else:
        from dataclasses import replace
        request = http.request_r4_binding_ticket
        async def wrong(*args, **kwargs):
            return replace(await request(*args, **kwargs), binding_id='foreign')
        http.request_r4_binding_ticket = wrong
    async def current():
        pass
    with pytest.raises((OSError, ConnectorError)):
        await recover_publications(state, owner.vault, http,
            server_id='srv', executor_id='exe', require_current=current)
    assert journal.ready('srv', 'exe') == [value]
    assert not native.opened and not attached and published.empty()


@pytest.mark.parametrize('selection', [True], indirect=True)
async def test_recovery_pages_more_than_256_receipts_without_reissuing_ticket(lifecycle):
    owner, http, state, frame, native, attached, published, keys = lifecycle
    journal = R4PublicationStore.for_state(state)
    def seed():
        for number in range(257):
            item = dict(frame, operation_id=f'op-{number:04}')
            item['intent_hash'] = r4_submit_intent_hash(item)
            journal.reserve(item)
            journal.record(receipt(item))
    await asyncio.to_thread(seed)
    async def current():
        pass
    await recover_publications(state, owner.vault, http,
        server_id='srv', executor_id='exe', require_current=current)
    assert published.qsize() == 257 and keys == ['agent-key']
    assert not native.opened and not attached and not journal.pending('srv', 'exe')
