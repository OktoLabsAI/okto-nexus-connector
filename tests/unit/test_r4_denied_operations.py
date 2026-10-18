"""Correlated lease refusals settle only the original queued operation."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from nexus_connector_core import OperationKey, SessionKey, CapacityConsumption, r4_lease_renew_frame
from nexus_connector_core.session_release import SESSION_RELEASE_REVISION, session_release_accepted
from okto_nexus_connector.transport.lease_denial import LeaseRequestDenied
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_execution import Native, operation, observed
from tests.unit.test_r4_lease_renewal import setup, renewal_entered, LeaseClock


async def matching(queue, owner, operation_id):
    async with asyncio.timeout(15):
        while True:
            receipt = await observed(queue, owner)
            if receipt['operation_id'] == operation_id:
                return receipt


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('refuse', ['scoped', 'expired_scoped', 'revalidation_scoped'])
async def test_queued_turn_is_refused_before_native_send_and_sibling_remains_available(lifecycle, monkeypatch, refuse):
    daemon, _, store, opening, factory, _, _, _ = lifecycle
    daemon.connection.state = replace(daemon.connection.state, control_capabilities=(SESSION_RELEASE_REVISION,))
    async def peer(prepared, session_id, context, *, stream_epoch):
        factory.native = Native()
        factory.opened.append(prepared)
        return factory.native
    monkeypatch.setattr(factory, 'open', peer)
    release_reports = []
    async def publish_release(report, *, stop_event):
        release_reports.append(report)
        return session_release_accepted(report)
    daemon.connection.publish_session_release = publish_release
    daemon, _, frame, _, published, session, renewing, allow_denial, _ = await setup(
        lifecycle, monkeypatch, hold=True, refuse=refuse, clock=LeaseClock())
    first_peer = factory.native
    try:
        sibling = dict(frame, session_id='healthy', operation_id='healthy-open')
        await daemon.connection.emit(sibling)
        await matching(published, daemon.owner, 'healthy-open')
        sibling_peer = factory.native
        await renewal_entered(daemon, renewing)
        queued = operation(frame, 'turn.submit', {'text': 'Must never reach the revoked session'}, operation_id='queued')
        await daemon.connection.emit(queued)
        async with asyncio.timeout(5):
            while not await asyncio.to_thread(daemon.owner.publications.lookup, frame['server_id'], frame['executor_id'], ['queued']):
                await asyncio.sleep(.01)
        assert not first_peer.sent
        allow_denial.set()
        refusal = await matching(published, daemon.owner, 'queued')
        assert refusal['stage'] == 'FAILED'
        assert refusal['error_code'] == ('LEASE_EXPIRED' if refuse == 'expired_scoped' else 'LEASE_REVALIDATION_REQUIRED')
        assert refusal['retry_safe'] and not refusal['possible_effect'] and not first_peer.sent
        await daemon.connection.emit(operation(sibling, 'turn.submit', {'text': 'Still available'}, operation_id='healthy-turn'))
        assert (await matching(published, daemon.owner, 'healthy-turn'))['stage'] == 'SUBMITTED'
        assert len(sibling_peer.sent) == 1 and daemon.ready and daemon.owner.failure is None
        await asyncio.wait_for(asyncio.shield(session.renewal), 8)
        assert session.contained and session.release_accepted
        assert len(release_reports) == 1 and release_reports[0]['receipt_count'] == 2
        row = daemon.owner.publications.lookup(frame['server_id'], frame['executor_id'], ['queued'])['queued']
        assert row['acknowledged'] and row['refusal'] is not None and row['receipt'] == refusal
        journal = await daemon.host.ensure_history_journal()
        fact = await journal.get_receipt(OperationKey(frame['server_id'],frame['executor_id'],'queued'))
        assert fact.stage == 'FAILED' and not fact.possible_effect
    finally:
        allow_denial.set()


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('with_capacity', [False, True])
async def test_initial_lease_denial_does_not_open_provider_or_close_shared_channel(lifecycle, monkeypatch, with_capacity):
    daemon, _, _, frame, factory, _, published, _ = lifecycle
    daemon.host.capacity_enabled = with_capacity
    original = daemon.connection.apply_lease
    async def lease(runtime, *, scope, grant_id, **kwargs):
        if scope['session_id'] == frame['session_id']:
            attempt = await runtime.begin_r4_lease_request(scope=scope, grant_id=grant_id, purpose='initial',
                connection_id=frame['connection_id'], connection_generation=frame['connection_generation'])
            raise LeaseRequestDenied(r4_lease_renew_frame(attempt))
        return await original(runtime, scope=scope, grant_id=grant_id)
    monkeypatch.setattr(daemon.connection, 'apply_lease', lease)
    await daemon.sync()
    if with_capacity:
        from tests.unit.test_capacity_authority import request
        service = await daemon.execution_capacity()
        capacity_request = request(daemon, frame)
        held = await service.try_reserve_capacity(capacity_request)
        assert held.state == 'HELD'
        consumed = await service.consume_capacity(capacity_request, held.reservation_id,
            CapacityConsumption('initial-turn', 'sha256:' + 'b' * 64,
                                frame['operation_id'], frame['intent_hash']))
        assert consumed.state == 'CONSUMED'
    await daemon.connection.emit(frame)
    receipt = await observed(published, daemon.owner)
    assert receipt['operation_id'] == frame['operation_id'] and receipt['stage'] == 'FAILED'
    assert receipt['error_code'] == 'LEASE_REVALIDATION_REQUIRED' and not receipt['possible_effect']
    assert not factory.opened and daemon.ready and daemon.connection.online
    journal = await daemon.host.ensure_history_journal()
    assert await journal.get_session_claim(SessionKey(frame['server_id'],frame['executor_id'],frame['session_id'])) is None
    if with_capacity:
        assert (await service.get_capacity_reservation(capacity_request)).state == 'RELEASED'
        assert not (await (await daemon.host.ensure_ledger()).owned_slot_page()).reservations
    sibling = dict(frame, operation_id='healthy-open', session_id='healthy')
    await daemon.connection.emit(sibling)
    assert (await observed(published, daemon.owner))['stage'] in ('SUBMITTED', 'SUCCEEDED')
    assert len(factory.opened) == 1 and daemon.owner.failure is None


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('persist_fact', [False, True])
async def test_reconnect_releases_only_proved_initial_denial_capacity(lifecycle, persist_fact):
    from nexus_connector_core import prepare_r4_lease_refusal, persist_r4_lease_refusal
    from nexus_connector_core.lease_denial import lease_denial_frame
    from okto_nexus_connector.errors import ConnectorError
    from okto_nexus_connector.services.r4_denied_capacity import recover_denied_opening_capacity
    from tests.unit.test_capacity_authority import request as capacity_request

    daemon, _, _, frame, factory, _, _, _ = lifecycle
    daemon.host.capacity_enabled = True
    await daemon.sync()
    service = await daemon.execution_capacity()
    reservation_request = capacity_request(daemon, frame)
    held = await service.try_reserve_capacity(reservation_request)
    await service.consume_capacity(reservation_request, held.reservation_id,
        CapacityConsumption('initial-turn', 'sha256:' + 'b' * 64,
                            frame['operation_id'], frame['intent_hash']))
    lease_request = {k: frame[k] for k in (
        'protocol_major', 'contract_revision', 'connection_id', 'connection_generation', 'grant_id')}
    lease_request.update(type='lease.renew', request_id='denied-original',
        expected_lease_serial=0, purpose='initial', scope={k: frame[k] for k in (
            'server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
            'workspace_binding_id', 'session_id', 'session_owner_generation',
            'authorization_revision', 'configuration_revision', 'binding_revision', 'credential_epoch')})
    refusal = prepare_r4_lease_refusal(frame, lease_denial_frame(lease_request))
    publications = daemon.owner.publications
    publications.reserve(frame)
    publications.bind(refusal['binding'], refusal=refusal)
    journal = await daemon.host.ensure_history_journal()
    key = OperationKey(frame['server_id'], frame['executor_id'], frame['operation_id'])
    if persist_fact:
        await persist_r4_lease_refusal(journal, refusal)
    ledger = await daemon.host.ensure_ledger()
    reporter = SimpleNamespace(server_id=frame['server_id'], executor_id=frame['executor_id'],
        publications=publications, boot_id=daemon.capacity_boot_id,
        require_authority=lambda _: None)
    if persist_fact:
        await recover_denied_opening_capacity(reporter, journal, ledger, {})
        assert (await service.get_capacity_reservation(reservation_request)).state == 'RELEASED'
        assert not (await ledger.owned_slot_page()).reservations
        await recover_denied_opening_capacity(reporter, journal, ledger, {})
    else:
        with pytest.raises(ConnectorError, match='RECONCILIATION_REQUIRED'):
            await recover_denied_opening_capacity(reporter, journal, ledger, {})
        assert (await service.get_capacity_reservation(reservation_request)).state == 'CONSUMED'
        assert len((await ledger.owned_slot_page()).reservations) == 1
    assert not factory.opened


@pytest.mark.parametrize('selection', [True], indirect=True)
@pytest.mark.parametrize('cut', ['association', 'admission', 'terminal', 'publication'])
@pytest.mark.parametrize('kind', ['denial', 'stopped'])
async def test_restart_finishes_original_refusal_at_each_persistence_cut_without_provider(lifecycle, cut, kind):
    from nexus_connector_core import prepare_r4_lease_refusal, prepare_r4_stopped_refusal, persist_r4_operation_refusal, project_r4_bound_receipt
    from nexus_connector_core.lease_denial import lease_denial_frame
    from okto_nexus_connector.storage.r4_publications import R4PublicationStore
    from okto_nexus_connector.services.r4_publications import recover_publications
    daemon, http, state, frame, factory, attached, published, _ = lifecycle
    request = {k: frame[k] for k in ('protocol_major','contract_revision','connection_id','connection_generation','grant_id')}
    request.update(type='lease.renew', request_id='denied-original', expected_lease_serial=0, purpose='initial',
        scope={k: frame[k] for k in ('server_id','executor_id','binding_id','agent_id','workspace_id',
            'workspace_binding_id','session_id','session_owner_generation','authorization_revision',
            'configuration_revision','binding_revision','credential_epoch')})
    if kind == 'stopped':
        frame = operation(frame, 'turn.submit', {'text':'Never execute after this local fence'})
        from nexus_connector_core import r4_submit_intent_hash
        frame['intent_hash'] = r4_submit_intent_hash(frame)
        proof = prepare_r4_stopped_refusal(frame, scope=request['scope'],
            connection_id=frame['connection_id'], connection_generation=frame['connection_generation'],
            error_code='LEASE_EXPIRED')
    else:
        proof = prepare_r4_lease_refusal(frame, lease_denial_frame(request))
    store = R4PublicationStore.for_state(state)
    store.reserve(frame)
    store.bind(proof['binding'], refusal=proof)
    # Production startup initializes the host root before recovery. This
    # fixture deliberately bypasses native composition to test a cold host.
    await daemon.host.ensure_ledger()
    journal = await daemon.host.ensure_history_journal()
    key = OperationKey(frame['server_id'],frame['executor_id'],frame['operation_id'])
    if cut != 'association':
        await journal.admit(key, proof['binding']['core_intent_hash'], frame['session_id'])
    if cut in ('terminal', 'publication'):
        fact = await persist_r4_operation_refusal(journal, proof)
        if cut == 'publication':
            store.record(project_r4_bound_receipt(proof['binding'], fact, key=key, receipt_revision=1))
    async def current(): pass
    await recover_publications(state, daemon.vault, http, server_id=frame['server_id'], executor_id=frame['executor_id'],
        require_current=current, journal=journal)
    result = await asyncio.wait_for(published.get(), 3)
    assert result['operation_id'] == frame['operation_id'] and result['intent_hash'] == frame['intent_hash']
    assert result['stage'] == 'FAILED' and not result['possible_effect'] and result['retry_safe']
    assert not factory.opened and not attached
    assert not R4PublicationStore.for_state(state).pending(frame['server_id'],frame['executor_id'])
    assert await journal.get_session_claim(SessionKey(key.server_id,key.executor_id,frame['session_id'])) is None
    await recover_publications(state, daemon.vault, http, server_id=frame['server_id'], executor_id=frame['executor_id'],
        require_current=current, journal=journal)
    assert published.empty() and not factory.opened
