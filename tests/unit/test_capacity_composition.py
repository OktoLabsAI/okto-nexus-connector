"""Remote host uses the same Core facade and physical budget as embedded."""
import asyncio
from dataclasses import replace

import pytest
from nexus_connector_core import (CAPACITY_CONTRACT_REVISION, CapacityRequest, CapacityScope, CoreError)
from okto_nexus_connector.services.core_host import CoreRuntimeHost


class Authority:
    async def check_capacity_scope(self, request, *, action):
        return 'AVAILABLE'


def test_slow_retiring_observer_does_not_block_another_capacity_namespace(tmp_path):
    async def run():
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class SlowAuthority:
            async def check_capacity_scope(self, request, *, action):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    await release.wait()
                return 'AVAILABLE'
        host = CoreRuntimeHost(tmp_path, None, capacity_enabled=True)
        observer = replacement = None
        try:
            old = await host.capacity_service(server_id='srv', executor_id='exe', boot_id='owner', authority=SlowAuthority())
            observer = asyncio.create_task(old.assess_capacity(request()))
            await entered.wait()
            replacement = asyncio.create_task(host.capacity_service(server_id='srv', executor_id='exe',
                boot_id='replacement', authority=Authority()))
            await cancelled.wait()
            other = await asyncio.wait_for(host.capacity_service(server_id='other', executor_id='exe',
                boot_id='independent', authority=Authority()), 1)
            assert other.boot_id == 'independent' and not replacement.done()
        finally:
            release.set()
            await asyncio.gather(*(task for task in (observer, replacement) if task is not None), return_exceptions=True)
            await host.shutdown_all()
    asyncio.run(run())


def request(server='srv', session='session'):
    return CapacityRequest(CAPACITY_CONTRACT_REVISION, 'attempt', CapacityScope(
        server, 'exe', 'agent', 'binding', 'workspace', 'wxb', session, 1, 1, 1, 1, 1, 1, 'owner'),
        'new_session', 'sha256:' + 'a' * 64, 10_000)


def test_connector_namespaces_share_budget_without_launch_or_cross_scope_release(tmp_path):
    async def run():
        host = CoreRuntimeHost(tmp_path, None, max_owned_slots=1, capacity_enabled=True)
        try:
            a = await host.capacity_service(server_id='srv', executor_id='exe', boot_id='owner', authority=Authority())
            b = await host.capacity_service(server_id='other', executor_id='exe', boot_id='owner', authority=Authority())
            req, other = request(), request('other', 'other-session')
            held = await a.try_reserve_capacity(req)
            assert held.state == 'HELD'
            assert (await b.try_reserve_capacity(other)).reason == 'HOST_FULL'
            with pytest.raises(CoreError, match='NOT_AUTHORIZED'):
                await b.release_capacity(req, held.reservation_id, 'LOST')
            assert host.journal_if_open() is None
            await a.release_capacity(req, held.reservation_id, 'LOST')
            await host.shutdown_executor(server_id='srv', executor_id='exe')
            # Closing an idle namespace must not close another owner's ledger.
            assert (await b.try_reserve_capacity(other)).state == 'HELD'
            assert host.journal_if_open() is None
        finally:
            await host.shutdown_all()
    asyncio.run(run())


def test_connector_reconnect_looks_up_original_hold_and_fences_old_facade(tmp_path):
    async def run():
        host = CoreRuntimeHost(tmp_path, None, max_owned_slots=1, capacity_enabled=True)
        try:
            a = await host.capacity_service(server_id='srv', executor_id='exe', boot_id='owner', authority=Authority())
            req = request()
            held = await a.try_reserve_capacity(req)
            b = await host.capacity_service(server_id='srv', executor_id='exe', boot_id='next', authority=Authority())
            assert (await a.try_reserve_capacity(req)).reason == 'RECOVERING'
            assert (await b.get_capacity_reservation(req)).reservation_id == held.reservation_id
            await b.release_capacity(req, held.reservation_id, 'REVOKED')
            req = replace(req, attempt_id='next', scope=replace(req.scope, session_id='next', boot_id='next'))
            assert (await b.try_reserve_capacity(req)).state == 'HELD'
        finally:
            await host.shutdown_all()
    asyncio.run(run())


def test_connector_default_capacity_is_not_enabled(tmp_path):
    async def run():
        host = CoreRuntimeHost(tmp_path, None)
        with pytest.raises(CoreError, match='CAPACITY_PROTOCOL_UNSUPPORTED'):
            await host.capacity_service(server_id='srv', executor_id='exe', boot_id='owner', authority=Authority())
        assert not list(tmp_path.glob('*.db'))
        await host.shutdown_all()
    asyncio.run(run())


@pytest.mark.parametrize('consumed', [False, True])
def test_reconciliation_cancels_fenced_holds_without_discarding_consumed_capacity(tmp_path, consumed):
    from nexus_connector_core import CapacityConsumption, R4_PREVIEW_REVISION
    from okto_nexus_connector.services.r4_reconciliation import R4ReconciliationReporter
    from okto_nexus_connector.storage.state_store import StateStore, ConnectorState
    from okto_nexus_connector.errors import ConnectorError
    async def run():
        store = StateStore(tmp_path / 'state.json')
        store.save(ConnectorState(connector_id='connector'))
        host = CoreRuntimeHost(tmp_path, None, capacity_enabled=True)
        try:
            port = await host.capacity_service(server_id='srv', executor_id='exe', boot_id='owner', authority=Authority())
            req = request()
            held = await port.try_reserve_capacity(req)
            if consumed:
                await port.consume_capacity(req, held.reservation_id,
                    CapacityConsumption('turn', 'sha256:' + 'b' * 64, 'open', 'sha256:' + 'c' * 64))
            reporter = R4ReconciliationReporter(store, host, 'srv', 'exe',
                                                validate_authority=lambda _request: None)
            frame = dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION,
                server_id='srv', executor_id='exe', connection_id='replacement', connection_generation=2,
                reconcile_id='cycle', cursor=None, operation_ids=[], session_ids=[], stream_watermarks=[])
            if consumed:
                with pytest.raises(ConnectorError, match='Owned resources'):
                    await reporter.report(frame)
                assert (await port.get_capacity_reservation(req)).state == 'CONSUMED'
            else:
                result = await reporter.report(frame)
                assert result['complete'] and result['claims'] == [] and result['ownership_facts'] == []
                assert (await port.get_capacity_reservation(req)).state == 'RELEASED'
        finally:
            await host.shutdown_all()
    asyncio.run(run())
