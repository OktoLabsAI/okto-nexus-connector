"""Remote host uses the same Core facade and physical budget as embedded."""
import asyncio
from dataclasses import replace

import pytest
from nexus_connector_core import (CAPACITY_CONTRACT_REVISION, CapacityRequest, CapacityScope, CoreError)
from okto_nexus_connector.services.core_host import CoreRuntimeHost


class Authority:
    async def check_capacity_scope(self, request, *, action):
        return 'AVAILABLE'


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
