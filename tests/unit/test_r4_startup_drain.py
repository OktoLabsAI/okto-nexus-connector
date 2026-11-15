"""Startup drain joins real control ownership; all peers are synthetic."""
import asyncio
from types import SimpleNamespace

import pytest

from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_r4_daemon_control import control as control_fixture, dispose, eventually

control = control_fixture


async def quiescent(owner):
    async with asyncio.timeout(5):
        while await owner.drain_status() == 'BUSY':
            await asyncio.sleep(.005)
    return await owner.drain_status()


async def test_vault_failure_can_drain_without_becoming_control_ready(control):
    owner, peer, _, host, vault = control
    def denied(_handle):
        raise ConnectorError('APPROVAL_REQUIRED', 'vault', 'Synthetic missing consent')
    vault.resolve = denied
    owner.start()
    try:
        await eventually(lambda: owner.consecutive_failures > 0)
        assert owner.execution is None and not peer.sockets
        attempts = owner.attempts
        owner.begin_drain()
        task = owner._drain_stop_task
        owner.begin_drain()
        assert owner._drain_stop_task is task
        assert await quiescent(owner) == 'QUIESCENT'
        assert owner._task.done() and owner.phase == 'STOPPED'
        assert owner.attempts == attempts and not peer.publications
        with pytest.raises(ConnectorError, match='another onboarding'):
            await owner.realize()
    finally:
        await dispose(owner, host)


async def test_inflight_bootstrap_is_joined_not_cancelled_or_published(control):
    owner, peer, _, host, _ = control
    entered, release = asyncio.Event(), asyncio.Event()
    register = peer.register_executor
    cancelled = False
    async def blocked(*args, **kwargs):
        nonlocal cancelled
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled = True
            raise
        return await register(*args, **kwargs)
    peer.register_executor = blocked
    owner.start()
    try:
        await asyncio.wait_for(entered.wait(), 5)
        owner.begin_drain()
        await asyncio.sleep(0)
        assert await owner.drain_status() == 'BUSY'
        assert not owner._task.done() and not cancelled
        release.set()
        assert await quiescent(owner) == 'QUIESCENT'
        assert not cancelled and not peer.publications and not peer.sockets
    finally:
        release.set()
        await dispose(owner, host)


async def test_owned_onboarding_join_delays_quiescence(control):
    owner, _, _, host, _ = control
    release = asyncio.Event()
    task = asyncio.create_task(release.wait())
    owner._onboarding_tasks.add(task)
    task.add_done_callback(owner._onboarding_tasks.discard)
    try:
        owner.begin_drain()
        await asyncio.sleep(0)
        assert await owner.drain_status() == 'BUSY'
        assert not task.cancelled()
        release.set()
        assert await quiescent(owner) == 'QUIESCENT'
    finally:
        release.set()
        await dispose(owner, host)


@pytest.mark.parametrize('failure', ['cleanup', 'recovery', 'close_exception'])
async def test_unconfirmed_owned_resource_remains_unknown(control, failure):
    owner, _, _, host, _ = control
    async def close():
        if failure == 'close_exception':
            raise OSError('Synthetic close failure')
    if failure != 'recovery':
        owner.connection = SimpleNamespace(close=close, close_error=OSError('unconfirmed'))
        owner.cleanup_pending = True
    else:
        owner._recovery_error = 'JOURNAL_UNAVAILABLE'
    try:
        owner.begin_drain()
        assert await quiescent(owner) == 'UNKNOWN'
    finally:
        owner.connection = None  # Dispose only the synthetic fixture resource.
        await dispose(owner, host)


async def test_ready_execution_keeps_link_and_existing_drain_contract(control):
    owner, _, _, host, _ = control
    began = []
    async def status():
        return 'BUSY'
    owner.execution = SimpleNamespace(begin_drain=lambda: began.append(True), drain_status=status)
    owner.phase = 'CONTROL_READY'
    try:
        owner.begin_drain()
        assert await owner.drain_status() == 'BUSY'
        assert began == [True] and owner._drain_stop_task is None
        assert not owner._stopped.is_set()
    finally:
        owner.execution = None
        await dispose(owner, host)


async def test_partial_execution_must_be_quiescent_before_startup_join(control):
    owner, _, _, host, _ = control
    state = 'BUSY'
    async def status():
        return state
    owner.execution = SimpleNamespace(begin_drain=lambda: None, drain_status=status)
    owner.phase = 'PUBLISHING_INVENTORY'
    async def owned_cleanup():
        await owner._stopped.wait()
        owner.execution = None
        owner.phase = 'STOPPED'
    owner._task = asyncio.create_task(owned_cleanup())
    try:
        owner.begin_drain()
        assert await owner.drain_status() == 'BUSY'
        assert not owner._stopped.is_set()
        state = 'UNKNOWN'
        assert await owner.drain_status() == 'UNKNOWN'
        assert not owner._stopped.is_set()
        state = 'QUIESCENT'
        assert await quiescent(owner) == 'QUIESCENT'
        assert owner._task.done()
    finally:
        await dispose(owner, host)
