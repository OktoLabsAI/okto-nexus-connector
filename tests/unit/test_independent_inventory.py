"""A slow/failed harness does not own connection renewal or sibling evidence."""
import asyncio
from dataclasses import replace
import time
from types import SimpleNamespace

import pytest

from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_r4_daemon_control import control, eventually, dispose
from tests.unit.inventory_fixture import accepted_batch


@pytest.mark.parametrize('renewed', [False, True])
async def test_publication_checks_current_renewal_after_storage_work(control, monkeypatch, renewed):
    owner, peer, _, host, _ = control
    instant = [0.0]
    owner.clock = lambda: instant[0]
    bootstrap = SimpleNamespace(executor=SimpleNamespace(executor_id='executor'),
                                ticket='nxt4_synthetic_0', deadline_monotonic=1.0)
    current = [bootstrap]
    require, calls = owner._require, 0
    async def during_publication(snapshot):
        nonlocal calls
        await require(snapshot)
        calls += 1
        if calls == 3:
            instant[0] = 2.0
            if renewed:
                current[0] = SimpleNamespace(executor=bootstrap.executor, ticket=bootstrap.ticket,
                                             deadline_monotonic=602.0)
    monkeypatch.setattr(owner, '_require', during_publication)
    try:
        invocation = owner._publish(peer, bootstrap, owner._snapshot(),
            batch=accepted_batch(now=0), current_bootstrap=lambda: current[0])
        if renewed:
            await invocation
            assert len(peer.publications) == 1
        else:
            with pytest.raises(ConnectorError, match='CONTROL_DISCONNECTED'):
                await invocation
            assert not peer.publications
    finally:
        await host.shutdown_all()


async def test_held_pi_allows_partial_inventory_and_repeated_connection_renewal(control):
    owner, peer, _, host, _ = control
    entered, release = asyncio.Event(), asyncio.Event()
    instant = [100.0]
    owner.clock = owner.registration.clock = lambda: instant[0]
    calls, renewals = [], []
    async def discover(*, adapter_ids, cancel_requested):
        assert len(adapter_ids) == 1
        calls.append(adapter_ids[0])
        if adapter_ids[0] == 'pi_rpc':
            entered.set()
            await release.wait()
        return []
    owner.discover = discover
    original = peer.connect
    async def connect(*args, **kwargs):
        connection = await original(*args, **kwargs)
        connection.state.control_capabilities = ('connection_renewal_v1',)
        async def renew():
            renewals.append(instant[0])
            return instant[0] + 600
        connection.renew_connection = renew
        return connection
    owner.connect = connect
    owner.start()
    stop = None
    try:
        await asyncio.wait_for(entered.wait(), 15)
        await eventually(lambda: owner.status()['control_ready'] and bool(peer.publications))
        inventory = owner.inventory_owner
        await eventually(lambda: inventory.published_batch is not None and
            inventory.published_batch.coverage()['codex_app_server']['state'] == 'ABSENT')
        batch = await inventory.require_batch('codex_app_server')
        assert batch.coverage()['pi_rpc']['state'] == 'PENDING'
        assert batch.coverage()['codex_app_server']['state'] == 'ABSENT'
        with pytest.raises(ConnectorError, match='INVENTORY_NOT_FRESH'):
            await inventory.require_batch('pi_rpc')
        connection = owner.connection
        for index in range(2):
            instant[0] += 421
            await eventually(lambda: len(renewals) == index + 1 and
                owner._bootstrap.deadline_monotonic == instant[0] + 600)
            assert owner.connection is connection and connection.online
            assert peer.calls == 1
        stop = asyncio.create_task(owner.stop())
        await eventually(inventory._stop.is_set)
        assert not stop.done()
        stop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop
        assert not inventory.observer._tasks['pi_rpc'].done()
        before = len(peer.publications)
        release.set()
        await owner.stop()
        assert len(peer.publications) == before
        assert all(task.done() for task in inventory.observer._tasks.values())
    finally:
        release.set()
        await dispose(owner, host)


async def test_failed_default_discovery_retries_only_its_adapter(control, monkeypatch):
    from okto_nexus_connector.daemon import r4_inventory
    owner, peer, _, host, _ = control
    calls = {}
    retry_release = asyncio.Event()
    async def discover(configuration, *, adapter_ids, observations, cancel_requested):
        assert configuration == {} and observations == []
        adapter, = adapter_ids
        calls[adapter] = calls.get(adapter, 0) + 1
        if adapter == 'pi_rpc' and calls[adapter] == 1:
            raise OSError('synthetic private path must not reach inventory')
        if adapter == 'pi_rpc':
            await retry_release.wait()
        return []
    monkeypatch.setattr(r4_inventory, 'configured_candidates', discover)
    owner.discover = None
    owner.start()
    try:
        def state():
            batch = owner.inventory_owner.published_batch if owner.inventory_owner else None
            return batch.coverage().get('pi_rpc', {}).get('state') if batch else None
        await eventually(lambda: state() == 'FAILED')
        await eventually(lambda: owner.status()['control_ready'] and
            owner.inventory_owner.published_batch.coverage()['codex_app_server']['state'] == 'ABSENT')
        await owner.inventory_owner.require_batch('codex_app_server')
        retry_release.set()
        await eventually(lambda: state() == 'ABSENT')
        await owner.inventory_owner.require_batch('pi_rpc')
        assert calls == {'codex_app_server': 1, 'claude_stream': 1, 'pi_rpc': 2}
        assert peer.calls == 1 and len(peer.sockets) == 1
        assert 'synthetic private path' not in str(peer.publications)
    finally:
        retry_release.set()
        await dispose(owner, host)


async def test_refresh_requires_post_claim_attempt_of_every_adapter(control, monkeypatch):
    owner, peer, _, host, _ = control
    protocol = peer.r4_protocol
    async def supported():
        return replace(await protocol(), inventory_refresh_supported=True)
    monkeypatch.setattr(peer, 'r4_protocol', supported)
    releases = [asyncio.Event(), asyncio.Event()]
    started = [asyncio.Event(), asyncio.Event()]
    count, offset = 0, 0
    delivery = None
    claims, acknowledgements = [], []
    owner.clock = lambda: time.monotonic() + offset
    async def discover(*, adapter_ids, cancel_requested):
        nonlocal count
        if adapter_ids == ('pi_rpc',):
            index = count
            count += 1
            if index < 2:
                started[index].set()
                await releases[index].wait()
        return []
    async def claim(ticket, **scope):
        claims.append(delivery)
        return delivery
    original = peer.publish_inventory
    async def publish(ticket, *, executor_id, snapshot, refresh_delivery_id=None):
        acknowledgements.append(refresh_delivery_id)
        return await original(ticket, executor_id=executor_id, snapshot=snapshot)
    owner.discover = discover
    monkeypatch.setattr(peer, 'claim_inventory_refresh', claim, raising=False)
    monkeypatch.setattr(peer, 'publish_inventory', publish)
    owner.start()
    try:
        await asyncio.wait_for(started[0].wait(), 15)
        await eventually(lambda: bool(peer.publications))
        delivery, offset = 'refresh-after-initial-start', 6
        await eventually(lambda: delivery in claims)
        releases[0].set()
        await asyncio.wait_for(started[1].wait(), 5)
        assert delivery not in acknowledgements
        releases[1].set()
        await eventually(lambda: delivery in acknowledgements)
        assert acknowledgements.count(delivery) == 1
        assert peer.calls == 1
    finally:
        for event in releases:
            event.set()
        await dispose(owner, host)
