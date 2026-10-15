"""One actual R4 reader correlates denials without dropping other sessions."""
import asyncio
import copy
from dataclasses import replace

import pytest
from nexus_connector_core import CoreError
from nexus_connector_core.lease_denial import LEASE_DENIAL_REVISION, encode_lease_denial, lease_denial_frame

from okto_nexus_connector.transport.r4_connection import R4Connection
from okto_nexus_connector.transport.lease_denial import LeaseRequestDenied
from okto_nexus_connector.errors import ConnectorError
from tests.contract.test_r4_connection import Socket, STATE, SCOPE, runtime, granted, attach, operation


async def deny(socket, request):
    await socket.incoming.put(encode_lease_denial(lease_denial_frame(request)).decode())


@pytest.mark.parametrize('confirmation', [False, True])
async def test_denial_during_grant_or_commit_barrier_is_scoped(runtime, confirmation):
    socket = Socket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(LEASE_DENIAL_REVISION,)),
                         boot_id=runtime.r4_boot_id)
    owner.start()
    tasks = []
    try:
        await attach(owner, socket)
        tasks = [asyncio.create_task(owner.apply_lease(runtime,
            scope=dict(SCOPE, session_id=name), grant_id='grant')) for name in ('denied', 'healthy')]
        requests = [await asyncio.wait_for(socket.outgoing.get(), 2) for _ in tasks]
        by_session = {r['scope']['session_id']: r for r in requests}
        rejected = by_session['denied']
        if confirmation:
            socket.confirm_replays = False
            await socket.emit(granted(rejected))
            assert (await asyncio.wait_for(socket.outgoing.get(), 2))['type'] == 'lease.applied'
            assert await asyncio.wait_for(socket.outgoing.get(), 2) == rejected
            socket.confirm_replays = True
        await deny(socket, rejected)
        with pytest.raises(LeaseRequestDenied) as error:
            await asyncio.wait_for(tasks[0], 2)
        assert error.value.request == rejected and not error.value.retry_safe
        await socket.emit(operation())
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        assert item.frame['operation_id'] == 'op'
        owner.release_operation(item)
        await socket.emit(granted(by_session['healthy']))
        application = await asyncio.wait_for(tasks[1], 2)
        assert application.context.r4_authority.session_id == 'healthy'
        assert owner.online and owner.failure is None and socket.peak_readers == 1
        assert not owner._pending
    finally:
        await owner.close()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize('tamper', ['session', 'grant', 'serial', 'connection', 'request', 'unnegotiated'])
async def test_foreign_denial_cannot_be_assigned_to_pending_session(runtime, tamper):
    socket = Socket()
    state = replace(STATE, control_capabilities=() if tamper == 'unnegotiated' else (LEASE_DENIAL_REVISION,))
    owner = R4Connection(socket, state, boot_id=runtime.r4_boot_id)
    owner.start()
    task = asyncio.create_task(owner.apply_lease(runtime, scope=SCOPE, grant_id='grant'))
    try:
        request = copy.deepcopy(await asyncio.wait_for(socket.outgoing.get(), 2))
        if tamper == 'session':
            request['scope']['session_id'] = 'foreign'
        elif tamper == 'grant':
            request['grant_id'] = 'foreign'
        elif tamper == 'serial':
            request['expected_lease_serial'] += 1
        elif tamper == 'connection':
            request['connection_generation'] += 1
        elif tamper == 'request':
            request['request_id'] = 'foreign'
        await deny(socket, request)
        with pytest.raises(ConnectorError) as error:
            await asyncio.wait_for(task, 2)
        assert not isinstance(error.value, LeaseRequestDenied)
        assert error.value.code == 'CONTROL_DISCONNECTED'
        assert not owner.online and owner.failure is not None
        assert owner.failure.code == ('VERSION_INCOMPATIBLE' if tamper == 'unnegotiated' else
                                     'STALE_GENERATION' if tamper in ('connection', 'request') else 'SCOPE_MISMATCH')
    finally:
        await owner.close()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_waiter_keeps_denial_correlation_until_original_producer_finishes(runtime):
    socket = Socket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(LEASE_DENIAL_REVISION,)),
                         boot_id=runtime.r4_boot_id)
    owner.start()
    task = asyncio.create_task(owner.apply_lease(runtime, scope=SCOPE, grant_id='grant'))
    try:
        request = await asyncio.wait_for(socket.outgoing.get(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(owner._producers) == 1
        await deny(socket, request)
        async with asyncio.timeout(2):
            while owner._producers:
                await asyncio.sleep(0)
        assert owner.online and not owner._pending
    finally:
        await owner.close()
