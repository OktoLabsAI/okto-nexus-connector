import asyncio
from dataclasses import replace
import time

import pytest

from tests.contract.test_r4_connection import Socket, STATE, attach, operation
from okto_nexus_connector.transport.r4_connection import R4Connection


async def test_renewal_keeps_socket_lane_attempt_and_queued_operation():
    socket = Socket()
    state = replace(STATE, control_capabilities=('connection_renewal_v1',))
    owner = R4Connection(socket, state, boot_id='boot')
    owner.start()
    try:
        await attach(owner, socket)
        lane = owner._lanes['binding']
        await socket.emit(operation())
        await asyncio.sleep(.01)
        for _ in range(3):
            task = asyncio.create_task(owner.renew_connection())
            request = await asyncio.wait_for(socket.outgoing.get(), 1)
            assert request['type'] == 'connection.renew'
            await socket.emit(dict(request, type='connection.renewed', expires_in=600, binding_ids=['binding']))
            deadline = await task
            assert time.monotonic() < deadline <= time.monotonic() + 600
            assert owner._lanes['binding'].attach_request_id == lane.attach_request_id
            assert owner.online and not socket.closed
        assert owner._lanes['binding'].deadline_monotonic > lane.deadline_monotonic
        assert len(owner._reservations) == 1
    finally:
        await owner.close()


async def test_missing_heartbeat_ack_fences_silent_peer():
    socket = Socket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=('heartbeat_ack_v1',)),
                         boot_id='boot', heartbeat_seconds=.01)
    owner.start()
    try:
        await asyncio.sleep(.06)
        assert not owner.online
        assert owner.failure.stage == 'r4_heartbeat'
    finally:
        await owner.close()


async def test_quiet_transport_resume_retains_live_lane():
    socket, replacement = Socket(), Socket()
    async def resume(state):
        assert state == STATE
        return replacement
    owner = R4Connection(socket, STATE, boot_id='boot', resume=resume)
    owner.start()
    try:
        await attach(owner, socket)
        lane = owner._lanes['binding']
        assert await owner._try_resume(socket)
        assert owner.websocket is replacement and owner.online
        assert owner._lanes['binding'] is lane
        assert socket.closed and not replacement.closed
    finally:
        await owner.close()


async def test_inflight_control_request_does_not_fast_resume():
    socket = Socket()
    resumed = []
    async def resume(state):
        resumed.append(state)
        return Socket()
    owner = R4Connection(socket, STATE, boot_id='boot', resume=resume)
    owner.start()
    future = asyncio.get_running_loop().create_future()
    owner._pending[('lease.granted', 'uncertain')] = (future, None)
    try:
        assert not await owner._try_resume(socket)
        assert not resumed and not socket.closed
    finally:
        owner._pending.clear()
        future.cancel()
        await owner.close()
