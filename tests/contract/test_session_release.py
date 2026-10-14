"""Single-reader release ACKs keep other sessions and exact retry ownership."""
import asyncio
import copy
from dataclasses import replace

import pytest
from nexus_connector_core import decode_control_frame
from nexus_connector_core.session_release import (
    SESSION_RELEASE_REVISION, encode_session_release, session_release_accepted, session_release_deferred,
)
from okto_nexus_connector.transport.r4_connection import R4Connection
from okto_nexus_connector.errors import ConnectorError
from tests.contract.test_r4_connection import Socket, STATE, SCOPE, attach, operation


class ReleaseSocket(Socket):
    async def send(self, raw):
        frame = decode_control_frame(raw.encode(), capabilities=(SESSION_RELEASE_REVISION,))
        if frame['type'] == 'session.release':
            await self.outgoing.put(frame)
        else:
            await super().send(raw)

    async def reply(self, frame):
        await self.incoming.put(encode_session_release(frame).decode())


def request():
    return dict(contract_revision=SESSION_RELEASE_REVISION, type='session.release', release_id='original',
        server_id=STATE.server_id, executor_id=STATE.executor_id, connection_id=STATE.connection_id,
        connection_generation=STATE.connection_generation, scope=dict(SCOPE), opening_operation_id='open',
        opening_intent_hash='sha256:'+'a'*64, proof_digest='sha256:'+'b'*64, stream_epoch='epoch',
        sequence=2, receipt_count=2, receipt_digest='sha256:'+'c'*64)


@pytest.mark.parametrize('deferred', [False, True])
async def test_release_reply_is_correlated_without_interfering_with_other_work(deferred):
    socket = ReleaseSocket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(SESSION_RELEASE_REVISION,)), boot_id='boot')
    owner.start()
    task = None
    try:
        await attach(owner, socket)
        original = request()
        task = asyncio.create_task(owner.publish_session_release(original))
        assert await asyncio.wait_for(socket.outgoing.get(), 2) == original
        reply = (session_release_deferred if deferred else session_release_accepted)(original)
        await socket.reply(reply)
        assert await asyncio.wait_for(task, 2) == reply
        await socket.emit(operation())
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        owner.release_operation(item)
        assert owner.online and owner.failure is None and socket.peak_readers == 1
    finally:
        await owner.close()
        if task: await asyncio.gather(task, return_exceptions=True)


async def test_lost_ack_retries_identical_request_and_duplicate_reply_does_not_fence_link():
    socket = ReleaseSocket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(SESSION_RELEASE_REVISION,)),
                         boot_id='boot', request_timeout=.03)
    owner.start()
    task = None
    try:
        await attach(owner, socket)
        task = asyncio.create_task(owner.publish_session_release(request()))
        first = await asyncio.wait_for(socket.outgoing.get(), 2)
        assert await asyncio.wait_for(socket.outgoing.get(), 2) == first
        assert owner.online and len(owner._producers) == 1
        reply = session_release_accepted(first)
        await socket.reply(reply)
        assert await asyncio.wait_for(task, 2) == reply
        await socket.reply(reply)
        await socket.emit(operation())
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        owner.release_operation(item)
        assert owner.online and not owner._pending
    finally:
        await owner.close()
        if task: await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_observer_retains_release_producer_until_ack():
    socket = ReleaseSocket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(SESSION_RELEASE_REVISION,)), boot_id='boot')
    owner.start()
    task = asyncio.create_task(owner.publish_session_release(request()))
    try:
        sent = await asyncio.wait_for(socket.outgoing.get(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert len(owner._producers) == 1
        await socket.reply(session_release_accepted(sent))
        async with asyncio.timeout(2):
            while owner._producers: await asyncio.sleep(0)
        assert owner.online and not owner._pending
    finally:
        await owner.close()


async def test_explicit_publication_stop_drains_without_closing_channel_or_accepting_late_ack():
    socket, stop = ReleaseSocket(), asyncio.Event()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(SESSION_RELEASE_REVISION,)),
                         boot_id='boot', request_timeout=30)
    owner.start()
    task = None
    try:
        await attach(owner, socket)
        task = asyncio.create_task(owner.publish_session_release(request(), stop_event=stop))
        sent = await asyncio.wait_for(socket.outgoing.get(), 2)
        stop.set()
        with pytest.raises(ConnectorError, match='PUBLICATION_INTERRUPTED'):
            await asyncio.wait_for(task, 2)
        assert owner.online and not owner._pending and not owner._producers
        await socket.reply(session_release_accepted(sent))
        await socket.emit(operation())
        item = await asyncio.wait_for(owner.receive_operation(), 2)
        owner.release_operation(item)
        assert owner.online
        # The durable publisher can retry those exact bytes; a retired late
        # ACK alone did not settle its journal or create a new execution.
        task = asyncio.create_task(owner.publish_session_release(sent))
        assert await asyncio.wait_for(socket.outgoing.get(), 2) == sent
        await socket.reply(session_release_accepted(sent))
        assert (await asyncio.wait_for(task, 2))['request'] == sent
    finally:
        await owner.close()
        if task: await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('tamper', ['release_id','proof_digest','receipt_digest','sequence','scope','connection_generation'])
async def test_changed_report_cannot_ack_the_original_release(tamper):
    socket = ReleaseSocket()
    owner = R4Connection(socket, replace(STATE, control_capabilities=(SESSION_RELEASE_REVISION,)), boot_id='boot')
    owner.start()
    task = asyncio.create_task(owner.publish_session_release(request()))
    try:
        sent = copy.deepcopy(await asyncio.wait_for(socket.outgoing.get(), 2))
        if tamper in ('sequence','connection_generation'): sent[tamper] += 1
        elif tamper == 'scope': sent['scope']['session_id'] = 'other'
        elif tamper.endswith('digest'): sent[tamper] = 'sha256:'+'f'*64
        else: sent[tamper] = 'other'
        await socket.reply(session_release_accepted(sent))
        with pytest.raises(ConnectorError): await asyncio.wait_for(task, 2)
        assert not owner.online and owner.failure is not None
    finally:
        await owner.close()
        await asyncio.gather(task, return_exceptions=True)
