import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from okto_nexus_connector.transport.wss_client import NXLTransport


async def test_stop_joins_attach_cleanup_without_cancelling_it_twice():
    cleanup_started, release_cleanup, interrupted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def attach():
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            try:
                await release_cleanup.wait()
            except asyncio.CancelledError:
                interrupted.set()
                raise
    async def idle(*_):
        await asyncio.Event().wait()
    transport = NXLTransport(server_id='s', executor_id='e', link_url='ws://127.0.0.1/link',
        ticket_provider=AsyncMock(), on_operation=AsyncMock(), on_approval=AsyncMock())
    attachment = asyncio.create_task(attach())
    lane = SimpleNamespace(attach_task=attachment, retry_timer=None, reattach_requested=False)
    transport._lanes['binding'] = lane
    transport._inflight.add(attachment)
    transport._sender = transport._receiver = transport._heartbeat = idle
    transport._watchdog = idle
    transport._task = asyncio.create_task(transport._session(SimpleNamespace(send=AsyncMock())))
    await asyncio.sleep(0)
    stopped = asyncio.create_task(transport.stop())
    try:
        await asyncio.wait_for(cleanup_started.wait(), 2)
        await asyncio.sleep(.05)
        assert not interrupted.is_set() and not stopped.done()
    finally:
        release_cleanup.set()
        await asyncio.wait_for(stopped, 2)
    assert attachment.done() and not interrupted.is_set()
