"""M6 opt-in pipe exercises the real IPC parser, auth gate and dispatcher."""
import asyncio
import json
import os

import pytest

from okto_nexus_connector.ipc.protocol import encode_message, response_ok
from okto_nexus_connector.ipc.server import IPCServer
from okto_nexus_connector.ipc.named_pipe import _security_attributes
from okto_nexus_connector.ipc.pipe_client import NamedPipeSocket


def test_pipe_client_rejects_nonlocal_address_without_starting_worker():
    client = NamedPipeSocket()
    with pytest.raises(OSError, match='private local'):
        client.connect(r'\\remote-host\pipe\okto-nexus-m6-test')
    assert client._thread is None


async def _connect(address):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=1024 * 1024)
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await loop.create_pipe_connection(lambda: protocol, address)
    return reader, asyncio.StreamWriter(transport, protocol, reader, loop)


async def _hello(reader, writer, token):
    writer.write(encode_message({'op': 'hello', 'token': token, 'version': 1}))
    await writer.drain()
    return json.loads(await asyncio.wait_for(reader.readline(), 5))


@pytest.mark.skipif(os.name != 'nt', reason='Windows named-pipe experiment')
def test_opt_in_named_pipe_authenticates_and_dispatches(tmp_path):
    import win32security
    acl = _security_attributes().SECURITY_DESCRIPTOR.GetSecurityDescriptorDacl()
    assert acl.GetAceCount() == 1
    assert win32security.ConvertSidToStringSid(acl.GetAce(0)[2]).startswith('S-1-5-5-')

    async def run():
        calls = []

        async def dispatch(request):
            calls.append(request.op)
            yield response_ok(request.seq, {'op': request.op})

        server = IPCServer(dispatch)
        await server.start(tmp_path, 'test-token', transport='named_pipe')
        assert server.transport == 'named_pipe'
        assert server.address.startswith('\\\\.\\pipe\\okto-nexus-m6-')

        try:
            reader, writer = await asyncio.wait_for(_connect(server.address), 5)
            denied = await _hello(reader, writer, 'wrong')
            assert denied['error']['code'] == 'AGENT_AUTH_REQUIRED'
            writer.close()
            await writer.wait_closed()
            assert calls == []

            reader, writer = await asyncio.wait_for(_connect(server.address), 5)
            assert (await _hello(reader, writer, 'test-token'))['result']['ready']
            writer.write(encode_message({'op': 'status', 'seq': 1, 'params': {}}))
            await writer.drain()
            reply = await asyncio.wait_for(reader.readline(), 5)
            assert b'"op":"status"' in reply and b'"done":true' in reply
            assert calls == ['status']
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()
        assert server.active_connections == 0

    asyncio.run(run())


@pytest.mark.skipif(os.name != 'nt', reason='Windows named-pipe experiment')
def test_named_pipe_uses_actual_daemon_dispatcher_and_core_host(tmp_path):
    from okto_nexus_connector.daemon.app import DaemonApp

    async def run():
        app = DaemonApp(tmp_path)
        try:
            ledger = await app.host.ensure_ledger()
            assert await ledger.physical_store_id()
            await app.ipc.start(tmp_path / 'run', 'm6-token', transport='named_pipe')
            reader, writer = await asyncio.wait_for(_connect(app.ipc.address), 5)
            try:
                assert (await _hello(reader, writer, 'm6-token'))['result']['ready']
                for seq, op in ((1, 'ping'), (2, 'status')):
                    writer.write(encode_message({'op': op, 'seq': seq, 'params': {}}))
                    await writer.drain()
                    reply = json.loads(await asyncio.wait_for(reader.readline(), 5))
                    assert reply['ok'] and reply['done'] and reply['seq'] == seq
                    if op == 'ping':
                        assert reply['result']['pong'] and reply['result']['pid'] == os.getpid()
                    else:
                        assert reply['result']['daemon']['pid'] == os.getpid()
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            await app.ipc.stop()
            await app.host.shutdown_all()

    asyncio.run(run())
