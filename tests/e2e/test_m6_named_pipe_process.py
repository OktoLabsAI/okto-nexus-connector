"""M6 pipe reaches the actual independent Connector host and dispatcher."""
import asyncio
import json
import os
from pathlib import Path
import sys
import threading

import pytest

from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.ipc.client import IPCClient, ping
from okto_nexus_connector.ipc.protocol import encode_message
from okto_nexus_connector.platform.paths import pid_dir


@pytest.mark.skipif(os.name != 'nt', reason='Windows named-pipe experiment')
def test_disposable_daemon_process_uses_authenticated_named_pipe(tmp_path):
    async def run():
        child = await asyncio.create_subprocess_exec(sys.executable,
            str(Path(__file__).with_name('m6_pipe_daemon.py')), str(tmp_path),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            creationflags=0x08000000)
        lock = InstanceLock(pid_dir(tmp_path))
        writer = None
        try:
            async with asyncio.timeout(20):
                while True:
                    readiness = lock.live_owner()
                    if readiness is not None:
                        break
                    if child.returncode is not None:
                        raise AssertionError('Disposable daemon exited before readiness: '
                                             + (await child.stderr.read()).decode(errors='replace'))
                    await asyncio.sleep(.05)
            # A Windows venv executable may be a launcher process. The
            # readiness PID is birth-token verified and belongs to the real
            # daemon, which may differ from the launcher PID.
            assert readiness.transport == 'named_pipe' and readiness.pid != os.getpid()
            token = lock.token_path.read_text(encoding='utf-8').strip()
            def cli_calls():
                try:
                    with IPCClient(readiness.transport, readiness.address, 'wrong'):
                        raise AssertionError('wrong token was accepted')
                except ConnectorError as exc:
                    assert exc.code == 'AGENT_AUTH_REQUIRED'
                with IPCClient(readiness.transport, readiness.address, token) as cli:
                    assert cli.call('ping')['result']['pong']
                    status = cli.call('status')
                    assert status['result']['daemon']['pid'] == readiness.pid
                assert ping(readiness, token)

            await asyncio.wait_for(asyncio.to_thread(cli_calls), 15)
            assert not any(thread.name == 'nexus-pipe-client'
                           for thread in threading.enumerate())
            loop = asyncio.get_running_loop()
            reader = asyncio.StreamReader(limit=1024 * 1024)
            protocol = asyncio.StreamReaderProtocol(reader)
            transport, _ = await asyncio.wait_for(loop.create_pipe_connection(
                lambda: protocol, readiness.address), 5)
            writer = asyncio.StreamWriter(transport, protocol, reader, loop)

            async def call(frame):
                writer.write(encode_message(frame))
                await writer.drain()
                return json.loads(await asyncio.wait_for(reader.readline(), 5))

            assert (await call({'op': 'hello', 'token': token, 'version': 1}))['result']['ready']
            status = await call({'op': 'status', 'seq': 1, 'params': {}})
            assert status['ok'] and status['result']['daemon']['pid'] == readiness.pid
            def cli_shutdown():
                with IPCClient(readiness.transport, readiness.address, token) as cli:
                    return cli.call('shutdown')
            stopped = await asyncio.wait_for(asyncio.to_thread(cli_shutdown), 10)
            assert stopped['ok'] and stopped['done']
            assert await asyncio.wait_for(child.wait(), 15) == 0
            assert lock.live_owner() is None
        finally:
            if writer is not None:
                writer.close()
                await writer.wait_closed()
            if child.returncode is None:
                child.kill()
                await child.wait()
            await child.stderr.read()

    asyncio.run(run())
