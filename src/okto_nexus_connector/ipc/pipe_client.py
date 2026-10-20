"""Synchronous client bridge for the opt-in Windows named-pipe transport.

The CLI stays synchronous; one private Proactor thread owns all overlapped
pipe I/O and its handle until shutdown has joined the reader task.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeout
import os
import queue
import socket
import threading


class NamedPipeSocket:
    def __init__(self) -> None:
        self._ready: Future[None] = Future()
        self._incoming: queue.Queue[bytes | BaseException] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task | None = None
        self._stop: asyncio.Event | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._timeout = 1.0
        self._remainder = b''
        self._closed = False

    def settimeout(self, seconds: float) -> None:
        self._timeout = seconds

    def connect(self, address: str) -> None:
        if os.name != 'nt' or not address.startswith('\\\\.\\pipe\\okto-nexus-m6-'):
            raise OSError('a private local Windows Nexus pipe is required')
        if self._thread is not None:
            raise OSError('named pipe is already connected')
        self._thread = threading.Thread(target=self._run, args=(address,),
                                        name='nexus-pipe-client', daemon=True)
        self._thread.start()
        try:
            self._ready.result(timeout=self._timeout)
        except FutureTimeout as exc:
            self.close()
            raise socket.timeout('named-pipe connection timed out') from exc
        except BaseException:
            self.close()
            raise

    def _run(self, address: str) -> None:
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._task = loop.create_task(self._connected(address))
        try:
            loop.run_until_complete(self._task)
        except asyncio.CancelledError:
            if not self._ready.done():
                self._ready.set_exception(OSError('named-pipe connection cancelled'))
        except BaseException as exc:
            if not self._ready.done():
                self._ready.set_exception(exc)
            else:
                self._incoming.put(exc)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending,
                                                        return_exceptions=True))
            loop.close()
            asyncio.set_event_loop(None)
            self._loop = None

    async def _connected(self, address: str) -> None:
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(limit=1024 * 1024)
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await loop.create_pipe_connection(lambda: protocol, address)
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        self._writer = writer
        self._stop = asyncio.Event()
        pump = asyncio.create_task(self._pump(reader))
        self._ready.set_result(None)
        try:
            await self._stop.wait()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            finally:
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
                self._incoming.put(b'')

    async def _pump(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                chunk = await reader.read(65536)
                self._incoming.put(chunk)
                if not chunk:
                    return
        except (OSError, ConnectionError) as exc:
            self._incoming.put(exc)

    def sendall(self, data: bytes) -> None:
        if self._closed or self._loop is None or self._writer is None:
            raise OSError('named pipe is closed')

        async def write() -> None:
            assert self._writer is not None
            self._writer.write(data)
            await self._writer.drain()

        future = asyncio.run_coroutine_threadsafe(write(), self._loop)
        try:
            future.result(timeout=30.0)
        except FutureTimeout as exc:
            future.cancel()
            raise socket.timeout('named-pipe write timed out') from exc

    def recv(self, size: int) -> bytes:
        if self._remainder:
            chunk, self._remainder = self._remainder[:size], self._remainder[size:]
            return chunk
        if self._closed:
            return b''
        try:
            item = self._incoming.get(timeout=self._timeout)
        except queue.Empty as exc:
            raise socket.timeout('named-pipe read timed out') from exc
        if isinstance(item, BaseException):
            raise OSError('named-pipe read failed') from item
        chunk, self._remainder = item[:size], item[size:]
        return chunk

    def close(self) -> None:
        self._closed = True
        loop, stop, task = self._loop, self._stop, self._task
        if loop is not None:
            try:
                if stop is not None:
                    loop.call_soon_threadsafe(stop.set)
                elif task is not None:
                    loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
