"""Experimental Windows named-pipe listener for the existing IPC dispatcher.

Only the current logon session can open the pipe. The normal IPC hello token
is still required before dispatch. This module is opt-in for M6 measurement;
the packaged daemon keeps its established transport by default.
"""
from __future__ import annotations

import asyncio
import os


def _security_attributes():
    if os.name != 'nt':
        raise OSError('Windows named pipes are unavailable on this host.')
    import pywintypes
    import win32api
    import win32con
    import win32security

    token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32con.TOKEN_QUERY)
    try:
        groups = win32security.GetTokenInformation(token, win32security.TokenGroups)
        logon = [sid for sid, attributes in groups
                 if attributes & 0xC0000000 == 0xC0000000]  # SE_GROUP_LOGON_ID
        if len(logon) != 1:
            raise OSError('Cannot identify the current logon session.')
        logon_sid = win32security.ConvertStringSidToSid(
            win32security.ConvertSidToStringSid(logon[0]))
    finally:
        token.Close()
    acl = win32security.ACL()
    acl.AddAccessAllowedAce(win32security.ACL_REVISION, win32con.GENERIC_ALL, logon_sid)
    descriptor = win32security.SECURITY_DESCRIPTOR()
    descriptor.SetSecurityDescriptorDacl(True, acl, False)
    attributes = pywintypes.SECURITY_ATTRIBUTES()
    attributes.SECURITY_DESCRIPTOR = descriptor
    attributes.bInheritHandle = False
    return attributes


def _pipe_server_type():
    import asyncio.windows_events as windows_events
    import asyncio.windows_utils as windows_utils
    import win32con
    import win32pipe

    class SecurePipeServer(windows_events.PipeServer):
        def __init__(self, address):
            self._security = _security_attributes()
            super().__init__(address)

        def _server_pipe_handle(self, first):
            if self.closed():
                return None
            flags = win32pipe.PIPE_ACCESS_DUPLEX | win32con.FILE_FLAG_OVERLAPPED
            if first:
                flags |= 0x00080000  # FILE_FLAG_FIRST_PIPE_INSTANCE
            mode = (win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE |
                    win32pipe.PIPE_WAIT | win32pipe.PIPE_REJECT_REMOTE_CLIENTS)
            handle = win32pipe.CreateNamedPipe(self._address, flags, mode,
                win32pipe.PIPE_UNLIMITED_INSTANCES, 65536, 65536, 5000,
                self._security)
            pipe = windows_utils.PipeHandle(handle.Detach())
            self._free_instances.add(pipe)
            return pipe

    return SecurePipeServer


async def serve_secure_pipe(protocol_factory, address):
    """Serve with Proactor overlapped I/O and an explicit per-logon DACL."""
    if os.name != 'nt' or not address.startswith('\\\\.\\pipe\\okto-nexus-m6-'):
        raise ValueError('A private local M6 pipe name is required.')
    loop = asyncio.get_running_loop()
    if not hasattr(loop, '_proactor'):
        raise RuntimeError('Windows ProactorEventLoop is required.')
    server = _pipe_server_type()(address)

    def accept(completed=None):
        pipe = None
        try:
            if completed is not None:
                pipe = completed.result()
                server._free_instances.discard(pipe)
                if server.closed():
                    pipe.close()
                    return
                loop._make_duplex_pipe_transport(pipe, protocol_factory(),
                    extra={'addr': address})
            pipe = server._get_unconnected_pipe()
            if pipe is None:
                return
            future = loop._proactor.accept_pipe(pipe)
        except (BrokenPipeError, OSError) as error:
            if pipe is not None and pipe.fileno() != -1:
                pipe.close()
            if not server.closed():
                loop.call_exception_handler(dict(message='M6 pipe accept failed',
                    exception=error, pipe=pipe))
                loop.call_later(.05, accept)
        except asyncio.CancelledError:
            if pipe is not None:
                pipe.close()
        else:
            server._accept_pipe_future = future
            future.add_done_callback(accept)

    loop.call_soon(accept)
    return server
