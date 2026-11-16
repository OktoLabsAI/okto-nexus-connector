"""Service instance identity; never an executor/agent identity or credential."""
from __future__ import annotations

import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path


def effective_principal() -> str:
    if os.name != 'nt':
        return f'uid:{os.geteuid()}'
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    security = ctypes.WinDLL('advapi32', use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.LocalFree.argtypes = [wintypes.HLOCAL]
    security.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
    security.GetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                            wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    security.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.LPWSTR)]
    token, size = wintypes.HANDLE(), wintypes.DWORD()
    if not security.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        security.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        if not 0 < size.value < 65536:
            raise OSError('OS user token unavailable')
        buffer = ctypes.create_string_buffer(size.value)
        if not security.GetTokenInformation(token, 1, buffer, size.value, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        sid = ctypes.c_void_p.from_buffer(buffer).value
        text = wintypes.LPWSTR()
        if not security.ConvertSidToStringSidW(sid, ctypes.byref(text)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return 'sid:' + text.value
        finally:
            kernel.LocalFree(ctypes.cast(text, wintypes.HLOCAL))
    finally:
        kernel.CloseHandle(token)


def service_instance_id(root: Path) -> str:
    payload = json.dumps(['connector', os.path.normcase(str(root.resolve())),
                          effective_principal()], separators=(',', ':')).encode()
    return 'svc_' + hashlib.sha256(payload).hexdigest()
