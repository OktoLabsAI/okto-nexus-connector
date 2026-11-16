"""Read existing snapshots without creating locks; share Windows delete access."""
from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


def open_readonly(path: Path) -> BinaryIO:
    """Open an existing file with the broadest Windows sharing flags.

    This is observation, never a substitute for the writer's transaction lock.
    Windows MoveFileEx can still reject replacement of an open destination;
    the atomic state writer retries that transient error for a bounded interval.
    Reads must stay bounded and close their handles before waiting or yielding.
    """
    if os.name != 'nt':
        return path.open('rb')
    import msvcrt
    import pywintypes
    import win32con
    import win32file

    try:
        handle = win32file.CreateFile(str(path), win32con.GENERIC_READ,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None, win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL, None)
    except pywintypes.error as error:
        # Normalize the native error without exposing native exception text.
        if error.winerror in (2, 3):
            raise FileNotFoundError(2, 'Snapshot does not exist', str(path)) from None
        if error.winerror in (5, 32, 33):
            raise PermissionError(13, 'Snapshot is inaccessible', str(path)) from None
        raise OSError(error.winerror, 'Snapshot could not be opened', str(path)) from None
    try:
        fd = msvcrt.open_osfhandle(int(handle), os.O_RDONLY | os.O_BINARY)
    except BaseException:
        handle.Close()
        raise
    handle.Detach()  # The CRT descriptor now owns the native handle.
    try:
        return os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise
