"""Bounded observation of the daemon log; no daemon or runtime operations."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from ..storage.read_only import open_readonly

MAX_READ = 256 * 1024
MAX_RECORD = 16 * 1024


@dataclass(frozen=True)
class LogCursor:
    identity: tuple[int, int]
    offset: int
    pending: bytes = b''
    discarding: bool = False


def read_log(path: Path, tail: int, cursor: LogCursor | None = None):
    """Return complete lines from at most the newest 256 KiB per observation.

    Partial/oversize records are never emitted as standalone fragments: the
    caller's secret redactor must see whole records. A slow follower may miss
    records rotated away or beyond the byte budget; this is reported explicitly.
    """
    if not 1 <= tail <= 10000:
        raise ValueError('tail must be between 1 and 10000')
    with open_readonly(path) as stream:
        info = os.fstat(stream.fileno())
        identity = (info.st_dev, info.st_ino)
        rotated = cursor is not None and cursor.identity != identity
        shrunk = cursor is not None and not rotated and info.st_size < cursor.offset
        offset = cursor.offset if cursor is not None and not rotated and not shrunk else 0
        start = max(offset, info.st_size - MAX_READ)
        skipped = start > offset
        pending = cursor.pending if cursor is not None and not rotated and not shrunk and not skipped else b''
        discarding = skipped or (cursor.discarding if cursor is not None and not rotated and not shrunk else False)
        stream.seek(start)
        blob = pending + stream.read(MAX_READ)
        offset = stream.tell()
    records = blob.split(b'\n')
    pending = records.pop()
    if discarding and records:
        records.pop(0)
        discarding = False
    complete = [record for record in records if len(record) <= MAX_RECORD]
    dropped = len(complete) != len(records)
    if discarding or len(pending) > MAX_RECORD:
        pending, discarding, dropped = b'', True, True
    limited = len(complete) > tail
    lines = [record.decode('utf-8', errors='replace').rstrip('\r') for record in complete[-tail:]]
    return lines, LogCursor(identity, offset, pending, discarding), {
        'state': 'available', 'rotated': rotated, 'truncated': shrunk or skipped or dropped or limited,
        'work_state': 'not_checked',
    }
