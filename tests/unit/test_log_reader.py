"""Bounded log reads preserve complete redaction units and close every handle."""
import io
import json
from types import SimpleNamespace as NS

import pytest

from okto_nexus_connector.cli.commands import diagnostics
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.daemon.log_reader import MAX_READ, MAX_RECORD, read_log


def test_reader_tails_with_a_byte_budget(tmp_path):
    path = tmp_path / 'daemon.log'
    path.write_bytes(b'ignored\n' * MAX_READ + b'latest one\nlatest two\n')
    lines, cursor, status = read_log(path, 2)
    assert lines == ['latest one', 'latest two']
    assert status['truncated'] is True and status['work_state'] == 'not_checked'
    assert cursor.offset == path.stat().st_size
    assert read_log(path, 2, cursor)[0] == []


def test_partial_secret_is_withheld_until_record_is_complete(tmp_path):
    path = tmp_path / 'daemon.log'
    path.write_bytes(b'ERROR nxs_')
    lines, cursor, _ = read_log(path, 50)
    assert lines == []
    with path.open('ab') as stream:
        stream.write(b'x' * 32 + b'\n')
    lines, cursor, _ = read_log(path, 50, cursor)
    assert lines == ['ERROR nxs_' + 'x' * 32]
    assert diagnostics.redact_text(lines[0]) == 'ERROR [redacted]'
    assert not cursor.pending


def test_oversize_record_never_exports_a_fragment(tmp_path):
    path = tmp_path / 'daemon.log'
    path.write_bytes(b'ERROR nxs_' + b'x' * (MAX_RECORD + 1))
    lines, cursor, status = read_log(path, 50)
    assert lines == [] and status['truncated'] is True
    assert cursor.discarding and not cursor.pending
    with path.open('ab') as stream:
        stream.write(b'secret-suffix\nsafe record\n')
    lines, cursor, _ = read_log(path, 50, cursor)
    assert lines == ['safe record'] and not cursor.discarding


def test_rotation_and_truncation_clear_pending_bytes(tmp_path):
    path = tmp_path / 'daemon.log'
    path.write_bytes(b'old prefix')
    _, cursor, _ = read_log(path, 50)
    path.rename(tmp_path / 'daemon.log.1')
    path.write_bytes(b'new record\n')
    lines, cursor, status = read_log(path, 50, cursor)
    assert lines == ['new record'] and status['rotated']
    path.write_bytes(b'short\n')
    lines, cursor, status = read_log(path, 50, cursor)
    assert lines == ['short'] and status['truncated']


async def test_json_follow_reports_missing_and_recovers_without_creating_root(tmp_path, monkeypatch):
    root = tmp_path / 'missing'
    calls = 0
    async def tick(_):
        nonlocal calls
        calls += 1
        if calls == 1:
            assert not root.exists()
            (root / 'logs').mkdir(parents=True)
            (root / 'logs' / 'daemon.log').write_text('ERROR nxs_' + 'x' * 32 + '\n', encoding='utf-8')
        else:
            raise KeyboardInterrupt
    monkeypatch.setattr(diagnostics.asyncio, 'sleep', tick)
    stream = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        await diagnostics.run_logs(NS(tail=50, follow=True, errors=False), Output(json_mode=True, stream=stream), root)
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [r['state'] for r in records if r['type'] == 'daemon_log_status'] == ['missing', 'available']
    assert records[-1] == {'type': 'daemon_log', 'line': 'ERROR [redacted]'}


async def test_follow_permission_failure_reports_uncertainty_and_keeps_observing(tmp_path, monkeypatch):
    calls = 0
    def denied(*args):
        raise PermissionError('sensitive exception')
    async def tick(_):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KeyboardInterrupt
    monkeypatch.setattr(diagnostics, 'read_log', denied)
    monkeypatch.setattr(diagnostics.asyncio, 'sleep', tick)
    stream = io.StringIO()
    with pytest.raises(KeyboardInterrupt):
        await diagnostics.run_logs(NS(tail=50, follow=True, errors=False), Output(json_mode=True, stream=stream), tmp_path)
    assert json.loads(stream.getvalue()) == {'type': 'daemon_log_status', 'state': 'unavailable',
                                           'code': 'LOG_UNAVAILABLE', 'work_state': 'not_checked'}
    assert calls == 2
