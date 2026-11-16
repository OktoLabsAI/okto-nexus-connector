"""Diagnostics observe existing state without provisioning or starting anything."""
from dataclasses import replace
from io import StringIO
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace as NS

import pytest

from okto_nexus_connector.cli.commands import diagnostics, doctor
from okto_nexus_connector.cli.main import main
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.daemon import manager
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage import read_only
from okto_nexus_connector.storage.state_store import (
    ConnectorState, IdentityRecord, ServerProfileRecord, StateStore, state_to_json,
)
from okto_nexus_connector.transport.proxy import load_policy


@pytest.fixture(autouse=True)
def empty_inventory(monkeypatch):
    # Discovery has its own passive-inventory contract tests; do not scan the
    # developer's installed harnesses in these state/vault/diagnostic tests.
    async def inventory():
        return []
    monkeypatch.setattr(doctor, 'discover_inventory', inventory)


def forbidden(*args, **kwargs):
    raise AssertionError('A passive command attempted provisioning or process launch')


def guard_mutations(monkeypatch):
    monkeypatch.setattr(StateStore, 'load', forbidden)
    monkeypatch.setattr(StateStore, 'save', forbidden)
    monkeypatch.setattr(StateStore, 'update', forbidden)
    monkeypatch.setattr(manager, 'start', forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    monkeypatch.setattr(Path, 'mkdir', forbidden)
    monkeypatch.setattr(Path, 'chmod', forbidden)
    monkeypatch.setattr(Path, 'write_text', forbidden)
    monkeypatch.setattr(Path, 'write_bytes', forbidden)


@pytest.mark.parametrize('command', ['status', 'logs', 'doctor'])
@pytest.mark.parametrize('default', [False, True])
def test_absent_instance_stays_absent(command, default, tmp_path, monkeypatch, capsys):
    root = tmp_path / 'absent'
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_STATE', str(root))
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file')
    guard_mutations(monkeypatch)
    args = ([] if default else ['--state-dir', str(root)]) + [command, '--json']
    assert main(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert not root.exists()
    if command == 'status':
        assert payload['daemon']['state'] == 'stopped'
        assert payload['daemon']['work_state'] == 'not_checked'
    elif command == 'logs':
        assert payload['state'] == 'missing' and payload['work_state'] == 'not_checked'
    else:
        assert next(c for c in payload['checks'] if c['name'] == 'state-dir')['status'] == 'fail'


@pytest.mark.parametrize('command', ['status', 'logs', 'doctor'])
def test_existing_snapshot_is_not_rewritten_or_locked(command, tmp_path, monkeypatch, capsys):
    state = ConnectorState(connector_id='retained', preferences={'network.proxy': {'mode': 'direct'}})
    path = tmp_path / 'state.json'
    path.write_text(json.dumps(state_to_json(state)), encoding='utf-8')
    before, mode, modified = path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file')
    guard_mutations(monkeypatch)
    assert main(['--state-dir', str(tmp_path), command, '--json']) == 0
    assert load_policy(tmp_path) == {'mode': 'direct'}
    assert list(tmp_path.iterdir()) == [path]
    assert (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns) == (before, mode, modified)
    capsys.readouterr()


@pytest.mark.parametrize('state_name', ['unknown', 'starting', 'failed', 'degraded'])
async def test_unconfirmed_daemon_never_becomes_stopped(state_name, tmp_path, monkeypatch):
    state = ConnectorState()
    state.servers['s'] = ServerProfileRecord('s', 'http://example.invalid', '', '')
    state.identities.append(IdentityRecord('agent', 's', 'a', 'vault:s/a', 1, ''))
    (tmp_path / 'state.json').write_text(json.dumps(state_to_json(state)), encoding='utf-8')
    current = replace(manager.status(tmp_path), state=state_name, reason='INSTANCE_OWNERSHIP_UNCONFIRMED',
                      running=state_name == 'degraded')
    monkeypatch.setattr(manager, 'status', lambda _: current)
    monkeypatch.setattr(manager, 'connect', forbidden)
    async def checked(*args):
        return {}
    monkeypatch.setattr(diagnostics, '_query_connections', checked)
    result = await diagnostics.run_status(NS(agent=None), Output(json_mode=True), tmp_path)
    assert result['daemon']['state'] == state_name
    assert result['servers'][0]['state'] == result['agents'][0]['status'] == f'DAEMON_{state_name.upper()}'
    stream = StringIO()
    diagnostics.render_status(result, Output(json_mode=False, stream=stream))
    assert f'Daemon: {state_name}' in stream.getvalue()
    assert 'stopped' not in stream.getvalue().lower()
    report = await doctor.run_doctor(NS(probe=False), Output(json_mode=True), tmp_path)
    process = next(c for c in report['checks'] if c['layer'] == 'daemon')
    assert process['status'] == 'warn' and state_name in process['detail']


async def test_status_reads_existing_credential_without_initializing_vault(tmp_path, monkeypatch):
    state = ConnectorState()
    state.servers['s'] = ServerProfileRecord('s', 'http://example.invalid', '', '')
    state.identities.append(IdentityRecord('agent', 's', 'a', 'vault:s/a', 1, ''))
    used = []
    def vault(path, **kwargs):
        used.append((path, kwargs))
        raise ConnectorError('APPROVAL_REQUIRED', 'vault', 'not approved')
    monkeypatch.setattr(diagnostics, 'open_vault', vault)
    guard_mutations(monkeypatch)
    result = await diagnostics._query_connections(tmp_path, state)
    assert result['agent']['error'] == 'APPROVAL_REQUIRED'
    assert used == [(tmp_path / 'vault', {'approved_fallback': False})]
    assert list(tmp_path.iterdir()) == []


def test_unreadable_logs_have_stable_unavailable_exit(tmp_path, monkeypatch, capsys):
    def denied(*args):
        raise PermissionError('nxs_' + 'x' * 32)
    monkeypatch.setattr(diagnostics, 'read_log', denied)
    assert main(['--state-dir', str(tmp_path), 'logs', '--json']) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload['error']['code'] == 'LOG_UNAVAILABLE'
    assert 'x' * 32 not in str(payload)
    assert 'unknown' in payload['error']['message']


@pytest.mark.parametrize('content,expected', [(b'{broken', 'STATE_INVALID'), (b'[]', 'STATE_INVALID'),
    (b' ' * (4 * 1024 * 1024 + 1), 'CAPACITY_EXCEEDED'),
    (b'{"schema_version":999}', 'VERSION_INCOMPATIBLE')], ids=['invalid-json', 'wrong-shape', 'oversize', 'newer-schema'])
def test_snapshot_error_preserves_file(content, expected, tmp_path):
    path = tmp_path / 'state.json'
    path.write_bytes(content)
    with pytest.raises(ConnectorError) as raised:
        StateStore(path).read_snapshot()
    assert raised.value.code == expected
    assert list(tmp_path.iterdir()) == [path] and path.read_bytes() == content


def test_snapshot_permission_error_is_not_empty_state(tmp_path, monkeypatch):
    def denied(*args):
        raise PermissionError('private path')
    monkeypatch.setattr(read_only, 'open_readonly', denied)
    with pytest.raises(ConnectorError, match='STATE_UNAVAILABLE'):
        StateStore(tmp_path / 'state.json').read_snapshot()


def test_atomic_replacement_survives_a_concurrent_snapshot_reader(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    path = tmp_path / 'state.json'
    old = b'{"connector_id":"old"}'
    path.write_bytes(old)
    reading = threading.Event()
    def observe():
        with read_only.open_readonly(path) as snapshot:
            reading.set()
            assert snapshot.read() == old
            # Reproduce the real Windows sharing conflict while a passive
            # reader is holding an existing atomic snapshot.
            threading.Event().wait(.1)
    with ThreadPoolExecutor(max_workers=1) as pool:
        observing = pool.submit(observe)
        assert reading.wait(5)
        StateStore(path).save(ConnectorState(connector_id='new'))
        observing.result(timeout=5)
    assert StateStore(path).read_snapshot().connector_id == 'new'


def test_diagnostic_does_not_wait_for_or_take_writer_lock(tmp_path):
    store = StateStore(tmp_path / 'state.json')
    store.save(ConnectorState(connector_id='previous'))
    with store.locked():
        assert store.read_snapshot().connector_id == 'previous'


def test_denied_replacement_preserves_previous_snapshot(tmp_path, monkeypatch):
    from okto_nexus_connector.storage import state_store
    path = tmp_path / 'state.json'
    store = StateStore(path)
    store.save(ConnectorState(connector_id='retained'))
    def denied(*args):
        raise PermissionError('persistent denial')
    monkeypatch.setattr(state_store.os, 'replace', denied)
    with pytest.raises(PermissionError):
        store.save(ConnectorState(connector_id='new'))
    assert store.read_snapshot().connector_id == 'retained'
    assert not list(tmp_path.glob('.state-*.tmp'))


@pytest.mark.parametrize('flags', [[], ['--json'], ['--verbose']])
def test_unconfirmed_status_exit_is_consistent(flags, tmp_path, monkeypatch, capsys):
    current = replace(manager.status(tmp_path), state='unknown', reason='INSTANCE_OWNERSHIP_UNCONFIRMED')
    monkeypatch.setattr(manager, 'status', lambda _: current)
    assert main(['--state-dir', str(tmp_path), 'status', *flags]) == 3
    assert 'stopped' not in capsys.readouterr().out.lower()
