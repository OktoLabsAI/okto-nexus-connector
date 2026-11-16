from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from okto_nexus_connector import __version__
from okto_nexus_connector.cli.main import main
from okto_nexus_connector.daemon import manager
from okto_nexus_connector.daemon.lock import InstanceLock, Readiness
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform.service_identity import service_instance_id
from okto_nexus_connector.platform.sysinfo import current_process_identity


@pytest.fixture
def owner(tmp_path):
    lock = InstanceLock(tmp_path / 'run')
    assert lock.acquire()
    birth = current_process_identity()
    readiness = Readiness(birth.pid, birth.birth_token, 'loopback', '127.0.0.1:1',
        'now', __version__, service_instance_id(tmp_path), 'generation')
    lock.write_readiness(readiness)
    lock.ensure_token()
    try:
        yield tmp_path, lock, readiness
    finally:
        lock.release()


def response(readiness, **changes):
    return {'ok': True, 'result': {'pong': True, 'version': __version__,
        'pid': readiness.pid, 'instance_id': readiness.instance_id,
        'generation': readiness.generation, 'protocol_revision': 1, 'draining': False, **changes}}


def client_response(monkeypatch, result):
    client = Mock()
    client.__enter__ = Mock(return_value=client)
    client.__exit__ = Mock(return_value=False)
    client.call.return_value = result
    monkeypatch.setattr(manager, 'IPCClient', Mock(return_value=client))
    return client


def test_absent_daemon_status_does_not_create_state_or_start(tmp_path, capsys, monkeypatch):
    root = tmp_path / 'missing'
    spawn = Mock(side_effect=AssertionError('Started from status'))
    monkeypatch.setattr(manager.subprocess, 'Popen', spawn)
    assert main(['--state-dir', str(root), 'daemon', 'status', '--json']) == 0
    data = json.loads(capsys.readouterr().out)
    assert data['state'] == 'stopped' and data['work_state'] == 'not_checked'
    assert not root.exists()


def test_matching_process_without_os_guard_never_proves_owner(owner):
    root, lock, _ = owner
    lock.release()
    result = manager.status(root)
    assert result.state == 'stopped' and not result.running


def test_local_readiness_does_not_claim_remote_link_or_work(owner, monkeypatch):
    root, _, readiness = owner
    client_response(monkeypatch, response(readiness))
    view = manager.status(root).to_json()
    assert view['state'] == 'ready' and view['ready_for_queries']
    assert view['generation'] == readiness.generation
    assert view['remote_link'] == view['work_state'] == 'not_checked'
    assert view['installed_version'] == view['active_version'] == __version__
    assert service_instance_id(root / 'alias' / '..') == view['instance_id']


@pytest.mark.parametrize('change', [{'generation': 'old'}, {'instance_id': 'different'},
                                   {'pid': 123456789}, {'version': 'future'}, {'protocol_revision': 0}])
def test_wrong_ipc_identity_never_becomes_ready(owner, monkeypatch, change):
    root, _, readiness = owner
    client_response(monkeypatch, response(readiness, **change))
    result = manager.status(root)
    assert result.running and result.state == 'degraded'
    assert result.reason == 'IPC_IDENTITY_MISMATCH'


def test_unreachable_live_owner_is_not_absent_or_respawned(owner, monkeypatch):
    root, _, readiness = owner
    client = client_response(monkeypatch, response(readiness))
    client.call.side_effect = OSError('offline')
    result = manager.status(root)
    assert result.running and result.state == 'degraded' and result.reason == 'IPC_UNREACHABLE'
    spawn = Mock(side_effect=AssertionError('Duplicate start'))
    monkeypatch.setattr(manager.subprocess, 'Popen', spawn)
    with pytest.raises(ConnectorError):
        manager.start(root, timeout=.01)
    assert not spawn.called


def test_stopping_owner_is_not_reused_as_ready(owner, monkeypatch):
    root, _, readiness = owner
    client_response(monkeypatch, response(readiness, draining=True))
    assert manager.status(root).state == 'stopping'
    with pytest.raises(ConnectorError):
        manager.start(root, timeout=.01)
