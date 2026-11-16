"""Reload observes persisted consent; only own synthetic file-vault data."""

from unittest.mock import Mock

import pytest

from okto_nexus_connector.daemon import app as daemon_module
from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.daemon.r4_control import R4DaemonControl
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import KeyringVault, RestrictedFileVault
from okto_nexus_connector.storage.state_store import (
    ExecutionExecutorRecord, ServerProfileRecord,
)


KEY = RestrictedFileVault.approval_key()
SENTINEL = 'synthetic-consent-reload-sentinel'
HANDLE = 'vault:srv/agent'


def persist(app, value):
    def mutate(state):
        if value is None:
            state.preferences.pop(KEY, None)
        else:
            state.preferences[KEY] = value
    app.store.update(mutate)


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file')
    result = DaemonApp(tmp_path)
    # A registration already exists when the CLI later asks this daemon to
    # reload. Its live client must keep the same vault reference.
    def registered(state):
        state.servers['srv'] = ServerProfileRecord(
            'srv', 'https://synthetic.invalid', 'https://synthetic.invalid', 'now')
        state.execution_executors.append(ExecutionExecutorRecord(
            'srv', state.connector_id, 'agent', 'synthetic-registration', 'test'))
    result.store.update(registered)
    control = R4DaemonControl(result.store, result.vault, result.host, 'srv')
    control.start = Mock(side_effect=AssertionError('No transport start selected'))
    result.r4_controls['srv'] = control
    return result


def assert_denied(vault):
    with pytest.raises(ConnectorError) as caught:
        vault.resolve(HANDLE)
    assert caught.value.code == 'APPROVAL_REQUIRED'


async def test_reload_enables_existing_references_only_after_persisted_consent(app, monkeypatch):
    original = app.vault
    clients = [app.host._vault, app.runtimes._vault, app.r4_controls['srv'].vault]
    assert all(v is original for v in clients)
    assert_denied(original)
    # Model the CLI's independently opened, explicitly approved vault. No
    # provider credentials, network, daemon process or service is involved.
    persist(app, True)
    cli_vault = RestrictedFileVault(original.directory, approved=True)
    cli_vault.store('srv/agent', SENTINEL)
    assert_denied(original)  # The pre-reload daemon snapshot was still denied.
    reopened = Mock(side_effect=AssertionError('Backend must not be reopened'))
    monkeypatch.setattr(daemon_module, 'open_vault', reopened)
    reads = Mock(wraps=original._load)
    monkeypatch.setattr(original, '_load', reads)
    state_before = app.store.path.read_bytes()

    await app.reload_state()

    reads.assert_not_called()
    reopened.assert_not_called()
    assert app.store.path.read_bytes() == state_before
    assert app.vault is original and all(v is original for v in clients)
    assert all(v.resolve(HANDLE) == SENTINEL for v in clients)
    app.r4_controls['srv'].start.assert_not_called()


@pytest.mark.parametrize('consent', [False, None, 'true', 1])
async def test_removed_or_non_boolean_consent_blocks_next_read(app, monkeypatch, consent):
    persist(app, True)
    cli_vault = RestrictedFileVault(app.vault.directory, approved=True)
    cli_vault.store('srv/agent', SENTINEL)
    await app.reload_state()
    existing_client = app.r4_controls['srv'].vault
    assert existing_client.resolve(HANDLE) == SENTINEL
    persist(app, consent)
    reads = Mock(side_effect=AssertionError('Denied access must not read the file'))
    monkeypatch.setattr(app.vault, '_load', reads)

    await app.reload_state()

    assert_denied(existing_client)
    reads.assert_not_called()
    app.r4_controls['srv'].start.assert_not_called()


async def test_missing_consent_does_not_become_approval_on_reload(app, monkeypatch):
    reads = Mock(side_effect=AssertionError('No secret read was authorized'))
    monkeypatch.setattr(app.vault, '_load', reads)
    await app.reload_state()
    assert_denied(app.vault)
    reads.assert_not_called()


async def test_keyring_backend_is_not_reselected_or_read(tmp_path, monkeypatch):
    keyring = Mock()
    selected = KeyringVault(keyring)
    opener = Mock(return_value=selected)
    monkeypatch.setattr(daemon_module, 'open_vault', opener)
    app = DaemonApp(tmp_path)
    persist(app, True)
    await app.reload_state()
    persist(app, False)
    await app.reload_state()
    assert app.vault is selected and app.host._vault is selected and app.runtimes._vault is selected
    opener.assert_called_once()
    assert not keyring.mock_calls


@pytest.mark.parametrize('consent', ['true', 1])
def test_initial_consent_is_also_strict_boolean(tmp_path, monkeypatch, consent):
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file')
    first = DaemonApp(tmp_path)
    persist(first, consent)
    second = DaemonApp(tmp_path)
    assert_denied(second.vault)
