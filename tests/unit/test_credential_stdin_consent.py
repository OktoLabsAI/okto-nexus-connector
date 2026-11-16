"""Consent must never consume the protected credential stream."""
import io
from types import SimpleNamespace

import pytest

from okto_nexus_connector.cli.commands import connect, identity
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.state_store import StateStore
from tests.unit import test_remote_setup as remote

host = remote.host
peer = remote.peer
continuation = remote.continuation
READ_KEY = connect._read_key


@pytest.mark.parametrize('forced_file', [False, True])
async def test_remote_resume_refuses_before_consuming_credential(continuation, monkeypatch, forced_file):
    r = continuation
    r.args.credential_stdin = True
    r.host.store.update(lambda state: state.preferences.pop('vault.fallback_file.approved'))
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file' if forced_file else '')
    monkeypatch.setattr(connect, 'vault_backend_names', lambda: ('synthetic',) if forced_file else ())
    monkeypatch.setattr(connect, '_read_key', READ_KEY)
    monkeypatch.setattr(identity, '_vault', lambda *a: pytest.fail('vault opened before consent'))
    stream = io.StringIO(remote.KEY + '\n')
    monkeypatch.setattr('sys.stdin', stream)
    before = r.host.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == 'APPROVAL_REQUIRED'
    assert stream.tell() == 0
    assert r.host.store.path.read_bytes() == before
    assert r.peer.requests == []


@pytest.mark.parametrize('approved', [False, True], ids=['backend-available', 'fallback-approved'])
async def test_remote_resume_reads_only_credential_after_storage_ready(continuation, monkeypatch, approved):
    r = continuation
    r.args.credential_stdin = True
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', '')
    if not approved:
        r.host.store.update(lambda state: state.preferences.pop('vault.fallback_file.approved'))
    monkeypatch.setattr(connect, 'vault_backend_names', lambda: () if approved else ('synthetic',))
    monkeypatch.setattr(connect, '_read_key', READ_KEY)
    stream = io.StringIO(remote.KEY + '\nuntouched-next-line\n')
    monkeypatch.setattr('sys.stdin', stream)
    calls = []

    async def pipeline(*args):
        calls.append('configured-after-authentication')
        return {'saved': False, 'status': 'awaiting_operator_approval'}

    monkeypatch.setattr(remote.configure_local, 'configure_local', pipeline)
    result = await connect.run_connect(r.args, r.host.output, r.host.root)
    assert result['status'] == 'awaiting_operator_approval'
    assert calls == ['configured-after-authentication']
    assert stream.read() == 'untouched-next-line\n'
    assert len(r.host.store.load().identities) == 1
    assert r.vault.resolve(r.host.store.load().identities[0].secret_handle) == remote.KEY
    assert remote.KEY not in r.host.store.path.read_text()


@pytest.mark.parametrize('answer,approved', [('yes', True), ('no', False)])
def test_interactive_consent_still_requires_explicit_answer(tmp_path, monkeypatch, answer, approved):
    store = StateStore(tmp_path / 'state.json')
    stream = io.StringIO(answer + '\ncredential-not-read\n')
    monkeypatch.setattr('sys.stdin', stream)
    args = SimpleNamespace(credential_stdin=False, non_interactive=False)
    assert identity._approve_fallback(args, Output(json_mode=False), store) is approved
    assert bool(store.load().preferences.get('vault.fallback_file.approved')) is approved
    assert stream.read() == 'credential-not-read\n'


async def test_remote_resume_denied_consent_stops_before_vault_or_key(continuation, monkeypatch):
    r = continuation
    r.host.store.update(lambda state: state.preferences.pop('vault.fallback_file.approved'))
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', '')
    monkeypatch.setattr(connect, 'vault_backend_names', lambda: ())
    monkeypatch.setattr(identity, '_vault', lambda *a: pytest.fail('vault opened after denial'))
    stream = io.StringIO('no\ncredential-not-read\n')
    monkeypatch.setattr('sys.stdin', stream)
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == 'APPROVAL_REQUIRED'
    assert stream.read() == 'credential-not-read\n'
    assert r.peer.requests == []


@pytest.mark.parametrize('command', ['connect', 'identity'])
async def test_other_import_entrypoints_preserve_stdin_before_consent(tmp_path, monkeypatch, command):
    monkeypatch.setenv('OKTO_NEXUS_CONNECTOR_VAULT', 'file')
    stream = io.StringIO(remote.KEY + '\n')
    monkeypatch.setattr('sys.stdin', stream)
    common = ['--server', 'https://nexus.example', '--agent', 'agent', '--credential-stdin']
    argv = ['connect', *common] if command == 'connect' else ['identity', 'add', '--alias', 'worker', *common]
    args = build_parser().parse_args(argv)
    output = Output(json_mode=False, stream=io.StringIO())
    runner = connect.run_connect if command == 'connect' else identity.run_identity
    with pytest.raises(ConnectorError) as error:
        await runner(args, output, tmp_path)
    assert error.value.code == 'APPROVAL_REQUIRED'
    assert stream.tell() == 0
