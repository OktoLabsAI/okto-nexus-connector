"""Opening a viewer is a separate decision after an exact saved binding."""

import io
import json
from pathlib import Path
import signal
from types import SimpleNamespace

import pytest

from okto_nexus_connector.cli.commands import _dispatch, connect, connect_terminal
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError

SCOPE = {'server_id': 'srv_test', 'executor_id': 'exe_host', 'binding_id': 'bnd_test',
         'workspace_id': 'ws_project', 'workspace_binding_id': 'wbs_test', 'agent_id': 'codex'}
BINDING = SCOPE | {'adapter_id': 'codex_app_server', 'binding_revision': 4}


@pytest.fixture
def wizard(monkeypatch, tmp_path):
    calls = []
    saved = {'saved': True, 'binding': BINDING, 'runtime_started': False}

    async def run(args, output, root):
        assert root == tmp_path
        calls.append('setup')
        return saved

    monkeypatch.setattr(connect, 'run_connect', run)
    monkeypatch.setattr(connect_terminal, '_tty', lambda _: True)
    monkeypatch.setattr(connect_terminal, '_nexus_executable', lambda: tmp_path / 'okto-nexus.exe')

    async def dispatch(*options):
        args = build_parser().parse_args(['--state-dir', str(tmp_path), 'connect', *options])
        return await _dispatch(args, Output(json_mode=args.json, stream=io.StringIO()))

    return dispatch, calls, saved


@pytest.mark.asyncio
@pytest.mark.parametrize('adapter,name', [('codex_app_server', 'Codex'), ('pi_rpc', 'Pi')])
async def test_terminal_offer_declines_by_default_or_is_explicitly_skipped(wizard, monkeypatch, adapter, name):
    run, calls, saved = wizard
    saved['binding'] = BINDING | {'adapter_id': adapter}

    def confirm(question, *, non_interactive, default):
        assert question == f'Open {name} in this terminal?'
        assert non_interactive is False and default is False and calls == ['setup']
        calls.append('offer')
        return default

    monkeypatch.setattr(connect_terminal, 'confirm', confirm)
    monkeypatch.setattr(connect_terminal, '_launch', lambda *_: pytest.fail('No viewer should open'))
    declined = await run()
    skipped = await run('--no-interactive')
    assert calls == ['setup', 'offer', 'setup'] and 'interaction' not in saved
    assert declined['saved'] and declined['interaction']['status'] == 'declined'
    assert skipped['saved'] and skipped['interaction']['status'] == 'not_requested'
    assert skipped['interaction']['attach_argv'][-1] == 'OPERATOR_PROFILE'


@pytest.mark.asyncio
async def test_explicit_open_rejects_incompatible_input_before_setup(wizard, monkeypatch):
    run, calls, _ = wizard
    with pytest.raises(ConnectorError, match='requires a terminal'):
        await run('-i', '--json')
    monkeypatch.setattr(connect_terminal, '_tty', lambda _: False)
    with pytest.raises(ConnectorError, match='requires a terminal'):
        await run('-i')
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('adapter,strategy', [('codex_app_server', 'codex_remote_read'), ('pi_rpc', 'pi_remote_read')])
@pytest.mark.parametrize('state', ['valid', 'wrong-scope', 'wrong-strategy', 'failed', 'missing'])
async def test_connector_starts_saved_human_viewer_and_checks_receipt(wizard, monkeypatch, tmp_path, state, adapter, strategy):
    run, calls, saved = wizard
    saved['binding'] = BINDING | {'adapter_id': adapter}
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'synthetic')
    monkeypatch.setenv('TERM', 'synthetic-agent-key')
    monkeypatch.setattr(connect_terminal, 'confirm', lambda *_args, **_kwargs: pytest.fail('Already requested'))
    previous_interrupt = signal.getsignal(signal.SIGINT)

    def launch(argv, *, env, check):
        assert calls == ['setup'] and check is False
        calls.append('launch')
        assert argv[:-2] == [str(tmp_path / 'okto-nexus.exe'), 'session', 'attach', '--binding-id', 'bnd_test',
                             '--server-id', 'srv_test', '--executor-id', 'exe_host', '--workspace-id', 'ws_project',
                             '--binding-revision', '4', '--operator-profile', 'human']
        assert argv[-2] == '--attachment-receipt'
        assert not {'ANTHROPIC_API_KEY', 'TERM'} & env.keys()
        if strategy == 'pi_remote_read':
            assert signal.getsignal(signal.SIGINT) is not previous_interrupt
            signal.raise_signal(signal.SIGINT)
        else:
            assert signal.getsignal(signal.SIGINT) is previous_interrupt
        if state in ('valid', 'wrong-scope', 'wrong-strategy'):
            selected = SCOPE | {'session_id': 'ses_test'}
            if state == 'wrong-scope':
                selected['executor_id'] = 'exe_other'
            recorded = strategy if state != 'wrong-strategy' else next(
                other for other in ('codex_remote_read', 'pi_remote_read') if other != strategy)
            Path(argv[-1]).write_text(json.dumps({'status': 'detached', 'scope': selected,
                'attachment_id': 'att_' + 'f' * 32, 'attach_strategy': recorded,
                'capability_revision': 'sha256:' + 'a' * 64}), encoding='utf-8')
        return SimpleNamespace(returncode=3 if state == 'failed' else 0)

    monkeypatch.setattr(connect_terminal.subprocess, 'run', launch)
    result = await run('-i', '--operator-profile', 'human', '--credential-env', 'TERM')
    assert signal.getsignal(signal.SIGINT) is previous_interrupt
    assert result['saved'] and result['binding'] == saved['binding'] and result['runtime_started'] is False
    interaction = result['interaction']
    if state == 'valid':
        assert interaction['status'] == 'detached' and interaction['resolved_surface'] == 'native'
        assert interaction['scope'] == SCOPE | {'session_id': 'ses_test'}
        assert interaction['attach_strategy'] == strategy
    else:
        assert interaction['status'] == ('unavailable' if state == 'failed' else 'unconfirmed')
        assert interaction['resolved_surface'] == 'none' and 'attachment_id' not in interaction
    assert calls == ['setup', 'launch']


@pytest.mark.asyncio
async def test_pending_and_unqualified_bindings_do_not_offer_viewer(wizard, monkeypatch):
    run, calls, result = wizard
    monkeypatch.setattr(connect_terminal, '_nexus_executable', lambda: pytest.fail('No native viewer is eligible'))
    result.update(saved=False, status='awaiting_operator_approval')
    assert await run('-i') == result
    result.update(saved=True, binding=BINDING | {'adapter_id': 'claude_stream'})
    assert (await run('-i'))['interaction']['status'] == 'unavailable'
    assert calls == ['setup', 'setup']
