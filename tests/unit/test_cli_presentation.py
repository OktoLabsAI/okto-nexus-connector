import argparse
import io
import json
import os

import pytest

from okto_nexus_connector.cli.main import build_parser, main
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.cli import presentation
from okto_nexus_connector.cli.commands.diagnostics import render_status


@pytest.mark.parametrize('tokens', [
    ['--json', '--verbose', 'status'], ['status', '--json', '--verbose'],
    ['--verbose', 'identity', 'list', '--json'],
    ['identity', '--verbose', 'list', '--json'],
    ['identity', 'list', '--verbose', '--json'],
    ['--verbose', 'discover'], ['discover', '--verbose'],
])
def test_output_flags_work_at_every_command_depth(tokens):
    args = build_parser().parse_args(tokens)
    assert args.verbose
    assert args.json == ('--json' in tokens)


def test_every_subcommand_documents_common_output_options():
    def visit(parser):
        assert '--json' in parser._option_string_actions
        assert '--verbose' in parser._option_string_actions
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for child in action.choices.values():
                    visit(child)
    visit(build_parser())


def test_usage_errors_are_json_with_original_exit_code(capsys):
    with pytest.raises(SystemExit) as error:
        main(['reach', '--json'])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out)['error']['code'] == 'VALIDATION_ERROR'
    assert captured.err == ''


@pytest.mark.parametrize('non_interactive', [False, True])
def test_json_preserves_explicit_interaction_mode_and_payload(monkeypatch, capsys, non_interactive):
    from okto_nexus_connector.cli import commands
    payload = {'items': [{'id': 'original', 'empty': None}], 'ok': True}
    async def dispatch(args, output):
        assert args.non_interactive == non_interactive and output.verbose
        output.heading('Must not appear')
        output.line('Must not appear')
        output.table(['Must not appear'], [['hidden']])
        return payload
    monkeypatch.setattr(commands, 'dispatch', dispatch)
    assert main((['--non-interactive'] if non_interactive else []) + ['status', '--json', '--verbose']) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == payload
    assert '\x1b' not in captured.out and not captured.err


@pytest.mark.parametrize('width', [32, 80, 120])
def test_tables_fit_terminal_without_losing_long_values(monkeypatch, width):
    monkeypatch.setattr(presentation.shutil, 'get_terminal_size', lambda fallback: os.terminal_size((width, 24)))
    stream = io.StringIO()
    output = Output(json_mode=False, stream=stream)
    output.heading('Connections')
    output.table(['Agent', 'Harness', 'Status'], [['claude-with-a-long-agent-name', 'Claude Code', 'CONNECTION_ERROR']])
    text = stream.getvalue()
    assert '\x1b' not in text
    assert all(len(line) <= width for line in text.splitlines())
    assert 'CONNECTION_ERROR' in text.replace('\n', '').replace(' ', '')


def test_color_disabled_for_redirects_no_color_and_dumb_terminal(monkeypatch):
    stream = io.StringIO()
    assert presentation.styled('Ready', 'ok', stream) == 'Ready'
    monkeypatch.setattr(stream, 'isatty', lambda: True)
    monkeypatch.setenv('NO_COLOR', '')
    assert presentation.styled('Ready', 'ok', stream) == 'Ready'
    monkeypatch.delenv('NO_COLOR')
    monkeypatch.setenv('TERM', 'dumb')
    assert presentation.styled('Ready', 'ok', stream) == 'Ready'


def test_color_is_explicit_and_terminal_control_payload_is_removed(monkeypatch):
    monkeypatch.setattr(presentation, 'supports_color', lambda stream: True)
    assert presentation.styled('Ready', 'ok', io.StringIO()) == '\x1b[32mReady\x1b[0m'
    assert presentation.plain('\x1b[2Jagent\r') == 'agent'


def test_status_summary_keeps_failures_and_actions_visible():
    stream = io.StringIO()
    output = Output(json_mode=False, stream=stream)
    render_status(dict(daemon=dict(running=True, pid=123),
        servers=[dict(server='http://nexus:8202', state='CONTROL_READY', error=None)],
        agents=[dict(identity='claude', agent='coder', harnesses=['claude_stream'],
                     status='HARNESS_ERROR', action='Inspect provider login.', pending_requests=[{'request_id': 'long-id'}])],
        connections=[dict(identity='claude', agent='coder', adapter_id='claude_stream',
                          status='HARNESS_ERROR', error='PROVIDER_AUTH_REQUIRED')]), output)
    text = stream.getvalue()
    assert 'Connection' in text and 'Claude Code' in text and 'HARNESS_ERROR' in text
    assert 'PROVIDER_AUTH_REQUIRED' in text and 'Inspect provider login.' in text
    assert 'long-id' not in text and '1 pending' in text
    assert '\x1b' not in text
