"""Offer the Nexus native viewer after a confirmed Connector setup."""

import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from ...errors import ConnectorError
from ..prompts import confirm

_PROFILE = re.compile(r'[A-Za-z0-9_-]{1,80}')
_ID = re.compile(r'[A-Za-z0-9_-]{1,160}')
_SCOPE = ('server_id', 'executor_id', 'binding_id', 'workspace_id', 'workspace_binding_id', 'agent_id')
_VIEWERS = {
    'codex_app_server': ('Codex', 'codex_remote_read'),
    'pi_rpc': ('Pi', 'pi_remote_read'),
    'claude_stream': ('Claude', 'claude_remote_read'),
}


def _tty(output):
    return sys.stdin.isatty() and output.stream.isatty()


def validate_terminal_request(args, output):
    if args.terminal is True and (args.json or args.non_interactive or not _tty(output)):
        raise ConnectorError('VALIDATION_ERROR', 'connect', 'Opening the native session requires a terminal.')
    if args.operator_profile is not None and not _PROFILE.fullmatch(args.operator_profile):
        raise ConnectorError('VALIDATION_ERROR', 'connect', 'Use a valid saved Nexus operator profile name.')


def _nexus_executable():
    # Consider only absolute PATH entries and invoke a native Windows launcher.
    name = 'okto-nexus.exe' if os.name == 'nt' else 'okto-nexus'
    for entry in os.get_exec_path():
        directory = Path(entry)
        candidate = directory / name
        if directory.is_absolute() and candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _operator_environment(credential_env):
    # The child opens its own human login. Agent and provider credentials from
    # Connector setup do not need to enter the viewer process environment.
    names = {'HOME', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA', 'PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR',
        'TEMP', 'TMP', 'TMPDIR', 'COMSPEC', 'PROGRAMDATA', 'HOMEDRIVE', 'HOMEPATH', 'TERM', 'TERM_PROGRAM',
        'COLORTERM', 'NO_COLOR', 'LANG', 'LC_ALL', 'LC_CTYPE', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME',
        'XDG_RUNTIME_DIR', 'DBUS_SESSION_BUS_ADDRESS', 'OKTO_NEXUS_HOME'}
    if credential_env:
        names.discard(credential_env.upper())
    return {key: value for key, value in os.environ.items() if key.upper() in names}


def _launch(executable, argv, scope, credential_env, *, expected_strategy):
    """Run the Nexus client in this terminal and read its non-secret detach receipt."""
    try:
        with TemporaryDirectory(prefix='nexus-connect-terminal-') as temporary:
            receipt = Path(temporary) / 'attachment.json'
            previous_interrupt = None
            if expected_strategy in ('pi_remote_read', 'claude_remote_read'):
                # Ctrl+C reaches the launcher too. The viewer handles detach
                # and writes the receipt before the launcher returns.
                previous_interrupt = signal.signal(signal.SIGINT, lambda *_: None)
            try:
                result = subprocess.run([str(executable), *argv[1:], '--attachment-receipt', str(receipt)],
                                        env=_operator_environment(credential_env), check=False)
            finally:
                if previous_interrupt is not None:
                    signal.signal(signal.SIGINT, previous_interrupt)
            if result.returncode != 0:
                return {'status': 'unavailable', 'reason': 'nexus_attach_failed'}
            with receipt.open('rb') as file:
                raw = file.read(4097)
            if len(raw) > 4096:
                raise ValueError('Oversized attachment receipt')
            value = json.loads(raw)
    except KeyboardInterrupt:
        return {'status': 'unconfirmed', 'reason': 'attachment_interrupted'}
    except (OSError, ValueError):
        return {'status': 'unconfirmed', 'reason': 'attachment_receipt_unavailable'}
    if value == {'status': 'declined'}:
        return value
    selected = value.get('scope') if isinstance(value, dict) else None
    if (not isinstance(selected, dict) or any(selected.get(key) != item for key, item in scope.items())
            or selected.keys() != {*scope, 'session_id'} or value.get('status') != 'detached'
            or type(selected['session_id']) is not str or not _ID.fullmatch(selected['session_id'])
            or type(value.get('attachment_id')) is not str or not re.fullmatch(r'att_[0-9a-f]{32}', value['attachment_id'])
            or type(value.get('capability_revision')) is not str
            or not re.fullmatch(r'sha256:[0-9a-f]{64}', value['capability_revision'])
            or value.get('attach_strategy') != expected_strategy):
        return {'status': 'unconfirmed', 'reason': 'attachment_receipt_mismatch'}
    return {key: value[key] for key in ('status', 'scope', 'attachment_id', 'attach_strategy', 'capability_revision')}


def offer_terminal(args, output, result):
    if result.get('saved') is not True:
        return result
    interactive = not args.json and not args.non_interactive and _tty(output)
    requested = 'native' if args.terminal is True else 'ask' if args.terminal is None and interactive else 'none'
    interaction = {'requested_surface': requested, 'resolved_surface': 'none', 'status': 'not_requested'}
    result = dict(result, interaction=interaction)
    binding = result.get('current_binding') or result.get('binding') or {}
    scope = {key: binding.get(key) for key in _SCOPE}
    revision = binding.get('binding_revision')
    adapter = binding.get('adapter_id')
    viewer = _VIEWERS.get(adapter) if isinstance(adapter, str) else None
    if (viewer is None or type(revision) is not int or revision <= 0
            or any(type(value) is not str or not _ID.fullmatch(value) for value in scope.values())):
        if requested != 'none':
            interaction.update(status='unavailable', reason='binding_presentation_unavailable')
        return result
    interaction['scope'] = scope
    argv = ['okto-nexus', 'session', 'attach', '--binding-id', scope['binding_id'],
            '--server-id', scope['server_id'], '--executor-id', scope['executor_id'],
            '--workspace-id', scope['workspace_id'], '--binding-revision', str(revision), '--operator-profile']
    profile = args.operator_profile
    interaction['attach_argv'] = [*argv, profile or 'OPERATOR_PROFILE']
    if requested == 'none':
        return result
    executable = _nexus_executable()
    if executable is None:
        interaction.update(status='unavailable', reason='nexus_cli_unavailable')
        return result
    output.line('Connection saved.')
    name, strategy = viewer
    if requested == 'ask' and not confirm(f'Open {name} in this terminal?', non_interactive=False, default=False):
        interaction['status'] = 'declined'
        return result
    if profile is None:
        try:
            profile = input('Saved Nexus operator profile (Enter to cancel): ').strip()
        except (EOFError, KeyboardInterrupt):
            profile = ''
        if not profile:
            interaction['status'] = 'declined'
            return result
        if not _PROFILE.fullmatch(profile):
            interaction.update(status='unavailable', reason='operator_profile_invalid')
            return result
    interaction['attach_argv'] = [*argv, profile]
    launched = _launch(executable, interaction['attach_argv'], scope, args.credential_env, expected_strategy=strategy)
    interaction.update(launched)
    if launched['status'] == 'detached':
        interaction['resolved_surface'] = 'native'
    return result
