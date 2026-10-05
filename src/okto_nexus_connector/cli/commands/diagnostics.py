"""Read-only connection overview and bounded daemon log inspection."""
import asyncio
from collections import deque
from datetime import datetime, timezone

from ...daemon import manager
from ...errors import ConnectorError
from ...platform import paths
from ...redaction import redact_mapping, redact_text
from ...storage.state_store import StateStore


def _stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value else None


async def run_status(args, output, root):
    state = StateStore(paths.state_file(root)).load()
    current = manager.status(root)
    live, ipc_error = {}, None
    if current.running:
        try:
            client = manager.connect(root)
            try:
                response = client.call('status')
                if not response.get('ok', False):
                    ipc_error = 'Daemon status request failed; inspect logs.'
                else:
                    live = response.get('result', {})
            finally:
                client.close()
        except (ConnectorError, OSError) as exc:
            ipc_error = redact_text(str(exc))
    controls = live.get('r4_controls', {})
    servers = []
    for server_id, profile in state.servers.items():
        control = controls.get(server_id, {})
        servers.append(dict(server=profile.base_url, server_id=server_id,
            state=control.get('state', ('UNKNOWN' if ipc_error else 'NOT_REGISTERED')
                              if current.running else 'DAEMON_STOPPED'),
            control_ready=control.get('control_ready', False),
            error=control.get('error_code'), attempts=control.get('attempts', 0),
            last_connected_at=_stamp(control.get('last_connected_at')),
            last_error_at=_stamp(control.get('last_error_at')),
            last_received_at=_stamp(control.get('last_received_at')),
            recovery_required=control.get('recovery_required', False)))
    agents = []
    for identity in state.identities:
        if getattr(args, 'agent', None) and identity.agent_id != args.agent:
            continue
        control = controls.get(identity.server_id, {})
        bindings = [b for b in state.execution_bindings
                    if (b.server_id, b.agent_id) == (identity.server_id, identity.agent_id)]
        attached = set(control.get('attached_bindings', []))
        connected = bool(control.get('control_ready') and control.get('execution_ready') and
                         any(b.binding_id in attached for b in bindings))
        prefix = f'configure.{identity.server_id}.{identity.agent_id}.'
        pending = [dict(request_id=key[len(prefix):], status=value.get('stage', 'unknown'))
                   for key, value in state.preferences.items()
                   if key.startswith(prefix) and isinstance(value, dict) and value.get('stage') != 'done']
        status = ('REVOKED' if identity.revoked else 'CONNECTED' if connected else
                  'DAEMON_STOPPED' if not current.running else
                  'CONNECTION_ERROR' if control.get('error_code') else
                  'SETUP_PENDING' if not bindings else 'NOT_ATTACHED')
        action = {'REVOKED': 'Import a valid agent key.',
                  'DAEMON_STOPPED': 'Run: okto-nexus-connector daemon start',
                  'CONNECTION_ERROR': 'Run: okto-nexus-connector logs --errors; check server availability and approvals.',
                  'SETUP_PENDING': ('Review Nexus approvals and resume connect with --request-id.'
                                    if pending else 'Run connect to complete this agent setup.'),
                  'NOT_ATTACHED': 'Review the Remote connection and runtime authorization in Nexus.'}.get(status, '')
        agents.append(dict(agent=identity.agent_id, identity=identity.alias,
            server_id=identity.server_id, status=status,
            harnesses=sorted({b.adapter_id for b in bindings}),
            bindings=[dict(id=b.binding_id, state=b.state, attached=b.binding_id in attached)
                      for b in bindings],
            pending_requests=pending,
            action=action))
    result = dict(daemon={'running': current.running, 'pid': current.pid, 'ipc_error': ipc_error},
                  servers=servers, agents=agents,
                  logs=str(root / 'logs' / 'daemon.log'))
    if not output.json_mode:
        output.line('Connector status')
        output.line(f"Daemon: {'running' if current.running else 'stopped'}" +
                    (f' (PID {current.pid})' if current.pid else ''))
        if ipc_error:
            output.line(f'IPC error: {ipc_error}')
        for server in servers:
            output.line(redact_text(f"Server: {server['server']} | {server['state']}"))
            for field in ('error', 'last_connected_at', 'last_received_at', 'last_error_at'):
                if server[field]:
                    output.line(f"  {field.replace('_', ' ')}: {server[field]}")
        for agent in agents:
            names = {'codex_app_server': 'Codex', 'claude_stream': 'Claude Code', 'pi_rpc': 'Pi'}
            harnesses = ', '.join(names.get(name, name) for name in agent['harnesses'])
            output.line(f"Agent: {agent['agent']} | {agent['status']}" + (f' | {harnesses}' if harnesses else ''))
            if agent['action']:
                output.line(f"  Next: {agent['action']}")
            for request in agent['pending_requests']:
                output.line(f"  Request: {request['request_id']} ({request['status']})")
        if not agents:
            output.line('No matching agents configured. Run connect to configure an agent.')
        output.line(f"Logs: {result['logs']}")
        output.line('Details: --json status | Live logs: logs --follow | Diagnostics: doctor --probe')
        return None
    return redact_mapping(result)


async def run_logs(args, output, root):
    path = root / 'logs' / 'daemon.log'
    offset, file_id = 0, None
    first = True
    while True:
        lines = []
        try:
            with path.open('r', encoding='utf-8', errors='replace') as stream:
                stat = path.stat()
                identity = (stat.st_dev, stat.st_ino)
                if first:
                    lines = list(deque(stream, maxlen=args.tail))
                else:
                    if identity != file_id or stat.st_size < offset:
                        offset = 0
                    stream.seek(offset)
                    lines = list(deque(stream, maxlen=args.tail))
                offset, file_id = stream.tell(), identity
        except FileNotFoundError:
            if first:
                output.line('No daemon log yet. Start or restart the updated daemon to enable logging.')
        for line in lines:
            if args.errors and not any(f' {level} ' in line for level in ('WARNING', 'ERROR', 'CRITICAL')):
                continue
            safe = redact_text(line.rstrip())
            if output.json_mode:
                output.stream_record({'type': 'daemon_log', 'line': safe})
            else:
                output.line(safe)
        output.stream.flush()
        if not args.follow:
            return None
        first = False
        await asyncio.sleep(1)
