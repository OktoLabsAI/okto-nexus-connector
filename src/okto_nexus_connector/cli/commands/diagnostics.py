"""Read-only connection overview and bounded daemon log inspection."""
import asyncio
from collections import deque
from datetime import datetime, timezone

from ...daemon import manager
from ...errors import ConnectorError
from ...platform import paths
from ...redaction import redact_mapping, redact_text
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from .identity import _vault


async def _query_connections(root, store, state):
    """Bound the whole status probe; a cached attachment alone is not connectivity."""
    gate = asyncio.Semaphore(8)
    async def query(identity):
        try:
            async with asyncio.timeout(4), gate:
                profile = state.servers[identity.server_id]
                key = await asyncio.to_thread(_vault(root, store).resolve, identity.secret_handle)
                async with NexusHTTPClient(profile.base_url) as http:
                    result = await http._request('GET', '/v1/connections/status', key=key)
                if result.get('agent_id') != identity.agent_id or not isinstance(result.get('connections'), list):
                    raise ConnectorError('VERSION_INCOMPATIBLE', 'status', 'Invalid connection status response.')
                return identity.alias, result
        except (ConnectorError, OSError, TimeoutError, KeyError) as error:
            return identity.alias, dict(error=getattr(error, 'code', 'SERVER_UNREACHABLE'), connections=[])
    return dict(await asyncio.gather(*(query(i) for i in state.identities if not i.revoked)))


def _stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value else None


def render_status(result, output):
    names = {'codex_app_server': 'Codex', 'claude_stream': 'Claude Code', 'pi_rpc': 'Pi'}
    output.heading('Connector status')
    daemon = result['daemon']
    output.line(f"Daemon: {'running' if daemon['running'] else 'stopped'}" +
                (f" (PID {daemon['pid']})" if daemon.get('pid') else ''))
    if daemon.get('ipc_error'):
        output.line(f"IPC error: {daemon['ipc_error']}")
    output.heading('Servers')
    if result['servers']:
        output.table(['Server', 'Status', 'Error'],
                     [[s['server'], s['state'], s.get('error') or '-'] for s in result['servers']])
    if not result['servers']:
        output.line('No servers configured.')
    output.heading('Agent connections')
    rows = [[c.get('alias') or c.get('identity') or c['agent'], c['agent'],
             names.get(c.get('adapter_id'), c.get('adapter_id') or '-'), c['status']]
            for c in result['connections']]
    connected_agents = {(c.get('identity'), c['agent']) for c in result['connections']}
    rows.extend([[a['identity'], a['agent'], ', '.join(names.get(h, h) for h in a['harnesses']) or '-', a['status']]
                 for a in result['agents'] if (a['identity'], a['agent']) not in connected_agents])
    if rows:
        output.table(['Connection', 'Agent', 'Harness', 'Status'], rows)
    if not rows:
        output.line('No matching agents configured. Run connect to configure an agent.')
    notices = []
    for server in result['servers']:
        if server.get('attention_required'):
            notices.append(f"{server['server']}: {server['consecutive_failures']} consecutive failures; still retrying.")
    for connection in result['connections']:
        if connection.get('error'):
            notices.append(f"{connection['agent']}: {connection['error']}")
    for agent in result['agents']:
        if agent.get('action'):
            notices.append(f"{agent['agent']}: {agent['action']}")
        if agent.get('pending_requests'):
            notices.append(f"{agent['agent']}: {len(agent['pending_requests'])} pending configuration request(s). Use status --verbose for details.")
    if notices:
        output.heading('Needs attention')
        for notice in dict.fromkeys(notices):
            output.line('  - ' + notice)
    output.line('\nDetails: status --verbose | JSON: status --json')
    output.line('Troubleshooting: logs --errors | doctor --probe')


async def run_status(args, output, root):
    store = StateStore(paths.state_file(root))
    state = store.load()
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
    checked = await _query_connections(root, store, state)
    connections = []
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
            last_disconnect_cause=control.get('last_disconnect_cause'),
            consecutive_failures=control.get('consecutive_failures', 0),
            attention_required=control.get('attention_required', False),
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
        for request in pending:
            saved = state.preferences[prefix + request['request_id']]
            if saved.get('single_approval'):
                progress = state.preferences.get(f"onboarding.{identity.server_id}.{request['request_id']}_prepare", {})
                request.update(status=progress.get('status', 'awaiting_approval'),
                               error=progress.get('code'), background=True)
        status = ('REVOKED' if identity.revoked else 'CONNECTED' if connected else
                  'DAEMON_STOPPED' if not current.running else
                  'CONNECTION_ERROR' if control.get('error_code') else
                  'ONBOARDING_ERROR' if any(p['status'] == 'error' for p in pending) else
                  'SETUP_PENDING' if not bindings else 'NOT_ATTACHED')
        action = {'REVOKED': 'Import a valid agent key.',
                  'DAEMON_STOPPED': 'Run: okto-nexus-connector daemon start',
                  'CONNECTION_ERROR': 'Run: okto-nexus-connector logs --errors; check server availability and approvals.',
                  'ONBOARDING_ERROR': 'Review the request error and Nexus approval. Expired or rejected requests need a new connection request.',
                  'SETUP_PENDING': ('Approve the request in Nexus. The daemon completes setup automatically.'
                                    if pending else 'Run connect to complete this agent setup.'),
                  'NOT_ATTACHED': 'Review the Remote connection and runtime authorization in Nexus.'}.get(status, '')
        observed = checked.get(identity.alias, {})
        known = {b.binding_id: b for b in bindings}
        selected = [r for r in getattr(state, 'binding_intents', [])
                    if (r.server_id, r.agent_id) == (identity.server_id, identity.agent_id)]
        proposal_ids = {r.proposal['proposal_id'] for r in selected if r.proposal}
        remote = {r['binding_id']: r for r in observed.get('connections', [])
                  if r.get('binding_id') in known or r.get('proposal_id') in proposal_ids}
        agent_connections = []
        for binding_id in dict.fromkeys([*known, *remote]):
            item = dict(remote.get(binding_id, {}))
            binding = known.get(binding_id)
            failure_state = ('STATUS_UNAVAILABLE' if observed.get('error') in ('NOT_FOUND','VERSION_INCOMPATIBLE') else
                             'AUTHENTICATION_REQUIRED' if observed.get('error') in ('AUTH_FAILED','AGENT_AUTH_REQUIRED','PERMISSION_DENIED') else 'SERVER_UNREACHABLE')
            actual = item.get('status', 'NOT_FOUND' if not observed.get('error') else failure_state)
            if identity.revoked:
                actual = 'REVOKED'
            elif actual not in ('REVOKED','DISABLED','REJECTED','EXPIRED','AWAITING_APPROVAL'):
                if not current.running:
                    actual = 'DAEMON_STOPPED'
                elif control.get('error_code'):
                    actual = 'CONNECTION_ERROR'
                elif binding_id in control.get('execution_errors', {}):
                    actual = 'HARNESS_ERROR'
                elif actual == 'CONNECTED' and (binding_id not in attached or not control.get('control_ready') or not control.get('execution_ready')):
                    actual = 'NOT_ATTACHED'
            item.update(binding_id=binding_id, agent=identity.agent_id, identity=identity.alias,
                server=state.servers[identity.server_id].base_url if identity.server_id in state.servers else None,
                status=actual, connected=actual=='CONNECTED',
                error=control.get('execution_errors', {}).get(binding_id) or observed.get('error'),
                adapter_id=item.get('adapter_id', getattr(binding,'adapter_id',None)),
                executor_id=item.get('executor_id', getattr(binding,'executor_id',None)),
                checked_at=observed.get('checked_at'))
            agent_connections.append(item)
        if agent_connections:
            states = {c['status'] for c in agent_connections}
            status = 'CONNECTED' if states == {'CONNECTED'} else 'PARTIAL' if 'CONNECTED' in states else next(iter(states)) if len(states)==1 else 'ATTENTION_REQUIRED'
            if observed.get('error'):
                action = 'Cannot verify the Server state. Check reach, credentials and logs.'
            elif states <= {'CONNECTED'}:
                action = ''
            elif 'HARNESS_ERROR' in states:
                action = 'Server connected; inspect logs --errors for the harness execution failure.'
        connections.extend(agent_connections)
        agents.append(dict(agent=identity.agent_id, identity=identity.alias,
            server_id=identity.server_id, status=status,
            harnesses=sorted({b.adapter_id for b in bindings}),
            bindings=[dict(id=b.binding_id, state=b.state, attached=b.binding_id in attached)
                      for b in bindings],
            pending_requests=pending,
            action=action))
    result = dict(daemon={'running': current.running, 'pid': current.pid, 'ipc_error': ipc_error},
                  servers=servers, agents=agents, connections=connections,
                  logs=str(root / 'logs' / 'daemon.log'))
    if not output.json_mode and not output.verbose:
        render_status(redact_mapping(result), output)
        return None
    if not output.json_mode:
        output.heading('Connector status - details')
        output.line(f"Daemon: {'running' if current.running else 'stopped'}" +
                    (f' (PID {current.pid})' if current.pid else ''))
        if ipc_error:
            output.line(f'IPC error: {ipc_error}')
        for server in servers:
            output.line('')
            output.line(redact_text(f"Server: {server['server']} | {server['state']}"))
            for field in ('error', 'last_connected_at', 'last_received_at', 'last_error_at',
                          'last_disconnect_cause'):
                if server[field]:
                    output.line(f"  {field.replace('_', ' ')}: {server[field]}")
            if server['attention_required']:
                output.line(f"  Attention: {server['consecutive_failures']} consecutive connection failures; "
                            "still retrying. Inspect logs --errors and the Server.")
        for agent in agents:
            output.line('')
            names = {'codex_app_server': 'Codex', 'claude_stream': 'Claude Code', 'pi_rpc': 'Pi'}
            harnesses = ', '.join(names.get(name, name) for name in agent['harnesses'])
            output.line(f"Agent: {agent['agent']} | {agent['status']}" + (f' | {harnesses}' if harnesses else ''))
            if agent['action']:
                output.line(f"  Next: {agent['action']}")
            for request in agent['pending_requests']:
                output.line(f"  Request: {request['request_id']} ({request['status']})")
                if request.get('error') and request['error'] != 'APPROVAL_REQUIRED':
                    output.line(f"    Error: {request['error']}")
        if not agents:
            output.line('No matching agents configured. Run connect to configure an agent.')
        for connection in connections:
            output.line('')
            output.line(f"Connection: {connection.get('alias') or connection['binding_id']} | {connection['agent']} | {connection['status']}")
            output.line(f"  Server: {connection['server']} | Host: {connection.get('host') or 'unknown'} | Machine: {connection.get('machine_id') or connection.get('executor_id') or 'unknown'}")
            if connection.get('error'):
                output.line(f"  Error: {connection['error']}")
        output.line(f"Logs: {result['logs']}")
        output.line('JSON: status --json | Live logs: logs --follow | Diagnostics: doctor --probe')
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
