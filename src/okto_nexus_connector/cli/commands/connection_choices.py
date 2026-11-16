"""Passive intent selection before vaults, discovery and replacement effects."""
from dataclasses import dataclass
from pathlib import Path
import hashlib
import uuid

from ...errors import ConnectorError
from ...transport.https_client import origin_of
from ...services import remote_setup
from . import connect_reuse


@dataclass(frozen=True)
class ProjectConnectionIntent:
    kind: str
    retained: tuple | None = None
    setup_run_id: str | None = None
    server: str | None = None
    workspace_hint: str | None = None


def operator_shortcut(root_id):
    return 'connector-' + hashlib.sha256(root_id.encode('utf-8')).hexdigest()[:24]


def _host_label(state, item):
    executor = item[3].get('executor_id') or item[3].get('result', {}).get('binding', {}).get('executor_id')
    matches = [record for record in state.execution_executors
        if record.server_id == item[2].server_id and record.executor_id == executor]
    value = matches[0].label if len(matches) == 1 else executor or 'unconfirmed host'
    return ''.join(char for char in str(value) if char.isprintable())[:160]


def choose_project_intent(args, state, choose):
    candidates = connect_reuse.saved_requests(args, state, include_other_harnesses=True)
    if args.request_id:
        if candidates:
            if len(candidates) != 1:
                raise ConnectorError('OPERATION_CONFLICT', 'connect', 'Select one retained request.')
            return ProjectConnectionIntent('resume', retained=candidates[0])
        if 'remote-setup.' + args.request_id in state.preferences:
            return ProjectConnectionIntent('pending', setup_run_id=args.request_id)
        return ProjectConnectionIntent('initial')
    project = (args.project or Path.cwd()).resolve()
    represented = {item[3].get('remote_setup', {}).get('setup_run_id') for item in candidates}
    roots = []
    for key, record in state.preferences.items():
        if not key.startswith('remote-setup.'):
            continue
        if not isinstance(record, dict):
            raise ConnectorError('PROFILE_DRIFT', 'connect', 'The retained setup checkpoint is unavailable; do not create a substitute root.')
        draft, local = record.get('draft', {}), record.get('local', {})
        if not isinstance(draft, dict) or not isinstance(local, dict):
            raise ConnectorError('PROFILE_DRIFT', 'connect', 'Inspect the invalid retained setup metadata before adding another connection.')
        root_id = draft.get('setup_run_id')
        if root_id in represented:
            continue
        configuration = local.get('configuration', {})
        if not isinstance(configuration, dict):
            raise ConnectorError('PROFILE_DRIFT', 'connect', 'The retained project configuration is unavailable.')
        if (not configuration.get('workspace_root')
                or Path(configuration['workspace_root']).resolve() != project
                or args.server and draft.get('server_origin') != origin_of(args.server)):
            continue
        if (key != remote_setup.PREFIX + remote_setup.identifier(root_id, 100)
                or draft.get('connector_id') != state.connector_id
                or record.get('draft_revision') != remote_setup.document_revision(draft)):
            raise ConnectorError('PROFILE_DRIFT', 'connect', 'The retained setup scope changed; preserve and inspect its original root.')
        remote_setup.validate_child_requests(root_id, record.get('child_requests'))
        remote_setup.normalize_origin(draft.get('server_origin'))
        roots.append((root_id, draft.get('server_origin'), draft.get('installation', {}).get('adapter_id'), record.get('stage')))
    if not candidates and not roots:
        return ProjectConnectionIntent('initial')
    options = [(('resume', item[0]),
        f'Resume {item[3]["configuration"]["adapter_id"]} / {item[2].agent_id} @ {item[2].server_id} / {_host_label(state, item)} ({item[3]["stage"]})')
        for item in candidates if args.harness in (None, item[3]['configuration']['adapter_id'])
        or item[3].get('stage') != 'done']
    options.extend((('pending', item[0]), f'Recover original setup {item[2]} @ {item[1]} ({item[3]})') for item in roots)
    options.extend([(('add', None), 'Add another collaborator (new operator-reviewed identity)'),
                    (('cancel', None), 'Cancel')])
    selected = choose('Resume a connection or add another?', options, non_interactive=False)
    if selected not in [value for value, _ in options]:
        raise ConnectorError('VALIDATION_ERROR', 'connect', 'Select a displayed connection intent.')
    action, identifier = selected
    if action == 'resume':
        return ProjectConnectionIntent('resume', retained=next(item for item in candidates if item[0] == identifier))
    if action == 'pending':
        return ProjectConnectionIntent('pending', setup_run_id=identifier)
    if action == 'cancel':
        return ProjectConnectionIntent('cancel')
    destinations = {state.servers[item[2].server_id].origin for item in candidates}
    destinations.update(item[1] for item in roots)
    if args.server:
        server = origin_of(args.server)
    elif len(destinations) == 1:
        server = next(iter(destinations))
    else:
        server = choose('Which Server should review the additional collaborator?',
            [(origin, origin) for origin in sorted(destinations)], non_interactive=False)
        if server not in destinations:
            raise ConnectorError('VALIDATION_ERROR', 'connect', 'Select a displayed Server.')
    workspaces = {item[3].get('workspace_id') or item[3].get('binding', {}).get('workspace_id')
        or item[3].get('result', {}).get('binding', {}).get('workspace_id') for item in candidates
        if state.servers[item[2].server_id].origin == server}
    workspaces.discard(None)
    hint = next(iter(workspaces)) if len(workspaces) == 1 else None
    if len(workspaces) > 1:
        hint = choose('Which previous project reference should the operator review?',
            [(workspace, workspace) for workspace in sorted(workspaces)], non_interactive=False)
        if hint not in workspaces:
            raise ConnectorError('VALIDATION_ERROR', 'connect', 'Select a displayed project reference.')
    return ProjectConnectionIntent('add', setup_run_id='remote_' + uuid.uuid4().hex,
        server=server, workspace_hint=hint)
