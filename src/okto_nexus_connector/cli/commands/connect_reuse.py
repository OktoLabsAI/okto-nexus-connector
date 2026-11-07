"""Resolve previously reviewed project requests before credential entry or discovery."""
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from urllib.parse import quote

from ...errors import ConnectorError
from ...services.binding_onboarding import BindingOnboarding
from ...services.executor_registration import _identity, _profile

_BINDING_FIELDS = ('binding_id', 'server_id', 'executor_id', 'agent_id', 'endpoint_id', 'workspace_id',
    'workspace_binding_id', 'adapter_id', 'candidate_ref', 'inventory_revision',
    'realization_ref', 'realization_revision', 'binding_revision')


def credential_supplied(args):
    return bool(args.credential_stdin or args.credential_env or args.mcp_entry)


def saved_request(args, state, choose):
    """A recorded canonical association may select an identity; folder names may not."""
    candidates = []
    project = (args.project or Path.cwd()).resolve()
    for key, saved in state.preferences.items():
        if not key.startswith('configure.') or not isinstance(saved, dict):
            continue
        configuration = saved.get('configuration', {})
        if not configuration.get('workspace_root'):
            continue
        identity = state.identity_by_alias(saved.get('identity'))
        if identity is None:
            if Path(configuration['workspace_root']).resolve() == project:
                raise ConnectorError('AGENT_AUTH_REQUIRED', 'connect',
                    'The saved project identity is unavailable. Inspect the original connection before importing another identity.')
            continue
        prefix = f'configure.{identity.server_id}.{identity.agent_id}.'
        if not key.startswith(prefix):
            continue
        request_id = key[len(prefix):]
        if args.request_id and request_id != args.request_id:
            continue
        if not args.request_id and Path(configuration['workspace_root']).resolve() != project:
            continue
        if (getattr(args, 'identity', None) or args.alias) not in (None, identity.alias):
            continue
        if args.agent not in (None, identity.agent_id):
            continue
        profile = _profile(state, identity.server_id)
        if args.server and args.server.rstrip('/') != profile.base_url.rstrip('/'):
            continue
        if args.harness and args.harness != configuration.get('adapter_id') and not args.request_id:
            continue
        if args.binding_alias and args.binding_alias != configuration.get('alias') and not args.request_id:
            continue
        records = [r for r in state.binding_intents if r.identity_alias == identity.alias
                   and r.client_intent_id == saved.get('child_requests', {}).get('prepare', request_id + '_prepare')]
        if any(r.status == 'SUPERSEDED' for r in records):
            if args.request_id:
                raise ConnectorError('OPERATION_CONFLICT', 'connect', 'This setup was superseded. Select the current connection.')
            continue
        candidates.append((key, request_id, identity, saved))
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    selected = choose('Which saved project connection should be resumed?', [
        (item[0], f'{item[2].alias} @ {item[2].server_id}; {item[3]["configuration"]["adapter_id"]}; request {item[1]} ({item[3]["stage"]})')
        for item in candidates], non_interactive=False)
    return next(item for item in candidates if item[0] == selected)


def selected_identity(args, state, saved):
    if getattr(args, 'identity', None) and args.alias and args.identity != args.alias:
        raise ConnectorError('OPERATION_CONFLICT', 'connect', '--identity and --alias select different identities.')
    alias = getattr(args, 'identity', None) or args.alias
    identity = saved[2] if saved else state.identity_by_alias(alias) if alias else None
    if identity is None and args.agent:
        matches = [i for i in state.identities if i.agent_id == args.agent
            and (not args.server or _profile(state, i.server_id).base_url.rstrip('/') == args.server.rstrip('/'))]
        if len(matches) > 1:
            raise ConnectorError('AMBIGUOUS_BINDING', 'connect', 'This agent exists on several Servers. Select --identity or --server.')
        identity = matches[0] if matches else None
    if identity is None:
        if getattr(args, 'identity', None):
            raise ConnectorError('AGENT_AUTH_REQUIRED', 'connect', 'The selected local identity does not exist.')
        return None
    if not credential_supplied(args):
        identity = _identity(state, identity.server_id, identity.agent_id)
    profile = _profile(state, identity.server_id)
    if (args.server and args.server.rstrip('/') != profile.base_url.rstrip('/') or
            args.agent and args.agent != identity.agent_id):
        raise ConnectorError('SCOPE_MISMATCH', 'connect', 'The explicit Server or agent differs from the saved identity.')
    args.server = profile.base_url
    return identity


def validate_choices(args, saved, state):
    from .configure_local import check_saved_choices
    check_saved_choices(args, saved)
    configuration = saved['configuration']
    if args.binding_alias and args.binding_alias != configuration['alias']:
        raise ConnectorError('OPERATION_CONFLICT', 'connect', 'The binding alias differs from this reviewed request.')
    if args.executable or args.pi_node:
        identity = state.identity_by_alias(saved['identity'])
        local = [r for r in state.realizations if r.realization_ref == saved.get('realization_ref')
                 and r.agent_id == identity.agent_id and r.server_id == identity.server_id]
        if len(local) != 1:
            raise ConnectorError('OPERATION_CONFLICT', 'connect',
                'Resume this saved installation without executable overrides; its canonical realization is not available yet.')
        executable = local[0].candidate_launch_script or local[0].candidate_executable
        if (args.executable and Path(args.executable).resolve() != Path(executable).resolve() or
                args.pi_node and Path(args.pi_node).resolve() != Path(local[0].candidate_executable).resolve()):
            raise ConnectorError('OPERATION_CONFLICT', 'connect', 'The installation differs from this reviewed request.')


def identity_snapshot(state, identity):
    return (asdict(_identity(state, identity.server_id, identity.agent_id)),
            asdict(_profile(state, identity.server_id)))


async def verify_binding(http, key, store, identity, saved):
    """Inspect current consent without replaying Finish or adopting changed mappings."""
    state = store.load()
    records = [r for r in state.binding_intents if r.identity_alias == identity.alias
        and r.client_intent_id == saved[3].get('child_requests', {}).get('prepare', saved[1] + '_prepare')]
    if len(records) > 1:
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The saved binding intent is ambiguous.')
    if not records or records[0].status != 'APPLIED':
        if saved[3]['stage'] == 'done':
            raise ConnectorError('BINDING_NOT_AUTHORIZED', 'connect', 'The completed request has no current canonical binding.')
        return None  # Pending requests continue through their original checkpoints.
    record = records[0]
    remote_key = 'remote-setup.' + saved[3]['remote_setup']['setup_run_id'] if saved[3].get('remote_setup') else None
    remote_before = deepcopy(state.preferences.get(remote_key)) if remote_key else None
    service = BindingOnboarding(store, None)
    context = service._context(state, identity.alias, record.realization_ref)
    local = context[2]
    configuration = saved[3]['configuration']
    if (Path(configuration['workspace_root']).resolve() != Path(local.workspace_root).resolve() or
            configuration['adapter_id'] != local.adapter_id or
            saved[3].get('workspace_id') not in (None, local.canonical_workspace_id)):
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The saved project differs from its canonical host mapping.')
    bindings = [b for b in state.execution_bindings if b.binding_id == record.binding_id
                and b.server_id == identity.server_id and b.agent_id == identity.agent_id]
    if len(bindings) != 1 or bindings[0].state != 'APPROVED':
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'connect', 'The saved binding is missing, disabled or ambiguous.')
    binding = bindings[0]
    current = await http._request('GET', '/v1/connections/bindings/' + quote(binding.binding_id, safe=''),
                                  key=key, require_revision=True)
    if not isinstance(current, dict):
        raise ConnectorError('VERSION_INCOMPATIBLE', 'connect', 'The Server returned an invalid binding view.')
    if current.get('state') != 'APPROVED':
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'connect',
            'The Server no longer approves this binding. Inspect its current state before reconnecting.')
    revisions = ('realization_revision', 'binding_revision', 'authorization_revision', 'configuration_revision')
    if any(type(current.get(name)) is not int or current[name] < 1 for name in revisions):
        raise ConnectorError('VERSION_INCOMPATIBLE', 'connect', 'The Server returned invalid binding revisions.')
    if any(current.get(name) != getattr(binding, name) for name in _BINDING_FIELDS):
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The Server binding differs from the approved local mapping.')
    after = store.load()
    if (after.preferences.get(saved[0]) != saved[3] or
            remote_key and (after.preferences.get(remote_key) != remote_before or
                [r for r in after.binding_intents if r.identity_alias == identity.alias
                 and r.client_intent_id == record.client_intent_id] != records) or
            service._context(after, identity.alias, record.realization_ref)[3] != context[3] or
            [b for b in after.execution_bindings if b.server_id == binding.server_id and b.binding_id == binding.binding_id] != [binding]):
        raise ConnectorError('STALE_GENERATION', 'connect', 'The saved connection changed while its authorization was checked.')
    return {name: current[name] for name in (*_BINDING_FIELDS, 'state', 'authorization_revision', 'configuration_revision')}


def completed_remote_reuse(args, store, identity, retained, current_binding, *, expected=None):
    """Return a completed result only after authenticated exact binding reuse.

    The first call follows /me and verify_binding. Recheck its ephemeral snapshot
    after closing HTTP, before daemon effects; it never updates setup history.
    """
    if not retained or not retained[3].get('remote_setup') or retained[3]['stage'] != 'done':
        return None
    from ...services.remote_setup import normalize_origin, require_completed_configuration
    saved = retained[3]
    root_id = saved['remote_setup']['setup_run_id']
    state = store.load()
    records = [r for r in state.binding_intents if r.identity_alias == identity.alias
        and r.client_intent_id == saved.get('child_requests', {}).get('prepare')]
    bindings = [b for b in state.execution_bindings if current_binding and b.binding_id == current_binding['binding_id']
        and b.server_id == identity.server_id and b.agent_id == identity.agent_id]
    snapshot = dict(identity=identity_snapshot(state, identity), configuration=deepcopy(state.preferences.get(retained[0])),
        root=deepcopy(state.preferences.get('remote-setup.' + root_id)), intents=deepcopy(records), bindings=deepcopy(bindings))
    if (snapshot['identity'][0] != asdict(identity) or snapshot['configuration'] != saved
            or expected is not None and any(snapshot[name] != expected[name] for name in snapshot)):
        raise ConnectorError('STALE_GENERATION', 'connect', 'The completed setup changed during authenticated reuse.')
    if len(records) != 1 or records[0].status != 'APPLIED' or len(bindings) != 1 or bindings[0].state != 'APPROVED':
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'connect', 'The completed setup has no exact current approved binding.')
    record = records[0]
    root = require_completed_configuration(store, saved, identity)
    if root != snapshot['root']:
        raise ConnectorError('STALE_GENERATION', 'connect', 'The retained setup changed while its local facts were checked.')
    if normalize_origin(args.server) != root['draft']['server_origin'] or normalize_origin(snapshot['identity'][1]['base_url']) != root['draft']['server_origin']:
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The current Server origin differs from the completed bootstrap.')
    service = BindingOnboarding(store, None)
    context = service._context(state, identity.alias, record.realization_ref)
    snapshot['context'] = context[3]
    if expected is not None and snapshot['context'] != expected['context']:
        raise ConnectorError('STALE_GENERATION', 'connect', 'The current host mapping changed during authenticated reuse.')
    service._proposal(record)  # Verify retained proposal shape/digest without reapplying its old authority.
    result = saved['result']
    historical = record.applied_binding
    if (not isinstance(historical, dict) or historical.get('state') != 'APPROVED'
            or saved.get('binding') != historical or result.get('binding') != historical
            or result.get('request_id') != root_id or result.get('connection_name') != record.alias
            or result.get('runtime_started') is not False or saved.get('proposal') != record.proposal
            or record.remote_setup != saved['remote_setup'] or record.apply_client_intent_id != saved['child_requests']['apply']
            or (record.server_id, record.agent_id, record.executor_id, record.realization_ref, record.alias, record.binding_id) !=
               (identity.server_id, identity.agent_id, saved.get('executor_id'), saved.get('realization_ref'),
                saved['configuration']['alias'], historical.get('binding_id'))
            or context[2].local_consent_id != root['draft']['local_consent']['local_consent_id']
            or any(current_binding.get(name) != historical.get(name) for name in _BINDING_FIELDS)):
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The completed result differs from its retained proposal or exact binding mapping.')
    plan = saved.get('login_start_plan')
    operation = saved.get('login_start_operation', {})
    if (plan != root['local']['login_start_plan'] or plan and
            (operation.get('state') != 'done' or operation.get('plan_revision') != plan.get('plan_revision'))):
        raise ConnectorError('LOGIN_START_CHECKPOINT_INVALID', 'connect', 'Completed reuse cannot resume an unfinished startup operation.')
    choice = getattr(args, 'start_at_login', None)
    if choice is not None and choice != bool(plan):
        raise ConnectorError('OPERATION_CONFLICT', 'connect', 'Resume the retained startup choice; use service controls for changes.')
    return deepcopy(result), snapshot
