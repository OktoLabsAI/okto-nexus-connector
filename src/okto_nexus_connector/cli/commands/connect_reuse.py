"""Resolve previously reviewed project requests before credential entry or discovery."""
from dataclasses import asdict
from pathlib import Path
from urllib.parse import quote

from ...errors import ConnectorError
from ...services.binding_onboarding import BindingOnboarding
from ...services.executor_registration import _identity, _profile


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
    fields = ('binding_id', 'server_id', 'executor_id', 'agent_id', 'endpoint_id', 'workspace_id',
              'workspace_binding_id', 'adapter_id', 'candidate_ref', 'inventory_revision',
              'realization_ref', 'realization_revision', 'binding_revision')
    if not isinstance(current, dict):
        raise ConnectorError('VERSION_INCOMPATIBLE', 'connect', 'The Server returned an invalid binding view.')
    if current.get('state') != 'APPROVED':
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'connect',
            'The Server no longer approves this binding. Inspect its current state before reconnecting.')
    revisions = ('realization_revision', 'binding_revision', 'authorization_revision', 'configuration_revision')
    if any(type(current.get(name)) is not int or current[name] < 1 for name in revisions):
        raise ConnectorError('VERSION_INCOMPATIBLE', 'connect', 'The Server returned invalid binding revisions.')
    if any(current.get(name) != getattr(binding, name) for name in fields):
        raise ConnectorError('PROFILE_DRIFT', 'connect', 'The Server binding differs from the approved local mapping.')
    after = store.load()
    if (after.preferences.get(saved[0]) != saved[3] or
            service._context(after, identity.alias, record.realization_ref)[3] != context[3] or
            [b for b in after.execution_bindings if b.server_id == binding.server_id and b.binding_id == binding.binding_id] != [binding]):
        raise ConnectorError('STALE_GENERATION', 'connect', 'The saved connection changed while its authorization was checked.')
    return {name: current[name] for name in (*fields, 'state', 'authorization_revision', 'configuration_revision')}
