"""Explicit local amendment; pending choices never replace acknowledged state."""
from copy import deepcopy
from pathlib import Path

from nexus_connector_core import discover_harness_configuration
from nexus_connector_core.connection_configuration import materialize_connection_configuration
from nexus_connector_core.installation import effective_installation_ref

from ...services import remote_setup as setup
from ...transport.https_client import NexusHTTPClient
from ..wizard_prompts import choice, preferences
from . import configure_startup


async def _current_progress(args, root, store, record):
    """No operator credential is accepted here; only the bootstrap subject."""
    if not record.get('handoff'):
        return None  # Host has not accepted/bootstrap-materialized any child.
    from .identity import _vault
    receipt = record['handoff']['bootstrap']
    identity = store.load().identity_for(receipt['server_id'], receipt['agent_id'])
    if identity is None or identity.revoked:
        setup.refuse('Import the current canonical identity before changing an acknowledged setup.', 'AGENT_AUTH_REQUIRED')
    setup.verify_identity(identity, receipt)
    key = _vault(root, store).resolve(identity.secret_handle)
    async with NexusHTTPClient(record['draft']['server_origin']) as http:
        setup.verify_identity(await http.me(key), receipt, require_remote_setup=True)
        progress = await http.recover_remote_setup_progress(key, setup_run_id=record['draft']['setup_run_id'])
    current = progress['handoff']
    if current.get('bootstrap') != receipt:
        setup.refuse('The original bootstrap changed.', 'OPERATION_CONFLICT')
    if progress['state'] == 'committed' or progress.get('final_binding'):
        setup.refuse('The original setup already has a Server Finish. Resume its handoff to recover the original result.', 'OPERATION_CONFLICT')
    if store.load().identity_for(receipt['server_id'], receipt['agent_id']) != identity:
        setup.refuse('The identity changed during setup review.', 'STALE_GENERATION')
    return progress


async def review_draft(args, output, root, store):
    from . import connect
    from .configure_local import local_directory
    record = setup.load(store, args.request_id)
    if args.identity or args.credential_stdin or args.credential_env or args.mcp_entry:
        setup.refuse('Local revision uses the already imported bootstrap identity, never another credential source.')
    if args.server and setup.normalize_origin(args.server) != record['draft']['server_origin']:
        setup.refuse('A local revision cannot change Server origin.', 'OPERATION_CONFLICT')
    receipt = (record.get('handoff') or {}).get('bootstrap')
    if args.agent and args.agent != (receipt['agent_id'] if receipt else record['draft']['agent_hint']):
        setup.refuse('A local revision cannot change the selected agent.', 'OPERATION_CONFLICT')
    if record['stage'] == 'configured' or record.get('recovered_final_binding'):
        setup.refuse('This setup already has a confirmed Finish.', 'OPERATION_CONFLICT')
    await _current_progress(args, root, store, record)
    pending = record.get('pending_local_review')
    if pending:
        from .connect_setup import _check_choices
        selected = record | {name: pending[name] for name in ('draft', 'draft_revision', 'local')}
        _check_choices(args, selected)
        setup.validate_local(selected)
        setup.export_draft(args.setup_draft, pending['draft'])
        return _result(args, output, pending, changed=True)
    original = record['local']['configuration']
    args.server = record['draft']['server_origin']
    args.harness = args.harness or original['adapter_id']
    rows = [r for r in store.load().execution_executors if receipt and r.server_id == receipt['server_id']]
    if len(rows) > 1:
        setup.refuse('The registered host is ambiguous.', 'PROFILE_DRIFT')
    args.discovery_configuration = rows[0].discovery_configuration if rows else {}
    args.installation_observations = rows[0].installation_observations if rows else ()
    adapter, candidate, _ = await connect._choose_harness(args, output)
    portable = deepcopy(record['draft']['configuration'])
    if adapter != portable['adapter_id']:
        portable['harness_settings'] = {}
    portable.update(adapter_id=adapter, alias=args.binding_alias or original['alias'])
    if args.customize:
        portable = preferences(portable, discover_harness_configuration(adapter))
    project = local_directory('Workspace folder on this machine', str(args.project) if args.project else original['workspace_root'])
    home = local_directory('Provider login directory (optional)',
        args.provider_home if args.provider_home is not None else original['provider_home'] or '', optional=True)
    configuration = materialize_connection_configuration(portable, execution_location='remote', workspace_root=project,
        workspace_label=original['workspace_label'] if project == original['workspace_root'] else Path(project).name or 'Project', provider_home=home)
    startup = deepcopy(record['local']['login_start_plan']) if args.start_at_login is None else configure_startup.choose(args, output, root, choice)
    output.heading('Review changed local choices for setup ' + args.request_id)
    output.line('The canonical Server, agent and workspace remain attached to the original bootstrap.')
    output.line('Installation: ' + candidate.executable + (' + ' + candidate.launch_script if candidate.launch_script else ''))
    for field in ('workspace_root', 'provider_home', 'alias', 'adapter_id', 'harness_settings', 'authorization'):
        output.line(field + ': ' + str(original[field]) + ' -> ' + str(configuration[field]))
    output.line('Startup: ' + str(record['draft']['start_at_login']) + ' -> ' + str(startup is not None))
    output.line('After operator review, only affected host steps resume with new Server request IDs. No project task is sent.')
    if not choice('Save this local revision for the same operator setup?', [(True, 'Save revision'), (False, 'Cancel')], False):
        return dict(saved=False, canceled=True, setup_run_id=args.request_id)
    await _current_progress(args, root, store, record)
    revised = setup.prepare(store, setup_run_id=args.request_id, server=args.server, configuration=configuration,
        candidate_ref=effective_installation_ref(candidate), inventory_revision=args.discovery_inventory_revision, candidate=candidate,
        agent_hint=record['draft']['agent_hint'], workspace_hint=record['draft']['workspace_hint'], login_start_plan=startup,
        review_current=True, expected=record)
    selected = revised.get('pending_local_review') or revised
    setup.export_draft(args.setup_draft, selected['draft'])
    return _result(args, output, selected, changed=bool(revised.get('pending_local_review')))


def _result(args, output, selected, *, changed):
    output.line('Draft saved: ' + str(args.setup_draft))
    output.line('On the operator client, resume the same profile with the revised draft: okto-nexus connect --profile CONNECT_PROFILE --remote-draft DRAFT_FILE --review-current')
    return dict(saved=False, status='awaiting_operator_review' if changed else 'local_review_unchanged',
        setup_run_id=args.request_id, request_id=args.request_id, draft_file=str(args.setup_draft),
        draft_revision=selected['draft_revision'], runtime_started=False)
