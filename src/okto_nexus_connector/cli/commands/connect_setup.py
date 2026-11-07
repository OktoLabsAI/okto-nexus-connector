"""Explicit host draft and protected continuation of remote setup."""
import asyncio
from copy import deepcopy
from pathlib import Path
import socket
from types import SimpleNamespace
import uuid

from nexus_connector_core import discover_harness_configuration
from nexus_connector_core.connection_configuration import materialize_connection_configuration
from nexus_connector_core.installation import effective_installation_ref
from nexus_connector_core.provider_discovery import discover_provider_home

from ...errors import ConnectorError
from ...services import remote_setup as setup
from ...transport.https_client import NexusHTTPClient
from ..wizard_prompts import choice, preferences
from . import configure_startup


async def run_setup(args, output, root, store):
    if args.start or args.operator_identity or args.operator_proof_ref:
        setup.refuse('Remote bootstrap accepts no runtime start or operator credential on this host.')
    if getattr(args, 'review_current', False):
        if not args.setup_draft or not args.request_id:
            setup.refuse('--review-current requires --setup-draft and the retained --request-id.')
        from .connect_setup_review import review_draft
        return await review_draft(args, output, root, store)
    if args.setup_draft:
        return await _draft(args, output, root, store)
    return await _resume(args, output, root, store)


def _check_choices(args, record):
    draft, local = record['draft'], record['local']['configuration']
    if (args.server and setup.normalize_origin(args.server) != draft['server_origin'] or
            args.harness and args.harness != draft['installation']['adapter_id'] or
            args.binding_alias and args.binding_alias != local['alias'] or
            args.agent and args.agent not in (draft['agent_hint'], (record.get('handoff') or {}).get('bootstrap', {}).get('agent_id')) or
            args.start_at_login is not None and args.start_at_login != draft['start_at_login']):
        setup.refuse('Explicit choices differ from this retained host review.', 'OPERATION_CONFLICT')
    for name, field in (('project', 'workspace_root'), ('provider_home', 'provider_home')):
        value = getattr(args, name, None)
        if value is not None and str(Path(value).expanduser().resolve()) != local[field]:
            setup.refuse('Explicit local directories differ from this retained host review.', 'OPERATION_CONFLICT')
    if args.executable or args.pi_node or args.customize:
        setup.refuse('Resume the retained installation and configuration without new selection flags.', 'OPERATION_CONFLICT')


async def _draft(args, output, root, store):
    from .connect import _choose_harness, _binding_alias
    from .configure import template
    from .configure_local import local_directory
    if args.credential_stdin or args.credential_env or args.mcp_entry or args.identity:
        setup.refuse('Draft preparation does not consume a credential or identity alias.')
    if args.request_id and setup.PREFIX + args.request_id in store.load().preferences:
        record = setup.load(store, args.request_id)
        _check_choices(args, record)
        setup.validate_local(record)
    else:
        if not args.server:
            setup.refuse('Select --server for the host draft.', 'SERVER_REQUIRED')
        setup.normalize_origin(args.server)
        adapter, candidate, _ = await _choose_harness(args, output)
        portable = template()
        portable.update(adapter_id=adapter, alias=args.binding_alias or _binding_alias(adapter))
        if args.customize:
            portable = preferences(portable, discover_harness_configuration(adapter))
        project = local_directory('Workspace folder on this machine', str((args.project or Path.cwd()).resolve()))
        home = local_directory('Provider login directory (optional)',
            args.provider_home if args.provider_home is not None else discover_provider_home(adapter) or '', optional=True)
        configuration = materialize_connection_configuration(portable, execution_location='remote',
            workspace_root=project, workspace_label=Path(project).name or 'Project', provider_home=home)
        startup = configure_startup.choose(args, output, root, choice)
        output.heading('Review host draft')
        output.line('Server: ' + setup.normalize_origin(args.server))
        output.line('Installation: ' + candidate.executable + (' + ' + candidate.launch_script if candidate.launch_script else ''))
        output.line('Local project: ' + project)
        output.line('Provider login directory: ' + (home or 'Harness default'))
        output.line('The operator will select or create the canonical agent and workspace. Folder names are labels only.')
        output.line('After bootstrap: validate the key, check this installation and submit the binding for operator approval. No project task is sent.')
        if not choice('Save this host draft for operator review?', [(True, 'Save draft'), (False, 'Cancel')], False):
            return dict(saved=False, canceled=True)
        args.request_id = args.request_id or 'remote_' + uuid.uuid4().hex
        record = setup.prepare(store, setup_run_id=args.request_id, server=args.server,
            configuration=configuration, candidate_ref=effective_installation_ref(candidate),
            inventory_revision=args.discovery_inventory_revision, candidate=candidate,
            agent_hint=args.agent, login_start_plan=startup)
    if getattr(args, 'setup_auto_export', False):
        args.setup_draft.parent.mkdir(parents=True, exist_ok=True)
    setup.export_draft(args.setup_draft, record['draft'])
    output.line('Setup request: ' + record['draft']['setup_run_id'])
    output.line('Host draft saved: ' + str(args.setup_draft))
    output.line('On the authenticated operator client: okto-nexus connect --remote-draft DRAFT_FILE --operator-profile OPERATOR_PROFILE --profile CONNECT_PROFILE --plan')
    output.line('Then resume here with connect --setup-receipt HANDOFF_FILE. Enter only the canonical agent key through the protected input.')
    return dict(saved=False, status='awaiting_operator_bootstrap', request_id=record['draft']['setup_run_id'],
                draft_file=str(args.setup_draft), draft_revision=record['draft_revision'], runtime_started=False)


async def _resume(args, output, root, store):
    from . import connect
    from .identity import _vault, _approve_fallback
    from .configure import PROJECT_PRESET
    from .configure_local import configure_local
    handoff = setup.read_document(args.setup_receipt)
    bootstrap = handoff.get('bootstrap', {}) if isinstance(handoff, dict) else {}
    root_id = setup.identifier(bootstrap.get('setup_run_id'), 100)
    if args.request_id and args.request_id != root_id:
        setup.refuse('The handoff belongs to another setup root.', 'OPERATION_CONFLICT')
    record = setup.load(store, root_id)
    pending = record.get('pending_local_review')
    selected = record
    if pending and handoff.get('draft_revision') == pending['draft_revision']:
        selected = record | {name: pending[name] for name in ('draft', 'draft_revision', 'local')}
    handoff = setup.validate_handoff(handoff, selected)
    _check_choices(args, selected | {'handoff': handoff})
    if not pending or selected is not record:
        setup.validate_local(selected)
    args.server = record['draft']['server_origin']
    args.agent = bootstrap['agent_id']
    args.request_id = root_id
    identity = store.load().identity_for(bootstrap['server_id'], bootstrap['agent_id'])
    alias = args.identity or args.alias or record['identity_alias'] or (identity.alias if identity else bootstrap['agent_id'])
    if record['identity_alias'] not in (None, alias) or identity and identity.alias != alias:
        setup.refuse('Resume with the original local identity alias.', 'OPERATION_CONFLICT')
    import os
    if os.environ.get('OKTO_NEXUS_CONNECTOR_VAULT', '').strip().lower() == 'file' or not connect.vault_backend_names():
        _approve_fallback(args, output, store)
    vault = _vault(root, store)
    supplied = connect.connect_reuse.credential_supplied(args)
    if identity and identity.revoked and not supplied:
        setup.refuse('The retained identity is locally revoked. Import its currently approved canonical key explicitly.', 'AGENT_AUTH_REQUIRED')
    key = await asyncio.to_thread(vault.resolve, identity.secret_handle) if identity and not supplied else connect._read_key(args, output)
    async with NexusHTTPClient(args.server) as http:
        setup.verify_identity(await http.me(key), bootstrap, require_remote_setup=True)
        progress = await http.recover_remote_setup_progress(key, setup_run_id=root_id)
        record = setup.accept_handoff(store, root_id, record, handoff, progress['handoff'], progress=progress)
        if identity and not supplied:
            setup.verify_identity(identity, bootstrap)
        else:
            imported = await connect.import_identity(http, vault, store, key=key, alias=alias,
                agent_hint=bootstrap['agent_id'], expected_identity=bootstrap, require_remote_setup=True)
            identity = imported.identity
        connect._remember_server(store, args.server, bootstrap['server_id'])
    if store.load().identity_for(bootstrap['server_id'], bootstrap['agent_id']) != identity:
        setup.refuse('The selected identity changed during authenticated setup recovery.', 'STALE_GENERATION')
    try:
        setup.validate_local(record)
    except ConnectorError as error:
        if error.code != 'PROFILE_DRIFT' or not record.get('recovered_final_binding'):
            raise
        return dict(saved=False, status='remote_finish_recovered', setup_run_id=root_id,
            final_binding=record['recovered_final_binding'], runtime_started=False,
            message='The original Server Finish was recovered. Its host files changed; restore or explicitly reconfigure the existing binding before attaching it.')
    record = setup.checkpoint(store, root_id, record, identity_alias=identity.alias,
        stage='configured' if record['stage'] == 'configured' else 'awaiting_host')
    storage_key = f'configure.{identity.server_id}.{identity.agent_id}.{root_id}'
    remote = dict(setup_run_id=bootstrap['setup_run_id'], review_revision=handoff['review_revision'])
    def stage(state):
        saved = state.preferences.get(storage_key)
        if saved is not None:
            if saved.get('remote_setup') != remote or saved.get('child_requests') != handoff['child_requests']:
                setup.refuse('This configure request belongs to another setup.', 'OPERATION_CONFLICT')
            return
        value = dict(configuration=deepcopy(record['local']['configuration']),
            candidate_ref=record['draft']['installation']['candidate_ref'],
            inventory_revision=record['local']['passive_inventory_revision'],
            stage='register', identity=identity.alias, host_label=socket.gethostname(), workspace_id=bootstrap['workspace_id'],
            replace_binding_id=None, conflict_checked=True, project_preset=dict(PROJECT_PRESET),
            remote_setup=remote, child_requests=deepcopy(handoff['child_requests']))
        value['local_consent_id'] = record['draft']['local_consent']['local_consent_id']
        if record['local']['login_start_plan'] is not None:
            plan = deepcopy(record['local']['login_start_plan'])
            value.update(login_start_plan=plan, login_start_operation=dict(state='pending', plan_revision=plan['plan_revision']))
        state.preferences[storage_key] = value
        setup.validate_capacity(state, record)
    store.update(stage)
    wizard = SimpleNamespace(identity=identity.alias, agent=identity.agent_id, request_id=root_id,
        candidate_ref=None, project=None, provider_home=None, workspace_label=None, workspace_id=bootstrap['workspace_id'],
        binding_id=None, operator_identity=None, operator_proof_ref=None, start_at_login=record['draft']['start_at_login'])
    result = await configure_local(wizard, output, root, store, identity, args.server, deepcopy(record['draft']['configuration']))
    if result.get('saved'):
        current = setup.load(store, root_id)
        setup.checkpoint(store, root_id, current, stage='configured')
    return dict(result, setup_run_id=root_id, runtime_started=False)
