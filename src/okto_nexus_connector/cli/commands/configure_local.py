"""Guided local R4 onboarding through the existing public services.

Machine choices remain local. Each acknowledged stage is durable; resuming never
re-imports keys or silently creates another binding after an uncertain response.
"""
import asyncio
from pathlib import Path
import socket
from types import SimpleNamespace
import uuid

from nexus_connector_core import discover_harness_configuration, calculate_inventory_revision
from nexus_connector_core.installation import effective_installation_ref
from nexus_connector_core.provider_discovery import discover_provider_home
from nexus_connector_core.connection_configuration import materialize_connection_configuration, parse_portable_connection_configuration
from ...errors import ConnectorError
from ...services.discovery_configuration import configured_candidates
from ...services.installation_observation import observe_installation
from ...services.executor_registration import ExecutorRegistrationService
from ...services.executor_onboarding import ExecutorOnboarding
from ...services.binding_onboarding import BindingOnboarding
from ...transport.https_client import NexusHTTPClient
from ...daemon import manager
from ..wizard_prompts import text,choice,preferences
from .identity import _vault
from .connection_config import apply_configuration
from .binding_conflicts import resolve_conflict


async def ipc(root,method,body=None):
    def call():
        with manager.connect(root) as client:
            result=client.call(method,body or {})
        if not result.get('ok'):
            raise ConnectorError.from_json(result.get('error',{}))
        return result.get('result',{})
    return await asyncio.to_thread(call)


def local_directory(label,provided,default='',optional=False):
    while True:
        value=provided if provided is not None else text(label,default,required=not optional)
        if not value and optional:return None
        path=Path(value).expanduser()
        if path.is_absolute() and path.is_dir():return str(path.resolve())
        if provided is not None:
            raise ConnectorError('WORKSPACE_UNAVAILABLE','configure',f'{label} must be an existing absolute directory.')
        default=''
        print('Use an existing absolute directory.')


async def configure_local(args,output,root,store,identity,server,portable):
    if args.agent and args.agent!=identity.agent_id:
        raise ConnectorError('AGENT_ID_MISMATCH','configure','Local onboarding uses the agent represented by --identity.')
    args.request_id=args.request_id or 'configure_'+uuid.uuid4().hex
    storage_key=f'configure.{identity.server_id}.{identity.agent_id}.{args.request_id}'
    saved=store.load().preferences.get(storage_key)
    if saved is None:
        portable['alias']=text('Connection name',portable['alias'] or identity.agent_id+'-'+portable['adapter_id'],required=True)
        conflict=resolve_conflict(store,identity,portable | {'workspace_root': args.project},output,
            request_id=args.request_id,binding_id=args.binding_id)
        if conflict.get('canceled'):return {'saved':False,'canceled':True}
        args.binding_id=conflict['binding_id']
        if conflict.get('workspace_id'):
            args.workspace_id=conflict['workspace_id']
            args.project=conflict['workspace_root']
        records=[r for r in store.load().execution_executors if r.server_id==identity.server_id]
        discovery=records[0].discovery_configuration if len(records)==1 else {}
        observations=records[0].installation_observations if len(records)==1 else ()
        candidates=await configured_candidates(discovery,observations=observations)
        matches=[c for c in candidates if c.adapter_id==portable['adapter_id']]
        selected=args.candidate_ref or choice('3. Installation on this machine',[
            (effective_installation_ref(c),f'{c.executable}'+(f' — {c.launch_script}' if c.launch_script else '')+f' ({c.version or "version check required"})') for c in matches])
        if not any(effective_installation_ref(c)==selected for c in matches):
            raise ConnectorError('BINARY_NOT_FOUND','configure','Choose an installation from this machine inventory.')
        project=local_directory('4. Workspace folder',args.project,str(Path.cwd()))
        label=args.workspace_label or text('Workspace name',Path(project).name,required=True)
        home=local_directory('Provider login directory (optional)',args.provider_home,
            discover_provider_home(portable['adapter_id']) or '',optional=True)
        output.line('6. Harness preferences and requested Nexus policies')
        portable=preferences(portable,discover_harness_configuration(portable['adapter_id']))
        configuration=materialize_connection_configuration(portable,execution_location='remote',
            workspace_root=project,workspace_label=label,provider_home=home)
        output.line(f'Review: agent {identity.agent_id} at {server}; installation {selected}; workspace {project}; login {home or "harness default"}.')
        output.line('Approve this connection and its requested execution limits once in Nexus. Setup will continue automatically.')
        if not choice('7. Finish local setup and submit this connection?',[(True,'Finish'),(False,'Cancel')],False):
            return {'saved':False,'canceled':True}
        saved=dict(configuration=configuration,candidate_ref=selected,inventory_revision=calculate_inventory_revision(candidates),
            stage='register',identity=args.identity,host_label=socket.gethostname(),workspace_id=args.workspace_id,
            replace_binding_id=args.binding_id,conflict_checked=True)
        store.update(lambda s:s.preferences.update({storage_key:saved}))
    elif saved['identity']!=args.identity:
        raise ConnectorError('OPERATION_CONFLICT','configure','Resume with the original identity.')
    output.line('Request ID: '+args.request_id)
    if saved['stage']=='done':return saved['result']
    c=saved['configuration']
    vault=_vault(root,store)
    def checkpoint(stage,**values):
        saved.update(stage=stage,**values)
        store.update(lambda s:s.preferences.update({storage_key:saved}))
    if saved['stage']=='register':
        output.line('Registering this execution host…')
        records=[r for r in store.load().execution_executors if r.server_id==identity.server_id]
        existing=records[0] if len(records)==1 else None
        registered=await ExecutorRegistrationService(store,vault).register(identity_alias=args.identity,
            label=existing.label if existing else saved['host_label'],
            client_intent_id=existing.client_intent_id if existing else args.request_id+'_register',
            control_capabilities=existing.control_capabilities if existing else ())
        checkpoint('probe',executor_id=registered.executor_id)
    if saved['stage']=='probe':
        output.line('Checking the selected installation version…')
        await observe_installation(store,server_id=identity.server_id,adapter_id=c['adapter_id'],
            candidate_ref=saved['candidate_ref'],inventory_revision=saved['inventory_revision'])
        record=next(r for r in store.load().execution_executors if r.server_id==identity.server_id)
        candidates=await configured_candidates(record.discovery_configuration,observations=record.installation_observations)
        checkpoint('launch',inventory_revision=calculate_inventory_revision(candidates))
    if saved['stage']=='launch':
        launch=await ExecutorOnboarding(store).configure_launch(identity_alias=args.identity,adapter_id=c['adapter_id'],
            local_consent_id=args.request_id+'_consent',profile_revision=1,provider_home=c['provider_home'])
        checkpoint('realize',configuration_digest=launch.configuration_digest)
    await asyncio.to_thread(manager.start,root)
    await ipc(root,'state.reload')
    if saved['stage']=='realize':
        output.line('Waiting for the daemon to publish this installation…')
        key=vault.resolve(identity.secret_handle)
        async with NexusHTTPClient(server) as http:
            ready=False
            for _ in range(30):
                try:
                    inventory=await http.read_r4_inventory(key,server_id=identity.server_id,executor_id=saved['executor_id'])
                    ready=inventory['freshness']=='FRESH' and inventory['snapshot']['inventory_revision']==saved['inventory_revision']
                except ConnectorError as exc:
                    if exc.code not in ('NOT_FOUND','EXECUTOR_OFFLINE','CONTROL_DISCONNECTED'):raise
                if ready:break
                await asyncio.sleep(1)
            if not ready:
                return dict(saved=False,status='awaiting_inventory',request_id=args.request_id,
                    message='Resume this request once the daemon has connected to Nexus.')
        publication=await ipc(root,'executor.realize',dict(identity_alias=args.identity,client_intent_id=args.request_id+'_realize',
            adapter_id=c['adapter_id'],candidate_ref=saved['candidate_ref'],inventory_revision=saved['inventory_revision'],
            configuration_digest=saved['configuration_digest'],workspace_root=c['workspace_root'],
            workspace_id=saved['workspace_id'],workspace_label=c['workspace_label']))
        checkpoint('prepare',realization_ref=publication['realization_ref'])
    bindings=BindingOnboarding(store,vault)
    if saved['stage']=='prepare':
        # Recover requests saved by older clients that discovered the alias
        # conflict only after the user completed the wizard.
        if not saved.get('conflict_checked'):
            conflict=resolve_conflict(store,identity,c,output,request_id=args.request_id,
                binding_id=saved['replace_binding_id'])
            if conflict.get('canceled'):return {'saved':False,'canceled':True,'request_id':args.request_id}
            checkpoint('prepare',replace_binding_id=conflict['binding_id'],conflict_checked=True)
        requested = parse_portable_connection_configuration(c)
        if requested['runtime_enabled'] is False:
            raise ConnectorError('VALIDATION_ERROR', 'configure', 'Enable runtime access to connect this harness.')
        requested['runtime_enabled'] = True
        proposal=await bindings.prepare(identity_alias=args.identity,realization_ref=saved['realization_ref'],
            alias=c['alias'],client_intent_id=args.request_id+'_prepare',replace_binding_id=saved['replace_binding_id'],
            connection_configuration=requested)
        checkpoint('apply',proposal=proposal['proposal'],single_approval=True)
    if saved['stage']=='apply':
        proposal=saved['proposal']
        proofs=[ref for ref in proposal['required_approvals'] if ref.startswith('apr_')]
        proof=args.operator_proof_ref or (proofs[0] if len(proofs)==1 else None)
        try:
            applied=await bindings.apply(identity_alias=args.identity,prepare_intent_id=args.request_id+'_prepare',
                client_intent_id=args.request_id+'_apply',approved_diff_hash=proposal['approved_diff_hash'],operator_proof_ref=proof)
        except ConnectorError as exc:
            if exc.code != 'APPROVAL_REQUIRED':
                raise
            output.line('Request submitted. Approve it in Nexus; the daemon will complete the connection in the background. You can close this terminal.')
            return dict(saved=False,status='awaiting_operator_approval',request_id=args.request_id,
                required_approvals=proposal['required_approvals'],background=True,
                message='The daemon will complete this connection automatically after approval in Nexus.')
        checkpoint('policies',binding=applied['binding'])
    if saved.get('single_approval'):
        checkpoint('done', result=dict(saved=True, request_id=args.request_id,
            connection_name=c['alias'], binding=saved['binding'], runtime_started=False))
        await ipc(root,'state.reload')
        output.line('Connection approved and configured. The daemon will attach it automatically.')
        return saved['result']
    operator=args.operator_identity
    if not operator:
        aliases=[i for i in store.load().identities if i.server_id==identity.server_id and not i.revoked]
        operator=choice('Apply Nexus policies with an operator identity',[(i.alias,i.alias) for i in aliases]+[('', 'Finish locally; apply policies later in Nexus')],'')
    if not operator:
        return dict(saved=False,local_setup_complete=True,status='awaiting_operator_configuration',request_id=args.request_id,
            binding=saved['binding'],message='Resume with --operator-identity to apply preferences and authorize execution. Runtime has not been started.')
    selected_operator=store.load().identity_by_alias(operator)
    if not selected_operator or selected_operator.server_id!=identity.server_id:
        raise ConnectorError('AGENT_AUTH_REQUIRED','configure','Select an operator identity belonging to the same Server.')
    finish_args=SimpleNamespace(identity=operator,agent=identity.agent_id,request_id=args.request_id+'_finish',
        executor_id=saved['executor_id'],candidate_ref=saved['candidate_ref'],inventory_revision=saved['inventory_revision'],
        workspace_id=saved['binding']['workspace_id'],binding_id=saved['binding']['binding_id'])
    # Nexus needs canonical binding IDs and portable policies, never local paths.
    policy_configuration=materialize_connection_configuration(c,execution_location='remote')
    result=await apply_configuration(finish_args,output,root,policy_configuration)
    checkpoint('done',result={**result,'request_id':args.request_id,'connection_name':c['alias'],'runtime_started':False})
    await ipc(root,'state.reload')
    output.line('Connection configured. Start it with: okto-nexus-connector runtime start '+c['alias'])
    return saved['result']
