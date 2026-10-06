"""Interactive connection wizard. JSON is optional and contains no host identity."""
from types import SimpleNamespace
from urllib.parse import quote, urlencode
import uuid

from nexus_connector_core import discover_harness_configuration, CoreError, DEFAULT_RUNTIME_AUTOMATION
from nexus_connector_core.connection_configuration import (parse_portable_connection_configuration,
    materialize_connection_configuration)
from ...errors import ConnectorError
from ...platform import paths
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from ...services.discovery_service import catalog_runtimes
from ..wizard_prompts import text, choice, preferences
from .identity import run_identity, _vault
from .harness_config import read_configuration_file
from .connection_config import apply_configuration


def template():
    return dict(format='okto-nexus-connection',version=2,adapter_id='',runtime_enabled=True,
        session_policy=None,alias='',harness_settings={},
        automatic_reply=DEFAULT_RUNTIME_AUTOMATION.automatic_messages,tool_access='ask',
        authorization=dict(minutes=60,actions=20,no_expiry=False,unlimited_actions=False))


async def select_identity(args, output, root):
    store=StateStore(paths.state_file(root))
    state=store.load()
    alias=args.identity
    if not alias:
        alias=choice('1. Nexus identity',[(i.alias,f'{i.alias} — {i.agent_id}') for i in state.identities if not i.revoked]+[('', 'Add an identity')])
    if not state.identity_by_alias(alias):
        server=args.server or text('Nexus Server URL',required=True)
        alias=alias or text('Local identity alias',required=True)
        output.line('Enter the canonical agent key at the hidden prompt. It stays in the local vault.')
        await run_identity(SimpleNamespace(subcommand='add',server=server,agent=args.agent,
            alias=alias,credential_stdin=args.credential_stdin,credential_env=args.credential_env,
            non_interactive=False),output,root)
        state=store.load()
    identity=state.identity_by_alias(alias)
    if not identity or identity.server_id not in state.servers:
        raise ConnectorError('AGENT_AUTH_REQUIRED','configure','The selected identity has no Server profile.')
    args.identity=alias
    return store,identity,state.servers[identity.server_id].base_url


async def run_configure(args, output, root):
    try:
        return await _run_configure(args,output,root)
    except (CoreError,ValueError) as exc:
        raise ConnectorError('VALIDATION_ERROR','configure','Invalid connection setting or template. Review the selected values.') from None


async def _run_configure(args, output, root):
    if args.non_interactive or args.json:
        raise ConnectorError('VALIDATION_ERROR','configure','configure is an interactive terminal wizard.',
            action='Use connection-config apply with --file and explicit destination flags for automation.')
    portable=parse_portable_connection_configuration(read_configuration_file(args.file)) if args.file else template()
    output.heading('Nexus connection setup')
    output.line('Ctrl+C cancels. Enter - to clear an optional default.')
    output.line('Imported files never select an identity, installation or folder.\n')
    store,identity,server=await select_identity(args,output,root)
    host=args.host or 'connector'
    output.heading('2. Execution host: '+('this Connector machine' if host=='connector' else 'Nexus Server machine'))
    if host=='connector' and args.request_id and store.load().preferences.get(f'configure.{identity.server_id}.{identity.agent_id}.{args.request_id}'):
        from .configure_local import configure_local
        return await configure_local(args,output,root,store,identity,server,portable)
    harness=args.harness or choice('Harness',[(r.adapter_id,r.display_name) for r in catalog_runtimes()],portable['adapter_id'])
    if harness != portable['adapter_id']:portable['harness_settings']={}
    portable['adapter_id']=harness
    if host=='connector':
        from .configure_local import configure_local
        return await configure_local(args,output,root,store,identity,server,portable)
    output.line('Server-hosted configuration requires an operator identity. Paths below refer to the Nexus Server machine.')
    args.agent=args.agent or text('Target agent ID',identity.agent_id,required=True)
    key=_vault(root,store).resolve(identity.secret_handle)
    async with NexusHTTPClient(server) as http:
        async def call(method,path,body=None):
            return await http._request(method,path,key=key,json_body=body)
        # Check operator authority before collecting configuration that cannot be applied.
        await call('GET',f'/v1/connections/setup/{quote(args.agent,safe="")}')
        rows=[]
        cursor=None
        while True:
            hosts=await call('GET',f'/v1/agents/{quote(args.agent,safe="")}/executors?'+urlencode({'limit':100,**({'after_executor_id':cursor} if cursor else {})}))
            rows.extend(hosts['items'])
            if not hosts['has_more']:break
            next_cursor=hosts.get('next_executor_id')
            if not next_cursor or next_cursor==cursor or len(rows)>=1000:
                raise ConnectorError('OPERATION_CONFLICT','configure','Executor listing changed. Restart configuration.')
            cursor=next_cursor
        embedded=[h for h in rows if h['kind']=='embedded']
        args.executor_id=args.executor_id or choice('Server installation host',[(h['executor_id'],h.get('label') or h['executor_id']) for h in embedded])
        query=urlencode({'executor_id':args.executor_id,**({'workspace_id':args.workspace_id} if args.workspace_id else {})})
        options_path=f'/v1/agents/{quote(args.agent,safe="")}/runtime-options?'+query
        options=await call('GET',options_path)
        rows=[row for row in options['options'] if row['adapter_id']==harness and row['candidate_ref']]
        args.candidate_ref=args.candidate_ref or choice('3. Installation',[(r['candidate_ref'],r['label']) for r in rows])
        selected=next((r for r in rows if r['candidate_ref']==args.candidate_ref),None)
        if not selected:raise ConnectorError('BINARY_NOT_FOUND','configure','The selected installation is not in the current inventory.')
        args.inventory_revision=options['inventory_revision']
        if selected['technical_state']!='READY_FOR_RUNTIME':
            if not choice('Run the selected installation version check?',[(True,'Yes'),(False,'Cancel')],True):return {'saved':False,'canceled':True}
            await call('POST',f'/v1/runtime/executors/{quote(args.executor_id,safe="")}/installations:check',
                dict(agent_id=args.agent,adapter_id=harness,candidate_ref=args.candidate_ref,
                     inventory_revision=args.inventory_revision,approved=True))
            options=await call('GET',options_path)
            args.inventory_revision=options['inventory_revision']
            selected=next((r for r in options['options'] if r['candidate_ref']==args.candidate_ref and r['adapter_id']==harness),None)
        if not selected or selected['technical_state']!='READY_FOR_RUNTIME':
            raise ConnectorError('BINARY_NOT_FOUND','configure','The installation is not ready. Review its version and host inventory in Nexus.')
    args.project=args.project or text('4. Workspace folder on the Server',required=True)
    args.workspace_label=args.workspace_label or text('Workspace name',required=True)
    args.provider_home=args.provider_home or text('Login directory on the Server (optional)',selected.get('provider_home_suggestion') or '') or None
    portable['alias']=text('5. Connection name',portable['alias'] or args.agent+'-'+harness,required=True)
    if selected.get('binding') and not args.binding_id:
        if choice('Existing connection for this workspace',[(True,'Update existing connection'),(False,'Cancel')],True):args.binding_id=selected['binding']['binding_id']
        else:return {'saved':False,'canceled':True}
    output.heading('6. Preferences and authorization')
    portable=preferences(portable,selected.get('harness_configuration') or discover_harness_configuration(harness))
    configuration=materialize_connection_configuration(portable,workspace_root=args.project,
        workspace_label=args.workspace_label,provider_home=args.provider_home)
    output.line(f'Review: {args.agent} on Nexus Server; {harness}; workspace {args.project}; login {args.provider_home or "harness default"}.')
    if not choice('7. Authorize the connection test and save after it succeeds?',[(True,'Test and Finish'),(False,'Cancel')],False):
        return {'saved':False,'canceled':True}
    args.request_id=args.request_id or 'configure_'+uuid.uuid4().hex
    output.line('Request ID: '+args.request_id+' — reuse it after an uncertain response.')
    result=await apply_configuration(args,output,root,configuration)
    output.line('Connection verified and saved.' if result.get('saved') else 'Connection was not saved. Review the test details.')
    return result
