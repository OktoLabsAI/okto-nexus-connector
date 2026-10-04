"""Apply portable connection configuration through the Server's atomic workflow."""
import asyncio
import time
from urllib.parse import quote

from nexus_connector_core import CoreError
from nexus_connector_core.connection_configuration import parse_portable_connection_configuration, materialize_connection_configuration
from ...errors import ConnectorError
from ...platform import paths
from ...storage.state_store import StateStore
from ...transport.https_client import NexusHTTPClient
from .harness_config import read_configuration_file
from .identity import _vault


async def run_connection_config(args, output, root):
    try:
        portable = parse_portable_connection_configuration(read_configuration_file(args.file))
    except (CoreError, ValueError, TypeError):
        raise ConnectorError('VALIDATION_ERROR','connection_config','Invalid complete connection configuration.') from None
    if args.subcommand == 'validate':
        return {'valid': True, 'configuration': portable}
    configuration = materialize_connection_configuration(portable,
        execution_location=getattr(args,'execution_location','local'),
        workspace_root=str(getattr(args,'project',None) or ''),
        workspace_label=getattr(args,'workspace_label',None) or '',
        provider_home=str(args.provider_home) if getattr(args,'provider_home',None) else None)
    if configuration['runtime_enabled'] is not False and configuration['execution_location'] != 'remote' and not configuration['workspace_root']:
        raise ConnectorError('VALIDATION_ERROR','connection_config','Pass --project and --workspace-label for this destination host.')
    return await apply_configuration(args, output, root, configuration)


async def apply_configuration(args, output, root, configuration):
    store = StateStore(paths.state_file(root))
    state = store.load()
    identity = state.identity_by_alias(args.identity)
    if identity is None or identity.server_id not in state.servers:
        raise ConnectorError('VALIDATION_ERROR','connection_config','Select an imported operator identity.')
    selection = dict(client_intent_id=args.request_id, agent_id=args.agent, executor_id=args.executor_id,
        candidate_ref=args.candidate_ref, inventory_revision=args.inventory_revision,
        workspace_id=args.workspace_id, binding_id=args.binding_id, configuration=configuration)
    storage_key = f'connection-setup.{identity.server_id}.{identity.agent_id}.{args.request_id}'
    saved = state.preferences.get(storage_key)
    if saved and {k:v for k,v in saved['request'].items() if k != 'baseline'} != selection:
        raise ConnectorError('OPERATION_CONFLICT','connection_config','This request ID already has different configuration.')
    key = _vault(root,store).resolve(identity.secret_handle)
    async with NexusHTTPClient(state.servers[identity.server_id].base_url) as http:
        me = await http.me(key)
        if (me.server_id,me.agent_id) != (identity.server_id,identity.agent_id):
            raise ConnectorError('AGENT_ID_MISMATCH','connection_config','The credential identity changed.')
        async def call(method, path, body=None):
            return await http._request(method,path,key=key,json_body=body)
        if not saved:
            current = await call('GET',f'/v1/connections/setup/{quote(args.agent,safe="")}' +
                (f'?binding_id={quote(args.binding_id,safe="")}' if args.binding_id else ''))
            saved = {'request':dict(selection,baseline=current['baseline']), 'stage':'test',
                     'runtime_enabled': configuration['runtime_enabled'] if configuration['runtime_enabled'] is not None else current['runtime_default']}
            store.update(lambda s: s.preferences.update({storage_key:saved}))
        request = saved['request']
        if saved['stage'] == 'done':
            return saved['result']
        if saved['stage'] == 'test' and saved.get('runtime_enabled', configuration['runtime_enabled']) and configuration['execution_location'] != 'remote':
            test = await call('POST','/v1/connections/setup:test',request)
            deadline = time.monotonic()+300
            last = None
            while test['status'] == 'running' and time.monotonic() < deadline:
                if test['stage'] != last:
                    output.line(test['stage']); last=test['stage']
                await asyncio.sleep(1)
                test = await call('GET',f'/v1/connections/setup-tests/{quote(args.request_id,safe="")}')
            if test['status'] != 'succeeded':
                return {'saved':False,'test':test,'request_id':args.request_id}
        saved['stage']='finish'
        store.update(lambda s:s.preferences.update({storage_key:saved}))
        result = await call('POST','/v1/connections/setup:finish',request)
        saved.update(stage='done',result=result)
        store.update(lambda s:s.preferences.update({storage_key:saved}))
        return result
