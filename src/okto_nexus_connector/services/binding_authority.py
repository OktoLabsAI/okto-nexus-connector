"""Refresh Server policy revisions without adopting a different local binding."""
from dataclasses import asdict, fields
from urllib.parse import quote

from ..errors import ConnectorError
from ..transport.https_client import R4BindingView


def adopt_policy_revisions(store, binding, current):
    expected = {field.name: getattr(binding, field.name) for field in fields(R4BindingView)}
    revisions = ('authorization_revision', 'configuration_revision')
    if (not isinstance(current, dict) or set(current) != set(expected)
            or any(current[key] != value for key, value in expected.items() if key not in revisions)
            or current['state'] != 'APPROVED'
            or any(type(current[key]) is not int or current[key] < expected[key] for key in revisions)):
        raise ConnectorError('PROFILE_DRIFT', 'binding_authority',
                             'The Server binding changed and requires connection review.')
    if all(current[key] == expected[key] for key in revisions):
        return
    def update(state):
        found = [item for item in state.execution_bindings if
                 (item.server_id,item.executor_id,item.binding_id) ==
                 (binding.server_id,binding.executor_id,binding.binding_id)]
        if len(found) != 1 or asdict(found[0]) != asdict(binding):
            raise ConnectorError('STALE_GENERATION', 'binding_authority', 'The local binding changed.')
        for key in revisions:
            setattr(found[0],key,current[key])
    store.update(update)


async def refresh_idle_binding_authority(store, vault, http, host, *, server_id, executor_id):
    state = store.load()
    for binding in state.execution_bindings:
        if (binding.server_id,binding.executor_id,binding.state) != (server_id,executor_id,'APPROVED'):
            continue
        # Never transfer authority over a running or uncertain native process.
        if not await host.binding_sessions_closed(server_id=server_id,executor_id=executor_id,binding_id=binding.binding_id):
            continue
        identity = state.identity_for(server_id,binding.agent_id)
        if identity is None or identity.revoked:
            continue
        current = await http._request('GET','/v1/connections/bindings/'+quote(binding.binding_id,safe=''),
                                      key=vault.resolve(identity.secret_handle))
        adopt_policy_revisions(store,binding,current)
