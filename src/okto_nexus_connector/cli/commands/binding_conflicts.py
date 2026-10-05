"""Resolve local alias conflicts before collecting or submitting a new setup."""
from pathlib import Path

from ..wizard_prompts import choice


def resolve_conflict(store, identity, configuration, output, *, request_id, binding_id=None):
    state = store.load()
    alias = configuration['alias']
    intents = [r for r in state.binding_intents if r.alias == alias
               and r.status != 'SUPERSEDED' and r.client_intent_id != request_id + '_prepare']
    legacy = [r for r in state.bindings if r.alias == alias]
    if not intents and not legacy:
        return {'binding_id': binding_id}
    output.line(f'Connection conflict: the name "{alias}" is already in use.')
    for record in intents:
        output.line(f'  Agent: {record.agent_id} | Server: {record.server_id} | '
                    f'Binding: {record.binding_id or "pending"} | Status: {record.status}')
        if record.status != 'APPLIED' and record.client_intent_id.endswith('_prepare'):
            output.line('  Pending request ID: ' + record.client_intent_id[:-8])
    previous = intents[0] if len(intents) == 1 and not legacy else None
    binding = next((b for b in state.execution_bindings if previous and
        b.binding_id == previous.binding_id and b.server_id == identity.server_id
        and b.agent_id == identity.agent_id and b.executor_id == previous.executor_id), None)
    compatible = (previous and previous.status == 'APPLIED' and
                  previous.identity_alias == identity.alias and binding and
                  binding.adapter_id == configuration['adapter_id'])
    if compatible:
        roots = [r for r in state.realizations if r.realization_ref == binding.realization_ref
                 and r.server_id == binding.server_id and r.executor_id == binding.executor_id]
        if len(roots) != 1:
            compatible = False
        else:
            root = roots[0].workspace_root
            requested = configuration.get('workspace_root')
            if requested and Path(requested).resolve() != Path(root).resolve():
                compatible = False
                output.line('Replacement must keep the existing workspace. Choose another connection name to use a different workspace.')
    if not compatible:
        output.line('This name belongs to a pending request, another identity, or a different binding scope. '
                    'Resume the original request or choose another connection name; it will not be overwritten.')
        return {'canceled': True}
    if binding_id and binding_id != binding.binding_id:
        output.line('The requested replacement ID does not match this connection.')
        return {'canceled': True}
    output.line(f'  Harness: {binding.adapter_id} | Workspace: {root}')
    output.line('Replacement requires Nexus approval. The current binding stays unchanged until approval and successful application. Active sessions must be closed first.')
    if choice('Existing connection', [('replace', 'Replace existing binding'),
                                      ('abort', 'Abort setup')], 'abort') != 'replace':
        return {'canceled': True}
    return {'binding_id': binding.binding_id, 'workspace_id': binding.workspace_id,
            'workspace_root': root}
