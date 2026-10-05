"""Resume explicitly submitted binding intents after the CLI has exited."""
import asyncio
import logging

from ..errors import ConnectorError
from .binding_onboarding import BindingOnboarding


async def continue_pending_bindings(store, vault, *, server_id, executor_id,
                                   http_factory=None):
    state = await asyncio.to_thread(store.load)
    service = BindingOnboarding(store, vault, **({'http_factory': http_factory} if http_factory else {}))
    for record in state.binding_intents:
        if ((record.server_id, record.executor_id) != (server_id, executor_id)
                or record.status != 'APPLY_PENDING' or not record.connection_configuration
                or not record.apply_client_intent_id):
            continue
        diagnostic_key = f'onboarding.{server_id}.{record.client_intent_id}'
        if state.preferences.get(diagnostic_key, {}).get('terminal'):
            continue
        try:
            applied = await service.apply(identity_alias=record.identity_alias,
                prepare_intent_id=record.client_intent_id,
                client_intent_id=record.apply_client_intent_id,
                approved_diff_hash=record.approved_diff_hash,
                operator_proof_ref=record.operator_proof_ref)
        except ConnectorError as error:
            pending = error.code == 'APPROVAL_REQUIRED'
            transient = error.retry_safe or error.code in {
                'SERVER_UNREACHABLE', 'EXECUTOR_OFFLINE', 'CONTROL_DISCONNECTED',
                'NETWORK_ERROR', 'TRANSPORT_ERROR', 'HTTP_ERROR', 'TIMEOUT', 'OUTCOME_UNKNOWN'}
            value = dict(status='awaiting_approval' if pending else 'error',
                         code=error.code, terminal=not (pending or transient))
            if value != state.preferences.get(diagnostic_key):
                await asyncio.to_thread(store.update, lambda current: current.preferences.update({diagnostic_key: value}))
                log = logging.getLogger(__name__).info if pending else logging.getLogger(__name__).warning
                log('Connection onboarding: alias=%s status=%s code=%s',
                    record.alias, value['status'], error.code)
            continue
        def complete(current):
            current.preferences[diagnostic_key] = dict(status='approved', terminal=False)
            for key, saved in current.preferences.items():
                if (key.startswith(f'configure.{server_id}.{record.agent_id}.')
                        and isinstance(saved, dict) and saved.get('single_approval')
                        and saved.get('proposal', {}).get('proposal_id') == record.proposal['proposal_id']):
                    saved.update(stage='done', binding=applied['binding'], result=dict(
                        saved=True, connection_name=record.alias, binding=applied['binding'],
                        runtime_started=False, request_id=record.client_intent_id.removesuffix('_prepare')))
        await asyncio.to_thread(store.update, complete)
        logging.getLogger(__name__).info('Connection approved and configured: alias=%s', record.alias)
