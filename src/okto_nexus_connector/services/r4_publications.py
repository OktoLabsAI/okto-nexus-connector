"""Replay only persisted receipts under fresh, scoped Server authority."""
import asyncio
from dataclasses import dataclass
import secrets
import time

from ..errors import ConnectorError
from ..storage.r4_publications import R4PublicationStore
from .executor_registration import _identity, _profile


@dataclass(frozen=True)
class PublicationAuthority:
    binding: object
    identity: object
    ticket: object
    deadline: float


def _binding(store, http, server_id, executor_id, frame):
    state = store.load()
    profile = _profile(state, server_id)
    if profile.origin != http.origin or profile.base_url.rstrip('/') != http.base_url:
        raise ConnectorError('PROFILE_DRIFT', 'r4_publication', 'The approved Server profile changed.')
    bindings = [r for r in state.execution_bindings if
                (r.server_id, r.executor_id, r.binding_id, r.agent_id, r.state) ==
                (server_id, executor_id, frame['binding_id'], frame['agent_id'], 'APPROVED')]
    if len(bindings) != 1:
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'r4_publication',
                             'The receipt has no current approved binding.')
    return bindings[0], _identity(state, server_id, frame['agent_id'])


async def recover_publications(store, vault, http, *, server_id, executor_id,
                               require_current, clock=time.monotonic):
    """Drain bounded ready pages before control negotiation, without Core effects.

    Unprojected reservations remain pending. A live previous ticket can refuse
    fresh issuance; preserve every obligation and let the daemon retry. Returned
    authority can be reused for lane attachment on this same startup attempt.
    """
    publications = R4PublicationStore.for_state(store)
    authorities = {}
    for _ in range(32):
        await require_current()
        page = await asyncio.to_thread(publications.ready, server_id, executor_id)
        if not page:
            return authorities
        for frame in page:
            await require_current()
            binding, identity = await asyncio.to_thread(_binding, store, http, server_id, executor_id, frame)
            authority = authorities.get(binding.binding_id)
            if authority is None:
                key = await asyncio.to_thread(vault.resolve, identity.secret_handle)
                await require_current()
                if await asyncio.to_thread(_binding, store, http, server_id, executor_id, frame) != (binding, identity):
                    raise ConnectorError('STALE_GENERATION', 'r4_publication', 'The receipt authority changed.')
                started = clock()
                ticket = await http.request_r4_binding_ticket(key, binding_id=binding.binding_id,
                    client_intent_id='recovery_' + secrets.token_hex(16),
                    credential_request_id='credential_' + secrets.token_hex(16),
                    scopes=('lane:attach', 'lease:request', 'receipt:publish'))
                if (ticket.executor_id != executor_id or ticket.binding_id != binding.binding_id or
                        ticket.agent_id != binding.agent_id or ticket.credential_epoch != identity.credential_epoch or
                        ticket.authorization_revision != binding.authorization_revision):
                    raise ConnectorError('STALE_GENERATION', 'r4_publication', 'The returned receipt authority is stale.')
                authority = PublicationAuthority(binding, identity, ticket, started + ticket.expires_in)
                authorities[binding.binding_id] = authority
            await require_current()
            if (clock() >= authority.deadline or
                    (binding, identity) != (authority.binding, authority.identity) or
                    await asyncio.to_thread(_binding, store, http, server_id, executor_id, frame) != (binding, identity)):
                raise ConnectorError('STALE_GENERATION', 'r4_publication', 'The receipt authority is no longer current.')
            # Preserve the original connection/generation. These identify the
            # authorized dispatch, not this recovery observer's new socket.
            await http.publish_operation_receipt(authority.ticket.ticket, frame=frame)
            await asyncio.to_thread(publications.acknowledge, frame)
    if await asyncio.to_thread(publications.ready, server_id, executor_id, limit=1):
        raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_publication',
                             'Receipt publication requires another recovery pass.')
    return authorities
