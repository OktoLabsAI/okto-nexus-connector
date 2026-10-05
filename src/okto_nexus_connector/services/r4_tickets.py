"""Acquire derivative material from a durable, shared ticket intent."""
import asyncio
from dataclasses import asdict
import hashlib
import time

from nexus_connector_core.protocol import canonical_json
from ..errors import ConnectorError, TicketMaterialUnavailable
from ..storage.r4_tickets import R4TicketStore

SCOPES = ("lane:attach", "lease:request", "receipt:publish")


async def acquire_ticket(store, vault, http, binding, identity, *, require_current,
                         clock=time.monotonic, wall_clock=time.time):
    context = dict(server_id=binding.server_id, executor_id=binding.executor_id,
                   binding_id=binding.binding_id, agent_id=binding.agent_id,
                   authority_digest=hashlib.sha256(canonical_json(dict(
                       binding=asdict(binding), identity=asdict(identity),
                       origin=http.origin, base_url=http.base_url))).hexdigest(),
                   scopes=list(SCOPES), expires_in=600)
    intents = R4TicketStore.for_state(store)
    await require_current()
    request = await asyncio.to_thread(intents.reserve, context, wall_clock())
    await require_current()
    key = await asyncio.to_thread(vault.resolve, identity.secret_handle)
    for attempt in range(2):
        await require_current()
        await asyncio.to_thread(intents.require_current, request)
        await require_current()
        started = clock()
        try:
            ticket = await http.request_r4_binding_ticket(
                key, binding_id=binding.binding_id,
                client_intent_id=request["client_intent_id"],
                credential_request_id=request["credential_request_id"],
                replaces_ticket_id=request["replaces_ticket_id"],
                scopes=SCOPES, expires_in=context["expires_in"])
        except TicketMaterialUnavailable as error:
            await require_current()
            request = await asyncio.to_thread(intents.replace, request, error.ticket_id, wall_clock())
            if attempt == 1:
                raise ConnectorError("RECONCILIATION_REQUIRED", "r4_tickets",
                                     "Ticket recovery requires another pass.") from None
            continue
        await require_current()
        await asyncio.to_thread(intents.require_current, request)
        deadline = started + ticket.expires_in
        if (ticket.binding_id != binding.binding_id or ticket.executor_id != binding.executor_id
                or ticket.agent_id != binding.agent_id or ticket.credential_epoch != identity.credential_epoch
                or ticket.authorization_revision != binding.authorization_revision
                or tuple(ticket.scopes) != SCOPES or ticket.expires_in != context["expires_in"]
                or clock() >= deadline):
            raise ConnectorError("STALE_GENERATION", "r4_tickets",
                                 "The returned ticket authority is stale.")
        await require_current()
        return ticket, deadline
