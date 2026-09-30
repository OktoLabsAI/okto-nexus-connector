"""Recover durable event obligations before the control-ready transition."""
import asyncio
import secrets
import time
from nexus_connector_core import EventCursor
from ..errors import ConnectorError
from .r4_events import R4EventPublisher,event_page
from .r4_publications import PublicationAuthority,_binding


async def recover_event_streams(store,vault,http,host,channel,*,require_current,authorities,clock=time.monotonic):
    publisher=R4EventPublisher(channel,store,host)
    server_id,executor_id=channel.state.server_id,channel.state.executor_id
    journal=await host.ensure_history_journal()
    after,high,batches=0,None,0
    try:
        for _ in range(32):
            await require_current()
            rows,high=await asyncio.to_thread(publisher.store.page,server_id,executor_id,after=after,high=high)
            if not rows: return
            for after,state in rows:
                scope=state["scope"]
                await require_current()
                cursor=EventCursor(server_id,executor_id,scope["session_id"],scope["stream_epoch"])
                # A durable Server acknowledgment does not require a new
                # ticket to finish its already-owned local Core application.
                if state["remote_acked"]>state["core_applied"]:
                    await journal.acknowledge_events(cursor,state["remote_acked"])
                    await asyncio.to_thread(publisher.store.advance,scope,core_applied=state["remote_acked"])
                    state["core_applied"]=state["remote_acked"]
                if not await event_page(journal,scope,state["core_applied"]):
                    continue
                binding,identity=await asyncio.to_thread(_binding,store,http,server_id,executor_id,scope)
                authority=authorities.get(binding.binding_id)
                if authority is not None and clock()>=authority.deadline:
                    authorities.pop(binding.binding_id,None)
                    authority=None
                if authority is None:
                    key=await asyncio.to_thread(vault.resolve,identity.secret_handle)
                    await require_current()
                    if await asyncio.to_thread(_binding,store,http,server_id,executor_id,scope)!=(binding,identity):
                        raise ConnectorError("STALE_GENERATION","r4_event_recovery","The event publication authority changed.")
                    started=clock()
                    ticket=await http.request_r4_binding_ticket(key,binding_id=binding.binding_id,
                        client_intent_id="event_recovery_"+secrets.token_hex(16),
                        credential_request_id="credential_"+secrets.token_hex(16),
                        scopes=("lane:attach","lease:request","receipt:publish"))
                    if (ticket.executor_id!=executor_id or ticket.binding_id!=binding.binding_id or
                            ticket.agent_id!=binding.agent_id or ticket.credential_epoch!=identity.credential_epoch or
                            ticket.authorization_revision!=binding.authorization_revision):
                        raise ConnectorError("STALE_GENERATION","r4_event_recovery","The returned event authority is stale.")
                    authority=PublicationAuthority(binding,identity,ticket,started+ticket.expires_in)
                    authorities[binding.binding_id]=authority
                async def current():
                    await require_current()
                    if (clock()>=authority.deadline or (binding,identity)!=(authority.binding,authority.identity) or
                            await asyncio.to_thread(_binding,store,http,server_id,executor_id,scope)!=(binding,identity)):
                        raise ConnectorError("STALE_GENERATION","r4_event_recovery","The event publication authority is no longer current.")
                await current()
                await channel.attach_binding(binding_id=binding.binding_id,agent_id=binding.agent_id,
                    ticket=authority.ticket.ticket,credential_epoch=authority.ticket.credential_epoch,
                    authorization_revision=authority.ticket.authorization_revision,configuration_revision=binding.configuration_revision)
                while True:
                    await current()
                    if batches>=4096:
                        raise ConnectorError("RECONCILIATION_REQUIRED","r4_event_recovery","Event recovery requires another pass.")
                    more=await publisher.step(scope)
                    await current()
                    if not more: break
                    batches+=1
            if after>=high: return
        raise ConnectorError("RECONCILIATION_REQUIRED","r4_event_recovery","The stream scan requires another recovery pass.")
    finally:
        await publisher.stop()
