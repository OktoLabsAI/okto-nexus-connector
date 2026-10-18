"""Recover durable event obligations before the control-ready transition."""
import asyncio
import time
from collections import deque
from nexus_connector_core import EventCursor
from ..errors import ConnectorError
from .r4_tickets import acquire_ticket
from .r4_events import R4EventPublisher,event_page
from .r4_publications import PublicationAuthority,_binding,current_publication_authority
from .r4_session_release import retire_accepted_event
from ..storage.r4_session_releases import R4SessionReleaseStore


async def recover_event_streams(store,vault,http,host,channel,*,require_current,authorities,clock=time.monotonic):
    publisher=R4EventPublisher(channel,store,host)
    server_id,executor_id=channel.state.server_id,channel.state.executor_id
    journal=await host.ensure_history_journal()
    releases=R4SessionReleaseStore.for_state(store)
    retired_after=0
    for _ in range(32):
        await require_current()
        retired=await asyncio.to_thread(releases.accepted_event_page,server_id,executor_id,after=retired_after)
        if not retired: break
        for retired_after,request in retired:
            await require_current()
            await retire_accepted_event(store,request,journal)
        if len(retired)<128: break
    else:
        if await asyncio.to_thread(releases.accepted_event_page,server_id,executor_id,after=retired_after,limit=1):
            raise ConnectorError('RECONCILIATION_REQUIRED','r4_event_recovery',
                                 'Terminal event checkpoints require another pass.')
    after,high,batches=0,None,0
    pending_scopes=deque()
    try:
        for _ in range(32):
            await require_current()
            rows,high=await asyncio.to_thread(publisher.store.page,server_id,executor_id,after=after,high=high)
            if not rows: break
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
                iterator = journal.events(EventCursor(server_id,executor_id,scope['session_id'],
                    scope['stream_epoch'],state['core_applied']))
                try:
                    pending = await anext(iterator,None)
                finally:
                    await iterator.aclose()
                if pending is None:
                    continue
                pending_scopes.append(scope)
            if after>=high: break
        scan_incomplete=after<high
        # A hot stream gets one finite page per turn. Its next page goes behind
        # every other pending stream, so it cannot monopolize pre-ready replay.
        first_error=None
        while pending_scopes:
            scope=pending_scopes.popleft()
            await require_current()
            binding,identity=await asyncio.to_thread(_binding,store,http,server_id,executor_id,scope)
            authority=current_publication_authority(authorities,binding,identity,clock())
            if authority is None:
                async def ticket_current():
                    await require_current()
                    if await asyncio.to_thread(_binding, store, http, server_id, executor_id, scope) != (binding, identity):
                        raise ConnectorError("STALE_GENERATION", "r4_event_recovery", "The event publication authority changed.")
                ticket, deadline = await acquire_ticket(store, vault, http, binding, identity,
                    require_current=ticket_current, clock=clock)
                authority = PublicationAuthority(binding, identity, ticket, deadline)
                authorities[binding.binding_id] = authority
            async def current():
                await require_current()
                if (clock()>=authority.deadline or (binding,identity)!=(authority.binding,authority.identity) or
                        await asyncio.to_thread(_binding,store,http,server_id,executor_id,scope)!=(binding,identity)):
                    raise ConnectorError("STALE_GENERATION","r4_event_recovery","The event publication authority is no longer current.")
            await current()
            attach_scope=dict(binding_id=binding.binding_id,agent_id=binding.agent_id,
                credential_epoch=authority.ticket.credential_epoch,
                authorization_revision=authority.ticket.authorization_revision,
                configuration_revision=binding.configuration_revision)
            if not channel.is_attached(**attach_scope):
                await channel.attach_binding(**attach_scope,ticket=authority.ticket.ticket)
            if batches>=4096:
                raise ConnectorError("RECONCILIATION_REQUIRED","r4_event_recovery","Event recovery requires another pass.")
            try:
                more=await publisher.step(scope)
            except Exception as error:
                # A failed local commit belongs to this stream. Keep the
                # recovery attempt unsuccessful, but let siblings make durable
                # progress before the next retry.
                await current()
                if first_error is None:
                    first_error=error
                continue
            await current()
            if more:
                batches+=1
                pending_scopes.append(scope)
        if first_error is not None:
            raise first_error
        if scan_incomplete:
            raise ConnectorError("RECONCILIATION_REQUIRED","r4_event_recovery","The stream scan requires another recovery pass.")
    finally:
        await publisher.stop()
