"""One bounded publisher per Core stream, with retained ACK application."""
import asyncio
from dataclasses import asdict
from nexus_connector_core import CoreError, EventCursor, encode_r4_frame
from nexus_connector_core.protocol import canonical_json
from ..storage.r4_events import R4EventStore, FIELDS


async def event_page(journal, scope, after):
    """Read a finite journal snapshot, never the runtime's continuous follower."""
    iterator = journal.events(EventCursor(scope["server_id"],scope["executor_id"],
                                         scope["session_id"],scope["stream_epoch"],after))
    events=[]
    size=0
    try:
        async for event in iterator:
            value=asdict(event)
            if value["operation_id"] is None: value.pop("operation_id")
            raw=canonical_json(value)
            if len(raw)>64*1024:
                raise CoreError("CAPACITY_EXCEEDED","r4_event")
            if any(value[k]!=scope[k] for k in ("server_id","executor_id","session_id","stream_epoch")):
                raise CoreError("EVENT_SESSION_MISMATCH","r4_event")
            if value["sequence"] != after+len(events)+1:
                raise CoreError("EVENT_GAP","r4_event")
            if size+len(raw)>768*1024: break
            events.append(value)
            size+=len(raw)
            if len(events)==128: break
    finally:
        close = getattr(iterator,'aclose',None)
        if close is not None:
            await close()
    return events


class R4EventPublisher:
    def __init__(self, connection, store, host, *, max_streams=64, poll_seconds=.25, retry_seconds=.5):
        if type(max_streams) is not int or max_streams<1 or min(poll_seconds,retry_seconds)<=0:
            raise ValueError("Invalid event publisher limits.")
        self.connection,self.host=connection,host
        self.store=R4EventStore.for_state(store)
        self.max_streams,self.poll_seconds,self.retry_seconds=max_streams,poll_seconds,retry_seconds
        self.tasks={}
        self.errors={}
        self._stopped=asyncio.Event()

    async def ensure(self, scope):
        if self._stopped.is_set(): return
        scope={k:scope[k] for k in FIELDS}
        key=self.store.key(scope)
        if key not in self.tasks and len(self.tasks)>=self.max_streams:
            raise CoreError("CAPACITY_EXCEEDED","r4_event")
        await asyncio.to_thread(self.store.register,scope)
        if self._stopped.is_set(): return
        task=self.tasks.get(key)
        if task is None or task.done():
            if key not in self.tasks and len(self.tasks)>=self.max_streams:
                raise CoreError('CAPACITY_EXCEEDED','r4_event')
            self.tasks[key]=asyncio.create_task(self._follow(scope),name="r4-event-publisher")

    async def step(self, scope):
        state=await asyncio.to_thread(self.store.read,scope)
        journal=await self.host.ensure_history_journal()
        cursor=EventCursor(scope["server_id"],scope["executor_id"],scope["session_id"],scope["stream_epoch"])
        if state["remote_acked"]>state["core_applied"]:
            await journal.acknowledge_events(cursor,state["remote_acked"])
            await asyncio.to_thread(self.store.advance,scope,core_applied=state["remote_acked"])
            state["core_applied"]=state["remote_acked"]
        if self._stopped.is_set(): return False
        events=await event_page(journal,scope,state["core_applied"])
        if not events or self._stopped.is_set(): return False
        ack=await self.connection.publish_events(**{k:scope[k] for k in ("binding_id","agent_id","session_id","stream_epoch")},events=events)
        # Validate even a custom transport port; never trust an unrelated ACK.
        from nexus_connector_core import decode_r4_frame
        ack=decode_r4_frame(encode_r4_frame(ack))
        if ack["type"]!="event.ack" or any(ack[k]!=scope[k] for k in FIELDS) or any(
                ack[k]!=getattr(self.connection.state,k) for k in ("connection_id","connection_generation")) or ack["sequence"]!=events[-1]["sequence"]:
            raise CoreError("EVENT_SESSION_MISMATCH","r4_event_ack")
        await asyncio.to_thread(self.store.advance,scope,remote_acked=ack["sequence"])
        await journal.acknowledge_events(cursor,ack["sequence"])
        await asyncio.to_thread(self.store.advance,scope,core_applied=ack["sequence"])
        return True

    async def _follow(self, scope):
        key=self.store.key(scope)
        while not self._stopped.is_set() and self.connection.online:
            delay=self.poll_seconds
            try:
                more=await self.step(scope)
                self.errors.pop(key,None)
                if more: continue
            except Exception as error:
                self.errors[key]=getattr(error,"code","JOURNAL_UNAVAILABLE")
                delay=self.retry_seconds
            try:
                await asyncio.wait_for(self._stopped.wait(),delay)
            except TimeoutError:
                pass

    async def stop(self):
        self._stopped.set()
        # A step can be writing remote ACK progress or applying it to Core.
        # Retain that producer when its caller cancels the stop wait.
        await asyncio.shield(asyncio.gather(*self.tasks.values(),return_exceptions=True))
