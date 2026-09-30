"""Exclusive pre-reconcile socket owner; only lane attachment and event replay."""
import asyncio
import secrets
import time
from nexus_connector_core import (
    CoreError,R4AttachAttempt,R4_PREVIEW_REVISION,decode_r4_frame,encode_r4_frame,
    reduce_r4_binding_attached,
)


class R4RecoveryChannel:
    def __init__(self, websocket, state, boot_id, *, timeout=15):
        self.websocket,self.state,self.boot_id=websocket,state,boot_id
        self.timeout=timeout
        self.online=True
        self.lanes={}

    def _base(self):
        return dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,
            server_id=self.state.server_id,executor_id=self.state.executor_id,
            connection_id=self.state.connection_id,connection_generation=self.state.connection_generation)

    async def _exchange(self, frame):
        if not self.online:
            raise CoreError('CONTROL_DISCONNECTED','r4_recovery')
        await asyncio.wait_for(self.websocket.send(encode_r4_frame(frame).decode()),self.timeout)
        raw=await asyncio.wait_for(self.websocket.recv(),self.timeout)
        reply=decode_r4_frame(raw.encode() if isinstance(raw,str) else raw)
        if any(reply.get(k)!=self._base()[k] for k in ("server_id","executor_id","connection_id","connection_generation")):
            raise CoreError("STALE_GENERATION","r4_recovery")
        return reply

    async def attach_binding(self, *, binding_id, agent_id, ticket, credential_epoch,
                             authorization_revision, configuration_revision):
        if not self.online:
            raise CoreError('CONTROL_DISCONNECTED','r4_recovery')
        if binding_id not in self.lanes and len(self.lanes)>=256:
            raise CoreError("CAPACITY_EXCEEDED","r4_recovery")
        existing=self.lanes.get(binding_id)
        if existing is not None:
            if time.monotonic()>=existing.deadline_monotonic or any(getattr(existing,k)!=v for k,v in
                    dict(agent_id=agent_id,credential_epoch=credential_epoch,authorization_revision=authorization_revision,
                         configuration_revision=configuration_revision).items()):
                raise CoreError("BINDING_NOT_AUTHORIZED","r4_recovery")
            return
        request_id="recover_attach_"+secrets.token_hex(16)
        attempt=R4AttachAttempt(request_id,self.state.server_id,self.state.executor_id,binding_id,agent_id,
            credential_epoch,authorization_revision,configuration_revision,self.state.connection_id,
            self.state.connection_generation,self.boot_id,time.monotonic())
        base=self._base()
        base["expected_connection_generation"]=base.pop("connection_generation")
        reply=await self._exchange(dict(**base,type="binding.attach",attach_request_id=request_id,binding_id=binding_id,
            agent_id=agent_id,ticket=ticket,credential_epoch=credential_epoch,authorization_revision=authorization_revision,
            configuration_revision=configuration_revision))
        self.lanes[binding_id]=reduce_r4_binding_attached(attempt,reply,received_at_monotonic=time.monotonic())

    async def publish_events(self, *, binding_id, agent_id, session_id, stream_epoch, events):
        lane=self.lanes.get(binding_id)
        if lane is None or lane.agent_id!=agent_id or time.monotonic()>=lane.deadline_monotonic:
            raise CoreError("BINDING_NOT_AUTHORIZED","r4_recovery")
        frame=dict(**self._base(),type="event.batch",binding_id=binding_id,agent_id=agent_id,
                   session_id=session_id,stream_epoch=stream_epoch,events=events)
        if not events or any(any(event[k]!=frame[k] for k in ("server_id","executor_id","session_id","stream_epoch")) for event in events):
            raise CoreError("EVENT_SESSION_MISMATCH","r4_recovery")
        reply=await self._exchange(frame)
        if reply["type"]!="event.ack" or any(reply[k]!=frame[k] for k in
                ("binding_id","agent_id","session_id","stream_epoch")) or reply["sequence"]!=events[-1]["sequence"]:
            raise CoreError("EVENT_GAP","r4_recovery")
        if time.monotonic()>=lane.deadline_monotonic:
            raise CoreError("BINDING_NOT_AUTHORIZED","r4_recovery")
        return reply
