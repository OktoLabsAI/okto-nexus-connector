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

    def is_attached(self, *, binding_id, agent_id, credential_epoch,
                    authorization_revision, configuration_revision):
        lane = self.lanes.get(binding_id)
        return bool(self.online and lane is not None and time.monotonic() < lane.deadline_monotonic
            and all(getattr(lane, name) == value for name, value in dict(
                agent_id=agent_id, credential_epoch=credential_epoch,
                authorization_revision=authorization_revision,
                configuration_revision=configuration_revision).items()))

    async def _exchange(self, frame, encode=encode_r4_frame):
        if not self.online:
            raise CoreError('CONTROL_DISCONNECTED','r4_recovery')
        await asyncio.wait_for(self.websocket.send(encode(frame).decode()),self.timeout)
        raw=await asyncio.wait_for(self.websocket.recv(),self.timeout)
        from nexus_connector_core.control_codec import decode_control_frame
        reply=decode_control_frame(raw.encode() if isinstance(raw,str) else raw,
                                  capabilities=getattr(self.state,'control_capabilities',()))
        envelope = reply['request'] if reply['type'] in (
            'startup.refuse.accepted', 'startup.refuse.deferred') else reply
        if any(envelope.get(k)!=self._base()[k] for k in ("server_id","executor_id","connection_id","connection_generation")):
            raise CoreError("STALE_GENERATION","r4_recovery")
        return reply

    async def publish_startup_refusal(self, report):
        from nexus_connector_core.startup_refusal_link import (
            STARTUP_REFUSAL_REVISION, encode_startup_refusal,
        )
        if STARTUP_REFUSAL_REVISION not in self.state.control_capabilities:
            raise CoreError('VERSION_INCOMPATIBLE', 'startup_refusal_recovery')
        if any(report[name] != getattr(self.state, name) for name in (
                'server_id', 'executor_id', 'connection_id', 'connection_generation')):
            raise CoreError('STALE_GENERATION', 'startup_refusal_recovery')
        response = await self._exchange(report, encode_startup_refusal)
        if response['request'] != report or response['type'] not in (
                'startup.refuse.accepted', 'startup.refuse.deferred'):
            raise CoreError('SCOPE_MISMATCH', 'startup_refusal_recovery')
        return response['type'] == 'startup.refuse.accepted'

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

    async def publish_event_artifact(self, scope, event, raw):
        from nexus_connector_core.event_artifacts import EVENT_ARTIFACT_REVISION, artifact_chunks, artifact_ack, encode_event_artifact
        if EVENT_ARTIFACT_REVISION not in getattr(self.state,'control_capabilities',()):
            raise CoreError('VERSION_INCOMPATIBLE','event_artifact')
        base = dict(self._base(),binding_id=scope['binding_id'],agent_id=scope['agent_id'])
        for chunk in artifact_chunks(base,event,raw):
            lane=self.lanes.get(scope['binding_id'])
            if lane is None or lane.agent_id!=scope['agent_id'] or time.monotonic()>=lane.deadline_monotonic:
                raise CoreError('BINDING_NOT_AUTHORIZED','event_artifact')
            reply = await self._exchange(chunk,encode_event_artifact)
            if reply['type']=='event.artifact.rejected':
                normalized={k:v for k,v in reply.items() if k!='code'}
                if dict(normalized,type='event.artifact.ack')!=artifact_ack(chunk):
                    raise CoreError('EVENT_CONFLICT','event_artifact_ack')
                raise CoreError(reply['code'],'event_artifact',possible_effect=True)
            if reply != artifact_ack(chunk):
                raise CoreError('EVENT_CONFLICT','event_artifact_ack')

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
