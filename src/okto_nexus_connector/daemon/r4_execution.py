"""Connection-scoped lane attachment and approved execution composition."""
import asyncio
from dataclasses import dataclass
import time

from ..errors import ConnectorError
from ..services.executor_registration import _identity, _profile
from ..services.r4_execution import R4ExecutionOwner
from ..services.r4_tickets import acquire_ticket
from ..services.session_capabilities import SessionCapabilityOwner, ApprovedToolLaunchProvider


@dataclass(frozen=True)
class _Lane:
    binding: object
    identity: object
    ticket: object
    deadline: float


class R4DaemonExecution:
    def __init__(self, control, http, *, native_factory=None, clock=time.monotonic, recovered_lanes=None):
        self.control, self.http, self.clock = control, http, clock
        self.store, self.vault, self.host = control.store, control.vault, control.host
        self.connection = control.connection
        self.native_factory = native_factory
        self.capabilities = SessionCapabilityOwner(self.store, self.vault, clock=clock)
        self.lanes = {}
        self.recovered_lanes = dict(recovered_lanes or {})
        self.owner = None
        self.closing = False
        self._close_task = None

    @property
    def ready(self):
        return bool(not self.closing and self.lanes and self.owner is not None
                    and self.owner.failure is None and self.connection.online)

    def _bindings(self):
        state = self.store.load()
        records = [r for r in state.execution_bindings if
                   (r.server_id, r.executor_id) == (self.control.server_id, self.control.executor_id)
                   and r.state == "APPROVED"]
        if records:
            profile = _profile(state, self.control.server_id)
            if profile.origin != self.http.origin or profile.base_url.rstrip("/") != self.http.base_url:
                raise ConnectorError("PROFILE_DRIFT", "r4_lanes",
                                     "The approved Server profile changed.")
        if len(records) > 256 or len({r.binding_id for r in records}) != len(records):
            raise ConnectorError("CAPACITY_EXCEEDED", "r4_lanes",
                                 "The approved lane selection is ambiguous or exceeds capacity.")
        return {r.binding_id: (r, _identity(state, r.server_id, r.agent_id)) for r in records}

    def _guard(self, frame):
        if self.closing or not self.connection.online:
            raise ConnectorError("CONTROL_DISCONNECTED", "r4_lanes", "The execution connection is closed.")
        if (frame["server_id"], frame["executor_id"], frame["connection_id"], frame["connection_generation"]) != (
                self.control.server_id, self.control.executor_id,
                self.connection.state.connection_id, self.connection.state.connection_generation):
            raise ConnectorError("SCOPE_MISMATCH", "r4_lanes", "The operation belongs to another connection.")
        lane = self.lanes.get(frame["binding_id"])
        if (lane is None or self.clock() >= lane.deadline or frame["agent_id"] != lane.binding.agent_id):
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "r4_lanes", "The approved lane is no longer current.")
        return lane

    async def _current(self, frame):
        lane = self._guard(frame)
        if (await asyncio.to_thread(self._bindings)).get(frame["binding_id"]) != (lane.binding, lane.identity):
            raise ConnectorError("STALE_GENERATION", "r4_lanes", "The approved lane changed.")
        return self._guard(frame)

    async def _candidates(self, frame):
        await self._current(frame)
        candidates = tuple(await self.control.discover())
        await self._current(frame)
        return candidates

    async def _launch(self, frame):
        lane = await self._current(frame)
        key = await asyncio.to_thread(self.vault.resolve, lane.identity.secret_handle)
        await self._current(frame)
        result = await ApprovedToolLaunchProvider(self.capabilities, self.http, key, self.host,
            self.store, candidate_provider=self._candidates, require_current=self._guard)(frame)
        await self._current(frame)
        return result

    async def _publish(self, frame):
        lane = await self._current(frame)
        await self.http.publish_operation_receipt(lane.ticket.ticket, frame=frame)

    async def sync(self):
        if self.closing:
            raise ConnectorError("RUNTIME_DRAINING", "r4_lanes", "The execution owner is draining.")
        if self.owner is not None and self.owner.failure is not None:
            raise self.owner.failure
        records = await asyncio.to_thread(self._bindings)
        for key, lane in self.lanes.items():
            if records.get(key) != (lane.binding, lane.identity) or self.clock() >= lane.deadline:
                raise ConnectorError("RECONCILIATION_REQUIRED", "r4_lanes",
                                     "The execution lane requires authority reconciliation.")
        for binding_id, (binding, identity) in records.items():
            if binding_id in self.lanes:
                continue
            recovered = self.recovered_lanes.pop(binding_id, None)
            if recovered is not None:
                if (recovered.binding, recovered.identity) != (binding, identity):
                    raise ConnectorError("STALE_GENERATION", "r4_lanes", "The recovered authority changed.")
                ticket = recovered.ticket
                deadline = recovered.deadline
            else:
                async def current():
                    if self.closing or not self.connection.online:
                        raise ConnectorError("CONTROL_DISCONNECTED", "r4_lanes", "The execution connection is closed.")
                    if (await asyncio.to_thread(self._bindings)).get(binding_id) != (binding, identity):
                        raise ConnectorError("STALE_GENERATION", "r4_lanes", "The approved identity changed.")
                ticket, deadline = await acquire_ticket(self.store, self.vault, self.http,
                    binding, identity, require_current=current, clock=self.clock)
            if (ticket.executor_id != binding.executor_id or ticket.agent_id != binding.agent_id
                    or ticket.credential_epoch != identity.credential_epoch
                    or ticket.authorization_revision != binding.authorization_revision
                    or self.clock() >= deadline
                    or (await asyncio.to_thread(self._bindings)).get(binding_id) != (binding, identity)):
                raise ConnectorError("STALE_GENERATION", "r4_lanes", "The returned lane authority is stale.")
            lane_scope = dict(binding_id=binding_id,agent_id=binding.agent_id,credential_epoch=ticket.credential_epoch,
                authorization_revision=ticket.authorization_revision,configuration_revision=binding.configuration_revision)
            if not self.connection.is_attached(**lane_scope):
                await self.connection.attach_binding(ticket=ticket.ticket,**lane_scope)
            if (await asyncio.to_thread(self._bindings)).get(binding_id) != (binding, identity):
                raise ConnectorError("STALE_GENERATION", "r4_lanes", "The binding changed during attachment.")
            self.lanes[binding_id] = _Lane(binding, identity, ticket, deadline)
        if self.lanes and self.owner is None:
            self.owner = R4ExecutionOwner(self.connection, self.store, self.host,
                candidate_provider=self._candidates, launch_provider=self._launch,
                publish_receipt=self._publish, native_factory=self.native_factory,
                require_current=self._current, clock=self.clock)
            self.owner.start()

    async def close(self, *, preserve_leases=False, stop_event=None):
        if self._close_task is None:
            self.closing = True
            self._close_task = asyncio.create_task(
                self._close(preserve_leases=preserve_leases, stop_event=stop_event),
                name="r4-execution-cleanup")
        await asyncio.shield(self._close_task)

    async def _close(self, *, preserve_leases=False, stop_event=None):
        self.closing = True
        await self.connection.close()
        if self.owner is not None:
            await self.owner.stop()
        await self.capabilities.close()
        if preserve_leases:
            self.control.cleanup_pending = True
            stop_event = stop_event if stop_event is not None else asyncio.Event()
            while True:
                try:
                    await self.host.wait_executor_leases(
                        server_id=self.control.server_id, executor_id=self.control.executor_id,
                        stop_event=stop_event)
                    break
                except Exception:
                    # Missing observation is not permission to shorten the
                    # already-granted lease. Keep the same host and retry.
                    if stop_event.is_set():
                        break
                    await asyncio.sleep(0.1)
        # Keep the HTTP client and Core stores owned while late native tool
        # producers settle. The caller retains this lifecycle task.
        while True:
            outcomes = await self.host.shutdown_executor(
                server_id=self.control.server_id, executor_id=self.control.executor_id)
            if all(outcome in ("graceful", "already_closed", "forced") for _, outcome in outcomes):
                break
            self.control.cleanup_pending = True
            await asyncio.sleep(0.1)
        self.control.cleanup_pending = False
