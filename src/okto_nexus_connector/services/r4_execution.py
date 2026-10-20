"""Owned consumers of canonical R4 operations for the Connector daemon.

The Server admits operations. This owner translates their immutable envelopes
through public Core APIs; it never resolves or admits a second intent.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import secrets
import time

from nexus_connector_core import (
    CoreError, LaunchIntent, OpenOperation, TurnOperation, ControlOperation, validate_harness_settings,
    project_r4_open_receipt, project_r4_turn_receipt, project_r4_steer_receipt,
    project_r4_interrupt_receipt, project_r4_close_receipt,
    project_r4_decision_receipt, r4_close_operation, r4_native_decision_operation,
    prepare_r4_receipt_binding, SessionKey, OperationKey, OperationNotAdmitted,
    OperationCapacityDeferred, CapacityRetryWaiter, CAPACITY_CONTRACT_REVISION,
    project_r4_bound_receipt,
    prepare_r4_lease_refusal, persist_r4_lease_refusal,
    prepare_r4_stopped_refusal, persist_r4_operation_refusal,
    prepare_r4_startup_refusal, persist_r4_startup_refusal,
)
from nexus_connector_core.native.adapter_types import NativeAdapterError
from nexus_connector_core.startup_refusal_link import STARTUP_REFUSAL_REVISION

from ..errors import ConnectorError
from ..transport.lease_denial import LeaseRequestDenied
from ..storage.r4_publications import R4PublicationStore
from .core_host import ExecutionRuntimeKey


_SCOPE = ('server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
          'workspace_binding_id', 'session_id', 'session_owner_generation',
          'binding_revision', 'credential_epoch', 'authorization_revision',
          'configuration_revision')


@dataclass(frozen=True, slots=True)
class R4LaunchSetup:
    """Trusted host configuration; no executable, argv or wire environment."""

    environment: object
    auth_refs: tuple[str, ...] = ()
    native_action_factory: object | None = None
    opening_selection: object | None = None


@dataclass(frozen=True, slots=True)
class _StartupRefusalOutcome:
    receipt: dict
    report: dict


class _RenewalGate:
    """Drain productive calls for renewal without serializing those calls."""
    def __init__(self):
        self.changed = asyncio.Condition()
        self.active = 0
        self.renewing = False

    @asynccontextmanager
    async def operation(self):
        async with self.changed:
            await self.changed.wait_for(lambda: not self.renewing)
            self.active += 1
        try:
            yield
        finally:
            async with self.changed:
                self.active -= 1
                self.changed.notify_all()

    @asynccontextmanager
    async def renewal(self):
        async with self.changed:
            await self.changed.wait_for(lambda: not self.renewing)
            self.renewing = True
            try:
                await self.changed.wait_for(lambda: self.active == 0)
            except BaseException:
                self.renewing = False
                self.changed.notify_all()
                raise
        try:
            yield
        finally:
            async with self.changed:
                self.renewing = False
                self.changed.notify_all()


@dataclass(slots=True)
class _Session:
    runtime: object
    opening_hash: str
    stream_epoch: str
    scope: dict
    connection_id: str
    connection_generation: int
    gate: _RenewalGate = field(default_factory=_RenewalGate)
    stopped: asyncio.Event = field(default_factory=asyncio.Event)
    deadline: float = 0
    renewal: asyncio.Task | None = None
    containment_error: Exception | None = None
    contained: bool = False
    release_accepted: bool = False
    denial: dict | None = None
    local_lease_failure: str | None = None
    opened: bool = False


class R4ExecutionOwner:
    """Two bounded consumers, with producers retained through cancellation.

    Providers are host composition ports: a complete fresh local inventory,
    approved per-session environment, and receipt publication. A publication
    failure fences this connection; it cannot cause native operation replay.
    Reconciliation from durable Core facts is required before reconnect.
    """

    def __init__(self, connection, store, host, *, candidate_provider,
                 launch_provider=None, publish_receipt, native_factory=None,
                 response_resolver=None, max_sessions=64, native_tools=None,
                 require_current=None, clock=time.monotonic, capacity_provider=None):
        if type(max_sessions) is not int or max_sessions <= 0:
            raise ValueError('Invalid execution session capacity.')
        self.connection, self.store, self.host = connection, store, host
        self.publications = R4PublicationStore.for_state(store)
        self.candidate_provider, self.launch_provider = candidate_provider, launch_provider
        self.publish_receipt = publish_receipt
        self.native_factory, self.response_resolver = native_factory, response_resolver
        if native_tools is not None:
            from .session_capabilities import R4NativeToolServices
            if not isinstance(native_tools, R4NativeToolServices) or launch_provider is not None:
                raise ValueError('Native tools require the default approved launch provider.')
        self.native_tools = native_tools
        self.require_authority, self.clock = require_current, clock
        self.capacity_provider = capacity_provider
        self.capacity_waiter = CapacityRetryWaiter(self._capacity_revision)
        self._capacity_bindings = {}
        self._prepared_openings = {}
        self._renewal_slots = asyncio.Semaphore(8)
        # The transport retains bounded item/byte reservations until _produce
        # releases them. Separate worker budgets keep slow opens from occupying
        # the workers for ready sessions or containment controls.
        self._operation_slots = {
            'opening': asyncio.Semaphore(max_sessions),
            'regular': asyncio.Semaphore(4),
            'control': asyncio.Semaphore(2),
        }
        from .r4_events import R4EventPublisher
        self.events = R4EventPublisher(connection, store, host, max_streams=max_sessions)
        self.max_sessions = min(max_sessions, host.max_owned_slots)
        self._sessions = {}
        self._opening_sessions = set()
        self._consumers = []
        self._producers = set()
        self._stopping = False
        self._receipt_stop = asyncio.Event()
        self._receipt_monitor = None
        self.failure = None
        self.execution_errors = {}
        from .r4_session_release import R4SessionReleaseReporter, R4SessionReleasePublisher
        from ..storage.r4_session_releases import R4SessionReleaseStore
        release_reporter = R4SessionReleaseReporter(store, host)
        self.session_releases = R4SessionReleasePublisher(
            R4SessionReleaseStore.for_state(store), release_reporter,
            self._publish_session_release, on_accepted=release_reporter.retire_accepted)

    async def _publish_session_release(self, request, *, stop_event):
        return await self.connection.publish_session_release(request, stop_event=stop_event)

    @property
    def pending_count(self):
        return len(self._producers)

    def _record_execution_outcome(self, receipt):
        binding_id = receipt['binding_id']
        if receipt['stage'] == 'FAILED':
            import logging
            code = receipt.get('error_code') or 'NATIVE_EXECUTION_FAILED'
            self.execution_errors[binding_id] = code
            logging.getLogger(__name__).warning(
                'Harness operation failed: binding=%s action=%s code=%s; control connection retained',
                binding_id, receipt.get('action'), code)
        elif receipt['stage'] in ('SUBMITTED', 'SUCCEEDED'):
            self.execution_errors.pop(binding_id, None)

    def start(self):
        if self._consumers or self._stopping:
            raise RuntimeError('The execution owner has already started.')
        self._consumers = [asyncio.create_task(self._consume(control),
            name='r4-execution-control' if control else 'r4-execution-productive')
            for control in (False, True)]

    async def stop(self):
        self._stopping = True
        self.capacity_waiter.stop()
        self._receipt_stop.set()
        for session in self._sessions.values():
            session.stopped.set()
        # Only consumers are observers. A producer may already be in the
        # native protocol or committing a receipt and must remain owned.
        for task in self._consumers:
            task.cancel()
        await asyncio.shield(asyncio.gather(*self._consumers, return_exceptions=True))
        if self._producers:
            await asyncio.shield(asyncio.gather(*tuple(self._producers), return_exceptions=True))
        renewals = [s.renewal for s in self._sessions.values() if s.renewal is not None]
        if renewals:
            await asyncio.shield(asyncio.gather(*renewals, return_exceptions=True))
        if self._receipt_monitor is not None:
            # Retain a publication already in progress through shutdown.
            await asyncio.shield(self._receipt_monitor)
        await self.events.stop()
        await self.capacity_waiter.aclose()
        for task in tuple(self._producers):
            if task.done():
                self._observe(task)

    async def _observe_receipts(self):
        from .r4_publications import refresh_receipt_facts
        after = ""
        try:
            journal = await self.host.ensure_journal()
            while not self._receipt_stop.is_set():
                state = self.connection.state
                after, frames = await refresh_receipt_facts(self.publications, journal,
                    server_id=state.server_id, executor_id=state.executor_id, after=after,
                    connection=(state.connection_id, state.connection_generation))
                for frame in frames:
                    self._record_execution_outcome(frame)
                    await self.publish_receipt(frame)
                    await asyncio.to_thread(self.publications.acknowledge, frame)
                try:
                    await asyncio.wait_for(self._receipt_stop.wait(), .25)
                except TimeoutError:
                    pass
        except Exception as error:
            if self.failure is None:
                self.failure = error
            await self.connection.close()

    def _observe(self, task):
        self._producers.discard(task)
        if not task.cancelled():
            task.exception()

    async def _consume(self, control):
        try:
            while not self._stopping:
                item = await self.connection.receive_operation(control=control)
                producer = asyncio.create_task(self._produce_independently(item), name='r4-operation-producer')
                self._producers.add(producer)
                producer.add_done_callback(self._observe)
                # Do not wait for another session's native acknowledgement.
                # The retained transport reservation bounds queued producers.
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self.failure is None:
                self.failure = error

    async def _produce_independently(self, item):
        await self._produce(item)

    async def _capacity_revision(self):
        ledger = await self.host.ensure_ledger()
        return await ledger.capacity_revision()

    async def _capacity_flow(self, item, state, sequence):
        # Older links keep their original bounded transport accounting. The
        # new flow-control hint is sent only under explicit negotiation.
        if CAPACITY_CONTRACT_REVISION in self.connection.state.control_capabilities:
            from nexus_connector_core.capacity_codec import capacity_dispatch_state
            frame = capacity_dispatch_state(self._capacity_bindings[item.token],
                                             state=state, sequence=sequence)
            await self.connection.publish_capacity_dispatch_state(item, frame)

    async def _execute_with_capacity_wait(self, item):
        action = item.frame['action']
        lane = 'opening' if action == 'runtime.open' else 'regular' if action == 'turn.submit' else 'control'
        deferred, sequence = None, 0
        while True:
            version = self.capacity_waiter.version
            async with self._operation_slots[lane]:
                if deferred is not None:
                    frame = item.frame
                    key = ExecutionRuntimeKey(frame['server_id'], frame['executor_id'],
                                              frame['binding_id'], frame['session_id'])
                    session = self._sessions.get(key)
                    if self._stopping or session is None or session.stopped.is_set():
                        return project_r4_bound_receipt(self._capacity_bindings[item.token],
                            deferred.stopped_receipt(), key=deferred.key, receipt_revision=1)
                    sequence += 1
                    await self._capacity_flow(item, 'RUNNING', sequence)
                elif self._stopping:
                    return None
                try:
                    return await self._execute(item)
                except OperationCapacityDeferred as error:
                    error.require_binding(self._capacity_bindings[item.token])
                    deferred = error
                    sequence += 1
                    await self._capacity_flow(item, 'WAITING', sequence)
                except CoreError as error:
                    # Close can finish while a deferred retry yields in flow
                    # publication, binding persistence or Core admission. Its
                    # pre-effect refusal belongs to this waiting operation;
                    # it must not disconnect unrelated sessions on the host.
                    if (deferred is None or error.possible_effect
                            or (error.stage, error.code) not in {
                                ('admission', 'SESSION_UNKNOWN'),
                                ('admission', 'SESSION_CLOSED'),
                                ('admission', 'SESSION_CLOSING'),
                                ('admission', 'RUNTIME_DRAINING'),
                                ('r4_execution', 'SESSION_UNKNOWN'),
                                ('r4_execution', 'AGENT_REVOKED')}):
                        raise
                    if not (self._stopping or session is None or session.stopped.is_set()):
                        raise
                    binding = self._capacity_bindings[item.token]
                    deferred.require_binding(binding)
                    journal = await self.host.ensure_history_journal()
                    if await journal.get_receipt(deferred.key) is not None:
                        raise
                    return project_r4_bound_receipt(binding, deferred.stopped_receipt(),
                        key=deferred.key, receipt_revision=1)
            # The same producer retains transport ownership and prepared
            # identity. Neither a worker slot nor a session/renewal gate is
            # held across this shared change observation.
            await self.capacity_waiter.wait(version)

    def _context(self, item, runtime):
        return runtime.r4_operation_context(self.connection.require_current(item),
            connection_id=self.connection.state.connection_id,
            connection_generation=self.connection.state.connection_generation)

    async def _bind(self, item, runtime, context, **options):
        binding = prepare_r4_receipt_binding(item.frame, context, **options)
        await asyncio.to_thread(self.publications.bind, binding,
                               **({'stream_epoch': options['stream_epoch']} if 'stream_epoch' in options else {}))
        self._capacity_bindings[item.token] = binding
        if 'stream_epoch' in options:
            await asyncio.to_thread(self.events.store.register,{**item.frame,'stream_epoch':options['stream_epoch']})
        if self.capacity_provider is not None and binding['action'] in ('runtime.open', 'turn.submit'):
            capacity = await self.capacity_provider()
            await capacity.bind_admitted_capacity_operation(binding)
        # Persistence can yield while the lane, link or lease is superseded.
        return self._context(item, runtime)

    async def _produce(self, item):
        try:
            await asyncio.to_thread(self.publications.reserve, self.connection.require_current(item))
            outcome = await self._execute_with_recorded_outcome(item)
            receipt = outcome.receipt if isinstance(outcome, _StartupRefusalOutcome) else outcome
            if receipt is None:
                return
            self._record_execution_outcome(receipt)
            # Once Core has produced a fact, loss of lane/link does not undo
            # it. Publish that same fact; never fabricate a second operation.
            if isinstance(outcome, _StartupRefusalOutcome):
                await asyncio.to_thread(self.publications.record_startup, outcome.report)
                await self._publish_startup_refusal(outcome.report)
            else:
                await asyncio.to_thread(self.publications.record, receipt)
                await self.publish_receipt(receipt)
            await asyncio.to_thread(self.publications.acknowledge, receipt)
            if self._receipt_monitor is None and not self._stopping:
                self._receipt_monitor = asyncio.create_task(self._observe_receipts(), name="r4-receipt-observer")
            frame = item.frame
            key = ExecutionRuntimeKey(frame['server_id'],frame['executor_id'],frame['binding_id'],frame['session_id'])
            session = self._sessions.get(key)
            if frame['action'] == 'runtime.open' and receipt['stage'] == 'FAILED':
                self._sessions.pop(key, None)
                if session is not None:
                    session.stopped.set()
                session = None
            if session is not None and not self._stopping:
                await self.events.ensure({**frame,'stream_epoch':session.stream_epoch})
                if (frame['action'] == 'runtime.open' and self.require_authority is not None
                        and session.renewal is None and receipt['stage'] in ('SUBMITTED', 'SUCCEEDED')):
                    session.renewal = asyncio.create_task(self._renew_session(session, dict(frame)),
                                                         name='r4-session-lease-renewal')
        except Exception as error:
            if self.failure is None:
                self.failure = error
            await self.connection.close()
            raise
        finally:
            self._capacity_bindings.pop(item.token, None)
            self._prepared_openings.pop(item.token, None)
            self.connection.release_operation(item)

    async def _execute_with_recorded_outcome(self, item):
        try:
            return await self._execute_with_capacity_wait(item)
        except Exception:
            # Core may have durably classified a native exception before
            # raising it. Publish that exact uncertain fact instead of taking
            # unrelated bindings offline. No operation is invoked again and
            # no physical slot is released by this observation.
            binding = self._capacity_bindings.get(item.token)
            if binding is None:
                raise
            source = item.frame
            key = OperationKey(source['server_id'], source['executor_id'], source['operation_id'])
            journal = await self.host.ensure_history_journal()
            fact = await journal.get_receipt(key)
            if (fact is None or fact.stage != 'OUTCOME_UNKNOWN'
                    or not fact.possible_effect or fact.retry_safe):
                raise
            # The public Core projector verifies namespace, operation, intent
            # and the previously persisted binding; a mismatched fact fails.
            return project_r4_bound_receipt(binding, fact, key=key, receipt_revision=1)

    async def _publish_startup_refusal(self, report):
        while True:
            reply = await self.connection.publish_startup_refusal(report,
                stop_event=self._receipt_stop)
            if reply['type'] == 'startup.refuse.accepted':
                return
            if self._stopping or not self.connection.online:
                raise ConnectorError('PUBLICATION_INTERRUPTED', 'r4_startup_refusal',
                    'The durable startup refusal remains pending.')
            await asyncio.sleep(.5)

    async def _renew_session(self, session, source):
        async def current():
            if self._stopping or session.stopped.is_set() or not self.connection.online:
                raise ConnectorError("CONTROL_DISCONNECTED", "r4_renewal", "The session renewal owner stopped.")
            await self.require_authority(source)
        try:
            while not self._stopping and not session.stopped.is_set():
                remaining = session.deadline - self.clock()
                if remaining <= 0:
                    raise CoreError("LEASE_EXPIRED", "r4_renewal")
                try:
                    await asyncio.wait_for(session.stopped.wait(), timeout=remaining * 0.5)
                    return
                except TimeoutError:
                    pass
                async with self._renewal_slots:
                    async with self._session_renewal(session):
                        await current()
                        if self.clock() >= session.deadline:
                            raise CoreError("LEASE_EXPIRED", "r4_renewal")
                        snapshot = await session.runtime.inspect(SessionKey(
                            source["server_id"], source["executor_id"], source["session_id"]))
                        if snapshot.ownership == "released" or snapshot.lease_state == "CLOSED":
                            return
                        await current()
                        try:
                            applied = await self.connection.apply_lease(session.runtime,
                                scope={k:source[k] for k in _SCOPE}, grant_id=source["grant_id"], purpose="renew",
                                require_current=current, fence_on_error=False)
                        except LeaseRequestDenied as error:
                            if self._scoped_denial(error, source, 'renew'):
                                # Latch the fence before releasing the renewal
                                # gate. Already-queued work cannot overtake it.
                                session.denial = error.request
                                session.stopped.set()
                            raise
                        await current()
                        if applied.context.lease_deadline_monotonic <= session.deadline:
                            raise CoreError("STALE_GENERATION", "r4_renewal")
                        session.deadline = applied.context.lease_deadline_monotonic
        except Exception as error:
            if self._stopping:
                return
            if self._fence_local_lease_failure(session, error) and self.connection.online:
                await self._contain_session(session, source)
                return
            if isinstance(error, LeaseRequestDenied):
                # The transport correlated this exact request before raising.
                # Recheck the supplied port's proof against the retained owner;
                # neither another scope nor an uncorrelated link failure can
                # be converted into a successful scoped cleanup.
                if self._scoped_denial(error, source, 'renew'):
                    await self._contain_session(session, source)
                    return
            if session.stopped.is_set():
                return
            try:
                snapshot = await session.runtime.inspect(SessionKey(
                    source["server_id"], source["executor_id"], source["session_id"]))
            except Exception:
                snapshot = None
            if snapshot is not None and (snapshot.ownership == "released" or snapshot.lease_state == "CLOSED"):
                return
            if self.failure is None:
                self.failure = error
            await self.connection.close()

    @staticmethod
    def _fence_local_lease_failure(session, error):
        # Before an opening fact exists, initial admission/recovery owns the
        # outcome. This path only contains a known opened session and must not
        # invent release evidence for preparation or a missing opening claim.
        if (session.opened and isinstance(error, CoreError)
                and error.code in ('LEASE_EXPIRED', 'LEASE_REVALIDATION_REQUIRED')):
            session.local_lease_failure = session.local_lease_failure or error.code
            session.stopped.set()
            return True
        return False

    @asynccontextmanager
    async def _session_renewal(self, session):
        async with session.gate.renewal():
            try:
                yield
            except Exception as error:
                # Latch before waking queued productive work. The gate only
                # drains this session; control and siblings remain independent.
                self._fence_local_lease_failure(session, error)
                raise

    @staticmethod
    def _scoped_denial(error, source, purpose):
        request = error.request if isinstance(error, LeaseRequestDenied) else None
        return (type(request) is dict and request.get('scope') == {k: source[k] for k in _SCOPE}
            and request.get('grant_id') == source['grant_id'] and request.get('purpose') == purpose
            and all(request.get(k) == source[k] for k in ('connection_id','connection_generation')))

    async def _refuse_denied_operation(self, item, request):
        from nexus_connector_core.lease_denial import lease_denial_frame
        frame = self.connection.require_current(item)
        refusal = prepare_r4_lease_refusal(frame, lease_denial_frame(request))
        binding = refusal['binding']
        # Persist the association first; recovery can finish either side of
        # the Core journal admission/terminal-receipt cut without a new effect.
        await asyncio.to_thread(self.publications.bind, binding, refusal=refusal)
        journal = await self.host.ensure_history_journal()
        receipt = await persist_r4_lease_refusal(journal, refusal)
        return project_r4_bound_receipt(binding, receipt,
            key=OperationKey(frame['server_id'], frame['executor_id'], frame['operation_id']), receipt_revision=1)

    async def _refuse_stopped_operation(self, item, session):
        frame = self.connection.require_current(item)
        if not session.stopped.is_set() or session.local_lease_failure is None:
            raise CoreError('OPERATION_CONFLICT', 'r4_stopped_refusal')
        refusal = prepare_r4_stopped_refusal(frame,
            scope=session.scope, connection_id=session.connection_id,
            connection_generation=session.connection_generation,
            error_code=session.local_lease_failure)
        binding = refusal['binding']
        await asyncio.to_thread(self.publications.bind, binding, refusal=refusal)
        journal = await self.host.ensure_history_journal()
        receipt = await persist_r4_operation_refusal(journal, refusal)
        return project_r4_bound_receipt(binding, receipt,
            key=OperationKey(frame['server_id'], frame['executor_id'], frame['operation_id']), receipt_revision=1)

    async def _contain_session(self, session, source, *, publish_release=True):
        """Retain one stop producer, retrying only this session's resources.

        The renewal task already belongs to this owner and is joined on stop.
        Physical release does not settle canonical work or fabricate a close
        receipt. Its original journal/publication obligations remain retained.
        """
        from nexus_connector_core import ShutdownPolicy
        from nexus_connector_core.session_release import SESSION_RELEASE_REVISION
        key = ExecutionRuntimeKey(source['server_id'], source['executor_id'],
                                  source['binding_id'], source['session_id'])
        session.stopped.set()
        while not self._stopping and self.connection.online and self._sessions.get(key) is session:
            try:
                if not session.contained:
                    report = await session.runtime.shutdown(ShutdownPolicy(5, 5))
                    resources = session.runtime.shutdown_resources()
                    if (any(value not in {'graceful', 'forced', 'already_closed'}
                            for value in report.session_outcomes.values())
                            or any(value['process_state'] != 'STOPPED' or value['release_pending']
                                   for value in resources.values())):
                        raise CoreError('RECONCILIATION_REQUIRED', 'r4_session_containment')
                    if not await self.host.close_native_actions(key, timeout_seconds=5):
                        raise CoreError('RECONCILIATION_REQUIRED', 'r4_session_native_actions')
                    session.contained = True
                if publish_release and SESSION_RELEASE_REVISION in self.connection.state.control_capabilities:
                    session.release_accepted = await self.session_releases.run(
                        scope={k: source[k] for k in _SCOPE}, connection_id=source['connection_id'],
                        connection_generation=source['connection_generation'], stop_event=self._receipt_stop)
                session.containment_error = None
                return
            except Exception as error:
                # Keep the exact runtime, journal and physical ownership.
                # A failed observation/stop never grants spare capacity and
                # never stops an unrelated session sharing the same channel.
                session.containment_error = error
                try:
                    await asyncio.wait_for(self._receipt_stop.wait(), 2)
                except TimeoutError:
                    pass

    async def _refuse_failed_preparation(self, item, session, *, prelease=False):
        """Finish only an opening that never entered Core native admission."""
        frame = self.connection.require_current(item)
        if session is not None:
            session.stopped.set()
            if session.renewal is not None:
                await asyncio.gather(session.renewal, return_exceptions=True)
            await self._contain_session(session, frame, publish_release=False)
            if not session.contained:
                raise CoreError('RECONCILIATION_REQUIRED', 'r4_startup_containment')
        elif not prelease or STARTUP_REFUSAL_REVISION not in self.connection.state.control_capabilities:
            raise CoreError('RECONCILIATION_REQUIRED', 'r4_startup_containment')
        operation = OperationKey(frame['server_id'], frame['executor_id'], frame['operation_id'])
        session_key = SessionKey(frame['server_id'], frame['executor_id'], frame['session_id'])
        journal = await self.host.ensure_history_journal()
        if await journal.get_session_claim(session_key) is not None:
            raise CoreError('RECONCILIATION_REQUIRED', 'r4_startup_claim')
        refusal = prepare_r4_startup_refusal(frame)
        binding = refusal['binding']
        # Persist the refusal association before touching capacity. A crash
        # before Core fencing leaves the original preopen recovery obligation.
        await asyncio.to_thread(self.publications.bind, binding, refusal=refusal,
            startup_prelease=prelease)
        self._capacity_bindings[item.token] = binding
        released_capacity = None
        if self.capacity_provider is not None:
            capacity = await self.capacity_provider()
            ledger = await self.host.ensure_ledger()
            reservation = await ledger.capacity_for_operation(operation,
                action='runtime.open', now=self.clock(), boot_id=capacity.boot_id)
            if reservation is not None:
                if reservation.request.unit != 'new_session' or any(
                        getattr(reservation.request.scope, name) != frame[name]
                        for name in ('server_id', 'executor_id', 'agent_id', 'binding_id',
                            'session_id', 'connection_generation', 'session_owner_generation')):
                    raise CoreError('SCOPE_MISMATCH', 'r4_startup_capacity')
                released = await capacity.release_capacity(reservation.request,
                    reservation.reservation_id, 'ROLLBACK')
                if released.state != 'RELEASED':
                    raise CoreError('RECONCILIATION_REQUIRED', 'r4_startup_capacity')
                released_capacity = released.to_dict()
        receipt = await persist_r4_startup_refusal(journal, refusal)
        projected = project_r4_bound_receipt(binding, receipt, key=operation, receipt_revision=1)
        if not prelease:
            return projected
        report = dict(contract_revision=STARTUP_REFUSAL_REVISION, type='startup.refuse',
            request_id='startup_' + secrets.token_hex(16),
            server_id=frame['server_id'], executor_id=frame['executor_id'],
            connection_id=frame['connection_id'],
            connection_generation=frame['connection_generation'], refusal=refusal,
            receipt=projected, capacity=released_capacity, native_absent=True)
        return _StartupRefusalOutcome(projected, report)

    async def _execute(self, item):
        frame = self.connection.require_current(item)
        key = ExecutionRuntimeKey(frame["server_id"], frame["executor_id"], frame["binding_id"], frame["session_id"])
        session = self._sessions.get(key)
        if frame['action'] == 'runtime.open':
            prepared = self._prepared_openings.get(item.token)
            resuming = prepared is not None and prepared[0] is session
            # Reserve before the first await: concurrent preparations must not
            # overbook the host or open the same identity twice.
            if key in self._opening_sessions or (key in self._sessions and not resuming):
                raise CoreError('OPERATION_CONFLICT', 'r4_execution')
            if not resuming and len(set(self._sessions) | self._opening_sessions) >= self.max_sessions:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_execution',
                                     'The execution session capacity is exhausted.')
            self._opening_sessions.add(key)
            try:
                return await self._execute_owned(item)
            finally:
                self._opening_sessions.discard(key)
        containment = frame["action"] in ("turn.interrupt", "runtime.close") or (
            frame["action"] in ("approval.decide", "input.provide") and
            frame["payload"].get("decision") in ("decline", "cancel") and
            all(frame["payload"].get(k) is None for k in ("response", "response_ref", "response_digest")))
        if session is not None and not containment:
            async with session.gate.operation():
                if session.stopped.is_set():
                    # A request already queued behind renewal must not enter
                    # native work while its owner is being contained.
                    if session.denial is not None:
                        return await self._refuse_denied_operation(item, session.denial)
                    if session.local_lease_failure is not None:
                        return await self._refuse_stopped_operation(item, session)
                    raise CoreError('AGENT_REVOKED', 'r4_execution')
                return await self._execute_owned(item)
        if session is not None and frame["action"] == "runtime.close":
            session.stopped.set()
        try:
            return await self._execute_owned(item)
        except CoreError as error:
            # A compatible renewal can commit between containment's context
            # read and its guarded native frontier. Retry only a proven
            # pre-effect stale context, retaining the original operation ID.
            if (containment and error.code == "STALE_GENERATION" and
                    error.retry_safe and not error.possible_effect):
                return await self._execute_owned(item)
            raise

    async def _execute_owned(self, item):
        frame = self.connection.require_current(item)
        key = ExecutionRuntimeKey(frame['server_id'], frame['executor_id'],
                                  frame['binding_id'], frame['session_id'])
        action, payload = frame['action'], frame['payload']
        session = self._sessions.get(key)
        retained = self._prepared_openings.get(item.token)
        if action == 'runtime.open' and retained is None:
            # Server recovery, not a repeated bootstrap, resolves an opening
            # whose effect may already exist in the host or durable journal.
            if session is not None:
                raise CoreError('OPERATION_CONFLICT', 'r4_execution')
            if len(self._sessions) >= self.max_sessions:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_execution',
                                     'The execution session capacity is exhausted.')
            try:
                candidates = tuple(await self.candidate_provider(item.frame))
            except (NativeAdapterError, ConnectorError) as error:
                if isinstance(error, ConnectorError) and error.code in (
                        'STALE_GENERATION', 'CONTROL_DISCONNECTED', 'BINDING_NOT_AUTHORIZED',
                        'RECONCILIATION_REQUIRED', 'JOURNAL_UNAVAILABLE'):
                    raise
                return await self._refuse_failed_preparation(item, None, prelease=True)
            self.connection.require_current(item)
            if self.launch_provider is not None:
                setup = await self.launch_provider(item.frame)
            elif self.native_tools is not None:
                from .session_capabilities import ApprovedNativeLaunchProvider
                async def selected(_):
                    return candidates
                def current(_):
                    self.connection.require_current(item)
                services = self.native_tools
                setup = await ApprovedNativeLaunchProvider(services.owner, services.http,
                    services.key, self.host, self.store, candidate_provider=selected,
                    require_current=current)(item.frame)
            else:
                setup = await self.host.approved_launch(self.store, frame=item.frame, candidates=candidates)
            self.connection.require_current(item)
            if (not isinstance(setup, R4LaunchSetup) or not callable(setup.environment) or
                    (setup.native_action_factory is not None and not callable(setup.native_action_factory)) or
                    type(setup.auth_refs) is not tuple or
                    any(type(ref) is not str or not ref for ref in setup.auth_refs)):
                raise ConnectorError('VALIDATION_ERROR', 'r4_execution',
                                     'The approved launch configuration is invalid.')
            runtime = await self.host.build_r4(self.store, frame=item.frame,
                candidates=candidates, environment=setup.environment, factory=self.native_factory,
                native_action_factory=setup.native_action_factory,
                opening_selection=setup.opening_selection)
            self.connection.require_current(item)
            session = _Session(runtime, frame['intent_hash'], 'stream_' + secrets.token_hex(16),
                {k: frame[k] for k in _SCOPE}, frame['connection_id'], frame['connection_generation'])
            self._sessions[key] = session
            try:
                applied = await self.connection.apply_lease(runtime, scope={k: frame[k] for k in _SCOPE},
                                                   grant_id=frame['grant_id'])
            except LeaseRequestDenied as error:
                if not self._scoped_denial(error, frame, 'initial'):
                    raise
                session.denial = error.request
                session.stopped.set()
                receipt = await self._refuse_denied_operation(item, error.request)
                # No native opening was admitted. Drain this runtime/its tool
                # resources, without fabricating an opening release proof.
                await self._contain_session(session, frame, publish_release=False)
                if self.capacity_provider is not None:
                    from .r4_denied_capacity import release_denied_opening_capacity
                    capacity = await self.capacity_provider()
                    await release_denied_opening_capacity(self.publications,
                        await self.host.ensure_history_journal(), await self.host.ensure_ledger(),
                        server_id=frame['server_id'], executor_id=frame['executor_id'],
                        operation_id=frame['operation_id'], boot_id=capacity.boot_id,
                        clock=self.clock, require_current=lambda: self.connection.require_current(item))
                return receipt
            session.deadline = applied.context.lease_deadline_monotonic
            if self.require_authority is not None:
                session.renewal = asyncio.create_task(self._renew_session(session, dict(frame)),
                    name='r4-session-lease-renewal')
            context = self._context(item, runtime)
            try:
                prepared = await runtime.prepare(LaunchIntent(frame['agent_id'], frame['workspace_id'],
                    payload['adapter_id'], mode=payload['mode'], model=payload.get('model'),
                    auth_refs=setup.auth_refs,
                    mcp_preset=tuple(payload.get('mcp_preset', [])),
                    harness_settings=validate_harness_settings(payload['adapter_id'], payload.get('harness_settings', {}))), context)
            except NativeAdapterError:
                return await self._refuse_failed_preparation(item, session)
            self._prepared_openings[item.token] = (session, prepared)
        if action == 'runtime.open':
            retained_session, prepared = self._prepared_openings[item.token]
            if session is not retained_session or session.stopped.is_set():
                raise CoreError('SESSION_UNKNOWN', 'r4_execution')
            runtime = session.runtime
            # prepare and environment discovery may yield. The current lane
            # and installed authority are checked again before open.
            context = self._context(item, runtime)
            context = await self._bind(item, runtime, context, prepared=prepared,
                                       stream_epoch=session.stream_epoch)
            try:
                receipt = await runtime.open(OpenOperation(frame['operation_id'], frame['session_id'],
                                                           session.stream_epoch, prepared), context)
            except OperationCapacityDeferred:
                raise
            except CoreError:
                # Publish a durable failure fact rather than taking unrelated
                # bindings offline. Unknown effects still require reconciliation.
                journal = await self.host.ensure_journal()
                receipt = await journal.get_receipt(OperationKey(
                    frame['server_id'], frame['executor_id'], frame['operation_id']))
                if receipt is None or receipt.stage != 'FAILED':
                    raise
            session.opened = receipt.stage in ('SUBMITTED', 'SUCCEEDED')
            return project_r4_open_receipt(frame, receipt, context, prepared,
                stream_epoch=session.stream_epoch, receipt_revision=1)
        if session is None:
            raise CoreError('SESSION_UNKNOWN', 'r4_execution')
        runtime = session.runtime
        context = self._context(item, runtime)
        if action not in ('approval.decide', 'input.provide'):
            context = await self._bind(item, runtime, context)
        if action == 'turn.submit':
            receipt = await self._command_receipt(lambda: runtime.submit(TurnOperation(frame['operation_id'], frame['session_id'],
                payload['text'], frame.get('expected_turn_id')), context))
            project = project_r4_turn_receipt
        elif action in ('turn.steer', 'turn.interrupt'):
            receipt = await self._command_receipt(lambda: runtime.control(ControlOperation(frame['operation_id'], frame['session_id'],
                action.split('.')[1], text=payload.get('text'), reason=payload.get('reason'),
                expected_turn_id=frame.get('expected_turn_id')), context))
            project = project_r4_steer_receipt if action == 'turn.steer' else project_r4_interrupt_receipt
        elif action == 'runtime.close':
            await self.host.close_native_actions(key)
            # The daemon owns the operation, not merely an observation window.
            # Core keeps its physical deadline and commits the eventual fact.
            receipt = await runtime.close(r4_close_operation(frame), context,
                                          wait_for_completion=True)
            project = project_r4_close_receipt
        elif action in ('approval.decide', 'input.provide'):
            response = None
            if payload.get('response_ref') is not None:
                if self.response_resolver is None:
                    raise CoreError('INPUT_RESPONSE_UNAVAILABLE', 'r4_execution')
                response = await self.response_resolver(item.frame)
                context = self._context(item, runtime)
            operation = r4_native_decision_operation(frame, resolved_response=response)
            context = await self._bind(item, runtime, context, applied_operation=operation)
            receipt = await runtime.decide_native_approval(operation=operation, context=context)
            return project_r4_decision_receipt(frame, receipt, context, operation, receipt_revision=1)
        else:
            raise CoreError('CAPABILITY_UNSUPPORTED', 'r4_execution')
        return project(frame, receipt, context, receipt_revision=1)

    async def _command_receipt(self, invoke):
        try:
            return await invoke()
        except OperationNotAdmitted as error:
            # The operation worker durably records the projected refusal in
            # its reserved publication before sending it over the connection.
            return error.refusal
