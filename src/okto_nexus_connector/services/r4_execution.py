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
    prepare_r4_receipt_binding, SessionKey, OperationKey,
)

from ..errors import ConnectorError
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
    gate: _RenewalGate = field(default_factory=_RenewalGate)
    stopped: asyncio.Event = field(default_factory=asyncio.Event)
    deadline: float = 0
    renewal: asyncio.Task | None = None


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
                 require_current=None, clock=time.monotonic):
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
        self._renewal_slots = asyncio.Semaphore(8)
        from .r4_events import R4EventPublisher
        self.events = R4EventPublisher(connection, store, host, max_streams=max_sessions)
        self.max_sessions = max_sessions
        self._sessions = {}
        self._consumers = []
        self._producers = set()
        self._stopping = False
        self._receipt_stop = asyncio.Event()
        self._receipt_monitor = None
        self.failure = None
        self.execution_errors = {}

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
                producer = asyncio.create_task(self._produce(item), name='r4-operation-producer')
                self._producers.add(producer)
                producer.add_done_callback(self._observe)
                await asyncio.shield(producer)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self.failure is None:
                self.failure = error

    def _context(self, item, runtime):
        return runtime.r4_operation_context(self.connection.require_current(item),
            connection_id=self.connection.state.connection_id,
            connection_generation=self.connection.state.connection_generation)

    async def _bind(self, item, runtime, context, **options):
        binding = prepare_r4_receipt_binding(item.frame, context, **options)
        await asyncio.to_thread(self.publications.bind, binding,
                               **({'stream_epoch': options['stream_epoch']} if 'stream_epoch' in options else {}))
        if 'stream_epoch' in options:
            await asyncio.to_thread(self.events.store.register,{**item.frame,'stream_epoch':options['stream_epoch']})
        # Persistence can yield while the lane, link or lease is superseded.
        return self._context(item, runtime)

    async def _produce(self, item):
        try:
            await asyncio.to_thread(self.publications.reserve, self.connection.require_current(item))
            receipt = await self._execute(item)
            self._record_execution_outcome(receipt)
            # Once Core has produced a fact, loss of lane/link does not undo
            # it. Publish that same fact; never fabricate a second operation.
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
            self.connection.release_operation(item)

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
                    async with session.gate.renewal():
                        await current()
                        if self.clock() >= session.deadline:
                            raise CoreError("LEASE_EXPIRED", "r4_renewal")
                        snapshot = await session.runtime.inspect(SessionKey(
                            source["server_id"], source["executor_id"], source["session_id"]))
                        if snapshot.ownership == "released" or snapshot.lease_state == "CLOSED":
                            return
                        await current()
                        applied = await self.connection.apply_lease(session.runtime,
                            scope={k:source[k] for k in _SCOPE}, grant_id=source["grant_id"], purpose="renew",
                            require_current=current, fence_on_error=False)
                        await current()
                        if applied.context.lease_deadline_monotonic <= session.deadline:
                            raise CoreError("STALE_GENERATION", "r4_renewal")
                        session.deadline = applied.context.lease_deadline_monotonic
        except Exception as error:
            if self._stopping or session.stopped.is_set():
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

    async def _execute(self, item):
        frame = self.connection.require_current(item)
        key = ExecutionRuntimeKey(frame["server_id"], frame["executor_id"], frame["binding_id"], frame["session_id"])
        session = self._sessions.get(key)
        containment = frame["action"] in ("turn.interrupt", "runtime.close") or (
            frame["action"] in ("approval.decide", "input.provide") and
            frame["payload"].get("decision") in ("decline", "cancel") and
            all(frame["payload"].get(k) is None for k in ("response", "response_ref", "response_digest")))
        if session is not None and not containment:
            async with session.gate.operation():
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
        if action == 'runtime.open':
            # Server recovery, not a repeated bootstrap, resolves an opening
            # whose effect may already exist in the host or durable journal.
            if session is not None:
                raise CoreError('OPERATION_CONFLICT', 'r4_execution')
            if len(self._sessions) >= self.max_sessions:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_execution',
                                     'The execution session capacity is exhausted.')
            candidates = tuple(await self.candidate_provider(item.frame))
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
                native_action_factory=setup.native_action_factory)
            self.connection.require_current(item)
            session = _Session(runtime, frame['intent_hash'], 'stream_' + secrets.token_hex(16))
            self._sessions[key] = session
            applied = await self.connection.apply_lease(runtime, scope={k: frame[k] for k in _SCOPE},
                                               grant_id=frame['grant_id'])
            session.deadline = applied.context.lease_deadline_monotonic
            context = self._context(item, runtime)
            prepared = await runtime.prepare(LaunchIntent(frame['agent_id'], frame['workspace_id'],
                payload['adapter_id'], mode=payload['mode'], model=payload.get('model'),
                auth_refs=setup.auth_refs,
                harness_settings=validate_harness_settings(payload['adapter_id'], payload.get('harness_settings', {}))), context)
            # prepare and environment discovery may yield. The current lane
            # and installed authority are checked again before open.
            context = self._context(item, runtime)
            context = await self._bind(item, runtime, context, prepared=prepared,
                                       stream_epoch=session.stream_epoch)
            try:
                receipt = await runtime.open(OpenOperation(frame['operation_id'], frame['session_id'],
                                                           session.stream_epoch, prepared), context)
            except CoreError:
                # Publish a durable failure fact rather than taking unrelated
                # bindings offline. Unknown effects still require reconciliation.
                journal = await self.host.ensure_journal()
                receipt = await journal.get_receipt(OperationKey(
                    frame['server_id'], frame['executor_id'], frame['operation_id']))
                if receipt is None or receipt.stage != 'FAILED':
                    raise
            return project_r4_open_receipt(frame, receipt, context, prepared,
                stream_epoch=session.stream_epoch, receipt_revision=1)
        if session is None:
            raise CoreError('SESSION_UNKNOWN', 'r4_execution')
        runtime = session.runtime
        context = self._context(item, runtime)
        if action not in ('approval.decide', 'input.provide'):
            context = await self._bind(item, runtime, context)
        if action == 'turn.submit':
            receipt = await runtime.submit(TurnOperation(frame['operation_id'], frame['session_id'],
                payload['text'], frame.get('expected_turn_id')), context)
            project = project_r4_turn_receipt
        elif action in ('turn.steer', 'turn.interrupt'):
            receipt = await runtime.control(ControlOperation(frame['operation_id'], frame['session_id'],
                action.split('.')[1], text=payload.get('text'), reason=payload.get('reason'),
                expected_turn_id=frame.get('expected_turn_id')), context)
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
