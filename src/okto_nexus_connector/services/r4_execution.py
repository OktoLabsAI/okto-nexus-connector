"""Owned consumers of canonical R4 operations for the Connector daemon.

The Server admits operations. This owner translates their immutable envelopes
through public Core APIs; it never resolves or admits a second intent.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import secrets

from nexus_connector_core import (
    CoreError, LaunchIntent, OpenOperation, TurnOperation, ControlOperation,
    project_r4_open_receipt, project_r4_turn_receipt, project_r4_steer_receipt,
    project_r4_interrupt_receipt, project_r4_close_receipt,
    project_r4_decision_receipt, r4_close_operation, r4_native_decision_operation,
)

from ..errors import ConnectorError
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


@dataclass(slots=True)
class _Session:
    runtime: object
    opening_hash: str
    stream_epoch: str


class R4ExecutionOwner:
    """Two bounded consumers, with producers retained through cancellation.

    Providers are host composition ports: a complete fresh local inventory,
    approved per-session environment, and receipt publication. A publication
    failure fences this connection; it cannot cause native operation replay.
    Reconciliation from durable Core facts is required before reconnect.
    """

    def __init__(self, connection, store, host, *, candidate_provider,
                 launch_provider, publish_receipt, native_factory=None,
                 response_resolver=None, max_sessions=64):
        if type(max_sessions) is not int or max_sessions <= 0:
            raise ValueError('Invalid execution session capacity.')
        self.connection, self.store, self.host = connection, store, host
        self.candidate_provider, self.launch_provider = candidate_provider, launch_provider
        self.publish_receipt = publish_receipt
        self.native_factory, self.response_resolver = native_factory, response_resolver
        self.max_sessions = max_sessions
        self._sessions = {}
        self._consumers = []
        self._producers = set()
        self._stopping = False
        self.failure = None

    @property
    def pending_count(self):
        return len(self._producers)

    def start(self):
        if self._consumers or self._stopping:
            raise RuntimeError('The execution owner has already started.')
        self._consumers = [asyncio.create_task(self._consume(control),
            name='r4-execution-control' if control else 'r4-execution-productive')
            for control in (False, True)]

    async def stop(self):
        self._stopping = True
        # Only consumers are observers. A producer may already be in the
        # native protocol or committing a receipt and must remain owned.
        for task in self._consumers:
            task.cancel()
        await asyncio.shield(asyncio.gather(*self._consumers, return_exceptions=True))
        if self._producers:
            await asyncio.shield(asyncio.gather(*tuple(self._producers), return_exceptions=True))
        for task in tuple(self._producers):
            if task.done():
                self._observe(task)

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

    async def _produce(self, item):
        try:
            receipt = await self._execute(item)
            # Once Core has produced a fact, loss of lane/link does not undo
            # it. Publish that same fact; never fabricate a second operation.
            await self.publish_receipt(receipt)
        except Exception as error:
            if self.failure is None:
                self.failure = error
            await self.connection.close()
            raise
        finally:
            self.connection.release_operation(item)

    async def _execute(self, item):
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
            setup = await self.launch_provider(item.frame)
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
            await self.connection.apply_lease(runtime, scope={k: frame[k] for k in _SCOPE},
                                               grant_id=frame['grant_id'])
            context = self._context(item, runtime)
            prepared = await runtime.prepare(LaunchIntent(frame['agent_id'], frame['workspace_id'],
                payload['adapter_id'], mode=payload['mode'], model=payload.get('model'),
                auth_refs=setup.auth_refs), context)
            # prepare and environment discovery may yield. The current lane
            # and installed authority are checked again before open.
            context = self._context(item, runtime)
            receipt = await runtime.open(OpenOperation(frame['operation_id'], frame['session_id'],
                                                       session.stream_epoch, prepared), context)
            return project_r4_open_receipt(frame, receipt, context, prepared,
                stream_epoch=session.stream_epoch, receipt_revision=1)
        if session is None:
            raise CoreError('SESSION_UNKNOWN', 'r4_execution')
        runtime = session.runtime
        context = self._context(item, runtime)
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
            receipt = await runtime.close(r4_close_operation(frame), context)
            project = project_r4_close_receipt
        elif action in ('approval.decide', 'input.provide'):
            response = None
            if payload.get('response_ref') is not None:
                if self.response_resolver is None:
                    raise CoreError('INPUT_RESPONSE_UNAVAILABLE', 'r4_execution')
                response = await self.response_resolver(item.frame)
                context = self._context(item, runtime)
            operation = r4_native_decision_operation(frame, resolved_response=response)
            receipt = await runtime.decide_native_approval(operation, context)
            return project_r4_decision_receipt(frame, receipt, context, operation, receipt_revision=1)
        else:
            raise CoreError('CAPABILITY_UNSUPPORTED', 'r4_execution')
        return project(frame, receipt, context, receipt_revision=1)
