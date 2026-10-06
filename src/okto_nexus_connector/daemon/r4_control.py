"""Daemon-owned R4 bootstrap, inventory and control negotiation.

Control readiness is independent of lane/session execution readiness. Confirmed
closed sessions and receipt history can be reconciled in bounded pages. Active
ownership and missing evidence keep this owner in recovery; durable event obligations are replayed before readiness.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import secrets
import time
import threading
import logging
import traceback
from urllib.parse import urlsplit
from nexus_connector_core import DEFAULT_RUNTIME_AUTOMATION

from ..errors import ConnectorError
from ..services.discovery_service import executor_inventory_snapshot
from ..services.discovery_configuration import configured_candidates, configuration_arguments
from ..services.executor_registration import ExecutorRegistrationService, _one, _identity, _profile
from ..transport.https_client import NexusHTTPClient, origin_of
from ..transport.wss_client import validate_link_url
from ..transport.wss_r4 import connect_r4_connection


_STATUS_ERRORS = frozenset({
    'VERSION_INCOMPATIBLE', 'PROFILE_DRIFT', 'STALE_GENERATION', 'AGENT_AUTH_REQUIRED',
    'CREDENTIAL_REPLACEMENT_REQUIRED', 'CREDENTIAL_MATERIAL_UNAVAILABLE',
    'AGENT_ID_MISMATCH', 'SCOPE_MISMATCH', 'OPERATION_CONFLICT', 'CONTROL_DISCONNECTED',
    'RECONCILIATION_REQUIRED', 'JOURNAL_UNAVAILABLE', 'EXECUTOR_OFFLINE', 'OUTCOME_UNKNOWN',
    'VALIDATION_ERROR', 'CAPACITY_EXCEEDED', 'RUNTIME_DRAINING', 'BINDING_NOT_AUTHORIZED',
    'CONFLICT', 'PERMISSION_DENIED',
})


# Reconnect management. Every failure is retried; these only decide when the
# status asks an operator to look instead of reporting a transient retry.
_ATTENTION_AFTER = 5
_PERSISTENT_ERRORS = frozenset({
    'PERMISSION_DENIED', 'AGENT_AUTH_REQUIRED', 'CREDENTIAL_REPLACEMENT_REQUIRED',
    'BINDING_NOT_AUTHORIZED', 'VERSION_INCOMPATIBLE', 'PROFILE_DRIFT', 'AGENT_ID_MISMATCH',
})
_RECONNECT_JITTER = 0.2


def _disconnect_cause(error):
    """Sanitized type/code/stage of a failure; never its text or close reason."""
    if error is None:
        return None
    parts = [type(error).__name__]
    # ConnectionClosed.code is deprecated in websockets; its frames carry it.
    code = None if hasattr(type(error), 'rcvd') else getattr(error, 'code', None)
    if type(code) is str and code.isupper() and len(code) <= 64:
        parts.append(code)
    stage = getattr(error, 'stage', None)
    if type(stage) is str and len(stage) <= 64:
        parts.append('stage=' + stage)
    # websockets ConnectionClosed: the close codes are numeric protocol facts.
    for side in ('rcvd', 'sent'):
        frame = getattr(error, side, None)
        if type(getattr(frame, 'code', None)) is int:
            parts.append(f'{side}_close={frame.code}')
    return ' '.join(parts)


def _link_url(profile, executor_id):
    expected = NexusHTTPClient(profile.base_url).link_url(executor_id)
    target = profile.link_url_override or expected
    validate_link_url(target)
    parts = urlsplit(target)
    http_target = parts._replace(scheme='https' if parts.scheme == 'wss' else 'http').geturl()
    if (origin_of(http_target) != profile.origin or parts.query or parts.fragment or
            (executor_id and parts.path != urlsplit(expected).path)):
        raise ConnectorError('PROFILE_DRIFT', 'r4_startup',
                             'The control URL differs from the approved Server endpoint.')
    return target


class R4DaemonControl:
    """One bounded lifecycle per explicitly registered Server.

    Stop joins the owned task rather than cancelling a registration or state
    write. Each reconnect obtains a fresh process-local bootstrap. No legacy
    identity fallback is used; approved lanes and consumers are composed after negotiation.
    """

    def __init__(self, store, vault, host, server_id, *, http_factory=NexusHTTPClient,
                 connect=connect_r4_connection, discover=None,
                 clock=time.monotonic, retry_delays=DEFAULT_RUNTIME_AUTOMATION.retry_delays,
                 poll_seconds=DEFAULT_RUNTIME_AUTOMATION.message_interval):
        if not retry_delays or min(retry_delays) <= 0 or poll_seconds <= 0:
            raise ValueError('Invalid R4 startup timing settings.')
        self.store, self.vault, self.host, self.server_id = store, vault, host, server_id
        self.http_factory, self.connect, self.discover, self.clock = http_factory, connect, discover, clock
        self.retry_delays, self.poll_seconds = retry_delays, poll_seconds
        self.registration = ExecutorRegistrationService(store, vault, http_factory=http_factory, clock=clock)
        self.boot_id = 'boot_' + secrets.token_hex(16)
        self.connection = None
        self.execution = None
        self.phase, self.error_code = 'STOPPED', None
        self.executor_id = ''
        self.inventory_revision = None
        self.publication_sequence = 0
        self.attempts = 0
        self.consecutive_failures = 0
        self.last_disconnect_cause = None
        self._interrupt_cause = None
        self._ready_since = None
        self._attention_logged = False
        self._task = None
        self._recovery_error = None
        self._reporter = None
        self._retained_lanes = {}
        self.cleanup_pending = False
        self._stopped = asyncio.Event()
        self._discovery_stopped = threading.Event()
        self._bootstrap_gate = asyncio.Lock()
        self._bootstrap = None
        self._onboarding_tasks = set()

    def start(self):
        if self._task is not None or self._stopped.is_set():
            raise RuntimeError('The R4 startup owner has already started.')
        self.phase = 'STARTING'
        self._task = asyncio.create_task(self._run(), name='r4-startup-' + self.server_id)

    async def stop(self):
        self._discovery_stopped.set()
        self._stopped.set()
        if self.connection is not None:
            await self.connection.close()
        if self._task is not None:
            await asyncio.shield(self._task)
        await asyncio.gather(*tuple(self._onboarding_tasks), return_exceptions=True)

    async def realize(self, **params):
        if self._stopped.is_set():
            raise ConnectorError("RUNTIME_DRAINING", "executor_onboarding",
                                 "The executor cannot accept another onboarding request.")
        if len(self._onboarding_tasks) >= 16:
            raise ConnectorError("CAPACITY_EXCEEDED", "executor_onboarding",
                                 "The executor onboarding capacity is exhausted.")
        task = asyncio.create_task(self._realize_owned(params), name="r4-realization-" + self.server_id)
        self._onboarding_tasks.add(task)
        def done(value):
            self._onboarding_tasks.discard(value)
            if not value.cancelled():
                value.exception()
        task.add_done_callback(done)
        return await asyncio.shield(task)

    async def _realize_owned(self, params):
        from ..services.executor_onboarding import ExecutorOnboarding
        async with self._bootstrap_gate:
            snapshot = await asyncio.to_thread(self._snapshot)
            await self._require(snapshot)
            bootstrap = self._bootstrap
            if bootstrap is None or self.clock() >= bootstrap.deadline_monotonic:
                raise ConnectorError("EXECUTOR_OFFLINE", "executor_onboarding",
                                     "Wait for the daemon executor bootstrap before publishing a realization.")
            record, identity, profile = snapshot
            key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
            async with self.http_factory(profile.base_url) as http:
                me = await http.me(key)
            await self._require(snapshot)
            if (me.server_id, me.agent_id) != (self.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "executor_onboarding",
                                     "The current credential differs from the executor registration identity.")
            if (me.authorization_revision != bootstrap.authorization_revision
                    or me.credential_epoch != bootstrap.credential_epoch):
                # Refresh this administrative publication's derivative only.
                # No control-channel ownership or native execution is retried.
                bootstrap = await self.registration.bootstrap(server_id=self.server_id)
                await self._require(snapshot)
            service = ExecutorOnboarding(self.store, self.vault, http_factory=self.http_factory, clock=self.clock)
            return await service.realize(**params, bootstrap=bootstrap,
                                         require_current=lambda: self._require(snapshot))

    def status(self):
        connection = self.connection
        return dict(state=self.phase, executor_id=self.executor_id,
            automatic_messages=DEFAULT_RUNTIME_AUTOMATION.automatic_messages,
            automatic_recovery=DEFAULT_RUNTIME_AUTOMATION.automatic_recovery,
            control_ready=bool(connection is not None and connection.online and
                               connection.state.control_ready and self.phase == 'CONTROL_READY'),
            execution_ready=bool(self.execution is not None and self.execution.ready), error_code=self.error_code,
            execution_errors=dict(self.execution.owner.execution_errors)
                if self.execution is not None and self.execution.owner is not None else {},
            recovery_required=self._recovery_error is not None, cleanup_pending=self.cleanup_pending,
            publication_sequence=self.publication_sequence, inventory_revision=self.inventory_revision,
            connection_generation=connection.state.connection_generation if connection else None,
            attempts=self.attempts,
            last_connected_at=getattr(self, 'last_connected_at', None),
            last_error_at=getattr(self, 'last_error_at', None),
            last_disconnect_cause=self.last_disconnect_cause,
            consecutive_failures=self.consecutive_failures,
            attention_required=self.phase == 'RECOVERY_ATTENTION_REQUIRED',
            last_received_at=getattr(connection, 'last_received_at', None),
            attached_bindings=sorted(self.execution.lanes) if self.execution else [])

    async def _wait(self, delay):
        try:
            await asyncio.wait_for(self._stopped.wait(), delay)
        except TimeoutError:
            pass

    def _snapshot(self):
        state = self.store.load()
        record = _one((r for r in state.execution_executors if r.server_id == self.server_id),
                      'The executor registration is missing or ambiguous.')
        if (record.connector_id != state.connector_id or
                record.state not in ('REGISTERED', 'REGISTRATION_PENDING')):
            raise ConnectorError('OPERATION_CONFLICT', 'r4_startup',
                                 'The executor registration is no longer valid.')
        configuration_arguments(record.discovery_configuration)
        profile = _profile(state, self.server_id)
        _link_url(profile, record.executor_id)  # Before any credential access.
        return (replace(record, inventory_publication_sequence=0),
                _identity(state, self.server_id, record.registration_agent_id), profile)

    async def _require(self, snapshot):
        if self._stopped.is_set():
            raise ConnectorError('RUNTIME_DRAINING', 'r4_startup', 'The daemon is stopping.')
        if await asyncio.to_thread(self._snapshot) != snapshot:
            raise ConnectorError('STALE_GENERATION', 'r4_startup',
                                 'The registration, identity or Server profile changed.')

    def _reserve_sequence(self, snapshot):
        result = None
        def reserve(state):
            nonlocal result
            expected, identity, profile = snapshot
            record = _one((r for r in state.execution_executors if r.server_id == self.server_id),
                          'The executor registration is missing or ambiguous.')
            if (replace(record, inventory_publication_sequence=0) != expected or
                    state.connector_id != expected.connector_id or
                    _identity(state, self.server_id, expected.registration_agent_id) != identity or
                    _profile(state, self.server_id) != profile):
                raise ConnectorError('STALE_GENERATION', 'r4_inventory',
                                     'The executor changed before inventory publication.')
            previous = record.inventory_publication_sequence
            if type(previous) is not int or not 0 <= previous < 2**53 - 1:
                raise ConnectorError('VALIDATION_ERROR', 'r4_inventory',
                                     'The inventory publication sequence is invalid.')
            record.inventory_publication_sequence += 1
            result = record.inventory_publication_sequence
        self.store.update(reserve)
        return result

    async def _publish(self, http, bootstrap, snapshot, refresh_delivery_id=None):
        await self._require(snapshot)
        observed_at = self.clock()
        candidates = tuple(await self.discover() if self.discover is not None else
                           await configured_candidates(snapshot[0].discovery_configuration,
                               observations=snapshot[0].installation_observations,
                               cancel_requested=self._discovery_stopped.is_set))
        await self._require(snapshot)
        if self.connection is not None and not self.connection.online:
            failure = (getattr(getattr(self.execution, 'owner', None), 'failure', None)
                       or getattr(self.connection, 'failure', None))
            if failure is not None:
                raise failure
            raise ConnectorError('CONTROL_DISCONNECTED', 'r4_inventory',
                                 'The control link closed during inventory discovery.')
        sequence = await asyncio.to_thread(self._reserve_sequence, snapshot)
        self.publication_sequence = sequence
        value = await asyncio.to_thread(executor_inventory_snapshot, candidates,
            server_id=self.server_id, executor_id=bootstrap.executor.executor_id,
            producer_instance_id=(self.connection.state.connection_id if self.connection else self.boot_id),
            publication_sequence=sequence)
        value['observation_age_ms'] = max(0, int((self.clock() - observed_at) * 1000))
        await self._require(snapshot)
        if self.clock() >= bootstrap.deadline_monotonic:
            raise ConnectorError('CONTROL_DISCONNECTED', 'r4_inventory',
                                 'The bootstrap ticket expired before inventory publication.')
        accepted = await http.publish_inventory(bootstrap.ticket,
            executor_id=bootstrap.executor.executor_id, snapshot=value,
            **({'refresh_delivery_id': refresh_delivery_id} if refresh_delivery_id is not None else {}))
        await self._require(snapshot)
        self.inventory_revision = accepted.inventory_revision
        return self.clock() + max(0.5, accepted.fresh_for_ms / 1000 * 0.7)

    async def _reconcile(self, request):
        from ..services.r4_reconciliation import R4ReconciliationReporter
        if self._reporter is None:
            self._reporter = R4ReconciliationReporter(self.store, self.host, self.server_id, self.executor_id)
        report = await self._reporter.report(request)
        self._recovery_error = 'RECONCILIATION_REQUIRED' if self._reporter.blocked else None
        return report

    async def _report_reconciliation(self, request):
        try:
            return await self._reconcile(request)
        except Exception as error:
            code = getattr(error, 'code', None)
            self._recovery_error = code if type(code) is str and code in _STATUS_ERRORS else 'JOURNAL_UNAVAILABLE'
            self.error_code = self._recovery_error
            raise

    async def _attempt(self):
        self._recovery_error = None
        self._reporter = None
        snapshot = await asyncio.to_thread(self._snapshot)
        record, _, profile = snapshot
        self.phase = 'CHECKING_PROTOCOL'
        async with self.http_factory(profile.base_url) as http:
            protocol = await http.r4_protocol()
            await self._require(snapshot)
            self.phase = 'REGISTERING'
            async with self._bootstrap_gate:
                await self._require(snapshot)
                bootstrap = await self.registration.bootstrap(server_id=self.server_id)
                self._bootstrap = bootstrap
            # Pending registration can now have a canonical result. All other
            # registration content, identity and origin must remain pinned.
            expected = (replace(record, state='REGISTERED', executor_id=bootstrap.executor.executor_id),
                        snapshot[1], profile)
            await self._require(expected)
            snapshot = expected
            self.executor_id = bootstrap.executor.executor_id
            if not protocol.remote_execution_ready:
                self.phase = 'PUBLISHING_INVENTORY'
                await self._publish(http, bootstrap, snapshot)
                raise ConnectorError('VERSION_INCOMPATIBLE', 'r4_startup',
                                     'The Server has not qualified remote R4 execution.')
            await self._require(snapshot)
            from ..services.r4_publications import recover_publications
            from ..services.binding_authority import refresh_idle_binding_authority
            await refresh_idle_binding_authority(self.store, self.vault, http, self.host,
                server_id=self.server_id, executor_id=self.executor_id)
            self._retained_lanes = {k:v for k,v in self._retained_lanes.items() if self.clock()<v.deadline}
            recovered_lanes = await recover_publications(self.store, self.vault, http,
                server_id=self.server_id, executor_id=self.executor_id,
                require_current=lambda: self._require(snapshot), clock=self.clock,
                journal=await self.host.ensure_history_journal(),
                authorities=self._retained_lanes)
            self._retained_lanes = recovered_lanes
            remaining = bootstrap.deadline_monotonic - self.clock()
            if remaining <= 0:
                raise ConnectorError('CONTROL_DISCONNECTED', 'r4_startup', 'The bootstrap ticket expired.')
            self.phase = 'RECOVERING'
            from ..services.r4_event_recovery import recover_event_streams
            recovery_tasks = set()
            async def recover_events(channel):
                task = asyncio.create_task(recover_event_streams(self.store,self.vault,http,self.host,channel,
                    require_current=lambda: self._require(snapshot),authorities=recovered_lanes,clock=self.clock),
                    name='r4-event-recovery')
                recovery_tasks.add(task)
                try:
                    return await asyncio.shield(task)
                except Exception as error:
                    code = getattr(error,'code',None)
                    self._recovery_error = code if type(code) is str and code in _STATUS_ERRORS else 'JOURNAL_UNAVAILABLE'
                    raise
            try:
                self.connection = await asyncio.wait_for(self.connect(
                    _link_url(profile, self.executor_id), bootstrap.ticket,
                    server_id=self.server_id, executor_id=self.executor_id,
                    management_revision=protocol.management_revision,
                    snapshot_format=protocol.executor_snapshot_format, boot_id=self.boot_id,
                    report_reconciliation=self._report_reconciliation,
                    recover_before_report=recover_events), min(30, remaining))
            finally:
                # Keep HTTP, vault and journal owners alive through a canceled
                # negotiation observer and any already-started ACK write.
                if recovery_tasks:
                    await asyncio.shield(asyncio.gather(*recovery_tasks,return_exceptions=True))
            if self._reporter is not None and self._reporter.blocked:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile',
                                     'Local recovery facts do not permit control readiness.')
            await self._require(snapshot)
            self.phase = 'PUBLISHING_INVENTORY'
            async def claim_refresh():
                if not getattr(protocol, 'inventory_refresh_supported', False):
                    return None
                delivery = await http.claim_inventory_refresh(bootstrap.ticket,
                    server_id=self.server_id, executor_id=self.executor_id,
                    producer_instance_id=self.connection.state.connection_id)
                await self._require(snapshot)
                return delivery
            delivery = await claim_refresh()
            refresh_at = await self._publish(http, bootstrap, snapshot, delivery)
            poll_refresh_at = self.clock() + 5
            from .r4_execution import R4DaemonExecution
            self.execution = R4DaemonExecution(self, http, recovered_lanes=recovered_lanes)
            onboarding_task = None
            try:
                await self.execution.sync()
                self.phase, self.error_code = 'CONTROL_READY', None
                self.last_connected_at = time.time()
                self._ready_since = self.clock()
                logging.getLogger(__name__).info('Runtime connected: server=%s executor=%s',
                                                 self.server_id, self.executor_id)
                renew_at = self.clock() + max(0, bootstrap.deadline_monotonic - self.clock()) * 0.7
                onboarding_at = self.clock()
                while not self._stopped.is_set() and self.connection.online:
                    await self._require(snapshot)
                    if self.clock() >= onboarding_at and (onboarding_task is None or onboarding_task.done()):
                        onboarding_task = asyncio.create_task(self._continue_onboarding())
                        onboarding_at = self.clock() + 5
                    await self.execution.sync()
                    if self.clock() >= renew_at:
                        # A bootstrap cannot rotate an attached lane or renew a
                        # runtime lease. Reconnect control with a fresh proof.
                        return
                    delivery = None
                    if self.clock() >= poll_refresh_at:
                        delivery = await claim_refresh()
                        poll_refresh_at = self.clock() + 5
                    if delivery is not None or self.clock() >= refresh_at:
                        refresh_at = await self._publish(http, bootstrap, snapshot, delivery)
                    await self._wait(min(self.poll_seconds, max(0.001, renew_at - self.clock())))
                if not self._stopped.is_set():
                    failure = getattr(self.execution.owner, 'failure', None) or getattr(self.connection, 'failure', None)
                    if failure is not None:
                        raise failure
                    raise ConnectorError('CONTROL_DISCONNECTED', 'r4_startup', 'The R4 control link was lost.')
            except Exception as error:
                code = getattr(error, 'code', None)
                self.error_code = code if type(code) is str and code in _STATUS_ERRORS else 'CONTROL_DISCONNECTED'
                # A closed link usually surfaces as a generic CONTROL_DISCONNECTED
                # from the next caller; the reader/heartbeat fenced the real cause.
                underlying = (getattr(self.execution.owner, 'failure', None)
                              or getattr(self.connection, 'failure', None))
                self._interrupt_cause = _disconnect_cause(
                    underlying if underlying is not None and underlying is not error else error)
                import traceback
                logging.getLogger(__name__).warning(
                    'Runtime execution interrupted: code=%s exception=%s stage=%s cause=%s location=%s',
                    getattr(error, 'code', 'CONTROL_DISCONNECTED'), type(error).__name__,
                    getattr(error, 'stage', 'unknown'), self._interrupt_cause,
                    ' > '.join(f'{frame.name}:{frame.lineno}' for frame in traceback.extract_tb(error.__traceback__)))
                raise
            finally:
                if onboarding_task is not None and not onboarding_task.done():
                    onboarding_task.cancel()
                    await asyncio.gather(onboarding_task, return_exceptions=True)
                self.phase = 'DRAINING'
                self._retained_lanes.update(self.execution.lanes)
                await self.execution.close(
                    preserve_leases=not self._stopped.is_set() and not self.connection.online,
                    stop_event=self._stopped)
                self.execution = None

    async def _continue_onboarding(self):
        from ..services.background_onboarding import continue_pending_bindings
        try:
            await continue_pending_bindings(self.store, self.vault,
                server_id=self.server_id, executor_id=self.executor_id,
                http_factory=self.http_factory)
        except Exception as error:
            logging.getLogger(__name__).warning('Background onboarding interrupted: code=%s exception=%s',
                getattr(error, 'code', 'ONBOARDING_ERROR'), type(error).__name__)

    async def _run(self):
        from nexus_connector_core import RuntimeAutomation, RuntimeAutomationPolicy
        async def cycle():
            self.attempts += 1
            if self.cleanup_pending and self.connection is not None:
                await self.connection.close()
                if getattr(self.connection, 'close_error', None) is not None:
                    raise ConnectorError('CONTROL_DISCONNECTED', 'r4_cleanup', 'Connection cleanup is still pending.')
                self.connection = None
                self.cleanup_pending = False
            try:
                await self._attempt()
            finally:
                if self.connection is not None:
                    await self.connection.close()
                    if getattr(self.connection, 'close_error', None) is not None:
                        self.cleanup_pending = True
                        self.error_code = 'CONTROL_DISCONNECTED'
                        raise ConnectorError('CONTROL_DISCONNECTED', 'r4_cleanup', 'Connection cleanup is still pending.')
                    else:
                        self.connection = None
        async def failed(error):
            self.last_error_at = time.time()
            # Error text can contain credentials or remote payloads.
            code = self._recovery_error or getattr(error, 'code', None)
            self.error_code = code if type(code) is str and code in _STATUS_ERRORS else 'CONTROL_DISCONNECTED'
            self.last_disconnect_cause = self._interrupt_cause or _disconnect_cause(error)
            self._interrupt_cause = None
            # A link that stayed ready for a stable period starts a new streak,
            # matching the backoff reset in RuntimeAutomation.
            if (self._ready_since is not None and
                    self.clock() - self._ready_since >= DEFAULT_RUNTIME_AUTOMATION.stable_seconds):
                self.consecutive_failures = 0
                self._attention_logged = False
            self._ready_since = None
            self.consecutive_failures += 1
            logging.getLogger(__name__).warning(
                'Runtime connection failed: server=%s phase=%s code=%s exception=%s stage=%s cause=%s consecutive=%d location=%s',
                self.server_id, self.phase, self.error_code, type(error).__name__,
                getattr(error, 'stage', 'unknown') if isinstance(error, ConnectorError) else 'unknown',
                self.last_disconnect_cause, self.consecutive_failures,
                ' > '.join(f'{frame.name}:{frame.lineno}' for frame in traceback.extract_tb(error.__traceback__)))
            recovering = self.error_code in ('RECONCILIATION_REQUIRED', 'JOURNAL_UNAVAILABLE')
            self._recovery_error = self.error_code if recovering else None
            self.phase = 'CLEANUP_PENDING' if self.cleanup_pending else 'RECOVERING' if recovering else 'RETRY_WAIT'
            if self.phase == 'RETRY_WAIT' and (self.consecutive_failures >= _ATTENTION_AFTER or (
                    self.error_code in _PERSISTENT_ERRORS and self.consecutive_failures >= 2)):
                # Still retried with backoff; the status asks for an operator.
                if not self._attention_logged:
                    self._attention_logged = True
                    logging.getLogger(__name__).warning(
                        'Runtime connection needs attention: server=%s code=%s consecutive=%d cause=%s',
                        self.server_id, self.error_code, self.consecutive_failures, self.last_disconnect_cause)
                self.phase = 'RECOVERY_ATTENTION_REQUIRED'
            return recovering
        async def exhausted():
            self.phase = 'RECOVERY_ATTENTION_REQUIRED'
        try:
            automation = RuntimeAutomation(RuntimeAutomationPolicy(
                retry_delays=self.retry_delays, retry_jitter=_RECONNECT_JITTER))
            await automation.supervise_connection(cycle=cycle, stop=self._stopped,
                failed=failed, exhausted=exhausted, wait=self._wait)
        finally:
            self.phase = 'CLEANUP_PENDING' if self.cleanup_pending else 'STOPPED'
