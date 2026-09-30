"""Daemon-owned R4 bootstrap, inventory and control negotiation.

Control readiness is independent of lane/session execution readiness. Until
nonempty reconciliation is integrated, durable claims keep this owner in
recovery; they are never replaced with an invented empty report.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import secrets
import time
from urllib.parse import urlsplit

from ..errors import ConnectorError
from ..services.discovery_service import inventory_candidates, executor_inventory_snapshot
from ..services.executor_registration import ExecutorRegistrationService, _one, _identity, _profile
from ..transport.https_client import NexusHTTPClient, origin_of
from ..transport.wss_client import validate_link_url
from ..transport.wss_r4 import connect_r4_connection


_STATUS_ERRORS = frozenset({
    'VERSION_INCOMPATIBLE', 'PROFILE_DRIFT', 'STALE_GENERATION', 'AGENT_AUTH_REQUIRED',
    'AGENT_ID_MISMATCH', 'SCOPE_MISMATCH', 'OPERATION_CONFLICT', 'CONTROL_DISCONNECTED',
    'RECONCILIATION_REQUIRED', 'JOURNAL_UNAVAILABLE', 'EXECUTOR_OFFLINE', 'OUTCOME_UNKNOWN',
    'VALIDATION_ERROR', 'CAPACITY_EXCEEDED', 'RUNTIME_DRAINING', 'BINDING_NOT_AUTHORIZED',
    'CONFLICT', 'PERMISSION_DENIED',
})


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
    identity fallback, lane attachment or runtime effect is implied here.
    """

    def __init__(self, store, vault, host, server_id, *, http_factory=NexusHTTPClient,
                 connect=connect_r4_connection, discover=inventory_candidates,
                 clock=time.monotonic, retry_delays=(0.5, 1, 2, 5, 10, 30),
                 poll_seconds=0.5):
        if not retry_delays or min(retry_delays) <= 0 or poll_seconds <= 0:
            raise ValueError('Invalid R4 startup timing settings.')
        self.store, self.vault, self.host, self.server_id = store, vault, host, server_id
        self.http_factory, self.connect, self.discover, self.clock = http_factory, connect, discover, clock
        self.retry_delays, self.poll_seconds = retry_delays, poll_seconds
        self.registration = ExecutorRegistrationService(store, vault, http_factory=http_factory, clock=clock)
        self.boot_id = 'boot_' + secrets.token_hex(16)
        self.connection = None
        self.phase, self.error_code = 'STOPPED', None
        self.executor_id = ''
        self.inventory_revision = None
        self.publication_sequence = 0
        self.attempts = 0
        self._task = None
        self._recovery_error = None
        self.cleanup_pending = False
        self._stopped = asyncio.Event()

    def start(self):
        if self._task is not None or self._stopped.is_set():
            raise RuntimeError('The R4 startup owner has already started.')
        self.phase = 'STARTING'
        self._task = asyncio.create_task(self._run(), name='r4-startup-' + self.server_id)

    async def stop(self):
        self._stopped.set()
        if self.connection is not None:
            await self.connection.close()
        if self._task is not None:
            await asyncio.shield(self._task)

    def status(self):
        connection = self.connection
        return dict(state=self.phase, executor_id=self.executor_id,
            control_ready=bool(connection is not None and connection.online and
                               connection.state.control_ready and self.phase == 'CONTROL_READY'),
            execution_ready=False, error_code=self.error_code,
            recovery_required=self._recovery_error is not None, cleanup_pending=self.cleanup_pending,
            publication_sequence=self.publication_sequence, inventory_revision=self.inventory_revision,
            connection_generation=connection.state.connection_generation if connection else None,
            attempts=self.attempts)

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

    async def _publish(self, http, bootstrap, snapshot):
        observed_at = self.clock()
        candidates = tuple(await self.discover())
        await self._require(snapshot)
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
            executor_id=bootstrap.executor.executor_id, snapshot=value)
        await self._require(snapshot)
        self.inventory_revision = accepted.inventory_revision
        return self.clock() + max(0.5, accepted.fresh_for_ms / 1000 * 0.7)

    async def _empty_reconciliation(self, request):
        """Only prove an empty namespace, using public durable Core readers."""
        if (request['server_id'] != self.server_id or request['executor_id'] != self.executor_id):
            raise ConnectorError('SCOPE_MISMATCH', 'r4_reconcile', 'The reconciliation scope changed.')
        if (request['operation_ids'] or request['session_ids'] or request.get('stream_watermarks') or
                request['cursor'] is not None):
            raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile',
                                 'The Server requires durable nonempty reconciliation.')
        journal = await self.host.ensure_history_journal()
        claims = await journal.claimed_sessions(self.server_id, self.executor_id, limit=1)
        if claims.claims or claims.next_after_rowid is not None:
            raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile',
                                 'Durable session claims require recovery before control readiness.')
        ledger = await self.host.ensure_ledger()
        for source in (journal, ledger):
            after, high = 0, None
            for _ in range(32):
                page = await source.owned_slot_page(after_rowid=after, high_water_rowid=high, limit=128)
                if any((row.key.server_id, row.key.executor_id) == (self.server_id, self.executor_id)
                       for row in page.reservations):
                    raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile',
                                         'Owned resources require recovery before control readiness.')
                high = page.high_water_rowid
                if page.next_after_rowid is None:
                    break
                if page.next_after_rowid <= after:
                    raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_reconcile',
                                         'The durable ownership cursor did not advance.')
                after = page.next_after_rowid
            else:
                raise ConnectorError('RECONCILIATION_REQUIRED', 'r4_reconcile',
                                     'The ownership scan requires another recovery page.')
        return {k: request[k] for k in ('protocol_major', 'contract_revision', 'server_id', 'executor_id',
            'connection_id', 'connection_generation', 'reconcile_id', 'cursor')} | dict(
                type='reconcile.report', next_cursor=None, complete=True, receipts=[], claims=[],
                stream_watermarks=[], ownership_facts=[])

    async def _report_reconciliation(self, request):
        try:
            return await self._empty_reconciliation(request)
        except Exception as error:
            code = getattr(error, 'code', None)
            self._recovery_error = code if type(code) is str and code in _STATUS_ERRORS else 'JOURNAL_UNAVAILABLE'
            self.error_code = self._recovery_error
            raise

    async def _attempt(self):
        self._recovery_error = None
        snapshot = await asyncio.to_thread(self._snapshot)
        record, _, profile = snapshot
        self.phase = 'CHECKING_PROTOCOL'
        async with self.http_factory(profile.base_url) as http:
            protocol = await http.r4_protocol()
            await self._require(snapshot)
            self.phase = 'REGISTERING'
            bootstrap = await self.registration.bootstrap(server_id=self.server_id)
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
            remaining = bootstrap.deadline_monotonic - self.clock()
            if remaining <= 0:
                raise ConnectorError('CONTROL_DISCONNECTED', 'r4_startup', 'The bootstrap ticket expired.')
            self.phase = 'RECOVERING'
            self.connection = await asyncio.wait_for(self.connect(
                _link_url(profile, self.executor_id), bootstrap.ticket,
                server_id=self.server_id, executor_id=self.executor_id,
                management_revision=protocol.management_revision,
                snapshot_format=protocol.executor_snapshot_format, boot_id=self.boot_id,
                report_reconciliation=self._report_reconciliation), min(30, remaining))
            await self._require(snapshot)
            self.phase = 'PUBLISHING_INVENTORY'
            refresh_at = await self._publish(http, bootstrap, snapshot)
            self.phase, self.error_code = 'CONTROL_READY', None
            renew_at = self.clock() + max(0, bootstrap.deadline_monotonic - self.clock()) * 0.7
            while not self._stopped.is_set() and self.connection.online:
                await self._require(snapshot)
                if self.clock() >= renew_at:
                    # A bootstrap cannot rotate an attached lane or renew a
                    # runtime lease. Reconnect control with a fresh proof.
                    return
                if self.clock() >= refresh_at:
                    refresh_at = await self._publish(http, bootstrap, snapshot)
                await self._wait(min(self.poll_seconds, max(0.001, renew_at - self.clock())))
            if not self._stopped.is_set():
                raise ConnectorError('CONTROL_DISCONNECTED', 'r4_startup', 'The R4 control link was lost.')

    async def _run(self):
        failures = 0
        try:
            while not self._stopped.is_set():
                self.attempts += 1
                try:
                    await self._attempt()
                    failures = 0
                except Exception as error:
                    # Never expose exception text containing credentials or
                    # remote payloads through IPC/status.
                    code = self._recovery_error or getattr(error, 'code', None)
                    self.error_code = code if type(code) is str and code in _STATUS_ERRORS else 'CONTROL_DISCONNECTED'
                    self.phase = 'RECOVERING' if self.error_code in ('RECONCILIATION_REQUIRED', 'JOURNAL_UNAVAILABLE') else 'RETRY_WAIT'
                    failures += 1
                finally:
                    if self.connection is not None:
                        await self.connection.close()
                        if getattr(self.connection, 'close_error', None) is not None:
                            self.cleanup_pending = True
                            self.error_code = 'CONTROL_DISCONNECTED'
                            return
                        self.connection = None
                if not self._stopped.is_set():
                    await self._wait(self.retry_delays[min(max(0, failures - 1), len(self.retry_delays) - 1)])
        finally:
            self.phase = 'CLEANUP_PENDING' if self.cleanup_pending else 'STOPPED'
