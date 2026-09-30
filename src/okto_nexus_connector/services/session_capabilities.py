"""Owned, durable session credential issuance before host configuration.

The vault preserves material; the state file preserves only request identity
and references. A restart must reconcile authority before reusing stored
material. This owner never infers a fresh lifetime from persisted timestamps
or silently replaces a credential after uncertain delivery.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import secrets
import time
from types import MappingProxyType

from nexus_connector_core import decode_r4_frame, encode_r4_frame
from nexus_connector_core.protocol import canonical_json

from ..errors import CapabilityMaterialUnavailable, ConnectorError
from ..storage.state_store import SessionCapabilityRecord


class CapabilityLaunchProvider:
    """R4ExecutionOwner launch port with durable issuance before configuration.

    The trusted configure callback renders the approved host configuration
    using the returned capability and must return R4LaunchSetup. It is never
    invoked before vault persistence or after current-lane validation fails.
    """

    def __init__(self, owner, http, key, *, audience, actions, configure, require_current):
        self.owner, self.http, self._key = owner, http, key
        self.audience, self.actions = audience, actions
        self.configure, self.require_current = configure, require_current

    async def __call__(self, frame):
        frozen = decode_r4_frame(encode_r4_frame(frame))
        def guard():
            self.require_current(decode_r4_frame(encode_r4_frame(frozen)))
        capability = await self.owner.reserve(self.http, self._key, frame=frozen,
            audience=self.audience, actions=self.actions, require_current=guard)
        guard()
        result = await self.configure(frozen, capability)
        guard()
        return result


class SessionCapabilityOwner:
    """One retained producer per immutable request, bounded by durable slots."""

    def __init__(self, store, vault, *, clock=time.monotonic, capacity=128):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("The session capability capacity must be positive.")
        self.store, self.vault, self.clock, self.capacity = store, vault, clock, capacity
        self._tasks = {}
        self._restores = set()
        self._closing = False

    @property
    def pending_count(self):
        return sum(not task.done() for _, task in self._tasks.values()) + len(self._restores)

    async def close(self):
        self._closing = True
        # Cancellation belongs to the observer, never the HTTP/vault producer.
        await asyncio.shield(asyncio.gather(
            *(task for _, task in self._tasks.values()), *tuple(self._restores), return_exceptions=True))

    async def reserve(self, http, key, *, frame, audience, actions, require_current):
        """Persist before POST and vault-commit before returning launch material.

        require_current is the host's synchronous current-lane/configuration
        guard. The caller supplies an authenticated, origin-pinned HTTP client
        for this Server and keeps it alive until this owner's close completes.
        """
        parsed, reservation, digest = self._request_identity(frame, audience, actions)
        if not callable(require_current):
            raise ConnectorError('VALIDATION_ERROR', 'capability_reservation',
                                 'A current host authority guard is required.')
        require_current()
        if self._closing:
            raise ConnectorError('CONTROL_DISCONNECTED', 'capability_reservation',
                                 'The session capability owner is shutting down.')
        cached = self._tasks.get(reservation)
        if cached is not None:
            if cached[0] != digest:
                raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                     'The session capability request changed.')
            task = cached[1]
        else:
            if len(self._tasks) >= self.capacity:
                raise ConnectorError('CAPACITY_EXCEEDED', 'capability_reservation',
                                     'The session capability capacity is exhausted.')
            task = asyncio.create_task(self._reserve(http, key, parsed, audience,
                tuple(sorted(actions)), reservation, digest, require_current),
                name='session-capability-producer')
            self._tasks[reservation] = (digest, task)
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        result = await asyncio.shield(task)
        require_current()
        if self.clock() >= result.deadline_monotonic:
            raise ConnectorError('AUTH_EXPIRED', 'capability_reservation',
                                 'The session capability requires authority renewal.')
        return replace(result, scope=MappingProxyType(dict(result.scope)))

    @staticmethod
    def _request_identity(frame, audience, actions):
        parsed = decode_r4_frame(encode_r4_frame(frame))
        if (parsed['type'] != 'operation.submit' or parsed['action'] != 'runtime.open'
                or audience not in ('nexus-mcp-session', 'nexus-native-session')
                or type(actions) is not tuple or len(actions) > 128
                or any(type(a) is not str or not 1 <= len(a) <= 160 for a in actions)
                or len(set(actions)) != len(actions)):
            raise ConnectorError('VALIDATION_ERROR', 'capability_reservation',
                                 'The session capability reservation is invalid.')
        body = {k: v for k, v in parsed.items()
                if k not in ('connection_id', 'connection_generation')}
        body.update(audience=audience, actions=sorted(actions))
        digest = hashlib.sha256(canonical_json(body)).hexdigest()
        identity = {k: parsed[k] for k in ('server_id', 'executor_id', 'session_id')}
        identity['audience'] = audience
        reservation = hashlib.sha256(canonical_json(identity)).hexdigest()
        return parsed, reservation, digest

    def _stage(self, frame, audience, reservation, digest, *, create=True):
        result = None
        def stage(state):
            nonlocal result
            rows = [r for r in state.session_capabilities if r.reservation_id == reservation]
            if rows:
                if len(rows) != 1 or rows[0].request_digest != digest:
                    raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                         'The durable capability reservation changed.')
                record = rows[0]
                if (any(getattr(record, name) != frame[name] for name in
                        ('server_id', 'executor_id', 'session_id'))
                        or record.audience != audience
                        or record.secret_handle != 'vault:session-capability/' + reservation
                        or type(record.request_id) is not str or not 1 <= len(record.request_id) <= 160
                        or record.status not in ('REQUESTED', 'MATERIAL_RECEIVED',
                                                 'MATERIAL_UNAVAILABLE', 'STORED')):
                    raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                         'The persisted capability metadata is inconsistent.')
            else:
                if not create:
                    raise ConnectorError('CREDENTIAL_MATERIAL_UNAVAILABLE', 'capability_restore',
                                         'No durable session capability is available.')
                if len(state.session_capabilities) >= self.capacity:
                    raise ConnectorError('CAPACITY_EXCEEDED', 'capability_reservation',
                                         'The durable capability capacity is exhausted.')
                record = SessionCapabilityRecord(reservation, 'capreq_' + secrets.token_hex(16),
                    digest, frame['server_id'], frame['executor_id'], frame['session_id'],
                    audience, 'vault:session-capability/' + reservation)
                state.session_capabilities.append(record)
            result = replace(record)
        self.store.update(stage)
        return result

    async def restore(self, http, key, *, frame, audience, actions, runtime, require_current):
        """Rehydrate vault material under matching Server and Core applied leases.

        This reads authority; it does not reopen a runtime, install a lease,
        replace a capability or resend an opening. Reconciliation owns those
        prerequisites. Every observation uses a new correlated metadata read.
        """
        parsed, reservation, digest = self._request_identity(frame, audience, actions)
        if not callable(require_current):
            raise ConnectorError('VALIDATION_ERROR', 'capability_restore',
                                 'A current host authority guard is required.')
        require_current()
        if self._closing or len(self._restores) >= self.capacity:
            raise ConnectorError('CAPACITY_EXCEEDED', 'capability_restore',
                                 'The capability recovery owner is unavailable.')
        task = asyncio.create_task(self._restore(http, key, parsed, audience, actions,
            reservation, digest, runtime, require_current), name='session-capability-recovery')
        self._restores.add(task)
        def observed(done):
            self._restores.discard(done)
            if not done.cancelled():
                done.exception()
        task.add_done_callback(observed)
        return await asyncio.shield(task)

    async def _restore(self, http, key, frame, audience, actions, reservation, digest, runtime, guard):
        from ..transport.https_client import R4SessionCapability
        record = await asyncio.to_thread(self._stage, frame, audience, reservation, digest, create=False)
        guard()
        if record.status not in ('STORED', 'MATERIAL_RECEIVED') or not record.capability_id:
            raise CapabilityMaterialUnavailable('CREDENTIAL_MATERIAL_UNAVAILABLE',
                'capability_restore', 'Protected session material is unavailable.',
                capability_id=record.capability_id, recovery_allowed=False)
        scope = {k: frame[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
            'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
            'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
        def context():
            guard()
            return runtime.r4_native_action_context(scope,
                connection_id=frame['connection_id'], connection_generation=frame['connection_generation'])
        context()  # Refuse missing, stale or unapplied local authority before HTTP.
        metadata = await http.describe_r4_session_capability(key, frame=frame,
            capability_id=record.capability_id, audience=audience, actions=actions)
        def deadline():
            current = context()
            authority = current.r4_authority
            if (metadata.capability_ref != record.capability_ref or authority is None
                    or authority.grant_id != frame['grant_id']
                    or (metadata.lease_id, metadata.lease_serial) != (authority.lease_id, authority.lease_serial)):
                raise ConnectorError('STALE_GENERATION', 'capability_restore',
                                     'Server and Core capability authority do not match.')
            until = min(metadata.deadline_monotonic, current.lease_deadline_monotonic)
            if until - self.clock() < 1:
                raise ConnectorError('AUTH_EXPIRED', 'capability_restore',
                                     'The reconciled capability authority has expired.')
            return until
        deadline()
        material = await asyncio.to_thread(self.vault.resolve, record.secret_handle)
        if (type(material) is not str or not material.startswith('nxc4_')
                or not 32 <= len(material) <= 4096):
            raise ConnectorError('CREDENTIAL_MATERIAL_UNAVAILABLE', 'capability_restore',
                                 'The protected session material is invalid.')
        deadline()
        await asyncio.to_thread(self._record, record, status='STORED',
            capability_id=metadata.capability_id, capability_ref=metadata.capability_ref)
        until = deadline()
        return R4SessionCapability(metadata.capability_id, metadata.capability_ref, material,
            MappingProxyType(scope), audience, tuple(sorted(actions)),
            min(120, int(until - self.clock())), until, metadata.mcp_url)

    def _record(self, record, *, status, capability_id='', capability_ref='', recovery_allowed=False):
        def commit(state):
            rows = [r for r in state.session_capabilities if r.reservation_id == record.reservation_id]
            if (len(rows) != 1 or rows[0].request_digest != record.request_digest
                    or rows[0].request_id != record.request_id
                    or any(getattr(rows[0], name) != getattr(record, name) for name in
                           ('secret_handle', 'server_id', 'executor_id', 'session_id', 'audience'))):
                raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                     'The durable capability reservation changed.')
            current = rows[0]
            if current.capability_id and capability_id != current.capability_id:
                raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                     'The capability result changed identity.')
            # Another process may have committed the one-time result while our
            # replay was in flight. Recovery metadata must never erase it.
            if current.status == 'STORED' and status != 'STORED':
                return
            current.status, current.capability_id = status, capability_id
            current.capability_ref = capability_ref or current.capability_ref
            current.recovery_allowed = recovery_allowed
        self.store.update(commit)

    async def _reserve(self, http, key, frame, audience, actions, reservation, digest, guard):
        record = await asyncio.to_thread(self._stage, frame, audience, reservation, digest)
        guard()
        if record.status != 'REQUESTED':
            raise CapabilityMaterialUnavailable('CREDENTIAL_MATERIAL_UNAVAILABLE',
                'capability_reservation', 'The stored capability requires authority reconciliation.',
                capability_id=record.capability_id, recovery_allowed=False,
                action='Reconcile the session and its protected configuration before reuse.')
        try:
            result = await http.request_r4_session_capability(key, frame=frame,
                capability_request_id=record.request_id, audience=audience, actions=actions)
        except CapabilityMaterialUnavailable as error:
            await asyncio.to_thread(self._record, record, status='MATERIAL_UNAVAILABLE',
                capability_id=error.capability_id, recovery_allowed=error.recovery_allowed)
            raise
        # Preserve the result even if the observer/lane disappeared. It may
        # already exist on the Server; never abandon a returned secret.
        await asyncio.to_thread(self._record, record, status='MATERIAL_RECEIVED',
            capability_id=result.capability_id, capability_ref=result.capability_ref)
        handle = await asyncio.to_thread(self.vault.store,
            record.secret_handle.removeprefix('vault:'), result.capability)
        if handle != record.secret_handle:
            raise ConnectorError('OPERATION_CONFLICT', 'capability_reservation',
                                 'The vault returned a different secret reference.')
        await asyncio.to_thread(self._record, record, status='STORED',
            capability_id=result.capability_id, capability_ref=result.capability_ref)
        guard()
        return result
