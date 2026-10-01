"""Recoverable executor registration using one explicitly selected identity."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
import secrets
import time

from ..errors import ConnectorError
from ..storage.state_store import ExecutionExecutorRecord
from ..transport.https_client import NexusHTTPClient, origin_of


@dataclass(frozen=True, slots=True)
class ExecutorBootstrap:
    executor: ExecutionExecutorRecord
    ticket: str = field(repr=False)
    deadline_monotonic: float
    credential_epoch: int
    authorization_revision: int


def _one(items, message):
    items = list(items)
    if len(items) != 1:
        raise ConnectorError('BINDING_NOT_AUTHORIZED', 'executor_registration', message)
    return items[0]


def _identity(state, server_id, agent_id):
    record = _one((i for i in state.identities if i.server_id == server_id and i.agent_id == agent_id),
                  'The executor registration identity is missing or ambiguous.')
    if record.revoked:
        raise ConnectorError('AGENT_AUTH_REQUIRED', 'executor_registration',
                             'The executor registration identity is revoked.')
    return record


def _profile(state, server_id):
    profile = state.servers.get(server_id)
    if (profile is None or profile.server_id != server_id or
            origin_of(profile.base_url) != profile.origin):
        raise ConnectorError('PROFILE_DRIFT', 'executor_registration',
                             'The approved Server profile is missing or has changed.')
    return profile


class ExecutorRegistrationService:
    def __init__(self, store, vault, *, http_factory=NexusHTTPClient, clock=time.monotonic):
        self.store, self.vault, self.http_factory, self.clock = store, vault, http_factory, clock

    async def register(self, *, identity_alias: str, label: str,
                       client_intent_id: str | None = None,
                       control_capabilities: tuple[str, ...] = ()) -> ExecutionExecutorRecord:
        """Persist the request before HTTP; repeat the same intent after loss."""
        attempt = await asyncio.to_thread(self._stage, identity_alias, label,
                                          client_intent_id, control_capabilities)
        return (await self._exchange(*attempt)).executor

    def _stage(self, alias, label, intent_id, capabilities):
        if (type(alias) is not str or not alias or type(label) is not str or len(label) > 120 or
                (intent_id is not None and (type(intent_id) is not str or not 1 <= len(intent_id) <= 160)) or
                type(capabilities) is not tuple or len(capabilities) > 64 or
                any(type(c) is not str or not 1 <= len(c) <= 160 for c in capabilities) or
                len(set(capabilities)) != len(capabilities)):
            raise ConnectorError('VALIDATION_ERROR', 'executor_registration',
                                 'The executor registration request is invalid.')
        result = None
        def stage(state):
            nonlocal result
            identity = _one((i for i in state.identities if i.alias == alias),
                            'Select one imported identity for executor registration.')
            identity = _identity(state, identity.server_id, identity.agent_id)
            profile = _profile(state, identity.server_id)
            if type(state.connector_id) is not str or not 1 <= len(state.connector_id) <= 160:
                raise ConnectorError('VALIDATION_ERROR', 'executor_registration',
                                     'Import an identity before registering this Connector.')
            records = [r for r in state.execution_executors if r.server_id == identity.server_id]
            if records:
                record = _one(records, 'The executor registration is ambiguous.')
                if (record.connector_id != state.connector_id or record.registration_agent_id != identity.agent_id or
                        record.label != label or record.control_capabilities != capabilities or
                        (intent_id is not None and record.client_intent_id != intent_id) or
                        record.state not in ('REGISTRATION_PENDING', 'REGISTERED')):
                    raise ConnectorError('OPERATION_CONFLICT', 'executor_registration',
                                         'The persisted executor registration has different content.')
            else:
                if len(state.execution_executors) >= 32:
                    raise ConnectorError('CAPACITY_EXCEEDED', 'executor_registration',
                                         'The executor registration capacity is exhausted.')
                record = ExecutionExecutorRecord(identity.server_id, state.connector_id, identity.agent_id,
                    intent_id or 'register_' + secrets.token_hex(16), label, capabilities)
                state.execution_executors.append(record)
            result = (replace(record), replace(identity), replace(profile))
        self.store.update(stage)
        return result

    async def bootstrap(self, *, server_id: str) -> ExecutorBootstrap:
        """Mint a fresh derivative by replaying the committed registration.

        The secret remains process-local. A restart obtains a new ticket, not
        plaintext reconstructed from a hash or a persisted wall-clock TTL.
        """
        state = await asyncio.to_thread(self.store.load)
        record = _one((r for r in state.execution_executors if r.server_id == server_id),
                      'The executor registration is missing or ambiguous.')
        if record.state not in ('REGISTRATION_PENDING', 'REGISTERED') or record.connector_id != state.connector_id:
            raise ConnectorError('OPERATION_CONFLICT', 'executor_registration',
                                 'The persisted executor registration has changed.')
        return await self._exchange(record, _identity(state, server_id, record.registration_agent_id),
                                    _profile(state, server_id))

    def _require_current(self, state, record, identity, profile):
        current = _one((r for r in state.execution_executors if r.server_id == record.server_id),
                       'The executor registration is missing or ambiguous.')
        if (state.connector_id != record.connector_id or
                _identity(state, record.server_id, record.registration_agent_id) != identity or
                _profile(state, record.server_id) != profile or
                replace(current, state=record.state, executor_id=record.executor_id,
                        inventory_publication_sequence=record.inventory_publication_sequence) != record or
                current.state not in ('REGISTRATION_PENDING', 'REGISTERED') or
                (record.executor_id and current.executor_id != record.executor_id)):
            raise ConnectorError('OPERATION_CONFLICT', 'executor_registration',
                                 'The registration or its local identity changed during the request.')
        return current

    async def _exchange(self, record, identity, profile):
        # Validate origin/TLS before loading or sending credentials.
        async with self.http_factory(profile.base_url) as http:
            key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
            self._require_current(await asyncio.to_thread(self.store.load), record, identity, profile)
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (record.server_id, record.registration_agent_id):
                raise ConnectorError('AGENT_ID_MISMATCH', 'executor_registration',
                                     'The credential does not match the selected registration identity.')
            self._require_current(await asyncio.to_thread(self.store.load), record, identity, profile)
            sent_at = self.clock()
            registered = await http.register_executor(key, client_intent_id=record.client_intent_id,
                connector_id=record.connector_id, label=record.label,
                control_capabilities=record.control_capabilities)
            if ((registered.server_id, registered.connector_id, registered.registration_agent_id) !=
                    (record.server_id, record.connector_id, record.registration_agent_id) or
                    registered.credential_epoch != me.credential_epoch or
                    registered.authorization_revision != me.authorization_revision or
                    not registered.executor_id or
                    (record.executor_id and registered.executor_id != record.executor_id)):
                raise ConnectorError('SCOPE_MISMATCH', 'executor_registration',
                                     'The executor registration response changed scope or authority.')
            result = None
            def commit(state):
                nonlocal result
                current = self._require_current(state, record, identity, profile)
                if current.executor_id and current.executor_id != registered.executor_id:
                    raise ConnectorError('OPERATION_CONFLICT', 'executor_registration',
                                         'Another executor result already owns this registration.')
                current.executor_id, current.state = registered.executor_id, 'REGISTERED'
                result = replace(current)
            await asyncio.to_thread(self.store.update, commit)
            deadline = sent_at + registered.ticket_expires_in - 0.5
            if self.clock() >= deadline:
                raise ConnectorError('CONTROL_DISCONNECTED', 'executor_registration',
                                     'The bootstrap ticket expired before local registration completed.')
            return ExecutorBootstrap(result, registered.bootstrap_ticket, deadline,
                                     registered.credential_epoch, registered.authorization_revision)
