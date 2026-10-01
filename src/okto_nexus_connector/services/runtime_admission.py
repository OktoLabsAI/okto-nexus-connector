"""Durable CLI intent admission; the Server dispatcher owns every effect."""
from __future__ import annotations

import asyncio
from dataclasses import asdict
import json

from ..errors import ConnectorError
from ..storage.state_store import RuntimeIntentRecord, state_to_json
from ..transport.https_client import NexusHTTPClient, R4IntentResolution
from .binding_onboarding import BindingOnboarding, _hash, _id
from .execution_selection import _digest, _matches
from .executor_registration import _one, _identity, _profile


class RuntimeAdmission:
    def __init__(self, store, vault, *, http_factory=NexusHTTPClient):
        self.store, self.vault, self.http_factory = store, vault, http_factory

    def context(self, state, alias):
        intent = _one((r for r in state.binding_intents if r.alias == alias and r.status != "SUPERSEDED"),
                      "Select one acknowledged R4 binding alias.")
        if intent.status != "APPLIED" or any(b.alias == alias for b in state.bindings):
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "runtime_admission",
                                 "The R4 binding is pending or its alias is ambiguous.")
        identity, profile, local, digest = BindingOnboarding(self.store, self.vault)._current(state, intent)
        binding = _one((b for b in state.execution_bindings if b.server_id == intent.server_id and
                        b.executor_id == intent.executor_id and b.binding_id == intent.binding_id),
                       "The acknowledged R4 binding is missing or ambiguous.")
        if (binding.state != "APPROVED" or local.status != "BOUND" or not _matches(local, binding)
                or _digest(local) != binding.realization_snapshot_digest):
            raise ConnectorError("PROFILE_DRIFT", "runtime_admission", "The approved binding mapping changed.")
        return identity, profile, binding, _hash(dict(binding=asdict(binding), authority=digest))

    def _record(self, state, server_id, agent_id, client_intent_id):
        return _one((r for r in state.runtime_intents if
                     (r.server_id, r.agent_id, r.client_intent_id) == (server_id, agent_id, client_intent_id)),
                    "The runtime intent is missing or ambiguous.")

    def _current(self, state, record):
        context = self.context(state, record.alias)
        if context[3] != record.scope_digest:
            raise ConnectorError("STALE_GENERATION", "runtime_admission", "The runtime intent authority changed.")
        return context

    def _resolution(self, record):
        if record.resolution is None or _hash(record.resolution) != record.resolution_digest:
            raise ConnectorError("PROFILE_DRIFT", "runtime_admission", "The stored resolution changed.")
        value = dict(record.resolution)
        value["blockers"] = tuple(value["blockers"])
        return R4IntentResolution(**value)

    def _view(self, record):
        resolution = self._resolution(record) if record.resolution is not None else None
        return dict(alias=record.alias, client_intent_id=record.client_intent_id, state=record.status,
                    operation_id=resolution.operation_id if resolution else None,
                    session_id=resolution.session_id if resolution else None,
                    blockers=list(resolution.blockers) if resolution else [],
                    operation=record.operation, reused=resolution.reuse if resolution else False)

    async def execute(self, *, alias, client_intent_id, intent, session_id=None,
                      new_session=None, text=None, target=None):
        if not _id(alias) or not _id(client_intent_id):
            raise ConnectorError("VALIDATION_ERROR", "runtime_admission",
                                 "An alias and explicit stable client intent ID are required.")
        if intent not in {"runtime.start", "turn.submit", "turn.steer", "turn.interrupt", "runtime.close"}:
            raise ConnectorError("VALIDATION_ERROR", "runtime_admission", "Unsupported runtime intent.")
        if intent == "runtime.start":
            if ((new_session is not None and type(new_session) is not bool) or (text is not None and (type(text) is not str or not 1 <= len(text) <= 65536))
                    or (new_session is True and session_id is not None)
                    or (session_id is not None and not _id(session_id))):
                raise ConnectorError("VALIDATION_ERROR", "runtime_admission",
                                     "Select a new or existing session with valid optional turn text.")
        elif not _id(session_id) or new_session is True:
            raise ConnectorError("VALIDATION_ERROR", "runtime_admission", "An existing session ID is required.")
        if intent in {"turn.submit", "turn.steer"} and (type(text) is not str or not 1 <= len(text) <= 65536):
            raise ConnectorError("VALIDATION_ERROR", "runtime_admission", "Turn text is required.")
        if intent in {"turn.interrupt", "runtime.close"} and text is not None and (
                type(text) is not str or len(text) > 1024):
            raise ConnectorError("VALIDATION_ERROR", "runtime_admission", "The control reason is too long.")
        request = dict(client_intent_id=client_intent_id, intent=intent)
        for name, value in dict(session_id=session_id, new_session=new_session, text=text, target=target).items():
            if value is not None:
                request[name] = value
        def stage(state):
            identity, _, binding, digest = self.context(state, alias)
            request.update(binding_id=binding.binding_id, workspace_binding_id=binding.workspace_binding_id)
            wanted = _hash(request)
            existing = [r for r in state.runtime_intents if
                        (r.server_id, r.agent_id, r.client_intent_id) ==
                        (identity.server_id, identity.agent_id, client_intent_id)]
            if existing:
                record = _one(existing, "The runtime intent is ambiguous.")
                if (record.request_digest, record.scope_digest, record.alias) != (wanted, digest, alias):
                    raise ConnectorError("OPERATION_CONFLICT", "runtime_admission",
                                         "The runtime client intent has different content or authority.")
            else:
                if len(state.runtime_intents) >= 32:
                    raise ConnectorError("CAPACITY_EXCEEDED", "runtime_admission",
                                         "The retained runtime intent capacity is exhausted.")
                state.runtime_intents.append(RuntimeIntentRecord(identity.server_id, identity.agent_id,
                    alias, client_intent_id, wanted, digest))
        state = await asyncio.to_thread(self.store.update, stage)
        identity, profile, _, _ = self.context(state, alias)
        record = self._record(state, identity.server_id, identity.agent_id, client_intent_id)
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        self._current(await asyncio.to_thread(self.store.load), record)
        async with self.http_factory(profile.base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "runtime_admission",
                                     "The credential differs from the runtime identity.")
            self._current(await asyncio.to_thread(self.store.load), record)
            if record.resolution is None:
                resolution = await http.resolve_r4_intent(key, **request)
                def save_resolution(state):
                    current = self._record(state, identity.server_id, identity.agent_id, client_intent_id)
                    _, _, binding, _ = self._current(state, current)
                    if any(resolution.scope.get(k) != getattr(binding, k) for k in
                           ("server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                            "workspace_binding_id", "configuration_revision", "authorization_revision", "binding_revision")):
                        raise ConnectorError("SCOPE_MISMATCH", "runtime_admission",
                                             "The resolution differs from the acknowledged binding.")
                    value = asdict(resolution)
                    if current.resolution is not None and current.resolution_digest != _hash(value):
                        raise ConnectorError("OPERATION_CONFLICT", "runtime_admission", "The resolution changed.")
                    current.resolution, current.resolution_digest = value, _hash(value)
                    if len(json.dumps(state_to_json(state)).encode("utf-8")) > 3 * 1024 * 1024:
                        raise ConnectorError("CAPACITY_EXCEEDED", "runtime_admission",
                                             "The retained runtime intent storage is full.")
                    if current.status == "RESOLVE_PENDING":
                        current.status = "RESOLVED"
                state = await asyncio.to_thread(self.store.update, save_resolution)
                record = self._record(state, identity.server_id, identity.agent_id, client_intent_id)
            resolution = self._resolution(record)
            if not resolution.can_submit or resolution.blockers:
                return self._view(record)
            def stage_admission(state):
                current = self._record(state, identity.server_id, identity.agent_id, client_intent_id)
                self._current(state, current)
                self._resolution(current)
                if current.status != "ADMITTED":
                    current.status = "ADMISSION_PENDING"
            state = await asyncio.to_thread(self.store.update, stage_admission)
            # An identical admission is idempotent on the Server. It never
            # invokes the legacy runtime manager or creates a local Core host.
            operation = await http.submit_r4_operation(key, resolution)
            try:
                state = await self._save_operation(record, operation)
            except ConnectorError as error:
                error.possible_effect = True
                error.retry_safe = False
                error.operation_id = resolution.operation_id
                raise
            return self._view(self._record(state, identity.server_id, identity.agent_id, client_intent_id))

    def _read_authority(self, state, record):
        identity = _identity(state, record.server_id, record.agent_id)
        profile = _profile(state, record.server_id)
        return identity, profile, _hash(dict(identity=asdict(identity), profile=asdict(profile)))

    def _read_current(self, state, record, authority_digest):
        current = self._record(state, record.server_id, record.agent_id, record.client_intent_id)
        if ((current.alias, current.request_digest, current.resolution_digest) !=
                (record.alias, record.request_digest, record.resolution_digest) or
                self._read_authority(state, current)[2] != authority_digest):
            raise ConnectorError("STALE_GENERATION", "runtime_read",
                                 "The operation query identity or retained intent changed.")
        return current

    async def _save_operation(self, record, operation, *, read_authority=None):
        def save(state):
            current = self._record(state, record.server_id, record.agent_id, record.client_intent_id)
            if read_authority is None:
                self._current(state, current)
            else:
                self._read_current(state, record, read_authority)
            resolution = self._resolution(current)
            if (operation.get("operation_id") != resolution.operation_id or
                    (not resolution.reuse and operation.get("client_intent_id") != resolution.client_intent_id) or
                    operation.get("intent_hash") != resolution.intent_hash or
                    operation.get("action") != resolution.semantic_intent["action"] or
                    any(operation.get("scope", {}).get(k) != resolution.scope.get(k) for k in
                        ("server_id", "executor_id", "binding_id", "agent_id", "workspace_id", "session_id"))):
                raise ConnectorError("SCOPE_MISMATCH", "runtime_admission",
                                     "The operation differs from the reserved intent.",
                                     possible_effect=True, operation_id=resolution.operation_id)
            # Concurrent polling must not regress a receipt.
            previous = current.operation
            revision = operation.get("receipt_revision")
            if type(revision) is not int or revision < 0:
                raise ConnectorError("VERSION_INCOMPATIBLE", "runtime_admission", "Invalid receipt revision.",
                                     possible_effect=True, operation_id=resolution.operation_id)
            if previous is None or revision >= previous["receipt_revision"]:
                current.operation = operation
            current.status = "ADMITTED"
        return await asyncio.to_thread(self.store.update, save)

    async def inspect(self, *, alias, client_intent_id):
        if not _id(alias) or not _id(client_intent_id):
            raise ConnectorError("VALIDATION_ERROR", "runtime_read",
                                 "An alias and client intent ID are required.")
        state = await asyncio.to_thread(self.store.load)
        record = _one((r for r in state.runtime_intents if
                       r.alias == alias and r.client_intent_id == client_intent_id),
                      "The retained runtime intent is missing or ambiguous.")
        # Historical reads require a current credential for the original
        # canonical agent. Physical launch evidence is not read authority.
        identity, profile, authority = self._read_authority(state, record)
        if record.resolution is None:
            return self._view(record)
        resolution = self._resolution(record)
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        self._read_current(await asyncio.to_thread(self.store.load), record, authority)
        async with self.http_factory(profile.base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "runtime_read", "The credential identity changed.")
            self._read_current(await asyncio.to_thread(self.store.load), record, authority)
            try:
                operation = await http.get_r4_operation(key, resolution.operation_id)
            except ConnectorError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                current = self._read_current(await asyncio.to_thread(self.store.load), record, authority)
                # Absence is an observation, never permission to replace the ID.
                return self._view(current) | {"operation_found": False}
            state = await self._save_operation(record, operation, read_authority=authority)
        return self._view(self._record(state, identity.server_id, identity.agent_id, client_intent_id))

    async def inspect_session(self, *, alias, session_id):
        if not _id(alias) or not _id(session_id):
            raise ConnectorError("VALIDATION_ERROR", "session_read", "An alias and session ID are required.")
        state = await asyncio.to_thread(self.store.load)
        records = [r for r in state.runtime_intents if r.alias == alias and r.resolution is not None
                   and r.resolution.get("session_id") == session_id]
        if not records:
            raise ConnectorError("NOT_FOUND", "session_read", "No retained session exists for this alias.")
        names = ("server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                 "workspace_binding_id", "session_id")
        scopes = {tuple(self._resolution(r).scope.get(name) for name in names) for r in records}
        if len(scopes) != 1:
            raise ConnectorError("OPERATION_CONFLICT", "session_read", "The retained session scope is ambiguous.")
        record = records[0]
        resolution = self._resolution(record)
        identity, profile, authority = self._read_authority(state, record)
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        self._read_current(await asyncio.to_thread(self.store.load), record, authority)
        async with self.http_factory(profile.base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "session_read", "The credential identity changed.")
            self._read_current(await asyncio.to_thread(self.store.load), record, authority)
            view = await http.get_r4_session(key, session_id=session_id,
                                            executor_id=resolution.scope["executor_id"])
            self._read_current(await asyncio.to_thread(self.store.load), record, authority)
            if any(view.get("scope", {}).get(name) != resolution.scope.get(name) for name in names):
                raise ConnectorError("SCOPE_MISMATCH", "session_read", "The session differs from its retained scope.")
            return view

    async def session_status(self, *, alias):
        if not _id(alias):
            raise ConnectorError("VALIDATION_ERROR", "session_read", "Select a binding alias.")
        state = await asyncio.to_thread(self.store.load)
        records = [r for r in state.runtime_intents if r.alias == alias and r.resolution is not None]
        guards = [(record, self._read_authority(state, record)[2]) for record in records]
        sessions = sorted({self._resolution(r).session_id for r in records})
        views = [await self.inspect_session(alias=alias, session_id=session) for session in sessions]
        current = await asyncio.to_thread(self.store.load)
        for record, authority in guards:
            self._read_current(current, record, authority)
        return {"sessions": views}
