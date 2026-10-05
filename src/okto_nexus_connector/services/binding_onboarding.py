"""Durable R4 binding proposals and explicit reviewed application."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, fields
import hashlib
import json

from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError
from ..storage.state_store import BindingIntentRecord
from ..transport.https_client import NexusHTTPClient, R4BindingProposal, R4BindingView
from .executor_registration import _identity, _profile, _one
from .execution_selection import acknowledge_execution_binding_state
from .realization_service import _root_digest
from .launch_configuration import _digest as configuration_digest, _home


def _hash(value):
    return "sha256:" + hashlib.sha256(canonical_json(json.loads(json.dumps(value)))).hexdigest()


def _id(value):
    return type(value) is str and 1 <= len(value) <= 160 and not any(c in value for c in "\r\n\x00")


class BindingOnboarding:
    def __init__(self, store, vault, *, http_factory=NexusHTTPClient):
        self.store, self.vault, self.http_factory = store, vault, http_factory

    def _context(self, state, identity_alias, realization_ref):
        selected = _one((r for r in state.identities if r.alias == identity_alias),
                        "Select one imported identity for binding onboarding.")
        identity = _identity(state, selected.server_id, selected.agent_id)
        profile = _profile(state, identity.server_id)
        local = _one((r for r in state.realizations if r.server_id == identity.server_id and
                      r.agent_id == identity.agent_id and r.realization_ref == realization_ref),
                     "The published local realization is missing or ambiguous.")
        executor = _one((r for r in state.execution_executors if r.server_id == local.server_id and
                         r.executor_id == local.executor_id and r.connector_id == state.connector_id),
                        "The registered executor is missing or ambiguous.")
        config = _one((r for r in state.launch_configurations if
                       r.configuration_digest == local.configuration_digest),
                      "The approved launch configuration is missing or ambiguous.")
        if (executor.state != "REGISTERED" or local.status not in ("PENDING_APPROVAL", "BOUND") or
                not local.canonical_workspace_id or not local.workspace_binding_id or
                _root_digest(local, local.root_proof_nonce) != local.local_root_proof_digest or
                configuration_digest(config) != local.configuration_digest or
                any(getattr(config, k) != getattr(local, k) for k in
                    ("server_id", "executor_id", "agent_id", "adapter_id", "local_consent_id")) or
                (config.provider_home, config.home_device, config.home_inode) != _home(config.provider_home)):
            raise ConnectorError("PROFILE_DRIFT", "binding_onboarding",
                                 "The realization or its approved launch configuration changed.")
        physical = asdict(local)
        physical.pop("status")
        registration = asdict(executor)
        registration.pop("inventory_publication_sequence")
        digest = _hash(dict(identity=asdict(identity), profile=asdict(profile),
                            realization=physical, executor=registration))
        return identity, profile, local, digest

    def _record(self, state, alias, intent_id):
        return _one((r for r in state.binding_intents if r.identity_alias == alias and
                     r.client_intent_id == intent_id), "The binding intent is missing or ambiguous.")

    def _current(self, state, record):
        context = self._context(state, record.identity_alias, record.realization_ref)
        if context[3] != record.scope_digest:
            raise ConnectorError("STALE_GENERATION", "binding_onboarding",
                                 "The binding intent authority or local realization changed.")
        if record.replace_binding_id and record.status not in ("APPLIED", "SUPERSEDED"):
            current = _one((b for b in state.execution_bindings if b.server_id == record.server_id
                and b.executor_id == record.executor_id and b.binding_id == record.replace_binding_id),
                "The replacement binding is missing or ambiguous.")
            if asdict(current) != record.replacement_snapshot:
                raise ConnectorError("STALE_GENERATION", "binding_onboarding", "The replacement binding changed after selection.")
        return context

    def _proposal(self, record):
        if record.proposal is None or _hash(record.proposal) != record.proposal_digest:
            raise ConnectorError("PROFILE_DRIFT", "binding_onboarding",
                                 "The stored binding proposal is missing or changed.")
        try:
            value = dict(record.proposal)
            for key in ("fields_changed", "required_approvals"):
                value[key] = tuple(value[key])
            return R4BindingProposal(**value)
        except (KeyError, TypeError, ValueError):
            raise ConnectorError("PROFILE_DRIFT", "binding_onboarding",
                                 "The stored binding proposal is invalid.") from None

    def _view(self, state, record):
        binding = None
        if record.applied_binding is not None:
            binding = dict(record.applied_binding)
        elif record.binding_id:
            saved = _one((b for b in state.execution_bindings if b.server_id == record.server_id and
                          b.executor_id == record.executor_id and b.binding_id == record.binding_id),
                         "The acknowledged binding is missing or ambiguous.")
            binding = {f.name: getattr(saved, f.name) for f in fields(R4BindingView)}
        return dict(alias=record.alias, client_intent_id=record.client_intent_id, state=record.status,
                    proposal=asdict(self._proposal(record)) if record.proposal else None, binding=binding)

    async def prepare(self, *, identity_alias, realization_ref, alias, client_intent_id, replace_binding_id=None,
                      connection_configuration=None):
        if not all(_id(v) for v in (identity_alias, realization_ref, alias, client_intent_id)):
            raise ConnectorError("VALIDATION_ERROR", "binding_onboarding", "Binding identity and intent fields are required.")
        if replace_binding_id is not None and not _id(replace_binding_id):
            raise ConnectorError("VALIDATION_ERROR", "binding_onboarding", "Invalid replacement binding ID.")
        def stage(state):
            identity, profile, local, digest = self._context(state, identity_alias, realization_ref)
            existing = [r for r in state.binding_intents if r.identity_alias == identity_alias and
                        r.client_intent_id == client_intent_id]
            if existing:
                record = _one(existing, "The binding intent is ambiguous.")
                if record.alias != alias or record.realization_ref != realization_ref or record.scope_digest != digest or record.replace_binding_id != replace_binding_id or record.connection_configuration != connection_configuration:
                    raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding",
                                         "The binding client intent has different content.")
            else:
                conflicts = [r for r in state.binding_intents if r.alias == alias and r.status != "SUPERSEDED"]
                replacement = None
                if replace_binding_id is not None:
                    previous = _one(conflicts, "Select the current local alias to replace.")
                    replacement = _one((b for b in state.execution_bindings if
                        b.server_id == local.server_id and b.executor_id == local.executor_id and
                        b.agent_id == local.agent_id and b.binding_id == replace_binding_id),
                        "The replacement binding is missing or ambiguous.")
                    if (previous.status != "APPLIED" or previous.binding_id != replace_binding_id
                            or previous.identity_alias != identity_alias
                            or replacement.workspace_id != local.canonical_workspace_id
                            or replacement.adapter_id != local.adapter_id):
                        raise ConnectorError("SCOPE_MISMATCH", "binding_onboarding", "The replacement must preserve the approved binding scope.")
                if any(r.alias == alias for r in state.bindings) or (conflicts and replacement is None):
                    raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding",
                                         "The local binding alias is already reserved.")
                if len(state.binding_intents) >= 256:
                    raise ConnectorError("CAPACITY_EXCEEDED", "binding_onboarding", "The binding intent capacity is exhausted.")
                state.binding_intents.append(BindingIntentRecord(local.server_id, local.executor_id, local.agent_id,
                    identity_alias, client_intent_id, alias, realization_ref, digest,
                    replace_binding_id=replace_binding_id, replacement_snapshot=asdict(replacement) if replacement else None,
                    connection_configuration=connection_configuration))
        state = await asyncio.to_thread(self.store.update, stage)
        record = self._record(state, identity_alias, client_intent_id)
        if record.proposal is not None:
            return self._view(state, record)
        identity, profile, local, _ = self._current(state, record)
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        self._current(await asyncio.to_thread(self.store.load), record)
        async with self.http_factory(profile.base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "binding_onboarding",
                                     "The credential differs from the selected binding identity.")
            self._current(await asyncio.to_thread(self.store.load), record)
            proposal = await http.prepare_r4_binding(key, client_intent_id=client_intent_id,
                executor_id=local.executor_id, adapter_id=local.adapter_id, candidate_ref=local.candidate_ref,
                inventory_revision=local.inventory_revision, realization_ref=local.realization_ref,
                workspace_id=local.canonical_workspace_id, alias=alias, agent_id_hint=identity.agent_id,
                **({"replace_binding_id": replace_binding_id} if replace_binding_id else {}),
                **({"connection_configuration": record.connection_configuration} if record.connection_configuration is not None else {}))
        def commit(state):
            current = self._record(state, identity_alias, client_intent_id)
            _, _, local, _ = self._current(state, current)
            if (proposal.server_id != local.server_id or proposal.workspace_binding_id != local.workspace_binding_id or
                    proposal.realization_revision != local.realization_revision):
                raise ConnectorError("SCOPE_MISMATCH", "binding_onboarding", "The proposal differs from the local realization.")
            value = asdict(proposal)
            if current.proposal is not None and _hash(value) != current.proposal_digest:
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "The stored proposal differs from the response.")
            current.proposal, current.proposal_digest = value, _hash(value)
            if current.status == "PREPARE_PENDING":
                current.status = "PREPARED"
        state = await asyncio.to_thread(self.store.update, commit)
        return self._view(state, self._record(state, identity_alias, client_intent_id))

    async def apply(self, *, identity_alias, prepare_intent_id, client_intent_id,
                    approved_diff_hash, operator_proof_ref=None):
        if (not all(_id(v) for v in (identity_alias, prepare_intent_id, client_intent_id)) or
                (operator_proof_ref is not None and not _id(operator_proof_ref))):
            raise ConnectorError("VALIDATION_ERROR", "binding_onboarding", "Invalid binding application fields.")
        saved_state = await asyncio.to_thread(self.store.load)
        saved = self._record(saved_state, identity_alias, prepare_intent_id)
        if saved.status == "SUPERSEDED":
            if (saved.apply_client_intent_id != client_intent_id or saved.approved_diff_hash != approved_diff_hash
                    or saved.operator_proof_ref != operator_proof_ref):
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "The historical application intent has different content.")
            return self._view(saved_state, saved)
        def stage(state):
            record = self._record(state, identity_alias, prepare_intent_id)
            self._current(state, record)
            proposal = self._proposal(record)
            delegated = tuple(ref for ref in proposal.required_approvals if ref.startswith("apr_"))
            if delegated and operator_proof_ref not in delegated:
                raise ConnectorError("APPROVAL_REQUIRED", "binding_onboarding",
                                     "Supply the explicit operator proof reference from the reviewed proposal.")
            if approved_diff_hash != proposal.approved_diff_hash:
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "Review the exact proposal before applying it.")
            if record.apply_client_intent_id and (
                    record.apply_client_intent_id != client_intent_id or
                    record.approved_diff_hash != approved_diff_hash or record.operator_proof_ref != operator_proof_ref):
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "The application intent has different content.")
            if any(other is not record and other.server_id == record.server_id and
                   other.agent_id == record.agent_id and other.apply_client_intent_id == client_intent_id
                   for other in state.binding_intents):
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding",
                                     "The application intent already belongs to another proposal.")
            record.apply_client_intent_id, record.approved_diff_hash = client_intent_id, approved_diff_hash
            record.operator_proof_ref = operator_proof_ref
            if record.status != "APPLIED":
                record.status = "APPLY_PENDING"
        state = await asyncio.to_thread(self.store.update, stage)
        record = self._record(state, identity_alias, prepare_intent_id)
        proposal = self._proposal(record)
        identity, profile, _, _ = self._current(state, record)
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        self._current(await asyncio.to_thread(self.store.load), record)
        async with self.http_factory(profile.base_url) as http:
            me = await http.me(key)
            if (me.server_id, me.agent_id) != (identity.server_id, identity.agent_id):
                raise ConnectorError("AGENT_ID_MISMATCH", "binding_onboarding",
                                     "The credential differs from the selected binding identity.")
            self._current(await asyncio.to_thread(self.store.load), record)
            binding = await http.apply_r4_binding(key, client_intent_id=client_intent_id,
                proposal=proposal, operator_proof_ref=operator_proof_ref)
        def commit(state):
            current = self._record(state, identity_alias, prepare_intent_id)
            self._current(state, current)
            if (current.apply_client_intent_id != client_intent_id or current.proposal_digest != record.proposal_digest or
                    current.approved_diff_hash != approved_diff_hash or current.operator_proof_ref != operator_proof_ref):
                raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "The binding application changed before acknowledgment.")
            acknowledged = acknowledge_execution_binding_state(state, binding=binding,
                replace_expected=current.replacement_snapshot)
            if current.replace_binding_id:
                for previous in state.binding_intents:
                    if previous is not current and previous.alias == current.alias and previous.status == "APPLIED":
                        if previous.binding_id != current.replace_binding_id:
                            raise ConnectorError("OPERATION_CONFLICT", "binding_onboarding", "The replaced alias changed.")
                        if previous.applied_binding is None:
                            previous.applied_binding = {f.name: current.replacement_snapshot[f.name] for f in fields(R4BindingView)}
                        previous.status = "SUPERSEDED"
            current.applied_binding = {f.name: getattr(acknowledged, f.name) for f in fields(R4BindingView)}
            current.binding_id, current.status = acknowledged.binding_id, "APPLIED"
        state = await asyncio.to_thread(self.store.update, commit)
        return self._view(state, self._record(state, identity_alias, prepare_intent_id))
