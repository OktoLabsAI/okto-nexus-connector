"""Public executor onboarding with durable local evidence before HTTP publication."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from pathlib import Path
import time

from ..errors import ConnectorError
from ..transport.https_client import NexusHTTPClient
from .discovery_configuration import configuration_arguments, configured_candidates
from .discovery_service import known_adapter
from .executor_registration import _identity, _one, _profile
from .launch_configuration import stage_launch_configuration, _digest, _home
from .realization_service import stage_local_realization, publication_body, acknowledge_local_realization


class ExecutorOnboarding:
    def __init__(self, store, vault=None, *, http_factory=NexusHTTPClient, clock=time.monotonic):
        self.store, self.vault = store, vault
        self.http_factory, self.clock = http_factory, clock

    def _scope(self, alias):
        state = self.store.load()
        selected = _one((r for r in state.identities if r.alias == alias),
                        "Select one imported identity for executor onboarding.")
        identity = _identity(state, selected.server_id, selected.agent_id)
        record = _one((r for r in state.execution_executors if r.server_id == identity.server_id),
                      "The Server executor registration is missing or ambiguous.")
        if (record.state != "REGISTERED" or not record.executor_id or
                record.connector_id != state.connector_id):
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "executor_onboarding",
                                 "Register the Server executor before onboarding a workspace.")
        configuration_arguments(record.discovery_configuration)
        return replace(record, inventory_publication_sequence=0), identity, _profile(state, identity.server_id)

    def _require(self, alias, scope):
        if self._scope(alias) != scope:
            raise ConnectorError("STALE_GENERATION", "executor_onboarding",
                                 "The executor, discovery configuration or identity changed.")

    async def configure_launch(self, *, identity_alias, adapter_id, local_consent_id,
                               profile_revision, secret_bindings=None, provider_home=None):
        if not known_adapter(adapter_id):
            raise ConnectorError("CAPABILITY_UNSUPPORTED", "executor_onboarding",
                                 "Select an adapter from the Core catalog.")
        scope = await asyncio.to_thread(self._scope, identity_alias)
        record, identity, _ = scope
        configuration = await asyncio.to_thread(stage_launch_configuration, self.store,
            server_id=record.server_id, executor_id=record.executor_id, agent_id=identity.agent_id,
            local_consent_id=local_consent_id, adapter_id=adapter_id,
            profile_revision=profile_revision, secret_bindings=secret_bindings, provider_home=provider_home)
        await asyncio.to_thread(self._require, identity_alias, scope)
        return configuration

    def _configuration(self, scope, digest, adapter_id):
        record, identity, _ = scope
        configuration = _one((r for r in self.store.load().launch_configurations
                              if r.configuration_digest == digest),
                             "The selected launch configuration is missing or ambiguous.")
        if (configuration.configuration_digest != _digest(configuration) or
                (configuration.server_id, configuration.executor_id, configuration.agent_id,
                 configuration.adapter_id) !=
                (record.server_id, record.executor_id, identity.agent_id, adapter_id) or
                (configuration.provider_home, configuration.home_device, configuration.home_inode) !=
                _home(configuration.provider_home)):
            raise ConnectorError("PROFILE_DRIFT", "executor_onboarding",
                                 "The selected launch configuration changed or belongs to another scope.")
        return configuration

    async def realize(self, *, identity_alias, client_intent_id, adapter_id,
                      candidate_ref, inventory_revision, configuration_digest,
                      workspace_root, workspace_id, workspace_label, bootstrap, require_current=None):
        scope = await asyncio.to_thread(self._scope, identity_alias)
        record, identity, profile = scope
        if identity.agent_id != record.registration_agent_id:
            raise ConnectorError("BINDING_NOT_AUTHORIZED", "executor_onboarding",
                                 "Bootstrap realization requires the executor registration identity.")
        configuration = await asyncio.to_thread(self._configuration, scope, configuration_digest, adapter_id)
        candidates = tuple(await configured_candidates(record.discovery_configuration,
                           observations=record.installation_observations))
        await asyncio.to_thread(self._require, identity_alias, scope)
        # Staging validates the exact inventory, physical candidate/root, stable
        # client intent and consent before any credential access or HTTP call.
        local = await asyncio.to_thread(stage_local_realization, self.store,
            server_id=record.server_id, executor_id=record.executor_id, agent_id=identity.agent_id,
            client_intent_id=client_intent_id, candidates=candidates, adapter_id=adapter_id,
            candidate_ref=candidate_ref, inventory_revision=inventory_revision,
            workspace_root=Path(workspace_root), workspace_id=workspace_id,
            workspace_label=workspace_label, configuration_digest=configuration_digest,
            local_consent_id=configuration.local_consent_id)
        if (bootstrap.executor.server_id, bootstrap.executor.executor_id,
                bootstrap.executor.registration_agent_id) != (record.server_id, record.executor_id, identity.agent_id):
            raise ConnectorError("SCOPE_MISMATCH", "executor_onboarding",
                                 "The daemon bootstrap differs from the realization scope.")
        async def current():
            if require_current is not None:
                await require_current()
            await asyncio.to_thread(self._require, identity_alias, scope)
            if self.clock() >= bootstrap.deadline_monotonic:
                raise ConnectorError("CONTROL_DISCONNECTED", "executor_onboarding",
                                     "The executor bootstrap ticket expired.")
            if await asyncio.to_thread(self._configuration, scope, configuration_digest, adapter_id) != configuration:
                raise ConnectorError("PROFILE_DRIFT", "executor_onboarding",
                                     "The launch configuration changed during publication.")
        await current()
        key = await asyncio.to_thread(self.vault.resolve, identity.secret_handle)
        async with self.http_factory(profile.base_url) as http:
            await current()
            view = await http.read_r4_inventory(key, server_id=record.server_id, executor_id=record.executor_id)
            if view["snapshot"]["inventory_revision"] != inventory_revision:
                raise ConnectorError("STALE_GENERATION", "executor_onboarding",
                    "The daemon inventory differs from the selected revision. Refresh discovery and reselect.")
            await current()
            # Recheck candidate/root and durable intent after the authenticated inventory read.
            refreshed = await asyncio.to_thread(stage_local_realization, self.store,
                server_id=record.server_id, executor_id=record.executor_id, agent_id=identity.agent_id,
                client_intent_id=client_intent_id, candidates=candidates, adapter_id=adapter_id,
                candidate_ref=candidate_ref, inventory_revision=inventory_revision,
                workspace_root=Path(workspace_root), workspace_id=workspace_id,
                workspace_label=workspace_label, configuration_digest=configuration_digest,
                local_consent_id=configuration.local_consent_id)
            if publication_body(refreshed) != publication_body(local):
                raise ConnectorError("STALE_GENERATION", "executor_onboarding",
                                     "The local realization changed before publication.")
            published = await http.publish_r4_realization(bootstrap.ticket,
                executor_id=record.executor_id, request=publication_body(local))
            await current()
            await asyncio.to_thread(acknowledge_local_realization, self.store,
                                    record=local, published=published)
        # Only the path-free acknowledged projection is returned.
        return asdict(published)
