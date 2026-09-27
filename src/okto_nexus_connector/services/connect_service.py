"""Guided connect flow: identity → discovery → selection → binding (C03).

The operator's intent drives one aggregated confirmation covering agent,
harness, host, project and limits before any technical configuration is
generated (plan 2.1). The flow is idempotent: repeating it reuses the
imported identity and binding without new remote state (TC-12). Progress
persists locally so a partially completed onboarding can be resumed without
duplicating endpoints or bindings (C03.4).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from ..errors import ConnectorError
from ..identity.import_flow import ImportResult
from ..storage.state_store import BindingRecord, StateStore
from ..transport.https_client import NexusHTTPClient
from .discovery_service import InventoryEntry, select_explicit

_ADAPTER_HOME_KEYS = {
    "codex_app_server": "provider_home.codex",
    "pi_rpc": "provider_home.pi",
    "claude_stream": "provider_home.claude",
}


@dataclass(slots=True)
class ConnectChoices:
    """Operator decisions gathered before any technical write."""

    adapter_id: str
    executable: str
    pi_node: str | None = None
    trusted_provider_home: bool = False
    binding_alias: str | None = None


@dataclass(slots=True)
class ConnectSummary:
    identity: ImportResult
    binding: BindingRecord
    created: bool
    harness_version: str | None
    confirmation: dict[str, object]

    def to_json(self) -> dict[str, object]:
        return {
            "agent": {
                "agent_id": self.identity.me_agent_id,
                "server_id": self.identity.server_id,
                "display_name": self.identity.me_agent_id,
                "alias": self.identity.identity.alias,
            },
            "binding": {
                "binding_id": self.binding.binding_id,
                "alias": self.binding.alias,
                "adapter_id": self.binding.adapter_id,
                "workspace_root": self.binding.workspace_root,
                "endpoint_id": self.binding.endpoint_id,
                "profile_id": self.binding.profile_id,
            },
            "created": self.created,
            "harness_version": self.harness_version,
            "confirmation": self.confirmation,
        }


def aggregated_confirmation(*, server_url: str, me_agent_id: str,
                            identity_alias: str, adapter_id: str,
                            executable: str, version: str | None,
                            workspace_root: str,
                            limits: dict[str, object] | None = None
                            ) -> dict[str, object]:
    """The single aggregated consent view required by plan 2.1."""
    return {
        "nexus": {"server_url": server_url, "agent_id": me_agent_id,
                  "identity_alias": identity_alias},
        "harness": {"adapter_id": adapter_id, "executable": executable,
                    "version": version},
        "project": {"workspace_root": workspace_root},
        "credential": {"storage": "local vault (referenced, never in "
                       "project manifests)", "provider_login": "kept on "
                       "this host; never sent to the Server"},
        "limits": limits or {"lease_seconds": 120, "idle_policy":
                             "keep reusable while host is active"},
    }


async def create_binding(http: NexusHTTPClient, store: StateStore,
                          *, identity: ImportResult, key: str,
                          alias: str, adapter_id: str, candidate,
                          version: str | None,
                          workspace_root: Path,
                          server_url: str) -> ConnectSummary:
    """Prepare/apply the binding on the Server and persist it locally."""
    state = store.load()
    connector_id = state.connector_id
    workspace_str = str(workspace_root.resolve())
    existing = [b for b in state.bindings
                if b.server_id == identity.server_id
                and b.agent_id == identity.me_agent_id
                and b.adapter_id == adapter_id
                and b.workspace_root == workspace_str]
    if existing and existing[0].alias == alias:
        return ConnectSummary(identity, existing[0], False, version,
                              aggregated_confirmation(
                                  server_url=server_url,
                                  me_agent_id=identity.me_agent_id,
                                  identity_alias=identity.identity.alias,
                                  adapter_id=adapter_id,
                                  executable=candidate.executable,
                                  version=version,
                                  workspace_root=workspace_str))
    if any(b.alias == alias for b in state.bindings):
        raise ConnectorError("AMBIGUOUS_BINDING", "bind",
                             f"binding alias {alias!r} already exists",
                             action="Choose another alias with "
                                    "--binding-alias.")

    proposal = await http.prepare_binding(
        key, agent_id_hint=identity.me_agent_id, connector_id=connector_id,
        adapter_id=adapter_id, candidate_version=version or "unknown",
        binding_alias=alias,
        workspace_hint={"workspace_root": workspace_str})
    applied = await http.apply_binding(key, proposal)

    binding = BindingRecord(
        binding_id=applied.binding_id,
        alias=alias,
        server_id=identity.server_id,
        agent_id=identity.me_agent_id,
        adapter_id=adapter_id,
        executor_id=connector_id,
        workspace_id=applied.workspace_id,
        workspace_root=workspace_str,
        endpoint_id=applied.workspace_binding_id,
        profile_id=applied.profile_id,
        authorization_revision=applied.authorization_revision,
        configuration_revision=applied.configuration_revision,
        candidate_executable=candidate.executable,
        candidate_fingerprint=candidate.fingerprint,
        candidate_version=version or "",
        candidate_build_identity=candidate.build_identity or "",
        candidate_launch_script=candidate.launch_script or "",
        created_at=_now(),
    )

    def _mutate(state_):
        if state_.binding_by_alias(alias) is None:
            state_.bindings.append(binding)
            key = _ADAPTER_HOME_KEYS.get(adapter_id)
            if key is not None:
                state_.preferences[f"binding.{binding.binding_id}"
                                   f".trusted_provider_home"] = False

    store.update(_mutate)
    return ConnectSummary(
        identity, binding, True, version,
        aggregated_confirmation(
            server_url=server_url, me_agent_id=identity.me_agent_id,
            identity_alias=identity.identity.alias, adapter_id=adapter_id,
            executable=candidate.executable, version=version,
            workspace_root=workspace_str))


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
