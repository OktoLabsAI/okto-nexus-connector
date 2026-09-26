"""Direct-HTTP MCP client configuration for harnesses (plan C06).

The Connector configures the harness's *own* MCP HTTP client to reach the
Nexus Server directly. It never proxies, terminates, tunnels or translates
MCP; there is no stdio fallback. Harnesses without an MCP HTTP client get
an explicit ``CAPABILITY_UNSUPPORTED`` diagnostic; Pi instead receives the
Core's non-MCP native-action bridge configuration (C06.4).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nexus_connector_core import CoreError
from nexus_connector_core.config_document import (
    plan_codex_toml_entry, plan_json_entry,
)
from nexus_connector_core.config_persistence import (
    apply_json_plan_file, apply_toml_plan_file,
)
from nexus_connector_core.harness_config import (
    HarnessHTTPTemplate, harness_http_template, render_codex_toml_fragment,
)

from ..errors import ConnectorError

HTTP_MCP_ADAPTERS = frozenset({"codex_app_server", "claude_stream"})
NATIVE_BRIDGE_ADAPTERS = frozenset({"pi_rpc"})


@dataclass(slots=True)
class MCPDirectPlan:
    adapter_id: str
    entry_name: str
    server_url: str
    bearer_env_name: str
    capability_ref: str
    persistent_file: Path | None
    preview: str

    def to_json(self, *, include_ref: bool = False) -> dict[str, object]:
        payload: dict[str, object] = {
            "adapter_id": self.adapter_id,
            "entry_name": self.entry_name,
            "server_url": self.server_url,
            "bearer_env_name": self.bearer_env_name,
            "persistent_file": str(self.persistent_file)
            if self.persistent_file else None,
            "transport": "direct-http (harness → Nexus Server; the "
                         "connector is not in this path)",
            "preview": self.preview,
        }
        if include_ref:
            payload["capability_ref"] = self.capability_ref
        return payload


def plan_direct_entry(*, adapter_id: str, server_url: str,
                      capability_ref: str, entry_name: str = "nexus",
                      approved_origins=None,
                      harness_is_local: bool = False,
                      loopback_reachable: bool = False) -> MCPDirectPlan:
    """Compose the declarative client entry via the Core templates."""
    try:
        return _plan_direct_entry(adapter_id, server_url, capability_ref,
                                  entry_name, approved_origins,
                                  harness_is_local, loopback_reachable)
    except CoreError as error:
        raise ConnectorError(error.code, error.stage, str(error),
                             possible_effect=error.possible_effect,
                             retry_safe=error.retry_safe) from None


def _plan_direct_entry(adapter_id, server_url, capability_ref, entry_name,
                       approved_origins, harness_is_local,
                       loopback_reachable) -> MCPDirectPlan:
    if adapter_id in NATIVE_BRIDGE_ADAPTERS:
        raise ConnectorError(
            "CAPABILITY_UNSUPPORTED", "mcp_client_configuration",
            "Pi has no built-in MCP HTTP client in the qualified build",
            action="Use the native non-MCP bridge configuration instead; "
                   "there is no stdio fallback.")
    if adapter_id not in HTTP_MCP_ADAPTERS:
        raise ConnectorError("CAPABILITY_UNSUPPORTED", "mcp_client_configuration",
                             f"adapter {adapter_id} has no MCP HTTP client")
    template = harness_http_template(
        adapter_id, server_url, capability_ref, entry_name=entry_name,
        harness_is_local=harness_is_local,
        approved_origins=approved_origins,
        loopback_reachable=loopback_reachable,
        format_qualified=True)
    preview = (render_codex_toml_fragment(template)
               if adapter_id == "codex_app_server"
               else _render_claude_json(template))
    return MCPDirectPlan(adapter_id, entry_name, server_url,
                         template.bearer_env_name, capability_ref, None,
                         preview)


def _render_claude_json(template: HarnessHTTPTemplate) -> str:
    import json
    document = {"mcpServers": {template.entry_name: template.entry()}}
    return json.dumps(document, indent=2)


def apply_persistent_entry(plan: MCPDirectPlan, target: Path, *,
                           previously_owned: dict | None = None
                           ) -> dict[str, object]:
    """Plan/apply the entry into the harness's config file with backup+CAS.

    Only the product-owned entry is touched; third-party entries survive
    (TC-13/TC-39). ``previously_owned`` is the exact entry content this
    product last wrote (from trusted local state), never inferred from
    the edited document.
    """
    template = harness_http_template(
        plan.adapter_id, plan.server_url, plan.capability_ref,
        entry_name=plan.entry_name, harness_is_local=True,
        approved_origins={_origin(plan.server_url)},
        loopback_reachable=_is_loopback(plan.server_url),
        format_qualified=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    result = _apply_entry(target, template, previously_owned)
    return {
        "changed": result.changed,
        "backup_path": str(result.backup_path) if result.backup_path else None,
        "bytes_written": result.bytes_written,
        "file": str(target),
        "owned_entry": template.entry(),
        "note": "other entries preserved; the harness client now talks to "
                "the Nexus Server directly",
    }


def _apply_entry(target: Path, template: HarnessHTTPTemplate,
                 previously_owned: dict | None):
    existing = target.read_bytes() if target.exists() else None
    try:
        if template.adapter_id == "codex_app_server":
            plan = plan_codex_toml_entry(
                existing, template,
                previously_owned=previously_owned)
            return apply_toml_plan_file(plan, target)
        plan = plan_json_entry(
            existing, section=template.section,
            entry_name=template.entry_name, proposed=template.entry(),
            previously_owned=previously_owned)
        return apply_json_plan_file(plan, target)
    except CoreError as error:
        raise ConnectorError(error.code, error.stage, str(error),
                             possible_effect=error.possible_effect,
                             retry_safe=error.retry_safe) from None


def remove_persistent_entry(adapter_id: str, target: Path, *,
                            entry_name: str = "nexus",
                            previously_owned: dict | None = None
                            ) -> dict[str, object]:
    """Remove only the product-owned entry, preserving everything else."""
    from nexus_connector_core.config_document import (
        plan_codex_toml_entry_removal, plan_json_entry_removal,
    )
    existing = target.read_bytes() if target.exists() else None
    if existing is None:
        return {"changed": False, "note": "file absent; nothing removed"}
    if previously_owned is None:
        previously_owned = _last_owned_entry(adapter_id, target, entry_name)
    if adapter_id == "codex_app_server":
        plan = plan_codex_toml_entry_removal(
            existing, entry_name=entry_name,
            previously_owned=previously_owned or {})
        result = apply_toml_plan_file(plan, target)
    else:
        plan = plan_json_entry_removal(
            existing, section="mcpServers", entry_name=entry_name,
            previously_owned=previously_owned or {})
        result = apply_json_plan_file(plan, target)
    return {"changed": result.changed,
            "backup_path": str(result.backup_path) if result.backup_path else None}


def _last_owned_entry(adapter_id: str, target: Path,
                      entry_name: str) -> dict | None:
    """Read back the entry only as a candidate; callers must confirm.

    A safe default of None fails the removal closed unless the trusted
    host recorded the entry it wrote; interactive flows confirm instead.
    """
    return None


def _origin(url: str) -> str:
    from ..transport.https_client import origin_of
    return origin_of(url)


def _is_loopback(url: str) -> bool:
    from urllib.parse import urlsplit
    host = (urlsplit(url).hostname or "").lower()
    return host in ("127.0.0.1", "::1", "localhost")


def tools_only_status(plan: MCPDirectPlan | None) -> dict[str, object]:
    """A tools-only conversation needs no daemon (plan 2.3, TC-24)."""
    return {
        "tools_only": plan is not None,
        "requires_connector_process": False,
        "requires_daemon": False,
        "note": "the harness's MCP HTTP client reaches the Server "
                "directly; installing or stopping the connector does not "
                "affect this path",
    }
