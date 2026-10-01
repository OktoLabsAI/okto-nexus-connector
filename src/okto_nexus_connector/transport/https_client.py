"""Outbound HTTPS client for the Nexus Server management API.

This is the Connector-side client for the contract routes of plan A.5
(`/v1/connections/*`, `/v1/runtime/*`). It never proxies MCP and never
forwards provider secrets. TLS is required outside explicit loopback
development; redirects to another origin are refused for credentialed
requests (A.15).
"""

from __future__ import annotations

import ipaddress
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx

if TYPE_CHECKING:
    from nexus_connector_core import (
        ExecutionContext, OperationReceipt, PreparedLaunch,
    )

from ..errors import CapabilityMaterialUnavailable, ConnectorError, TicketMaterialUnavailable
from ..redaction import redact_text

DEFAULT_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
_USER_AGENT = "okto-nexus-connector/" + __import__(
    "okto_nexus_connector").__version__
MANAGEMENT_REVISION = "nexus-connections-2026-09-29-r4"


@dataclass(frozen=True, slots=True)
class MeInfo:
    server_id: str
    agent_id: str
    display_name: str
    permissions: tuple[str, ...]
    authorization_revision: int
    configuration_revision: int
    credential_epoch: int


@dataclass(frozen=True, slots=True)
class ExecutorRegistration:
    server_id: str
    executor_id: str
    connector_id: str
    state: str
    bootstrap_ticket: str
    ticket_expires_in: int
    registration_agent_id: str = ""
    credential_epoch: int = 0
    authorization_revision: int = 0


@dataclass(frozen=True, slots=True)
class InventoryAccepted:
    server_id: str
    executor_id: str
    publication_sequence: int
    inventory_revision: str
    fresh_for_ms: int


@dataclass(frozen=True, slots=True)
class R4ProtocolInfo:
    management_revision: str
    executor_snapshot_format: int
    remote_execution_ready: bool


@dataclass(frozen=True, slots=True)
class R4Realization:
    server_id: str
    executor_id: str
    realization_ref: str
    local_realization_ref: str
    realization_revision: int
    agent_id: str
    workspace_id: str
    workspace_binding_id: str
    inventory_revision: str
    configuration_digest: str


@dataclass(frozen=True, slots=True)
class R4BindingProposal:
    proposal_id: str
    proposal_revision: int
    expires_at: str
    server_id: str
    executor_id: str
    agent_id: str
    binding_id: str
    endpoint_id: str
    profile_id: str | None
    workspace_id: str
    workspace_binding_id: str
    adapter_id: str
    candidate_ref: str
    inventory_revision: str
    realization_ref: str
    realization_revision: int
    authorization_revision: int
    configuration_revision: int
    approved_diff_hash: str
    summary: str
    requires_operator: bool
    fields_changed: tuple[str, ...]
    required_approvals: tuple[str, ...]
    can_apply: bool


@dataclass(frozen=True, slots=True)
class R4BindingView:
    binding_id: str
    server_id: str
    executor_id: str
    agent_id: str
    endpoint_id: str
    workspace_id: str
    workspace_binding_id: str
    adapter_id: str
    candidate_ref: str
    inventory_revision: str
    realization_ref: str
    realization_revision: int
    binding_revision: int
    authorization_revision: int
    configuration_revision: int
    state: str


@dataclass(frozen=True, slots=True)
class R4IntentResolution:
    client_intent_id: str
    intent_id: str
    operation_id: str
    session_id: str
    reuse: bool
    scope: dict[str, object]
    semantic_intent: dict[str, object]
    intent_hash: str
    resolution_revision: int
    expires_at: str
    can_submit: bool
    blockers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReceiptAccepted:
    operation_id: str
    receipt_revision: int
    stage: str
    reused: bool


@dataclass(frozen=True, slots=True)
class R4BindingTicket:
    ticket_id: str
    ticket: str
    executor_id: str
    binding_id: str
    agent_id: str
    expires_in: int
    credential_epoch: int
    authorization_revision: int
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BindingProposal:
    proposal_id: str
    proposal_revision: int
    binding_id: str
    endpoint_id: str
    profile_id: str
    workspace_binding_id: str
    workspace_id: str
    authorization_revision: int
    configuration_revision: int


@dataclass(frozen=True, slots=True)
class IntentResolution:
    operation_id: str
    session_id: str
    reuse: bool
    execution: dict[str, object]
    lease_seconds: float
    allowed_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SessionCapability:
    capability_ref: str
    capability: str
    expires_in: float


@dataclass(frozen=True, slots=True)
class R4SessionCapability:
    capability_id: str
    capability_ref: str
    capability: str = field(repr=False)
    scope: dict[str, object]
    audience: str
    actions: tuple[str, ...]
    expires_in: int
    deadline_monotonic: float
    mcp_url: str | None


@dataclass(frozen=True, slots=True)
class R4CapabilityMetadata:
    capability_id: str
    capability_ref: str
    scope: dict[str, object]
    audience: str
    actions: tuple[str, ...]
    deadline_monotonic: float
    lease_id: str
    lease_serial: int
    mcp_url: str | None


def origin_of(base_url: str) -> str:
    parts = urlsplit(base_url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConnectorError("PROFILE_DRIFT", "origin",
                             "base URL must be http(s) with a host")
    host = f"[{parts.hostname.lower()}]" if ":" in parts.hostname \
        else parts.hostname.lower()
    default = (parts.scheme == "https" and parts.port == 443) or \
              (parts.scheme == "http" and parts.port == 80)
    port = "" if (parts.port is None or default) else f":{parts.port}"
    return f"{parts.scheme}://{host}{port}"


def is_loopback_origin(base_url: str) -> bool:
    parts = urlsplit(base_url)
    host = (parts.hostname or "").lower()
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def _no_proxy_for(base_url: str) -> bool:
    """Honor NO_PROXY for explicitly excluded hosts."""
    import os
    parts = urlsplit(base_url)
    host = (parts.hostname or "").lower()
    excludes = os.environ.get("NO_PROXY", "") or os.environ.get(
        "no_proxy", "")
    if not excludes:
        return False
    for entry in excludes.split(","):
        entry = entry.strip().lower().lstrip(".")
        if entry and (host == entry or host.endswith("." + entry)):
            return True
    return False


class NexusHTTPClient:
    """One Server profile's authenticated HTTPS client.

    CN1/A06: the origin tuple (scheme, host, port — defaults stripped)
    is validated BEFORE any credential is attached or request is sent.
    Non-loopback origins MUST be https/wss; explicit userinfo, missing
    hosts and ambiguous shapes are refused up front, so no canonical
    key is ever transmitted over plaintext to a remote host (test_10:
    zero authenticated requests reach the transport).
    """

    def __init__(self, base_url: str, *, verify: bool = True,
                 timeout: httpx.Timeout | None = None,
                 client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self.origin = origin_of(self.base_url)
        self._loopback = is_loopback_origin(self.base_url)
        self._validate_origin_safety()
        if not verify and not self._loopback:
            raise ConnectorError("PROFILE_DRIFT", "tls",
                                 "TLS verification cannot be disabled for a "
                                 "non-loopback origin")
        self._verify = verify
        self._timeout = timeout or DEFAULT_TIMEOUT
        self._client = client
        self._owned = client is None

    def _validate_origin_safety(self) -> None:
        parts = urlsplit(self.base_url)
        if parts.username or parts.password or "@" in parts.netloc:
            raise ConnectorError(
                "PROFILE_DRIFT", "origin",
                "userinfo in the Server URL is refused",
                action="Provide scheme://host[:port] without credentials "
                       "in the URL; keys enter through protected input.")
        if not self._loopback and parts.scheme != "https":
            raise ConnectorError(
                "PROFILE_DRIFT", "origin",
                f"non-loopback Server origin must be HTTPS, got "
                f"{parts.scheme!r} for {self.origin}",
                action="Use the Server's https:// address; the canonical "
                       "key is never sent over plaintext. Loopback http "
                       "is the documented laboratory exception.")

    async def __aenter__(self) -> "NexusHTTPClient":
        if self._client is None:
            # Loopback targets never use environment proxies; remote
            # origins keep normal proxy/trust_env behavior.
            trust_env = not (self._loopback or
                             _no_proxy_for(self.base_url))
            self._client = httpx.AsyncClient(
                verify=self._verify, timeout=self._timeout,
                headers={"User-Agent": _USER_AGENT},
                follow_redirects=False, trust_env=trust_env)
        return self

    async def __aexit__(self, *_exc) -> None:
        if self._owned and self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- low level -------------------------------------------------------

    async def _request(self, method: str, path: str, *, key: str | None,
                        json_body: dict[str, object] | None = None,
                        expect: int | tuple[int, ...] = 200,
                        require_revision: bool = False,
                        binding_ticket_errors: bool = False) -> dict[str, object]:
        assert self._client is not None, "use 'async with NexusHTTPClient'"
        headers: dict[str, str] = {}
        if key is not None:
            headers["Authorization"] = f"Bearer {key}"
        mutating = method.upper() in ("POST", "PUT", "PATCH", "DELETE")
        try:
            response = await self._client.request(
                method, f"{self.base_url}{path}", headers=headers,
                json=json_body)
        except httpx.HTTPError as exc:
            # CN1/A07 (test_11): once the request has been handed to the
            # transport, a LOST REPLY never proves absence of effect.
            # Read/timeout/remote errors after a mutating send are
            # possible-effect and NOT retry-safe; only a failure provably
            # BEFORE delivery (connect-phase) keeps the safe-retry policy.
            connect_phase = isinstance(
                exc, (httpx.ConnectError, httpx.ConnectTimeout))
            if mutating and not connect_phase:
                raise ConnectorError(
                    "OUTCOME_UNKNOWN", "http",
                    redact_text(f"reply unavailable after {method} was "
                                f"sent: {exc}"),
                    possible_effect=True,
                    retry_safe=False,
                    action="The Server may have received the request. "
                           "Query the operation by its id; do not re-send "
                           "a new intent for the same work.") from None
            raise ConnectorError(
                "EXECUTOR_OFFLINE", "http",
                redact_text(f"transport failure: {exc}"),
                retry_safe=True,
                action="Check the Server address/network and retry; the "
                       "request was not delivered.") from None
        if response.has_redirect_location:
            from urllib.parse import urljoin
            target = origin_of(urljoin(str(response.url),
                                       response.headers["location"]))
            if target != self.origin:
                raise ConnectorError(
                    "PROFILE_DRIFT", "http",
                    "credential redirect to another origin refused",
                    action="Verify the approved Server origin; "
                           "cross-origin redirects are refused.")
        if (require_revision and response.headers.get(
                "X-Nexus-Connections-Revision") != MANAGEMENT_REVISION):
            raise ConnectorError("VERSION_INCOMPATIBLE", "http",
                                 "Server management revision is incompatible")
        if isinstance(expect, int):
            expect = (expect,)
        if response.status_code not in expect:
            if binding_ticket_errors and response.status_code == 409:
                raise _ticket_error_from(response)
            raise _error_from(response)
        try:
            payload = response.json()
        except ValueError:
            raise ConnectorError("VERSION_INCOMPATIBLE", "http",
                                  "non-JSON response from Server") from None
        if not isinstance(payload, dict):
            raise ConnectorError("VERSION_INCOMPATIBLE", "http",
                                 "malformed Server response")
        return payload

    # -- contract routes (plan A.5) ---------------------------------------

    async def r4_protocol(self) -> R4ProtocolInfo:
        """Check public compatibility before requesting any credential."""
        from nexus_connector_core import R4_PREVIEW_REVISION, SNAPSHOT_FORMAT_VERSION, __version__
        payload = await self._request("GET", "/v1/connections/protocol", key=None,
                                      require_revision=True)
        accepted = payload.get("nxl_accepted")
        ready = payload.get("remote_execution_ready")
        if (payload.get("management_revision") != MANAGEMENT_REVISION or
                type(payload.get("protocol_major")) is not int or payload["protocol_major"] != 1 or
                payload.get("core_version") != __version__ or
                type(payload.get("executor_snapshot_format")) is not int or
                payload["executor_snapshot_format"] != SNAPSHOT_FORMAT_VERSION or
                type(ready) is not bool or not isinstance(accepted, list) or
                any(type(value) is not str for value in accepted) or
                (ready and R4_PREVIEW_REVISION not in accepted)):
            raise ConnectorError("VERSION_INCOMPATIBLE", "protocol",
                                 "The Server does not advertise a compatible R4 protocol.")
        return R4ProtocolInfo(MANAGEMENT_REVISION, SNAPSHOT_FORMAT_VERSION, ready)

    async def me(self, key: str) -> MeInfo:
        payload = await self._request("GET", "/v1/connections/me", key=key,
                                      require_revision=True)
        agent_id = payload.get("agent_id")
        server_id = payload.get("server_id")
        if not isinstance(agent_id, str) or not 1 <= len(agent_id) <= 160:
            raise ConnectorError("VERSION_INCOMPATIBLE", "me",
                                 "Server /me did not return an agent identity")
        if not isinstance(server_id, str) or not server_id:
            raise ConnectorError("SERVER_ID_CHANGED", "me",
                                 "Server /me did not identify itself",
                                 action="Confirm the Server installation "
                                        "identity and update the profile.")
        permissions = payload.get("permissions")
        revisions = payload.get("revisions")
        display_name = payload.get("display_name")
        if (not isinstance(display_name, str) or len(display_name) > 4096 or
                not isinstance(permissions, list) or
                any(not isinstance(item, str) or not 1 <= len(item) <= 160
                    for item in permissions) or
                not isinstance(revisions, dict) or
                any(type(revisions.get(name)) is not int or revisions[name] < 1
                    for name in ("authorization", "configuration",
                                 "credential_epoch"))):
            raise ConnectorError("VERSION_INCOMPATIBLE", "me",
                                 "Server /me returned invalid permissions or revisions")
        return MeInfo(
            server_id=server_id,
            agent_id=agent_id,
            display_name=display_name,
            permissions=tuple(permissions),
            authorization_revision=revisions["authorization"],
            configuration_revision=revisions["configuration"],
            credential_epoch=revisions["credential_epoch"],
        )

    async def register_executor(self, key: str, *, client_intent_id: str,
                                connector_id: str, label: str,
                                control_capabilities: tuple[str, ...] = (
                                )) -> ExecutorRegistration:
        payload = await self._request(
            "POST", "/v1/connections/executors:register", key=key,
            json_body={"client_intent_id": client_intent_id,
                       "connector_id": connector_id, "label": label,
                       "control_capabilities": list(control_capabilities)},
            expect=(200, 201), require_revision=True,
        )
        ticket = payload.get("bootstrap_ticket")
        if (not isinstance(ticket, dict) or
                ticket.get("audience") != "nexus-executor-control" or
                not isinstance(ticket.get("scopes"), list) or
                any(not isinstance(item, str) for item in ticket["scopes"]) or
                 set(ticket["scopes"]) != {
                     "link:connect", "inventory:publish",
                     "realization:publish"} or
                not isinstance(ticket.get("ticket"), str) or
                not ticket["ticket"].startswith("nxt4_") or
                type(ticket.get("expires_in")) is not int or
                not 1 <= ticket["expires_in"] <= 600 or
                not all(isinstance(payload.get(name), str) and payload[name]
                        for name in ("server_id", "executor_id", "connector_id")) or
                ticket.get("executor_id") != payload.get("executor_id") or
                ticket.get("binding_id") is not None or
                not isinstance(ticket.get("agent_id"), str) or
                not ticket["agent_id"] or
                any(type(ticket.get(name)) is not int or ticket[name] < 1
                    for name in ("credential_epoch", "authorization_revision")) or
                payload.get("connector_id") != connector_id or
                payload.get("state") != "AWAITING_INVENTORY"):
            raise ConnectorError("VERSION_INCOMPATIBLE", "register_executor",
                                 "Server returned an invalid executor registration")
        return ExecutorRegistration(
            server_id=payload["server_id"], executor_id=payload["executor_id"],
            connector_id=payload["connector_id"], state=payload["state"],
            bootstrap_ticket=ticket["ticket"],
            ticket_expires_in=ticket["expires_in"],
            registration_agent_id=ticket["agent_id"],
            credential_epoch=ticket["credential_epoch"],
            authorization_revision=ticket["authorization_revision"],
        )

    async def publish_inventory(self, ticket: str, *, executor_id: str,
                                snapshot: dict[str, object]) -> InventoryAccepted:
        payload = await self._request(
            "PUT", f"/v1/runtime/executors/{executor_id}/inventory", key=ticket,
            json_body=snapshot, require_revision=True,
        )
        if (payload.get("accepted") is not True or
                payload.get("executor_id") != executor_id or
                payload.get("server_id") != snapshot.get("server_id") or
                payload.get("inventory_revision") != snapshot.get("inventory_revision") or
                payload.get("publication_sequence") != snapshot.get(
                    "publication_sequence") or
                type(payload.get("fresh_for_ms")) is not int or
                not 0 <= payload["fresh_for_ms"] <= 120_000):
            raise ConnectorError("VERSION_INCOMPATIBLE", "inventory",
                                 "Server returned an invalid inventory receipt")
        return InventoryAccepted(
            server_id=payload["server_id"], executor_id=executor_id,
            publication_sequence=payload["publication_sequence"],
            inventory_revision=payload["inventory_revision"],
            fresh_for_ms=payload["fresh_for_ms"],
        )

    async def read_r4_inventory(self, key: str, *, server_id: str, executor_id: str) -> dict:
        """Read the daemon publication; this never grants runtime readiness."""
        from nexus_connector_core import CoreError, verify_executor_inventory_snapshot
        payload = await self._request("GET", f"/v1/runtime/executors/{executor_id}/inventory",
                                      key=key, require_revision=True)
        snapshot = payload.get("snapshot")
        if (not isinstance(snapshot, dict) or snapshot.get("server_id") != server_id or
                snapshot.get("executor_id") != executor_id or
                payload.get("freshness") not in ("FRESH", "STALE", "OFFLINE") or
                type(payload.get("eligible_for_new_start")) is not bool):
            raise ConnectorError("VERSION_INCOMPATIBLE", "inventory",
                                 "Server returned an invalid executor inventory view.")
        try:
            verify_executor_inventory_snapshot(snapshot)
        except (CoreError, TypeError, ValueError):
            raise ConnectorError("VERSION_INCOMPATIBLE", "inventory",
                                 "Server returned invalid inventory evidence.") from None
        return payload

    async def publish_r4_realization(
            self, ticket: str, *, executor_id: str,
            request: dict[str, object]) -> R4Realization:
        """Publish executor-owned opaque evidence after local validation."""
        payload = await self._request(
            "POST", f"/v1/runtime/executors/{executor_id}/realizations",
            key=ticket, json_body=request, expect=(200, 201),
            require_revision=True,
        )
        identity = (
            "server_id", "executor_id", "realization_ref",
            "local_realization_ref", "agent_id", "workspace_id",
            "workspace_binding_id", "inventory_revision",
            "configuration_digest",
        )
        if (any(not isinstance(payload.get(name), str) or not payload[name]
                for name in identity) or
                payload["executor_id"] != executor_id or
                any(payload[name] != request[name] for name in (
                    "local_realization_ref", "agent_id",
                    "inventory_revision", "configuration_digest")) or
                (request.get("workspace_id") is not None and
                 payload["workspace_id"] != request["workspace_id"]) or
                type(payload.get("realization_revision")) is not int or
                payload["realization_revision"] != request.get(
                    "realization_revision")):
            raise ConnectorError("VERSION_INCOMPATIBLE", "realization",
                                 "Server returned an invalid realization mapping")
        return R4Realization(**{name: payload[name] for name in identity},
                             realization_revision=payload["realization_revision"])

    async def prepare_r4_binding(
            self, key: str, *, client_intent_id: str,
            executor_id: str, adapter_id: str, candidate_ref: str,
            inventory_revision: str, realization_ref: str,
            workspace_id: str, alias: str,
            agent_id_hint: str | None = None, replace_binding_id: str | None = None) -> R4BindingProposal:
        """Request a reviewable Server proposal without starting a runtime."""
        body = {
            "client_intent_id": client_intent_id,
            "executor_id": executor_id, "adapter_id": adapter_id,
            "candidate_ref": candidate_ref,
            "inventory_revision": inventory_revision,
            "realization_ref": realization_ref,
            "workspace_id": workspace_id, "alias": alias,
        }
        if agent_id_hint is not None:
            body["agent_id_hint"] = agent_id_hint
        if replace_binding_id is not None:
            body["replace_binding_id"] = replace_binding_id
        payload = await self._request(
            "POST", "/v1/connections/bindings:prepare", key=key,
            json_body=body, require_revision=True,
        )
        names = (
            "proposal_id", "expires_at", "server_id", "executor_id",
            "agent_id", "binding_id", "endpoint_id", "workspace_id",
            "workspace_binding_id", "adapter_id", "candidate_ref",
            "inventory_revision", "realization_ref",
        )
        diff = payload.get("diff")
        if (any(not isinstance(payload.get(name), str) or not payload[name]
                for name in names) or
                any(payload[name] != body[name] for name in (
                    "executor_id", "adapter_id", "candidate_ref",
                    "inventory_revision", "realization_ref", "workspace_id")) or
                (agent_id_hint is not None and payload["agent_id"] != agent_id_hint) or
                (replace_binding_id is not None and payload["binding_id"] != replace_binding_id) or
                any(type(payload.get(name)) is not int or payload[name] < 1
                    for name in ("proposal_revision", "realization_revision",
                                 "authorization_revision",
                                 "configuration_revision")) or
                not isinstance(diff, dict) or
                not isinstance(diff.get("approved_diff_hash"), str) or
                not diff["approved_diff_hash"].startswith("sha256:") or
                not isinstance(diff.get("summary"), str) or
                type(diff.get("requires_operator")) is not bool or
                not isinstance(diff.get("fields_changed"), list) or
                any(not isinstance(item, str) or not item for item in diff["fields_changed"]) or
                "profile_id" not in payload or
                (payload["profile_id"] is not None and
                 (not isinstance(payload["profile_id"], str) or not 1 <= len(payload["profile_id"]) <= 160)) or
                type(payload.get("can_apply")) is not bool or
                not isinstance(payload.get("required_approvals"), list) or
                any(not isinstance(item, str) for item in
                    payload["required_approvals"])):
            raise ConnectorError("VERSION_INCOMPATIBLE", "binding_prepare",
                                 "Server returned an invalid binding proposal")
        return R4BindingProposal(
            **{name: payload[name] for name in names},
            proposal_revision=payload["proposal_revision"],
            realization_revision=payload["realization_revision"],
            authorization_revision=payload["authorization_revision"],
            configuration_revision=payload["configuration_revision"],
            approved_diff_hash=diff["approved_diff_hash"],
            summary=diff["summary"],
            profile_id=payload["profile_id"],
            requires_operator=diff["requires_operator"],
            fields_changed=tuple(diff["fields_changed"]),
            required_approvals=tuple(payload["required_approvals"]),
            can_apply=payload["can_apply"],
        )

    async def apply_r4_binding(
            self, key: str, *, client_intent_id: str,
            proposal: R4BindingProposal,
            operator_proof_ref: str | None = None) -> R4BindingView:
        """Commit only the exact reviewed proposal and check its resolution."""
        body = {
            "client_intent_id": client_intent_id,
            "proposal_id": proposal.proposal_id,
            "proposal_revision": proposal.proposal_revision,
            "approved_diff_hash": proposal.approved_diff_hash,
        }
        if operator_proof_ref is not None:
            body["operator_proof_ref"] = operator_proof_ref
        payload = await self._request(
            "POST", "/v1/connections/bindings:apply", key=key,
            json_body=body, require_revision=True,
        )
        fields = (
            "binding_id", "server_id", "executor_id", "agent_id",
            "endpoint_id", "workspace_id", "workspace_binding_id",
            "adapter_id", "candidate_ref", "inventory_revision",
            "realization_ref", "state",
        )
        versions = (
            "realization_revision", "binding_revision",
            "authorization_revision", "configuration_revision",
        )
        if (any(not isinstance(payload.get(name), str) or not payload[name]
                for name in fields) or
                any(type(payload.get(name)) is not int or payload[name] < 1
                    for name in versions) or
                any(payload[name] != getattr(proposal, name) for name in (
                    "binding_id", "server_id", "executor_id", "agent_id",
                    "endpoint_id", "workspace_id", "workspace_binding_id",
                    "adapter_id", "candidate_ref", "inventory_revision",
                    "realization_ref", "realization_revision")) or
                payload["state"] != "APPROVED"):
            raise ConnectorError("VERSION_INCOMPATIBLE", "binding_apply",
                                 "Server returned an invalid binding resolution")
        return R4BindingView(**{name: payload[name] for name in
                               (*fields, *versions)})

    async def resolve_r4_intent(
            self, key: str, *, client_intent_id: str,
            intent: str, binding_id: str, workspace_binding_id: str,
            session_id: str | None = None, new_session: bool | None = None,
            text: str | None = None,
            target: dict[str, object] | None = None) -> R4IntentResolution:
        """Reserve a Server intent without admitting an effect."""
        body: dict[str, object] = {
            "client_intent_id": client_intent_id,
            "intent": intent, "binding_id": binding_id,
            "workspace_binding_id": workspace_binding_id,
        }
        if session_id is not None:
            body["session_id"] = session_id
        if new_session is not None:
            body["new_session"] = new_session
        if text is not None:
            body["text"] = text
        if target is not None:
            body["target"] = dict(target)
        payload = await self._request(
            "POST", "/v1/runtime/intents:resolve", key=key,
            json_body=body, require_revision=True,
        )
        names = ("client_intent_id", "intent_id", "operation_id",
                 "session_id", "intent_hash", "expires_at")
        if (any(not isinstance(payload.get(name), str) or not payload[name]
                for name in names) or
                payload["client_intent_id"] != client_intent_id or
                type(payload.get("reuse")) is not bool or
                not isinstance(payload.get("scope"), dict) or
                payload["scope"].get("binding_id") != binding_id or
                payload["scope"].get("workspace_binding_id") !=
                workspace_binding_id or
                not isinstance(payload.get("semantic_intent"), dict) or
                type(payload.get("resolution_revision")) is not int or
                payload["resolution_revision"] < 1 or
                type(payload.get("can_submit")) is not bool or
                not isinstance(payload.get("blockers"), list) or
                any(not isinstance(item, str) for item in payload["blockers"]) or
                payload.get("dispatch_owner") != "server"):
            raise ConnectorError("VERSION_INCOMPATIBLE", "intent_resolve",
                                 "Server returned an invalid intent resolution")
        from nexus_connector_core import CoreError, r4_submit_intent_hash

        semantic = payload["semantic_intent"]
        expected_target = target if target is not None else {
            "kind": "none", "expected_turn_id": None}
        try:
            wire_intent = {name: value for name, value in semantic.items() if name != "target"}
            if semantic["target"]["expected_turn_id"] is not None:
                wire_intent["expected_turn_id"] = semantic["target"]["expected_turn_id"]
            content_key = "reason" if intent in {"turn.interrupt", "runtime.close"} else "text"
            matches = (
                semantic["action"] == ("runtime.open" if intent == "runtime.start" else intent)
                and (not payload["reuse"] or intent == "runtime.start")
                and (new_session is not True or not payload["reuse"])
                and (intent != "runtime.start" or session_id is None or payload["reuse"])
                and semantic["target"] == expected_target
                and semantic["session_id"] == payload["session_id"]
                and (session_id is None or payload["session_id"] == session_id)
                and all(semantic[name] == payload["scope"][name] for name in (
                    "server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                    "workspace_binding_id", "session_id", "configuration_revision"))
                and (text is None or intent == "runtime.start" or semantic["payload"][content_key] == text)
                and r4_submit_intent_hash(wire_intent) == payload["intent_hash"]
            )
        except (CoreError, ValueError, TypeError, KeyError, RecursionError):
            matches = False
        if not matches:
            raise ConnectorError("VERSION_INCOMPATIBLE", "intent_resolve",
                                 "The resolved operation does not match the requested intent.")
        return R4IntentResolution(
            **{name: payload[name] for name in names},
            reuse=payload["reuse"], scope=payload["scope"],
            semantic_intent=payload["semantic_intent"],
            resolution_revision=payload["resolution_revision"],
            can_submit=payload["can_submit"],
            blockers=tuple(payload["blockers"]),
        )

    async def submit_r4_operation(
            self, key: str, resolution: R4IntentResolution
            ) -> dict[str, object]:
        """Admit an exact Server resolution, then return its operation view."""
        if not resolution.can_submit or resolution.blockers:
            raise ConnectorError(
                "EXECUTOR_OFFLINE", "operation_admission",
                "The resolved intent is not eligible for execution.",
                operation_id=resolution.operation_id,
                action="Resolve a new intent after the Server reports a ready "
                       "executor and binding.")
        payload = await self._request(
            "POST", "/v1/runtime/operations", key=key,
            json_body={
                "client_intent_id": resolution.client_intent_id,
                "operation_id": resolution.operation_id,
                "resolution_revision": resolution.resolution_revision,
                "intent_hash": resolution.intent_hash,
            }, expect=(200, 202), require_revision=True,
        )
        scope = payload.get("scope")
        if (payload.get("operation_id") != resolution.operation_id or
                (not resolution.reuse and payload.get("client_intent_id") !=
                 resolution.client_intent_id) or
                payload.get("intent_hash") != resolution.intent_hash or
                not isinstance(scope, dict) or
                any(scope.get(name) != resolution.scope.get(name)
                    for name in ("server_id", "executor_id", "binding_id",
                                 "agent_id", "workspace_id", "session_id")) or
                payload.get("action") !=
                resolution.semantic_intent.get("action") or
                payload.get("admission_state") not in {
                    "ACCEPTED", "DISPATCH_PENDING", "DISPATCHED",
                    "RECONCILING", "RESOLVED_TERMINAL"} or
                type(payload.get("possible_effect")) is not bool or
                type(payload.get("retry_safe")) is not bool or
                type(payload.get("receipt_revision")) is not int or
                payload["receipt_revision"] < 0):
            raise ConnectorError(
                "VERSION_INCOMPATIBLE", "operation_admission",
                "Server returned an invalid operation view.",
                possible_effect=True, retry_safe=False,
                operation_id=resolution.operation_id,
                action="Query the operation by its ID before taking another action.")
        return payload

    async def publish_operation_receipt(self, ticket: str, *,
                                        frame: dict[str, object]
                                        ) -> ReceiptAccepted:
        from nexus_connector_core import CoreError, decode_r4_frame
        from nexus_connector_core.protocol import canonical_json

        try:
            parsed = decode_r4_frame(canonical_json(frame))
        except (CoreError, ValueError, TypeError) as exc:
            raise ConnectorError("VERSION_INCOMPATIBLE", "receipt",
                                 "Invalid Core operation receipt") from exc
        if parsed["type"] != "operation.receipt":
            raise ConnectorError("VERSION_INCOMPATIBLE", "receipt",
                                 "A Core operation receipt is required")
        operation_id = parsed["operation_id"]
        payload = await self._request(
            "POST", f"/v1/runtime/operations/{operation_id}/receipts",
            key=ticket, json_body=parsed, require_revision=True,
        )
        if (payload.get("accepted") is not True or
                payload.get("operation_id") != operation_id or
                payload.get("receipt_revision") != parsed["receipt_revision"] or
                payload.get("stage") != parsed["stage"] or
                type(payload.get("reused")) is not bool):
            raise ConnectorError("VERSION_INCOMPATIBLE", "receipt",
                                 "Server returned an invalid receipt acknowledgment")
        return ReceiptAccepted(operation_id, parsed["receipt_revision"],
                               parsed["stage"], payload["reused"])

    async def publish_core_turn_receipt(
            self, ticket: str, *, submit_frame: dict[str, object],
            core_receipt: OperationReceipt, context: ExecutionContext,
            receipt_revision: int) -> ReceiptAccepted:
        """Verify a Core turn receipt before publishing its R4 wire fact."""
        from nexus_connector_core import project_r4_turn_receipt

        frame = project_r4_turn_receipt(
            submit_frame, core_receipt, context,
            receipt_revision=receipt_revision)
        return await self.publish_operation_receipt(ticket, frame=frame)

    async def publish_core_open_receipt(
            self, ticket: str, *, submit_frame: dict[str, object],
            core_receipt: OperationReceipt, context: ExecutionContext,
            prepared: PreparedLaunch, stream_epoch: str,
            receipt_revision: int) -> ReceiptAccepted:
        """Verify a Core open receipt and its selected local launch."""
        from nexus_connector_core import project_r4_open_receipt

        frame = project_r4_open_receipt(
            submit_frame, core_receipt, context, prepared,
            stream_epoch=stream_epoch, receipt_revision=receipt_revision)
        return await self.publish_operation_receipt(ticket, frame=frame)

    async def publish_core_steer_receipt(
            self, ticket: str, *, submit_frame: dict[str, object],
            core_receipt: OperationReceipt, context: ExecutionContext,
            receipt_revision: int) -> ReceiptAccepted:
        """Verify a Core steer receipt before publishing its R4 wire fact."""
        from nexus_connector_core import project_r4_steer_receipt

        frame = project_r4_steer_receipt(
            submit_frame, core_receipt, context,
            receipt_revision=receipt_revision)
        return await self.publish_operation_receipt(ticket, frame=frame)

    async def publish_core_interrupt_receipt(
            self, ticket: str, *, submit_frame: dict[str, object],
            core_receipt: OperationReceipt, context: ExecutionContext,
            receipt_revision: int) -> ReceiptAccepted:
        """Verify a Core interrupt reason before publishing its R4 receipt."""
        from nexus_connector_core import project_r4_interrupt_receipt

        frame = project_r4_interrupt_receipt(
            submit_frame, core_receipt, context,
            receipt_revision=receipt_revision)
        return await self.publish_operation_receipt(ticket, frame=frame)

    async def publish_core_close_receipt(
            self, ticket: str, *, submit_frame: dict[str, object],
            core_receipt: OperationReceipt, context: ExecutionContext,
            receipt_revision: int) -> ReceiptAccepted:
        """Verify a Core close reason before publishing its R4 receipt."""
        from nexus_connector_core import project_r4_close_receipt

        frame = project_r4_close_receipt(
            submit_frame, core_receipt, context,
            receipt_revision=receipt_revision)
        return await self.publish_operation_receipt(ticket, frame=frame)

    async def request_r4_binding_ticket(
            self, key: str, *, binding_id: str, client_intent_id: str,
            credential_request_id: str, scopes: tuple[str, ...],
            replaces_ticket_id: str | None = None,
            expires_in: int = 600) -> R4BindingTicket:
        """Issue one scoped derivative; a lost secret requires a new request ID."""
        payload = await self._request(
            "POST", f"/v1/connections/bindings/{binding_id}/ticket",
            key=key, json_body={
                "client_intent_id": client_intent_id,
                "credential_request_id": credential_request_id,
                "replaces_ticket_id": replaces_ticket_id,
                "audience": "nexus-executor-control",
                "scopes": list(scopes), "expires_in": expires_in,
            }, require_revision=True, binding_ticket_errors=True,
        )
        if (payload.get("audience") != "nexus-executor-control" or
                payload.get("binding_id") != binding_id or
                not isinstance(payload.get("ticket_id"), str) or
                not payload["ticket_id"].startswith("ept_") or
                not isinstance(payload.get("ticket"), str) or
                not payload["ticket"].startswith("nxt4_") or
                not isinstance(payload.get("executor_id"), str) or
                not payload["executor_id"] or
                not isinstance(payload.get("agent_id"), str) or
                not payload["agent_id"] or
                payload.get("scopes") != sorted(scopes) or
                any(type(payload.get(name)) is not int or payload[name] < 1
                    for name in ("credential_epoch", "authorization_revision")) or
                payload.get("expires_in") != expires_in):
            raise ConnectorError("VERSION_INCOMPATIBLE", "binding_ticket",
                                 "Server returned an invalid R4 binding ticket")
        return R4BindingTicket(
            ticket_id=payload["ticket_id"], ticket=payload["ticket"],
            executor_id=payload["executor_id"], binding_id=binding_id,
            agent_id=payload["agent_id"], expires_in=expires_in,
            credential_epoch=payload["credential_epoch"],
            authorization_revision=payload["authorization_revision"],
            scopes=tuple(payload["scopes"]),
        )

    async def prepare_binding(self, key: str, *, agent_id_hint: str,
                              connector_id: str, adapter_id: str,
                              candidate_version: str,
                              binding_alias: str,
                              workspace_hint: dict[str, object] | None = None
                              ) -> BindingProposal:
        payload = await self._request(
            "POST", "/v1/connections/bindings:prepare", key=key, json_body={
                "agent_id_hint": agent_id_hint,
                "executor": {"connector_id": connector_id,
                             "adapter_id": adapter_id,
                             "candidate_version": candidate_version},
                "binding_alias": binding_alias,
                "workspace_hint": workspace_hint or {},
            })
        return _proposal_from(payload)

    async def apply_binding(self, key: str, proposal: BindingProposal
                            ) -> BindingProposal:
        payload = await self._request(
            "POST", "/v1/connections/bindings:apply", key=key, json_body={
                "proposal_id": proposal.proposal_id,
                "proposal_revision": proposal.proposal_revision,
            })
        return _proposal_from(payload)

    async def binding_ticket(self, key: str, binding_id: str) -> tuple[str, float]:
        payload = await self._request(
            "POST", f"/v1/connections/bindings/{binding_id}/ticket",
            key=key, json_body={})
        ticket = payload.get("ticket")
        expires = payload.get("expires_in", 600)
        if not isinstance(ticket, str) or not ticket:
            raise ConnectorError("AGENT_AUTH_REQUIRED", "ticket",
                                 "Server refused to issue a binding ticket")
        return ticket, float(expires)

    async def resolve_intent(self, key: str, *, binding_id: str,
                             workspace_binding_id: str,
                             intent: str,
                             session_id: str | None = None,
                             new_session: bool = False,
                             text: str | None = None) -> IntentResolution:
        body: dict[str, object] = {
            "binding_id": binding_id,
            "workspace_binding_id": workspace_binding_id,
            "intent": intent,
            "new_session": new_session,
        }
        if session_id is not None:
            body["session_id"] = session_id
        if text is not None:
            body["text"] = text
        payload = await self._request(
            "POST", "/v1/runtime/intents:resolve", key=key, json_body=body,
            expect=(200, 409))
        if payload.get("error") is not None:
            raise _error_payload(payload, "intents:resolve")
        execution = payload.get("execution", {})
        lease = payload.get("lease", {})
        actions = execution.get("allowed_actions", [])
        return IntentResolution(
            operation_id=str(payload["operation_id"]),
            session_id=str(payload["session_id"]),
            reuse=bool(payload.get("reuse", False)),
            execution=execution if isinstance(execution, dict) else {},
            lease_seconds=float(lease.get("seconds", 120)),
            allowed_actions=tuple(str(item) for item in actions
                                  if isinstance(item, str)),
        )

    async def submit_operation(self, key: str, *, operation_id: str,
                               binding_id: str, session_id: str,
                               action: str, payload: dict[str, object]
                               ) -> dict[str, object]:
        return await self._request(
            "POST", "/v1/runtime/operations", key=key, json_body={
                "operation_id": operation_id,
                "binding_id": binding_id,
                "session_id": session_id,
                "action": action,
                "payload": payload,
            }, expect=(200, 202))

    async def get_r4_operation(self, key: str, operation_id: str) -> dict[str, object]:
        """Read canonical operation state with the negotiated R4 revision."""
        return await self._request(
            "GET", f"/v1/runtime/operations/{operation_id}", key=key, require_revision=True)

    async def get_r4_session(self, key: str, *, session_id: str, executor_id: str) -> dict:
        """Read a bounded canonical session view; never resolve or launch."""
        from urllib.parse import quote, urlencode
        from datetime import datetime
        if any(type(value) is not str or not 1 <= len(value) <= 160 for value in (session_id, executor_id)):
            raise ConnectorError("VALIDATION_ERROR", "session_read", "Invalid session selection.")
        payload = await self._request("GET", "/v1/runtime/sessions/" + quote(session_id, safe="") +
            "?" + urlencode({"executor_id": executor_id}), key=key, require_revision=True)
        ids = ("server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
               "workspace_binding_id", "session_id")
        revisions = ("session_owner_generation", "authorization_revision", "configuration_revision",
                     "binding_revision", "credential_epoch")
        fields = {"scope", "connection_generation", "lifecycle_state", "process_state", "ownership",
                  "lease_state", "durable_release_pending", "control_available", "last_observed_at"}
        scope = payload.get("scope")
        valid = (set(payload) == fields and type(scope) is dict and set(scope) == set(ids + revisions)
            and all(type(scope.get(name)) is str and 1 <= len(scope[name]) <= 160 for name in ids)
            and all(type(scope.get(name)) is int and scope[name] >= 1 for name in revisions)
            and scope["session_id"] == session_id and scope["executor_id"] == executor_id
            and all(type(payload.get(name)) is str and 1 <= len(payload[name]) <= 160
                    for name in ("lifecycle_state", "process_state", "ownership", "lease_state"))
            and all(type(payload.get(name)) is bool for name in ("durable_release_pending", "control_available"))
            and (payload["connection_generation"] is None or
                 type(payload["connection_generation"]) is int and payload["connection_generation"] >= 1))
        stamp = payload.get("last_observed_at")
        if stamp is not None:
            try:
                valid = valid and type(stamp) is str and len(stamp) <= 64 and datetime.fromisoformat(
                    stamp.replace("Z", "+00:00")).tzinfo is not None
            except ValueError:
                valid = False
        if not valid:
            raise ConnectorError("VERSION_INCOMPATIBLE", "session_read", "The Server returned an invalid session view.")
        return payload

    async def get_operation(self, key: str, operation_id: str
                            ) -> dict[str, object]:
        return await self._request(
            "GET", f"/v1/runtime/operations/{operation_id}", key=key)

    async def native_action(self, capability: R4SessionCapability, *, request) -> dict:
        """One bounded native-domain request, with no redirect or automatic retry."""
        import re
        from nexus_connector_core import CoreError
        from nexus_connector_core.native_action_bridge import native_action_request_body
        from nexus_connector_core.protocol import canonical_json, strict_json
        from .native_actions import native_capability_snapshot
        capability = native_capability_snapshot(capability)
        try:
            body = native_action_request_body(request, capability.scope)
        except CoreError as error:
            raise ConnectorError(error.code, 'native_action', 'Invalid native action request.') from None
        domain_action = {'context': 'handoff.get', 'claim': 'handoff.claim', 'complete': 'handoff.complete'}[body['action']]
        if request.capability_ref != capability.capability_ref or domain_action not in capability.actions:
            raise ConnectorError('BINDING_NOT_AUTHORIZED', 'native_action', 'The native action is outside the capability scope.')
        if time.monotonic() >= capability.deadline_monotonic:
            raise ConnectorError('AUTH_EXPIRED', 'native_action', 'The native capability has expired.')
        assert self._client is not None, "Use 'async with NexusHTTPClient'."
        mutation = body['action'] != 'context'

        def uncertain():
            return ConnectorError('OUTCOME_UNKNOWN' if mutation else 'EXECUTOR_OFFLINE', 'native_action',
                'The native action response could not be confirmed.', possible_effect=mutation,
                retry_safe=not mutation, operation_id=request.operation_id,
                action='Recover the same action ID and payload. Do not create a new action ID.')

        try:
            async with self._client.stream('POST', self.base_url + '/v1/runtime/native-actions',
                    headers={'Authorization': 'Bearer ' + capability.capability,
                             'Content-Type': 'application/json', 'Accept-Encoding': 'identity'},
                    content=canonical_json(body), follow_redirects=False, timeout=self._timeout) as response:
                if (response.is_redirect or
                        response.headers.get('X-Nexus-Connections-Revision') != MANAGEMENT_REVISION or
                        response.headers.get('Content-Encoding', 'identity').lower() != 'identity'):
                    raise uncertain()
                length = response.headers.get('Content-Length')
                if length is not None and (not length.isdecimal() or int(length) > 16384):
                    raise uncertain()
                parts, size = [], 0
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    size += len(chunk)
                    if size > 16384:
                        raise uncertain()
                    parts.append(chunk)
                data = strict_json(b''.join(parts).decode('utf-8', errors='strict'))
                if response.status_code != 200:
                    error = data.get('error') if type(data) is dict else None
                    if (response.status_code not in (400, 401, 403, 404, 409, 413, 422)
                            or type(error) is not dict
                            or type(error.get('code')) is not str
                            or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', error['code']) is None
                            or error.get('possible_effect') is not False
                            or error.get('retry_safe') is not False
                            or error.get('operation_id') not in (None, request.operation_id)):
                        raise uncertain()
                    raise ConnectorError(error['code'], 'native_action',
                        'The Server rejected the native action.', operation_id=request.operation_id)
                if (type(data) is not dict or set(data) != {'action_id', 'action', 'state', 'result'}
                        or data['action_id'] != request.operation_id or data['action'] != body['action']
                        or type(data['state']) is not str or not 1 <= len(data['state']) <= 160
                        or type(data['result']) is not dict or len(data['result']) > 64
                        or data['result'].get('handoff_id') != request.handoff_id
                        or data['result'].get('status') != data['state']
                        or {'jsonrpc', 'method', 'params', 'tools', 'mcpServers'} & data['result'].keys()):
                    raise uncertain()
                # A late response cannot refresh an expired credential or disclose
                # scoped content. A mutation may already have committed.
                if time.monotonic() >= capability.deadline_monotonic:
                    raise uncertain()
                return data['result']
        except ConnectorError:
            raise
        except httpx.HTTPError as error:
            if isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout)):
                raise ConnectorError('EXECUTOR_OFFLINE', 'native_action',
                    'The native action could not reach the Server.', retry_safe=True,
                    operation_id=request.operation_id) from None
            raise uncertain() from None
        except (ValueError, UnicodeError, RecursionError):
            raise uncertain() from None

    async def describe_r4_session_capability(self, key: str, *, frame: dict,
            capability_id: str, audience: str, actions: tuple[str, ...]) -> R4CapabilityMetadata:
        """Read current metadata with a fresh nonce; never mint or return material."""
        import secrets
        from urllib.parse import quote, urlencode
        from nexus_connector_core import decode_r4_frame, encode_r4_frame
        frame = decode_r4_frame(encode_r4_frame(frame))
        if (frame['type'] != 'operation.submit' or frame['action'] != 'runtime.open'
                or type(capability_id) is not str or not 1 <= len(capability_id) <= 160
                or audience not in ('nexus-mcp-session', 'nexus-native-session')
                or type(actions) is not tuple or len(actions) > 128
                or any(type(a) is not str or not 1 <= len(a) <= 160 for a in actions)
                or len(set(actions)) != len(actions)):
            raise ConnectorError('VALIDATION_ERROR', 'capability_metadata',
                                 'Invalid session capability metadata request.')
        scope = {k: frame[k] for k in ('server_id', 'executor_id', 'binding_id', 'agent_id',
            'workspace_id', 'workspace_binding_id', 'session_id', 'session_owner_generation',
            'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
        nonce = 'capmeta_' + secrets.token_hex(16)
        query = urlencode(dict(binding_id=scope['binding_id'], capability_id=capability_id, request_id=nonce))
        sent_at = time.monotonic()
        payload = await self._request('GET',
            '/v1/runtime/sessions/' + quote(scope['session_id'], safe='') + '/capability?' + query,
            key=key, require_revision=True)
        prefix = 'mcp-cap:' if audience == 'nexus-mcp-session' else 'native-cap:'
        if (set(payload) != {'request_id', 'capability_id', 'capability_ref', 'scope',
                'audience', 'actions', 'expires_in', 'lease_id', 'lease_serial', 'mcp_url'}
                or payload.get('request_id') != nonce or payload.get('capability_id') != capability_id
                or payload.get('capability_ref') != prefix + capability_id
                or payload.get('scope') != scope
                or any(type(payload['scope'].get(k)) is not type(v) for k, v in scope.items())
                or payload.get('audience') != audience or payload.get('actions') != sorted(actions)
                or type(payload.get('expires_in')) is not int or not 1 <= payload['expires_in'] <= 120
                or type(payload.get('lease_id')) is not str or not 1 <= len(payload['lease_id']) <= 160
                or type(payload.get('lease_serial')) is not int or payload['lease_serial'] < 1
                or payload.get('mcp_url') != (self.base_url + '/mcp' if audience == 'nexus-mcp-session' else None)):
            raise ConnectorError('VERSION_INCOMPATIBLE', 'capability_metadata',
                                 'The Server returned invalid capability metadata.')
        deadline = sent_at + payload['expires_in'] - .5
        if time.monotonic() >= deadline:
            raise ConnectorError('AUTH_EXPIRED', 'capability_metadata',
                                 'The capability metadata response arrived too late.')
        return R4CapabilityMetadata(capability_id, payload['capability_ref'], scope, audience,
            tuple(payload['actions']), deadline, payload['lease_id'], payload['lease_serial'], payload['mcp_url'])

    async def request_r4_session_capability(
            self, key: str, *, frame: dict, capability_request_id: str,
            audience: str, actions: tuple[str, ...],
            replaces_capability_id: str | None = None) -> R4SessionCapability:
        """Reserve one session secret from an immutable canonical opening.

        No automatic replacement or retry is safe after a lost issuance reply.
        The approved host persists the request identity before calling this port.
        """
        from nexus_connector_core import decode_r4_frame, encode_r4_frame
        frame = decode_r4_frame(encode_r4_frame(frame))
        if (frame['type'] != 'operation.submit' or frame['action'] != 'runtime.open' or
                audience not in ('nexus-mcp-session', 'nexus-native-session') or
                type(capability_request_id) is not str or not 1 <= len(capability_request_id) <= 160 or
                type(actions) is not tuple or len(actions) > 128 or
                any(type(a) is not str or not 1 <= len(a) <= 160 for a in actions) or
                len(set(actions)) != len(actions) or
                (replaces_capability_id is not None and
                 (type(replaces_capability_id) is not str or not 1 <= len(replaces_capability_id) <= 160))):
            raise ConnectorError('VALIDATION_ERROR', 'capability', 'Invalid R4 session capability request.')
        scope = {k: frame[k] for k in (
            'server_id', 'executor_id', 'binding_id', 'agent_id', 'workspace_id',
            'workspace_binding_id', 'session_id', 'session_owner_generation',
            'binding_revision', 'credential_epoch', 'authorization_revision', 'configuration_revision')}
        sent_at = time.monotonic()
        payload = await self._request('POST', f"/v1/runtime/sessions/{scope['session_id']}/capability",
            key=key, json_body=dict(capability_request_id=capability_request_id,
                binding_id=scope['binding_id'], audience=audience, actions=list(actions),
                replaces_capability_id=replaces_capability_id), require_revision=True)
        expected_fields = {'capability_id', 'capability_ref', 'capability', 'scope',
                           'audience', 'actions', 'expires_in', 'mcp_url'}
        prefix = 'mcp-cap:' if audience == 'nexus-mcp-session' else 'native-cap:'
        if (set(payload) != expected_fields or payload.get('scope') != scope or
                # Python bools compare equal to ints; preserve exact scope types.
                any(type(payload['scope'].get(k)) is not type(v) for k, v in scope.items()) or
                payload.get('audience') != audience or payload.get('actions') != sorted(actions) or
                type(payload.get('capability_id')) is not str or not 1 <= len(payload['capability_id']) <= 160 or
                payload.get('capability_ref') != prefix + payload['capability_id'] or
                type(payload.get('capability')) is not str or not 32 <= len(payload['capability']) <= 4096 or
                type(payload.get('expires_in')) is not int or not 1 <= payload['expires_in'] <= 120 or
                (audience == 'nexus-native-session' and payload.get('mcp_url') is not None) or
                (audience == 'nexus-mcp-session' and payload.get('mcp_url') != self.base_url + '/mcp')):
            raise ConnectorError('VERSION_INCOMPATIBLE', 'capability',
                                 'The Server returned an invalid R4 session capability.')
        deadline = sent_at + payload['expires_in'] - 0.5
        if time.monotonic() >= deadline:
            raise ConnectorError('AUTH_EXPIRED', 'capability', 'The session capability response arrived too late.')
        return R4SessionCapability(
            capability_id=payload['capability_id'], capability_ref=payload['capability_ref'],
            capability=payload['capability'], scope=dict(scope), audience=audience,
            actions=tuple(payload['actions']), expires_in=payload['expires_in'],
            deadline_monotonic=deadline, mcp_url=payload['mcp_url'])

    async def session_capability(self, key: str, *, binding_id: str,
                                 session_id: str,
                                 actions: tuple[str, ...]
                                 ) -> SessionCapability:
        payload = await self._request(
            "POST", f"/v1/runtime/sessions/{session_id}/capability",
            key=key, json_body={
                "binding_id": binding_id,
                "actions": list(actions),
            })
        ref = payload.get("capability_ref")
        capability = payload.get("capability")
        if (not isinstance(ref, str) or not ref.startswith("mcp-cap:") or
                not isinstance(capability, str) or not capability):
            raise ConnectorError("AGENT_AUTH_REQUIRED", "capability",
                                 "Server did not issue a session capability")
        return SessionCapability(ref, capability,
                                 float(payload.get("expires_in", 3600)))

    async def approval_decision(self, key: str, *, request_id: str,
                                decision: str, cas_token: str,
                                response: dict[str, object] | None = None
                                ) -> bool:
        payload = await self._request(
            "POST", "/v1/runtime/approval-decisions", key=key, json_body={
                "request_id": request_id,
                "decision": decision,
                "cas_token": cas_token,
                "response": response or {},
            }, expect=(200, 409))
        if payload.get("error") is not None:
            raise _error_payload(payload, "approval-decisions")
        applied = payload.get("applied")
        # CN5-01.04: ONLY a strictly boolean True authorizes the native
        # application — a string, object or absent value never becomes
        # authorization by truthiness, and False stays an explicit
        # refusal.
        if not isinstance(applied, bool):
            raise ConnectorError(
                "VERSION_INCOMPATIBLE", "approval-decisions",
                f"server returned non-boolean applied={applied!r}",
                action="The Server contract requires a boolean "
                       "confirmation; treat this reply as unknown.")
        return applied

    def link_url(self, executor_id: str) -> str:
        base = self.base_url
        scheme = "wss" if base.startswith("https") else "ws"
        return (base.replace("https", "wss", 1) if scheme == "wss"
                else base.replace("http", "ws", 1)) + \
            f"/v1/runtime/executors/{executor_id}/link"


def _proposal_from(payload: dict[str, object]) -> BindingProposal:
    required = ("proposal_id", "binding_id", "endpoint_id", "profile_id",
                "workspace_binding_id", "workspace_id")
    missing = [key for key in required
               if not isinstance(payload.get(key), str)
               or not payload.get(key)]
    if missing:
        raise ConnectorError("VERSION_INCOMPATIBLE", "bindings",
                             f"binding proposal missing fields: {missing}")
    return BindingProposal(
        proposal_id=str(payload["proposal_id"]),
        proposal_revision=int(payload.get("proposal_revision", 1)),
        binding_id=str(payload["binding_id"]),
        endpoint_id=str(payload["endpoint_id"]),
        profile_id=str(payload["profile_id"]),
        workspace_binding_id=str(payload["workspace_binding_id"]),
        workspace_id=str(payload["workspace_id"]),
        authorization_revision=int(payload.get("authorization_revision", 1)),
        configuration_revision=int(payload.get("configuration_revision", 1)),
    )


def _error_from(response: httpx.Response) -> ConnectorError:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        return _error_payload(payload, "http")
    hint = {401: "Import a valid canonical agent key.",
            403: "The canonical key lacks the required scope.",
            404: "The Server does not implement this contract route yet.",
            409: "A conflicting state exists; inspect it before retrying."}
    return ConnectorError(
        "EXECUTOR_OFFLINE" if response.status_code >= 500
        else "AGENT_AUTH_REQUIRED",
        "http", f"HTTP {response.status_code}",
        retry_safe=response.status_code >= 500 or response.status_code == 429,
        action=hint.get(response.status_code))


def _error_payload(payload: dict[str, object], stage: str) -> ConnectorError:
    error = payload["error"]
    if error.get('code') == 'CREDENTIAL_MATERIAL_UNAVAILABLE' and 'capability_id' in error:
        if (type(error['capability_id']) is not str or not 1 <= len(error['capability_id']) <= 160 or
                type(error.get('recovery_allowed')) is not bool):
            return ConnectorError('VERSION_INCOMPATIBLE', stage, 'Invalid capability recovery metadata.')
        return CapabilityMaterialUnavailable('CREDENTIAL_MATERIAL_UNAVAILABLE', 'capability',
            'The capability secret is returned only once.', capability_id=error['capability_id'],
            recovery_allowed=error['recovery_allowed'], action=
            'Request a replacement only after checking the current session state.'
            if error['recovery_allowed'] else 'Recover the existing executor configuration.')
    return ConnectorError(
        str(error.get("code", "UNKNOWN")), str(error.get("stage", stage)),
        redact_text(str(error.get("message", ""))),
        bool(error.get("possible_effect", False)),
        bool(error.get("retry_safe", False)),
        error.get("operation_id") if isinstance(
            error.get("operation_id"), str) else None,
        error.get("action") if isinstance(error.get("action"), str) else None)


def _ticket_error_from(response: httpx.Response) -> ConnectorError:
    try:
        payload = response.json()
    except ValueError:
        return ConnectorError("VERSION_INCOMPATIBLE", "binding_ticket",
                              "Invalid ticket recovery metadata.")
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return ConnectorError("VERSION_INCOMPATIBLE", "binding_ticket",
                              "Invalid ticket recovery metadata.")
    if error.get("code") != "CREDENTIAL_MATERIAL_UNAVAILABLE":
        return _error_from(response)
    ticket_id = error.get("ticket_id")
    if (type(ticket_id) is not str or not 5 <= len(ticket_id) <= 160
            or not ticket_id.startswith("ept_")
            or not all(c.isascii() and (c.isalnum() or c in "_-") for c in ticket_id)
            or error.get("stage") != "credential"
            or error.get("possible_effect") is not False
            or error.get("retry_safe") is not False
            or error.get("operation_id") is not None):
        return ConnectorError("VERSION_INCOMPATIBLE", "binding_ticket",
                              "Invalid ticket recovery metadata.")
    return TicketMaterialUnavailable(
        "CREDENTIAL_MATERIAL_UNAVAILABLE", "credential",
        "The ticket secret is returned only once.",
        action="Recover the persisted intent before requesting a replacement.",
        ticket_id=ticket_id)
