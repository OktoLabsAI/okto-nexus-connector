"""Outbound HTTPS client for the Nexus Server management API.

This is the Connector-side client for the contract routes of plan A.5
(`/v1/connections/*`, `/v1/runtime/*`). It never proxies MCP and never
forwards provider secrets. TLS is required outside explicit loopback
development; redirects to another origin are refused for credentialed
requests (A.15).
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from ..errors import ConnectorError
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


@dataclass(frozen=True, slots=True)
class InventoryAccepted:
    server_id: str
    executor_id: str
    publication_sequence: int
    inventory_revision: str
    fresh_for_ms: int


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
                        require_revision: bool = False) -> dict[str, object]:
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
                    "link:connect", "inventory:publish"} or
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

    async def get_operation(self, key: str, operation_id: str
                            ) -> dict[str, object]:
        return await self._request(
            "GET", f"/v1/runtime/operations/{operation_id}", key=key)

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
    return ConnectorError(
        str(error.get("code", "UNKNOWN")), str(error.get("stage", stage)),
        redact_text(str(error.get("message", ""))),
        bool(error.get("possible_effect", False)),
        bool(error.get("retry_safe", False)),
        error.get("operation_id") if isinstance(
            error.get("operation_id"), str) else None,
        error.get("action") if isinstance(error.get("action"), str) else None)
