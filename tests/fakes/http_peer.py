"""Minimal asyncio HTTP/1.1 peer implementing the A.5 contract routes.

This is a *contract fake* for unit/integration/e2e tests — not a Nexus
Server. It speaks exactly the routes the Connector's HTTPS client uses,
with configurable agents, keys, tickets, capabilities and failure modes.
Loopback only, no TLS (the client allows http on loopback).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import urlsplit

_MAX_BODY = 1024 * 1024


@dataclass
class FakeAgent:
    agent_id: str
    key: str
    display_name: str = "Agent"
    server_id: str = "srv_fake"
    permissions: tuple[str, ...] = ("connect", "runtime")
    authorization_revision: int = 1
    configuration_revision: int = 1
    revoked: bool = False


@dataclass
class FakeBinding:
    binding_id: str
    agent_id: str
    alias: str
    adapter_id: str
    workspace_id: str = "ws_1"
    workspace_binding_id: str = "wb_1"
    endpoint_id: str = "ep_1"
    profile_id: str = "prof_1"
    proposal_revision: int = 1
    authorization_revision: int = 1
    configuration_revision: int = 1


@dataclass
class FakeIntent:
    operation_id: str
    session_id: str
    binding: FakeBinding
    intent: str
    text: str | None = None
    resolved_at: float = field(default_factory=time.time)


class FakeNexusHTTPPeer:
    """Bindable loopback HTTP peer with the /v1 contract surface."""

    def __init__(self, *, server_id: str = "srv_fake"):
        self.server_id = server_id
        self.agents: dict[str, FakeAgent] = {}
        self.bindings: dict[str, FakeBinding] = {}
        self.proposals: dict[str, FakeBinding] = {}
        self.intents: dict[str, FakeIntent] = {}
        self.tickets: dict[str, tuple[str, str, float]] = {}  # ticket→(agent,binding,exp)
        self.revoked_binding_ids: set[str] = set()
        self.capabilities: dict[str, str] = {}
        self.approvals: dict[str, dict[str, object]] = {}
        self.operations: dict[str, dict[str, object]] = {}
        self.session_counter = 0
        self.fail_modes: dict[str, Callable[[dict], None]] = {}
        self.requests: list[tuple[str, str, dict | None]] = []
        self.redirect_cross_origin = False
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0

    # -- lifecycle -------------------------------------------------------

    async def start(self) -> str:
        self.server = await asyncio.start_server(
            self._client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{self.port}"

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    def add_agent(self, agent: FakeAgent) -> None:
        self.agents[agent.key] = agent

    def authenticate(self, headers: dict[str, str]) -> FakeAgent:
        auth = headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise _HttpError(401, "AGENT_AUTH_REQUIRED", "auth",
                             "missing bearer credential")
        key = auth[len("Bearer "):]
        agent = self.agents.get(key)
        if agent is None:
            raise _HttpError(401, "AGENT_AUTH_REQUIRED", "auth",
                             "unknown or rejected credential")
        if agent.revoked:
            raise _HttpError(403, "AGENT_REVOKED", "auth",
                             "credential revoked")
        return agent

    # -- server internals --------------------------------------------------

    async def _client(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    return
                parts = request_line.decode("latin-1").strip().split(" ")
                if len(parts) != 3:
                    return
                method, target, _version = parts
                headers: dict[str, str] = {}
                while True:
                    line = await reader.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                    name, _, value = line.decode("latin-1").partition(":")
                    headers[name.strip().lower()] = value.strip()
                body: dict | None = None
                length = int(headers.get("content-length", "0") or 0)
                if length:
                    if length > _MAX_BODY:
                        return
                    raw = await reader.readexactly(length)
                    if raw:
                        try:
                            body = json.loads(raw.decode("utf-8"))
                        except ValueError:
                            body = None
                self.requests.append((method, target, body))
                try:
                    status, payload = self._route(method, target, headers,
                                                  body)
                    headers_out = ("Content-Type: application/json\r\n"
                                   f"Content-Length: {len(json.dumps(payload))}"
                                   "\r\nConnection: keep-alive\r\n")
                    blob = json.dumps(payload).encode("utf-8")
                except _Redirect as redirect:
                    status = 302
                    blob = b""
                    headers_out = (f"Location: {redirect.location}\r\n"
                                   "Content-Length: 0\r\n"
                                   "Connection: keep-alive\r\n")
                except _HttpError as error:
                    status, payload = error.status, {
                        "error": {"code": error.code, "stage": error.stage,
                                  "message": error.message,
                                  "retry_safe": error.retry_safe}}
                    blob = json.dumps(payload).encode("utf-8")
                    headers_out = ("Content-Type: application/json\r\n"
                                   f"Content-Length: {len(blob)}"
                                   "\r\nConnection: keep-alive\r\n")
                writer.write(
                    f"HTTP/1.1 {status} {_PHRASES.get(status, 'OK')}\r\n"
                    f"{headers_out}\r\n".encode("latin-1") + blob)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    def _route(self, method: str, target: str, headers: dict[str, str],
               body: dict | None):
        path = urlsplit(target).path
        hook = self.fail_modes.get(path)
        if hook is not None and body is not None:
            hook(body)
        if self.redirect_cross_origin and path == "/v1/connections/me":
            self.redirect_cross_origin = False  # one-shot for the test
            raise _Redirect("https://evil.example.net/v1/connections/me")
        if path == "/v1/connections/me" and method == "GET":
            return self._me(headers)
        if path == "/v1/connections/bindings:prepare" and method == "POST":
            return self._prepare(headers, body or {})
        if path == "/v1/connections/bindings:apply" and method == "POST":
            return self._apply(headers, body or {})
        match = re.fullmatch(r"/v1/connections/bindings/([^/]+)/ticket", path)
        if match and method == "POST":
            return self._ticket(headers, match.group(1))
        if path == "/v1/runtime/intents:resolve" and method == "POST":
            return self._resolve(headers, body or {})
        if path == "/v1/runtime/operations" and method == "POST":
            return self._operation(headers, body or {})
        match = re.fullmatch(r"/v1/runtime/operations/([^/]+)", path)
        if match and method == "GET":
            return 200, self.operations.get(match.group(1), {
                "operation_id": match.group(1), "stage": "OUTCOME_UNKNOWN",
                "possible_effect": True})
        match = re.fullmatch(r"/v1/runtime/sessions/([^/]+)/capability", path)
        if match and method == "POST":
            return self._capability(headers, match.group(1), body or {})
        if path == "/v1/runtime/approval-decisions" and method == "POST":
            return self._approval(headers, body or {})
        raise _HttpError(404, "CAPABILITY_UNSUPPORTED", "routing",
                         f"no contract route {path}")

    # -- routes -----------------------------------------------------------

    def _me(self, headers) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        return 200, {
            "agent_id": agent.agent_id,
            "server_id": agent.server_id or self.server_id,
            "display_name": agent.display_name,
            "permissions": list(agent.permissions),
            "revisions": {"authorization": agent.authorization_revision,
                          "configuration": agent.configuration_revision},
        }

    def _prepare(self, headers, body) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        hint = body.get("agent_id_hint")
        if hint and hint != agent.agent_id:
            raise _HttpError(403, "AGENT_ID_MISMATCH", "prepare",
                             "hint does not match the authenticated agent")
        executor = body.get("executor", {})
        alias = body.get("binding_alias", "binding")
        binding = FakeBinding(
            binding_id=f"bind_{len(self.bindings) + 1:04d}",
            agent_id=agent.agent_id,
            alias=str(alias),
            adapter_id=str(executor.get("adapter_id", "unknown")))
        proposal_id = f"prop_{len(self.proposals) + 1:04d}"
        self.proposals[proposal_id] = binding
        return 200, {
            "proposal_id": proposal_id,
            "proposal_revision": 1,
            "binding_id": binding.binding_id,
            "endpoint_id": binding.endpoint_id,
            "profile_id": binding.profile_id,
            "workspace_binding_id": binding.workspace_binding_id,
            "workspace_id": binding.workspace_id,
            "authorization_revision": binding.authorization_revision,
            "configuration_revision": binding.configuration_revision,
        }

    def _apply(self, headers, body) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        proposal_id = str(body.get("proposal_id", ""))
        binding = self.proposals.get(proposal_id)
        if binding is None:
            raise _HttpError(409, "OPERATION_CONFLICT", "apply",
                             "unknown proposal")
        if binding.agent_id != agent.agent_id:
            raise _HttpError(403, "BINDING_NOT_AUTHORIZED", "apply",
                             "proposal belongs to another agent")
        revision = int(body.get("proposal_revision", 1))
        if revision != 1:
            raise _HttpError(409, "OPERATION_CONFLICT", "apply",
                             "stale proposal revision")
        self.bindings[binding.binding_id] = binding
        return 200, {
            "proposal_id": proposal_id,
            "proposal_revision": 1,
            "binding_id": binding.binding_id,
            "endpoint_id": binding.endpoint_id,
            "profile_id": binding.profile_id,
            "workspace_binding_id": binding.workspace_binding_id,
            "workspace_id": binding.workspace_id,
            "authorization_revision": binding.authorization_revision,
            "configuration_revision": binding.configuration_revision,
        }

    def _ticket(self, headers, binding_id: str) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        binding = self.bindings.get(binding_id)
        if binding is None:
            raise _HttpError(404, "BINDING_NOT_AUTHORIZED", "ticket",
                             "unknown binding")
        if binding.agent_id != agent.agent_id:
            raise _HttpError(403, "BINDING_NOT_AUTHORIZED", "ticket",
                             "binding belongs to another agent")
        if binding_id in self.revoked_binding_ids:
            raise _HttpError(403, "AGENT_REVOKED", "ticket",
                             "binding revoked")
        ticket = f"nstkt_{binding_id}_{int(time.time() * 1000):x}"
        self.tickets[ticket] = (agent.agent_id, binding_id,
                                time.time() + 600)
        return 200, {"ticket": ticket, "expires_in": 600}

    def _resolve(self, headers, body) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        binding = self.bindings.get(str(body.get("binding_id", "")))
        if binding is None:
            raise _HttpError(404, "BINDING_NOT_AUTHORIZED", "resolve",
                             "unknown binding")
        if binding.agent_id != agent.agent_id:
            raise _HttpError(403, "BINDING_NOT_AUTHORIZED", "resolve",
                             "binding belongs to another agent")
        intent = str(body.get("intent", ""))
        if intent == "runtime.start":
            self.session_counter += 1
            session_id = f"rs_{self.session_counter:06d}"
            operation_id = f"op_open_{self.session_counter:06d}"
        else:
            session_id = str(body.get("session_id", ""))
            if not session_id:
                raise _HttpError(409, "AMBIGUOUS_BINDING", "resolve",
                                 "session required for this intent")
            operation_id = f"op_{intent.replace('.', '_')}_" \
                           f"{int(time.time() * 1000):x}"
        allowed = ["runtime.open", "turn.submit", "turn.interrupt",
                   "runtime.close", "turn.steer", "approval.decide",
                   "input.provide"]
        self.intents[operation_id] = FakeIntent(
            operation_id, session_id, binding, intent,
            body.get("text") if isinstance(body.get("text"), str) else None)
        return 200, {
            "operation_id": operation_id,
            "session_id": session_id,
            "reuse": False,
            "execution": {
                "server_id": agent.server_id or self.server_id,
                "authorization_revision": binding.authorization_revision,
                "configuration_revision": binding.configuration_revision,
                "session_owner_generation": 1,
                "allowed_actions": allowed,
            },
            "lease": {"seconds": 120},
        }

    def _operation(self, headers, body) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        operation_id = str(body.get("operation_id", ""))
        self.operations[operation_id] = {
            "operation_id": operation_id,
            "session_id": body.get("session_id"),
            "stage": "RECEIVED_DURABLE",
            "possible_effect": False,
            "agent_id": agent.agent_id,
        }
        return 202, self.operations[operation_id]

    def _capability(self, headers, session_id: str, body
                    ) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        binding = self.bindings.get(str(body.get("binding_id", "")))
        if binding is None or binding.agent_id != agent.agent_id:
            raise _HttpError(403, "BINDING_NOT_AUTHORIZED", "capability",
                             "binding not owned by this agent")
        ref = f"mcp-cap:{session_id}"
        capability = f"nxcap_{session_id}_{agent.agent_id}"
        self.capabilities[ref] = capability
        return 200, {"capability_ref": ref, "capability": capability,
                     "expires_in": 3600}

    def _approval(self, headers, body) -> tuple[int, dict]:
        agent = self.authenticate(headers)
        request_id = str(body.get("request_id", ""))
        record = self.approvals.get(request_id)
        if record is None:
            raise _HttpError(409, "OPERATION_CONFLICT", "approval",
                             "unknown request")
        if record.get("agent_id") not in (None, agent.agent_id):
            raise _HttpError(403, "APPROVAL_REQUIRED", "approval",
                             "this request requires a different authority")
        if record.get("answered"):
            raise _HttpError(409, "OPERATION_CONFLICT", "approval",
                             "already answered (CAS)")
        record["answered"] = True
        record["decision"] = body.get("decision")
        return 200, {"applied": True}

    # -- test controls ------------------------------------------------------

    def add_approval(self, request_id: str, *, agent_id: str | None = None,
                     require_operator: bool = False) -> None:
        self.approvals[request_id] = {
            "agent_id": None if require_operator else agent_id,
            "require_operator": require_operator,
        }


class _HttpError(Exception):
    def __init__(self, status: int, code: str, stage: str, message: str,
                 *, retry_safe: bool = False):
        self.status = status
        self.code = code
        self.stage = stage
        self.message = message
        self.retry_safe = retry_safe


_PHRASES = {200: "OK", 202: "Accepted", 302: "Found", 401: "Unauthorized",
            403: "Forbidden", 404: "Not Found", 409: "Conflict"}


class _Redirect(Exception):
    def __init__(self, location: str):
        self.location = location
