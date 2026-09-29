"""CN5-01: internal approval DTOs — operational copy vs. display view.

The receiving path used to store ``redact_mapping(proposal)``, which
mangles the Core's correlation evidence (``request_hash`` is a 64-hex
string by contract) before the decision ever reached the Core. The
operational proposal is now kept as an immutable deep copy — never
re-hashed, never re-calculated — while ONLY the presentation projection
is redacted. The public projection never serializes these DTOs whole.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from nexus_connector_core import SessionKey

from ..redaction import redact_mapping

#: CN5-02: terminal tombstone bound (entries), per the existing policy.
TOMBSTONE_LIMIT = 128

#: Phase vocabulary of the decision state machine (CN5-02.02).
PENDING = "pending"
SERVER_PENDING = "server_pending"
SERVER_UNKNOWN = "server_unknown"
SERVER_REFUSED = "server_refused"
SERVER_CONFIRMED = "server_confirmed"
NATIVE_PENDING = "native_pending"
NATIVE_REFUSED = "native_refused"
NATIVE_UNKNOWN = "native_unknown"
APPLIED = "applied"

#: Phases whose record claims an ACTIVE producer (CN5-02.03: status
#: must reflect the task's real existence).
PRODUCER_PHASES = frozenset({SERVER_PENDING, NATIVE_PENDING})


@dataclass(frozen=True, slots=True)
class ApprovalKey:
    """The immutable identity of one approval request (full key)."""
    server_id: str
    executor_id: str
    request_id: str


@dataclass(frozen=True, slots=True)
class ApprovalTarget:
    """The full resource scope of one approval request.

    ``session_key`` is ``None`` ONLY for administrative (Server-side)
    requests — identified by their contract, never by a lookup failure.
    The generation/revision snapshot was authorized when the request
    was captured and is re-checked before and after the Server POST.
    """
    key: ApprovalKey
    binding_id: str
    agent_id: str
    workspace_id: str
    session_key: SessionKey | None = None
    connection_generation: int | None = None
    session_owner_generation: int | None = None
    authorization_revision: int = 1
    configuration_revision: int = 1
    #: The observed request KIND (drives input.provide vs approval.decide).
    kind: str | None = None


def canonical_proposal_digest(proposal: Mapping[str, Any]) -> str:
    """A stable digest of the operational proposal (conflict test)."""
    blob = json.dumps(proposal, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _frozen_copy(value: Any) -> Any:
    """Deep copy that hardens the operational record against later
    mutation of the received frame (CN5-01.01 acceptance)."""
    cloned = copy.deepcopy(value)
    if isinstance(cloned, dict):
        return {str(k): _frozen_copy(v) for k, v in cloned.items()}
    if isinstance(cloned, list):
        return [_frozen_copy(v) for v in cloned]
    return cloned


@dataclass(slots=True)
class DecisionAttempt:
    """One owned decision attempt over one request (CN5-02)."""
    attempt_id: str
    key: ApprovalKey
    target: ApprovalTarget
    local_decision: str              # operator vocabulary (approve/deny)
    core_decision: str               # Core vocabulary (accept/decline)
    intent_digest: str
    native_operation_id: str
    producer_task: object | None = None
    phase: str = PENDING
    server_confirmed: bool = False
    error_code: str | None = None
    error_stage: str | None = None
    result: dict[str, Any] | None = None


@dataclass(slots=True)
class PendingApproval:
    """A received request: operational copy + redacted display view."""
    target: ApprovalTarget
    #: Immutable deep copy of the VALIDATED operational payload — the
    #: correlation evidence (request_id/request_hash/method/params)
    #: reaches the Core exactly as observed. Never re-calculated.
    proposal: Mapping[str, Any]
    display_proposal: object
    kind: str | None
    received_at: str
    #: The session id AS RECEIVED in the r3 frame (kept for target
    #: resolution at decide time; None on administrative requests).
    frame_session_id: str | None = None
    attempt: DecisionAttempt | None = None

    @classmethod
    def receive(cls, *, target: ApprovalTarget, proposal: object,
                kind: str | None, received_at: str,
                frame_session_id: str | None = None
                ) -> "PendingApproval":
        operational = _frozen_copy(
            proposal if isinstance(proposal, Mapping) else {})
        return cls(
            target=target,
            proposal=operational,
            frame_session_id=frame_session_id,
            display_proposal=redact_mapping(operational),
            kind=kind,
            received_at=received_at)

    def proposal_digest(self) -> str:
        return canonical_proposal_digest(self.proposal)

    def to_public_dict(self) -> dict[str, Any]:
        """The ONLY projection served to IPC/logs/export (CN5-01.01):
        ids needed for selection plus the phase — content redacted."""
        record: dict[str, Any] = {
            "request_id": self.target.key.request_id,
            "server_id": self.target.key.server_id,
            "executor_id": self.target.key.executor_id,
            "binding_id": self.target.binding_id,
            "agent_id": self.target.agent_id,
            "workspace_id": self.target.workspace_id,
            "kind": self.kind,
            "received_at": self.received_at,
            "phase": self.attempt.phase if self.attempt else PENDING,
            "awaiting": "operator" if self.attempt is None else
                        "decision",
            "proposal": self.display_proposal,
        }
        if self.target.session_key is not None:
            record["session_id"] = self.target.session_key.session_id
        return record


def native_operation_id(key: ApprovalKey, decision: str) -> str:
    """Derive the Core operation id ONCE per intent (CN5-02.04): a
    canonical serialization of the full namespace + request id +
    decision identity, bounded for the Core's id contract. No retry
    mints a new id."""
    blob = json.dumps(
        [key.server_id, key.executor_id, key.request_id, decision],
        separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:20]
    readable = "".join(
        char if char.isalnum() else "_" for char in key.request_id)[:24]
    return f"op_appr_{readable}_{digest}"
