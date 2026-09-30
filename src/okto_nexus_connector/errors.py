"""Typed connector failures with stable codes (plan A.17 vocabulary).

Every mutable path raises ``ConnectorError`` with a code, stage, honest
``possible_effect``/``retry_safe`` flags and an optional corrective action.
Secrets must never appear in ``message`` or ``action``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ConnectorError(Exception):
    code: str
    stage: str
    message: str = ""
    possible_effect: bool = False
    retry_safe: bool = False
    operation_id: str | None = None
    action: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        if self.action:
            return f"{self.code} ({self.stage}): {self.message} -- {self.action}"
        return f"{self.code} ({self.stage}): {self.message}"

    def to_json(self) -> dict[str, object]:
        return {
            "code": self.code,
            "stage": self.stage,
            "message": self.message,
            "possible_effect": self.possible_effect,
            "retry_safe": self.retry_safe,
            "operation_id": self.operation_id,
            "action": self.action,
        }

    @staticmethod
    def from_json(payload: dict[str, object]) -> "ConnectorError":
        return ConnectorError(
            str(payload.get("code", "UNKNOWN")),
            str(payload.get("stage", "unknown")),
            str(payload.get("message", "")),
            bool(payload.get("possible_effect", False)),
            bool(payload.get("retry_safe", False)),
            payload.get("operation_id"),  # type: ignore[arg-type]
            payload.get("action") if payload.get("action") is not None else None,  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class CapabilityMaterialUnavailable(ConnectorError):
    """Non-secret recovery metadata; permission is rechecked on replacement."""

    capability_id: str = ""
    recovery_allowed: bool = False

    def to_json(self) -> dict[str, object]:
        return ConnectorError.to_json(self) | {
            'capability_id': self.capability_id, 'recovery_allowed': self.recovery_allowed}


@dataclass(slots=True)
class TicketMaterialUnavailable(ConnectorError):
    """The Server retained the request but cannot return its secret again."""

    ticket_id: str = ""

    def to_json(self) -> dict[str, object]:
        return ConnectorError.to_json(self) | {"ticket_id": self.ticket_id}


def auth_required(stage: str, message: str = "") -> ConnectorError:
    return ConnectorError("AGENT_AUTH_REQUIRED", stage, message,
                          action="Import a valid canonical agent key with "
                                 "'okto-nexus-connector identity add'.")


def agent_mismatch(stage: str, message: str) -> ConnectorError:
    return ConnectorError(
        "AGENT_ID_MISMATCH", stage, message,
        action="The imported key belongs to a different agent than the "
               "requested hint. Aborting; the canonical registration was "
               "not changed.")


def daemon_unreachable(stage: str, message: str) -> ConnectorError:
    return ConnectorError("DAEMON_UNAVAILABLE", stage, message, retry_safe=True,
                          action="Start it with 'okto-nexus-connector daemon start'.")
