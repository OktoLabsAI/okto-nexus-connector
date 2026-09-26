"""Process/syscall-free helpers kept outside Core's redaction boundary.

The connector redacts values it knows are secret (canonical keys, tickets,
capabilities) before they reach logs, IPC responses, exports or child argv.
Core scrubs native adapter output; this module scrubs connector-owned
surfaces. ``redact_secrets`` is applied to every IPC response and log line.
"""

from __future__ import annotations

import re
from typing import Iterable

# Bearer tokens, our ticket/capability prefixes and long hex/base64 blobs.
_BEARER = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
_TICKET = re.compile(r"\b\w{0,3}tkt_[A-Za-z0-9._-]{6,}")
_CAP = re.compile(r"(?:mcp-cap|native-cap)(?::|_)[A-Za-z0-9._:-]{4,}")
_KEY = re.compile(r"nxs_[A-Za-z0-9._-]{16,}")
_HEX64 = re.compile(r"\b[0-9a-f]{64}\b", re.IGNORECASE)

_REDACTED = "[redacted]"


def redact_text(value: str, extra: Iterable[str] = ()) -> str:
    """Redact known secret shapes and any caller-supplied literals."""
    for secret in extra:
        if isinstance(secret, str) and secret:
            value = value.replace(secret, _REDACTED)
    value = _BEARER.sub(r"\1" + _REDACTED, value)
    value = _TICKET.sub(_REDACTED, value)
    value = _CAP.sub(_REDACTED, value)
    value = _KEY.sub(_REDACTED, value)
    value = _HEX64.sub(_REDACTED, value)
    return value


def redact_mapping(value: object, extra: Iterable[str] = ()) -> object:
    """Recursively redact strings inside JSON-like structures."""
    secrets = [item for item in extra if isinstance(item, str) and item]
    if isinstance(value, str):
        return redact_text(value, secrets)
    if isinstance(value, dict):
        return {key: redact_mapping(item, secrets)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_mapping(item, secrets) for item in value]
    return value
