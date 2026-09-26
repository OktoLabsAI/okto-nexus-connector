"""Local harness inventory via the Core, without executing candidates.

Discovery never authorizes anything and never collects secrets (plan 2.3,
TC-11): candidates found on PATH or explicit paths are listed redacted;
selection happens through explicit operator choice at connect time. The
Core's sealed version probes are used only for explicitly selected
candidates.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from nexus_connector_core import CoreError, DiscoveryRequest, Inventory
from nexus_connector_core.discovery import (
    candidate as make_candidate,
    discover_path,
)

from ..errors import ConnectorError

ADAPTERS = ("codex_app_server", "pi_rpc", "claude_stream")
ADAPTER_LABELS = {
    "codex_app_server": "Codex app-server",
    "pi_rpc": "Pi RPC",
    "claude_stream": "Claude Code stream-json",
}


@dataclass(slots=True)
class InventoryEntry:
    adapter_id: str
    executable: str
    source: str
    trust: str
    fingerprint: str
    version: str | None
    architecture: str | None

    def to_json(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "label": ADAPTER_LABELS.get(self.adapter_id, self.adapter_id),
            "executable": self.executable,
            "source": self.source,
            "trust": self.trust,
            "fingerprint": self.fingerprint,
            "version": self.version,
            "architecture": self.architecture,
        }


async def discover_inventory(adapter_ids=ADAPTERS) -> list[InventoryEntry]:
    entries: list[InventoryEntry] = []
    for adapter_id in adapter_ids:
        try:
            candidates = await asyncio.to_thread(
                discover_path, adapter_id)
        except CoreError:
            continue
        for found in candidates:
            entries.append(InventoryEntry(
                adapter_id=found.adapter_id,
                executable=found.executable,
                source=found.source,
                trust=found.trust,
                fingerprint=found.fingerprint,
                version=found.version,
                architecture=found.architecture))
    return entries


def select_explicit(adapter_id: str, executable: str | Path):
    """Build a selected candidate from an explicit operator choice."""
    try:
        return make_candidate(adapter_id, executable, explicit=True)
    except CoreError as error:
        raise ConnectorError(error.code, "selection",
                             str(error), retry_safe=error.retry_safe) from None


async def probe_version(selected) -> str | None:
    """Sealed, secret-free ``--version`` probe for one selected candidate."""
    import os
    from nexus_connector_core.discovery import (
        probe_selected_claude, probe_selected_codex, probe_selected_pi,
    )
    # Only whitelisted essentials reach the probe; never credentials.
    essentials = frozenset({"SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT",
                            "PATH", "TEMP", "TMP", "LANG", "LC_ALL",
                            "TERM"})
    env = {key: value for key, value in os.environ.items()
           if key.upper() in essentials}
    try:
        if selected.adapter_id == "codex_app_server":
            probed = await probe_selected_codex(selected, cwd=Path.cwd(),
                                                env=env)
        elif selected.adapter_id == "claude_stream":
            probed = await probe_selected_claude(selected, cwd=Path.cwd(),
                                                 env=env)
        elif selected.adapter_id == "pi_rpc":
            probed = await probe_selected_pi(selected, cwd=Path.cwd(),
                                             env=env)
        else:
            return None
        return probed.version
    except CoreError as error:
        raise ConnectorError(error.code, "probe", str(error)) from None
    except Exception:
        # A probe that cannot run or parse simply yields no version;
        # selection and qualification happen through the Core anyway.
        return None


def pi_candidate(node: str | Path, script: str | Path):
    """Composite Node+CLI selection for Pi (fingerprint binds both)."""
    from nexus_connector_core.discovery import candidate_pi_node_cli
    try:
        return candidate_pi_node_cli(node, script, explicit=True)
    except CoreError as error:
        raise ConnectorError(error.code, "selection", str(error)) from None
