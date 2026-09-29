"""``discover`` — redacted local inventory (plan 6, TC-11)."""

from __future__ import annotations

from pathlib import Path

from ...errors import ConnectorError
from ...services.discovery_service import (
    discover_inventory, pi_release_candidates,
)
from ..output import Output


async def run_discover(args, output: Output, root: Path):
    harness = getattr(args, "harness", None)
    entries = await discover_inventory([harness] if harness else None)
    payload: list[dict[str, object]] = [entry.to_json() for entry in entries]
    notes = ["selection is an explicit connect-time decision; PATH "
             "candidates are not trusted automatically"]
    pi_root = getattr(args, "pi_releases_root", None)
    pi_node = getattr(args, "pi_node", None)
    release_candidates: list = []
    if pi_root:
        if not pi_node:
            raise ConnectorError("VALIDATION_ERROR", "discover",
                                 "--pi-releases-root requires --pi-node",
                                 action="Name the trusted Node executable "
                                        "that pairs the releases.")
        from pathlib import Path as P
        trusted = (P(pi_root).expanduser().absolute(),)
        for candidate in pi_release_candidates(
                P(pi_root).expanduser(), P(pi_node).expanduser(),
                trusted_roots=trusted):
            release_candidates.append(candidate)
            payload.append({
                "adapter_id": candidate.adapter_id, "label": "Pi RPC",
                "executable": candidate.executable, "source": "releases",
                "trust": "selected",
                "fingerprint": candidate.fingerprint,
                "version": candidate.version,
                "architecture": candidate.architecture,
                "build_identity": candidate.build_identity,
                "launch_script": candidate.launch_script,
            })
        notes.append("Pi release layouts are enumerated passively; the "
                     "Node executable you named is the trusted pair")
    # CN4-04.01 (G01/P04): the DISCOVER flow publishes the versioned
    # technical availability snapshot of THIS executor (Core-assessed)
    # over the FULL Core candidates — the same service function the
    # daemon serves over IPC; the Pi pair the operator named enters
    # the SAME effective inventory (never a re-created candidate that
    # would drop the CLI identity/version/build).
    snapshot = None
    try:
        from ...services.discovery_service import (
            availability_snapshot, inventory_candidates,
        )
        candidates = await inventory_candidates(
            extra=release_candidates)
        snapshot = availability_snapshot(candidates)
    except Exception:
        snapshot = None
    output.line(f"{len(payload)} candidate(s); discovery executes nothing "
                "and authorizes nothing")
    result = {"candidates": payload, "note": " ".join(notes)}
    if snapshot is not None:
        result["availability"] = snapshot
    return result
