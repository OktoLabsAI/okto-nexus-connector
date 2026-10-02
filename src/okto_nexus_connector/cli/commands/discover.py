"""``discover`` — redacted local inventory (plan 6, TC-11)."""

from __future__ import annotations

from pathlib import Path

from nexus_connector_core import CoreError

from ...errors import ConnectorError
from ...services.discovery_service import (
    _entry_of, availability_snapshot, inventory_candidates, pi_release_candidates,
)
from ..output import Output


def _human_status(row):
    reasons = row.get("reasons", ())
    if ("containment_unavailable:platform" in reasons or
            any(reason.startswith("platform_not_implemented:") for reason in reasons)):
        return "Unsupported on this OS"
    if row.get("state") == "NOT_INSTALLED":
        return "Not detected"
    if row.get("containment") == "unavailable":
        return "Host check failed"
    if row.get("state") == "NOT_PROBED" and "selection_required" in reasons:
        return "Needs selection and probe"
    return {
        "UNSUPPORTED_PLATFORM": "Unsupported on this OS",
        "NOT_PROBED": "Needs version check",
        "UNQUALIFIED_BUILD": "Build not qualified",
        "CONTAINMENT_UNAVAILABLE": "Host check failed",
        "PREPARATION_REQUIRED": "Needs local preparation",
        "READY_FOR_RUNTIME": "Technically ready",
    }.get(row.get("state"), "Needs review")


def render_summary(result, output: Output, *, harness=None):
    """One human-readable row per Core-assessed family; no new discovery."""
    candidates = result["candidates"]
    families = {}
    for row in result["availability"]["rows"]:
        adapter = row["adapter_id"]
        if harness and adapter != harness:
            continue
        family = families.setdefault(adapter, {"name": row["display_name"], "states": set()})
        family["states"].add(_human_status(row))
    rows = []
    for adapter, family in families.items():
        count = sum(candidate["adapter_id"] == adapter for candidate in candidates)
        states = family["states"]
        status = next(iter(states)) if len(states) == 1 else "Mixed readiness; see --verbose"
        rows.append((family["name"], str(count), status))
    headers = ("Harness", "Found", "Status")
    widths = [max(len(row[index]) for row in [headers, *rows]) for index in range(3)]
    def line(row):
        return "  ".join(value.ljust(width) for value, width in zip(row, widths)).rstrip()
    output.line(line(headers))
    output.line(line(tuple("-" * width for width in widths)))
    for row in rows:
        output.line(line(row))
    output.line("")
    output.line("Discovery starts nothing; runtime approval is separate.")
    if not candidates:
        output.line("No installations detected. Check local CLI paths and discovery configuration.")
    output.line("Use --verbose for installation details, or --json for automation.")


def _assessment(candidates):
    try:
        return availability_snapshot(candidates)
    except CoreError as error:
        raise ConnectorError(error.code, "discovery", str(error),
                             retry_safe=error.retry_safe) from None


def _empty_hint(*, configured: bool) -> str:
    scope = (
        "Review executor show SERVER_ID and executor configure-discovery --help; "
        "approved discovery roots must also be on PATH."
        if configured else
        "Check the provider CLI location on this computer (PowerShell: "
        "Get-Command codex,claude,pi,node -ErrorAction SilentlyContinue). "
        "For a registered executor, use executor configure-discovery --help "
        "and discover --server-id SERVER_ID to preview its approved roots."
    )
    return (
        "No local candidate was found under the current discovery policy. "
        "This command does not discover Nexus servers or other computers on the network. "
        + scope + " Discovery does not execute providers or grant runtime authorization."
    )


async def run_discover(args, output: Output, root: Path):
    harness = getattr(args, "harness", None)
    server_id = getattr(args, "server_id", None)
    if server_id:
        import asyncio
        from ...platform import paths
        from ...storage.state_store import StateStore
        from ...services.discovery_configuration import configured_candidates
        if getattr(args, "pi_releases_root", None) or getattr(args, "pi_node", None):
            raise ConnectorError("VALIDATION_ERROR", "discover",
                "Use executor configure-discovery to change persisted Pi discovery paths.")
        store = StateStore(paths.state_file(root))
        state = await asyncio.to_thread(store.load)
        records = [r for r in state.execution_executors if r.server_id == server_id
                   and r.state == "REGISTERED" and r.executor_id]
        if len(records) != 1:
            raise ConnectorError("VALIDATION_ERROR", "discover",
                "The Server executor registration is missing or ambiguous.")
        record = records[0]
        candidates = await configured_candidates(record.discovery_configuration,
            adapter_ids=(harness,) if harness else None, observations=record.installation_observations)
        current = await asyncio.to_thread(store.load)
        matches = [r for r in current.execution_executors if r.server_id == server_id]
        if len(matches) != 1 or matches[0] != record:
            raise ConnectorError("STALE_GENERATION", "discover",
                "The executor configuration changed during discovery.")
        result = {"candidates": [_entry_of(found).to_json() for found in candidates],
                "availability": _assessment(candidates),
                "note": "Persisted executor discovery configuration; runtime authorization is separate."}
        if not candidates:
            result["hint"] = _empty_hint(configured=True)
        return result

    notes = ["selection is an explicit local operator decision; PATH "
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
        notes.append("Pi release layouts are enumerated passively; the "
                     "Node executable you named is the trusted pair")
    # CN4-04.01 (G01/P04): the DISCOVER flow publishes the versioned
    # technical availability snapshot of THIS executor (Core-assessed)
    # over the FULL Core candidates — the same service function the
    # daemon serves over IPC; the Pi pair the operator named enters
    # the SAME effective inventory (never a re-created candidate that
    # would drop the CLI identity/version/build).
    candidates = await inventory_candidates(
        [harness] if harness else None, extra=release_candidates)
    payload = [_entry_of(found).to_json() for found in candidates]
    snapshot = _assessment(candidates)
    result = {"candidates": payload, "note": " ".join(notes)}
    result["availability"] = snapshot
    if not candidates:
        result["hint"] = _empty_hint(configured=False)
    return result
