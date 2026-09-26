"""``discover`` — redacted local inventory (plan 6, TC-11)."""

from __future__ import annotations

from pathlib import Path

from ...services.discovery_service import discover_inventory
from ..output import Output


async def run_discover(args, output: Output, root: Path):
    harness = getattr(args, "harness", None)
    adapters = [harness] if harness else None
    entries = await discover_inventory(adapters) if adapters else \
        await discover_inventory()
    output.line(f"{len(entries)} candidate(s); discovery executes nothing "
                "and authorizes nothing")
    return {
        "candidates": [entry.to_json() for entry in entries],
        "note": "selection is an explicit connect-time decision; PATH "
                "candidates are not trusted automatically",
    }
