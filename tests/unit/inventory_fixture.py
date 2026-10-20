"""Accepted observation evidence for consumer tests (no discovery fallback)."""
import time
from nexus_connector_core import get_runtime_catalog
from nexus_connector_core.inventory_observer import HarnessObservation, InventoryObservationBatch


def accepted_batch(candidates=(), *, states=None, now=None):
    candidates = tuple(candidates)
    states = states or {}
    rows = []
    for entry in get_runtime_catalog().runtimes:
        if entry.discoverable:
            state = states.get(entry.adapter_id, 'PRESENT' if any(
                c.adapter_id == entry.adapter_id for c in candidates) else 'ABSENT')
            rows.append(HarnessObservation(entry.adapter_id, state,
                0 if state in ('PRESENT', 'ABSENT') else None,
                'DISCOVERY_FAILED' if state == 'FAILED' else None))
    return InventoryObservationBatch(1, time.monotonic() if now is None else now,
                                     candidates, candidates, tuple(rows))
