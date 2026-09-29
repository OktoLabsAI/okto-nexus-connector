"""Batch helpers for event publication (contract-valid contiguous)."""

from __future__ import annotations


def _contiguous_batches(events: list) -> list[list]:
    """Split a sequence list into contract-valid contiguous batches."""
    batches: list[list] = []
    current: list = []
    expected: int | None = None
    for event in events:
        if expected is not None and event.sequence != expected:
            batches.append(current)
            current = []
        current.append(event)
        expected = event.sequence + 1
    if current:
        batches.append(current)
    return batches
