"""Integration: log streaming stays independent of the CLI window (TC-09
shape at the IPC level) and `--follow` semantics (plan 2.2)."""

from __future__ import annotations

import asyncio

import pytest

from tests.integration.test_daemon_app import harness  # noqa: F401 (fixture)


async def test_logs_snapshot_and_follow(harness):
    started = await harness.call("runtime.start", {"alias": "codex"})
    assert started["ok"]
    session_id = started["result"]["session_id"]

    # snapshot (no follow): terminal response arrives immediately
    frames = await harness.stream("runtime.logs", {
        "session_id": session_id, "follow": False})
    assert frames and frames[-1].get("done") is True

    # follow: streaming delivers records; "closing the terminal" (closing
    # the IPC client) does not touch the daemon session (TC-09)
    received: list[dict] = []
    stream = harness.stream_iter("runtime.logs", {
        "session_id": session_id, "follow": True})
    try:
        async for frame in stream:
            received.append(frame)
            if len(received) >= 1:
                break
    finally:
        await stream.aclose()

    # the daemon session is still alive after the follower closed
    status = await harness.call("runtime.status")
    ids = {s["session_id"] for s in status["result"]["sessions"]}
    assert session_id in ids
    stop = await harness.call("runtime.stop", {"session_id": session_id})
    assert stop["ok"]
