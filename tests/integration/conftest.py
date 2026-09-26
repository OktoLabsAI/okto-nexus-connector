"""Shared helpers for daemon integration tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))


@pytest.fixture(autouse=True)
def file_vault(monkeypatch):
    """Never touch the real OS keyring in tests."""
    monkeypatch.setenv("OKTO_NEXUS_CONNECTOR_VAULT", "file")


class RecordingNative:
    """Fake native adapter session for the Core runtime."""

    native_id = "native-fake"

    def __init__(self):
        self.sent: list[tuple[str, str]] = []
        self.stopped = False
        self.queue = None
        import asyncio
        self.queue = asyncio.Queue()
        self._events = []

    async def send(self, verb, payload, operation_id, *,
                   expected_turn_id=None):
        self.sent.append((verb, operation_id))

    async def events(self):
        import asyncio
        from nexus_connector_core import RuntimeEvent
        while True:
            event = await self.queue.get()
            if event is None:
                return
            yield event

    async def close(self):
        self.stopped = True
        await self.queue.put(None)
        return "graceful"

    async def observe(self):
        return ("STOPPED" if self.stopped else "RUNNING", "IDLE")

    async def reply_native_approval(self, request, decision, response):
        return None

    def emit(self, sequence: int, category: str = "text_delta",
             payload: dict | None = None):
        from nexus_connector_core import RuntimeEvent
        self.queue.put_nowait(RuntimeEvent(
            "srv_fake", "conn_test", "rs_integration", "ep-1", sequence,
            category, f"{category}.native", payload or {"text": "x"}))


class RecordingFactory:
    """Fake NativeFactory; Core treats it as any injected factory."""

    def __init__(self):
        self.native = RecordingNative()
        self.opens = 0

    async def open(self, prepared, session_id, context, *, stream_epoch):
        self.opens += 1
        return self.native
