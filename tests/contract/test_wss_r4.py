"""R4 control never treats a journal failure as an empty reconcile report."""

from __future__ import annotations

import asyncio
import json

import pytest

from nexus_connector_core import R4_PREVIEW_REVISION, SNAPSHOT_FORMAT_VERSION, decode_r4_frame

from okto_nexus_connector.transport.wss_r4 import negotiate_r4_control


BASE = {"protocol_major": 1, "contract_revision": R4_PREVIEW_REVISION,
        "server_id": "srv", "executor_id": "exe"}
MANAGEMENT = "nexus-connections-2026-09-29-r4"


class Peer:
    def __init__(self):
        self.inbox = asyncio.Queue()
        self.sent = []
        self.connection = {"connection_id": "conn", "connection_generation": 2}
        self.requests = 0

    async def recv(self):
        return json.dumps(await self.inbox.get())

    async def send(self, raw):
        frame = decode_r4_frame(raw.encode("utf-8"))
        self.sent.append(frame)
        if frame["type"] == "hello":
            await self.inbox.put({
                **BASE, **self.connection, "type": "welcome",
                "link_attempt_id": frame["link_attempt_id"],
                "management_revision": MANAGEMENT,
                "accepted_nxl": R4_PREVIEW_REVISION,
                "snapshot_format": SNAPSHOT_FORMAT_VERSION, "control_capabilities": [],
            })
            await self._request()
        elif frame["type"] == "heartbeat":
            await self._request()
        elif frame["type"] == "reconcile.report":
            await self.inbox.put({
                **BASE, **self.connection, "type": "reconcile.accepted",
                "reconcile_id": frame["reconcile_id"],
                "recovery_remaining": False,
                "ready_lane_ids": [], "session_lease_requirements": [],
            })

    async def _request(self):
        self.requests += 1
        await self.inbox.put({
            **BASE, **self.connection, "type": "reconcile.request",
            "reconcile_id": f"reconcile-{self.requests}",
            "cursor": None, "operation_ids": [],
            "session_ids": [], "stream_watermarks": [],
        })


@pytest.mark.asyncio
async def test_r4_control_retries_journal_failure_without_false_report():
    peer = Peer()
    calls = 0

    async def report(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("journal unavailable")
        return {
            **BASE, **peer.connection, "type": "reconcile.report",
            "reconcile_id": request["reconcile_id"],
            "cursor": request["cursor"], "next_cursor": None,
            "complete": True, "receipts": [], "claims": [],
            "stream_watermarks": [], "ownership_facts": [],
        }

    state = await asyncio.wait_for(negotiate_r4_control(
        peer, server_id="srv", executor_id="exe",
        management_revision=MANAGEMENT, snapshot_format=SNAPSHOT_FORMAT_VERSION,
        boot_id="boot", report_reconciliation=report), 2)
    assert state.control_ready is True
    assert state.connection_generation == 2
    assert [frame["type"] for frame in peer.sent] == [
        "hello", "error", "heartbeat", "reconcile.report"]
    assert all(frame["type"] != "reconcile.report" for frame in peer.sent[:3])


@pytest.mark.asyncio
async def test_r4_control_rejects_cross_scoped_welcome():
    peer = Peer()

    original_send = peer.send

    async def wrong_send(raw):
        await original_send(raw)
        welcome = await peer.inbox.get()
        request = await peer.inbox.get()
        await peer.inbox.put({**welcome, "executor_id": "other"})
        await peer.inbox.put(request)

    peer.send = wrong_send
    with pytest.raises(ValueError, match="welcome"):
        await negotiate_r4_control(
            peer, server_id="srv", executor_id="exe",
            management_revision=MANAGEMENT, snapshot_format=SNAPSHOT_FORMAT_VERSION,
            boot_id="boot", report_reconciliation=lambda _: None)


@pytest.mark.asyncio
async def test_r4_control_refuses_empty_report_for_pending_operation():
    peer = Peer()

    async def pending_request():
        peer.requests += 1
        await peer.inbox.put({
            **BASE, **peer.connection, "type": "reconcile.request",
            "reconcile_id": f"reconcile-{peer.requests}",
            "cursor": None, "operation_ids": ["operation"],
            "session_ids": [], "stream_watermarks": [],
        })

    peer._request = pending_request

    async def empty_report(request):
        return {
            **BASE, **peer.connection, "type": "reconcile.report",
            "reconcile_id": request["reconcile_id"],
            "cursor": None, "next_cursor": None,
            "complete": True, "receipts": [], "claims": [],
            "stream_watermarks": [], "ownership_facts": [],
        }

    with pytest.raises(ValueError, match="did not complete"):
        await asyncio.wait_for(negotiate_r4_control(
            peer, server_id="srv", executor_id="exe",
            management_revision=MANAGEMENT, snapshot_format=SNAPSHOT_FORMAT_VERSION,
            boot_id="boot", report_reconciliation=empty_report,
            max_reconcile_attempts=1), 2)
    assert [frame["type"] for frame in peer.sent] == [
        "hello", "error", "heartbeat"]
