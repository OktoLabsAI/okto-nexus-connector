"""NXL WSS contract peer (server side) for transport tests.

Implements the Server half of the control channel against the Core's
bundled frame codec: hello/welcome negotiation with the exact r3 revision
gate, per-binding lane tickets, durable-ingress event ACK with contiguous
watermarks, operation.submit injection (test-driven), lease grants and
generation fencing. Contract fake, not a Server (plan 8/C04.5).
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

import websockets

from nexus_connector_core import CONTRACT_REVISION
from nexus_connector_core.protocol import PROTOCOL_MAJOR
from nexus_connector_core.frame_codec import decode_frame, encode_frame
from nexus_connector_core.protocol import submit_frame_intent_hash


@dataclass
class PeerSession:
    websocket: object
    server_id: str = ""
    executor_id: str = ""
    connection_generation: int = 1
    welcomed: bool = False
    lanes: dict[str, str] = field(default_factory=dict)  # binding→agent
    acked: dict[tuple[str, str], int] = field(default_factory=dict)
    receipts: list[dict] = field(default_factory=list)
    submitted: list[dict] = field(default_factory=list)
    approvals: list[dict] = field(default_factory=list)
    closed: bool = False


class FakeNXLPeer:
    def __init__(self, *, server_id: str = "srv_fake",
                 valid_tickets: set[str] | None = None,
                 require_intent_hash: bool = True):
        self.server_id = server_id
        self.valid_tickets = valid_tickets if valid_tickets is not None \
            else set()
        self.require_intent_hash = require_intent_hash
        self.sessions: list[PeerSession] = []
        self.server: object | None = None
        self.port: int = 0
        self.goaway_after_welcome = False
        self.drop_after_hello = False
        self._generation_counter = 0

    async def start(self, host: str = "127.0.0.1") -> str:
        async def handler(websocket):
            await self._client(websocket)
        self.server = await websockets.serve(
            handler, host, 0, subprotocols=["nxl.v1"], max_size=1048576)
        self.port = self.server.sockets[0].getsockname()[1]
        return f"ws://{host}:{self.port}"

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for session in self.sessions:
            session.closed = True

    @property
    def latest(self) -> PeerSession | None:
        return self.sessions[-1] if self.sessions else None

    async def send_operation(self, session: PeerSession, *, binding_id: str,
                             agent_id: str, session_id: str,
                             operation_id: str, action: str,
                             payload: dict | None = None,
                             workspace_id: str = "ws_1",
                             workspace_binding_id: str = "wb_1",
                             intent_hash: str | None = None) -> None:
        frame: dict = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "operation.submit",
            "server_id": session.server_id,
            "executor_id": session.executor_id,
            "binding_id": binding_id,
            "agent_id": agent_id,
            "workspace_id": workspace_id,
            "workspace_binding_id": workspace_binding_id,
            "session_id": session_id,
            "operation_id": operation_id,
            "action": action,
            "connection_generation": session.connection_generation,
            "authorization_revision": 1,
            "configuration_revision": 1,
            "payload": payload or {"text": "hello from server"},
        }
        if intent_hash is None and self.require_intent_hash:
            frame["intent_hash"] = submit_frame_intent_hash(frame)
        elif intent_hash is not None:
            frame["intent_hash"] = intent_hash
        await session.websocket.send(encode_frame(frame))

    async def send_approval(self, session: PeerSession, *, binding_id: str,
                            agent_id: str, session_id: str,
                            request_id: str,
                            kind: str = "escalation") -> None:
        frame = {
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "approval.request",
            "server_id": session.server_id,
            "executor_id": session.executor_id,
            "binding_id": binding_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "operation_id": f"op_appr_{request_id}",
            "request_id": request_id,
            "kind": kind,
            "proposal": {"summary": "operator escalation"},
        }
        await session.websocket.send(encode_frame(frame))

    async def _client(self, websocket) -> None:
        session = PeerSession(websocket=websocket)
        self.sessions.append(session)
        try:
            async for raw in websocket:
                frame = decode_frame(raw)
                kind = frame.get("type")
                if kind == "hello":
                    session.server_id = frame["server_id"]
                    session.executor_id = frame["executor_id"]
                    # CN1/A02: the SERVER authorizes each new connection
                    # with a strictly increasing generation.
                    self._generation_counter += 1
                    session.connection_generation = self._generation_counter
                    if frame["contract_revision"] != CONTRACT_REVISION or \
                            frame["protocol_major"] != PROTOCOL_MAJOR:
                        await self._error(websocket, session,
                                          "VERSION_INCOMPATIBLE",
                                          "negotiation")
                        return
                    if self.drop_after_hello:
                        await websocket.close()
                        return
                    await websocket.send(encode_frame({
                        "protocol_major": PROTOCOL_MAJOR,
                        "contract_revision": CONTRACT_REVISION,
                        "type": "welcome",
                        "server_id": self.server_id,
                        "executor_id": session.executor_id,
                        "core_version": "fake-peer",
                        "connection_generation":
                            session.connection_generation,
                        "event_types": ["lifecycle", "turn_state",
                                        "text_delta", "tool_activity"],
                        "limits": {"max_frame_bytes": 1048576},
                        "capabilities": [],
                    }))
                    session.welcomed = True
                    if self.goaway_after_welcome:
                        await websocket.send(encode_frame({
                            "protocol_major": PROTOCOL_MAJOR,
                            "contract_revision": CONTRACT_REVISION,
                            "type": "goaway",
                            "server_id": self.server_id,
                            "executor_id": session.executor_id,
                            "reason": "test shutdown"}))
                        return
                elif kind == "binding.attach":
                    ticket = frame["ticket"]
                    binding_id = frame["binding_id"]
                    if ticket not in self.valid_tickets:
                        await self._error(websocket, session,
                                          "AGENT_AUTH_REQUIRED",
                                          "binding.attach")
                        continue
                    session.lanes[binding_id] = frame["agent_id"]
                elif kind == "operation.receipt":
                    session.receipts.append(dict(frame))
                elif kind == "approval.decision":
                    session.approvals.append(dict(frame))
                elif kind == "event.batch":
                    await self._ingest(session, frame)
                elif kind == "heartbeat":
                    await websocket.send(encode_frame({
                        "protocol_major": PROTOCOL_MAJOR,
                        "contract_revision": CONTRACT_REVISION,
                        "type": "heartbeat",
                        "server_id": self.server_id,
                        "executor_id": session.executor_id,
                        "connection_generation":
                            session.connection_generation,
                    }))
                elif kind == "operation.query":
                    await websocket.send(encode_frame({
                        "protocol_major": PROTOCOL_MAJOR,
                        "contract_revision": CONTRACT_REVISION,
                        "type": "operation.receipt",
                        "server_id": self.server_id,
                        "executor_id": session.executor_id,
                        "session_id": "rs_000001",
                        "operation_id": frame["operation_id"],
                        "intent_hash": "sha256:" + "0" * 64,
                        "stage": "OUTCOME_UNKNOWN",
                        "possible_effect": True,
                        "retry_safe": False,
                    }))
        except websockets.ConnectionClosed:
            pass
        finally:
            session.closed = True

    async def _ingest(self, session: PeerSession, frame: dict) -> None:
        """Durable-ingress simulation: contiguous watermark ACK only."""
        for event in frame["events"]:
            key = (event["session_id"], event["stream_epoch"])
            watermark = session.acked.get(key, 0)
            if event["sequence"] == watermark + 1:
                session.acked[key] = event["sequence"]
            elif event["sequence"] <= watermark:
                continue  # duplicate
            # gaps remain un-ACKed (honest watermark)
        for key, watermark in list(session.acked.items()):
            if watermark > 0:
                await session.websocket.send(encode_frame({
                    "protocol_major": PROTOCOL_MAJOR,
                    "contract_revision": CONTRACT_REVISION,
                    "type": "event.ack",
                    "server_id": self.server_id,
                    "executor_id": session.executor_id,
                    "session_id": key[0],
                    "stream_epoch": key[1],
                    "sequence": watermark,
                }))

    async def _error(self, websocket, session, code: str, stage: str
                     ) -> None:
        wire_stage = "OUTCOME_UNKNOWN"
        await websocket.send(encode_frame({
            "protocol_major": PROTOCOL_MAJOR,
            "contract_revision": CONTRACT_REVISION,
            "type": "error",
            "server_id": self.server_id,
            "executor_id": session.executor_id or "unknown-executor",
            "code": code,
            "stage": wire_stage,
            "possible_effect": False,
            "retry_safe": False,
            "corrective_action": f"[stage: {stage}]",
        }))
