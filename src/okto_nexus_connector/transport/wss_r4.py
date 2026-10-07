"""R4 control negotiation for the Connector's outbound WSS link.

The returned session has no operation sender. Control reconciliation alone
never authorizes a native effect or declares a binding ready.
"""

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping
from urllib.parse import urlsplit
import secrets

import websockets

from nexus_connector_core import (
    R4_PREVIEW_REVISION, R4ReconcileAttempt, __version__ as CORE_VERSION,
    CoreError, R4LeaseApplication, RuntimeCore,
    decode_r4_frame, encode_r4_frame, reduce_r4_reconcile_accepted,
    r4_lease_renew_frame,
)

from .wss_client import validate_link_url


ReconcileReporter = Callable[[dict], Awaitable[Mapping[str, object]]]


async def _send(websocket, frame: Mapping[str, object]) -> None:
    await websocket.send(encode_r4_frame(frame).decode("utf-8"))


async def _receive(websocket) -> dict:
    raw = await websocket.recv()
    return decode_r4_frame(raw.encode("utf-8") if isinstance(raw, str) else raw)


@dataclass(frozen=True, slots=True)
class R4ControlState:
    server_id: str
    executor_id: str
    connection_id: str
    connection_generation: int
    control_ready: bool
    control_capabilities: tuple[str, ...] = ()


async def apply_r4_lease(
        websocket, state: R4ControlState, runtime: RuntimeCore, *,
        scope: Mapping[str, object], grant_id: str,
        purpose: str = "initial") -> R4LeaseApplication:
    """Exchange a lease on an authenticated, exclusively owned control socket.

    A raw socket requires exclusive reader ownership. An R4Connection routes
    grants through its single reader and retains the install/ACK producer
    independently of a cancelled waiter. Durable grant recovery remains
    a separate responsibility.
    A lost ACK requires reconciliation of the same request, never a new
    initial grant or a fabricated local deadline.
    """
    scope = dict(scope)
    if (not state.control_ready or scope.get("server_id") != state.server_id or
            scope.get("executor_id") != state.executor_id):
        raise CoreError("LEASE_REVALIDATION_REQUIRED", "r4_link")
    from .r4_connection import R4Connection
    if isinstance(websocket, R4Connection):
        if websocket.state != state:
            raise CoreError("STALE_GENERATION", "r4_link")
        return await websocket.apply_lease(runtime, scope=scope, grant_id=grant_id, purpose=purpose)
    attempt = await runtime.begin_r4_lease_request(
        scope=scope, grant_id=grant_id, connection_id=state.connection_id,
        connection_generation=state.connection_generation, purpose=purpose)
    await _send(websocket, r4_lease_renew_frame(attempt))
    grant = await _receive(websocket)
    application = await runtime.install_r4_lease(attempt, grant)
    # Core may need a durable CAS here. Receipt of a grant alone is not an ACK.
    await _send(websocket, application.acknowledgement)
    return application


def _scope(frame: Mapping[str, object], *, server_id: str,
           executor_id: str, connection_id: str,
           generation: int) -> bool:
    return (frame.get("server_id") == server_id and
            frame.get("executor_id") == executor_id and
            frame.get("connection_id") == connection_id and
            frame.get("connection_generation") == generation)


async def negotiate_r4_control(
        websocket, *, server_id: str, executor_id: str,
        management_revision: str, snapshot_format: int, boot_id: str,
        report_reconciliation: ReconcileReporter,
        max_reconcile_attempts: int = 3, recover_before_report=None, recovery_lanes=None) -> R4ControlState:
    """Negotiate one socket; a failed journal cannot become an empty report."""
    if not boot_id or max_reconcile_attempts < 1:
        raise ValueError("Invalid R4 control settings.")
    link_attempt_id = "link_" + secrets.token_hex(16)
    hello = {
        "protocol_major": 1, "contract_revision": R4_PREVIEW_REVISION,
        "type": "hello", "link_attempt_id": link_attempt_id,
        "server_id": server_id, "executor_id": executor_id,
        "core_version": CORE_VERSION,
        "management_revision": management_revision,
        "supported_nxl": [R4_PREVIEW_REVISION],
        "snapshot_formats": [snapshot_format],
        "control_capabilities": ['connection_renewal_v1', 'heartbeat_ack_v1', 'connection_resume_v1'],
    }
    await _send(websocket, hello)
    welcome = await _receive(websocket)
    if (welcome["type"] != "welcome" or
            welcome["link_attempt_id"] != link_attempt_id or
            welcome["server_id"] != server_id or
            welcome["executor_id"] != executor_id or
            welcome["management_revision"] != management_revision or
            welcome["accepted_nxl"] != R4_PREVIEW_REVISION or
            welcome["snapshot_format"] != snapshot_format):
        raise ValueError("The R4 control welcome does not match this link.")
    connection_id = welcome["connection_id"]
    generation = welcome["connection_generation"]
    from .r4_recovery import R4RecoveryChannel
    recovery = R4RecoveryChannel(websocket,
        R4ControlState(server_id,executor_id,connection_id,generation,False),boot_id)
    pending: R4ReconcileAttempt | None = None
    attempts = 0
    cycle_id, next_cursor, pages = None, None, 0
    while True:
        frame = await _receive(websocket)
        if not _scope(frame, server_id=server_id,
                      executor_id=executor_id,
                      connection_id=connection_id, generation=generation):
            raise ValueError("The R4 control frame changed scope.")
        kind = frame["type"]
        if kind == "heartbeat" and 'heartbeat_ack_v1' in welcome['control_capabilities']:
            # Acknowledgements can arrive between reconciliation pages or
            # retries. Liveness does not grant control readiness.
            continue
        if kind == "reconcile.request":
            if frame['cursor'] is None:
                attempts += 1
                cycle_id, pages = frame['reconcile_id'], 0
            elif frame['reconcile_id'] != cycle_id or frame['cursor'] != next_cursor:
                raise ValueError("The R4 reconciliation page is stale.")
            pages += 1
            if pages > 128:
                raise ValueError("R4 reconciliation exceeded its page budget.")
            if attempts > max_reconcile_attempts:
                raise ValueError("R4 reconciliation did not complete.")
            pending = R4ReconcileAttempt(
                frame["reconcile_id"], server_id, executor_id,
                connection_id, generation, boot_id)
            try:
                if recover_before_report is not None and frame['cursor'] is None:
                    await recover_before_report(recovery)
                    if recovery_lanes is not None:
                        recovery_lanes.update(recovery.lanes)
                report = dict(await report_reconciliation(frame))
                if (report.get("type") != "reconcile.report" or
                        report.get("reconcile_id") != frame["reconcile_id"] or
                        report.get("cursor") != frame["cursor"] or
                        not _scope(report, server_id=server_id,
                                   executor_id=executor_id,
                                   connection_id=connection_id,
                                   generation=generation)):
                    raise ValueError("The R4 reconciliation report changed scope.")
                if ((frame["operation_ids"] or frame["session_ids"]) and
                        report.get("complete") is True and
                        not any(report.get(key) for key in (
                            "receipts", "claims", "stream_watermarks",
                            "ownership_facts"))):
                    raise ValueError("The R4 reconciliation report is incomplete.")
                encoded = encode_r4_frame(report)
                next_cursor = report['next_cursor']
            except Exception:
                # A report failure is observable. It must never become an
                # invented empty report or a readiness transition.
                error = {
                    "protocol_major": 1,
                    "contract_revision": R4_PREVIEW_REVISION,
                    "type": "error", "server_id": server_id,
                    "executor_id": executor_id,
                    "connection_id": connection_id,
                    "connection_generation": generation,
                    "code": "JOURNAL_UNAVAILABLE",
                    "stage": "reconcile.report",
                    "possible_effect": False, "retry_safe": True,
                }
                await _send(websocket, error)
                await _send(websocket, {
                    "protocol_major": 1,
                    "contract_revision": R4_PREVIEW_REVISION,
                    "type": "heartbeat", "server_id": server_id,
                    "executor_id": executor_id,
                    "connection_id": connection_id,
                    "connection_generation": generation,
                })
                pending = None
                continue
            await websocket.send(encoded.decode("utf-8"))
            continue
        if kind == "reconcile.accepted":
            if pending is None:
                raise ValueError("Unsolicited R4 reconciliation acknowledgement.")
            projection = reduce_r4_reconcile_accepted(pending, frame)
            pending = None
            if projection.ready:
                recovery.online = False
                return R4ControlState(
                    server_id, executor_id, connection_id, generation, True,
                    tuple(welcome['control_capabilities']))
            await _send(websocket, {
                "protocol_major": 1,
                "contract_revision": R4_PREVIEW_REVISION,
                "type": "heartbeat", "server_id": server_id,
                "executor_id": executor_id,
                "connection_id": connection_id,
                "connection_generation": generation,
            })
            continue
        raise ValueError("Unexpected R4 control frame during negotiation.")


async def connect_r4_control(
        link_url: str, ticket: str, *, server_id: str, executor_id: str,
        management_revision: str, snapshot_format: int, boot_id: str,
        report_reconciliation: ReconcileReporter, recover_before_report=None, recovery_lanes=None):
    """Open an authenticated R4 socket and return it with control state."""
    validate_link_url(link_url)
    if urlsplit(link_url).scheme not in ("wss", "ws"):
        raise ValueError("Invalid R4 link URL.")
    from .proxy import resolve_proxy
    websocket = await websockets.connect(
        link_url, subprotocols=["nxl.v1"],
        proxy=resolve_proxy(link_url),
        additional_headers=[("Authorization", f"Bearer {ticket}")],
        max_size=1024 * 1024, open_timeout=15, ping_interval=None,
    )
    try:
        state = await negotiate_r4_control(
            websocket, server_id=server_id, executor_id=executor_id,
            management_revision=management_revision,
            snapshot_format=snapshot_format, boot_id=boot_id,
            report_reconciliation=report_reconciliation, recover_before_report=recover_before_report,
            recovery_lanes=recovery_lanes,
        )
        return websocket, state
    except BaseException:
        await websocket.close()
        raise


async def connect_r4_connection(link_url: str, ticket: str, *, server_id: str,
        executor_id: str, management_revision: str, snapshot_format: int,
        boot_id: str, report_reconciliation: ReconcileReporter, recover_before_report=None):
    """Negotiate a real socket and transfer its reader to one connection owner."""
    from .r4_connection import R4Connection
    lanes = {}
    websocket, state = await connect_r4_control(link_url, ticket,
        server_id=server_id, executor_id=executor_id, management_revision=management_revision,
        snapshot_format=snapshot_format, boot_id=boot_id, report_reconciliation=report_reconciliation,
        recover_before_report=recover_before_report,recovery_lanes=lanes)
    async def resume(current):
        from .proxy import resolve_proxy
        for attempt in range(4):
            replacement = None
            try:
                replacement = await websockets.connect(link_url, subprotocols=['nxl.v1'],
                    proxy=resolve_proxy(link_url), additional_headers=[('Authorization', f'Bearer {ticket}')],
                    max_size=1024 * 1024, open_timeout=2, ping_interval=None)
                request_id = 'resume_' + secrets.token_hex(16)
                await _send(replacement, dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION,
                    type='hello', link_attempt_id=request_id, server_id=server_id, executor_id=executor_id,
                    core_version=CORE_VERSION, management_revision=management_revision,
                    supported_nxl=[R4_PREVIEW_REVISION], snapshot_formats=[snapshot_format],
                    control_capabilities=list(current.control_capabilities),
                    resume_connection_id=current.connection_id, resume_connection_generation=current.connection_generation))
                reply = await asyncio.wait_for(_receive(replacement), 2)
                if (reply['type'] != 'welcome' or reply.get('resumed') is not True
                        or reply['link_attempt_id'] != request_id or not _scope(reply, server_id=server_id,
                            executor_id=executor_id, connection_id=current.connection_id, generation=current.connection_generation)):
                    raise ValueError('The Server did not confirm the existing connection owner.')
                return replacement
            except BaseException as error:
                if replacement is not None:
                    await replacement.close()
                if isinstance(error, asyncio.CancelledError):
                    raise
                if attempt == 3:
                    raise
                await asyncio.sleep(.25)
    try:
        connection = R4Connection(websocket, state, boot_id=boot_id, initial_lanes=lanes,
            resume=resume if 'connection_resume_v1' in state.control_capabilities else None)
        connection.start()
        return connection
    except BaseException:
        await websocket.close()
        raise
