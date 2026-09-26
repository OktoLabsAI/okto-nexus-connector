"""``runtime`` subcommands via the daemon's authenticated IPC (plan 6)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...daemon import manager
from ...errors import ConnectorError
from ..output import Output


def _connect(root: Path):
    try:
        return manager.connect(root)
    except ConnectorError as error:
        if error.code == "DAEMON_UNAVAILABLE":
            started = manager.start(root)
            if not started.get("started") and not started.get(
                    "already_running"):
                raise
            return manager.connect(root)
        raise


async def start_runtime(output: Output, root: Path, *, alias: str,
                        project: Path | None, harness: str | None,
                        new_session: bool, text: str | None
                        ) -> dict[str, object]:
    with _connect(root) as client:
        response = client.call("runtime.start", {
            "alias": alias,
            "project": str(project) if project else None,
            "harness": harness,
            "new_session": new_session,
            "text": text,
        })
    if not response.get("ok"):
        raise ConnectorError.from_json(response.get("error", {}))
    return dict(response.get("result", {}))


async def run_runtime(args, output: Output, root: Path):
    sub = args.subcommand
    if sub == "start":
        result = await start_runtime(
            output, root, alias=args.alias, project=args.project,
            harness=args.harness, new_session=args.new_session,
            text=args.text)
        receipt = result.get("receipt") or {}
        output.line(f"session {result.get('session_id')} "
                    f"({'reused' if result.get('reused') else 'opened'}); "
                    f"stage {receipt.get('stage', 'n/a')}")
        return result
    if sub == "status":
        with _connect(root) as client:
            response = client.call("runtime.status")
        _raise_for(response)
        payload = dict(response.get("result", {}))
        sessions = payload.get("sessions", [])
        output.line(f"{len(sessions)} managed session(s)")
        return payload
    if sub == "inspect":
        with _connect(root) as client:
            response = client.call("runtime.inspect", {
                "session_id": args.session_id})
        _raise_for(response)
        return dict(response.get("result", {}))
    if sub == "logs":
        with _connect(root) as client:
            for frame in client.stream("runtime.logs", {
                    "session_id": args.session_id,
                    "follow": args.follow}):
                if not frame.get("ok"):
                    _raise_for(frame)
                if "record" in frame:
                    output.stream_record(frame["record"])
                elif frame.get("done"):
                    break
        return None
    if sub == "submit":
        with _connect(root) as client:
            response = client.call("runtime.submit", {
                "session_id": args.session_id, "text": args.text})
        _raise_for(response)
        result = dict(response.get("result", {}))
        output.line(f"submitted; stage "
                    f"{(result.get('receipt') or {}).get('stage')}")
        return result
    if sub == "interrupt":
        with _connect(root) as client:
            response = client.call("runtime.interrupt", {
                "session_id": args.session_id})
        _raise_for(response)
        return dict(response.get("result", {}))
    if sub == "stop":
        with _connect(root) as client:
            response = client.call("runtime.stop", {
                "session_id": args.session_id})
        _raise_for(response)
        return dict(response.get("result", {}))
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "runtime",
                         f"unknown subcommand {sub!r}")


def _raise_for(response: dict) -> None:
    if not response.get("ok"):
        raise ConnectorError.from_json(response.get("error", {}))
