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
    from ...platform import paths
    from ...storage.state_store import StateStore
    store = StateStore(paths.state_file(root))
    state = await asyncio.to_thread(store.load)
    alias = getattr(args, "alias", None)
    canonical = any(r.alias == alias for r in state.binding_intents) if alias else False
    if not alias and sub in {"submit", "interrupt", "stop"}:
        aliases = {r.alias for r in state.runtime_intents if r.resolution is not None and
                   r.resolution.get("session_id") == args.session_id}
        if len(aliases) > 1:
            raise ConnectorError("OPERATION_CONFLICT", "runtime", "Select an explicit binding alias.")
        if aliases:
            alias, canonical = next(iter(aliases)), True
    if canonical or sub in {"steer", "operation"} or (
            sub in {"submit", "interrupt", "stop"} and alias is not None):
        from .identity import _vault
        from ...services.runtime_admission import RuntimeAdmission
        service = RuntimeAdmission(store, _vault(root, store))
        if sub == "operation":
            return await service.inspect(alias=alias, client_intent_id=args.client_intent_id)
        if sub == "start" and (args.project is not None or args.harness is not None):
            raise ConnectorError("VALIDATION_ERROR", "runtime",
                                 "Use the approved R4 binding configuration to start this runtime.")
        expected = getattr(args, "expected_turn_id", None)
        current = getattr(args, "current_run", False)
        if expected and current:
            raise ConnectorError("VALIDATION_ERROR", "runtime", "Select only one control target.")
        target = dict(kind="native_turn_id" if expected else "current_run" if current else "none",
                      expected_turn_id=expected)
        result = await service.execute(alias=alias, client_intent_id=getattr(args, "client_intent_id", None),
            intent={"start": "runtime.start", "submit": "turn.submit", "steer": "turn.steer",
                    "interrupt": "turn.interrupt", "stop": "runtime.close"}[sub],
            session_id=getattr(args, "session_id", None),
            new_session=args.new_session if sub == "start" else None,
            text=getattr(args, "reason", None) if sub in {"interrupt", "stop"} else getattr(args, "text", None),
            target=target)
        output.line(f"Runtime intent {result['client_intent_id']}: {result['state']}.")
        return result
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
