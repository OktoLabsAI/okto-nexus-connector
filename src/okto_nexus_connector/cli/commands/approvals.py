"""``approvals`` — pending HITL requests (plan C07.4, TC-29).

The CLI is a presentation channel. Decisions travel to the Server's
authorized mechanism with CAS correlation; the agent's own key never
approves an escalation that requires an operator — the Server enforces
the authority, and a late or conflicting decision fails the CAS instead
of being applied to another turn.
"""

from __future__ import annotations

from pathlib import Path

from ...daemon import manager
from ...errors import ConnectorError
from ..output import Output
from ..prompts import confirm


async def run_approvals(args, output: Output, root: Path):
    with manager.connect(root) as client:
        if args.subcommand == "list":
            response = client.call("approvals.pending")
            if not response.get("ok"):
                raise ConnectorError.from_json(response.get("error", {}))
            pending = response["result"]["approvals"]
            output.line(f"{len(pending)} pending request(s); decisions are "
                        "applied by the Server's authorized mechanism")
            return {"approvals": pending}
        if args.subcommand == "decide":
            if not args.non_interactive:
                shown = (f"{args.decide.upper()} request "
                         f"{args.request_id!r}?")
                if not confirm(shown, non_interactive=False, default=False):
                    return {"decided": False, "aborted": True}
            response = client.call("approvals.decide", {
                "request_id": args.request_id,
                "decision": args.decision,
                "cas_token": args.cas_token})
            if not response.get("ok"):
                raise ConnectorError.from_json(response.get("error", {}))
            result = response["result"]
            output.line(f"decision forwarded (applied={result['applied']}); "
                        "authority remains server-side")
            return result
    raise ConnectorError("CAPABILITY_UNSUPPORTED", "approvals",
                         f"unknown subcommand {args.subcommand!r}")
