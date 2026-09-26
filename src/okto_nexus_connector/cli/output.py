"""Human/JSON output helpers with stable exit codes and receipts."""

from __future__ import annotations

import json
import sys
from typing import IO

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_UNAVAILABLE = 3
EXIT_UNKNOWN_OUTCOME = 4


class Output:
    def __init__(self, *, json_mode: bool, stream: IO | None = None):
        self.json_mode = json_mode
        self.stream = stream or sys.stdout

    def result(self, payload: dict[str, object]) -> None:
        if self.json_mode:
            json.dump(payload, self.stream, indent=1, sort_keys=True,
                      default=str)
            self.stream.write("\n")
            return
        for line in _render(payload):
            print(line, file=self.stream)

    def line(self, text: str) -> None:
        if not self.json_mode:
            print(text, file=self.stream)

    def stream_record(self, record: dict[str, object]) -> None:
        if self.json_mode:
            self.stream.write(json.dumps(record, default=str) + "\n")
            self.stream.flush()
            return
        category = record.get("category", "")
        payload = record.get("payload", {})
        text = payload.get("text") if isinstance(payload, dict) else None
        native = record.get("native_type") or ""
        marker = f"[{category}]" if category else ""
        if native:
            marker += f"[{native}]"
        suffix = f" {text}" if text else ""
        self.stream.write(f"{record.get('at', '')} {marker}{suffix}\n")
        self.stream.flush()

    def error(self, error) -> int:
        payload = error.to_json() if hasattr(error, "to_json") else {
            "code": "UNKNOWN", "stage": "cli", "message": str(error)}
        if self.json_mode:
            json.dump({"error": payload}, self.stream, indent=1,
                      sort_keys=True, default=str)
            self.stream.write("\n")
        else:
            print(f"error: {payload.get('code')} ({payload.get('stage')}): "
                  f"{payload.get('message', '')}", file=sys.stderr)
            if payload.get("action"):
                print(f"action: {payload['action']}", file=sys.stderr)
        if payload.get("code") == "OUTCOME_UNKNOWN":
            return EXIT_UNKNOWN_OUTCOME
        if payload.get("code") in ("DAEMON_UNAVAILABLE", "EXECUTOR_OFFLINE"):
            return EXIT_UNAVAILABLE
        return EXIT_ERROR


def _render(payload: dict[str, object], indent: int = 0) -> list[str]:
    lines: list[str] = []
    pad = "  " * indent
    for key, value in payload.items():
        if isinstance(value, dict):
            lines.append(f"{pad}{key}:")
            lines.extend(_render(value, indent + 1))
        elif isinstance(value, list):
            lines.append(f"{pad}{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.extend(_render(item, indent + 1))
                    lines.append(f"{pad}  ---")
                    if lines and lines[-1] == f"{pad}  ---":
                        lines.pop()
                else:
                    lines.append(f"{pad}  - {item}")
        else:
            display = value if value is not None else ""
            lines.append(f"{pad}{key}: {display}")
    return lines
