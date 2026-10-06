"""Human/JSON output helpers with stable exit codes and receipts."""

from __future__ import annotations

import json
import sys
from typing import IO
from .presentation import plain, styled, table

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_UNAVAILABLE = 3
EXIT_UNKNOWN_OUTCOME = 4


class Output:
    def __init__(self, *, json_mode: bool, stream: IO | None = None, verbose: bool = False):
        self.json_mode = json_mode
        self.stream = stream or sys.stdout
        self.verbose = verbose

    def result(self, payload: dict[str, object]) -> None:
        if self.json_mode:
            json.dump(payload, self.stream, indent=1, sort_keys=True,
                      default=str)
            self.stream.write("\n")
            return
        if self.verbose:
            self.heading('Details')
            for line in _render(payload):
                self.line(line)
        else:
            self._human(payload)

    def heading(self, text):
        if not self.json_mode:
            print('\n' + styled(text, 'heading', self.stream), file=self.stream)

    def table(self, headers, rows):
        if not self.json_mode:
            table(headers, rows, self.stream)

    def _human(self, payload, indent=0):
        for key, value in payload.items():
            label = key.replace('_', ' ').capitalize()
            if isinstance(value, dict):
                self.heading(label)
                self._human(value, indent + 1)
            elif isinstance(value, list):
                self.heading(label)
                if not value:
                    self.line('  None')
                elif all(isinstance(item, dict) for item in value):
                    keys = list(dict.fromkeys(k for item in value for k in item))
                    if len(keys) <= 5 and all(not isinstance(v, (dict, list)) for item in value for v in item.values()):
                        self.table([k.replace('_', ' ').capitalize() for k in keys],
                                   [[item.get(k) for k in keys] for item in value])
                    else:
                        for index, item in enumerate(value, 1):
                            self.line(f'\n  {label} {index}')
                            self._human(item, indent + 1)
                else:
                    for item in value:
                        self.line(f'  - {item}')
            else:
                self.line('  ' * indent + f'{label}: {value if value is not None else "-"}')

    def line(self, text: str) -> None:
        if not self.json_mode:
            safe = plain(text)
            label, separator, value = safe.partition(':')
            print(styled(label, 'label', self.stream) + separator + value if separator else safe, file=self.stream)

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
            print('\n' + styled(f"error: {payload.get('code')} ({payload.get('stage')}):", 'error', sys.stderr), file=sys.stderr)
            print('  ' + plain(payload.get('message', '')), file=sys.stderr)
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
                    lines.append('')
                else:
                    lines.append(f"{pad}  - {item}")
        else:
            display = value if value is not None else ""
            lines.append(f"{pad}{key}: {display}")
    return lines
