"""Interactive helpers: masked input, confirmations, selection (C07.2).

Non-interactive mode never guesses: an ambiguity that would need a human
decision returns an actionable error instead (plan A.4.1).
"""

from __future__ import annotations

from ..errors import ConnectorError


def confirm(question: str, *, non_interactive: bool,
            default: bool = False) -> bool:
    if non_interactive:
        return default
    import sys
    suffix = " [Y/n] " if default else " [y/N] "
    print(question + suffix, end="", file=sys.stderr, flush=True)
    try:
        answer = input().strip().lower()
    except EOFError:
        return default
    if not answer:
        return default
    return answer in ("y", "yes")


def select(message: str, options: list[tuple[str, str]], *,
           non_interactive: bool) -> str:
    """Return the chosen option key; options are (key, label)."""
    import sys
    print(message, file=sys.stderr)
    for index, (_key, label) in enumerate(options, start=1):
        print(f"  {index}. {label}", file=sys.stderr)
    if non_interactive or not options:
        raise ConnectorError("AMBIGUOUS_BINDING", "selection",
                             f"selection required: {message}",
                             action="Re-run interactively or pass the "
                                    "choice explicitly via flags.")
    try:
        answer = input("Choice [number]: ").strip()
        index = int(answer) - 1
    except (ValueError, EOFError):
        raise ConnectorError("VALIDATION_ERROR", "selection",
                             "invalid choice") from None
    if 0 <= index < len(options):
        return options[index][0]
    raise ConnectorError("VALIDATION_ERROR", "selection", "choice out of range")
