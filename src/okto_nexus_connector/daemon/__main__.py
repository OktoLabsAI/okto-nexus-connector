"""``python -m okto_nexus_connector.daemon`` — detached daemon entry point."""

from __future__ import annotations

import asyncio
import logging
import sys

from ..platform import paths


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    root = paths.state_dir()
    from .app import DaemonApp
    app = DaemonApp(root)
    try:
        return asyncio.run(app.run_forever())
    except Exception as exc:  # detach target must never hang silently
        logging.getLogger(__name__).error("daemon failed: %r", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
