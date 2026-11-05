"""``python -m okto_nexus_connector.daemon`` — detached daemon entry point."""

from __future__ import annotations

import asyncio
import argparse
import logging
import sys

from ..platform import paths


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Run the Connector under the current foreground owner.')
    parser.add_argument('--state-dir', default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    root = paths.state_dir(args.state_dir).resolve()
    from .manager import run_foreground
    try:
        return asyncio.run(run_foreground(root))
    except Exception as exc:  # detach target must never hang silently
        logging.getLogger(__name__).error("daemon failed: %r", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
