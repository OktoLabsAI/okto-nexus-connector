"""Disposable M6 daemon entry point, never an installed service."""
import asyncio
from pathlib import Path
import sys

from okto_nexus_connector.daemon.app import DaemonApp


if __name__ == '__main__':
    raise SystemExit(asyncio.run(DaemonApp(Path(sys.argv[1]),
        ipc_transport='named_pipe').run_forever()))
