"""Credential-free reachability probe; no daemon or local state required."""
import time

from ... import __version__
from ...transport.https_client import NexusHTTPClient


async def run_reach(args):
    started = time.monotonic()
    async with NexusHTTPClient(args.server) as client:
        result = await client.reach()
    return dict(reachable=True, server_url=client.base_url, **result,
                cli_version=__version__, latency_ms=round((time.monotonic() - started) * 1000, 1))
