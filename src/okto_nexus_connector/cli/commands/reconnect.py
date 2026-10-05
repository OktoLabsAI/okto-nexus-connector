"""Reconnect all configured servers through a graceful daemon restart."""
import asyncio

from ...daemon import manager
from ...errors import ConnectorError


async def run_reconnect(args, output, root):
    output.line('Reconnecting all configured connections: draining and restarting the daemon…')
    stopped = await asyncio.to_thread(manager.stop, root, timeout=30)
    if stopped.get('was_running') and not stopped.get('stopped'):
        raise ConnectorError('OPERATION_CONFLICT', 'reconnect',
            'The daemon is still draining. Reconnection was not started; inspect status and logs, then retry.')
    started = await asyncio.to_thread(manager.start, root)
    output.line('Daemon ready. All configured connections will reconnect asynchronously; use status to check progress.')
    return dict(reconnect_requested=True, scope='all', daemon=started)
