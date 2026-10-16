"""Disposable Windows launch producer; never targets a user's harness."""
import asyncio
import os
from pathlib import Path
import sys

from nexus_connector_core.journal import SQLiteJournal
from nexus_connector_core import OperationKey, SessionKey
from nexus_connector_core.native.process import spawn_owned_process, snapshot_owned_process_birth
from nexus_connector_core.native.process.launch_exclusion import HostLaunchExclusion
from nexus_connector_core.process_launch_control import ProcessLaunchController
from okto_nexus_connector.platform.paths import journal_path


async def main():
    directory, boundary = Path(sys.argv[1]), sys.argv[2]
    journal = SQLiteJournal(journal_path(directory))
    key = SessionKey('srv','exe','session')
    launch = (await journal.get_process_launch(key)).identity
    if boundary == 'intent':
        os._exit(17)
    if boundary == 'container':
        from nexus_connector_core.native.process.windows_process import OwnedWindowsPopen
        OwnedWindowsPopen._execute_child = lambda *_args, **_kwargs: os._exit(17)
    def start():
        if boundary == 'claim': os._exit(17)
        child = spawn_owned_process([sys.executable,'-c','import time; time.sleep(60)'],
            cwd=str(directory), env=dict(os.environ), container_id=launch.container_id, text=True)
        if boundary == 'spawn': os._exit(17)
        return child
    child = await ProcessLaunchController(journal,HostLaunchExclusion()).produce(
        launch, validate_authority=lambda: None, start=start)
    await journal.record_process_birth(OperationKey('srv','exe','open'),key.session_id,snapshot_owned_process_birth(child))
    os._exit(17)


asyncio.run(main())
