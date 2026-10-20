"""A cancelled negotiation retains the producer but revokes its authority."""
import asyncio
import threading
from types import SimpleNamespace

import pytest
from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_r4_daemon_control import control


async def test_cancelled_negotiation_joins_native_recovery_before_return(control, monkeypatch):
    owner, peer, store, host, _ = control
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    from okto_nexus_connector.services import r4_event_recovery
    async def no_events(*args, **kwargs): return None
    monkeypatch.setattr(r4_event_recovery, 'recover_event_streams', no_events)
    failures = []
    async def held_report(request, *, validate_authority, publish_receipts):
        def worker():
            validate_authority(request)
            entered.set()
            assert release.wait(10)
            try:
                validate_authority(request)
            except ConnectorError as error:
                failures.append(error.code)
            finally:
                finished.set()
        await asyncio.to_thread(worker)
        raise ConnectorError('STALE_GENERATION','test','Recovery observer was cancelled')
    monkeypatch.setattr(owner, '_report_reconciliation', held_report)
    async def connect(*args, **params):
        fields = dict(server_id='srv',executor_id='executor',connection_id='replacement',connection_generation=2)
        await params['recover_before_report'](SimpleNamespace(online=True,state=SimpleNamespace(**fields)))
        return await params['report_reconciliation'](fields)
    owner.connect = connect
    task = asyncio.create_task(owner._attempt())
    try:
        async with asyncio.timeout(10):
            while not entered.is_set():
                if task.done(): await task
                await asyncio.sleep(.01)
        task.cancel()
        await asyncio.sleep(.05)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError): await task
        assert finished.is_set() and failures == ['STALE_GENERATION']
    finally:
        release.set()
        await asyncio.gather(task,return_exceptions=True)
        await host.shutdown_all()
