"""Shared store registration and retained lifetime across cancellation/reconnect."""
import asyncio
import threading

import pytest
from nexus_connector_core import CoreError, JournalLocation
from okto_nexus_connector.services import core_host


async def test_shared_registration_precedes_open_and_survives_executor_reconnect(tmp_path, monkeypatch):
    host = core_host.CoreRuntimeHost(tmp_path, None)
    original, records = core_host.open_journal, []
    async def observe(path, **kwargs):
        record = await (await host.ensure_ledger()).journal_location(path.name)
        assert record.location == JournalLocation(path.name)
        records.append(record)
        return await original(path, **kwargs)
    monkeypatch.setattr(core_host, 'open_journal', observe)
    try:
        journal = await host.ensure_journal()
        await host.shutdown_executor(server_id='server', executor_id='executor')
        assert await host.ensure_journal() is journal
        assert len(records) == 1 and not host._runtimes
    finally:
        await host.shutdown_all()
    try:
        await host.ensure_journal()
        assert records == [records[0], records[0]]
    finally:
        await host.shutdown_all()


async def test_failed_open_retries_same_registration(tmp_path, monkeypatch):
    host = core_host.CoreRuntimeHost(tmp_path, None)
    original, calls = core_host.open_journal, []
    async def fail_once(path, **kwargs):
        calls.append(path)
        if len(calls) == 1:
            raise OSError('creation cut')
        return await original(path, **kwargs)
    monkeypatch.setattr(core_host, 'open_journal', fail_once)
    try:
        with pytest.raises(OSError, match='creation cut'):
            await host.ensure_journal()
        ledger = await host.ensure_ledger()
        record = await ledger.journal_location(host._journal_path.name)
        assert record is not None and not host._journal_path.exists()
        await host.ensure_journal()
        assert await ledger.journal_location(host._journal_path.name) == record
        assert len((await ledger.journal_location_page()).registrations) == 1
    finally:
        await host.shutdown_all()


@pytest.mark.parametrize('cut', ['ledger', 'journal'])
async def test_cancelled_creation_and_shutdown_observers_do_not_abandon_stores(tmp_path, monkeypatch, cut):
    host = core_host.CoreRuntimeHost(tmp_path, None)
    entered, release, calls = threading.Event(), threading.Event(), []
    original_ledger, original_journal = core_host.SQLiteOwnedSlotLedger, core_host.open_journal
    def delayed_ledger(*args, **kwargs):
        calls.append('ledger')
        entered.set()
        assert release.wait(20)
        return original_ledger(*args, **kwargs)
    async def delayed_journal(*args, **kwargs):
        calls.append('journal')
        entered.set()
        assert await asyncio.to_thread(release.wait, 20)
        return await original_journal(*args, **kwargs)
    monkeypatch.setattr(core_host, 'SQLiteOwnedSlotLedger' if cut == 'ledger' else 'open_journal',
                        delayed_ledger if cut == 'ledger' else delayed_journal)
    observer = asyncio.create_task(host.ensure_journal())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        closing = asyncio.create_task(host.shutdown_all())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert host._stores_closing and not host._shutdown_task.done()
        with pytest.raises(CoreError, match='RUNTIME_DRAINING'):
            await host.ensure_ledger()
        release.set()
        await asyncio.wait_for(host.shutdown_all(), 10)
        assert calls == [cut]
        assert host._journal is None and host._ledger is None
        assert host._journal_task is None and host._ledger_task is None
    finally:
        release.set()
        await asyncio.gather(observer, return_exceptions=True)
        await host.shutdown_all()


async def test_late_capacity_facade_cannot_publish_a_closed_store(tmp_path, monkeypatch):
    host = core_host.CoreRuntimeHost(tmp_path, None, capacity_enabled=True)
    entered = asyncio.Event()
    original = host.ensure_ledger
    async def observe():
        ledger = await original()
        entered.set()
        return ledger
    monkeypatch.setattr(host, 'ensure_ledger', observe)
    class Authority:
        async def check_capacity_scope(self, request, *, action):
            return 'AVAILABLE'
    await host._capacity_gate.acquire()
    producer = asyncio.create_task(host.capacity_service(server_id='server', executor_id='executor',
        boot_id='boot', authority=Authority()))
    try:
        # Hosted Windows may spend several seconds opening the durable ledger.
        # This is a deadlock guard; the race begins only after `entered`.
        await asyncio.wait_for(entered.wait(), 20)
        await host.shutdown_all()
        host._capacity_gate.release()
        with pytest.raises(CoreError, match='RUNTIME_DRAINING'):
            await producer
        assert not host._capacity_services
        await host.capacity_service(server_id='server', executor_id='executor',
            boot_id='new-boot', authority=Authority())
    finally:
        if host._capacity_gate.locked():
            host._capacity_gate.release()
        await asyncio.gather(producer, return_exceptions=True)
        await host.shutdown_all()
