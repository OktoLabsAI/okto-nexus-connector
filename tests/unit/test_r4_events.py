"""Real Core finite replay and durable acknowledgment recovery."""
import asyncio
from types import SimpleNamespace
import pytest
from nexus_connector_core import RuntimeEvent, R4_PREVIEW_REVISION
from nexus_connector_core.journal import open_journal
from okto_nexus_connector.services.r4_events import R4EventPublisher, event_page
from okto_nexus_connector.storage.state_store import StateStore

SCOPE=dict(server_id="srv",executor_id="exe",binding_id="binding",agent_id="agent",
           session_id="session",stream_epoch="epoch")


@pytest.fixture
async def publisher(tmp_path):
    journal=await open_journal(tmp_path/"core.db")
    async def history(): return journal
    class Connection:
        online=True
        state=SimpleNamespace(connection_id="connection",connection_generation=1)
        calls=[]
        async def publish_events(self, **values):
            self.calls.append(values["events"])
            return dict(protocol_major=1,contract_revision=R4_PREVIEW_REVISION,type="event.ack",**SCOPE,
                        connection_id="connection",connection_generation=1,sequence=values["events"][-1]["sequence"])
    connection=Connection()
    owner=R4EventPublisher(connection,StateStore(tmp_path/"state.json"),SimpleNamespace(ensure_history_journal=history),
                           poll_seconds=.01,retry_seconds=.01)
    owner.store.register(SCOPE)
    try: yield owner,journal,connection
    finally:
        await owner.stop()
        await journal.aclose()


async def append(journal, count):
    for _ in range(count):
        await journal.record_event(RuntimeEvent("srv","exe","session","epoch",0,"text_delta",None,{"text":"Hello"}))


@pytest.mark.parametrize("count",[0,1,127,128,129])
async def test_finite_snapshot_does_not_wait_for_a_full_batch(publisher,count):
    owner,journal,_=publisher
    await append(journal,count)
    page=await asyncio.wait_for(event_page(journal,SCOPE,0),1)
    assert len(page)==min(count,128)
    after=page[-1]["sequence"] if page else 0
    following=await asyncio.wait_for(event_page(journal,SCOPE,after),1)
    assert len(following)==max(0,count-128)


@pytest.mark.parametrize("fault",["read","send","core_ack","ack_progress"])
async def test_retry_without_new_event_converges_without_repeating_native_effect(publisher,monkeypatch,fault):
    owner,journal,connection=publisher
    await append(journal,1)
    failures=0
    if fault=="read":
        original=journal.events
        async def fail(cursor):
            nonlocal failures
            if failures==0:
                failures+=1
                raise OSError("Injected journal read failure.")
            async for event in original(cursor): yield event
        monkeypatch.setattr(journal,"events",fail)
    elif fault=="send":
        original=connection.publish_events
        async def fail(**values):
            nonlocal failures
            if failures==0:
                failures+=1
                raise OSError("Injected lost delivery.")
            return await original(**values)
        monkeypatch.setattr(connection,"publish_events",fail)
    elif fault=="core_ack":
        original=journal.acknowledge_events
        async def fail(*args):
            nonlocal failures
            if failures==0:
                failures+=1
                raise OSError("Injected Core acknowledgment failure.")
            return await original(*args)
        monkeypatch.setattr(journal,"acknowledge_events",fail)
    else:
        original=owner.store.advance
        def fail(scope, **changes):
            nonlocal failures
            if changes.get("core_applied") and failures==0:
                failures+=1
                raise OSError("Injected progress write failure.")
            return original(scope,**changes)
        monkeypatch.setattr(owner.store,"advance",fail)
    await owner.ensure(SCOPE)
    await owner.ensure(SCOPE)
    progress = None
    try:
        # This exercises durable FULL-sync SQLite writes, not a latency SLA.
        # Windows CI can spend several seconds flushing those writes; keep a
        # bounded deadline without making the observer monopolize the store.
        async with asyncio.timeout(15):
            while True:
                # Match the publisher: SQLite may wait on a writer, so the
                # observer must not block the event loop that drives recovery.
                progress = await asyncio.to_thread(owner.store.read, SCOPE)
                if progress['core_applied'] == 1:
                    break
                await asyncio.sleep(.05)
    except TimeoutError as error:
        from tests.unit.async_diagnostics import pending_task_locations
        counters = None if progress is None else {
            name: progress[name] for name in ('remote_acked', 'core_applied')}
        error.add_note(f'Event retry fault={fault}; injected={failures}; '
                       f'progress={counters}; sends={len(connection.calls)}\n' + pending_task_locations())
        raise
    assert failures==1 and len(owner.tasks)==1
    assert owner.store.read(SCOPE)["remote_acked"]==1
    assert len(connection.calls)==1
    await owner.stop()
    await owner.ensure(SCOPE)
    assert all(task.done() for task in owner.tasks.values())


async def test_recreated_publisher_applies_persisted_remote_ack_before_sending(publisher):
    owner,journal,connection=publisher
    await append(journal,1)
    owner.store.advance(SCOPE,remote_acked=1)
    recreated=R4EventPublisher(connection,StateStore(owner.store.path.with_name("state.json")),owner.host)
    try:
        assert not await recreated.step(SCOPE)
        assert recreated.store.read(SCOPE)["core_applied"]==1
        assert connection.calls==[]
    finally: await recreated.stop()


async def test_cancelled_stop_waiter_retains_core_ack_writer(publisher,monkeypatch):
    owner,journal,connection=publisher
    await append(journal,1)
    entered,release=asyncio.Event(),asyncio.Event()
    original=journal.acknowledge_events
    async def blocked(*args):
        entered.set()
        await release.wait()
        return await original(*args)
    monkeypatch.setattr(journal,"acknowledge_events",blocked)
    try:
        await owner.ensure(SCOPE)
        await asyncio.wait_for(entered.wait(),2)
        waiter=asyncio.create_task(owner.stop())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError): await waiter
        assert owner.store.read(SCOPE)["remote_acked"]==1
        assert owner.store.read(SCOPE)["core_applied"]==0
        release.set()
        await owner.stop()
        assert owner.store.read(SCOPE)["core_applied"]==1
        assert len(connection.calls)==1
    finally:
        release.set()


def test_stream_store_rejects_cross_binding_and_corrupted_identity(tmp_path):
    import sqlite3
    from okto_nexus_connector.storage.r4_events import R4EventStore
    from okto_nexus_connector.errors import ConnectorError
    store=R4EventStore(tmp_path/"events.db")
    store.register(SCOPE)
    store.register(SCOPE)
    foreign={**SCOPE,"binding_id":"foreign"}
    for action in (lambda:store.register(foreign),lambda:store.read(foreign),
                   lambda:store.advance(foreign,remote_acked=1)):
        with pytest.raises(ConnectorError) as refused: action()
        assert refused.value.code=="SCOPE_MISMATCH"
    store.advance(SCOPE,remote_acked=1)
    with pytest.raises(ConnectorError):
        store.advance(SCOPE,core_applied=2)
    assert store.read(SCOPE)["core_applied"]==0
    with sqlite3.connect(store.path) as conn:
        conn.execute("UPDATE streams SET metadata='{}'")
    with pytest.raises(ConnectorError) as refused: store.read(SCOPE)
    assert refused.value.code=="JOURNAL_UNAVAILABLE"
