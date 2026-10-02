"""Automatic session renewal uses Core deadlines and retains its producer."""
import asyncio
import time

import pytest
from nexus_connector_core import R4_PREVIEW_REVISION, SessionKey

from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_execution import observed, operation


from tests.unit.async_diagnostics import pending_task_locations


async def setup(lifecycle, monkeypatch, *, hold=False, refuse=False):
    owner, _, store, frame, native, _, published, _ = lifecycle
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def lease(runtime, *, scope, grant_id, purpose="initial", require_current=None, fence_on_error=True):
        if require_current is not None:
            await require_current()
        attempt = await runtime.begin_r4_lease_request(scope=scope, grant_id=grant_id,
            connection_id=owner.connection.state.connection_id,
            connection_generation=owner.connection.state.connection_generation, purpose=purpose)
        calls.append(attempt)
        if purpose == "renew":
            entered.set()
            if hold:
                await release.wait()
            if refuse:
                raise ConnectorError("AGENT_AUTH_REQUIRED", "test_lease", "Renewal was refused.")
        if require_current is not None:
            await require_current()
        return await runtime.install_r4_lease(attempt, dict(protocol_major=1,
            contract_revision=R4_PREVIEW_REVISION,type="lease.granted",
            request_id=attempt.request_id,grant_id=grant_id,scope=scope,
            lease_id="lease-"+str(attempt.expected_lease_serial+1),
            lease_serial=attempt.expected_lease_serial+1,valid_for_ms=1600,
            allowed_actions=["runtime.open","turn.submit","turn.steer","turn.interrupt","runtime.close"]))
    monkeypatch.setattr(owner.connection,"apply_lease",lease)
    await owner.sync()
    await owner.connection.emit(frame)
    await observed(published,owner.owner)
    session=next(iter(owner.owner._sessions.values()))
    return owner,store,frame,native,published,session,entered,release,calls


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_automatic_renewal_keeps_the_same_runtime_past_its_initial_deadline(lifecycle,monkeypatch):
    owner,_,frame,native,_,session,entered,_,calls=await setup(lifecycle,monkeypatch)
    initial=session.deadline
    await asyncio.wait_for(entered.wait(),3)
    async with asyncio.timeout(4):
        while time.monotonic()<=initial or session.deadline<=initial:
            await asyncio.sleep(.02)
    snapshot=await session.runtime.inspect(SessionKey(frame["server_id"],frame["executor_id"],frame["session_id"]))
    assert snapshot.lease_state=="ACTIVE" and snapshot.ownership=="owned"
    assert owner.ready and len(native.opened)==1 and not native.native.stopped
    assert calls[0].purpose=="initial" and calls[1].purpose=="renew"
    assert calls[1].expected_lease_serial==1 and calls[1].request_id!=calls[0].request_id


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_renewal_failure_does_not_extend_deadline_or_repeat_native_open(lifecycle,monkeypatch):
    owner,_,_,native,_,session,entered,_,_=await setup(lifecycle,monkeypatch,refuse=True)
    initial=session.deadline
    await asyncio.wait_for(entered.wait(),3)
    async with asyncio.timeout(3):
        while owner.owner.failure is None:
            await asyncio.sleep(.01)
    assert session.deadline==initial and not owner.connection.online
    assert owner.owner.failure.code=="AGENT_AUTH_REQUIRED"
    assert len(native.opened)==1


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_authority_change_while_grant_is_pending_prevents_install(lifecycle,monkeypatch):
    owner,store,_,_,_,session,entered,release,_=await setup(lifecycle,monkeypatch,hold=True)
    initial=session.deadline
    await asyncio.wait_for(entered.wait(),3)
    store.update(lambda state: setattr(state.execution_bindings[0],"state","REVOKED"))
    release.set()
    async with asyncio.timeout(3):
        while owner.owner.failure is None:
            await asyncio.sleep(.01)
    assert session.deadline==initial and not owner.connection.online


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_interrupt_progresses_while_productive_work_waits_for_renewal(lifecycle,monkeypatch):
    owner,_,frame,native,published,session,entered,release,_=await setup(lifecycle,monkeypatch,hold=True)
    try:
        await asyncio.wait_for(entered.wait(),3)
        await owner.connection.emit(operation(frame,"turn.submit",{"text":"After renewal"}))
        await owner.connection.emit(operation(frame,"turn.interrupt",{"reason":"Stop the turn"}))
        receipt=await observed(published,owner.owner)
        assert receipt["operation_id"]=="turn.interrupt"
        assert [row[0] for row in native.native.sent]==["interrupt"]
        release.set()
        receipt=await observed(published,owner.owner)
        assert receipt["operation_id"]=="turn.submit"
        assert [row[0] for row in native.native.sent]==["interrupt","send_turn"]
        assert owner.owner.failure is None
    finally:
        release.set()


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_cancelled_cleanup_observer_does_not_abandon_pending_renewal(lifecycle,monkeypatch):
    owner,_,_,native,_,session,entered,release,_=await setup(lifecycle,monkeypatch,hold=True)
    try:
        await asyncio.wait_for(entered.wait(),3)
        closing=asyncio.create_task(owner.close())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert not session.renewal.done() and not owner._close_task.done()
        retained_cleanup = owner._close_task
        release.set()
        try:
            await asyncio.wait_for(owner.close(),3)
        except TimeoutError as error:
            raise AssertionError(
                "Cleanup exceeded the existing three-second observation limit.\n"
                + pending_task_locations()) from error
        assert owner._close_task is retained_cleanup and retained_cleanup.done()
        assert session.renewal.done() and native.native.stopped
    finally:
        release.set()


async def test_productive_calls_share_the_gate_and_renewal_waits_for_all():
    from okto_nexus_connector.services.r4_execution import _RenewalGate
    gate=_RenewalGate()
    entered=[asyncio.Event(),asyncio.Event()]
    release=asyncio.Event()
    renewed=asyncio.Event()
    async def operation(index):
        async with gate.operation():
            entered[index].set()
            await release.wait()
    async def renewal():
        async with gate.renewal():
            renewed.set()
    tasks=[asyncio.create_task(operation(i)) for i in range(2)]
    renewing=None
    try:
        await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered)),2)
        assert gate.active==2
        renewing=asyncio.create_task(renewal())
        await asyncio.sleep(0)
        assert not renewed.is_set()
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks,renewing),2)
        assert renewed.is_set() and gate.active==0 and not gate.renewing
    finally:
        release.set()
        await asyncio.gather(*tasks,*([renewing] if renewing is not None else []))
