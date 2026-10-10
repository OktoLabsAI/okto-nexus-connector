"""Automatic session renewal uses Core deadlines and retains its producer."""
import asyncio
import time

import pytest
from nexus_connector_core import R4_PREVIEW_REVISION, SessionKey

from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle
from tests.unit.test_r4_execution import observed, operation, failed


from tests.unit.async_diagnostics import pending_task_locations


class LeaseClock:
    now = 100.0

    def monotonic(self):
        return self.now


async def renewal_entered(owner, entered):
    try:
        await asyncio.wait_for(entered.wait(), 3)
    except TimeoutError as error:
        code = getattr(owner.owner.failure, 'code', None)
        if code not in (None, 'LEASE_EXPIRED', 'AGENT_AUTH_REQUIRED', 'CONTROL_DISCONNECTED', 'STALE_GENERATION'):
            code = 'OTHER'
        error.add_note(f'Renewal entry timed out; owner failure code: {code}\n' + pending_task_locations())
        raise


async def setup(lifecycle, monkeypatch, *, hold=False, refuse=False, clock=None, wait_open=True):
    owner, _, store, frame, native, _, published, _ = lifecycle
    if clock is not None:
        from okto_nexus_connector.services import core_host
        original = core_host.create_runtime
        def controlled_runtime(**kwargs):
            return original(**kwargs, clock=clock)
        monkeypatch.setattr(core_host, "create_runtime", controlled_runtime)
        owner.clock = clock.monotonic
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    last_request_at = clock.now if clock is not None else None
    async def lease(runtime, *, scope, grant_id, purpose="initial", require_current=None, fence_on_error=True):
        nonlocal last_request_at
        if clock is not None and purpose == "renew":
            # Advance to the next request time, preserving explicit advances
            # made by expiry/boundary tests instead of adding host latency.
            clock.now = max(clock.now, last_request_at + .8)
            last_request_at = clock.now
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
    if wait_open:
        await observed(published,owner.owner)
    else:
        async with asyncio.timeout(3):
            while not owner.owner._sessions:
                await asyncio.sleep(.01)
    session=next(iter(owner.owner._sessions.values()))
    return owner,store,frame,native,published,session,entered,release,calls


@pytest.mark.parametrize("selection", [True], indirect=True)
async def test_renewal_starts_while_native_open_is_still_pending(lifecycle, monkeypatch):
    native = lifecycle[4]
    entered_open, release_open = asyncio.Event(), asyncio.Event()
    original = native.open
    async def held(*args, **kwargs):
        entered_open.set()
        await release_open.wait()
        return await original(*args, **kwargs)
    monkeypatch.setattr(native, 'open', held)
    owner, _, _, _, published, session, renewed, _, calls = await setup(
        lifecycle, monkeypatch, clock=LeaseClock(), wait_open=False)
    try:
        await asyncio.wait_for(entered_open.wait(), 3)
        await renewal_entered(owner, renewed)
        async with asyncio.timeout(3):
            while session.deadline <= calls[0].sent_at_monotonic + 1.6:
                await asyncio.sleep(.01)
        assert published.empty() and not native.opened
    finally:
        release_open.set()
    await observed(published, owner.owner)
    assert len(native.opened) == 1 and owner.owner.failure is None


@pytest.mark.parametrize("selection",[True],indirect=True)
@pytest.mark.parametrize("scheduling_delay", [0, 1.7], ids=["normal", "delayed-loop"])
async def test_automatic_renewal_keeps_the_same_runtime_past_its_initial_deadline(lifecycle,monkeypatch,scheduling_delay):
    clock = LeaseClock()
    owner,_,frame,native,_,session,entered,_,calls=await setup(lifecycle,monkeypatch,clock=clock)
    initial=session.deadline
    time.sleep(scheduling_delay)
    await renewal_entered(owner, entered)
    async with asyncio.timeout(4):
        while session.deadline<=initial:
            await asyncio.sleep(.02)
    # Cross the original authority boundary only after a renewed grant is
    # installed. The exact same public clock drives Core and the daemon.
    clock.now = max(clock.now, initial + .01)
    assert initial < clock.monotonic() < session.deadline
    snapshot=await session.runtime.inspect(SessionKey(frame["server_id"],frame["executor_id"],frame["session_id"]))
    assert snapshot.lease_state=="ACTIVE" and snapshot.ownership=="owned"
    assert owner.ready and len(native.opened)==1 and not native.native.stopped
    assert calls[0].purpose=="initial" and calls[1].purpose=="renew"
    assert calls[1].expected_lease_serial==1 and calls[1].request_id!=calls[0].request_id


@pytest.mark.parametrize("selection",[True],indirect=True)
@pytest.mark.parametrize("scheduling_delay", [0, 1.7], ids=["normal", "delayed-loop"])
async def test_renewal_failure_does_not_extend_deadline_or_repeat_native_open(lifecycle,monkeypatch,scheduling_delay):
    owner,_,_,native,_,session,entered,_,_=await setup(lifecycle,monkeypatch,refuse=True,clock=LeaseClock())
    initial=session.deadline
    time.sleep(scheduling_delay)
    await renewal_entered(owner, entered)
    async with asyncio.timeout(3):
        while owner.owner.failure is None:
            await asyncio.sleep(.01)
    assert session.deadline==initial and not owner.connection.online
    assert owner.owner.failure.code=="AGENT_AUTH_REQUIRED"
    assert len(native.opened)==1


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_authority_change_while_grant_is_pending_prevents_install(lifecycle,monkeypatch):
    owner,store,_,_,_,session,entered,release,_=await setup(lifecycle,monkeypatch,hold=True,clock=LeaseClock())
    initial=session.deadline
    await renewal_entered(owner, entered)
    store.update(lambda state: setattr(state.execution_bindings[0],"state","REVOKED"))
    release.set()
    async with asyncio.timeout(3):
        while owner.owner.failure is None:
            await asyncio.sleep(.01)
    assert session.deadline==initial and not owner.connection.online


@pytest.mark.parametrize("selection",[True],indirect=True)
@pytest.mark.parametrize("expire_before_reply", [False, True])
async def test_interrupt_progresses_while_productive_work_waits_for_renewal(lifecycle,monkeypatch,expire_before_reply):
    class Clock:
        now = 100.0
        def monotonic(self):
            return self.now
    clock = Clock()
    owner,_,frame,native,published,session,entered,release,_=await setup(
        lifecycle,monkeypatch,hold=True,clock=clock)
    try:
        await asyncio.wait_for(entered.wait(),3)
        # Both owners use the same public injected Core clock. Disk latency
        # cannot consume the authority window of this ordering test.
        await owner.connection.emit(operation(frame,"turn.submit",{"text":"After renewal"}))
        await owner.connection.emit(operation(frame,"turn.interrupt",{"reason":"Stop the turn"}))
        receipt=await observed(published,owner.owner)
        assert receipt["operation_id"]=="turn.interrupt"
        assert [row[0] for row in native.native.sent]==["interrupt"]
        if expire_before_reply:
            clock.now = session.deadline
        release.set()
        if expire_before_reply:
            await failed(owner.owner)
            assert owner.owner.failure.code == "LEASE_EXPIRED"
            assert [row[0] for row in native.native.sent] == ["interrupt"]
            return
        receipt=await observed(published,owner.owner)
        assert receipt["operation_id"]=="turn.submit"
        assert [row[0] for row in native.native.sent]==["interrupt","send_turn"]
        assert owner.owner.failure is None
    finally:
        release.set()


@pytest.mark.parametrize("selection",[True],indirect=True)
@pytest.mark.parametrize("scheduling_delay", [0, 1.7], ids=["normal", "delayed-loop"])
async def test_cancelled_cleanup_observer_does_not_abandon_pending_renewal(lifecycle,monkeypatch,scheduling_delay):
    owner,_,_,native,_,session,entered,release,_=await setup(
        lifecycle,monkeypatch,hold=True,clock=LeaseClock())
    try:
        # Deliberate scheduling fault: the renewal task cannot run during this
        # pause. The cleanup ordering assertion must not depend on host speed.
        time.sleep(scheduling_delay)
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


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_expired_lease_before_renewal_refuses_another_request(lifecycle,monkeypatch):
    clock = LeaseClock()
    owner,_,_,native,_,session,entered,release,calls=await setup(
        lifecycle,monkeypatch,hold=True,clock=clock)
    try:
        initial = session.deadline
        clock.now = initial
        await failed(owner.owner)
        assert owner.owner.failure.code == "LEASE_EXPIRED"
        assert session.deadline == initial and not owner.connection.online
        assert not entered.is_set() and len(calls) == 1
        assert len(native.opened) == 1
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
