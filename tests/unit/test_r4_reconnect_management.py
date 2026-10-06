"""Reconnect diagnostics: real cause, operator attention and streak reset."""

import asyncio
import logging

from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from okto_nexus_connector.daemon import r4_control
from okto_nexus_connector.daemon.r4_control import R4DaemonControl, _disconnect_cause, _root_failure
from okto_nexus_connector.errors import ConnectorError


def test_cause_keeps_codes_and_drops_remote_text():
    error = ConnectorError('PERMISSION_DENIED', 'inventory.refresh.claim', 'token nxs_secret rejected')
    assert _disconnect_cause(error) == 'ConnectorError PERMISSION_DENIED stage=inventory.refresh.claim'
    closed = ConnectionClosedError(Close(1011, 'boom nxs_secret'), None)
    assert _disconnect_cause(closed) == 'ConnectionClosedError rcvd_close=1011'
    assert _disconnect_cause(None) is None


def _owner(errors, *, clock=None, ready_for=0):
    """Drive the real supervisor with a scripted attempt and no sleeping."""
    owner = R4DaemonControl(None, None, None, 'srv', retry_delays=(0.001,),
                            **({'clock': clock} if clock else {}))
    script = iter(errors)
    seen = []
    async def attempt():
        error = next(script, None)
        if error is None:
            owner._stopped.set()
            return
        if ready_for:
            owner._ready_since = owner.clock()
            clock.now += ready_for
        raise error
    async def wait(delay):
        seen.append((owner.phase, owner.consecutive_failures))
    owner._attempt, owner._wait = attempt, wait
    return owner, seen


async def test_repeated_network_failures_ask_for_attention_but_keep_retrying(caplog):
    caplog.set_level(logging.WARNING, logger=r4_control.__name__)
    owner, seen = _owner([ConnectorError('CONTROL_DISCONNECTED', 'r4_link', 'x')] * 7)
    await asyncio.wait_for(owner._run(), 5)
    assert [phase for phase, _ in seen] == ['RETRY_WAIT'] * 4 + ['RECOVERY_ATTENTION_REQUIRED'] * 3
    assert seen[-1][1] == 7
    assert sum('needs attention' in r.getMessage() for r in caplog.records) == 1
    assert 'cause=ConnectorError CONTROL_DISCONNECTED stage=r4_link' in caplog.records[0].getMessage()


async def test_persistent_server_refusal_asks_for_attention_on_second_failure():
    owner, seen = _owner([ConnectorError('PERMISSION_DENIED', 'inventory.refresh.claim', 'x')] * 2)
    await asyncio.wait_for(owner._run(), 5)
    assert [phase for phase, _ in seen] == ['RETRY_WAIT', 'RECOVERY_ATTENTION_REQUIRED']


async def test_reconciliation_keeps_recovering_phase():
    owner, seen = _owner([ConnectorError('RECONCILIATION_REQUIRED', 'r4_lanes', 'x')] * 6)
    await asyncio.wait_for(owner._run(), 5)
    assert {phase for phase, _ in seen} == {'RECOVERING'}


async def test_stable_connection_starts_a_new_failure_streak():
    class Clock:
        now = 0.0
        def __call__(self): return self.now
    clock = Clock()
    owner, seen = _owner([ConnectorError('CONTROL_DISCONNECTED', 'r4_link', 'x')] * 6,
                         clock=clock, ready_for=120)
    await asyncio.wait_for(owner._run(), 5)
    assert [count for _, count in seen] == [1] * 6
    assert {phase for phase, _ in seen} == {'RETRY_WAIT'}
    status = owner.status()
    assert status['consecutive_failures'] == 1 and not status['attention_required']
    assert status['last_disconnect_cause'] == 'ConnectorError CONTROL_DISCONNECTED stage=r4_link'


def _closed(stage='r4_link'):
    return ConnectorError('CONTROL_DISCONNECTED', stage, 'The control connection was closed.')


def test_root_failure_prefers_the_link_fence_over_waiting_callers():
    # Server error frame fenced the link; the owner's lease renewal only saw
    # the generic close and must not hide it (12:22 incident).
    server = ConnectorError('ATTACH_DENIED', 'r4_link', 'The Server rejected a control request.')
    assert _root_failure(server, _closed(), _closed()) is server
    remote = ConnectionClosedError(Close(1011, 'detail'), None)
    assert _root_failure(remote, _closed(), _closed()) is remote


def test_root_failure_keeps_a_specific_owner_or_visible_error():
    native = ConnectorError('NATIVE_OPERATION_FAILED', 'renewal', 'x')
    assert _root_failure(_closed(), native, _closed()) is native
    # A restart closed the link while an HTTP call failed with its own code.
    conflict = ConnectorError('CONFLICT', 'inventory.refresh.claim', 'x')
    assert _root_failure(_closed(), None, conflict) is conflict


def test_root_failure_falls_back_to_the_first_generic_failure():
    first, visible = _closed(), _closed('r4_lanes')
    assert _root_failure(first, None, visible) is first
    assert _root_failure(None, None, visible) is visible
    assert _root_failure(None, None, None) is None
