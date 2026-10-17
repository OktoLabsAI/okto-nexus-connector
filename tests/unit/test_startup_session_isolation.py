"""Preparation failure must not disconnect already usable sessions."""
import asyncio

import pytest
from nexus_connector_core import LocalRuntimeCore
from nexus_connector_core.native.adapter_types import NativeAdapterError, ErrorCode

from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, observed, operation, Native


@pytest.mark.parametrize('selection', [None, 'pi', 'claude'], indirect=True)
async def test_preparation_failure_preserves_control_and_healthy_session(execution, monkeypatch):
    owner, connection, factory, receipts, opening = execution
    peers = {}

    async def native_open(prepared, session_id, context, *, stream_epoch):
        peers[session_id] = Native()
        return peers[session_id]

    monkeypatch.setattr(factory, 'open', native_open)
    await connection.emit(opening)
    assert (await observed(receipts, owner))['stage'] == 'SUBMITTED'
    prepare = LocalRuntimeCore.prepare
    injected = asyncio.Event()

    async def failed_once(runtime, *args, **kwargs):
        if not injected.is_set():
            injected.set()
            raise NativeAdapterError(ErrorCode.CONFIG_ERROR, 'Version probe deadline',
                                     {'reason': 'version_probe_timeout'})
        return await prepare(runtime, *args, **kwargs)

    monkeypatch.setattr(LocalRuntimeCore, 'prepare', failed_once)
    await connection.emit({**opening, 'operation_id': 'failed-open', 'session_id': 'failed-session'})
    await asyncio.wait_for(injected.wait(), 15)
    async with asyncio.timeout(15):
        while owner.failure is None and receipts.empty():
            await asyncio.sleep(.01)
    assert owner.failure is None, 'Preparation failed the entire control connection'
    failed = await observed(receipts, owner)
    assert failed['operation_id'] == 'failed-open' and failed['stage'] == 'FAILED'
    assert failed['possible_effect'] is False
    assert connection.online and not peers[opening['session_id']].stopped
    assert 'failed-session' not in peers
    await connection.emit(operation(opening, 'turn.submit', {'text': 'Still usable'}))
    assert (await observed(receipts, owner))['stage'] == 'SUBMITTED'
    await connection.emit({**opening, 'operation_id': 'next-open', 'session_id': 'next-session'})
    assert (await observed(receipts, owner))['stage'] == 'SUBMITTED'
    assert set(peers) == {opening['session_id'], 'next-session'}
