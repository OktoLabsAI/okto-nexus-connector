"""Operator drain never borrows safety containment or abandons its owner."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from nexus_connector_core import CoreError

from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.daemon.drain import OperatorDrain
from okto_nexus_connector.daemon import lifecycle, manager
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform.service_context import read_document, write_document


def test_first_deadline_and_force_are_retained_and_persist_before_effects():
    now, calls = [10.0], []
    drain = OperatorDrain(clock=lambda: now[0], persist=lambda value: calls.append(('persist', value)),
                          begin=lambda: calls.append(('begin', None)))
    with pytest.raises(ConnectorError, match='existing drain'):
        drain.request(force=True)
    assert not calls
    drain.request(timeout_seconds=5)
    assert [name for name, _ in calls] == ['persist', 'begin']
    now[0] = 14
    drain.request(timeout_seconds=300)
    assert drain.deadline == 15 and len(calls) == 2
    with pytest.raises(ConnectorError):
        drain.request(force=True)
    now[0] = 15
    assert drain.status()['force_available'] and not drain.force_requested
    drain.request(force=True)
    drain.request(force=True)
    assert len(calls) == 3 and calls[-1][1]['force_requested'] is True


@pytest.mark.parametrize('value', [1, 'true', None, [], {}])
def test_force_is_strict_boolean_without_side_effects(value):
    persist, begin = Mock(), Mock()
    with pytest.raises(ConnectorError):
        OperatorDrain(persist=persist, begin=begin).request(force=value)
    persist.assert_not_called()
    begin.assert_not_called()


@pytest.fixture
def app(tmp_path):
    # No constructor: no vault, provider, discovery or personal service access.
    app = object.__new__(DaemonApp)
    app.root = tmp_path
    (tmp_path / 'run').mkdir()
    app.service_identity, app.service_generation = 'instance', 'generation'
    app._draining, app._active_requests = False, 0
    app._stopped, app._drain_wake, app._audit_stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    app._audit_task = None
    app.runtimes = SimpleNamespace(begin_drain=Mock(), shutdown=AsyncMock(return_value=[]))
    app.host = SimpleNamespace(begin_drain=Mock(), drain_status=AsyncMock(
        return_value={'state': 'BUSY', 'quiescent': False}), defer_store_close=Mock(),
        finish_drain_observation=AsyncMock(return_value=True))
    app.r4_controls, app.r4_executions, app.transports = {}, {}, {}
    app._stream_watchers = set()
    app.bridge = SimpleNamespace(quiescent=AsyncMock(return_value=True), drain=AsyncMock())
    app.ipc = SimpleNamespace(stop=AsyncMock())
    app.operator_drain = OperatorDrain(persist=app._persist_drain, begin=app._begin_drain)
    app._stop_latch = Mock()
    return app


async def reply(app, **params):
    return [item async for item in app.dispatch(SimpleNamespace(op='shutdown', params=params, seq=1))][0]


async def test_ipc_fences_instance_generation_types_and_early_force(app):
    for params in ({'force': True}, {'instance_id': 'foreign', 'generation': 'generation'},
                   {'instance_id': 'instance', 'generation': 'old'},
                   {'instance_id': 'instance', 'generation': 'generation', 'force': 1}):
        assert not (await reply(app, **params))['ok']
    assert not app._draining and not app._stopped.is_set()
    assert not (app.root / 'run' / 'drain.json').exists()
    result = await reply(app, instance_id='instance', generation='generation', timeout_seconds=1)
    assert result['ok'] and app._draining and not app._stopped.is_set()
    assert read_document(app.root / 'run' / 'drain.json')['desired'] == 'draining'
    assert not (await reply(app, instance_id='instance', generation='generation', force=True))['ok']


async def test_direct_ipc_latches_supervisor_before_admission_changes(app):
    effects = []
    def latch(**identity):
        assert identity == {'instance_id': 'instance', 'generation': 'generation'}
        assert read_document(app.root / 'run' / 'drain.json')['desired'] == 'draining'
        write_document(app.root / 'service-control.json', dict(desired='draining', **identity))
        effects.append('supervisor')
    app._stop_latch = latch
    app.host.begin_drain.side_effect = lambda: effects.append('core fence')
    assert (await reply(app))['ok']
    assert effects == ['supervisor', 'core fence']
    assert read_document(app.root / 'service-control.json')['generation'] == 'generation'


async def test_failed_supervisor_latch_prevents_core_and_force(app):
    app._stop_latch.side_effect = ConnectorError('CONFLICT', 'shutdown', 'Changed generation')
    assert not (await reply(app))['ok']
    app.host.begin_drain.assert_not_called()
    assert not app._draining and app.operator_drain.deadline is None


@pytest.mark.parametrize('observation', ['BUSY', 'UNKNOWN'])
async def test_timeout_keeps_control_until_explicit_force_or_quiescence(app, observation):
    now = [0]
    app.operator_drain.clock = lambda: now[0]
    app.host.drain_status.return_value = {'state': observation, 'quiescent': False}
    app.operator_drain.request(timeout_seconds=1)
    now[0] = 2
    observed = asyncio.Event()
    async def status():
        observed.set()
        return {'state': observation, 'quiescent': False}
    app.host.drain_status.side_effect = status
    app._shutdown = AsyncMock(return_value=0)
    task = asyncio.create_task(app._serve_until_drained())
    try:
        await asyncio.wait_for(observed.wait(), 3)
        app._shutdown.assert_not_called()
        assert not task.done() and app.operator_drain.status()['force_available']
        if observation == 'BUSY':
            app.host.drain_status.side_effect = None
            app.host.drain_status.return_value = {'state': 'QUIESCENT', 'quiescent': True}
        else:
            app.operator_drain.request(force=True)
        assert await asyncio.wait_for(task, 3) == 0
        app._shutdown.assert_awaited_once()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unknown_shutdown_does_not_dispose_ipc_publication_or_owner(app):
    app.runtimes.shutdown.return_value = [{'outcome': 'unknown'}]
    assert await app._shutdown() == 1
    app.ipc.stop.assert_not_called()
    app.bridge.drain.assert_not_called()
    assert read_document(app.root / 'run' / 'shutdown.json')['state'] == 'RECOVERY_REQUIRED'
    app.runtimes.shutdown.return_value = [{'outcome': 'already_closed'}]
    assert await app._shutdown() == 0
    app.ipc.stop.assert_awaited_once()
    assert read_document(app.root / 'run' / 'shutdown.json')['state'] == 'DRAINED'


async def test_shutdown_exception_keeps_owner_loop_and_control_available(app):
    failed = asyncio.Event()
    async def close():
        failed.set()
        raise OSError('private-sentinel')
    app._shutdown = AsyncMock(side_effect=close)
    app.request_stop()  # Safety containment does not discard unknown ownership either.
    task = asyncio.create_task(app._serve_until_drained())
    try:
        await asyncio.wait_for(failed.wait(), 3)
        assert not task.done() and app.operator_drain.observation == 'UNKNOWN'
        app.ipc.stop.assert_not_called()
        app._shutdown.side_effect = None
        app._shutdown.return_value = 0
        assert await asyncio.wait_for(task, 3) == 0
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_explicit_force_contains_while_status_reader_remains_owned(app):
    now, entered, release = [0], asyncio.Event(), asyncio.Event()
    app.operator_drain.clock = lambda: now[0]
    async def observe():
        entered.set()
        await release.wait()
        return {'state': 'UNKNOWN', 'quiescent': False}
    app.host.drain_status.side_effect = observe
    contained = asyncio.Event()
    async def shutdown():
        contained.set()
        return []
    app.runtimes.shutdown.side_effect = shutdown
    app.operator_drain.request(timeout_seconds=1)
    task = asyncio.create_task(app._serve_until_drained())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        reader = app._drain_read_task
        now[0] = 2
        assert (await reply(app, instance_id='instance', generation='generation', force=True))['ok']
        await asyncio.wait_for(contained.wait(), 3)
        assert app._drain_read_task is reader and not reader.done() and not reader.cancelled()
        assert not task.done()
        app.host.finish_drain_observation.assert_not_called()
        app.ipc.stop.assert_not_called()
        release.set()
        assert await asyncio.wait_for(task, 3) == 0
        app.host.finish_drain_observation.assert_awaited_once()
        app.ipc.stop.assert_awaited_once()
    finally:
        release.set()
        if not task.done():
            await asyncio.wait_for(task, 3)


async def test_unknown_control_is_retained_across_shutdown_retries(app):
    control = SimpleNamespace(stop=AsyncMock(), cleanup_pending=True,
        status=lambda: {'recovery_required': False})
    app.r4_controls['server'] = control
    for _ in range(2):
        assert await app._shutdown() == 1
        assert app.r4_controls['server'] is control
        app.ipc.stop.assert_not_called()
    control.cleanup_pending = False
    assert await app._shutdown() == 0 and not app.r4_controls
    assert control.stop.await_count == 3


def test_cli_force_requires_generation_before_status_or_ipc(tmp_path, monkeypatch):
    status = Mock()
    monkeypatch.setattr(manager, 'status', status)
    with pytest.raises(ConnectorError):
        lifecycle.stop(tmp_path, timeout=1, force=True)
    status.assert_not_called()


def test_repeated_force_uses_same_completed_generation(tmp_path, monkeypatch):
    (tmp_path / 'run').mkdir()
    current = manager.DaemonStatus(False, None, '', '', '', '', None, False, state='stopped')
    monkeypatch.setattr(manager, 'status', lambda *_a, **_k: current)
    write_document(tmp_path / 'run' / 'shutdown.json', {'generation': 'old', 'state': 'DRAINED'})
    assert lifecycle.stop(tmp_path, timeout=1, force=True, expected_generation='old')['stopped']
    assert lifecycle.stop(tmp_path, timeout=1, force=True, expected_generation='foreign')['reason'] == 'INSTANCE_CHANGED'


def test_cli_forwards_public_force_and_generation(tmp_path, monkeypatch, capsys):
    from okto_nexus_connector.cli.main import main
    stop = Mock(return_value={'stopped': True})
    monkeypatch.setattr(manager, 'stop', stop)
    assert main(['--state-dir', str(tmp_path), '--non-interactive', 'daemon', 'stop',
        '--force', '--expected-generation', 'reviewed-generation', '--timeout', '2', '--json']) == 0
    assert stop.call_args.kwargs == dict(timeout=2.0, force=True, expected_generation='reviewed-generation')
    assert 'true' in capsys.readouterr().out


async def test_host_missing_proof_and_inflight_composition_are_not_quiescent(tmp_path, monkeypatch):
    from okto_nexus_connector.services.core_host import CoreRuntimeHost, BindingKey
    host = CoreRuntimeHost(tmp_path, None)
    key = BindingKey('server', 'binding')
    host._runtimes[key] = SimpleNamespace(begin_drain=Mock())
    host.begin_drain()
    assert (await host.drain_status())['state'] == 'UNKNOWN'
    host._runtimes.clear()
    host._draining = False
    entered, release = asyncio.Event(), asyncio.Event()
    async def journal():
        entered.set()
        await release.wait()
    monkeypatch.setattr(host, 'candidate_for', lambda _: None)
    monkeypatch.setattr(host, 'ensure_journal', journal)
    monkeypatch.setattr(host, 'ensure_ledger', AsyncMock())
    monkeypatch.setattr(host, '_require_stores', Mock())
    task = asyncio.create_task(host.build(SimpleNamespace(server_id='server', binding_id='binding'), environment=None))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        host.begin_drain()
        assert (await host.drain_status())['state'] == 'BUSY'
        release.set()
        with pytest.raises(CoreError) as raised:
            await task
        assert raised.value.code == 'RUNTIME_DRAINING'
        assert not host._runtimes and (await host.drain_status())['quiescent']
    finally:
        release.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)


async def test_draining_capacity_refuses_new_reservations_but_keeps_history_and_release():
    from okto_nexus_connector.daemon.r4_execution import R4DaemonExecution
    execution = object.__new__(R4DaemonExecution)
    execution.connection = SimpleNamespace(online=True)
    execution.control = SimpleNamespace(server_id='s', executor_id='e', connection=execution.connection)
    execution.closing, execution.draining = False, True
    request = SimpleNamespace(scope=SimpleNamespace(server_id='s', executor_id='e'))
    assert await execution.check_capacity_scope(request, action='reserve') == 'RECOVERING'
    assert await execution.check_capacity_scope(request, action='probe') == 'RECOVERING'
    assert await execution.check_capacity_scope(request, action='get') == 'AVAILABLE'
    assert await execution.check_capacity_scope(request, action='release') == 'AVAILABLE'


def test_owner_supervisor_latch_proves_generation_inside_exclusion(tmp_path, monkeypatch):
    from okto_nexus_connector.platform.service_context import INSTALLATION, launch_exclusion
    (tmp_path / INSTALLATION).touch()
    current = SimpleNamespace(instance_id='instance', generation='generation')
    monkeypatch.setattr(manager, '_process_status', lambda *_a, **_k: current)
    service = Mock()
    def begin(generation):
        assert generation == 'generation'
        with pytest.raises(ConnectorError):
            with launch_exclusion(tmp_path):
                pass
    service.begin_stop.side_effect = begin
    monkeypatch.setattr(lifecycle, 'UserService', lambda _: service)
    lifecycle.persist_stop(tmp_path, instance_id='instance', generation='generation')
    service.begin_stop.assert_called_once()
    with pytest.raises(ConnectorError):
        lifecycle.persist_stop(tmp_path, instance_id='instance', generation='foreign')
    service.begin_stop.assert_called_once()


def test_force_refusal_from_ipc_is_not_reported_as_requested_containment(tmp_path, monkeypatch):
    (tmp_path / 'run').mkdir()
    current = manager.DaemonStatus(True, 1, 'loopback', 'synthetic', '', '', None, True,
        draining=True, state='stopping', instance_id='instance', generation='generation')
    status = Mock(return_value=current)
    monkeypatch.setattr(manager, 'status', status)
    monkeypatch.setattr(manager, '_process_status', lambda *_a, **_k: current)
    monkeypatch.setattr(manager, '_lock', lambda _: SimpleNamespace(load_token=lambda: 'synthetic-local-token'))
    client = Mock()
    client.__enter__, client.__exit__ = Mock(return_value=client), Mock(return_value=False)
    client.call.return_value = {'ok': False, 'error': {'code': 'DRAIN_NOT_EXPIRED', 'message': 'private-sentinel'}}
    monkeypatch.setattr(lifecycle, 'IPCClient', Mock(return_value=client))
    result = lifecycle.stop(tmp_path, timeout=1, force=True, expected_generation='generation')
    assert result['reason'] == 'DRAIN_NOT_EXPIRED' and not result['stopped']
    assert 'private-sentinel' not in str(result)
    status.assert_called_once()
