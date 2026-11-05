"""Wizard + real lifecycle/installer/checkpoints with injectable native effects."""
import os
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from . import test_configure as wizard_fixtures
from .test_user_service_lifecycle import FakeTasks
from okto_nexus_connector.cli.commands import configure, configure_local as local
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.daemon import manager, lifecycle
from okto_nexus_connector.daemon.lock import InstanceLock
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.platform import paths, service_install
from okto_nexus_connector.platform.service_context import CONTEXT, write_document
from okto_nexus_connector.platform.user_service import UserService, SUPERVISOR
from okto_nexus_connector.storage.state_store import StateStore

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows user-task contract; native effects injected')
local_wizard = wizard_fixtures.local_wizard


@pytest.fixture
def login(local_wizard, monkeypatch):
    args, output, root, old_store, identity, portable, calls, gate = local_wizard
    args.start_at_login = True
    # Retain the wizard fixture's network peers but persist every preference
    # through the real store; reopening must not retain unsaved Python objects.
    shadow = old_store.load()
    disk = StateStore(paths.state_file(root))
    def load():
        shadow.preferences = disk.load().preferences
        return shadow
    def update(change):
        def write(current):
            shadow.preferences = current.preferences
            change(shadow)
            current.preferences = shadow.preferences
        return disk.update(write)
    store = NS(load=load, update=update)
    key = 'configure.server.agent.wizard-test'
    tasks = FakeTasks()
    monkeypatch.setattr('okto_nexus_connector.platform.user_service.WindowsTasks', lambda: tasks)
    service = UserService(root, tasks=tasks)
    process = dict(active=True, supervisor='detached', drain=True, ready=True)
    effects = []
    def observed(*args, **kwargs):
        return manager.DaemonStatus(process['active'], os.getpid() if process['active'] else None,
            'loopback', '127.0.0.1:1', '0.0.12', '', None, process['active'],
            state='ready' if process['active'] else 'stopped', instance_id=service.instance_id,
            generation='generation' if process['active'] else '', supervisor=process['supervisor'])
    monkeypatch.setattr(manager, '_process_status', observed)
    monkeypatch.setattr(InstanceLock, 'load_token', lambda _: 'synthetic-ipc-token')
    client = Mock()
    client.__enter__ = Mock(return_value=client)
    client.__exit__ = Mock(return_value=False)
    def shutdown(*args, **kwargs):
        effects.append('drain')
        if process['drain']:
            write_document(root / 'run/shutdown.json', {'generation': 'generation', 'state': 'DRAINED'})
            process['active'] = False
        return {'ok': True}
    client.call.side_effect = shutdown
    monkeypatch.setattr(lifecycle, 'IPCClient', Mock(return_value=client))
    monkeypatch.setattr(manager, 'start', lambda root: lifecycle.start(root, timeout=.3))
    monkeypatch.setattr(manager, 'stop', lambda root: lifecycle.stop(root, timeout=.1))
    monkeypatch.setattr(manager, '_spawn', lambda *args: pytest.fail('Detached fallback or duplicate process'))
    create = tasks.create
    def checked_create(name, xml):
        assert disk.load().preferences[key]['login_start_operation']['state'] == 'pending'
        return create(name, xml)
    tasks.create = checked_create
    start = tasks.start
    def native_start(name, expected):
        start(name, expected)
        process.update(active=process['ready'], supervisor=SUPERVISOR)
    tasks.start = native_start
    async def run():
        return await local.configure_local(args, output, root, store, identity, 'http://127.0.0.1:8202', portable)
    return NS(args=args, output=output, root=root, store=store, disk=disk, key=key, tasks=tasks,
        service=service, process=process, effects=effects, calls=calls, gate=gate, run=run)


async def test_same_review_installs_before_remote_approval_then_reuses_saved_host_choice(login):
    r = login
    pending = await r.run()
    assert pending['status'] == 'awaiting_operator_approval'
    assert pending['login_start']['operation_state'] == 'done'
    assert r.effects == ['drain'] and [c[0] for c in r.tasks.calls] == ['create', 'start']
    saved = r.disk.load().preferences[r.key]
    assert saved['login_start_plan']['scope']['root'] == str(r.root.resolve())
    assert 'environment' not in saved['login_start_plan']['scope']
    assert saved['stage'] == 'apply' and saved['login_start_operation']['state'] == 'done'
    assert str(r.root) in r.output.stream.getvalue()
    assert 'secret-not-in-output' not in r.output.stream.getvalue()
    r.gate['approved'] = True
    assert (await r.run())['saved']
    before = list(r.calls)
    assert (await r.run())['saved']
    assert r.calls == before
    assert [c[0] for c in r.tasks.calls] == ['create', 'start']
    assert all(r.calls.count(stage) == 1 for stage in ('register', 'probe', 'launch', 'prepare', 'executor.realize'))


async def test_declining_login_retains_on_demand_without_native_effects(login):
    r = login
    r.args.start_at_login = None  # The visible default choice is no startup change.
    assert (await r.run())['status'] == 'awaiting_operator_approval'
    assert not r.tasks.calls and not r.effects
    assert 'login_start_plan' not in r.disk.load().preferences[r.key]
    assert r.process['supervisor'] == 'detached'


async def test_cancel_after_startup_review_has_no_host_or_onboarding_effect(login, monkeypatch):
    r = login
    monkeypatch.setattr(local, 'choice', lambda label, options, default=None: False if label.startswith('7.') else options[0][0])
    assert (await r.run())['canceled']
    assert not r.disk.load().preferences and not r.tasks.calls and not r.calls and not r.effects


@pytest.mark.parametrize('failure', ['drain', 'checkpoint', 'readiness'])
async def test_partial_host_transition_reopens_same_intent_without_duplicate_effects(login, monkeypatch, failure):
    r = login
    if failure == 'drain':
        r.process['drain'] = False
    elif failure == 'readiness':
        r.process['ready'] = False
    else:
        update = r.store.update
        failed = False
        def lose_checkpoint(change):
            nonlocal failed
            def check(state):
                nonlocal failed
                change(state)
                if state.preferences[r.key]['login_start_operation']['state'] == 'installed' and not failed:
                    failed = True
                    raise OSError('checkpoint response loss')
            return update(check)
        monkeypatch.setattr(r.store, 'update', lose_checkpoint)
    with pytest.raises(ConnectorError) as error:
        await r.run()
    assert error.value.possible_effect
    assert r.disk.load().preferences[r.key]['stage'] == 'register'
    assert r.disk.load().preferences[r.key]['login_start_operation']['state'] == ('installed' if failure == 'readiness' else 'pending')
    assert not r.calls  # No registration, probe, realization or proposal was repeated.
    if failure == 'readiness':
        r.process['active'] = True  # The same task becomes ready after the CLI timeout.
    r.process.update(drain=True, ready=True)
    assert (await r.run())['status'] == 'awaiting_operator_approval'
    assert [c[0] for c in r.tasks.calls] == ['create', 'start']
    assert all(r.calls.count(stage) == 1 for stage in ('register', 'probe', 'launch', 'prepare'))


async def test_unknown_native_registration_is_not_repeated_or_adopted(login, monkeypatch):
    r = login
    create = r.tasks.create
    def lose(name, xml):
        create(name, xml)
        raise OSError('native response lost')
    monkeypatch.setattr(r.tasks, 'create', lose)
    with pytest.raises(ConnectorError) as error:
        await r.run()
    assert error.value.possible_effect and error.value.code == 'LOGIN_START_UNCONFIRMED'
    with pytest.raises(ConnectorError) as error:
        await r.run()
    assert error.value.code == 'INSTALLATION_PENDING'
    assert [c[0] for c in r.tasks.calls] == ['create'] and not r.calls


@pytest.mark.parametrize('change', ['context', 'project', 'startup'])
async def test_changed_review_is_refused_before_resuming_native_effects(login, change, tmp_path):
    r = login
    r.process['drain'] = False
    with pytest.raises(ConnectorError):
        await r.run()
    before = list(r.effects)
    if change == 'context':
        # Persist another valid context with a different retained PATH.
        from okto_nexus_connector.platform.service_context import saved_context
        context = saved_context(r.root)
        context['environment']['PATH'] = ''
        write_document(r.root / CONTEXT, context)
    elif change == 'project':
        r.args.project = str(tmp_path / 'another-project')
    else:
        r.args.start_at_login = False
    with pytest.raises(ConnectorError) as error:
        await r.run()
    assert error.value.code == ('LOGIN_START_REVIEW_CHANGED' if change == 'context' else 'OPERATION_CONFLICT')
    assert not r.tasks.calls and not r.calls and r.effects == before


async def test_disabled_task_requires_enable_review_and_later_disable_is_preserved(login):
    r = login
    r.process['active'] = False
    # Existing installation predates this onboarding request.
    r.tasks.create = FakeTasks.create.__get__(r.tasks)
    assert service_install.install(r.root, dry_run=False)['installed']
    service_install.set_enabled(r.root, False)
    r.process.update(active=True, supervisor=SUPERVISOR)
    r.gate['approved'] = True
    assert (await r.run())['saved']
    saved = r.disk.load().preferences[r.key]
    assert saved['login_start_plan']['action'] == 'enable'
    assert service_install.status(r.root)['enabled']
    assert not r.effects and [c[0] for c in r.tasks.calls].count('create') == 1
    service_install.set_enabled(r.root, False)
    before = list(r.tasks.calls)
    result = await r.run()
    assert result['login_start']['current_user_service']['enabled'] is False
    assert r.tasks.calls == before


@pytest.mark.parametrize('already_enabled', [False, True])
async def test_existing_task_reuse_or_lost_enablement_receipt_does_not_recreate(login, monkeypatch, already_enabled):
    r = login
    r.process['active'] = False
    r.tasks.create = FakeTasks.create.__get__(r.tasks)
    assert service_install.install(r.root, dry_run=False)['installed']
    if not already_enabled:
        service_install.set_enabled(r.root, False)
    r.process.update(active=True, supervisor=SUPERVISOR)
    if not already_enabled:
        setter = r.tasks.set_enabled
        lost = False
        def lose(name, expected, enabled):
            nonlocal lost
            result = setter(name, expected, enabled)
            if not lost:
                lost = True
                raise OSError('native enable response lost')
            return result
        monkeypatch.setattr(r.tasks, 'set_enabled', lose)
        with pytest.raises(ConnectorError) as error:
            await r.run()
        assert error.value.possible_effect
        assert r.service.receipt()['enablement']['target'] is True
        assert not r.calls
    assert (await r.run())['status'] == 'awaiting_operator_approval'
    assert service_install.status(r.root)['enabled']
    assert r.disk.load().preferences[r.key]['login_start_plan']['action'] == ('reuse' if already_enabled else 'enable')
    assert [c[0] for c in r.tasks.calls].count('create') == 1
    assert [c for c in r.tasks.calls if c[0] == 'enabled' and c[2]] == ([] if already_enabled else [('enabled', r.service.name, True)])
    assert not r.effects


async def test_server_host_refuses_local_startup_before_identity_access(login, monkeypatch):
    r = login
    args = build_parser().parse_args(['configure', '--host', 'server', '--start-at-login'])
    async def unexpected(*args):
        pytest.fail('Identity accessed for invalid topology')
    monkeypatch.setattr(configure, 'select_identity', unexpected)
    with pytest.raises(ConnectorError) as error:
        await configure.run_configure(args, r.output, r.root)
    assert error.value.code == 'REMOTE_STARTUP_NOT_LOCAL'
