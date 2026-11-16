"""Pending local reviews keep old Finish recovery and immutable child bodies."""
from copy import deepcopy
from dataclasses import replace

import pytest
from nexus_connector_core.discovery import fingerprint
from nexus_connector_core.installation import effective_installation_ref

from okto_nexus_connector.cli.commands import connect, configure_local, connect_setup_review
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services import remote_setup as setup
from okto_nexus_connector.storage.state_store import BindingIntentRecord
from tests.unit import test_remote_setup as fixtures

host = fixtures.host
peer = fixtures.peer
continuation = fixtures.continuation
binding = fixtures.binding
onboarding = fixtures.onboarding
registration = fixtures.registration


def acknowledged(host):
    record = fixtures.draft(host)
    original = fixtures.handoff(record)
    record = setup.accept_handoff(host.store, 'setup-test', record, original, original)
    row = BindingIntentRecord('srv_fake', 'executor', 'agent', 'worker', 'setup-test_prepare', 'project',
        'realization', 'scope', status='APPLY_PENDING', proposal={'proposal_id': 'old-proposal'},
        apply_client_intent_id='setup-test_apply', remote_setup={'setup_run_id': 'setup-test', 'review_revision': 'a' * 64})
    saved = dict(stage='apply', identity='worker', executor_id='executor', realization_ref='realization',
        configuration=deepcopy(record['local']['configuration']), inventory_revision='sha256:' + 'c' * 64,
        candidate_ref=record['draft']['installation']['candidate_ref'], workspace_id='canonical-workspace',
        replace_binding_id=None, proposal=deepcopy(row.proposal), single_approval=True,
        child_requests=deepcopy(original['child_requests']), remote_setup=deepcopy(row.remote_setup),
        local_consent_id='setup-test_consent', probe_operation='confirmed')
    def stage(state):
        state.binding_intents.append(row)
        state.preferences['configure.srv_fake.agent.setup-test'] = saved
    host.store.update(stage)
    return record, original, deepcopy(saved)


def revised(host, expected, change='project'):
    args = deepcopy(host.parameters)
    if change == 'project':
        path = host.root / 'other-project'
        path.mkdir(exist_ok=True)
        args['configuration']['workspace_root'] = str(path)
        args['configuration']['workspace_label'] = 'Other project'
    elif change == 'provider':
        args['configuration']['provider_home'] = str(host.project)
    elif change == 'alias':
        args['configuration']['alias'] = 'new-local-alias'
    elif change == 'candidate':
        path = host.root / 'other-codex.exe'
        path.write_bytes(b'another synthetic binary, never executed')
        args['candidate'] = replace(host.candidate, executable=str(path), fingerprint=fingerprint(path))
        args['candidate_ref'] = effective_installation_ref(args['candidate'])
    elif change == 'startup':
        args['login_start_plan'] = {'plan_revision': 'new-startup', 'action': 'synthetic-never-executed'}
    return setup.prepare(host.store, **args, review_current=True, expected=expected)


def next_handoff(previous, pending, *, native):
    current = deepcopy(previous)
    current.update(review_revision='b' * 64, draft_revision=pending['draft_revision'])
    for phase in ('realize', 'prepare', 'apply') if native else ('prepare', 'apply'):
        current['child_requests'][phase] += '_' + 'e' * 24
    return current


@pytest.mark.parametrize('change', ['project', 'provider', 'alias', 'candidate'])
def test_local_material_changes_remain_pending_until_current_server_handoff(host, change):
    original, old_handoff, saved = acknowledged(host)
    pending = revised(host, original, change)
    assert pending['draft'] == original['draft'] and pending['local'] == original['local']
    local = pending['pending_local_review']
    assert local['native_changed'] is True
    assert local['draft']['local_consent']['local_consent_id'] != original['draft']['local_consent']['local_consent_id']
    assert host.store.load().binding_intents[0].status == 'REVIEW_SUSPENDED'
    assert host.store.load().preferences['configure.srv_fake.agent.setup-test'] == saved
    current = next_handoff(old_handoff, local, native=True)
    promoted = setup.accept_handoff(host.store, 'setup-test', pending, current, current,
        progress={'state': 'awaiting_host', 'handoff': current, 'final_binding': None})
    assert promoted['handoff']['bootstrap'] == old_handoff['bootstrap']
    assert promoted['local'] == local['local'] and 'pending_local_review' not in promoted
    assert promoted['handoff_history'][0]['local'] == original['local']
    now = host.store.load().preferences['configure.srv_fake.agent.setup-test']
    assert now['stage'] == ('probe' if change == 'candidate' else 'launch')
    assert now['child_requests']['realize'] != saved['child_requests']['realize']
    assert 'realization_ref' not in now and 'proposal' not in now
    assert host.store.load().binding_intents[0].proposal == saved['proposal']


def test_startup_only_review_preserves_consent_and_realization(host):
    original, old_handoff, saved = acknowledged(host)
    pending = revised(host, original, 'startup')
    local = pending['pending_local_review']
    assert not local['native_changed'] and local['startup_changed']
    assert local['draft']['local_consent'] == original['draft']['local_consent']
    current = next_handoff(old_handoff, local, native=False)
    setup.accept_handoff(host.store, 'setup-test', pending, current, current,
        progress={'state': 'awaiting_host', 'handoff': current, 'final_binding': None})
    now = host.store.load().preferences['configure.srv_fake.agent.setup-test']
    assert now['stage'] == 'prepare' and now['realization_ref'] == saved['realization_ref']
    assert now['child_requests']['realize'] == saved['child_requests']['realize']
    assert now['login_start_operation']['state'] == 'pending'


def test_same_startup_choice_with_changed_concrete_plan_refused_before_pending(host):
    host.parameters['login_start_plan'] = {'plan_revision': 'original-startup', 'action': 'synthetic-never-executed'}
    original, _, saved = acknowledged(host)
    before = host.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        revised(host, original, 'startup')
    assert error.value.code == 'OPERATION_CONFLICT'
    assert 'service controls' in error.value.message
    assert host.store.path.read_bytes() == before
    assert 'pending_local_review' not in setup.load(host.store, 'setup-test')
    assert host.store.load().preferences['configure.srv_fake.agent.setup-test'] == saved


def test_unchanged_review_has_no_new_consent_or_pending_effects(host):
    original, _, saved = acknowledged(host)
    assert revised(host, original, 'unchanged') == original
    assert host.store.load().preferences['configure.srv_fake.agent.setup-test'] == saved
    assert host.store.load().binding_intents[0].status == 'APPLY_PENDING'


def test_same_root_revision_cas_never_overwrites_concurrent_progress(host):
    original, _, _ = acknowledged(host)
    winner = setup.checkpoint(host.store, 'setup-test', original, stage='updated-by-other-client')
    with pytest.raises(ConnectorError) as error:
        revised(host, original)
    assert error.value.code == 'STALE_GENERATION'
    assert setup.load(host.store, 'setup-test') == winner


@pytest.mark.parametrize('limit', ['history', 'checkpoint', 'state'])
def test_pending_review_reserves_capacity_for_racing_finish(host, limit, monkeypatch):
    original, _, _ = acknowledged(host)
    def fill(state):
        if limit == 'history':
            state.preferences[setup.PREFIX + 'setup-test']['handoff_history'] = [{'retained': index} for index in range(16)]
        elif limit == 'state':
            state.preferences['other'] = 'x' * (4 * 1024 * 1024 - 11000)
    host.store.update(fill)
    original = setup.load(host.store, 'setup-test')
    if limit == 'checkpoint':
        monkeypatch.setattr(setup, 'MAX_DOCUMENT_BYTES', 6500)
    before = host.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        revised(host, original)
    assert error.value.code == 'CAPACITY_EXCEEDED'
    assert host.store.path.read_bytes() == before
    assert host.store.load().binding_intents[0].status == 'APPLY_PENDING'


@pytest.mark.parametrize('confirmed', ['local-binding', 'root-result'])
def test_confirmed_finish_cannot_get_a_local_revision(host, confirmed):
    original, _, _ = acknowledged(host)
    def done(state):
        if confirmed == 'local-binding':
            state.binding_intents[0].status = 'APPLIED'
        else:
            state.preferences[setup.PREFIX + 'setup-test']['stage'] = 'configured'
    host.store.update(done)
    record = setup.load(host.store, 'setup-test')
    before = host.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        revised(host, record)
    assert error.value.code == 'OPERATION_CONFLICT' and host.store.path.read_bytes() == before


def test_lost_old_apply_can_be_recovered_after_pending_review_race(host):
    original, old_handoff, saved = acknowledged(host)
    pending = revised(host, original)
    final = dict(server_id='srv_fake', agent_id='agent', workspace_id='canonical-workspace', executor_id='executor',
        proposal_id='old-proposal', finish_id='old-proposal_setup', binding_id='old-binding')
    recovered = setup.accept_handoff(host.store, 'setup-test', pending, old_handoff, old_handoff,
        progress={'state': 'committed', 'handoff': old_handoff, 'final_binding': final})
    assert recovered['draft'] == original['draft'] and recovered['local'] == original['local']
    assert recovered['recovered_final_binding'] == final and 'pending_local_review' not in recovered
    assert recovered['handoff_history'][0]['outcome'] == 'prior_finish_committed'
    assert host.store.load().binding_intents[0].status == 'APPLY_PENDING'
    assert host.store.load().binding_intents[0].apply_client_intent_id == 'setup-test_apply'
    assert host.store.load().preferences['configure.srv_fake.agent.setup-test'] == saved
    with pytest.raises(ConnectorError):
        revised(host, recovered)


def test_pending_local_review_never_reactivates_old_uncommitted_apply(host):
    original, old_handoff, _ = acknowledged(host)
    pending = revised(host, original)
    with pytest.raises(ConnectorError) as error:
        setup.accept_handoff(host.store, 'setup-test', pending, old_handoff, old_handoff,
            progress={'state': 'awaiting_host', 'handoff': old_handoff, 'final_binding': None})
    assert error.value.code == 'REMOTE_SETUP_REVIEW_REQUIRED'
    assert host.store.load().binding_intents[0].status == 'REVIEW_SUSPENDED'


async def test_inflight_apply_ack_cannot_overwrite_suspended_local_review(binding):
    service, peer, store, prepare_args, apply_args = binding
    await service.prepare(**prepare_args)
    peer.approved = True
    peer.during_apply = lambda: store.update(lambda state: setattr(state.binding_intents[0], 'status', 'REVIEW_SUSPENDED'))
    with pytest.raises(ConnectorError) as error:
        await service.apply(**apply_args)
    assert error.value.code == 'REMOTE_SETUP_REVIEW_REQUIRED'
    retained = store.load().binding_intents[0]
    assert retained.apply_client_intent_id == apply_args['client_intent_id']
    assert retained.status == 'REVIEW_SUSPENDED' and retained.proposal is not None


@pytest.mark.parametrize('confirmed', [False, True])
async def test_cli_local_review_is_explicit_and_reexport_is_stable(continuation, monkeypatch, confirmed):
    r = continuation
    async def pipeline(*a): return {'saved': False}
    monkeypatch.setattr(configure_local, 'configure_local', pipeline)
    await connect.run_connect(r.args, r.host.output, r.host.root)
    old = setup.load(r.host.store, 'setup-test')
    destination = r.host.root / 'revised.json'
    project = r.host.root / 'new-project'
    project.mkdir()
    args = build_parser().parse_args(['connect', '--setup-draft', str(destination), '--request-id', 'setup-test',
                                     '--review-current', '--project', str(project)])
    async def selected(args, output):
        args.discovery_inventory_revision = r.host.parameters['inventory_revision']
        return r.host.candidate.adapter_id, r.host.candidate, None
    monkeypatch.setattr(connect, '_choose_harness', selected)
    monkeypatch.setattr(connect_setup_review, 'choice', lambda *a, **k: confirmed)
    result = await connect.run_connect(args, r.host.output, r.host.root)
    current = setup.load(r.host.store, 'setup-test')
    assert current['handoff'] == old['handoff'] and current['local'] == old['local']
    if not confirmed:
        assert result['canceled'] and current == old and not destination.exists()
    else:
        assert result['status'] == 'awaiting_operator_review'
        saved_bytes = destination.read_bytes()
        async def unexpected(*a): pytest.fail('rediscovery on reexport')
        monkeypatch.setattr(connect, '_choose_harness', unexpected)
        await connect.run_connect(args, r.host.output, r.host.root)
        assert destination.read_bytes() == saved_bytes
        assert setup.load(r.host.store, 'setup-test') == current


@pytest.mark.parametrize('physical_drift', [False, True])
async def test_cli_recovers_exact_prior_finish_over_tcp_even_with_pending_local_review(continuation, monkeypatch, physical_drift):
    r = continuation
    async def initial(*a): return {'saved': False}
    monkeypatch.setattr(configure_local, 'configure_local', initial)
    await connect.run_connect(r.args, r.host.output, r.host.root)
    record = setup.load(r.host.store, 'setup-test')
    old = record['handoff']
    final = dict(server_id='srv_fake', agent_id='agent', workspace_id='canonical-workspace', executor_id='executor',
        proposal_id='old-proposal', finish_id='old-proposal_setup', binding_id='old-binding')
    def saved(state):
        state.binding_intents.append(BindingIntentRecord('srv_fake', 'executor', 'agent', 'worker', 'setup-test_prepare',
            'project', 'realization', 'scope', status='APPLY_PENDING', proposal={'proposal_id': 'old-proposal'},
            apply_client_intent_id='setup-test_apply', remote_setup={'setup_run_id': 'setup-test', 'review_revision': 'a' * 64}))
        state.preferences['configure.srv_fake.agent.setup-test'].update(stage='apply', executor_id='executor',
            realization_ref='realization', proposal={'proposal_id': 'old-proposal'})
    r.host.store.update(saved)
    revised(r.host, record)
    route = r.peer._route
    def committed(method, target, headers, body):
        if target == '/v1/connections/remote-setup:recover':
            r.peer.authenticate(headers)
            assert body == {'setup_run_id': 'setup-test'}
            return 200, dict(state='committed', handoff=deepcopy(old), final_binding=final, material=None)
        return route(method, target, headers, body)
    monkeypatch.setattr(r.peer, '_route', committed)
    calls = []
    async def original_apply(args, output, root, store, identity, server, portable):
        item = store.load().preferences['configure.srv_fake.agent.setup-test']
        setup.require_configuration(store, item, identity)
        assert item['child_requests'] == old['child_requests'] and item['stage'] == 'apply'
        assert item['configuration'] == record['local']['configuration']
        calls.append(item['child_requests']['apply'])
        return {'saved': True, 'binding': final}
    monkeypatch.setattr(configure_local, 'configure_local', original_apply)
    if physical_drift:
        r.host.executable.write_bytes(b'changed after local review')
    result = await connect.run_connect(r.args, r.host.output, r.host.root)
    now = setup.load(r.host.store, 'setup-test')
    assert 'pending_local_review' not in now and now['draft'] == record['draft']
    assert now['recovered_final_binding'] == final
    assert now['handoff_history'][-1]['outcome'] == 'prior_finish_committed'
    assert calls == ([] if physical_drift else ['setup-test_apply'])
    assert result['status'] == 'remote_finish_recovered' if physical_drift else result['saved']


async def test_late_host_checkpoint_cannot_overwrite_promoted_local_choices(continuation, monkeypatch):
    r = continuation
    actual_pipeline = configure_local.configure_local
    async def initial(*a): return {'saved': False}
    monkeypatch.setattr(configure_local, 'configure_local', initial)
    await connect.run_connect(r.args, r.host.output, r.host.root)
    record = setup.load(r.host.store, 'setup-test')
    key = 'configure.srv_fake.agent.setup-test'
    r.host.store.update(lambda state: state.preferences[key].update(stage='launch', executor_id='executor'))
    winner = []
    class Launch:
        def __init__(self, *a): pass
        async def configure_launch(self, **kw):
            pending = revised(r.host, setup.load(r.host.store, 'setup-test'))
            current = next_handoff(record['handoff'], pending['pending_local_review'], native=True)
            setup.accept_handoff(r.host.store, 'setup-test', pending, current, current,
                progress={'state': 'awaiting_host', 'handoff': current, 'final_binding': None})
            winner.append(deepcopy(r.host.store.load().preferences[key]))
            from types import SimpleNamespace
            return SimpleNamespace(configuration_digest='sha256:' + 'e' * 64)
    monkeypatch.setattr(configure_local, 'ExecutorOnboarding', Launch)
    monkeypatch.setattr(configure_local, 'configure_local', actual_pipeline)
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == 'STALE_GENERATION'
    assert r.host.store.load().preferences[key] == winner[0]
    assert winner[0]['configuration']['workspace_root'] != record['local']['configuration']['workspace_root']
