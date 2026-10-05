"""Explicit observation never grants execution or survives changed bytes/scope."""
from dataclasses import asdict, replace
import io
from types import SimpleNamespace
import sys

import pytest
from nexus_connector_core import InstallationCandidate, calculate_inventory_revision, installation_ref

from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.commands.executor import run_executor
from okto_nexus_connector.cli.commands.discover import run_discover
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services import installation_observation as service
from okto_nexus_connector.services import discovery_configuration as configuration
from okto_nexus_connector.storage.state_store import state_from_json, state_to_json
from tests.unit.test_discovery_configuration import registered
from tests.unit.test_r4_daemon_control import control, eventually, dispose


@pytest.fixture
def host(registered, tmp_path, monkeypatch):
    import nexus_connector_core.protocol_probe as protocol
    monkeypatch.setattr(protocol, 'probe_selected_protocol', lambda *args, **kwargs: {'protocol_verified': True})
    executable = tmp_path / 'codex.exe'
    executable.write_bytes(b'synthetic binary; never executed')
    candidate = InstallationCandidate('codex_app_server', str(executable), 'sha256:' + 'a' * 64,
        'trusted_root', 'selected', architecture='x86_64',
        installation_ref=installation_ref('codex_app_server', str(executable)))
    calls = []
    inventory = [candidate]
    async def candidates(config, *, observations=(), **kwargs):
        return service.apply_observations(inventory, observations)
    async def probe(selected, *, strict):
        assert selected == candidate and strict is True
        calls.append(selected)
        return replace(selected, version='0.159.0')
    monkeypatch.setattr(configuration, 'configured_candidates', candidates)
    monkeypatch.setattr(service, 'probe_version', probe)
    return registered, candidate, calls, inventory


def arguments(candidate):
    return dict(server_id='first', adapter_id=candidate.adapter_id,
        candidate_ref=candidate.installation_ref, inventory_revision=calculate_inventory_revision([candidate]))


async def test_failed_protocol_probe_does_not_save_observation(host, monkeypatch):
    from nexus_connector_core import CoreError
    import nexus_connector_core.protocol_probe as protocol
    store, candidate, _, _ = host
    def reject(*args, **kwargs):
        raise CoreError('NATIVE_PROTOCOL_INCOMPATIBLE', 'protocol_probe')
    monkeypatch.setattr(protocol, 'probe_selected_protocol', reject)
    with pytest.raises(ConnectorError, match='NATIVE_PROTOCOL_INCOMPATIBLE'):
        await service.observe_installation(store, **arguments(candidate))
    assert not store.load().execution_executors[0].installation_observations


async def test_explicit_probe_selects_discovered_candidate_without_changing_inventory_trust(host, monkeypatch):
    store, candidate, _, inventory = host
    discovered = replace(candidate, trust='discovered')
    inventory[:] = [discovered]
    async def probe(selected, *, strict):
        assert selected == replace(discovered, trust='selected') and strict
        return replace(selected, version='0.159.0')
    monkeypatch.setattr(service, 'probe_version', probe)
    await service.observe_installation(store, **arguments(discovered))
    observations = store.load().execution_executors[0].installation_observations
    assert observations[0]['source'] == asdict(discovered)
    assert service.apply_observations(inventory, observations)[0].trust == 'discovered'


async def test_public_probe_persists_observation_and_passive_preview_reuses_it(host, tmp_path):
    store, candidate, calls, _ = host
    args = arguments(candidate)
    parser = build_parser()
    result = await run_executor(parser.parse_args(['executor', 'probe', '--server-id', args['server_id'],
        '--harness', args['adapter_id'], '--candidate-ref', args['candidate_ref'],
        '--inventory-revision', args['inventory_revision']]), Output(json_mode=True), tmp_path)
    assert calls == [candidate]
    assert result['publication_pending'] and result['runtime_authorized'] is False
    preview = await run_discover(parser.parse_args(['discover', '--server-id', 'first']),
        Output(json_mode=True, stream=io.StringIO()), tmp_path)
    assert calls == [candidate]  # Reading the retained evidence executes nothing.
    assert preview['candidates'][0]['version'] == '0.159.0'
    assert preview['availability'] == result['availability']
    assert result['availability']['executor_revision'] != args['inventory_revision']
    state = store.load()
    assert state.schema_version == 13
    assert not state.realizations and not state.execution_bindings and not state.runtime_intents
    assert not state.execution_executors[1].installation_observations


@pytest.mark.parametrize('change', ['fingerprint', 'platform', 'configuration'])
async def test_observation_cannot_be_reused_after_identity_change(host, change):
    store, candidate, _, _ = host
    await service.observe_installation(store, **arguments(candidate))
    observations = store.load().execution_executors[0].installation_observations
    if change == 'fingerprint':
        candidate = replace(candidate, fingerprint='sha256:' + 'b' * 64)
    elif change == 'configuration':
        configuration.configure_discovery(store, server_id='first')
        observations = store.load().execution_executors[0].installation_observations
        assert observations == []
    else:
        observations[0]['core_version' if change == 'core' else 'platform'] = 'different'
    assert service.apply_observations([candidate], observations)[0].version is None


async def test_core_upgrade_preserves_observation_of_identical_harness_bytes(host):
    store, candidate, _, _ = host
    await service.observe_installation(store, **arguments(candidate))
    observations = store.load().execution_executors[0].installation_observations
    observations[0]['core_version'] = 'previous-core'
    assert service.apply_observations([candidate], observations)[0].version == '0.159.0'


async def test_stale_selection_is_refused_before_probe(host):
    store, candidate, calls, _ = host
    before = store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        await service.observe_installation(store, **{**arguments(candidate), 'inventory_revision': 'sha256:' + 'f' * 64})
    assert error.value.code == 'STALE_GENERATION'
    assert not calls and store.path.read_bytes() == before


@pytest.mark.parametrize('change', ['registration', 'bytes', 'failed'])
async def test_change_or_failure_during_probe_does_not_commit_evidence(host, monkeypatch, change):
    store, candidate, _, inventory = host
    async def probe(selected, **kwargs):
        if change == 'registration':
            store.update(lambda s: setattr(s.execution_executors[0], 'executor_id', 'replaced'))
        elif change == 'bytes':
            inventory[:] = [replace(candidate, fingerprint='sha256:' + 'c' * 64)]
        else:
            raise ConnectorError('PROCESS_CONTAINMENT_UNAVAILABLE', 'probe', 'Refused')
        return replace(selected, version='0.159.0')
    monkeypatch.setattr(service, 'probe_version', probe)
    with pytest.raises(ConnectorError):
        await service.observe_installation(store, **arguments(candidate))
    assert store.load().execution_executors[0].installation_observations == []


def test_schema_twelve_migrates_without_observation_authority(registered):
    payload = state_to_json(registered.load())
    payload['schema_version'] = 12
    for entry in payload['execution_executors']:
        entry.pop('installation_observations')
    migrated = state_from_json(payload)
    assert migrated.schema_version == 13
    assert all(not record.installation_observations for record in migrated.execution_executors)


async def test_default_daemon_publishes_retained_observation_without_probing(control, tmp_path, monkeypatch):
    owner, peer, store, host, _ = control
    candidate = InstallationCandidate('codex_app_server', str(tmp_path / 'codex.exe'),
        'sha256:' + 'd' * 64, 'trusted_root', 'selected', architecture='x86_64')
    entry = dict(core_version=service.CORE_VERSION, platform=sys.platform,
                 source=asdict(candidate), version='0.159.0')
    def registered(state):
        record = state.execution_executors[0]
        record.state, record.executor_id = 'REGISTERED', 'executor'
        record.installation_observations = [entry]
    store.update(registered)
    monkeypatch.setattr(configuration, 'discover_installations',
        lambda **kwargs: SimpleNamespace(candidates=(candidate,)))
    monkeypatch.setattr(service, 'probe_version', lambda *a, **k: pytest.fail('Passive publication must not probe'))
    owner.discover = None
    owner.start()
    try:
        await eventually(lambda: owner.status()['control_ready'], diagnostics=owner.status)
        evidence = peer.publications[-1]['evidence'][0]
        assert evidence['version'] == '0.159.0'
        assert str(tmp_path) not in str(peer.publications[-1])
        assert evidence['state'] == 'READY_FOR_RUNTIME'  # Eligible for a live handshake; not a recorded build grant.
        assert not store.load().execution_bindings
    finally:
        await dispose(owner, host)
