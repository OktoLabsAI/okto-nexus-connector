"""Public onboarding preserves durable intent, scope and local consent."""
from pathlib import Path

import pytest

from nexus_connector_core import InstallationCandidate, calculate_inventory_revision
from nexus_connector_core.discovery import fingerprint, installation_ref
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services import executor_onboarding as module
from okto_nexus_connector.transport.https_client import R4Realization, InventoryAccepted
from tests.unit.test_executor_registration import registration


@pytest.fixture
async def onboarding(registration, tmp_path, monkeypatch):
    store, peer, registration_service = registration
    await registration_service.register(identity_alias="selected", label="Host")
    service = module.ExecutorOnboarding(store, registration_service.vault,
        http_factory=registration_service.http_factory)
    binary = tmp_path / "codex.exe"
    binary.write_bytes(b"technical selected executable")
    selected = InstallationCandidate("codex_app_server", str(binary), fingerprint(binary),
        "explicit", "selected", installation_ref=installation_ref("codex_app_server", str(binary)))
    async def discover(configuration):
        assert configuration == {}
        return [selected]
    monkeypatch.setattr(module, "configured_candidates", discover)
    configuration = await service.configure_launch(identity_alias="selected", adapter_id="codex_app_server",
        local_consent_id="consent", profile_revision=1, provider_home=tmp_path)
    peer.inventory = []
    peer.realizations = []
    peer.during_inventory = None
    peer.during_realization = None
    peer.lose_realization = False
    async def inventory(key, *, server_id, executor_id):
        from okto_nexus_connector.services.discovery_service import executor_inventory_snapshot
        assert key == "synthetic-canonical-key"
        snapshot = executor_inventory_snapshot([selected], server_id=server_id, executor_id=executor_id,
            producer_instance_id="daemon", publication_sequence=1)
        peer.inventory.append(snapshot)
        if peer.during_inventory:
            peer.during_inventory()
        return dict(snapshot=snapshot, freshness="OFFLINE", eligible_for_new_start=False)
    async def realization(ticket, *, executor_id, request):
        peer.realizations.append(request)
        assert len(store.load().realizations) == 1
        if peer.during_realization:
            peer.during_realization()
        if peer.lose_realization:
            peer.lose_realization = False
            raise ConnectorError("OUTCOME_UNKNOWN", "realization", "The response was lost.")
        return R4Realization(server_id="srv", executor_id=executor_id, realization_ref="realization",
            local_realization_ref=request["local_realization_ref"], realization_revision=1,
            agent_id=request["agent_id"], workspace_id="workspace", workspace_binding_id="workspace-binding",
            inventory_revision=request["inventory_revision"], configuration_digest=request["configuration_digest"])
    peer.read_r4_inventory, peer.publish_r4_realization = inventory, realization
    bootstrap = await registration_service.bootstrap(server_id="srv")
    project = tmp_path / "project"
    project.mkdir()
    arguments = dict(bootstrap=bootstrap, identity_alias="selected", client_intent_id="realize", adapter_id="codex_app_server",
        candidate_ref=selected.installation_ref, inventory_revision=calculate_inventory_revision([selected]),
        configuration_digest=configuration.configuration_digest, workspace_root=project,
        workspace_id=None, workspace_label="Project")
    return service, peer, store, arguments, binary


async def test_lost_realization_response_replays_same_persisted_intent(onboarding):
    service, peer, store, args, _ = onboarding
    peer.lose_realization = True
    with pytest.raises(ConnectorError):
        await service.realize(**args)
    first = store.load().realizations[0]
    assert first.status == "LOCAL_VALIDATED"
    result = await service.realize(**args)
    assert peer.realizations[0] == peer.realizations[1]
    assert [r["publication_sequence"] for r in peer.inventory] == [1, 1]
    assert result["realization_ref"] == "realization"
    assert store.load().realizations[0].status == "PENDING_APPROVAL"
    assert not store.load().execution_bindings
    raw = store.path.read_text()
    assert "nxt4_" not in raw and "synthetic-canonical-key" not in raw
    assert str(args["workspace_root"]) not in str(result)
    assert await service.realize(**args) == result
    assert len(store.load().realizations) == 1


@pytest.mark.parametrize("change", ["revision", "candidate", "config", "scope"])
async def test_invalid_selection_has_no_network_effect(onboarding, change):
    service, peer, store, args, _ = onboarding
    before = len(peer.requests)
    if change == "revision": args["inventory_revision"] = "sha256:" + "a" * 64
    elif change == "candidate": args["candidate_ref"] = "installation:missing"
    elif change == "config": args["configuration_digest"] = "sha256:" + "b" * 64
    else:
        store.update(lambda state: setattr(state.launch_configurations[0], "agent_id", "other"))
    with pytest.raises(ConnectorError):
        await service.realize(**args)
    assert len(peer.requests) == before and not peer.inventory and not peer.realizations


@pytest.mark.parametrize("change", ["identity", "configuration", "root", "binary"])
async def test_change_during_inventory_prevents_realization_publication(onboarding, change):
    service, peer, store, args, binary = onboarding
    def mutate():
        if change == "identity":
            store.update(lambda state: setattr(state.identities[0], "credential_epoch", 99))
        elif change == "configuration":
            store.update(lambda state: setattr(state.launch_configurations[0], "local_consent_id", "changed"))
        elif change == "binary":
            binary.write_bytes(b"changed executable")
        else:
            args["workspace_root"].rename(args["workspace_root"].with_name("retained"))
            args["workspace_root"].mkdir()
    peer.during_inventory = mutate
    with pytest.raises(ConnectorError):
        await service.realize(**args)
    assert len(peer.inventory) == 1 and not peer.realizations
    assert not store.load().realizations[0].realization_ref


async def test_identity_change_after_realization_does_not_acknowledge_mapping(onboarding):
    service, peer, store, args, _ = onboarding
    peer.during_realization = lambda: store.update(lambda state: setattr(state.identities[0], "revoked", True))
    with pytest.raises(ConnectorError):
        await service.realize(**args)
    assert not store.load().realizations[0].realization_ref


async def test_configure_launch_is_idempotent_and_refuses_raw_secrets(onboarding):
    service, peer, store, _, _ = onboarding
    before = len(peer.requests)
    args = dict(identity_alias="selected", adapter_id="codex_app_server",
        local_consent_id="explicit", profile_revision=1, secret_bindings={"API_KEY": "vault:protected"})
    configured = await service.configure_launch(**args)
    assert await service.configure_launch(**args) == configured
    with pytest.raises(ConnectorError):
        await service.configure_launch(**(args | {"secret_bindings": {"API_KEY": "plaintext"}}))
    assert len(peer.requests) == before and not peer.realizations
    assert not store.load().execution_bindings
