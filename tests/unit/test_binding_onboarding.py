"""Reviewable binding intent persistence, exact application and replay."""
from dataclasses import replace

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.binding_onboarding import BindingOnboarding
from okto_nexus_connector.storage.state_store import state_from_json
from okto_nexus_connector.transport.https_client import R4BindingProposal, R4BindingView
from tests.unit.test_executor_onboarding import onboarding
from tests.unit.test_executor_registration import registration


@pytest.fixture
async def binding(onboarding):
    owner, peer, store, arguments, _ = onboarding
    realized = await owner.realize(**arguments)
    service = BindingOnboarding(store, owner.vault, http_factory=owner.http_factory)
    peer.prepares, peer.applies = [], []
    peer.lose_prepare = peer.lose_apply = peer.approved = False
    peer.during_prepare = peer.during_apply = None
    async def prepare(key, **body):
        assert store.load().binding_intents[0].client_intent_id == body["client_intent_id"]
        peer.prepares.append(body)
        if peer.during_prepare: peer.during_prepare()
        if peer.lose_prepare:
            peer.lose_prepare = False
            raise ConnectorError("OUTCOME_UNKNOWN", "http", "The response was lost.", possible_effect=True)
        return R4BindingProposal(proposal_id="proposal", proposal_revision=1, expires_at="2099-01-01T00:00:00Z",
            server_id="srv", executor_id="executor", agent_id="agent", binding_id="binding", endpoint_id="endpoint",
            profile_id="profile", workspace_id="workspace", workspace_binding_id="workspace-binding",
            adapter_id=body["adapter_id"], candidate_ref=body["candidate_ref"], inventory_revision=body["inventory_revision"],
            realization_ref=body["realization_ref"], realization_revision=1, authorization_revision=3,
            configuration_revision=4, approved_diff_hash="sha256:" + "a"*64, summary="Review this binding.",
            requires_operator=True, fields_changed=("binding",), required_approvals=("apr_proof",), can_apply=False)
    async def apply(key, *, client_intent_id, proposal, operator_proof_ref=None):
        record = store.load().binding_intents[0]
        assert record.apply_client_intent_id == client_intent_id and record.operator_proof_ref == operator_proof_ref
        peer.applies.append((client_intent_id, proposal.proposal_id, operator_proof_ref))
        if not peer.approved:
            raise ConnectorError("PERMISSION_DENIED", "binding", "Operator approval is required.")
        if peer.during_apply: peer.during_apply()
        if peer.lose_apply:
            peer.lose_apply = False
            raise ConnectorError("OUTCOME_UNKNOWN", "http", "The response was lost.", possible_effect=True)
        return R4BindingView(binding_id="binding", server_id="srv", executor_id="executor", agent_id="agent",
            endpoint_id="endpoint", workspace_id="workspace", workspace_binding_id="workspace-binding",
            adapter_id=proposal.adapter_id, candidate_ref=proposal.candidate_ref, inventory_revision=proposal.inventory_revision,
            realization_ref=proposal.realization_ref, state="APPROVED", realization_revision=1, binding_revision=1,
            authorization_revision=3, configuration_revision=4)
    peer.prepare_r4_binding, peer.apply_r4_binding = prepare, apply
    prepare_args = dict(identity_alias="selected", realization_ref=realized["realization_ref"],
                        alias="assistant", client_intent_id="prepare")
    apply_args = dict(identity_alias="selected", prepare_intent_id="prepare", client_intent_id="apply",
                      approved_diff_hash="sha256:" + "a"*64, operator_proof_ref="apr_proof")
    return service, peer, store, prepare_args, apply_args


async def test_prepare_and_apply_lost_responses_keep_intents_and_one_binding(binding):
    service, peer, store, prepare, apply = binding
    peer.lose_prepare = True
    with pytest.raises(ConnectorError):
        await service.prepare(**prepare)
    assert store.load().binding_intents[0].status == "PREPARE_PENDING"
    proposal = await service.prepare(**prepare)
    assert peer.prepares[0] == peer.prepares[1] and proposal["state"] == "PREPARED"
    assert await service.prepare(**prepare) == proposal
    assert len(peer.prepares) == 2
    with pytest.raises(ConnectorError):
        await service.apply(**apply)
    assert store.load().binding_intents[0].status == "APPLY_PENDING"
    assert not store.load().execution_bindings
    peer.approved, peer.lose_apply = True, True
    with pytest.raises(ConnectorError):
        await service.apply(**apply)
    assert not store.load().execution_bindings
    result = await service.apply(**apply)
    assert result["state"] == "APPLIED" and result["binding"]["binding_id"] == "binding"
    assert await service.apply(**apply) == result
    assert len(set(peer.applies)) == 1
    state = store.load()
    assert len(state.execution_bindings) == 1 and state.realizations[0].status == "BOUND"
    assert not state.bindings
    assert "synthetic-canonical-key" not in store.path.read_text()


@pytest.mark.parametrize("change", ["alias", "realization"])
async def test_prepare_intent_cannot_be_reused_with_different_content(binding, change):
    service, peer, store, prepare, _ = binding
    await service.prepare(**prepare)
    before = len(peer.prepares)
    key = "alias" if change == "alias" else "realization_ref"
    with pytest.raises(ConnectorError):
        await service.prepare(**(prepare | {key: "other"}))
    assert len(peer.prepares) == before


@pytest.mark.parametrize("change", ["diff", "proof"])
async def test_unreviewed_diff_or_missing_proof_never_reaches_server(binding, change):
    service, peer, store, prepare, apply = binding
    await service.prepare(**prepare)
    apply["approved_diff_hash" if change == "diff" else "operator_proof_ref"] = "wrong"
    with pytest.raises(ConnectorError):
        await service.apply(**apply)
    assert not peer.applies and not store.load().binding_intents[0].apply_client_intent_id


async def test_unknown_apply_outcome_cannot_be_retried_with_new_id(binding):
    service, peer, store, prepare, apply = binding
    await service.prepare(**prepare)
    peer.approved, peer.lose_apply = True, True
    with pytest.raises(ConnectorError):
        await service.apply(**apply)
    with pytest.raises(ConnectorError) as error:
        await service.apply(**(apply | {"client_intent_id": "new-apply"}))
    assert error.value.code == "OPERATION_CONFLICT" and len(peer.applies) == 1


@pytest.mark.parametrize("phase", ["prepare", "apply"])
async def test_identity_change_after_response_cannot_acknowledge(binding, phase):
    service, peer, store, prepare, apply = binding
    def change():
        store.update(lambda state: setattr(state.identities[0], "credential_epoch", 9))
    if phase == "prepare":
        peer.during_prepare = change
        with pytest.raises(ConnectorError):
            await service.prepare(**prepare)
        assert store.load().binding_intents[0].proposal is None
    else:
        await service.prepare(**prepare)
        peer.approved, peer.during_apply = True, change
        with pytest.raises(ConnectorError):
            await service.apply(**apply)
        assert not store.load().execution_bindings
        assert store.load().realizations[0].status == "PENDING_APPROVAL"


async def test_vault_identity_mismatch_is_refused_before_prepare(binding):
    service, peer, store, prepare, _ = binding
    peer.me_info = replace(peer.me_info, agent_id="other")
    with pytest.raises(ConnectorError) as error:
        await service.prepare(**prepare)
    assert error.value.code == "AGENT_ID_MISMATCH" and not peer.prepares


async def test_proposal_mutation_is_not_application_authority(binding):
    service, peer, store, prepare, apply = binding
    await service.prepare(**prepare)
    store.update(lambda state: state.binding_intents[0].proposal.update(approved_diff_hash="sha256:" + "b"*64))
    with pytest.raises(ConnectorError) as error:
        await service.apply(**apply)
    assert error.value.code == "PROFILE_DRIFT" and not peer.applies


def test_schema_nine_has_no_implicit_binding_intents():
    state = state_from_json({"schema_version": 9})
    assert state.schema_version == 10 and not state.binding_intents and not state.execution_bindings


async def test_local_acknowledgment_failure_keeps_one_recoverable_apply(binding, monkeypatch):
    from okto_nexus_connector.services import binding_onboarding as module
    service, peer, store, prepare, apply = binding
    await service.prepare(**prepare)
    peer.approved = True
    original = module.acknowledge_execution_binding_state
    def fail_after_binding(state, *, binding):
        original(state, binding=binding)
        raise OSError("Technical local commit interruption")
    monkeypatch.setattr(module, "acknowledge_execution_binding_state", fail_after_binding)
    with pytest.raises(OSError):
        await service.apply(**apply)
    state = store.load()
    assert not state.execution_bindings and state.realizations[0].status == "PENDING_APPROVAL"
    assert state.binding_intents[0].status == "APPLY_PENDING"
    monkeypatch.setattr(module, "acknowledge_execution_binding_state", original)
    result = await service.apply(**apply)
    assert result["state"] == "APPLIED" and len(set(peer.applies)) == 1
    assert len(store.load().execution_bindings) == 1
