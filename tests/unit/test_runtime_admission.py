"""Canonical runtime admission recovery without a second execution owner."""
from dataclasses import asdict

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services.runtime_admission import RuntimeAdmission
from okto_nexus_connector.storage.state_store import state_from_json
from okto_nexus_connector.transport.https_client import R4IntentResolution
from tests.unit.test_binding_onboarding import binding
from tests.unit.test_executor_onboarding import onboarding
from tests.unit.test_executor_registration import registration


@pytest.fixture
async def runtime(binding):
    owner, peer, store, prepare, apply = binding
    await owner.prepare(**prepare)
    peer.approved = True
    await owner.apply(**apply)
    service = RuntimeAdmission(store, owner.vault, http_factory=owner.http_factory)
    peer.resolves, peer.submissions = [], []
    peer.lose_resolve = peer.lose_submit = peer.blocked = False
    peer.during_resolve = peer.during_submit = None
    peer.operations = {}
    async def resolve(key, **body):
        assert len(store.load().runtime_intents) == 1
        peer.resolves.append(body)
        if peer.lose_resolve:
            peer.lose_resolve = False
            raise ConnectorError("OUTCOME_UNKNOWN", "resolve", "The response was lost.")
        local = store.load().execution_bindings[0]
        scope = {k: getattr(local, k) for k in ("server_id", "executor_id", "binding_id",
                 "agent_id", "workspace_id", "workspace_binding_id", "configuration_revision",
                 "authorization_revision", "binding_revision")}
        scope["session_id"] = body.get("session_id", "session")
        resolution = R4IntentResolution(body["client_intent_id"], "intent", "operation", scope["session_id"],
            False, scope, {"action": "runtime.open" if body["intent"] == "runtime.start" else body["intent"]},
            "sha256:" + "b"*64, 1, "2099-01-01T00:00:00Z", not peer.blocked,
            ("executor_not_ready",) if peer.blocked else ())
        if peer.during_resolve: peer.during_resolve()
        return resolution
    async def submit(key, resolution):
        assert store.load().runtime_intents[0].status in {"ADMISSION_PENDING", "ADMITTED"}
        peer.submissions.append(asdict(resolution))
        operation = dict(operation_id=resolution.operation_id, client_intent_id=resolution.client_intent_id,
            intent_hash=resolution.intent_hash, scope=resolution.scope, action=resolution.semantic_intent["action"],
            admission_state="ACCEPTED", possible_effect=False, retry_safe=False, receipt_revision=0)
        peer.operations[resolution.operation_id] = operation
        if peer.during_submit: peer.during_submit()
        if peer.lose_submit:
            peer.lose_submit = False
            raise ConnectorError("OUTCOME_UNKNOWN", "admit", "The response was lost.", possible_effect=True)
        return operation
    async def get(key, operation_id):
        if operation_id not in peer.operations:
            raise ConnectorError("NOT_FOUND", "read", "The operation is not admitted.")
        return peer.operations[operation_id]
    peer.resolve_r4_intent, peer.submit_r4_operation, peer.get_r4_operation = resolve, submit, get
    return service, peer, store, dict(alias="assistant", client_intent_id="start-one",
                                     intent="runtime.start", new_session=True)


async def test_lost_resolution_and_admission_recover_one_intent(runtime):
    service, peer, store, args = runtime
    peer.lose_resolve = True
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    assert store.load().runtime_intents[0].status == "RESOLVE_PENDING"
    peer.lose_submit = True
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    assert store.load().runtime_intents[0].status == "ADMISSION_PENDING"
    # Query after process restart adopts the admitted operation; no resubmit.
    recovered = RuntimeAdmission(store, service.vault, http_factory=service.http_factory)
    view = await recovered.inspect(alias="assistant", client_intent_id="start-one")
    assert view["state"] == "ADMITTED" and len(peer.submissions) == 1
    assert await recovered.execute(**args) == view
    assert peer.resolves[0] == peer.resolves[1]
    assert peer.submissions[0] == peer.submissions[1]
    assert len(store.load().runtime_intents) == 1
    assert not store.load().bindings


async def test_blocked_resolution_never_admits_or_replaces(runtime):
    service, peer, store, args = runtime
    peer.blocked = True
    view = await service.execute(**args)
    assert view["blockers"] == ["executor_not_ready"] and view["state"] == "RESOLVED"
    peer.blocked = False
    assert await service.execute(**args) == view
    assert len(peer.resolves) == 1 and not peer.submissions
    queried = await service.inspect(alias="assistant", client_intent_id="start-one")
    assert queried["operation_found"] is False


async def test_changed_intent_content_is_refused_before_network(runtime):
    service, peer, store, args = runtime
    peer.lose_submit = True
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    with pytest.raises(ConnectorError) as refused:
        await service.execute(**(args | dict(intent="turn.submit", session_id="session", new_session=None, text="changed")))
    assert refused.value.code == "OPERATION_CONFLICT"
    assert len(peer.submissions) == len(peer.resolves) == 1


@pytest.mark.parametrize("boundary", ["resolve", "submit"])
async def test_authority_changes_cannot_be_acknowledged(runtime, boundary):
    service, peer, store, args = runtime
    def change():
        store.update(lambda state: setattr(state.identities[0], "credential_epoch", 2))
    setattr(peer, "during_" + boundary, change)
    with pytest.raises(ConnectorError) as refused:
        await service.execute(**args)
    assert refused.value.possible_effect is (boundary == "submit")
    record = store.load().runtime_intents[0]
    assert record.operation is None
    assert len(peer.submissions) == (1 if boundary == "submit" else 0)


async def test_tampered_resolution_never_admits(runtime):
    service, peer, store, args = runtime
    peer.blocked = True
    await service.execute(**args)
    def change(state):
        state.runtime_intents[0].resolution["can_submit"] = True
    store.update(change)
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    assert not peer.submissions


@pytest.mark.parametrize("intent,text,target", [
    ("turn.submit", "A bounded prompt.", {"kind": "none", "expected_turn_id": None}),
    ("turn.steer", "A correction.", {"kind": "native_turn_id", "expected_turn_id": "turn"}),
    ("turn.interrupt", "User requested interruption.", {"kind": "current_run", "expected_turn_id": None}),
    ("runtime.close", "User requested shutdown.", {"kind": "none", "expected_turn_id": None}),
])
async def test_session_actions_preserve_exact_target_and_content(runtime, intent, text, target):
    service, peer, _, args = runtime
    view = await service.execute(**(args | dict(intent=intent, session_id="session",
                                   new_session=None, text=text, target=target)))
    assert view["state"] == "ADMITTED"
    assert peer.resolves[0]["target"] == target and peer.resolves[0]["text"] == text
    assert peer.submissions[0]["semantic_intent"]["action"] == intent


async def test_cli_routes_r4_start_and_query_without_legacy_owner(runtime, monkeypatch, tmp_path):
    service, peer, store, _ = runtime
    from okto_nexus_connector.cli.main import build_parser
    from okto_nexus_connector.cli.commands.runtime import run_runtime
    from okto_nexus_connector.cli.commands import identity
    from okto_nexus_connector.cli.output import Output
    from okto_nexus_connector.services import runtime_admission
    from okto_nexus_connector.daemon import manager
    monkeypatch.setattr(runtime_admission, "RuntimeAdmission", lambda *args, **kwargs: service)
    monkeypatch.setattr(identity, "_vault", lambda *args: service.vault)
    monkeypatch.setattr(manager, "connect", lambda *args: pytest.fail("Legacy runtime owner called"))
    parser = build_parser()
    root = store.path.parent
    args = parser.parse_args(["runtime", "start", "assistant", "--new-session", "--client-intent-id", "start-one"])
    result = await run_runtime(args, Output(json_mode=True), root)
    assert result["state"] == "ADMITTED"
    args = parser.parse_args(["runtime", "operation", "--alias", "assistant", "--client-intent-id", "start-one"])
    assert await run_runtime(args, Output(json_mode=True), root) == result


def test_schema_ten_migration_adds_no_runtime_authority():
    state = state_from_json({"schema_version": 10})
    assert state.schema_version == 11 and not state.runtime_intents


async def test_cross_scope_operation_read_cannot_overwrite_intent(runtime):
    service, peer, store, args = runtime
    await service.execute(**args)
    before = store.load().runtime_intents[0].operation
    peer.operations["operation"] = dict(peer.operations["operation"], scope={"server_id": "other"})
    with pytest.raises(ConnectorError) as refused:
        await service.inspect(alias="assistant", client_intent_id="start-one")
    assert refused.value.possible_effect and refused.value.code == "SCOPE_MISMATCH"
    assert store.load().runtime_intents[0].operation == before


@pytest.mark.parametrize("change", [
    {"client_intent_id": None}, {"new_session": "false"}, {"text": ""},
])
async def test_invalid_start_does_not_reserve_or_call_network(runtime, change):
    service, peer, store, args = runtime
    with pytest.raises(ConnectorError):
        await service.execute(**(args | change))
    assert not store.load().runtime_intents and not peer.resolves and not peer.submissions


async def test_old_receipt_cannot_replace_new_receipt(runtime):
    service, peer, store, args = runtime
    await service.execute(**args)
    old = dict(peer.operations["operation"])
    peer.operations["operation"] = dict(old, receipt_revision=2, admission_state="RESOLVED_TERMINAL")
    current = await service.inspect(alias="assistant", client_intent_id="start-one")
    peer.operations["operation"] = old
    assert await service.inspect(alias="assistant", client_intent_id="start-one") == current


@pytest.mark.parametrize("change", ["root", "mapping", "binding"])
async def test_query_recovers_after_local_launch_evidence_changes(runtime, change):
    service, peer, store, args = runtime
    peer.lose_submit = True
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    def changed(state):
        if change == "root":
            state.realizations[0].workspace_root += "-moved"
        elif change == "mapping":
            state.binding_intents.clear()
        else:
            state.execution_bindings[0].state = "REVOKED"
    store.update(changed)
    view = await service.inspect(alias="assistant", client_intent_id="start-one")
    assert view["state"] == "ADMITTED" and len(peer.submissions) == 1
    with pytest.raises(ConnectorError):
        await service.execute(**args)
    assert len(peer.submissions) == 1


async def test_query_uses_current_credential_but_refuses_mid_read_rotation(runtime):
    service, peer, store, args = runtime
    await service.execute(**args)
    store.update(lambda state: setattr(state.identities[0], "credential_epoch", 2))
    assert (await service.inspect(alias="assistant", client_intent_id="start-one"))["state"] == "ADMITTED"
    original = peer.get_r4_operation
    async def rotated(*values):
        result = await original(*values)
        store.update(lambda state: setattr(state.identities[0], "credential_epoch", 3))
        return dict(result, receipt_revision=2)
    peer.get_r4_operation = rotated
    with pytest.raises(ConnectorError) as refused:
        await service.inspect(alias="assistant", client_intent_id="start-one")
    assert refused.value.code == "STALE_GENERATION"
    assert store.load().runtime_intents[0].operation["receipt_revision"] == 0


async def test_revoked_read_identity_never_contacts_server(runtime):
    service, peer, store, args = runtime
    await service.execute(**args)
    store.update(lambda state: setattr(state.identities[0], "revoked", True))
    async def unexpected(*args):
        pytest.fail("A revoked identity cannot query operations.")
    peer.get_r4_operation = unexpected
    with pytest.raises(ConnectorError) as refused:
        await service.inspect(alias="assistant", client_intent_id="start-one")
    assert refused.value.code == "AGENT_AUTH_REQUIRED"


async def test_reuse_retains_original_operation_provenance(runtime):
    from dataclasses import replace
    service, peer, store, args = runtime
    first = await service.execute(**args)
    original = service._resolution(store.load().runtime_intents[0])
    confirmations = []
    async def resolve(key, **body):
        assert body["new_session"] is False
        return replace(original, client_intent_id=body["client_intent_id"], intent_id="reuse-intent", reuse=True)
    async def confirm(key, selected):
        confirmations.append(selected)
        return peer.operations[original.operation_id]
    peer.resolve_r4_intent = resolve
    peer.submit_r4_operation = confirm
    reused = await service.execute(**(args | dict(client_intent_id="reuse", new_session=False)))
    assert reused["reused"] and reused["operation_id"] == first["operation_id"]
    assert reused["operation"]["client_intent_id"] == "start-one"
    assert len(peer.operations) == 1 and len(confirmations) == 1
    assert await service.inspect(alias="assistant", client_intent_id="reuse") == reused
    assert len(confirmations) == 1


async def test_explicit_selection_preserves_session_and_rejects_new_flag(runtime):
    service, peer, store, args = runtime
    with pytest.raises(ConnectorError):
        await service.execute(**(args | dict(session_id="session", new_session=True)))
    assert not store.load().runtime_intents
    result = await service.execute(**(args | dict(session_id="session", new_session=False)))
    assert result["state"] == "ADMITTED"
    assert peer.resolves[0]["session_id"] == "session"


@pytest.mark.asyncio
async def test_start_preserves_initial_prompt_for_server_admission(runtime):
    service, peer, store, args = runtime
    view = await service.execute(**(args | {"text": "Initial request"}))
    assert view["state"] == "ADMITTED"
    assert peer.resolves[0]["text"] == "Initial request"
    assert len(peer.resolves) == 1
