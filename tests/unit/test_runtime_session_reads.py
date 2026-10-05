"""R4 session observation uses current identity and no local runtime owner."""
from copy import deepcopy
import pytest
import httpx

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import NexusHTTPClient, MANAGEMENT_REVISION
from tests.unit.test_runtime_admission import runtime
from tests.unit.test_binding_onboarding import binding
from tests.unit.test_executor_onboarding import onboarding
from tests.unit.test_executor_registration import registration


@pytest.fixture
async def session(runtime):
    service, peer, store, args = runtime
    await service.execute(**args)
    resolution = store.load().runtime_intents[0].resolution
    view = dict(scope=resolution["scope"], connection_generation=1, lifecycle_state="READY",
        process_state="UNKNOWN", ownership="UNKNOWN", lease_state="ACTIVE",
        control_available=True, durable_release_pending=False, last_observed_at=None)
    calls = []
    async def get(key, **selection):
        calls.append(selection)
        return deepcopy(view)
    peer.get_r4_session = get
    return service, peer, store, view, calls


@pytest.mark.parametrize("change", ["none", "mapping", "root", "binding"])
async def test_session_query_is_read_only_and_independent_of_launch_mapping(session, change):
    service, peer, store, view, calls = session
    def mutate(state):
        if change == "mapping":
            state.binding_intents.clear()
        elif change == "root":
            state.realizations[0].workspace_root += "-moved"
        elif change == "binding":
            state.execution_bindings[0].state = "REVOKED"
    store.update(mutate)
    before = store.path.read_bytes()
    assert await service.inspect_session(alias="assistant", session_id="session") == view
    assert await service.session_status(alias="assistant") == {"sessions": [view]}
    assert len(calls) == 2 and len(peer.submissions) == len(peer.resolves) == 1
    assert store.path.read_bytes() == before


@pytest.mark.parametrize("fault", ["scope", "rotation", "revoked"])
async def test_changed_session_or_identity_is_not_disclosed(session, fault):
    service, peer, store, view, calls = session
    original = peer.get_r4_session
    async def get(*args, **kwargs):
        result = await original(*args, **kwargs)
        if fault == "scope":
            result["scope"]["workspace_id"] = "foreign"
        else:
            store.update(lambda state: setattr(state.identities[0], "credential_epoch", 2))
        return result
    peer.get_r4_session = get
    if fault == "revoked":
        store.update(lambda state: setattr(state.identities[0], "revoked", True))
    with pytest.raises(ConnectorError) as refused:
        await service.inspect_session(alias="assistant", session_id="session")
    assert refused.value.code == {"scope": "SCOPE_MISMATCH", "rotation": "STALE_GENERATION",
                                  "revoked": "AGENT_AUTH_REQUIRED"}[fault]
    assert not refused.value.possible_effect
    assert len(calls) == (0 if fault == "revoked" else 1)


async def test_public_cli_session_reads_never_call_legacy_daemon(session, monkeypatch):
    service, peer, store, view, calls = session
    from okto_nexus_connector.cli.main import build_parser
    from okto_nexus_connector.cli.commands.runtime import run_runtime
    from okto_nexus_connector.cli.commands import identity
    from okto_nexus_connector.cli.output import Output
    from okto_nexus_connector.services import runtime_admission
    from okto_nexus_connector.daemon import manager
    monkeypatch.setattr(runtime_admission, "RuntimeAdmission", lambda *args, **kwargs: service)
    monkeypatch.setattr(identity, "_vault", lambda *args: service.vault)
    monkeypatch.setattr(manager, "connect", lambda *args: pytest.fail("Legacy daemon called"))
    store.update(lambda state: state.binding_intents.clear())
    parser = build_parser()
    for words in (["inspect", "session"], ["inspect", "session", "--alias", "assistant"]):
        args = parser.parse_args(["runtime", *words])
        assert await run_runtime(args, Output(json_mode=True), store.path.parent) == view
    args = parser.parse_args(["runtime", "status", "--alias", "assistant"])
    assert await run_runtime(args, Output(json_mode=True), store.path.parent) == {"sessions": [view]}


@pytest.mark.parametrize("fault", [None, "scope", "extra", "boolean", "revision", "timestamp", "wire"])
async def test_session_transport_validates_closed_shape_and_revision(fault):
    scope = dict(server_id="server", executor_id="executor", binding_id="binding", agent_id="agent",
        workspace_id="workspace", workspace_binding_id="workspace-binding", session_id="session",
        session_owner_generation=1, authorization_revision=1, configuration_revision=1,
        binding_revision=1, credential_epoch=1)
    view = dict(scope=scope, connection_generation=1, lifecycle_state="READY", process_state="UNKNOWN",
        ownership="UNKNOWN", lease_state="ACTIVE", durable_release_pending=False,
        control_available=True, last_observed_at="2026-10-01T00:00:00Z")
    if fault == "scope": scope["session_id"] = "foreign"
    if fault == "extra": view["secret"] = "unexpected"
    if fault == "boolean": view["control_available"] = 1
    if fault == "revision": scope["credential_epoch"] = True
    if fault == "timestamp": view["last_observed_at"] = "yesterday"
    def handler(request):
        assert request.method == "GET"
        assert request.url.path == "/v1/runtime/sessions/session"
        assert dict(request.url.params) == {"executor_id": "executor"}
        return httpx.Response(200, json=view, headers={
            "X-Nexus-Connections-Revision": "old" if fault == "wire" else MANAGEMENT_REVISION})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with NexusHTTPClient("https://nexus.test", client=client) as http:
            if fault is None:
                assert await http.get_r4_session("test-key", session_id="session", executor_id="executor") == view
            else:
                with pytest.raises(ConnectorError):
                    await http.get_r4_session("test-key", session_id="session", executor_id="executor")


async def test_status_discards_observations_after_credential_rotation(session):
    service, peer, store, view, calls = session
    original = service.inspect_session
    async def rotated(**kwargs):
        result = await original(**kwargs)
        store.update(lambda state: setattr(state.identities[0], "credential_epoch", 2))
        return result
    service.inspect_session = rotated
    with pytest.raises(ConnectorError) as refused:
        await service.session_status(alias="assistant")
    assert refused.value.code == "STALE_GENERATION"


async def test_unknown_session_never_queries_or_creates_a_runtime(session):
    service, peer, store, view, calls = session
    with pytest.raises(ConnectorError) as refused:
        await service.inspect_session(alias="assistant", session_id="missing")
    assert refused.value.code == "NOT_FOUND" and not calls
    assert len(peer.submissions) == len(peer.resolves) == 1
