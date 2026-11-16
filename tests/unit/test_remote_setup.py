"""Remote setup uses real state/vault/HTTP and explicit native/provider doubles."""

from copy import deepcopy
from dataclasses import asdict, replace
import io
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from nexus_connector_core import InstallationCandidate, calculate_inventory_revision
from nexus_connector_core.connection_configuration import (
    materialize_connection_configuration,
)
from nexus_connector_core.discovery import fingerprint
from nexus_connector_core.installation import effective_installation_ref

from okto_nexus_connector.cli.commands import connect, connect_setup, configure_local
from okto_nexus_connector.cli.commands.configure import template
from okto_nexus_connector.cli.main import build_parser
from okto_nexus_connector.cli.output import Output
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.import_flow import import_identity
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.platform import paths
from okto_nexus_connector.services import remote_setup as setup
from okto_nexus_connector.storage.state_store import (
    StateStore,
    ExecutionExecutorRecord,
    BindingIntentRecord,
    state_from_json,
    state_to_json,
)
from okto_nexus_connector.transport.https_client import NexusHTTPClient
from tests.fakes.http_peer import FakeAgent, FakeNexusHTTPPeer
from tests.unit import test_binding_onboarding as binding_fixtures

binding = binding_fixtures.binding
onboarding = binding_fixtures.onboarding
registration = binding_fixtures.registration


KEY = "nxs_fixture_remote_key"


@pytest.fixture
def host(tmp_path, monkeypatch):
    project = tmp_path / "project"
    project.mkdir()
    executable = tmp_path / "synthetic-codex.exe"
    executable.write_bytes(b"synthetic fixture, never executed")
    candidate = InstallationCandidate(
        "codex_app_server", str(executable), fingerprint(executable), "path", "selected"
    )
    store = StateStore(paths.state_file(tmp_path))
    configuration = materialize_connection_configuration(
        template() | {"adapter_id": candidate.adapter_id, "alias": "project"},
        execution_location="remote",
        workspace_root=str(project),
        workspace_label="Project",
        provider_home=None,
    )
    parameters = dict(
        setup_run_id="setup-test",
        server="https://nexus.example",
        configuration=configuration,
        candidate=candidate,
        candidate_ref=effective_installation_ref(candidate),
        inventory_revision=calculate_inventory_revision([candidate]),
    )
    monkeypatch.setattr(
        "asyncio.create_subprocess_exec",
        lambda *a, **k: pytest.fail("unexpected native process"),
    )
    monkeypatch.setattr(
        configure_local.manager,
        "start",
        lambda *a, **k: pytest.fail("unexpected daemon"),
    )
    return NS(
        root=tmp_path,
        project=project,
        executable=executable,
        candidate=candidate,
        store=store,
        parameters=parameters,
        output=Output(json_mode=False, stream=io.StringIO()),
    )


def draft(host):
    return setup.prepare(host.store, **host.parameters)


def handoff(record, **changes):
    receipt = dict(
        setup_run_id=record["draft"]["setup_run_id"],
        review_revision="a" * 64,
        server_id="srv_fake",
        agent_id="agent",
        workspace_id="canonical-workspace",
        credential_epoch=1,
    )
    receipt.update(changes)
    return dict(
        format="okto.remote-setup-handoff",
        version=1,
        bootstrap=receipt,
        review_revision=receipt["review_revision"],
        child_requests=deepcopy(record["child_requests"]),
        draft_revision=record["draft_revision"],
    )


def test_draft_is_durable_bounded_and_contains_no_local_paths_or_authority(host):
    record = draft(host)
    public = record["draft"]
    assert setup.load(StateStore(host.store.path), "setup-test") == record
    assert public["installation"]["inventory_revision"] is None
    assert public["local_consent"] == dict(
        local_consent_id="setup-test_consent",
        local_root_proof_digest=None,
        configuration_digest=None,
    )
    assert host.store.load().connector_id == public["connector_id"]
    raw = setup.document_bytes(public)
    assert (
        str(host.root).encode() not in raw
        and b"provider_home" not in raw
        and KEY.encode() not in raw
    )
    assert (
        record["local"]["passive_inventory_revision"]
        == host.parameters["inventory_revision"]
    )
    assert host.store.load().identities == host.store.load().execution_executors == []
    destination = host.root / "draft.json"
    setup.export_draft(destination, public)
    before = destination.read_bytes()
    setup.export_draft(destination, draft(host)["draft"])
    assert destination.read_bytes() == before


@pytest.mark.parametrize(
    "field",
    ["server", "agent_hint", "workspace_hint", "inventory_revision", "configuration"],
)
def test_same_root_cannot_change_material_choices(host, field):
    draft(host)
    before = host.store.path.read_bytes()
    values = deepcopy(host.parameters)
    values[field] = {
        "server": "https://other.example",
        "agent_hint": "other-agent",
        "workspace_hint": "other-workspace",
        "inventory_revision": "sha256:" + "f" * 64,
        "configuration": host.parameters["configuration"] | {"alias": "other"},
    }[field]
    with pytest.raises(ConnectorError) as error:
        setup.prepare(host.store, **values)
    assert error.value.code == "OPERATION_CONFLICT"
    assert host.store.path.read_bytes() == before


def test_capacity_retains_existing_roots_and_never_prunes_recovery(host, monkeypatch):
    monkeypatch.setattr(setup, "MAX_DRAFTS", 2)
    first = draft(host)
    setup.prepare(host.store, **(host.parameters | {"setup_run_id": "second"}))
    with pytest.raises(ConnectorError) as error:
        setup.prepare(host.store, **(host.parameters | {"setup_run_id": "third"}))
    assert error.value.code == "CAPACITY_EXCEEDED"
    assert draft(host) == first


def test_concurrent_checkpoint_cas_preserves_winner(host):
    initial = draft(host)
    current = setup.checkpoint(
        host.store, "setup-test", initial, stage="awaiting_material"
    )
    with pytest.raises(ConnectorError) as error:
        setup.checkpoint(host.store, "setup-test", initial, stage="configured")
    assert error.value.code == "STALE_GENERATION"
    assert setup.load(host.store, "setup-test") == current


@pytest.mark.parametrize(
    "mutation", ["binary", "directory", "configuration", "provider"]
)
def test_private_local_selection_is_revalidated(host, mutation):
    record = draft(host)
    if mutation == "binary":
        host.executable.write_bytes(b"changed")
    elif mutation == "directory":
        host.project.rename(host.root / "original-project")
        host.project.mkdir()
    elif mutation == "configuration":
        record["local"]["configuration"]["alias"] = "changed"
    else:
        record["local"]["configuration"]["provider_home"] = str(host.project)
    with pytest.raises(ConnectorError) as error:
        setup.validate_local(record)
    assert error.value.code == "PROFILE_DRIFT"


@pytest.mark.parametrize(
    "mutation",
    [
        "secret",
        "field",
        "epoch-bool",
        "epoch-zero",
        "root",
        "child",
        "revision",
        "version-bool",
    ],
)
def test_handoff_is_closed_and_bound_to_exact_draft(host, mutation):
    record = draft(host)
    value = handoff(record)
    if mutation == "secret":
        value["material"] = {"api_key": KEY}
    elif mutation == "field":
        value["bootstrap"]["extra"] = "field"
    elif mutation == "epoch-bool":
        value["bootstrap"]["credential_epoch"] = True
    elif mutation == "epoch-zero":
        value["bootstrap"]["credential_epoch"] = 0
    elif mutation == "root":
        value["bootstrap"]["setup_run_id"] = "other"
    elif mutation == "child":
        value["child_requests"]["prepare"] = "other_prepare"
    elif mutation == "revision":
        value["draft_revision"] = "f" * 64
    else:
        value["version"] = True
    with pytest.raises(ConnectorError):
        setup.validate_handoff(value, record)
    assert setup.load(host.store, "setup-test")["handoff"] is None


@pytest.mark.parametrize(
    "raw",
    [b'{"a":1,"a":2}', b'{"a":NaN}', b"[" * 100, b"x" * 65537],
    ids=["duplicate", "nonfinite", "deep", "oversized"],
)
def test_document_boundary_rejects_duplicate_nonfinite_deep_or_large(host, raw):
    path = host.root / "invalid.json"
    path.write_bytes(raw)
    with pytest.raises(ConnectorError):
        setup.read_document(path)


def test_export_failure_preserves_checkpoint_and_existing_file(host):
    record = draft(host)
    target = host.root / "existing.json"
    target.write_text('{"other":true}', encoding="utf-8")
    with pytest.raises(ConnectorError):
        setup.export_draft(target, record["draft"])
    assert target.read_text() == '{"other":true}'
    assert setup.load(host.store, "setup-test") == record


@pytest.fixture
async def peer():
    server = FakeNexusHTTPPeer()
    server.add_agent(FakeAgent(agent_id="agent", key=KEY))
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.mark.parametrize(
    "field,value",
    [
        ("server_id", "other"),
        ("agent_id", "other"),
        ("credential_epoch", 2),
        ("credential_epoch", True),
    ],
)
async def test_real_http_identity_scope_checked_before_vault_or_state(
    host, peer, field, value
):
    vault = RestrictedFileVault(host.root / "vault", approved=True)
    expected = dict(server_id="srv_fake", agent_id="agent", credential_epoch=1) | {
        field: value
    }
    async with NexusHTTPClient(peer.url) as http:
        with pytest.raises(ConnectorError) as error:
            await import_identity(
                http,
                vault,
                host.store,
                key=KEY,
                alias="worker",
                agent_hint="agent",
                expected_identity=expected,
            )
    assert error.value.code == "AGENT_ID_MISMATCH"
    assert host.store.load().identities == [] and not host.store.load().connector_id
    assert vault.list_namespaces() == []
    assert all(
        request[:2] == ("GET", "/v1/connections/me") for request in peer.requests
    )


@pytest.mark.parametrize("after_write", [False, True])
async def test_lost_vault_ack_recovers_same_identity_without_remote_effects(
    host, peer, monkeypatch, after_write
):
    (host.root / "vault").mkdir()
    vault = RestrictedFileVault(host.root / "vault", approved=True)
    original = vault.store

    def fail(namespace, key):
        if after_write:
            original(namespace, key)
        raise OSError("synthetic vault acknowledgement lost")

    monkeypatch.setattr(vault, "store", fail)
    expected = dict(server_id="srv_fake", agent_id="agent", credential_epoch=1)
    async with NexusHTTPClient(peer.url) as http:
        with pytest.raises(OSError):
            await import_identity(
                http,
                vault,
                host.store,
                key=KEY,
                alias="worker",
                agent_hint="agent",
                expected_identity=expected,
            )
        assert host.store.load().identities == []
        monkeypatch.setattr(vault, "store", original)
        imported = await import_identity(
            http,
            vault,
            host.store,
            key=KEY,
            alias="worker",
            agent_hint="agent",
            expected_identity=expected,
        )
        repeated = await import_identity(
            http,
            vault,
            host.store,
            key=KEY,
            alias="worker",
            agent_hint="agent",
            expected_identity=expected,
        )
    assert (
        imported.identity.namespace == repeated.identity.namespace == "srv_fake/agent"
    )
    assert (
        len(host.store.load().identities) == 1
        and vault.resolve(imported.identity.secret_handle) == KEY
    )
    assert len(peer.agents) == 1 and all(
        request[:2] == ("GET", "/v1/connections/me") for request in peer.requests
    )


@pytest.mark.parametrize(
    "implicit", [False, True], ids=["explicit-file", "guided-first-use"]
)
async def test_cli_draft_without_key_has_no_vault_http_probe_or_daemon(
    host, monkeypatch, implicit
):
    async def selected(args, output):
        args.discovery_inventory_revision = host.parameters["inventory_revision"]
        return host.candidate.adapter_id, host.candidate, None

    monkeypatch.setattr(connect, "_choose_harness", selected)
    monkeypatch.setattr(connect_setup, "discover_provider_home", lambda _: None)
    monkeypatch.setattr(connect_setup, "choice", lambda *a, **k: True)
    monkeypatch.setattr(connect, "select", lambda *a, **k: "draft")
    monkeypatch.setattr(
        connect, "open_vault", lambda *a, **k: pytest.fail("vault access in draft")
    )
    monkeypatch.setattr(
        connect, "_read_key", lambda *a, **k: pytest.fail("key requested in draft")
    )
    target = host.root / "export.json"
    args = build_parser().parse_args(
        [
            "connect",
            "--server",
            "https://nexus.example",
            "--project",
            str(host.project),
            "--request-id",
            "setup-test",
            "--no-start-at-login",
            *([] if implicit else ["--setup-draft", str(target)]),
        ]
    )
    result = await connect.run_connect(args, host.output, host.root)
    assert (
        result["status"] == "awaiting_operator_bootstrap"
        and not result["runtime_started"]
    )
    assert Path(result["draft_file"]).is_file()

    async def unexpected(*a, **k):
        pytest.fail("rediscovery on same root")

    monkeypatch.setattr(connect, "_choose_harness", unexpected)
    assert await connect.run_connect(args, host.output, host.root) == result
    assert host.store.load().identities == []


@pytest.fixture
async def continuation(host, peer, monkeypatch):
    original_me = peer._me

    def supported(headers):
        status, body = original_me(headers)
        return status, body | {
            "capabilities": {
                "remote_setup": {
                    "revision": 1,
                    "draft_version": 1,
                    "handoff_version": 1,
                }
            }
        }

    monkeypatch.setattr(peer, "_me", supported)
    host.parameters["server"] = peer.url
    record = draft(host)
    document = handoff(record)
    original_route = peer._route

    def route(method, target, headers, body):
        if target == "/v1/connections/remote-setup:recover":
            peer.authenticate(headers)
            assert method == "POST" and body == {"setup_run_id": "setup-test"}
            return 200, {
                "handoff": deepcopy(document),
                "material": None,
                "state": "awaiting_host",
                "final_binding": None,
            }
        return original_route(method, target, headers, body)

    monkeypatch.setattr(peer, "_route", route)
    receipt_path = host.root / "handoff.json"
    receipt_path.write_bytes(setup.document_bytes(document))
    host.store.update(
        lambda state: state.preferences.update({"vault.fallback_file.approved": True})
    )
    (host.root / "vault").mkdir()
    vault = RestrictedFileVault(host.root / "vault", approved=True)
    monkeypatch.setattr(
        "okto_nexus_connector.cli.commands.identity._vault", lambda *a: vault
    )
    monkeypatch.setattr(configure_local, "_vault", lambda *a: vault)
    monkeypatch.setattr(connect, "_read_key", lambda *a: KEY)
    monkeypatch.setattr(connect, "vault_backend_names", lambda: ("synthetic",))
    args = build_parser().parse_args(
        ["connect", "--setup-receipt", str(receipt_path), "--alias", "worker"]
    )
    return NS(
        host=host,
        peer=peer,
        record=record,
        handoff=document,
        args=args,
        vault=vault,
        path=receipt_path,
    )


@pytest.mark.parametrize(
    "current_revision",
    ["a" * 64, "b" * 64],
    ids=["original-review", "new-review-same-bootstrap"],
)
async def test_continuation_pins_canonical_scope_and_child_ids_before_pipeline(
    continuation, monkeypatch, current_revision
):
    r = continuation
    r.handoff["review_revision"] = current_revision
    r.path.write_bytes(setup.document_bytes(r.handoff))
    calls = []

    async def pipeline(args, output, root, store, identity, server, portable):
        saved = store.load().preferences["configure.srv_fake.agent.setup-test"]
        setup.require_configuration(store, saved, identity)
        calls.append(deepcopy(saved))
        assert (
            args.workspace_id == "canonical-workspace"
            and args.request_id == "setup-test"
        )
        assert saved["remote_setup"] == dict(
            setup_run_id="setup-test", review_revision=current_revision
        )
        assert (
            setup.load(store, "setup-test")["handoff"]["bootstrap"]["review_revision"]
            == "a" * 64
        )
        assert saved["child_requests"] == r.handoff["child_requests"]
        assert server == r.peer.url and "provider_home" not in portable
        return dict(saved=False, status="awaiting_operator_approval")

    monkeypatch.setattr(configure_local, "configure_local", pipeline)
    assert (await connect.run_connect(r.args, r.host.output, r.host.root))[
        "status"
    ] == "awaiting_operator_approval"
    monkeypatch.setattr(
        connect,
        "_read_key",
        lambda *a: pytest.fail("key reimported instead of vault reuse"),
    )
    await connect.run_connect(r.args, r.host.output, r.host.root)
    assert calls[0] == calls[1]
    assert len(r.host.store.load().identities) == 1
    assert KEY not in r.host.store.path.read_text()
    assert all(
        item[:2]
        in [
            ("GET", "/v1/connections/me"),
            ("POST", "/v1/connections/remote-setup:recover"),
        ]
        for item in r.peer.requests
    )


@pytest.mark.parametrize(
    "field,value", [("server_id", "other"), ("credential_epoch", 9)]
)
async def test_wrong_handoff_key_never_enters_pipeline(
    continuation, monkeypatch, field, value
):
    r = continuation
    r.handoff["bootstrap"][field] = value
    r.path.write_bytes(setup.document_bytes(r.handoff))

    async def unexpected(*a):
        pytest.fail("pipeline entered before identity proof")

    monkeypatch.setattr(configure_local, "configure_local", unexpected)
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == "AGENT_ID_MISMATCH"
    assert r.host.store.load().identities == [] and r.vault.list_namespaces() == []


def test_schema_16_upgrade_preserves_checkpoint_and_existing_intent_fields():
    state = state_from_json(
        {
            "schema_version": 16,
            "connector_id": "connector",
            "preferences": {"old": {"stage": "done"}},
        }
    )
    assert state.schema_version == 17
    assert state_to_json(state)["preferences"] == {"old": {"stage": "done"}}


async def test_pipeline_resumes_without_repeating_committed_children(
    continuation, monkeypatch
):
    r = continuation
    calls, approved = [], [False]

    async def candidates(*a, **k):
        return [replace(r.host.candidate, version="fixture-v1")]

    monkeypatch.setattr(configure_local, "configured_candidates", candidates)

    class Registration:
        def __init__(self, *a):
            pass

        async def register(self, **kw):
            calls.append(("register", kw))
            state = r.host.store.load()
            row = ExecutionExecutorRecord(
                "srv_fake",
                state.connector_id,
                "agent",
                kw["client_intent_id"],
                kw["label"],
                executor_id="executor",
                state="REGISTERED",
            )
            r.host.store.update(lambda s: s.execution_executors.append(row))
            return row

    monkeypatch.setattr(configure_local, "ExecutorRegistrationService", Registration)

    async def probe(*a, **kw):
        calls.append(("probe", kw))

    monkeypatch.setattr(configure_local, "observe_installation", probe)

    class Launch:
        def __init__(self, *a):
            pass

        async def configure_launch(self, **kw):
            calls.append(("launch", kw))
            return NS(configuration_digest="sha256:" + "b" * 64)

    monkeypatch.setattr(configure_local, "ExecutorOnboarding", Launch)
    monkeypatch.setattr(
        configure_local.manager, "start", lambda *a: calls.append(("daemon-double", {}))
    )

    async def ipc(root, method, body=None):
        calls.append((method, body or {}))
        return {"realization_ref": "realization"}

    monkeypatch.setattr(configure_local, "ipc", ipc)

    class InventoryHTTP:
        def __init__(self, *a):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

        async def read_r4_inventory(self, *a, **k):
            return dict(
                freshness="FRESH",
                snapshot=dict(
                    inventory_revision=calculate_inventory_revision(await candidates())
                ),
            )

    monkeypatch.setattr(configure_local, "NexusHTTPClient", InventoryHTTP)

    class Binding:
        def __init__(self, *a):
            pass

        async def prepare(self, **kw):
            calls.append(("prepare", kw))
            return {
                "proposal": {
                    "required_approvals": ["apr_synthetic"],
                    "approved_diff_hash": "diff",
                }
            }

        async def apply(self, **kw):
            calls.append(("apply", kw))
            if not approved[0]:
                raise ConnectorError("APPROVAL_REQUIRED", "apply")
            return {
                "binding": {
                    "binding_id": "binding",
                    "workspace_id": "canonical-workspace",
                }
            }

    monkeypatch.setattr(configure_local, "BindingOnboarding", Binding)
    first = await connect.run_connect(r.args, r.host.output, r.host.root)
    assert first["status"] == "awaiting_operator_approval"
    approved[0] = True
    assert (await connect.run_connect(r.args, r.host.output, r.host.root))["saved"]
    for phase in ("register", "prepare"):
        entries = [kw for name, kw in calls if name == phase]
        assert (
            len(entries) == 1
            and entries[0]["client_intent_id"] == r.handoff["child_requests"][phase]
        )
    realization = [kw for name, kw in calls if name == "executor.realize"]
    assert (
        len(realization) == 1
        and realization[0]["workspace_id"] == "canonical-workspace"
    )
    assert realization[0]["client_intent_id"] == "setup-test_realize"
    assert next(kw for name, kw in calls if name == "prepare")["remote_setup"] == {
        "setup_run_id": "setup-test",
        "review_revision": "a" * 64,
    }
    assert sum(name == "probe" for name, kw in calls) == 1
    assert setup.load(r.host.store, "setup-test")["stage"] == "configured"


@pytest.mark.parametrize("durable", [False, True])
def test_uncertain_probe_reuses_durable_evidence_or_refuses_never_executes(
    host, durable
):
    record = draft(host)
    row = ExecutionExecutorRecord(
        "srv_fake",
        record["draft"]["connector_id"],
        "agent",
        "setup-test_register",
        "Host",
    )
    if durable:
        row.installation_observations = [
            dict(source=asdict(host.candidate), version="fixture-v1")
        ]
    host.store.update(lambda state: state.execution_executors.append(row))
    saved = {"remote_setup": {"setup_run_id": "setup-test"}}
    if durable:
        setup.recover_probe(host.store, saved, "srv_fake")
    else:
        with pytest.raises(ConnectorError) as error:
            setup.recover_probe(host.store, saved, "srv_fake")
        assert error.value.code == "REMOTE_SETUP_EFFECT_UNCONFIRMED"


async def test_binding_root_metadata_survives_lost_reply_and_cannot_change_on_retry(
    binding,
):
    service, peer, store, prepare_args, _ = binding
    prepare_args.update(
        client_intent_id="root_prepare",
        remote_setup={"setup_run_id": "root", "review_revision": "a" * 64},
    )
    peer.lose_prepare = True
    with pytest.raises(ConnectorError):
        await service.prepare(**prepare_args)
    assert store.load().binding_intents[0].remote_setup == prepare_args["remote_setup"]
    await service.prepare(**prepare_args)
    assert peer.prepares[0] == peer.prepares[1]
    with pytest.raises(ConnectorError) as error:
        await service.prepare(
            **(
                prepare_args
                | {
                    "remote_setup": {
                        "setup_run_id": "root",
                        "review_revision": "b" * 64,
                    }
                }
            )
        )
    assert error.value.code == "OPERATION_CONFLICT"
    assert len(peer.prepares) == 2


async def test_orphan_vault_with_different_key_is_not_replaced(host, peer):
    (host.root / "vault").mkdir()
    vault = RestrictedFileVault(host.root / "vault", approved=True)
    handle = vault.store("srv_fake/agent", "other-protected-material")
    async with NexusHTTPClient(peer.url) as http:
        with pytest.raises(ConnectorError) as error:
            await import_identity(
                http,
                vault,
                host.store,
                key=KEY,
                alias="worker",
                agent_hint="agent",
                expected_identity=dict(
                    server_id="srv_fake", agent_id="agent", credential_epoch=1
                ),
            )
    assert error.value.code == "OPERATION_CONFLICT"
    assert (
        vault.resolve(handle) == "other-protected-material"
        and host.store.load().identities == []
    )


@pytest.mark.parametrize(
    "capability",
    [
        None,
        {"revision": 1},
        {"revision": True, "draft_version": 1, "handoff_version": 1},
        {"revision": 2, "draft_version": 1, "handoff_version": 1},
    ],
    ids=["absent", "partial", "bool", "future"],
)
async def test_remote_capability_required_before_vault_write(
    host, peer, monkeypatch, capability
):
    original_me = peer._me

    def advertised(headers):
        status, body = original_me(headers)
        return status, body | {"capabilities": {"remote_setup": capability}}

    monkeypatch.setattr(peer, "_me", advertised)
    vault = RestrictedFileVault(host.root / "vault", approved=True)
    async with NexusHTTPClient(peer.url) as http:
        with pytest.raises(ConnectorError) as error:
            await import_identity(
                http,
                vault,
                host.store,
                key=KEY,
                alias="worker",
                agent_hint="agent",
                expected_identity=dict(
                    server_id="srv_fake", agent_id="agent", credential_epoch=1
                ),
                require_remote_setup=True,
            )
    assert error.value.code == "VERSION_INCOMPATIBLE"
    assert host.store.load().identities == [] and vault.list_namespaces() == []


@pytest.mark.parametrize("unicode_content", [False, True])
def test_state_capacity_refusal_preserves_readable_previous_state(
    host, unicode_content
):
    content = (
        "\u2603" * ((4 * 1024 * 1024 - 1000) // 6)
        if unicode_content
        else "x" * (4 * 1024 * 1024 - 1000)
    )
    host.store.update(lambda state: state.preferences.update({"other": content}))
    before = host.store.path.read_bytes()
    with pytest.raises(ConnectorError) as error:
        draft(host)
    assert error.value.code == "CAPACITY_EXCEEDED"
    assert host.store.path.read_bytes() == before
    assert setup.PREFIX + "setup-test" not in host.store.load().preferences


async def test_stale_handoff_refused_before_import_or_checkpoint(
    continuation, monkeypatch
):
    r = continuation
    # The explicit file remains at the former revision; the authenticated
    # current Server metadata has advanced and must not be silently adopted.
    r.handoff["review_revision"] = "c" * 64

    async def unexpected(*a):
        pytest.fail("pipeline reached with stale handoff")

    monkeypatch.setattr(configure_local, "configure_local", unexpected)
    with pytest.raises(ConnectorError) as error:
        await connect.run_connect(r.args, r.host.output, r.host.root)
    assert error.value.code == "REMOTE_SETUP_REVIEW_REQUIRED"
    assert setup.load(r.host.store, "setup-test")["handoff"] is None
    assert not r.host.store.load().identities and r.vault.list_namespaces() == []


def test_authenticated_review_update_retires_pending_child_but_preserves_history(host):
    record = draft(host)
    first = handoff(record)
    record = setup.accept_handoff(host.store, "setup-test", record, first, first)
    old_intent = BindingIntentRecord(
        "srv_fake",
        "executor",
        "agent",
        "worker",
        "setup-test_prepare",
        "project",
        "realization",
        "digest",
        status="APPLY_PENDING",
        proposal={"proposal_id": "old-proposal"},
        apply_client_intent_id="setup-test_apply",
        remote_setup={"setup_run_id": "setup-test", "review_revision": "a" * 64},
    )
    saved_key = "configure.srv_fake.agent.setup-test"
    old_saved = dict(
        stage="apply",
        realization_ref="realization",
        child_requests=deepcopy(first["child_requests"]),
        proposal={"proposal_id": "old-proposal"},
        single_approval=True,
        remote_setup=deepcopy(old_intent.remote_setup),
    )

    def initial(state):
        state.binding_intents.append(old_intent)
        state.preferences[saved_key] = deepcopy(old_saved)

    host.store.update(initial)
    current = deepcopy(first)
    current["review_revision"] = "b" * 64
    for phase in ("prepare", "apply"):
        current["child_requests"][phase] += "_" + "f" * 24
    updated = setup.accept_handoff(host.store, "setup-test", record, current, current)
    state = host.store.load()
    assert updated["handoff"]["bootstrap"] == first["bootstrap"]
    assert updated["handoff_history"][0]["handoff"] == first
    assert (
        updated["handoff_history"][0]["configuration_checkpoint"]["proposal"]
        == old_saved["proposal"]
    )
    retired = state.binding_intents[0]
    assert (
        retired.status == "REVIEW_REPLACED" and retired.proposal == old_intent.proposal
    )
    assert retired.apply_client_intent_id == "setup-test_apply"
    assert state.preferences[saved_key]["stage"] == "prepare"
    assert state.preferences[saved_key]["realization_ref"] == "realization"
    assert state.preferences[saved_key]["child_requests"] == current["child_requests"]
    assert (
        setup.accept_handoff(host.store, "setup-test", updated, current, current)
        == updated
    )


@pytest.mark.parametrize(
    "change", ["finished", "realize", "same-prepared-child", "different-bootstrap"]
)
def test_review_update_cannot_discard_committed_or_changed_host_scope(host, change):
    record = draft(host)
    first = handoff(record)
    record = setup.accept_handoff(host.store, "setup-test", record, first, first)
    current = deepcopy(first)
    current["review_revision"] = "b" * 64
    for phase in ("prepare", "apply"):
        current["child_requests"][phase] += "_" + "f" * 24
    row = BindingIntentRecord(
        "srv_fake",
        "executor",
        "agent",
        "worker",
        "setup-test_prepare",
        "project",
        "realization",
        "digest",
    )
    if change == "finished":
        row.status, row.binding_id = "APPLIED", "binding"
    elif change == "realize":
        current["child_requests"]["realize"] += "_" + "f" * 24
    elif change == "same-prepared-child":
        current["child_requests"] = deepcopy(first["child_requests"])
    else:
        current["bootstrap"]["agent_id"] = "other"
    host.store.update(lambda state: state.binding_intents.append(row))
    before = host.store.path.read_bytes()
    with pytest.raises(ConnectorError):
        setup.accept_handoff(host.store, "setup-test", record, current, current)
    assert host.store.path.read_bytes() == before


async def test_retired_pending_apply_is_never_repeated_by_binding_service(binding):
    from okto_nexus_connector.services.background_onboarding import (
        continue_pending_bindings,
    )

    service, peer, store, prepare_args, apply_args = binding
    await service.prepare(**prepare_args)
    store.update(
        lambda state: setattr(state.binding_intents[0], "status", "REVIEW_REPLACED")
    )
    with pytest.raises(ConnectorError) as error:
        await service.apply(**apply_args)
    assert error.value.code == "OPERATION_CONFLICT" and not peer.applies
    await continue_pending_bindings(
        store,
        service.vault,
        server_id="srv",
        executor_id="executor",
        http_factory=service.http_factory,
    )
    assert not peer.applies


async def test_first_authenticated_handoff_uses_server_generated_child_map(
    continuation, monkeypatch
):
    r = continuation
    r.handoff["review_revision"] = "b" * 64
    for phase in ("prepare", "apply"):
        r.handoff["child_requests"][phase] += "_" + "e" * 24
    r.path.write_bytes(setup.document_bytes(r.handoff))

    async def pipeline(args, output, root, store, identity, server, portable):
        saved = store.load().preferences["configure.srv_fake.agent.setup-test"]
        setup.require_configuration(store, saved, identity)
        assert saved["child_requests"] == r.handoff["child_requests"]
        assert saved["remote_setup"]["review_revision"] == "b" * 64
        return {"saved": False}

    monkeypatch.setattr(configure_local, "configure_local", pipeline)
    await connect.run_connect(r.args, r.host.output, r.host.root)
    assert (
        setup.load(r.host.store, "setup-test")["child_requests"]
        == r.handoff["child_requests"]
    )


@pytest.mark.parametrize(
    "response",
    [
        {"handoff": {}, "material": {"api_key": KEY}},
        {"material": None},
        {"handoff": [], "material": None},
    ],
    ids=["secret-material", "no-handoff", "invalid-handoff"],
)
async def test_recover_transport_accepts_only_nonsecret_handoff(
    host, peer, monkeypatch, response
):
    original = peer._route

    def route(method, target, headers, body):
        if target == "/v1/connections/remote-setup:recover":
            peer.authenticate(headers)
            return 200, response
        return original(method, target, headers, body)

    monkeypatch.setattr(peer, "_route", route)
    async with NexusHTTPClient(peer.url) as http:
        with pytest.raises(ConnectorError) as error:
            await http.recover_remote_setup(KEY, setup_run_id="setup-test")
    assert error.value.code == "VERSION_INCOMPATIBLE" and KEY not in str(error.value)


async def test_new_prepared_child_can_use_alias_retired_by_authenticated_review(
    binding,
):
    service, peer, store, prepare_args, _ = binding
    prepare_args.update(
        client_intent_id="root_prepare",
        remote_setup={"setup_run_id": "root", "review_revision": "a" * 64},
    )
    await service.prepare(**prepare_args)
    previous = store.load().binding_intents[0]
    original = service._proposal(previous)
    store.update(
        lambda state: setattr(state.binding_intents[0], "status", "REVIEW_REPLACED")
    )
    calls = []

    async def prepare(key, **body):
        calls.append(body)
        return replace(original, proposal_id="new-proposal")

    peer.prepare_r4_binding = prepare
    revised = prepare_args | {
        "client_intent_id": "root_prepare_" + "e" * 24,
        "remote_setup": {"setup_run_id": "root", "review_revision": "b" * 64},
    }
    current = await service.prepare(**revised)
    assert current["proposal"]["proposal_id"] == "new-proposal"
    assert calls[0]["client_intent_id"] == revised["client_intent_id"]
    assert len(store.load().binding_intents) == 2
    assert store.load().binding_intents[0].proposal == previous.proposal


async def test_background_completion_keeps_root_id_for_revised_child(binding):
    from okto_nexus_connector.services.background_onboarding import (
        continue_pending_bindings,
    )

    service, peer, store, prepare_args, apply_args = binding
    intent = "root_prepare_" + "e" * 24
    prepare_args.update(
        client_intent_id=intent,
        remote_setup={"setup_run_id": "root", "review_revision": "b" * 64},
        connection_configuration={"version": 2},
    )
    proposal = await service.prepare(**prepare_args)
    key = "configure.srv.agent.root"
    store.update(
        lambda state: state.preferences.update(
            {
                key: {
                    "single_approval": True,
                    "proposal": proposal["proposal"],
                    "stage": "apply",
                }
            }
        )
    )
    apply_args.update(
        prepare_intent_id=intent, client_intent_id="root_apply_" + "e" * 24
    )
    with pytest.raises(ConnectorError):
        await service.apply(**apply_args)
    peer.approved = True
    await continue_pending_bindings(
        store,
        service.vault,
        server_id="srv",
        executor_id="executor",
        http_factory=service.http_factory,
    )
    result = store.load().preferences[key]
    assert result["stage"] == "done" and result["result"]["request_id"] == "root"
