"""Approved Pi launch uses durable capability issuance and real Core authority."""
import asyncio
import time

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.identity.vault import RestrictedFileVault
from okto_nexus_connector.services.session_capabilities import SessionCapabilityOwner, ApprovedNativeLaunchProvider, R4NativeToolServices
from okto_nexus_connector.transport.https_client import R4SessionCapability
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_execution import execution, observed, failed


@pytest.mark.parametrize("selection", ["pi"], indirect=True)
@pytest.mark.parametrize("fault", [None, "scope", "drift", "lane", "native_before", "native_after"])
async def test_approved_native_launch_owns_bridge_and_rejects_changed_authority(execution, selection, tmp_path, fault, monkeypatch):
    owner, connection, factory, receipts, opening = execution
    store, candidate, *_ = selection
    vault = RestrictedFileVault(tmp_path, approved=True)
    capability_owner = SessionCapabilityOwner(store, vault)
    calls = []
    metadata_calls = []
    native_calls = []
    class HTTP:
        async def request_r4_session_capability(self, key, *, frame, **kwargs):
            assert key == "operator-test-key"
            assert kwargs["audience"] == "nexus-native-session"
            calls.append(kwargs)
            assert store.load().session_capabilities[0].status == "REQUESTED"
            scope = {name: frame[name] for name in (
                "server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                "workspace_binding_id", "session_id", "session_owner_generation",
                "authorization_revision", "configuration_revision", "binding_revision", "credential_epoch")}
            if fault == "scope":
                scope["agent_id"] = "other"
            if fault == "drift":
                store.update(lambda s: setattr(s.launch_configurations[0], "profile_revision", 2))
            if fault == "lane":
                await connection.close()
            return R4SessionCapability("cap", "native-cap:cap", "nxc4_" + "x" * 40,
                scope, "nexus-native-session", tuple(kwargs["actions"]), 60, time.monotonic() + 60, None)
        async def describe_r4_session_capability(self, key, *, frame, **kwargs):
            from okto_nexus_connector.transport.https_client import R4CapabilityMetadata
            assert key == 'operator-test-key'
            metadata_calls.append(kwargs)
            if fault == 'native_after':
                store.update(lambda s: setattr(s.launch_configurations[0], 'profile_revision', 2))
            current = runtime.r4_native_action_context(scope, connection_id='connection', connection_generation=1)
            return R4CapabilityMetadata('cap','native-cap:cap',scope,'nexus-native-session',
                kwargs['actions'],time.monotonic()+60,current.r4_authority.lease_id,
                current.r4_authority.lease_serial,None)
        async def native_action(self, capability, *, request):
            native_calls.append(request)
            assert capability.capability_ref == request.capability_ref
            return {'status':'OPEN'}
    async def candidates(frame):
        return [candidate]
    def guard(frame):
        if not connection.online:
            raise ConnectorError("CONTROL_DISCONNECTED", "test", "The lane is closed.")
    owner.launch_provider = None
    owner.native_tools = R4NativeToolServices(capability_owner, HTTP(), "operator-test-key")
    try:
        await connection.emit(opening)
        if fault in (None, "native_before", "native_after"):
            await observed(receipts, owner)
            assert len(factory.opened) == 1 and connection.lease_count == 1
            assert len(owner.host._native_action_owners) == 1
            native_owner = next(iter(owner.host._native_action_owners.values()))
            runtime = next(iter(owner.host._runtimes.values()))
            cap = store.load().session_capabilities[0]
            assert cap.capability_ref in factory.opened[0].secret_refs
            scope = {name: opening[name] for name in (
                "server_id", "executor_id", "binding_id", "agent_id", "workspace_id",
                "workspace_binding_id", "session_id", "session_owner_generation",
                "authorization_revision", "configuration_revision", "binding_revision", "credential_epoch")}
            context = runtime.r4_native_action_context(scope, connection_id="connection",
                                                       connection_generation=1)
            await native_owner.launch(factory.opened[0], opening["session_id"], context)
            assert native_owner.session_key.session_id == opening["session_id"]
            from nexus_connector_core.native_action_bridge import ContextGet
            from okto_nexus_connector.services import launch_configuration
            def no_new_launch(*args, **kwargs):
                raise AssertionError("A running native action must not qualify a new launch.")
            monkeypatch.setattr(launch_configuration, 'resolve_execution_selection', no_new_launch)
            if fault == 'native_before':
                store.update(lambda s: setattr(s.launch_configurations[0], 'profile_revision', 2))
            request = ContextGet('read', opening['session_id'], cap.capability_ref, 'work')
            if fault is None:
                assert await native_owner._service._bridge.invoke(request, context) == {'status':'OPEN'}
                assert len(metadata_calls) == len(native_calls) == 1
            else:
                from nexus_connector_core import CoreError
                with pytest.raises(CoreError) as refused:
                    await native_owner._service._bridge.invoke(request, context)
                assert refused.value.code == 'PROFILE_DRIFT'
                assert not native_calls
                assert len(metadata_calls) == (0 if fault == 'native_before' else 1)
            assert await native_owner.close(timeout_seconds=1)
        else:
            await failed(owner)
            assert not factory.opened and connection.lease_count == 0
            assert not owner.host._native_action_owners
        assert len(calls) == 1
        record = store.load().session_capabilities[0]
        assert record.status == "STORED"
        assert vault.resolve(record.secret_handle) == "nxc4_" + "x" * 40
        assert "nxc4_" not in store.path.read_text()
    finally:
        await capability_owner.close()
