from dataclasses import fields
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.unit.test_binding_onboarding import binding
from tests.unit.test_executor_onboarding import onboarding
from tests.unit.test_executor_registration import registration
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.transport.https_client import R4BindingView
from okto_nexus_connector.services.binding_authority import adopt_policy_revisions, refresh_idle_binding_authority


async def setup(binding):
    service, peer, store, prepare, apply = binding
    await service.prepare(**prepare)
    peer.approved = True
    await service.apply(**apply)
    record = store.load().execution_bindings[0]
    current = {f.name:getattr(record,f.name) for f in fields(R4BindingView)}
    return service, store, record, current


async def test_refresh_policy_revisions_preserves_local_consent(binding):
    _, store, record, current = await setup(binding)
    adopt_policy_revisions(store,record,current | {'authorization_revision':5,'configuration_revision':6})
    updated = store.load().execution_bindings[0]
    assert (updated.authorization_revision,updated.configuration_revision) == (5,6)
    assert updated.realization_snapshot_digest == record.realization_snapshot_digest


@pytest.mark.parametrize('change', [{'candidate_ref':'other'}, {'agent_id':'other'},
    {'binding_revision':2}, {'state':'REVOKED'}, {'authorization_revision':2}, {'configuration_revision':True}])
async def test_refresh_rejects_binding_changes_and_revision_rollback(binding, change):
    _, store, record, current = await setup(binding)
    with pytest.raises(ConnectorError):
        adopt_policy_revisions(store,record,current | change)
    assert store.load().execution_bindings[0] == record


async def test_refresh_skips_live_native_sessions(binding):
    service,store,record,current = await setup(binding)
    http = SimpleNamespace(_request=AsyncMock(return_value=current))
    host = SimpleNamespace(binding_sessions_closed=AsyncMock(return_value=False))
    await refresh_idle_binding_authority(store,service.vault,http,host,server_id='srv',executor_id='executor')
    http._request.assert_not_called()
