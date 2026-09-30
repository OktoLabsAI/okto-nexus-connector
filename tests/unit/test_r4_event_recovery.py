"""Pre-ready event replay uses persisted streams and current binding authority."""
import pytest
from nexus_connector_core import RuntimeEvent
from okto_nexus_connector.services.r4_event_recovery import recover_event_streams
from okto_nexus_connector.storage.r4_events import R4EventStore,FIELDS
from okto_nexus_connector.errors import ConnectorError
from tests.unit.test_execution_selection import selection
from tests.unit.test_r4_daemon_execution import lifecycle


async def prepared(lifecycle):
    owner,http,store,frame,native,attached,published,keys=lifecycle
    scope={k:frame[k] for k in FIELDS if k!="stream_epoch"} | dict(stream_epoch="epoch")
    progress=R4EventStore.for_state(store)
    progress.register(scope)
    owner.host.root.mkdir(parents=True,exist_ok=True)
    journal=await owner.host.ensure_history_journal()
    await journal.record_event(RuntimeEvent(scope["server_id"],scope["executor_id"],scope["session_id"],
                                           scope["stream_epoch"],0,"text_delta",None,{"text":"Recovered"}))
    return owner,http,scope,progress,journal,native,attached,keys


async def current(): pass


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_pending_stream_replays_once_and_reuses_its_authority(lifecycle):
    owner,http,scope,progress,journal,native,attached,keys=await prepared(lifecycle)
    authorities={}
    await recover_event_streams(owner.store,owner.vault,http,owner.host,owner.connection,
                               require_current=current,authorities=authorities)
    assert progress.read(scope)["core_applied"]==progress.read(scope)["remote_acked"]==1
    assert len(attached)==len(authorities)==len(keys)==1 and not native.opened
    await recover_event_streams(owner.store,owner.vault,http,owner.host,owner.connection,
                               require_current=current,authorities=authorities)
    assert len(attached)==len(keys)==1 and not native.opened


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_local_ack_failure_keeps_progress_and_does_not_issue_another_ticket(lifecycle,monkeypatch):
    owner,http,scope,progress,journal,native,attached,keys=await prepared(lifecycle)
    authorities={}
    original=journal.acknowledge_events
    calls=0
    async def fail(*args):
        nonlocal calls
        calls+=1
        if calls==1: raise OSError("Injected Core ACK interruption.")
        return await original(*args)
    monkeypatch.setattr(journal,"acknowledge_events",fail)
    with pytest.raises(OSError):
        await recover_event_streams(owner.store,owner.vault,http,owner.host,owner.connection,
                                   require_current=current,authorities=authorities)
    assert progress.read(scope)["remote_acked"]==1 and progress.read(scope)["core_applied"]==0
    await recover_event_streams(owner.store,owner.vault,http,owner.host,owner.connection,
                               require_current=current,authorities=authorities)
    assert progress.read(scope)["core_applied"]==1
    assert calls==2 and len(keys)==len(attached)==1 and not native.opened


@pytest.mark.parametrize("selection",[True],indirect=True)
async def test_identity_change_during_ticket_acquisition_preserves_the_event(lifecycle):
    owner,http,scope,progress,journal,native,attached,keys=await prepared(lifecycle)
    def revoke():
        def change(state): state.execution_bindings[0].state="REVOKED"
        owner.store.update(change)
    http.after_ticket=revoke
    with pytest.raises(ConnectorError):
        await recover_event_streams(owner.store,owner.vault,http,owner.host,owner.connection,
                                   require_current=current,authorities={})
    assert progress.read(scope)["remote_acked"]==0 and not attached and not native.opened


def test_stream_scan_is_namespace_scoped_and_has_a_stable_high_water(tmp_path):
    store=R4EventStore(tmp_path/"events.db")
    scope=dict(server_id="srv",executor_id="exe",binding_id="binding",agent_id="agent",session_id="session",stream_epoch="epoch")
    for n in range(129): store.register({**scope,"session_id":str(n)})
    store.register({**scope,"server_id":"foreign"})
    first,high=store.page("srv","exe")
    assert len(first)==128
    store.register({**scope,"session_id":"later"})
    second,_=store.page("srv","exe",after=first[-1][0],high=high)
    assert len(second)==1 and second[0][1]["scope"]["server_id"]=="srv"
    assert store.page("srv","exe",after=second[-1][0],high=high)[0]==[]
