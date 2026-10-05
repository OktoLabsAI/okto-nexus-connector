from types import SimpleNamespace as NS
import io

import pytest

from okto_nexus_connector.cli.commands import binding_conflicts as conflicts
from okto_nexus_connector.cli.output import Output


@pytest.fixture
def existing(tmp_path):
    identity = NS(alias='worker', agent_id='agent', server_id='server')
    intent = NS(alias='claude', status='APPLIED', client_intent_id='old_prepare',
                agent_id='agent', server_id='server', executor_id='host',
                identity_alias='worker', binding_id='binding')
    binding = NS(binding_id='binding', agent_id='agent', server_id='server',
                 executor_id='host', adapter_id='claude_stream',
                 workspace_id='workspace', realization_ref='realization')
    root = NS(realization_ref='realization', server_id='server', executor_id='host',
              workspace_root=str(tmp_path))
    state = NS(binding_intents=[intent], bindings=[], execution_bindings=[binding], realizations=[root])
    return NS(load=lambda:state), identity, {'alias':'claude','adapter_id':'claude_stream'}, Output(json_mode=False,stream=io.StringIO())


def test_abort_is_default_and_does_not_change_existing_binding(existing, monkeypatch):
    def abort(label, options, default):
        assert default == 'abort'
        return default
    monkeypatch.setattr(conflicts,'choice',abort)
    assert conflicts.resolve_conflict(*existing,request_id='new') == {'canceled':True}
    assert existing[0].load().binding_intents[0].status == 'APPLIED'


def test_replace_selects_exact_binding_and_preserves_workspace(existing, monkeypatch):
    monkeypatch.setattr(conflicts,'choice',lambda *a:'replace')
    result = conflicts.resolve_conflict(*existing,request_id='new')
    assert result['binding_id'] == 'binding'
    assert result['workspace_id'] == 'workspace'
    assert existing[0].load().binding_intents[0].status == 'APPLIED'


@pytest.mark.parametrize('change',['identity','harness','workspace','pending','target'])
def test_incompatible_scope_cannot_be_replaced(existing, monkeypatch, change):
    store, identity, config, output = existing
    target = None
    if change == 'identity': identity.alias='other'
    if change == 'harness': config['adapter_id']='pi_rpc'
    if change == 'workspace': config['workspace_root']=str(__file__)
    if change == 'pending': store.load().binding_intents[0].status='APPLY_PENDING'
    if change == 'target': target='other'
    monkeypatch.setattr(conflicts,'choice',lambda *a:pytest.fail('must not offer unsafe replacement'))
    assert conflicts.resolve_conflict(*existing,request_id='new',binding_id=target)['canceled']


def test_resuming_own_pending_request_is_not_a_conflict(existing,monkeypatch):
    existing[0].load().binding_intents[0].status='PREPARE_PENDING'
    monkeypatch.setattr(conflicts,'choice',lambda *a:pytest.fail('unexpected prompt'))
    assert conflicts.resolve_conflict(*existing,request_id='old') == {'binding_id':None}
