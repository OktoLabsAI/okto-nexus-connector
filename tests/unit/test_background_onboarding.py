from types import SimpleNamespace as NS

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.services import background_onboarding as background


@pytest.fixture
def pending(monkeypatch):
    record = NS(server_id='server', executor_id='host', agent_id='agent',
        status='APPLY_PENDING', connection_configuration={'version': 2},
        client_intent_id='configure_test_prepare', apply_client_intent_id='configure_test_apply',
        identity_alias='agent', alias='claude', approved_diff_hash='hash', operator_proof_ref='apr_test',
        proposal={'proposal_id': 'proposal'})
    state = NS(binding_intents=[record], preferences={})
    store = NS(load=lambda: state, update=lambda change: change(state))
    calls = []
    gate = {'error': 'APPROVAL_REQUIRED'}
    class Bindings:
        def __init__(self, *a, **kw): pass
        async def apply(self, **kw):
            calls.append(kw)
            if gate['error']:
                raise ConnectorError(gate['error'], 'binding.apply')
            record.status = 'APPLIED'
            return {'binding': {'binding_id': 'binding'}}
    monkeypatch.setattr(background, 'BindingOnboarding', Bindings)
    return store, state, record, calls, gate


async def test_background_continues_after_cli_exit_and_does_not_apply_twice(pending):
    store, state, record, calls, gate = pending
    await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert record.status == 'APPLY_PENDING'
    assert next(iter(state.preferences.values()))['status'] == 'awaiting_approval'
    # New worker invocation models daemon restart; all input is durable state.
    gate['error'] = None
    await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert record.status == 'APPLIED'
    await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert calls[1]['operator_proof_ref'] == 'apr_test'


@pytest.mark.parametrize('code', ['PERMISSION_DENIED', 'CONFLICT', 'PROFILE_DRIFT'])
async def test_rejection_expiry_and_drift_stop_background_retries(pending, code):
    store, state, _, calls, gate = pending
    gate['error'] = code
    for _ in range(2):
        await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert len(calls) == 1
    assert next(iter(state.preferences.values()))['terminal'] is True


async def test_network_failure_retries_but_other_hosts_and_unsubmitted_intents_do_not(pending):
    store, _, record, calls, gate = pending
    gate['error'] = 'NETWORK_ERROR'
    await background.continue_pending_bindings(store, None, server_id='server', executor_id='other')
    assert not calls
    for _ in range(2):
        await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert len(calls) == 2
    record.status = 'PREPARED'
    await background.continue_pending_bindings(store, None, server_id='server', executor_id='host')
    assert len(calls) == 2
