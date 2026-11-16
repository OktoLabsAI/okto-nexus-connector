"""Explicit local review, pending promotion, and recovery of a racing Finish."""
from copy import deepcopy
import json
import uuid

from . import remote_setup as setup


def _configured(state, root, record):
    receipt = (record.get('handoff') or {}).get('bootstrap')
    if receipt is None:
        return None
    return state.preferences.get(f'configure.{receipt["server_id"]}.{receipt["agent_id"]}.{root}')


def _intents(state, record):
    receipt = (record.get('handoff') or {}).get('bootstrap')
    if not receipt:
        return []
    return [r for r in state.binding_intents if r.server_id == receipt['server_id'] and r.agent_id == receipt['agent_id']
            and r.client_intent_id == record['child_requests']['prepare']]


def _native(local):
    candidate = local['candidate']
    return dict(configuration=local['configuration'], root=local['root'], provider_home=local['provider_home'],
        candidate={name: candidate.get(name) for name in ('adapter_id', 'executable', 'launch_script', 'fingerprint', 'build_identity', 'installation_ref')})


def stage_review(state, root, expected, proposed, local):
    record = state.preferences.get(setup.PREFIX + root)
    if record != expected:
        setup.refuse('The retained setup changed during local review.', 'STALE_GENERATION')
    if record.get('pending_local_review'):
        setup.refuse('A local review is already pending. Reexport it and resume its current handoff.', 'OPERATION_CONFLICT')
    saved = _configured(state, root, record)
    if (record.get('recovered_final_binding') or record['stage'] == 'configured'
            or saved and (saved.get('binding') or saved.get('stage') in ('done', 'policies'))
            or any(r.status in ('APPLIED', 'SUPERSEDED') or r.binding_id or r.applied_binding for r in _intents(state, record))):
        setup.refuse('This setup already has a confirmed Finish. Use explicit configuration of the existing binding.', 'OPERATION_CONFLICT')
    old = record['draft']
    if any(proposed[name] != old[name] for name in ('server_origin', 'connector_id', 'agent_hint', 'workspace_hint', 'setup_run_id')):
        setup.refuse('A local review cannot change the bootstrap namespace.', 'OPERATION_CONFLICT')
    native_changed = _native(local) != _native(record['local'])
    startup_changed = proposed['start_at_login'] != old['start_at_login'] or local['login_start_plan'] != record['local']['login_start_plan']
    if not native_changed and not startup_changed:
        return
    if startup_changed and proposed['start_at_login'] == old['start_at_login']:
        setup.refuse('Changing a concrete startup plan without changing the startup choice requires the existing service controls. Resume with the retained startup plan.', 'OPERATION_CONFLICT')
    if len(record.get('handoff_history', [])) >= 16:
        setup.refuse('The review history is full; no pending change or recovery record was created.', 'CAPACITY_EXCEEDED')
    proposed['local_consent'] = deepcopy(old['local_consent'])
    if native_changed:
        proposed['local_consent']['local_consent_id'] = root + '_consent_' + uuid.uuid4().hex[:24]
        proposed['local_consent']['local_root_proof_digest'] = None
        proposed['local_consent']['configuration_digest'] = None
    candidate_changed = proposed['installation']['candidate_ref'] != old['installation']['candidate_ref'] or (
        _native(local)['candidate'] != _native(record['local'])['candidate'])
    suspended = {}
    for child in _intents(state, record):
        if child.status not in ('REVIEW_REPLACED', 'REVIEW_SUSPENDED'):
            suspended[child.client_intent_id] = child.status
            child.status = 'REVIEW_SUSPENDED'
    record['pending_local_review'] = dict(draft=deepcopy(proposed), draft_revision=setup.document_revision(proposed),
        local=deepcopy(local), native_changed=native_changed, candidate_changed=candidate_changed,
        startup_changed=startup_changed, suspended_children=suspended, previous_stage=record['stage'])
    record['stage'] = 'awaiting_operator_review'
    # Reserve room for the bounded seven-ID Finish receipt and archive envelope.
    # A racing committed apply must remain recoverable without pruning history.
    pending_nodes, count = [record], 0
    while pending_nodes:
        value = pending_nodes.pop()
        count += 1
        if isinstance(value, dict):
            pending_nodes.extend(value.values())
        elif isinstance(value, list):
            pending_nodes.extend(value)
    from ..storage.state_store import state_to_json
    if (count > 4000 or len(setup.document_bytes(record)) > setup.MAX_DOCUMENT_BYTES - 4096
            or len(json.dumps(state_to_json(state), indent=1, sort_keys=True).encode('utf-8')) > 4 * 1024 * 1024 - 8192):
        setup.refuse('The pending review would consume space reserved for Finish recovery.', 'CAPACITY_EXCEEDED')


def _history(record, entry):
    history = record.setdefault('handoff_history', [])
    if len(history) >= 16:
        setup.refuse('The retained review history is full; no receipt was removed.', 'CAPACITY_EXCEEDED')
    history.append(deepcopy(entry))


def adopt_pending(state, root, record, current):
    pending = record['pending_local_review']
    old_ids, new_ids = record['child_requests'], current['child_requests']
    saved = _configured(state, root, record)
    if new_ids['register'] != old_ids['register']:
        setup.refuse('The reviewed registration intent changed.', 'OPERATION_CONFLICT')
    if saved and saved.get('realization_ref'):
        if pending['native_changed'] == (new_ids['realize'] == old_ids['realize']):
            setup.refuse('The current child map does not preserve unchanged realization content or replace changed content.', 'OPERATION_CONFLICT')
    retired = _intents(state, record)
    if any(r.status in ('APPLIED', 'SUPERSEDED') or r.binding_id or r.applied_binding for r in retired):
        setup.refuse('The previous Finish has a local committed result.', 'OPERATION_CONFLICT')
    if retired and (new_ids['prepare'] == old_ids['prepare'] or new_ids['apply'] == old_ids['apply']):
        setup.refuse('Changed prepared children require new Server request IDs.', 'OPERATION_CONFLICT')
    _history(record, dict(handoff=record['handoff'], draft=record['draft'], draft_revision=record['draft_revision'],
        local=record['local'], configuration_checkpoint=deepcopy(saved), outcome='local_review_promoted'))
    for child in retired:
        child.status = 'REVIEW_REPLACED'
    if saved is not None:
        saved.update(configuration=deepcopy(pending['local']['configuration']),
            candidate_ref=pending['draft']['installation']['candidate_ref'],
            local_consent_id=pending['draft']['local_consent']['local_consent_id'],
            remote_setup=dict(setup_run_id=root, review_revision=current['review_revision']), child_requests=deepcopy(new_ids))
        if pending['native_changed'] and saved.get('executor_id'):
            saved['stage'] = 'probe' if pending['candidate_changed'] else 'launch'
            saved['inventory_revision'] = pending['local']['passive_inventory_revision']
            saved.pop('realization_ref', None)
            saved.pop('configuration_digest', None)
            saved.pop('probe_operation', None)
        elif saved.get('realization_ref'):
            saved['stage'] = 'prepare'
        for name in ('proposal', 'binding', 'result', 'single_approval'):
            saved.pop(name, None)
        plan = pending['local']['login_start_plan']
        if plan != saved.get('login_start_plan'):
            saved.pop('login_start_plan', None)
            saved.pop('login_start_operation', None)
            if plan:
                saved.update(login_start_plan=deepcopy(plan),
                    login_start_operation=dict(state='pending', plan_revision=plan['plan_revision']))
    record.update(**{name: deepcopy(pending[name]) for name in ('draft', 'draft_revision', 'local')},
        handoff=deepcopy(current), child_requests=deepcopy(new_ids), stage='awaiting_host')
    record.pop('pending_local_review')


def recover_prior_finish(state, root, record, current, progress):
    final = (progress or {}).get('final_binding')
    if not progress or progress.get('state') != 'committed' or not isinstance(final, dict) or current != record['handoff']:
        setup.refuse('The local review is pending. Obtain its current operator handoff before continuing.', 'REMOTE_SETUP_REVIEW_REQUIRED')
    if set(final) != {'server_id', 'agent_id', 'workspace_id', 'executor_id', 'binding_id', 'proposal_id', 'finish_id'}:
        setup.refuse('The recovered Finish has an invalid canonical shape.', 'OPERATION_CONFLICT')
    for value in final.values():
        setup.identifier(value)
    receipt = current['bootstrap']
    for name in ('server_id', 'agent_id', 'workspace_id'):
        if final.get(name) != receipt[name]:
            setup.refuse('The recovered Finish differs from the retained bootstrap.', 'OPERATION_CONFLICT')
    children = _intents(state, record)
    if len(children) != 1 or not children[0].proposal or final.get('proposal_id') != children[0].proposal.get('proposal_id'):
        setup.refuse('The recovered Finish does not identify the retained prepared child.', 'OPERATION_CONFLICT')
    if (final['executor_id'] != children[0].executor_id or final.get('finish_id') != final['proposal_id'] + '_setup'
            or children[0].apply_client_intent_id != record['child_requests']['apply']):
        setup.refuse('The recovered Finish differs from the retained apply intent.', 'OPERATION_CONFLICT')
    pending = record['pending_local_review']
    _history(record, dict(unapplied_local_review=pending, outcome='prior_finish_committed', final_binding=final))
    for child in children:
        if child.status == 'REVIEW_SUSPENDED':
            child.status = pending['suspended_children'].get(child.client_intent_id, 'APPLY_PENDING')
    record.update(stage=pending['previous_stage'], recovered_final_binding=deepcopy(final))
    record.pop('pending_local_review')
