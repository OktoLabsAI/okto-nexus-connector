"""Bounded, private host checkpoints for reviewed remote identity bootstrap.

Drafts carry no credential or canonical authority. Paths and physical directory
evidence stay in the Connector state; the operator receives only the public draft.
"""
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit
import uuid

from nexus_connector_core.connection_configuration import parse_portable_connection_configuration
from nexus_connector_core import InstallationCandidate
from nexus_connector_core.discovery import selected_fingerprint

from ..errors import ConnectorError
from ..transport.https_client import origin_of

MAX_DOCUMENT_BYTES = 65536
MAX_DRAFTS = 32
PREFIX = 'remote-setup.'
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,159}\Z')
_REVISION = re.compile(r'[0-9a-f]{64}\Z')
_SECRET = re.compile(r'(?:nxs_|nxt4_|nxobs_|nxsept_|nxc4_)[A-Za-z0-9_-]{20,}')


def refuse(message, code='REMOTE_SETUP_INVALID'):
    raise ConnectorError(code, 'remote_setup', message)


def identifier(value, maximum=160):
    if type(value) is not str or len(value) > maximum or not _ID.fullmatch(value):
        refuse('Invalid setup identifier.')
    return value


def revision(value):
    if type(value) is not str or not _REVISION.fullmatch(value):
        refuse('Invalid setup revision.')
    return value


def document_bytes(value):
    try:
        pending, count = [(value, 0)], 0
        while pending:
            part, depth = pending.pop()
            count += 1
            if count > 4096 or depth > 12:
                refuse('Setup document exceeds its structural limit.')
            if type(part) is dict:
                if len(part) > 256 or any(type(k) is not str or len(k) > 160 for k in part):
                    refuse('Invalid setup object.')
                pending.extend((v, depth + 1) for v in part.values())
            elif type(part) is list:
                if len(part) > 256:
                    refuse('Invalid setup list.')
                pending.extend((v, depth + 1) for v in part)
            elif type(part) is str:
                if len(part) > 4096 or '\x00' in part or _SECRET.search(part):
                    refuse('Setup documents cannot contain credential material.')
            elif part is not None and type(part) not in (bool, int):
                refuse('Invalid setup value.')
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (ValueError, UnicodeError, RecursionError):
        refuse('Invalid setup document.')
    if len(raw) > MAX_DOCUMENT_BYTES:
        refuse('Setup document exceeds 64 KiB.', 'CAPACITY_EXCEEDED')
    return raw


def document_revision(value):
    return hashlib.sha256(document_bytes(value)).hexdigest()


def read_document(path):
    try:
        with Path(path).open('rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                refuse('Select a regular setup document.')
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
        if len(raw) > MAX_DOCUMENT_BYTES:
            refuse('Setup document exceeds 64 KiB.', 'CAPACITY_EXCEEDED')
        def unique(pairs):
            value = {}
            for key, part in pairs:
                if key in value:
                    refuse('Duplicate setup document fields.')
                value[key] = part
            return value
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                           parse_constant=lambda _: refuse('Invalid setup number.'))
        document_bytes(value)
        return value
    except (OSError, ValueError, UnicodeError, RecursionError):
        refuse('Cannot read the selected setup document.')


def export_draft(path, draft):
    """Never overwrite an unrelated file; identical exports are recoverable."""
    raw = document_bytes(draft) + b'\n'
    try:
        with Path(path).open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if document_revision(read_document(path)) != document_revision(draft):
            refuse('The selected export file contains another document.', 'OPERATION_CONFLICT')
    except OSError:
        refuse('The draft is retained locally, but its export was not confirmed.', 'STATE_UNAVAILABLE')


def child_requests(root):
    identifier(root, 100)
    return {name: root + '_' + name for name in ('register', 'realize', 'prepare', 'apply')}


def validate_child_requests(root, value):
    initial = child_requests(root)
    if type(value) is not dict or set(value) != set(initial):
        refuse('Invalid remote setup child map.')
    for phase, child in value.items():
        identifier(child)
        allowed = re.escape(initial[phase]) + (r'(?:_[0-9a-f]{24})?' if phase != 'register' else '')
        if not re.fullmatch(allowed, child):
            refuse('A child request belongs to another setup root.', 'OPERATION_CONFLICT')
    return deepcopy(value)


def directory_evidence(path):
    if path is None:
        return None
    try:
        root = Path(path).expanduser().resolve(strict=True)
        info = root.stat()
        if not root.is_dir():
            raise ValueError()
        return dict(path=str(root), device=str(info.st_dev), inode=str(info.st_ino))
    except (OSError, ValueError, TypeError):
        refuse('The reviewed local directory is unavailable.', 'PROFILE_DRIFT')


def normalize_origin(server):
    try:
        parsed = urlsplit(server)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
            refuse('Use an explicit Server origin without credentials or paths.')
        return origin_of(server)
    except (ValueError, TypeError):
        refuse('Invalid Server origin.')


def prepare(store, *, setup_run_id, server, configuration, candidate_ref, inventory_revision,
            candidate, agent_hint=None, workspace_hint=None, login_start_plan=None,
            review_current=False, expected=None):
    """Persist one immutable host selection before exporting or contacting Server."""
    root = identifier(setup_run_id, 100)
    portable = parse_portable_connection_configuration(configuration)
    if portable.get('runtime_enabled') is not True or portable.get('automatic_reply') is not True:
        refuse('Remote project setup requires runtime access and automatic replies.')
    if not re.fullmatch(r'nexus-install-v1:[0-9a-f]{64}', candidate_ref or ''):
        refuse('Select one inventoried installation.')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', inventory_revision or ''):
        refuse('An exact inventory revision is required.')
    for hint in (agent_hint, workspace_hint):
        if hint is not None:
            identifier(hint)
    local = dict(configuration=deepcopy(configuration), root=directory_evidence(configuration['workspace_root']),
                 provider_home=directory_evidence(configuration['provider_home']), login_start_plan=deepcopy(login_start_plan),
                 passive_inventory_revision=inventory_revision, candidate=asdict(candidate))
    document_bytes(local)
    label = configuration['workspace_label']
    if type(label) is not str or not 1 <= len(label) <= 120 or label != label.strip() or not label.isprintable():
        refuse('The project label must have 1 to 120 printable characters.')
    try:
        if selected_fingerprint(candidate) != candidate.fingerprint:
            refuse('The selected installation changed before the draft was saved.', 'PROFILE_DRIFT')
    except (OSError, ValueError):
        refuse('The selected installation is unavailable.', 'PROFILE_DRIFT')
    origin = normalize_origin(server)
    def stage(state):
        existing = state.preferences.get(PREFIX + root)
        connector_id = state.connector_id or 'conn_' + uuid.uuid4().hex[:24]
        draft = dict(format='okto.remote-setup-draft', version=1, setup_run_id=root,
            server_origin=origin, connector_id=connector_id, project_label=configuration['workspace_label'],
            agent_hint=agent_hint, workspace_hint=workspace_hint,
            installation=dict(adapter_id=portable['adapter_id'], candidate_ref=candidate_ref, inventory_revision=None),
            local_consent=dict(local_consent_id=root + '_consent', local_root_proof_digest=None, configuration_digest=None),
            configuration=portable, start_at_login=login_start_plan is not None)
        digest = document_revision(draft)
        if existing is not None:
            if review_current:
                from .remote_setup_revision import stage_review
                stage_review(state, root, expected, draft, local)
                validate_capacity(state, state.preferences[PREFIX + root])
                return
            if existing['draft_revision'] != digest or existing['local'] != local:
                refuse('This setup root already has different reviewed host choices.', 'OPERATION_CONFLICT')
            return
        if sum(key.startswith(PREFIX) for key in state.preferences) >= MAX_DRAFTS:
            refuse('The retained setup capacity is full. Existing roots remain resumable.', 'CAPACITY_EXCEEDED')
        state.connector_id = connector_id
        state.preferences[PREFIX + root] = dict(draft=draft, draft_revision=digest, child_requests=child_requests(root),
            local=local, handoff=None, handoff_history=[], identity_alias=None, stage='drafted')
        validate_capacity(state, state.preferences[PREFIX + root])
    store.update(stage)
    return load(store, root)


def load(store, root):
    state = store.load()
    record = state.preferences.get(PREFIX + identifier(root, 100))
    if type(record) is not dict or record.get('draft_revision') != document_revision(record.get('draft')):
        refuse('The retained setup is missing or changed.', 'PROFILE_DRIFT')
    validate_child_requests(root, record['child_requests'])
    if record['draft']['connector_id'] != state.connector_id:
        refuse('The retained setup scope changed.', 'PROFILE_DRIFT')
    return deepcopy(record)


def validate_local(record):
    local = record['local']
    for name in ('root', 'provider_home'):
        evidence = local[name]
        if evidence != directory_evidence(evidence['path'] if evidence else None):
            refuse('A reviewed local directory changed. Review this setup again.', 'PROFILE_DRIFT')
    if parse_portable_connection_configuration(local['configuration']) != record['draft']['configuration']:
        refuse('The reviewed local configuration changed.', 'PROFILE_DRIFT')
    if (local['configuration']['workspace_root'] != local['root']['path'] or
            local['configuration']['provider_home'] != (local['provider_home']['path'] if local['provider_home'] else None)):
        refuse('The local configuration differs from its reviewed directories.', 'PROFILE_DRIFT')
    try:
        candidate = InstallationCandidate(**local['candidate'])
        if selected_fingerprint(candidate) != candidate.fingerprint:
            refuse('The reviewed installation bytes changed.', 'PROFILE_DRIFT')
    except (OSError, ValueError, TypeError):
        refuse('The reviewed installation is unavailable.', 'PROFILE_DRIFT')


def validate_handoff(value, record):
    document_bytes(value)
    if (type(value) is not dict or set(value) != {'format', 'version', 'bootstrap', 'review_revision', 'child_requests', 'draft_revision'}
            or value['format'] != 'okto.remote-setup-handoff' or type(value['version']) is not int or value['version'] != 1):
        refuse('Select the non-secret setup handoff, not the credential response.')
    receipt = value['bootstrap']
    if type(receipt) is not dict or set(receipt) != {'setup_run_id', 'review_revision', 'server_id', 'agent_id', 'workspace_id', 'credential_epoch'}:
        refuse('Invalid bootstrap receipt.')
    for name in ('setup_run_id', 'server_id', 'agent_id', 'workspace_id'):
        identifier(receipt[name], 100 if name == 'setup_run_id' else 160)
    revision(receipt['review_revision'])
    revision(value['review_revision'])
    if type(receipt['credential_epoch']) is not int or not 1 <= receipt['credential_epoch'] <= (1 << 63) - 1:
        refuse('Invalid canonical credential revision.')
    validate_child_requests(receipt['setup_run_id'], value['child_requests'])
    if receipt['setup_run_id'] != record['draft']['setup_run_id'] or value['draft_revision'] != record['draft_revision']:
        refuse('The handoff differs from the retained host review.', 'OPERATION_CONFLICT')
    if record['handoff'] is not None and record['handoff']['bootstrap'] != receipt:
        refuse('This setup already has a different original bootstrap receipt.', 'OPERATION_CONFLICT')
    return deepcopy(value)


def accept_handoff(store, root, expected, file_handoff, authenticated_handoff, *, progress=None):
    """Commit an exact Server-confirmed current map, keeping retired children.

    The caller has reauthenticated the bootstrap subject and read recover on
    the same origin. This metadata does not authorize any binding application.
    """
    pending = expected.get('pending_local_review')
    target = expected
    promote = bool(pending and file_handoff.get('draft_revision') == pending['draft_revision'])
    if promote:
        target = expected | {name: pending[name] for name in ('draft', 'draft_revision', 'local')}
    supplied = validate_handoff(file_handoff, target)
    current = validate_handoff(authenticated_handoff, target)
    if document_revision(supplied) != document_revision(current):
        refuse('The handoff file is not the current Server review. Obtain its current non-secret handoff.', 'REMOTE_SETUP_REVIEW_REQUIRED')
    def commit(state):
        if state.preferences.get(PREFIX + root) != expected:
            refuse('The retained setup changed while its current Server review was checked.', 'STALE_GENERATION')
        value = deepcopy(expected)
        previous = value['handoff']
        if pending:
            from .remote_setup_revision import adopt_pending, recover_prior_finish
            if not promote:
                recover_prior_finish(state, root, value, current, progress)
                state.preferences[PREFIX + root] = value
                validate_capacity(state, value)
                return
            if progress and (progress['state'] == 'committed' or progress.get('final_binding')):
                refuse('This revision already has a Server Finish. Inspect its committed host mapping before changing local choices.', 'OPERATION_CONFLICT')
            adopt_pending(state, root, value, current)
            state.preferences[PREFIX + root] = value
            validate_capacity(state, value)
            return
        if previous == current:
            return
        if previous is not None:
            if previous['review_revision'] == current['review_revision']:
                refuse('The same review revision returned a different child map.', 'OPERATION_CONFLICT')
            old_ids, new_ids = previous['child_requests'], current['child_requests']
            # A changed local draft needs another explicit host review. This
            # continuation only changes Server-side review/prepare guards.
            if new_ids['register'] != old_ids['register'] or new_ids['realize'] != old_ids['realize']:
                refuse('This revision changes a materialized host child. Review the host setup explicitly.', 'REMOTE_SETUP_REVIEW_REQUIRED')
            receipt = current['bootstrap']
            key = f'configure.{receipt["server_id"]}.{receipt["agent_id"]}.{root}'
            saved = state.preferences.get(key)
            retired = [r for r in state.binding_intents if r.server_id == receipt['server_id']
                       and r.agent_id == receipt['agent_id'] and r.client_intent_id == old_ids['prepare']]
            if any(r.status in ('APPLIED', 'SUPERSEDED') or r.applied_binding or r.binding_id for r in retired) or (
                    saved and (saved.get('binding') or saved.get('stage') in ('done', 'policies'))):
                refuse('The previous binding has a committed local result; preserve it and inspect the current Server setup.', 'OPERATION_CONFLICT')
            if retired and (new_ids['prepare'] == old_ids['prepare'] or new_ids['apply'] == old_ids['apply']):
                refuse('A revised prepared child requires new durable request IDs.', 'OPERATION_CONFLICT')
            history = value.setdefault('handoff_history', [])
            if len(history) >= 16:
                refuse('The retained review history is full; no recovery record was removed.', 'CAPACITY_EXCEEDED')
            history.append(dict(handoff=deepcopy(previous), configuration_checkpoint={
                name: deepcopy(saved[name]) for name in ('stage', 'proposal', 'binding', 'result', 'remote_setup', 'child_requests')
                if saved and name in saved}))
            for retired_record in retired:
                retired_record.status = 'REVIEW_REPLACED'
            if saved is not None:
                saved.update(remote_setup=dict(setup_run_id=root, review_revision=current['review_revision']),
                             child_requests=deepcopy(new_ids))
                if saved.get('realization_ref'):
                    saved['stage'] = 'prepare'
                for name in ('proposal', 'binding', 'result', 'single_approval'):
                    saved.pop(name, None)
        value.update(handoff=current, child_requests=deepcopy(current['child_requests']), stage='awaiting_material')
        state.preferences[PREFIX + root] = value
        validate_capacity(state, value)
    store.update(commit)
    return load(store, root)


def verify_identity(me, expected, *, require_remote_setup=False):
    if any(type(getattr(me, name, None)) is not type(expected[name]) or getattr(me, name) != expected[name]
           for name in ('server_id', 'agent_id', 'credential_epoch')):
        refuse('The canonical key does not match the reviewed Server, agent and credential revision.', 'AGENT_ID_MISMATCH')
    if require_remote_setup and getattr(me, 'remote_setup_supported', False) is not True:
        refuse('This Server does not advertise the reviewed remote setup contract.', 'VERSION_INCOMPATIBLE')


def require_configuration(store, saved, identity):
    metadata = saved.get('remote_setup')
    if metadata is None:
        return
    record = load(store, metadata['setup_run_id'])
    if record['handoff'] is None:
        refuse('The configuration has no bootstrap handoff.', 'PROFILE_DRIFT')
    handoff = validate_handoff(record['handoff'], record)
    receipt = handoff['bootstrap']
    verify_identity(identity, receipt)
    if (metadata != dict(setup_run_id=receipt['setup_run_id'], review_revision=handoff['review_revision'])
            or record['child_requests'] != handoff['child_requests'] or saved['child_requests'] != handoff['child_requests']
            or saved['workspace_id'] != receipt['workspace_id']
            or saved['identity'] != record['identity_alias'] or saved['configuration'] != record['local']['configuration']
            or saved['candidate_ref'] != record['draft']['installation']['candidate_ref']
            or saved.get('local_consent_id', receipt['setup_run_id'] + '_consent') != record['draft']['local_consent']['local_consent_id']
            or saved['replace_binding_id'] is not None):
        refuse('The materialized configuration differs from this host review.', 'PROFILE_DRIFT')
    if record.get('pending_local_review'):
        refuse('A local review is awaiting the current operator handoff.', 'REMOTE_SETUP_REVIEW_REQUIRED')
    validate_local(record)


def recover_probe(store, saved, server_id):
    """A lost probe result may reuse persisted evidence, never repeat execution."""
    record = load(store, saved['remote_setup']['setup_run_id'])
    rows = [row for row in store.load().execution_executors if row.server_id == server_id]
    if len(rows) == 1 and any(entry.get('source') == record['local']['candidate'] and entry.get('version')
                             for entry in rows[0].installation_observations):
        return
    refuse('The installation check may have run, but no durable observation confirms it. Inspect this host before another explicit check.',
           'REMOTE_SETUP_EFFECT_UNCONFIRMED')


def checkpoint(store, root, expected, **changes):
    """CAS prevents concurrent CLI steps from overwriting an acknowledged stage."""
    def update(state):
        if state.preferences.get(PREFIX + root) != expected:
            refuse('The setup changed during this operation. Resume its current checkpoint.', 'STALE_GENERATION')
        state.preferences[PREFIX + root] = deepcopy(expected | changes)
        validate_capacity(state, state.preferences[PREFIX + root])
    store.update(update)
    return load(store, root)


def validate_capacity(state, record):
    document_bytes(record)
    from ..storage.state_store import state_to_json
    # Match the StateStore writer exactly, including Unicode escaping.
    if len(json.dumps(state_to_json(state), indent=1, sort_keys=True).encode('utf-8')) > 4 * 1024 * 1024:
        refuse('The local state capacity is exhausted. Existing recovery checkpoints were preserved.', 'CAPACITY_EXCEEDED')
