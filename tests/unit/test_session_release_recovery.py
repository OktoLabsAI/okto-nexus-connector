"""Only accepted original opening/ownership facts retire old-channel reports."""
import copy
import json
import sqlite3
from hashlib import sha256

import pytest
from nexus_connector_core import R4_PREVIEW_REVISION
from nexus_connector_core.session_release import encode_session_release
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.r4_session_releases import R4SessionReleaseStore
from tests.contract.test_session_release import request
from tests.unit.test_session_release_outbox import identity


def evidence(original):
    scope = original['scope']
    ack = dict(protocol_major=1, contract_revision=R4_PREVIEW_REVISION, type='reconcile.accepted',
        server_id=original['server_id'], executor_id=original['executor_id'], connection_id='replacement',
        connection_generation=original['connection_generation'] + 1, reconcile_id='accepted-cycle',
        recovery_remaining=False, ready_lane_ids=[], session_lease_requirements=[])
    opening = {k: original[k] for k in ('server_id','executor_id','connection_id','connection_generation','stream_epoch')}
    opening.update({k: scope[k] for k in ('binding_id','agent_id','session_id','session_owner_generation')})
    opening.update(operation_id=original['opening_operation_id'], intent_hash=original['opening_intent_hash'])
    key = dict(session_id=scope['session_id'], owner_generation=scope['session_owner_generation'])
    observation = dict(opening=opening, claim=dict(**key, state='RELEASED'),
        fact=dict(**key, process_state='EXITED', proof_digest=original['proof_digest']),
        watermark=dict(session_id=scope['session_id'], stream_epoch=original['stream_epoch'], sequence=original['sequence'] + 1))
    return ack, {(scope['session_id'],scope['session_owner_generation']): observation}


def test_recovery_keeps_original_bytes_and_separate_authenticated_proof(tmp_path):
    original = request()
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    outbox.prepare(original)
    ack, observations = evidence(original)
    assert outbox.reconcile_page(ack, observations) is None
    rebuilt = R4SessionReleaseStore(outbox.path)
    assert rebuilt.pending(**identity(original)) is None
    assert rebuilt.reconcile_page(ack, observations) is None
    with pytest.raises(ConnectorError, match='STALE_GENERATION'):
        rebuilt.prepare(original)
    with pytest.raises(ConnectorError, match='STALE_GENERATION'):
        rebuilt.prepare(dict(original, release_id='another-observation', sequence=original['sequence'] + 1))
    with sqlite3.connect(outbox.path) as conn:
        raw, proof, state = conn.execute('SELECT request,recovery,state FROM releases').fetchone()
    assert raw == encode_session_release(original) and state == 'PENDING'
    assert json.loads(proof)['ack'] == ack
    # The original request never acquires the replacement generation or a
    # fabricated live-session ACK. Resolution has its own recorded provenance.
    assert json.loads(raw)['connection_generation'] < ack['connection_generation']


@pytest.mark.parametrize('tamper', ['server','generation','connection','opening','owner','stream','sequence','claim','agent','opening_channel','absent'])
def test_ready_channel_without_matching_release_evidence_preserves_pending(tmp_path, tamper):
    original = request()
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    outbox.prepare(original)
    ack, observations = evidence(original)
    observation = next(iter(observations.values()))
    if tamper == 'server': ack['server_id'] = 'another'
    elif tamper == 'generation': ack['connection_generation'] = original['connection_generation']
    elif tamper == 'connection': ack['connection_id'] = original['connection_id']
    elif tamper == 'opening': observation['opening']['operation_id'] = 'another'
    elif tamper == 'owner': observation['claim']['owner_generation'] += 1
    elif tamper == 'stream': observation['watermark']['stream_epoch'] = 'another'
    elif tamper == 'sequence': observation['watermark']['sequence'] = original['sequence'] - 1
    elif tamper == 'claim': observation['claim']['state'] = 'UNKNOWN'
    elif tamper == 'agent': observation['opening']['agent_id'] = 'another'
    elif tamper == 'opening_channel': observation['opening']['connection_id'] = 'another'
    else: observations = {}
    assert outbox.reconcile_page(ack, observations) is None
    assert R4SessionReleaseStore(outbox.path).pending(**identity(original)) == original


def test_recovery_ack_commit_failure_rolls_back_and_identical_retry_resolves(tmp_path):
    original = request()
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    outbox.prepare(original)
    ack, observations = evidence(original)
    with sqlite3.connect(outbox.path) as conn:
        conn.execute("CREATE TRIGGER fail_recovery BEFORE UPDATE ON releases BEGIN SELECT RAISE(ABORT,'fault'); END")
    with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
        outbox.reconcile_page(ack, observations)
    assert outbox.pending(**identity(original)) == original
    with sqlite3.connect(outbox.path) as conn:
        assert conn.execute('SELECT recovery FROM releases').fetchone() == (None,)
        conn.execute('DROP TRIGGER fail_recovery')
    R4SessionReleaseStore(outbox.path).reconcile_page(ack, observations)
    assert outbox.pending(**identity(original)) is None


def test_prior_schema_upgrades_without_replacing_pending_request(tmp_path):
    original, path = request(), tmp_path / 'old.sqlite3'
    raw = encode_session_release(original)
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE releases (release_id TEXT PRIMARY KEY,identity TEXT NOT NULL,'
                     'request BLOB NOT NULL,digest TEXT NOT NULL,state TEXT NOT NULL,reply BLOB)')
        conn.execute('INSERT INTO releases VALUES (?,?,?,?,?,?)', (original['release_id'],
            R4SessionReleaseStore._identity(**identity(original)), raw, sha256(raw).hexdigest(), 'PENDING', None))
        conn.execute('PRAGMA user_version=1')
    assert R4SessionReleaseStore(path).pending(**identity(original)) == original
    with sqlite3.connect(path) as conn:
        assert conn.execute('PRAGMA user_version').fetchone()[0] == 3
        assert conn.execute('SELECT request,recovery,event_retired FROM releases').fetchone() == (raw, None, 0)


def test_recovery_pages_do_not_skip_an_unmatched_record_or_rewrite_history(tmp_path):
    original = request()
    outbox = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    observations, originals = {}, []
    for index in range(130):
        value = copy.deepcopy(original)
        value['release_id'] = f'release-{index}'
        value['scope']['session_id'] = f'session-{index}'
        outbox.prepare(value)
        ack, facts = evidence(value)
        if index != 64: observations.update(facts)
        originals.append(value)
    after = 0
    for page in range(3):
        after = outbox.reconcile_page(ack, observations, after=after, limit=64)
        assert (after is None) == (page == 2)
    for index, value in enumerate(originals):
        assert outbox.pending(**identity(value)) == (value if index == 64 else None)
