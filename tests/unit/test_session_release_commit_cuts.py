"""Release publications retain their original bytes across every local commit."""
from contextlib import contextmanager
import sqlite3

import pytest
from nexus_connector_core.session_release import encode_session_release, session_release_accepted
from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.storage.r4_session_releases import R4SessionReleaseStore
from tests.contract.test_session_release import request
from tests.unit.test_session_release_outbox import identity
from tests.unit.test_session_release_recovery import evidence


def rows(path):
    with sqlite3.connect(path) as conn:
        return conn.execute('SELECT * FROM releases ORDER BY release_id').fetchall()


@pytest.mark.parametrize('boundary', ['rollback', 'lost_reply'])
@pytest.mark.parametrize('stage', ['prepare', 'acknowledge', 'reconcile'])
def test_release_store_commit_cut_reconstructs_original_report(tmp_path, monkeypatch, boundary, stage):
    store = R4SessionReleaseStore(tmp_path / 'releases.sqlite3')
    original = request()
    ack = session_release_accepted(original)
    recovery_ack, observations = evidence(original)
    assert store.pending(**identity(original)) is None
    if stage != 'prepare':
        store.prepare(original)
    before = rows(store.path)
    transaction, fired = store._transaction, []

    @contextmanager
    def cut():
        with transaction() as conn:
            yield conn
            fired.append(stage)
            if boundary == 'rollback':
                raise OSError('Before release commit')
        raise OSError('After release commit')

    monkeypatch.setattr(store, '_transaction', cut)

    def action(target):
        if stage == 'prepare':
            return target.prepare(original)
        if stage == 'acknowledge':
            return target.acknowledge(ack)
        return target.reconcile_page(recovery_ack, observations)

    with pytest.raises((ConnectorError, OSError)):
        action(store)
    assert fired == [stage]
    observed = rows(store.path)
    if boundary == 'rollback':
        assert observed == before
    else:
        assert observed != before
    rebuilt = R4SessionReleaseStore(store.path)
    expected_pending = original if stage == 'prepare' and boundary == 'lost_reply' or stage != 'prepare' and boundary == 'rollback' else None
    assert rebuilt.pending(**identity(original)) == expected_pending
    action(rebuilt)
    settled = rows(store.path)
    assert len(settled) == 1
    action(rebuilt)
    assert rows(store.path) == settled
    if boundary == 'lost_reply':
        assert settled == observed
    with sqlite3.connect(store.path) as conn:
        raw, state, reply, recovery = conn.execute('SELECT request,state,reply,recovery FROM releases').fetchone()
    assert raw == encode_session_release(original)
    assert state == ('ACCEPTED' if stage == 'acknowledge' else 'PENDING')
    assert reply == (encode_session_release(ack) if stage == 'acknowledge' else None)
    assert (recovery is not None) is (stage == 'reconcile')
    assert rebuilt.pending(**identity(original)) == (original if stage == 'prepare' else None)
    if stage == 'reconcile':
        with pytest.raises(ConnectorError, match='STALE_GENERATION'):
            rebuilt.prepare(original)
