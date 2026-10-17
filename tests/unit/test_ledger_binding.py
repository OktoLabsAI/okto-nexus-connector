"""The daemon's JSON state must identify the physical Core ledger it owns."""
import shutil
import sqlite3

import pytest

from okto_nexus_connector.errors import ConnectorError
from okto_nexus_connector.daemon.app import DaemonApp
from okto_nexus_connector.platform import paths
from okto_nexus_connector.services.core_host import CoreRuntimeHost
from okto_nexus_connector.storage.ledger_binding import StateStoreLedgerBinding
from okto_nexus_connector.storage.state_store import StateStore


def bound_host(root):
    store = StateStore(paths.state_file(root))
    return CoreRuntimeHost(root, None, store_binding=StateStoreLedgerBinding(store)), store


@pytest.mark.asyncio
async def test_daemon_composition_installs_the_binding_adapter(tmp_path):
    app = DaemonApp(tmp_path)
    try:
        ledger = await app.host.ensure_ledger()
        assert app.store.load().core_store_id == await ledger.physical_store_id()
        await app._stage_audit_anchor()
        state = app.store.load()
        core = await ledger.journal_audit_anchor()
        assert (state.core_audit_nonce, state.core_audit_revision,
                state.core_audit_digest) == (core.nonce, core.revision, core.digest)
    finally:
        await app.host.shutdown_all()


@pytest.mark.asyncio
async def test_audit_anchor_retries_after_core_commit_without_state_commit(tmp_path, monkeypatch):
    host, store = bound_host(tmp_path)
    binding = host._store_binding
    ledger = await host.ensure_ledger()
    original = store.update
    calls = 0
    def interrupted(mutate):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError('state commit interrupted')
        return original(mutate)
    monkeypatch.setattr(store, 'update', interrupted)
    try:
        with pytest.raises(OSError, match='interrupted'):
            await binding.stage_audit_anchor(ledger, paths.journal_path(tmp_path))
        assert await ledger.journal_audit_anchor() is not None
        assert store.load().core_audit_nonce == ''
        anchor = await binding.stage_audit_anchor(ledger, paths.journal_path(tmp_path))
        assert binding.stored_audit_anchor(await ledger.physical_store_id()) == anchor
        assert calls == 2
        snapshot = store.load()
        snapshot.core_audit_nonce = ''
        with pytest.raises(ConnectorError, match='PROFILE_DRIFT'):
            store.save(snapshot)
    finally:
        await host.shutdown_all()


@pytest.mark.asyncio
async def test_journal_creation_advances_head_and_reanchors_without_readiness_gate(tmp_path):
    host, store = bound_host(tmp_path)
    try:
        ledger = await host.ensure_ledger()
        binding = host._store_binding
        first = await binding.stage_audit_anchor(ledger, paths.journal_path(tmp_path))
        assert first.revision == 0
        await host.ensure_journal()
        second = await binding.stage_audit_anchor(ledger, paths.journal_path(tmp_path))
        assert second.revision == 1 and second.digest != first.digest
        assert binding.stored_audit_anchor(await ledger.physical_store_id()) == second
    finally:
        await host.shutdown_all()


@pytest.mark.asyncio
async def test_same_identity_ledger_rollback_is_refused_before_ready(tmp_path):
    host, store = bound_host(tmp_path)
    ledger = await host.ensure_ledger()
    await host.ensure_journal()
    anchor = await host._store_binding.stage_audit_anchor(
        ledger, paths.journal_path(tmp_path))
    assert anchor.revision == 1
    await host.shutdown_all()

    # Simulate a Core-only physical snapshot restored behind independent
    # Connector state while retaining the exact same store identity.
    with sqlite3.connect(paths.owned_slot_ledger_path(tmp_path)) as db:
        db.execute('DROP TRIGGER managed_journal_locations_no_delete')
        db.execute('DELETE FROM managed_journal_locations')
        db.execute('UPDATE managed_journal_head SET revision=0,digest=?,complete=1',
                   ('0' * 64,))
        db.execute('DELETE FROM managed_journal_audit_anchor')
    restarted, _ = bound_host(tmp_path)
    try:
        with pytest.raises(Exception, match='PROFILE_DRIFT'):
            await restarted.ensure_ledger()
        assert store.load().core_audit_nonce == anchor.nonce
    finally:
        await restarted.shutdown_all()


@pytest.mark.asyncio
async def test_daemon_refuses_mismatched_store_before_advertising_ready(tmp_path, monkeypatch):
    app = DaemonApp(tmp_path)
    async def unavailable():
        raise ConnectorError('JOURNAL_UNAVAILABLE', 'connector_store_binding')
    async def premature_ipc(*args):
        pytest.fail('IPC advertised readiness before validating the physical store')
    monkeypatch.setattr(app.host, 'ensure_ledger', unavailable)
    monkeypatch.setattr(app.ipc, 'start', premature_ipc)
    with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
        await app._run_forever()
    assert not app.lock.live_owner()


@pytest.mark.asyncio
async def test_first_ledger_identity_survives_restart_and_missing_store_is_refused(tmp_path):
    host, store = bound_host(tmp_path)
    ledger = await host.ensure_ledger()
    store_id = await ledger.physical_store_id()
    assert store.load().core_store_id == store_id
    await host.shutdown_all()

    restarted, _ = bound_host(tmp_path)
    try:
        assert await (await restarted.ensure_ledger()).physical_store_id() == store_id
    finally:
        await restarted.shutdown_all()

    snapshot = store.load()
    snapshot.core_store_id = ''
    with pytest.raises(ConnectorError, match='PROFILE_DRIFT'):
        store.save(snapshot)

    ledger_path = paths.owned_slot_ledger_path(tmp_path)
    ledger_path.rename(tmp_path / 'retained-ledger.db')
    missing, _ = bound_host(tmp_path)
    try:
        with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
            await missing.ensure_ledger()
        assert not ledger_path.exists()
    finally:
        await missing.shutdown_all()


@pytest.mark.asyncio
async def test_replaced_ledger_is_refused_and_closed(tmp_path):
    root = tmp_path / 'original'
    root.mkdir()
    host, store = bound_host(root)
    original = await (await host.ensure_ledger()).physical_store_id()
    await host.shutdown_all()

    replacement_root = tmp_path / 'replacement'
    replacement = CoreRuntimeHost(replacement_root, None)
    other = await (await replacement.ensure_ledger()).physical_store_id()
    assert other != original
    await replacement.shutdown_all()
    shutil.copyfile(paths.owned_slot_ledger_path(replacement_root),
                    paths.owned_slot_ledger_path(root))

    restarted, _ = bound_host(root)
    try:
        with pytest.raises(ConnectorError, match='PROFILE_DRIFT'):
            await restarted.ensure_ledger()
        assert store.load().core_store_id == original
        paths.owned_slot_ledger_path(root).rename(root / 'refused-ledger.db')
    finally:
        await restarted.shutdown_all()


@pytest.mark.asyncio
@pytest.mark.parametrize('remnant', ['journal', 'journal-wal', 'ledger-wal'])
async def test_unbound_state_with_retained_journal_cannot_create_empty_ledger(tmp_path, remnant):
    path = (paths.journal_path(tmp_path) if remnant.startswith('journal')
            else paths.owned_slot_ledger_path(tmp_path))
    if remnant.endswith('-wal'):
        path = path.with_name(path.name + '-wal')
    path.touch()
    host, store = bound_host(tmp_path)
    try:
        with pytest.raises(ConnectorError, match='JOURNAL_UNAVAILABLE'):
            await host.ensure_ledger()
        assert store.load().core_store_id == ''
        assert not paths.owned_slot_ledger_path(tmp_path).exists()
    finally:
        await host.shutdown_all()
