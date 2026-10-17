"""Independent Connector-state binding for the Core physical ledger."""
import asyncio
from pathlib import Path
import secrets

from nexus_connector_core import JournalAuditAnchor, JournalLocation

from ..errors import ConnectorError
from .state_store import StateStore


class StateStoreLedgerBinding:
    def __init__(self, store: StateStore):
        self.store = store

    def preflight(self, ledger_path: Path, journal_path: Path) -> None:
        """A missing ledger is fresh only when no counterpart remembers it."""
        state = self.store.load()
        remnants = (journal_path, *(Path(str(path) + suffix)
            for path in (journal_path, ledger_path) for suffix in ('-wal', '-shm')))
        if not ledger_path.is_file() and (
            state.core_store_id or state.core_audit_nonce
            or any(path.exists() for path in remnants)
        ):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'connector_store_binding',
                                 'The configured physical Core ledger is unavailable.')

    def bind(self, store_id: str) -> None:
        if (type(store_id) is not str or len(store_id) != 38
                or not store_id.startswith('store_')
                or any(char not in '0123456789abcdef' for char in store_id[6:])):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'connector_store_binding',
                                 'The physical Core ledger has no valid identity.')

        def mutate(state):
            if state.core_audit_nonce and not state.core_store_id:
                raise ConnectorError('PROFILE_DRIFT', 'connector_store_binding',
                                     'The physical audit anchor has no store identity.')
            if state.core_store_id and state.core_store_id != store_id:
                raise ConnectorError('PROFILE_DRIFT', 'connector_store_binding',
                                     'The physical Core ledger changed.')
            state.core_store_id = store_id

        self.store.update(mutate)

    def stored_audit_anchor(self, store_id: str) -> JournalAuditAnchor | None:
        state = self.store.load()
        if state.core_store_id != store_id:
            raise ConnectorError('PROFILE_DRIFT', 'connector_audit_anchor',
                                 'The physical Core ledger changed.')
        if not state.core_audit_nonce and state.core_audit_revision == 0 and not state.core_audit_digest:
            return None
        if (type(state.core_audit_nonce) is not str
                or len(state.core_audit_nonce) != 38
                or not state.core_audit_nonce.startswith('audit_')
                or any(char not in '0123456789abcdef' for char in state.core_audit_nonce[6:])
                or type(state.core_audit_revision) is not int or state.core_audit_revision < 0
                or type(state.core_audit_digest) is not str
                or len(state.core_audit_digest) != 64
                or any(char not in '0123456789abcdef' for char in state.core_audit_digest)):
            raise ConnectorError('PROFILE_DRIFT', 'connector_audit_anchor',
                                 'The physical audit anchor is incomplete.')
        return JournalAuditAnchor(store_id, state.core_audit_nonce,
                                  state.core_audit_revision, state.core_audit_digest)

    async def stage_audit_anchor(self, ledger, journal_path: Path,
                                 *, stop: asyncio.Event | None = None) -> JournalAuditAnchor | None:
        """Stage matching tokens after inspecting the shared journal location.

        The token is not used for admission; daemon readiness does not wait for
        conversion or this state write.
        """
        store_id = await ledger.physical_store_id()
        self.stored_audit_anchor(store_id)
        if not journal_path.is_file() and any(
                Path(str(journal_path) + suffix).exists() for suffix in ('-wal', '-shm')):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'connector_audit_anchor',
                                 'The physical journal is unavailable.')
        if journal_path.is_file():
            await ledger.register_journal_locations((JournalLocation(journal_path.name),))
        while stop is None or not stop.is_set():
            head = await ledger.journal_registry_head()
            if head.complete:
                break
            await ledger.advance_journal_registry_head(limit=128)
            await asyncio.sleep(0.01)
        else:
            return None
        while stop is None or not stop.is_set():
            if await ledger.advance_journal_extension_index(limit=128):
                break
            await asyncio.sleep(0.01)
        else:
            return None
        stored = self.stored_audit_anchor(store_id)
        if stored is not None:
            prefix = await ledger.journal_digest_at_revision(stored.revision)
            if prefix != stored.digest:
                raise ConnectorError('PROFILE_DRIFT', 'connector_audit_anchor',
                                     'The physical registry no longer extends the stored audit.')
        core_anchor = await ledger.journal_audit_anchor()
        if (stored is not None and stored == core_anchor
                and stored.revision == head.revision and stored.digest == head.digest):
            return stored
        anchor = await ledger.stage_journal_audit_anchor(
            nonce='audit_' + secrets.token_hex(16), head=head)
        def mutate(state):
            if state.core_store_id != anchor.store_id:
                raise ConnectorError('PROFILE_DRIFT', 'connector_audit_anchor',
                                     'The physical Core ledger changed.')
            state.core_audit_nonce = anchor.nonce
            state.core_audit_revision = anchor.revision
            state.core_audit_digest = anchor.digest
        await asyncio.to_thread(self.store.update, mutate)
        return anchor
