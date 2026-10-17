"""Independent Connector-state binding for the Core physical ledger."""
from pathlib import Path

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
            state.core_store_id or any(path.exists() for path in remnants)
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
            if state.core_store_id and state.core_store_id != store_id:
                raise ConnectorError('PROFILE_DRIFT', 'connector_store_binding',
                                     'The physical Core ledger changed.')
            state.core_store_id = store_id

        self.store.update(mutate)
