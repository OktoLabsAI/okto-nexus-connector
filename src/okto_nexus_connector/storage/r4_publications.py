"""Durable publication obligations, never an authority to repeat an effect.

Reserve before invoking Core. A reservation without a projected receipt stays
unresolved after a crash. Only a correlated durable Server acknowledgment can
remove a ready receipt. Raw operation payloads and credentials are not stored.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

from nexus_connector_core import decode_r4_frame, encode_r4_frame
from nexus_connector_core.protocol import canonical_json

from ..errors import ConnectorError


_KEY = ('server_id', 'executor_id', 'operation_id')
_PROVENANCE = (*_KEY, 'binding_id', 'agent_id', 'session_id', 'intent_hash',
               'connection_id', 'connection_generation')


class R4PublicationStore:
    def __init__(self, path: Path, *, capacity=4096):
        if type(capacity) is not int or not 1 <= capacity <= 4096:
            raise ValueError('Invalid publication capacity.')
        self.path, self.capacity = Path(path), capacity

    @classmethod
    def for_state(cls, store):
        return cls(store.path.with_suffix('.r4-publications.sqlite3'))

    @contextmanager
    def _transaction(self):
        connection = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=10)
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA synchronous=FULL')
            # Reuse freed pages after acknowledgment; bound physical growth.
            connection.execute('PRAGMA max_page_count=65536')
            connection.execute('BEGIN IMMEDIATE')
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1):
                raise ConnectorError('VERSION_INCOMPATIBLE', 'r4_publication',
                                     'The publication store requires a newer Connector.')
            connection.execute('CREATE TABLE IF NOT EXISTS publications ('
                'server_id TEXT NOT NULL, executor_id TEXT NOT NULL, operation_id TEXT NOT NULL,'
                'metadata TEXT NOT NULL, receipt TEXT, digest TEXT,'
                'PRIMARY KEY(server_id,executor_id,operation_id))')
            connection.execute('PRAGMA user_version=1')
            yield connection
            connection.commit()
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as error:
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication',
                                 'The durable publication store is unavailable.') from error
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _frame(frame, kind):
        value = decode_r4_frame(encode_r4_frame(frame))
        if value['type'] != kind:
            raise ConnectorError('VALIDATION_ERROR', 'r4_publication',
                                 'The publication frame has the wrong type.')
        return value

    def reserve(self, frame):
        frame = self._frame(frame, 'operation.submit')
        metadata = canonical_json({k: frame[k] for k in (*_PROVENANCE, 'action')}).decode()
        with self._transaction() as conn:
            key = tuple(frame[k] for k in _KEY)
            prior = conn.execute('SELECT metadata FROM publications WHERE '
                                 'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            if prior is not None:
                code = 'RECONCILIATION_REQUIRED' if prior['metadata'] == metadata else 'OPERATION_CONFLICT'
                raise ConnectorError(code, 'r4_publication',
                                     'An existing publication obligation must be reconciled.')
            if conn.execute('SELECT count(*) FROM publications').fetchone()[0] >= self.capacity:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_publication',
                                     'The durable publication capacity is exhausted.')
            conn.execute('INSERT INTO publications(server_id,executor_id,operation_id,metadata) '
                         'VALUES (?,?,?,?)', (*key, metadata))

    def record(self, frame):
        frame = self._frame(frame, 'operation.receipt')
        raw = canonical_json(frame).decode()
        digest = hashlib.sha256(raw.encode()).hexdigest()
        with self._transaction() as conn:
            key = tuple(frame[k] for k in _KEY)
            row = conn.execute('SELECT metadata,receipt,digest FROM publications WHERE '
                               'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            if (row is None or any(json.loads(row['metadata'])[k] != frame[k] for k in _PROVENANCE)
                    or (row['receipt'] is not None and (row['receipt'] != raw or row['digest'] != digest))):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The receipt does not match its publication obligation.')
            conn.execute('UPDATE publications SET receipt=?,digest=? WHERE '
                         'server_id=? AND executor_id=? AND operation_id=?', (raw, digest, *key))

    def acknowledge(self, frame):
        frame = self._frame(frame, 'operation.receipt')
        raw = canonical_json(frame).decode()
        with self._transaction() as conn:
            changed = conn.execute('DELETE FROM publications WHERE server_id=? AND executor_id=? '
                'AND operation_id=? AND receipt=? AND digest=?',
                (*(frame[k] for k in _KEY), raw, hashlib.sha256(raw.encode()).hexdigest())).rowcount
            if changed != 1:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The acknowledgment does not match the pending receipt.')

    def ready(self, server_id, executor_id, *, limit=128):
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError('Invalid publication page size.')
        with self._transaction() as conn:
            rows = conn.execute('SELECT operation_id,metadata,receipt,digest FROM publications WHERE server_id=? '
                'AND executor_id=? AND receipt IS NOT NULL ORDER BY operation_id LIMIT ?',
                (server_id, executor_id, limit)).fetchall()
            result = []
            for row in rows:
                if hashlib.sha256(row['receipt'].encode()).hexdigest() != row['digest']:
                    raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication',
                                         'The stored receipt failed its integrity check.')
                frame = self._frame(json.loads(row['receipt']), 'operation.receipt')
                if ((frame['server_id'], frame['executor_id'], frame['operation_id']) !=
                        (server_id, executor_id, row['operation_id']) or
                        any(json.loads(row['metadata'])[k] != frame[k] for k in _PROVENANCE)):
                    raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication',
                                         'The stored receipt has inconsistent provenance.')
                result.append(frame)
            return result

    def pending(self, server_id, executor_id):
        with self._transaction() as conn:
            return conn.execute('SELECT 1 FROM publications WHERE server_id=? AND executor_id=? LIMIT 1',
                                (server_id, executor_id)).fetchone() is not None
