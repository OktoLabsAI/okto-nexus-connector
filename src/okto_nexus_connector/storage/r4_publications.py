"""Durable publication obligations, never an authority to repeat an effect.

Reserve before invoking Core. A reservation without a projected receipt stays
unresolved after a crash. Only a correlated durable Server acknowledgment can
acknowledge a ready receipt. Confirmed associations remain available for reconciliation. Raw operation payloads and credentials are not stored.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3

from nexus_connector_core import (
    CoreError, R4ReceiptProjection, decode_r4_frame, encode_r4_frame,
    validate_r4_receipt_binding, reduce_r4_receipt,
)
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
            # Acknowledged associations remain queryable; bound physical growth.
            connection.execute('PRAGMA max_page_count=65536')
            connection.execute('BEGIN IMMEDIATE')
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, 2, 3):
                raise ConnectorError('VERSION_INCOMPATIBLE', 'r4_publication',
                                     'The publication store requires a newer Connector.')
            connection.execute('CREATE TABLE IF NOT EXISTS publications ('
                'server_id TEXT NOT NULL, executor_id TEXT NOT NULL, operation_id TEXT NOT NULL,'
                'metadata TEXT NOT NULL, receipt TEXT, digest TEXT,'
                'PRIMARY KEY(server_id,executor_id,operation_id))')
            if version < 2:
                connection.execute('ALTER TABLE publications ADD COLUMN projection_binding TEXT')
            if version < 3:
                connection.execute('ALTER TABLE publications ADD COLUMN acknowledged INTEGER NOT NULL DEFAULT 0')
                connection.execute('ALTER TABLE publications ADD COLUMN metadata_digest TEXT')
                for row in connection.execute('SELECT rowid,metadata FROM publications').fetchall():
                    connection.execute('UPDATE publications SET metadata_digest=? WHERE rowid=?',
                                       (hashlib.sha256(row['metadata'].encode()).hexdigest(), row['rowid']))
            connection.execute('PRAGMA user_version=3')
            connection.execute("CREATE INDEX IF NOT EXISTS publication_sessions ON publications "
                "(server_id,executor_id,json_extract(metadata,'$.session_id'),json_extract(metadata,'$.action'))")
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
        metadata = canonical_json({k: frame[k] for k in (*_PROVENANCE, 'action', 'session_owner_generation')}).decode()
        with self._transaction() as conn:
            key = tuple(frame[k] for k in _KEY)
            prior = conn.execute('SELECT metadata,acknowledged FROM publications WHERE '
                                 'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            if prior is not None:
                code = 'RECONCILIATION_REQUIRED' if prior['metadata'] == metadata and not prior['acknowledged'] else 'OPERATION_CONFLICT'
                raise ConnectorError(code, 'r4_publication',
                                     'An existing publication obligation must be reconciled.')
            if conn.execute('SELECT count(*) FROM publications WHERE acknowledged=0').fetchone()[0] >= self.capacity:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_publication',
                                     'The durable publication capacity is exhausted.')
            conn.execute('INSERT INTO publications(server_id,executor_id,operation_id,metadata,metadata_digest) '
                         'VALUES (?,?,?,?,?)', (*key, metadata, hashlib.sha256(metadata.encode()).hexdigest()))

    def record(self, frame):
        frame = self._frame(frame, 'operation.receipt')
        raw = canonical_json(frame).decode()
        digest = hashlib.sha256(raw.encode()).hexdigest()
        with self._transaction() as conn:
            key = tuple(frame[k] for k in _KEY)
            row = conn.execute('SELECT * FROM publications WHERE '
                               'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            if row is None:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The receipt does not match its publication obligation.')
            stored = self._decode(row)
            if any(stored['metadata'][k] != frame[k] for k in _PROVENANCE):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The receipt does not match its publication obligation.')
            previous = stored['receipt']
            if previous == frame:
                return
            if previous is not None:
                projection = R4ReceiptProjection(*(previous[k] for k in (
                    'server_id','executor_id','binding_id','agent_id','session_id','operation_id','intent_hash',
                    'receipt_revision','stage','possible_effect','retry_safe')),
                    previous.get('native_id'), previous['connection_id'], previous['connection_generation'],
                    ((previous['receipt_revision'], 'sha256:' + row['digest']),))
                try:
                    reduce_r4_receipt(projection, frame)
                except CoreError as error:
                    raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                         'The receipt revision does not advance its stored fact.') from error
            if row['acknowledged'] and conn.execute('SELECT count(*) FROM publications WHERE acknowledged=0').fetchone()[0] >= self.capacity:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_publication', 'The durable publication capacity is exhausted.')
            conn.execute('UPDATE publications SET receipt=?,digest=?,acknowledged=0 WHERE '
                         'server_id=? AND executor_id=? AND operation_id=?', (raw, digest, *key))

    def bind(self, binding, *, stream_epoch=None):
        binding = validate_r4_receipt_binding(binding)
        source = binding['source']
        raw = canonical_json(binding).decode()
        with self._transaction() as conn:
            key = tuple(source[k] for k in _KEY)
            row = conn.execute('SELECT * FROM publications WHERE '
                               'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            stored = self._decode(row) if row is not None else None
            if (stored is None or stored['metadata']['action'] != binding['action'] or
                    any(stored['metadata'][k] != source[k] for k in _PROVENANCE) or
                    row['projection_binding'] not in (None, raw)):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The Core receipt binding conflicts with its reserved operation.')
            conn.execute('UPDATE publications SET projection_binding=? WHERE '
                         'server_id=? AND executor_id=? AND operation_id=?', (raw, *key))
            if stream_epoch is not None:
                if binding['action'] != 'runtime.open' or type(stream_epoch) is not str or not 1 <= len(stream_epoch) <= 160:
                    raise ValueError('Invalid opening stream epoch.')
                metadata = stored['metadata']
                if metadata.get('stream_epoch') not in (None, stream_epoch):
                    raise ConnectorError('OPERATION_CONFLICT', 'r4_publication', 'The opening stream changed.')
                metadata['stream_epoch'] = stream_epoch
                encoded = canonical_json(metadata).decode()
                conn.execute('UPDATE publications SET metadata=?,metadata_digest=? WHERE '
                    'server_id=? AND executor_id=? AND operation_id=?',
                    (encoded, hashlib.sha256(encoded.encode()).hexdigest(), *key))

    def unprojected(self, server_id, executor_id, *, after='', limit=128):
        if type(limit) is not int or not 1 <= limit <= 256 or type(after) is not str:
            raise ValueError('Invalid publication page cursor.')
        with self._transaction() as conn:
            rows = conn.execute('SELECT * FROM publications '
                'WHERE server_id=? AND executor_id=? AND operation_id>? AND receipt IS NULL '
                'AND projection_binding IS NOT NULL ORDER BY operation_id LIMIT ?',
                (server_id, executor_id, after, limit)).fetchall()
            return [self._decode(row)['binding'] for row in rows]

    def watching(self, server_id, executor_id, *, after="", limit=128, connection=None):
        """Page acknowledged nonterminal facts; acknowledgment is not completion."""
        if type(limit) is not int or not 1 <= limit <= 256 or type(after) is not str:
            raise ValueError("Invalid receipt observation cursor.")
        clause, values = "", [server_id, executor_id, after]
        if connection is not None:
            connection_id, generation = connection
            clause = " AND json_extract(metadata,'$.connection_id')=? AND json_extract(metadata,'$.connection_generation')=?"
            values += [connection_id, generation]
        with self._transaction() as conn:
            rows = conn.execute("SELECT * FROM publications WHERE server_id=? AND executor_id=? "
                "AND operation_id>? AND acknowledged=1 AND projection_binding IS NOT NULL AND receipt IS NOT NULL "
                "AND json_extract(receipt,'$.stage') NOT IN ('SUCCEEDED','FAILED','CANCELLED')" + clause +
                " ORDER BY operation_id LIMIT ?", (*values, limit)).fetchall()
            return [self._decode(row) for row in rows]

    def acknowledge(self, frame):
        frame = self._frame(frame, 'operation.receipt')
        raw = canonical_json(frame).decode()
        with self._transaction() as conn:
            changed = conn.execute('UPDATE publications SET acknowledged=1 WHERE server_id=? AND executor_id=? '
                'AND operation_id=? AND receipt=? AND digest=?',
                (*(frame[k] for k in _KEY), raw, hashlib.sha256(raw.encode()).hexdigest())).rowcount
            if changed != 1:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The acknowledgment does not match the pending receipt.')

    def ready(self, server_id, executor_id, *, limit=128):
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError('Invalid publication page size.')
        with self._transaction() as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? '
                'AND executor_id=? AND acknowledged=0 AND receipt IS NOT NULL ORDER BY operation_id LIMIT ?',
                (server_id, executor_id, limit)).fetchall()
            return [self._decode(row)['receipt'] for row in rows]

    def pending(self, server_id, executor_id):
        with self._transaction() as conn:
            return conn.execute('SELECT 1 FROM publications WHERE server_id=? AND executor_id=? AND acknowledged=0 LIMIT 1',
                                (server_id, executor_id)).fetchone() is not None

    def _decode(self, row):
        if hashlib.sha256(row['metadata'].encode()).hexdigest() != row['metadata_digest']:
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The stored metadata failed its integrity check.')
        metadata = json.loads(row['metadata'])
        if tuple(metadata[k] for k in _KEY) != tuple(row[k] for k in _KEY):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The stored operation scope is inconsistent.')
        receipt = json.loads(row['receipt']) if row['receipt'] is not None else None
        if receipt is not None:
            if hashlib.sha256(row['receipt'].encode()).hexdigest() != row['digest']:
                raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The stored receipt failed its integrity check.')
            self._frame(receipt, 'operation.receipt')
            if any(receipt[k] != metadata[k] for k in _PROVENANCE):
                raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The receipt provenance is inconsistent.')
        binding = validate_r4_receipt_binding(json.loads(row['projection_binding'])) if row['projection_binding'] else None
        if binding is not None and (binding['action'] != metadata['action'] or
                any(binding['source'][k] != metadata[k] for k in _PROVENANCE)):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The Core association is inconsistent.')
        return dict(metadata=metadata, receipt=receipt, binding=binding, acknowledged=bool(row['acknowledged']))

    def lookup(self, server_id, executor_id, operation_ids):
        if not isinstance(operation_ids, (tuple, list)) or len(operation_ids) > 256 or any(type(i) is not str for i in operation_ids):
            raise ValueError('Invalid operation lookup page.')
        with self._transaction() as conn:
            result = {}
            for operation_id in operation_ids:
                row = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? AND operation_id=?',
                                   (server_id, executor_id, operation_id)).fetchone()
                if row is not None:
                    result[operation_id] = self._decode(row)
            return result

    def session_history(self, server_id, executor_id, session_id):
        with self._transaction() as conn:
            rows = conn.execute("SELECT * FROM publications WHERE server_id=? AND executor_id=? "
                "AND json_extract(metadata,'$.session_id')=? AND json_extract(metadata,'$.action') "
                "IN ('runtime.open','runtime.close') ORDER BY operation_id LIMIT 257",
                (server_id, executor_id, session_id)).fetchall()
            if len(rows) > 256:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_reconcile', 'Session reconciliation requires another history page.')
            return [self._decode(row) for row in rows]
