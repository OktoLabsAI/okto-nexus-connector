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
from threading import Lock

from nexus_connector_core import (
    CoreError, R4ReceiptProjection, decode_r4_frame, encode_r4_frame,
    validate_r4_receipt_binding, reduce_r4_receipt,
    validate_r4_operation_refusal,
    project_r4_bound_receipt, OperationKey,
)
from nexus_connector_core.preopen_refusal_r4 import validate_r4_preopen_refusal
from nexus_connector_core.startup_refusal_link import decode_startup_refusal, encode_startup_refusal
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
        self._schema_lock = Lock()
        self._schema_ready = False

    @classmethod
    def for_state(cls, store):
        return cls(store.path.with_suffix('.r4-publications.sqlite3'))

    def _ensure_schema(self):
        # Schema changes need a writer lock once, not on every publication read.
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            connection = None
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                connection = sqlite3.connect(self.path, timeout=10)
                connection.row_factory = sqlite3.Row
                connection.execute('PRAGMA synchronous=FULL')
                connection.execute('PRAGMA max_page_count=65536')
                connection.execute('BEGIN IMMEDIATE')
                version = connection.execute('PRAGMA user_version').fetchone()[0]
                if version not in (0, 1, 2, 3, 4, 5, 6):
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
                if version < 4:
                    connection.execute('ALTER TABLE publications ADD COLUMN refusal_binding TEXT')
                if version < 5:
                    connection.execute('ALTER TABLE publications ADD COLUMN preopen_refusal TEXT')
                    connection.execute('ALTER TABLE publications ADD COLUMN preopen_state TEXT')
                if version < 6:
                    connection.execute('ALTER TABLE publications ADD COLUMN startup_report TEXT')
                    connection.execute('ALTER TABLE publications ADD COLUMN startup_prelease INTEGER NOT NULL DEFAULT 0')
                connection.execute('PRAGMA user_version=6')
                connection.execute("CREATE INDEX IF NOT EXISTS publication_sessions ON publications "
                    "(server_id,executor_id,json_extract(metadata,'$.session_id'),json_extract(metadata,'$.action'))")
                connection.commit()
                self._schema_ready = True
            finally:
                if connection is not None:
                    connection.close()

    @contextmanager
    def _transaction(self, *, write=True):
        connection = None
        try:
            self._ensure_schema()
            connection = sqlite3.connect(self.path, timeout=10)
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA synchronous=FULL')
            if not write:
                connection.execute('PRAGMA query_only=ON')
            connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
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
            if stored['preopen_state'] == 'FENCED' and stored['receipt'] != frame:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication', 'A fenced opening cannot acquire another outcome.')
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

    def record_startup(self, report):
        report = decode_startup_refusal(encode_startup_refusal(report))
        if report['type'] != 'startup.refuse':
            raise ValueError('A startup refusal request is required.')
        refusal, frame = report['refusal'], report['receipt']
        raw_report = canonical_json(report).decode()
        raw_receipt = canonical_json(frame).decode()
        key = tuple(frame[k] for k in _KEY)
        with self._transaction() as conn:
            row = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? '
                'AND operation_id=?', key).fetchone()
            stored = self._decode(row) if row is not None else None
            if (stored is None or stored['refusal'] != refusal
                    or stored['metadata']['action'] != 'runtime.open' or not row['startup_prelease']
                    or any(stored['metadata'][k] != frame[k] for k in _PROVENANCE)
                    or row['startup_report'] is not None and (
                        stored['startup_report']['request_id'] != report['request_id']
                        or stored['startup_report']['refusal'] != refusal
                        or stored['startup_report']['receipt'] != frame
                        or stored['startup_report']['capacity'] != report['capacity']
                        or stored['startup_report']['connection_generation'] > report['connection_generation']
                        or stored['startup_report']['connection_generation'] == report['connection_generation']
                        and stored['startup_report']['connection_id'] != report['connection_id'])
                    or row['receipt'] not in (None, raw_receipt)):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_startup_refusal',
                    'The durable startup refusal changed.')
            conn.execute('UPDATE publications SET receipt=?,digest=?,startup_report=?,acknowledged=0 '
                'WHERE server_id=? AND executor_id=? AND operation_id=?',
                (raw_receipt, hashlib.sha256(raw_receipt.encode()).hexdigest(), raw_report, *key))

    def startup_page(self, server_id, executor_id, *, connection_id, generation, limit=128):
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError('Invalid startup refusal page size.')
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? '
                'AND acknowledged=0 AND startup_report IS NOT NULL '
                "AND json_extract(metadata,'$.connection_id')=? "
                "AND json_extract(metadata,'$.connection_generation')=? "
                'ORDER BY operation_id LIMIT ?',
                (server_id, executor_id, connection_id, generation, limit)).fetchall()
            return [self._decode(row)['startup_report'] for row in rows]

    def startup_recovery_page(self, server_id, executor_id, *, generation, after='', limit=128):
        if type(generation) is not int or generation < 1 or type(after) is not str or not 1 <= limit <= 128:
            raise ValueError('Invalid startup recovery page.')
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? '
                "AND operation_id>? AND acknowledged=0 AND startup_prelease=1 "
                "AND json_extract(metadata,'$.connection_generation')<? "
                'ORDER BY operation_id LIMIT ?',
                (server_id, executor_id, after, generation, limit)).fetchall()
            return [self._decode(row) for row in rows]

    def recovered_startup_sessions(self, server_id, executor_id, *, generation, session_ids):
        if (not isinstance(session_ids, (tuple, list)) or len(session_ids) > 256
                or any(type(value) is not str for value in session_ids)):
            raise ValueError('Invalid recovered startup session page.')
        with self._transaction(write=False) as conn:
            return {session_id for session_id in session_ids if conn.execute(
                "SELECT 1 FROM publications WHERE server_id=? AND executor_id=? "
                "AND json_extract(metadata,'$.session_id')=? AND acknowledged=1 "
                "AND startup_prelease=1 AND json_extract(metadata,'$.connection_generation')<? LIMIT 1",
                (server_id, executor_id, session_id, generation)).fetchone() is not None}

    def bind(self, binding, *, stream_epoch=None, refusal=None, startup_prelease=False):
        binding = validate_r4_receipt_binding(binding)
        refusal = validate_r4_operation_refusal(refusal) if refusal is not None else None
        if refusal is not None and (refusal['binding'] != binding or stream_epoch is not None):
            raise ValueError('Invalid refusal association.')
        if startup_prelease and refusal is None:
            raise ValueError('A prelease refusal needs its immutable association.')
        refusal_raw = canonical_json(refusal).decode() if refusal is not None else None
        source = binding['source']
        raw = canonical_json(binding).decode()
        with self._transaction() as conn:
            key = tuple(source[k] for k in _KEY)
            row = conn.execute('SELECT * FROM publications WHERE '
                               'server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            stored = self._decode(row) if row is not None else None
            if (stored is None or stored['metadata']['action'] != binding['action'] or
                    any(stored['metadata'][k] != source[k] for k in _PROVENANCE) or
                    stored['preopen_refusal'] is not None and binding != stored['binding'] or
                    row['projection_binding'] not in (None, raw)
                    or row['refusal_binding'] not in (None, refusal_raw)
                    or row['startup_prelease'] and not startup_prelease
                    or refusal is not None and (
                        stored['metadata']['session_owner_generation'] != refusal['scope']['session_owner_generation']
                        or row['receipt'] is not None and row['refusal_binding'] != refusal_raw)):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_publication',
                                     'The Core receipt binding conflicts with its reserved operation.')
            conn.execute('UPDATE publications SET projection_binding=?,refusal_binding=?,startup_prelease=? WHERE '
                         'server_id=? AND executor_id=? AND operation_id=?',
                         (raw, refusal_raw, int(startup_prelease), *key))
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
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications '
                'WHERE server_id=? AND executor_id=? AND operation_id>? AND receipt IS NULL '
                "AND projection_binding IS NOT NULL AND coalesce(preopen_state,'')<>'PREPARED' ORDER BY operation_id LIMIT ?",
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
        with self._transaction(write=False) as conn:
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
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? '
                "AND executor_id=? AND acknowledged=0 AND receipt IS NOT NULL "
                "AND startup_prelease=0 "
                "AND coalesce(preopen_state,'')<>'FENCED' ORDER BY operation_id LIMIT ?",
                (server_id, executor_id, limit)).fetchall()
            return [self._decode(row)['receipt'] for row in rows]

    def pending(self, server_id, executor_id):
        with self._transaction(write=False) as conn:
            return conn.execute('SELECT 1 FROM publications WHERE server_id=? AND executor_id=? AND acknowledged=0 LIMIT 1',
                                (server_id, executor_id)).fetchone() is not None

    def pending_unfenced(self, server_id, executor_id):
        with self._transaction(write=False) as conn:
            return conn.execute("SELECT 1 FROM publications WHERE server_id=? AND executor_id=? "
                "AND acknowledged=0 AND coalesce(preopen_state,'')<>'FENCED' LIMIT 1",
                (server_id, executor_id)).fetchone() is not None

    def preopen_page(self, server_id, executor_id, *, generation, after='', limit=128):
        if type(generation) is not int or generation < 1 or type(after) is not str or type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError('Invalid preopening recovery page.')
        with self._transaction(write=False) as conn:
            rows = conn.execute("SELECT * FROM publications WHERE server_id=? AND executor_id=? "
                "AND operation_id>? AND acknowledged=0 AND json_extract(metadata,'$.action')='runtime.open' "
                "AND json_extract(metadata,'$.connection_generation')<? AND refusal_binding IS NULL "
                "AND (receipt IS NULL OR preopen_state='FENCED') AND coalesce(preopen_state,'')<>'CONFLICT' "
                "ORDER BY operation_id LIMIT ?", (server_id, executor_id, after, generation, limit)).fetchall()
            return [self._decode(row) for row in rows]

    def prepare_preopen(self, refusal):
        refusal = validate_r4_preopen_refusal(refusal)
        source = refusal['source']
        raw = canonical_json(refusal).decode()
        with self._transaction() as conn:
            key = tuple(source[k] for k in _KEY)
            row = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            stored = self._decode(row) if row is not None else None
            if (stored is None or stored['metadata']['action'] != 'runtime.open'
                    or any(stored['metadata'][k] != source[k] for k in _PROVENANCE)
                    or stored['metadata']['session_owner_generation'] != refusal['session_owner_generation']
                    or stored['refusal'] is not None or stored['preopen_state'] == 'CONFLICT'):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'The opening provenance changed.')
            if stored['preopen_refusal'] is not None:
                if stored['preopen_refusal'] != refusal:
                    raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'The persisted recovery association changed.')
                return
            if stored['receipt'] is not None or stored['binding'] != refusal['prior_binding']:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'The opening already has another fact or binding.')
            conn.execute("UPDATE publications SET preopen_refusal=?,preopen_state='PREPARED' WHERE "
                         'server_id=? AND executor_id=? AND operation_id=?', (raw, *key))

    def fence_preopen(self, refusal, fact):
        refusal = validate_r4_preopen_refusal(refusal)
        source = refusal['source']
        key = tuple(source[k] for k in _KEY)
        frame = project_r4_bound_receipt(refusal['binding'], fact, key=OperationKey(*key), receipt_revision=1)
        if (frame['stage'] != 'FAILED' or frame['possible_effect'] or not frame['retry_safe']
                or frame.get('native_id') is not None or frame.get('error_code') != 'PRE_EXECUTION_FENCED'):
            raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'The Core fact is not a confirmed preopening refusal.')
        with self._transaction() as conn:
            row = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? AND operation_id=?', key).fetchone()
            stored = self._decode(row) if row is not None else None
            if stored is None or stored['preopen_refusal'] != refusal or stored['preopen_state'] not in ('PREPARED', 'FENCED'):
                raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'No matching recovery association was persisted.')
            if stored['preopen_state'] == 'FENCED':
                if stored['receipt'] != frame:
                    raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'The refusal outcome changed.')
                return frame
            if stored['receipt'] is not None:
                raise ConnectorError('OPERATION_CONFLICT', 'r4_preopen', 'A native outcome already exists.')
            raw = canonical_json(frame).decode()
            conn.execute("UPDATE publications SET projection_binding=?,receipt=?,digest=?,preopen_state='FENCED' "
                'WHERE server_id=? AND executor_id=? AND operation_id=?',
                (canonical_json(refusal['binding']).decode(), raw, hashlib.sha256(raw.encode()).hexdigest(), *key))
            return frame

    def conflict_preopen(self, refusal):
        refusal = validate_r4_preopen_refusal(refusal)
        with self._transaction() as conn:
            conn.execute("UPDATE publications SET preopen_state='CONFLICT' WHERE server_id=? AND executor_id=? "
                "AND operation_id=? AND preopen_state='PREPARED' AND preopen_refusal=?",
                (*(refusal['source'][k] for k in _KEY), canonical_json(refusal).decode()))

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
        refusal = validate_r4_operation_refusal(json.loads(row['refusal_binding'])) if row['refusal_binding'] else None
        if refusal is not None and (refusal['binding'] != binding or
                refusal['scope']['session_owner_generation'] != metadata['session_owner_generation']):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_publication', 'The refusal association is inconsistent.')
        preopen = validate_r4_preopen_refusal(json.loads(row['preopen_refusal'])) if row['preopen_refusal'] else None
        startup_report = decode_startup_refusal(row['startup_report'].encode()) if row['startup_report'] else None
        if startup_report is not None and not row['startup_prelease']:
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_startup_refusal',
                'The startup report has no prelease marker.')
        if startup_report is not None and (startup_report['type'] != 'startup.refuse'
                or startup_report['refusal'] != refusal or startup_report['receipt'] != receipt):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_startup_refusal',
                'The stored startup refusal association is inconsistent.')
        preopen_state = row['preopen_state']
        if (preopen is None) != (preopen_state is None) or preopen is not None and (
                preopen_state not in ('PREPARED', 'FENCED', 'CONFLICT') or refusal is not None
                or any(preopen['source'][k] != metadata[k] for k in _PROVENANCE)
                or preopen['session_owner_generation'] != metadata['session_owner_generation']
                or binding != (preopen['binding'] if preopen_state == 'FENCED' else preopen['prior_binding'])
                or preopen_state == 'FENCED' and receipt is None):
            raise ConnectorError('JOURNAL_UNAVAILABLE', 'r4_preopen', 'The stored preopening recovery association is inconsistent.')
        return dict(metadata=metadata, receipt=receipt, binding=binding,
                    acknowledged=bool(row['acknowledged']), refusal=refusal,
                    preopen_refusal=preopen, preopen_state=preopen_state,
                    startup_report=startup_report, startup_prelease=bool(row['startup_prelease']))

    def refusal_page(self, server_id, executor_id, *, after='', limit=128):
        if type(after) is not str or type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError('Invalid refusal page cursor.')
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? '
                'AND operation_id>? AND refusal_binding IS NOT NULL AND receipt IS NULL '
                'ORDER BY operation_id LIMIT ?', (server_id, executor_id, after, limit)).fetchall()
            return [self._decode(row)['refusal'] for row in rows]

    def lookup(self, server_id, executor_id, operation_ids):
        if not isinstance(operation_ids, (tuple, list)) or len(operation_ids) > 256 or any(type(i) is not str for i in operation_ids):
            raise ValueError('Invalid operation lookup page.')
        with self._transaction(write=False) as conn:
            result = {}
            for operation_id in operation_ids:
                row = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? AND operation_id=?',
                                   (server_id, executor_id, operation_id)).fetchone()
                if row is not None:
                    result[operation_id] = self._decode(row)
            return result

    def session_history(self, server_id, executor_id, session_id):
        with self._transaction(write=False) as conn:
            rows = conn.execute("SELECT * FROM publications WHERE server_id=? AND executor_id=? "
                "AND json_extract(metadata,'$.session_id')=? AND json_extract(metadata,'$.action') "
                "IN ('runtime.open','runtime.close') ORDER BY operation_id LIMIT 257",
                (server_id, executor_id, session_id)).fetchall()
            if len(rows) > 256:
                raise ConnectorError('CAPACITY_EXCEEDED', 'r4_reconcile', 'Session reconciliation requires another history page.')
            return [self._decode(row) for row in rows]

    def session_page(self, server_id, executor_id, session_id, *, after='', limit=128):
        """All actions for one session, with bounded memory and stable ordering."""
        if type(after) is not str or type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError('Invalid session publication cursor.')
        with self._transaction(write=False) as conn:
            rows = conn.execute('SELECT * FROM publications WHERE server_id=? AND executor_id=? '
                "AND json_extract(metadata,'$.session_id')=? AND operation_id>? ORDER BY operation_id LIMIT ?",
                (server_id,executor_id,session_id,after,limit)).fetchall()
            return [self._decode(row) for row in rows]
