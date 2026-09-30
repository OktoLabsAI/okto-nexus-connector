"""Durable stream identity and separate remote/Core acknowledgment cursors."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
from nexus_connector_core.protocol import canonical_json
from ..errors import ConnectorError

FIELDS = ("server_id","executor_id","binding_id","agent_id","session_id","stream_epoch")


class R4EventStore:
    def __init__(self, path):
        self.path = Path(path)

    @classmethod
    def for_state(cls, store):
        return cls(store.path.with_suffix(".r4-events.sqlite3"))

    @contextmanager
    def _transaction(self):
        conn = None
        try:
            self.path.parent.mkdir(parents=True,exist_ok=True)
            conn = sqlite3.connect(self.path,timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA max_page_count=4096")
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("PRAGMA user_version").fetchone()[0] not in (0,1):
                raise ConnectorError("VERSION_INCOMPATIBLE","r4_events","The event store requires a newer Connector.")
            conn.execute("CREATE TABLE IF NOT EXISTS streams ("
                "server_id TEXT NOT NULL,executor_id TEXT NOT NULL,session_id TEXT NOT NULL,stream_epoch TEXT NOT NULL,"
                "metadata TEXT NOT NULL,digest TEXT NOT NULL,remote_acked INTEGER NOT NULL DEFAULT 0,"
                "core_applied INTEGER NOT NULL DEFAULT 0,CHECK (remote_acked>=core_applied AND core_applied>=0),"
                "PRIMARY KEY(server_id,executor_id,session_id,stream_epoch))")
            conn.execute("PRAGMA user_version=1")
            yield conn
            conn.commit()
        except (OSError,sqlite3.Error,ValueError,KeyError,TypeError) as error:
            raise ConnectorError("JOURNAL_UNAVAILABLE","r4_events","The durable event store is unavailable.") from error
        finally:
            if conn is not None: conn.close()

    @staticmethod
    def key(scope):
        return tuple(scope[k] for k in ("server_id","executor_id","session_id","stream_epoch"))

    def register(self, scope):
        metadata = {k:scope[k] for k in FIELDS}
        if any(type(v) is not str or not 1 <= len(v) <= 160 for v in metadata.values()):
            raise ValueError("Invalid event stream identity.")
        raw = canonical_json(metadata).decode()
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM streams WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",
                               self.key(scope)).fetchone()
            if row is not None:
                if self._decode(row)["scope"] != metadata:
                    raise ConnectorError("SCOPE_MISMATCH","r4_events","The stored stream identity changed.")
                return
            if conn.execute("SELECT count(*) FROM streams").fetchone()[0]>=4096:
                raise ConnectorError("CAPACITY_EXCEEDED","r4_events","The durable event stream capacity is exhausted.")
            conn.execute("INSERT INTO streams(server_id,executor_id,session_id,stream_epoch,metadata,digest) VALUES (?,?,?,?,?,?)",
                         (*self.key(scope),raw,hashlib.sha256(raw.encode()).hexdigest()))

    def _decode(self, row):
        if row is None or hashlib.sha256(row["metadata"].encode()).hexdigest()!=row["digest"]:
            raise ConnectorError("JOURNAL_UNAVAILABLE","r4_events","The stored event stream failed its integrity check.")
        scope=json.loads(row["metadata"])
        if self.key(scope)!=tuple(row[k] for k in ("server_id","executor_id","session_id","stream_epoch")):
            raise ConnectorError("JOURNAL_UNAVAILABLE","r4_events","The stored event stream has inconsistent scope.")
        return dict(scope=scope,remote_acked=row["remote_acked"],core_applied=row["core_applied"])

    def read(self, scope):
        with self._transaction() as conn:
            value=self._decode(conn.execute("SELECT * FROM streams WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",
                                           self.key(scope)).fetchone())
            if value["scope"]!={k:scope[k] for k in FIELDS}:
                raise ConnectorError("SCOPE_MISMATCH","r4_events","The event stream scope changed.")
            return value

    def advance(self, scope, *, remote_acked=None, core_applied=None):
        with self._transaction() as conn:
            row=self._decode(conn.execute("SELECT * FROM streams WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",
                                         self.key(scope)).fetchone())
            if row["scope"]!={k:scope[k] for k in FIELDS}:
                raise ConnectorError("SCOPE_MISMATCH","r4_events","The event stream scope changed.")
            remote=row["remote_acked"] if remote_acked is None else remote_acked
            applied=row["core_applied"] if core_applied is None else core_applied
            if any(type(v) is not int or not 0<=v<=9007199254740991 for v in (remote,applied)) or not (
                    remote>=row["remote_acked"] and applied>=row["core_applied"] and remote>=applied):
                raise ValueError("Invalid event acknowledgment progression.")
            conn.execute("UPDATE streams SET remote_acked=?,core_applied=? WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",
                         (remote,applied,*self.key(scope)))
