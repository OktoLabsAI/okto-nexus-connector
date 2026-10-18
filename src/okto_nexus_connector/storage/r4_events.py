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
            # Terminal checkpoints remain durable after live stream rows are
            # retired. Bound the physical store without making 4096 sessions
            # a lifetime limit for an otherwise healthy Connector.
            conn.execute("PRAGMA max_page_count=131072")
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("PRAGMA user_version").fetchone()[0] not in (0,1,2):
                raise ConnectorError("VERSION_INCOMPATIBLE","r4_events","The event store requires a newer Connector.")
            conn.execute("CREATE TABLE IF NOT EXISTS streams ("
                "server_id TEXT NOT NULL,executor_id TEXT NOT NULL,session_id TEXT NOT NULL,stream_epoch TEXT NOT NULL,"
                "metadata TEXT NOT NULL,digest TEXT NOT NULL,remote_acked INTEGER NOT NULL DEFAULT 0,"
                "core_applied INTEGER NOT NULL DEFAULT 0,CHECK (remote_acked>=core_applied AND core_applied>=0),"
                "PRIMARY KEY(server_id,executor_id,session_id,stream_epoch))")
            conn.execute("CREATE TABLE IF NOT EXISTS terminal_streams ("
                "server_id TEXT NOT NULL,executor_id TEXT NOT NULL,session_id TEXT NOT NULL,stream_epoch TEXT NOT NULL,"
                "metadata TEXT NOT NULL,digest TEXT NOT NULL,remote_acked INTEGER NOT NULL,"
                "core_applied INTEGER NOT NULL,release_id TEXT NOT NULL,release_digest TEXT NOT NULL,"
                "CHECK(remote_acked=core_applied AND core_applied>=0),"
                "PRIMARY KEY(server_id,executor_id,session_id,stream_epoch))")
            conn.execute("PRAGMA user_version=2")
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
            terminal = conn.execute("SELECT * FROM terminal_streams WHERE server_id=? AND executor_id=? "
                "AND session_id=? AND stream_epoch=?",self.key(scope)).fetchone()
            if terminal is not None:
                if self._decode(terminal)["scope"] != metadata:
                    raise ConnectorError("SCOPE_MISMATCH","r4_events","The stored stream identity changed.")
                raise ConnectorError("STALE_GENERATION","r4_events","A terminal stream cannot be registered again.")
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
            row=conn.execute("SELECT * FROM streams WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",
                             self.key(scope)).fetchone()
            if row is None:
                row=conn.execute("SELECT * FROM terminal_streams WHERE server_id=? AND executor_id=? "
                    "AND session_id=? AND stream_epoch=?",self.key(scope)).fetchone()
            value=self._decode(row)
            if value["scope"]!={k:scope[k] for k in FIELDS}:
                raise ConnectorError("SCOPE_MISMATCH","r4_events","The event stream scope changed.")
            return value

    def retire(self, scope, *, sequence, release_id, release_digest):
        """Keep final ACK evidence while removing a Server-accepted stream from replay."""
        metadata={k:scope[k] for k in FIELDS}
        if (type(sequence) is not int or sequence<0 or type(release_id) is not str
                or not 1<=len(release_id)<=160 or type(release_digest) is not str
                or len(release_digest)!=64 or any(c not in '0123456789abcdef' for c in release_digest)):
            raise ValueError('Invalid terminal stream checkpoint.')
        with self._transaction() as conn:
            key=self.key(scope)
            terminal=conn.execute("SELECT * FROM terminal_streams WHERE server_id=? AND executor_id=? "
                "AND session_id=? AND stream_epoch=?",key).fetchone()
            if terminal is not None:
                if (self._decode(terminal)['scope']!=metadata or terminal['remote_acked']!=sequence
                        or terminal['release_id']!=release_id or terminal['release_digest']!=release_digest):
                    raise ConnectorError('SCOPE_MISMATCH','r4_events','The terminal stream checkpoint changed.')
                return
            row=conn.execute("SELECT * FROM streams WHERE server_id=? AND executor_id=? "
                "AND session_id=? AND stream_epoch=?",key).fetchone()
            state=self._decode(row)
            if state['scope']!=metadata:
                raise ConnectorError('SCOPE_MISMATCH','r4_events','The stored stream identity changed.')
            if state['remote_acked']!=sequence or state['core_applied']!=sequence:
                raise ConnectorError('RECONCILIATION_REQUIRED','r4_events','The stream ACK is not complete.')
            conn.execute("INSERT INTO terminal_streams VALUES (?,?,?,?,?,?,?,?,?,?)",
                (*key,row['metadata'],row['digest'],sequence,sequence,release_id,release_digest))
            conn.execute("DELETE FROM streams WHERE server_id=? AND executor_id=? AND session_id=? AND stream_epoch=?",key)

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


    def page(self, server_id, executor_id, *, after=0, high=None, limit=128):
        if type(after) is not int or after<0 or type(limit) is not int or not 1<=limit<=128 or (
                high is not None and (type(high) is not int or high<0)):
            raise ValueError("Invalid event stream page.")
        with self._transaction() as conn:
            if high is None:
                high=conn.execute("SELECT coalesce(max(rowid),0) FROM streams WHERE server_id=? AND executor_id=?",
                                  (server_id,executor_id)).fetchone()[0]
            rows=conn.execute("SELECT rowid,* FROM streams WHERE server_id=? AND executor_id=? AND rowid>? AND rowid<=? "
                              "ORDER BY rowid LIMIT ?",(server_id,executor_id,after,high,limit)).fetchall()
            return [(row["rowid"],self._decode(row)) for row in rows],high
