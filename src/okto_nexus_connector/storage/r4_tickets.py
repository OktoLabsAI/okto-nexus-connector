"""Bounded, non-secret ticket request intents persisted before issuance."""
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import secrets
import sqlite3

from nexus_connector_core.protocol import canonical_json
from ..errors import ConnectorError


class R4TicketStore:
    def __init__(self, path):
        self.path = Path(path)

    @classmethod
    def for_state(cls, store):
        return cls(store.path.with_suffix(".r4-tickets.sqlite3"))

    @contextmanager
    def _transaction(self):
        conn = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.path, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA max_page_count=4096")
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
                raise ConnectorError("VERSION_INCOMPATIBLE", "r4_tickets",
                                     "The ticket store requires a newer Connector.")
            conn.execute("CREATE TABLE IF NOT EXISTS intents ("
                         "context_hash TEXT PRIMARY KEY, metadata TEXT NOT NULL, digest TEXT NOT NULL)")
            conn.execute("PRAGMA user_version=1")
            yield conn
            conn.commit()
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError) as error:
            raise ConnectorError("JOURNAL_UNAVAILABLE", "r4_tickets",
                                 "The durable ticket intent store is unavailable.") from error
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _key(context):
        return hashlib.sha256(canonical_json(context)).hexdigest()

    def _decode(self, row, context):
        if row is None or hashlib.sha256(row["metadata"].encode()).hexdigest() != row["digest"]:
            raise ConnectorError("JOURNAL_UNAVAILABLE", "r4_tickets",
                                 "The ticket intent failed its integrity check.")
        value = json.loads(row["metadata"])
        if (value["context"] != context or self._key(context) != row["context_hash"]
                or type(value["created_at"]) not in (int, float)
                or not math.isfinite(value["created_at"])):
            raise ConnectorError("JOURNAL_UNAVAILABLE", "r4_tickets",
                                 "The stored ticket intent has inconsistent authority.")
        return value

    def _write(self, conn, value):
        raw = canonical_json(value).decode()
        conn.execute("INSERT OR REPLACE INTO intents VALUES (?,?,?)",
                     (self._key(value["context"]), raw, hashlib.sha256(raw.encode()).hexdigest()))

    @staticmethod
    def _request(context, now, *, intent=None, replaces=None):
        if type(now) not in (int, float) or not math.isfinite(now):
            raise ValueError("Invalid ticket request time.")
        return dict(context=context, client_intent_id=intent or "lane_" + secrets.token_hex(16),
                    credential_request_id="credential_" + secrets.token_hex(16),
                    replaces_ticket_id=replaces, created_at=now)

    def reserve(self, context, now):
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM intents WHERE context_hash=?", (self._key(context),)).fetchone()
            if row is not None:
                return self._decode(row, context)
            if conn.execute("SELECT count(*) FROM intents").fetchone()[0] >= 4096:
                raise ConnectorError("CAPACITY_EXCEEDED", "r4_tickets",
                                     "The ticket intent capacity is exhausted.")
            value = self._request(context, now)
            self._write(conn, value)
            return value

    def require_current(self, expected):
        with self._transaction() as conn:
            current = self._decode(conn.execute("SELECT * FROM intents WHERE context_hash=?",
                                   (self._key(expected["context"]),)).fetchone(), expected["context"])
            if current != expected:
                raise ConnectorError("STALE_GENERATION", "r4_tickets",
                                     "The ticket request was superseded.")

    def replace(self, expected, ticket_id, now):
        with self._transaction() as conn:
            current = self._decode(conn.execute("SELECT * FROM intents WHERE context_hash=?",
                                   (self._key(expected["context"]),)).fetchone(), expected["context"])
            if current != expected:
                raise ConnectorError("STALE_GENERATION", "r4_tickets",
                                     "The ticket request was superseded.")
            # Wall time selects a recovery attempt; only the Server decides
            # whether the previous ticket expired or can be replaced.
            expired = now >= current["created_at"] + current["context"]["expires_in"]
            value = self._request(current["context"], now, intent=current["client_intent_id"],
                                  replaces=None if expired else ticket_id)
            self._write(conn, value)
            return value
