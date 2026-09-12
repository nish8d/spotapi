"""Upserts play events into `raw_plays`. Deleted in M4 along with the rest
of `consumer/`, when Flink's passthrough statement takes over this write.

The only module in the project that knows the raw_plays column order.
"""

from __future__ import annotations

import logging
from typing import Sequence

import psycopg

from events.schema import PlayEvent

log = logging.getLogger(__name__)

# ON CONFLICT is the other half of the deterministic event_id. The id is
# sha1(listener_id | track_id | started_at floored to 5s), so re-reading the
# same Kafka message -- after a crash, a rebalance, or a deliberate offset
# reset -- produces a row that collides with the one already there and
# updates it in place instead of duplicating it.
#
# DO UPDATE rather than DO NOTHING: M4 replaces this with Flink's JDBC
# connector in upsert mode, which is DO UPDATE. Matching it now means the
# handover changes nothing about what ends up in the table. In practice the
# only field that can differ between two writes of one play is observed_at,
# and taking the newer value is the more truthful record of when it was seen.
UPSERT_SQL = """
INSERT INTO raw_plays (
    event_id, listener_id, is_synthetic, track_id, track_name,
    artist_name, album_name, duration_ms, started_at, observed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO UPDATE SET
    listener_id  = EXCLUDED.listener_id,
    is_synthetic = EXCLUDED.is_synthetic,
    track_id     = EXCLUDED.track_id,
    track_name   = EXCLUDED.track_name,
    artist_name  = EXCLUDED.artist_name,
    album_name   = EXCLUDED.album_name,
    duration_ms  = EXCLUDED.duration_ms,
    started_at   = EXCLUDED.started_at,
    observed_at  = EXCLUDED.observed_at
"""


def _row(event: PlayEvent) -> tuple:
    """Flatten to the INSERT column order. psycopg adapts aware datetimes to
    TIMESTAMPTZ itself, so started_at/observed_at are passed as objects."""
    return (
        event.event_id,
        event.listener_id,
        event.is_synthetic,
        event.track_id,
        event.track_name,
        event.artist_name,
        event.album_name,
        event.duration_ms,
        event.started_at,
        event.observed_at,
    )


class PostgresSink:
    def __init__(self, dsn: str, connect=psycopg.connect) -> None:
        self._dsn = dsn
        self._connect = connect
        self._conn = None

    def _connection(self):
        """Connect lazily and reconnect after a drop. The consumer outlives
        any single connection; Postgres restarting must not end the process."""
        if self._conn is None or getattr(self._conn, "closed", False):
            self._conn = self._connect(self._dsn, autocommit=False)
        return self._conn

    def write_batch(self, events: Sequence[PlayEvent]) -> int:
        """Upsert every event in one round trip and one transaction.

        Either the whole batch is committed or none of it is, which is what
        lets the caller treat a successful return as permission to commit the
        corresponding Kafka offsets.
        """
        if not events:
            return 0

        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, [_row(e) for e in events])
            conn.commit()
        except Exception:
            # Leave the connection usable for the retry the caller will make.
            try:
                conn.rollback()
            except Exception:
                log.warning("rollback failed; dropping the connection")
                self._conn = None
            raise
        return len(events)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
