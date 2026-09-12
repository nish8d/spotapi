from datetime import datetime, timezone

import pytest

from consumer.pg_sink import UPSERT_SQL, PostgresSink
from events.schema import PlayEvent

UTC = timezone.utc


class FakeCursor:
    def __init__(self, conn, fail=False):
        self._conn = conn
        self._fail = fail

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, rows):
        if self._fail:
            raise RuntimeError("connection reset by peer")
        self._conn.calls.append(("executemany", sql, list(rows)))


class FakeConnection:
    """Records the order of operations, which is what the tests care about."""

    def __init__(self, fail_on_write=False):
        self.calls = []
        self.closed = False
        self._fail_on_write = fail_on_write

    def cursor(self):
        return FakeCursor(self, fail=self._fail_on_write)

    def commit(self):
        self.calls.append(("commit",))

    def rollback(self):
        self.calls.append(("rollback",))

    def close(self):
        self.closed = True


def make_sink(conn):
    return PostgresSink("postgresql://unused", connect=lambda dsn, **kw: conn)


def make_event(listener_id="listener-00", track_id="t1", second=0):
    started = datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC)
    return PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=True,
        track_id=track_id,
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        started_at=started,
        observed_at=datetime(2026, 1, 1, 0, 0, second + 3, tzinfo=UTC),
    )


def test_upsert_targets_the_event_id_primary_key():
    # Invariant 3: without ON CONFLICT on the PK, replay duplicates rows.
    assert "INSERT INTO raw_plays" in UPSERT_SQL
    assert "ON CONFLICT (event_id) DO UPDATE" in UPSERT_SQL


def test_write_batch_sends_one_row_per_event_in_column_order():
    conn = FakeConnection()
    sink = make_sink(conn)

    events = [make_event(second=0), make_event(track_id="t2", second=30)]
    assert sink.write_batch(events) == 2

    kind, sql, rows = conn.calls[0]
    assert kind == "executemany"
    assert len(rows) == 2
    # Column order must match the INSERT column list exactly.
    assert rows[0] == (
        events[0].event_id, "listener-00", True, "t1", "Mr. Brightside",
        "The Killers", "Hot Fuss", 222075,
        events[0].started_at, events[0].observed_at,
    )


def test_write_commits_after_the_rows_are_sent():
    conn = FakeConnection()
    make_sink(conn).write_batch([make_event()])
    assert [c[0] for c in conn.calls] == ["executemany", "commit"]


def test_empty_batch_never_touches_the_database():
    conn = FakeConnection()
    assert make_sink(conn).write_batch([]) == 0
    assert conn.calls == []


def test_failed_write_rolls_back_and_raises_without_committing():
    conn = FakeConnection(fail_on_write=True)
    sink = make_sink(conn)

    with pytest.raises(RuntimeError):
        sink.write_batch([make_event()])

    # The caller must not commit Kafka offsets for data that is not in the DB.
    assert ("commit",) not in conn.calls
    assert ("rollback",) in conn.calls
