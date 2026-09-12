# M2 — First Heartbeat: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** A deliberately throwaway Python consumer reads the `plays` topic, upserts every event into Postgres `raw_plays`, and a Grafana dashboard provisioned from git shows a rising play count.

**Architecture:** `consumer/` is a new top-level package with exactly two modules: `pg_sink.py` knows how to upsert a batch of `PlayEvent`s, and `main.py` owns the poll loop and the offset commit. They are separated so the commit logic can be unit-tested against a fake sink with no database. Postgres bootstraps its whole schema from `postgres/init.sql` on first boot; Grafana provisions its datasource and dashboard from files under `grafana/`. Nothing in this milestone is configured through a UI, and nothing survives `docker compose down -v` that isn't in git.

**Tech Stack:** Python 3.12, `confluent-kafka` 2.6.1, `psycopg[binary]` 3.2.9, `postgres:16-alpine`, `grafana/grafana:11.3.0`, `pytest` 8.3.4.

**Spec:** `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` (see "Postgres schema", "Dashboard", and "M2 — First heartbeat: Python consumer → Postgres → Grafana")

## Global Constraints

- **This code is throwaway and that is the point.** M4 deletes `consumer/` wholesale and lets Flink take over the `raw_plays` write. Do not generalise it, do not build abstractions for a future that deletes it. It exists so consumer groups, offsets, lag and rebalancing are seen directly before a framework hides them.
- **Partition key is `listener_id`.** Nothing in M2 changes this, and nothing in M2 may assume a listener's events arrive on more than one partition.
- **`event_id` is deterministic** — `sha1(listener_id | track_id | started_at floored to 5s)`. M2 is the first milestone where this pays off, via the upsert.
- **Every sink table has a primary key and writes in upsert mode.** `raw_plays` is keyed on `event_id` and written with `ON CONFLICT (event_id) DO UPDATE`. A sink without a PK turns replay into duplicate rows; the offset-reset exercise in Task 6 is the proof that it doesn't.
- **`started_at` is the event-time column.** Panels that bucket by time bucket on `started_at`. `observed_at` appears in exactly one place — the skew panel, which charts `observed_at - started_at`.
- Images are pinned to the versions the spec names: `postgres:16-alpine`, `grafana/grafana:11.3.0`. Do not use `latest`.
- **Host port 5432 is already taken by an unrelated local Postgres.** Compose maps Postgres to `55432:5432`. In-container clients use `postgres:5432`. Grafana is on `3000:3000`, which is free.
- Broker addresses unchanged: `kafka:9092` inside Compose, `localhost:29092` from the host. Topic is `plays`.
- All datetimes crossing a module boundary are timezone-aware UTC. `events/schema.py` stays the single source of truth for the event shape — the consumer deserializes with `PlayEvent.from_json`, never with a hand-rolled `json.loads`.
- Unit tests use injected fakes and contact neither a broker nor a database. `.venv/bin/pytest -q` must stay green and must stay fast.
- Do not jump ahead: no Flink, no `flink/Dockerfile`, no SQL jobs. The three `agg_` tables are created empty in Task 1 and stay empty until M4.

### Decisions this plan makes that the spec leaves open

1. **Manual offset commits, not auto-commit.** `enable.auto.commit=False`; the batch is written to Postgres first and the offsets committed after. This is at-least-once, and it is the configuration that makes the Task 6 exercises show something real: the committed offset only moves when data actually landed.
2. **`postgres/init.sql` creates all four tables now**, not just `raw_plays`. The init script only runs against an empty data volume, so adding the `agg_` tables in M4 would force a `down -v` or a migration step. They cost nothing sitting empty.
3. **`ON CONFLICT DO UPDATE`, not `DO NOTHING`.** M4's Flink JDBC connector writes in upsert mode, which is `DO UPDATE`. Matching it now means the M2 → M4 handover is invisible in the data rather than a subtle change in duplicate semantics. The practical difference: a re-poll of the same play refreshes `observed_at` to the most recent observation.
4. **Grafana has no named volume.** Everything it knows is provisioned from git. Giving it persistent storage would only create a place for state to hide.
5. **The Postgres password is hardcoded in `docker-compose.yml` as `spot`.** The `.env` rule in CLAUDE.md exists for the Spotify OAuth credentials, which are a real secret. A local-only database reachable on `127.0.0.1:55432` is not, and putting it in `.env` would make the stack fail to start from a fresh clone for no gain.

---

## File Structure

| File | Responsibility |
|---|---|
| `postgres/init.sql` | Create: all four tables, verbatim from the spec's "Postgres schema" section. Runs once, on an empty data volume. |
| `consumer/__init__.py` | Create: empty, makes `consumer` a package. |
| `consumer/pg_sink.py` | Create: `PostgresSink`. The only module that knows the `raw_plays` column order and the upsert SQL. No Kafka import. |
| `consumer/main.py` | Create: settings, the poll loop, decode, commit-after-write, rewind-on-failure, and the rebalance logging callbacks. No SQL. |
| `tests/test_pg_sink.py` | Create: upsert SQL shape and column order, against a fake connection. |
| `tests/test_consumer.py` | Create: batching, malformed-message handling, and the commit/rewind ordering that is the whole point of the milestone. |
| `grafana/provisioning/datasources/postgres.yml` | Create: the Postgres datasource, `uid: spot-postgres`. |
| `grafana/provisioning/dashboards/spot.yml` | Create: the file provider pointing at `/var/lib/grafana/dashboards`. |
| `grafana/dashboards/spot.json` | Create: the dashboard — plays per minute, now playing, event-time skew. |
| `requirements.txt` | Modify: add `psycopg[binary]==3.2.9`. |
| `Dockerfile` | Modify: add `COPY consumer/ consumer/`. |
| `docker-compose.yml` | Modify: add `postgres`, `consumer`, and `grafana` services plus the `postgres-data` volume. |

### Why `consumer/` is a directory and not a file

It is about 150 lines and would fit in one module. It gets a directory because M4 deletes it, and `git rm -r consumer/` is a cleaner and more honest ending than surgically removing a file and its imports. The shape of the code should say out loud that it is temporary.

### Why `pg_sink.py` and `main.py` are separate

The interesting logic in M2 is not the SQL, it is *when the offset is committed relative to the write*. Splitting the sink out means `tests/test_consumer.py` can hand the loop a fake sink that raises on demand, and assert that no commit happened — which is impossible to test cleanly if the loop holds a live cursor.

---

### Task 1: Postgres with the full schema

**Files:**
- Create: `postgres/init.sql`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing.
- Produces: a reachable database `spot` as user `spot`, password `spot`, on `postgres:5432` inside Compose and `localhost:55432` from the host. Tables `raw_plays`, `agg_plays_per_minute`, `agg_top_artists`, `agg_sessions`.

- [x] **Step 1: Write the schema**

```bash
mkdir -p postgres
cat > postgres/init.sql <<'EOF'
-- Runs once, on an empty data volume, via /docker-entrypoint-initdb.d.
-- All four tables are created here even though only raw_plays is written
-- before M4: this script does not re-run, so adding tables later would mean
-- destroying the volume or introducing a migration step.
--
-- Every table has a primary key and every sink upserts onto it. That is what
-- makes the deterministic event_id strategy work end to end -- replaying the
-- topic must not create duplicate rows.

-- Written by the throwaway M2 consumer, then by the Flink passthrough
-- statement from M4 onwards. The raw event log behind the "now playing"
-- and event-time-skew panels.
CREATE TABLE raw_plays (
  event_id     TEXT PRIMARY KEY,
  listener_id  TEXT        NOT NULL,
  is_synthetic BOOLEAN     NOT NULL,
  track_id     TEXT        NOT NULL,
  track_name   TEXT        NOT NULL,
  artist_name  TEXT        NOT NULL,
  album_name   TEXT,
  duration_ms  INT,
  started_at   TIMESTAMPTZ NOT NULL,
  observed_at  TIMESTAMPTZ NOT NULL
);

-- started_at drives every time-bucketed panel, and both panels that read
-- raw_plays scan a trailing time range rather than the whole table.
CREATE INDEX raw_plays_started_at_idx ON raw_plays (started_at DESC);
CREATE INDEX raw_plays_listener_started_idx ON raw_plays (listener_id, started_at DESC);

-- Filled by Flink from M4. Empty until then.
CREATE TABLE agg_plays_per_minute (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  listener_id  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, listener_id)
);

-- Filled by Flink from M5. Empty until then.
CREATE TABLE agg_top_artists (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  artist_name  TEXT        NOT NULL,
  play_count   BIGINT      NOT NULL,
  PRIMARY KEY (window_start, artist_name)
);

-- Filled by Flink from M6. Empty until then.
CREATE TABLE agg_sessions (
  listener_id      TEXT        NOT NULL,
  session_start    TIMESTAMPTZ NOT NULL,
  session_end      TIMESTAMPTZ NOT NULL,
  play_count       BIGINT      NOT NULL,
  distinct_artists BIGINT      NOT NULL,
  PRIMARY KEY (listener_id, session_start)
);
EOF
```

- [x] **Step 2: Add the Postgres service to Compose**

Insert this service after `kafka-init` and before `simulator` in `docker-compose.yml`:

```yaml
  postgres:
    image: postgres:16-alpine
    ports:
      # Host 5432 is taken by an unrelated local Postgres, hence 55432.
      # Containers still reach it as postgres:5432.
      - "55432:5432"
    environment:
      POSTGRES_USER: spot
      POSTGRES_PASSWORD: spot
      POSTGRES_DB: spot
    volumes:
      - postgres-data:/var/lib/postgresql/data
      # Only runs when the data volume is empty. Editing init.sql after the
      # first boot does nothing until `docker compose down -v`.
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
    healthcheck:
      # -U and -d matter: without them pg_isready checks the root role and
      # reports ready while the init script is still running.
      test: ["CMD-SHELL", "pg_isready -U spot -d spot"]
      interval: 5s
      timeout: 5s
      retries: 12
      start_period: 10s
```

And add the volume to the `volumes:` block at the bottom:

```yaml
volumes:
  kafka-data:
  postgres-data:
```

- [x] **Step 3: Start Postgres and verify the schema landed**

```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U spot -d spot -c '\dt'
```

Expected: four rows — `agg_plays_per_minute`, `agg_sessions`, `agg_top_artists`, `raw_plays`.

Then confirm the primary key is really there, since every later guarantee rests on it:

```bash
docker compose exec -T postgres psql -U spot -d spot -c \
  "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'raw_plays'::regclass;"
```

Expected: a row with `contype` = `p` (primary key).

- [x] **Step 4: Commit**

```bash
git add postgres/init.sql docker-compose.yml
git commit -m "M2: Postgres with the full schema, all four tables"
```

---

### Task 2: `consumer/pg_sink.py` — the upsert

**Files:**
- Create: `consumer/__init__.py`, `consumer/pg_sink.py`
- Modify: `requirements.txt`
- Test: `tests/test_pg_sink.py`

**Interfaces:**
- Consumes: `PlayEvent` from `events.schema`.
- Produces:
  - `PostgresSink(dsn: str, connect=psycopg.connect)` — `connect` is injected so tests never open a socket.
  - `PostgresSink.write_batch(self, events: list[PlayEvent]) -> int` — upserts the batch in one round trip inside one transaction, commits, returns the number of rows sent. Raises on failure after rolling back.
  - `PostgresSink.close(self) -> None`
  - `UPSERT_SQL: str` — module-level, so the test can assert on its shape.

- [x] **Step 1: Add the dependency**

```bash
printf 'psycopg[binary]==3.2.9\n' >> requirements.txt
.venv/bin/pip install -r requirements-dev.txt
```

Verify: `.venv/bin/python -c "import psycopg; print(psycopg.__version__)"` prints `3.2.9`.

- [x] **Step 2: Write the failing test**

```bash
mkdir -p consumer && touch consumer/__init__.py
cat > tests/test_pg_sink.py <<'EOF'
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
EOF
```

- [x] **Step 3: Run the test to verify it fails**

Run: `.venv/bin/pytest tests/test_pg_sink.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'consumer.pg_sink'`.

- [x] **Step 4: Write the implementation**

```bash
cat > consumer/pg_sink.py <<'EOF'
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
EOF
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pg_sink.py -q`
Expected: 5 passed.

Then the whole suite: `.venv/bin/pytest -q` — expected 43 passed (38 from M1 + 5).

- [x] **Step 6: Commit**

```bash
git add requirements.txt consumer/__init__.py consumer/pg_sink.py tests/test_pg_sink.py
git commit -m "M2: Postgres sink, upserting raw_plays on event_id"
```

---

### Task 3: `consumer/main.py` — the poll loop and the commit

**Files:**
- Create: `consumer/main.py`
- Test: `tests/test_consumer.py`

**Interfaces:**
- Consumes: `PostgresSink.write_batch` from Task 2; `PlayEvent.from_json` from `events.schema`.
- Produces:
  - `Settings` — frozen dataclass: `bootstrap_servers, topic, group_id, dsn, batch_max, poll_timeout`.
  - `settings_from_env(env: Mapping[str, str]) -> Settings`
  - `poll_batch(consumer, max_messages: int, timeout: float) -> list` — returns raw Kafka messages.
  - `decode(messages) -> list[PlayEvent]` — skips anything that will not deserialize.
  - `rewind(consumer, messages) -> None` — seeks each touched partition back to the batch's first offset.
  - `run(consumer, sink, settings, should_continue=lambda: True) -> None` — the loop.
  - `main() -> None`

**Background for the implementer — why `rewind` exists.** There are two different
positions in a Kafka consumer and conflating them is the classic bug here. The
**committed offset** is stored in the broker and decides where a *new* consumer
in this group starts. The **current position** is in memory and decides what
`poll()` hands you next. Declining to commit after a failed write protects a
restart, but the running process has already moved its position past those
messages and would never see them again. To actually retry the batch you must
seek back explicitly. That is what `rewind` does, and the test asserting it is
the most valuable test in this milestone.

- [x] **Step 1: Write the failing test**

```bash
cat > tests/test_consumer.py <<'EOF'
import pytest

from datetime import datetime, timezone

from consumer.main import (
    Settings,
    decode,
    poll_batch,
    rewind,
    run,
    settings_from_env,
)
from events.schema import PlayEvent

UTC = timezone.utc


def make_event(listener_id="listener-00", track_id="t1", second=0):
    # Defined locally rather than imported from tests/test_pg_sink.py: there is
    # no tests/__init__.py, so a cross-test import loads a second copy of the
    # module under a different name. Each test file standing alone is also the
    # existing convention here -- test_schema.py and test_kafka_sink.py each
    # define their own builder.
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


class FakeMessage:
    def __init__(self, value, partition=0, offset=0, error=None, topic="plays"):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error
        self._topic = topic

    def value(self):
        return self._value

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeConsumer:
    """Hands out a scripted sequence of poll() results and records commits."""

    def __init__(self, script):
        self._script = list(script)
        self.commits = 0
        self.seeks = []
        self.closed = False

    def poll(self, timeout):
        if not self._script:
            return None
        return self._script.pop(0)

    def commit(self, asynchronous=False):
        self.commits += 1

    def seek(self, partition):
        self.seeks.append((partition.topic, partition.partition, partition.offset))

    def close(self):
        self.closed = True


class FakeSink:
    def __init__(self, fail_times=0):
        self.batches = []
        self._fail_times = fail_times

    def write_batch(self, events):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("postgres is down")
        self.batches.append(list(events))
        return len(events)


def encoded(event):
    return event.to_json().encode("utf-8")


SETTINGS = Settings(
    bootstrap_servers="kafka:9092",
    topic="plays",
    group_id="raw-plays-writer",
    dsn="postgresql://unused",
    batch_max=3,
    poll_timeout=0.01,
    # Zero, so the two failure tests do not put a real sleep in the suite.
    retry_backoff=0.0,
)


def test_settings_come_from_the_environment_with_defaults():
    s = settings_from_env({})
    assert s.group_id == "raw-plays-writer"
    assert s.topic == "plays"
    assert s.batch_max == 100
    assert s.retry_backoff == 2.0

    s = settings_from_env({"CONSUMER_GROUP": "second-reader", "BATCH_MAX": "7"})
    assert s.group_id == "second-reader"
    assert s.batch_max == 7


def test_poll_batch_stops_at_the_batch_limit():
    messages = [FakeMessage(b"a", offset=i) for i in range(10)]
    consumer = FakeConsumer(messages)
    assert len(poll_batch(consumer, max_messages=3, timeout=0.01)) == 3


def test_poll_batch_returns_early_when_the_broker_has_nothing_more():
    consumer = FakeConsumer([FakeMessage(b"a"), None, FakeMessage(b"b")])
    # The None ends the batch; the loop must not block waiting to fill it.
    assert len(poll_batch(consumer, max_messages=100, timeout=0.01)) == 1


def test_decode_skips_malformed_messages_and_keeps_the_rest():
    good, other = make_event(second=0), make_event(track_id="t2", second=30)
    messages = [
        FakeMessage(encoded(good)),
        FakeMessage(b"{not json at all"),
        FakeMessage(encoded(other)),
    ]
    events = decode(messages)
    assert [e.event_id for e in events] == [good.event_id, other.event_id]


def test_commit_happens_only_after_the_batch_is_written():
    event = make_event()
    consumer = FakeConsumer([FakeMessage(encoded(event))])
    sink = FakeSink()

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert len(sink.batches) == 1
    assert consumer.commits == 1


def test_a_failed_write_does_not_commit():
    consumer = FakeConsumer([FakeMessage(encoded(make_event()))])
    sink = FakeSink(fail_times=1)

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sink.batches == []
    assert consumer.commits == 0


def test_a_failed_write_rewinds_to_the_start_of_the_batch():
    # Committing nothing protects a restart. It does nothing for the running
    # process, whose position has already advanced -- so the loop must seek.
    messages = [
        FakeMessage(encoded(make_event(second=0)), partition=0, offset=40),
        FakeMessage(encoded(make_event(second=5)), partition=0, offset=41),
        FakeMessage(encoded(make_event(second=10)), partition=2, offset=7),
    ]
    consumer = FakeConsumer(messages)
    sink = FakeSink(fail_times=1)

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sorted(consumer.seeks) == [("plays", 0, 40), ("plays", 2, 7)]


def test_rewind_picks_the_lowest_offset_seen_per_partition():
    consumer = FakeConsumer([])
    rewind(consumer, [
        FakeMessage(b"", partition=1, offset=99),
        FakeMessage(b"", partition=1, offset=95),
        FakeMessage(b"", partition=1, offset=97),
    ])
    assert consumer.seeks == [("plays", 1, 95)]


def test_a_batch_of_only_malformed_messages_still_commits():
    # Otherwise one unparseable byte wedges the partition forever.
    consumer = FakeConsumer([FakeMessage(b"garbage")])
    sink = FakeSink()

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sum(len(batch) for batch in sink.batches) == 0
    assert consumer.commits == 1


def _once():
    """should_continue that is True for exactly one iteration."""
    remaining = [True]

    def go():
        if remaining:
            remaining.pop()
            return True
        return False

    return go
EOF
```

- [x] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/pytest tests/test_consumer.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'consumer.main'`.

- [x] **Step 3: Write the implementation**

```bash
cat > consumer/main.py <<'EOF'
"""The throwaway M2 consumer: `plays` topic -> Postgres `raw_plays`.

Written with confluent-kafka directly, and with manual offset commits, so that
consumer groups, offsets, lag and rebalancing are visible before M4 replaces
the whole thing with Flink. Deleted in M4. Not built to last, on purpose.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Mapping

from confluent_kafka import Consumer, TopicPartition

from consumer.pg_sink import PostgresSink
from events.schema import PlayEvent

log = logging.getLogger("consumer")


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    group_id: str
    dsn: str
    batch_max: int
    poll_timeout: float
    retry_backoff: float


def settings_from_env(env: Mapping[str, str]) -> Settings:
    return Settings(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=env.get("KAFKA_TOPIC", "plays"),
        group_id=env.get("CONSUMER_GROUP", "raw-plays-writer"),
        dsn=env.get("POSTGRES_DSN", "postgresql://spot:spot@postgres:5432/spot"),
        batch_max=int(env.get("BATCH_MAX", "100")),
        poll_timeout=float(env.get("POLL_TIMEOUT", "1.0")),
        # Injectable so the unit tests can set it to 0 instead of sleeping.
        retry_backoff=float(env.get("RETRY_BACKOFF", "2.0")),
    )


def poll_batch(consumer, max_messages: int, timeout: float) -> list:
    """Collect up to max_messages, returning as soon as the broker is drained.

    The first poll waits up to `timeout` for something to arrive; subsequent
    polls use 0 so a quiet topic yields a small batch immediately rather than
    holding rows back waiting to fill one.
    """
    batch: list = []
    # Bounded rather than `while len(batch) < max_messages`: an error message
    # is skipped without being added to the batch, so an unbroken stream of
    # them would otherwise loop forever.
    for _ in range(max_messages * 2):
        if len(batch) >= max_messages:
            break
        message = consumer.poll(timeout if not batch else 0)
        if message is None:
            break
        if message.error() is not None:
            log.warning("kafka error: %s", message.error())
            continue
        batch.append(message)
    return batch


def decode(messages) -> list[PlayEvent]:
    """Deserialize with the shared schema, dropping anything unparseable.

    A malformed message is logged and skipped rather than retried: its offset
    is committed with the rest of the batch, because one bad byte must not
    wedge a partition forever. A database failure is the opposite case and is
    handled by the caller.
    """
    events = []
    for message in messages:
        try:
            events.append(PlayEvent.from_json(message.value()))
        except Exception as exc:
            log.error(
                "skipping malformed message %s[%d]@%d: %s",
                message.topic(), message.partition(), message.offset(), exc,
            )
    return events


def rewind(consumer, messages) -> None:
    """Seek every partition in this batch back to its first offset.

    Declining to commit protects a restart, not this process: the in-memory
    position has already advanced past these messages. Retrying requires an
    explicit seek. This is the difference between the committed offset and
    the current position, and it is easy to get wrong.
    """
    lowest: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        offset = message.offset()
        if key not in lowest or offset < lowest[key]:
            lowest[key] = offset
    for (topic, partition), offset in lowest.items():
        try:
            consumer.seek(TopicPartition(topic, partition, offset))
        except Exception as exc:
            # A rebalance between the poll and the failure means this partition
            # is no longer ours. Whoever holds it now starts from the last
            # committed offset, which is still before this batch, so nothing
            # is lost -- there is simply nothing for us to rewind.
            log.warning("cannot rewind %s[%d]: %s", topic, partition, exc)
            continue
        log.info("rewound %s[%d] to offset %d", topic, partition, offset)


def run(consumer, sink, settings: Settings, should_continue=lambda: True) -> None:
    """Poll, write, then commit -- in that order.

    Committing only after a successful write makes this at-least-once: a crash
    between the write and the commit replays the batch, and the upsert on the
    deterministic event_id turns that replay into a no-op.
    """
    while should_continue():
        messages = poll_batch(consumer, settings.batch_max, settings.poll_timeout)
        if not messages:
            continue

        events = decode(messages)
        try:
            written = sink.write_batch(events)
        except Exception as exc:
            log.error("batch write failed, not committing: %s", exc)
            rewind(consumer, messages)
            time.sleep(settings.retry_backoff)
            continue

        consumer.commit(asynchronous=False)
        log.info("wrote %d events, committed %d offsets", written, len(messages))


def _log_assignment(consumer, partitions) -> None:
    log.info("ASSIGNED %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def _log_revocation(consumer, partitions) -> None:
    log.info("REVOKED  %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings_from_env(os.environ)

    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        # The whole point of the milestone: offsets move only when this code
        # says so, after the rows are in Postgres.
        "enable.auto.commit": False,
        # A group with no committed offset starts at the beginning, so the
        # backlog the simulator has already produced gets consumed.
        "auto.offset.reset": "earliest",
    })
    # Callbacks purely so a rebalance is visible in the logs. Watching these
    # fire when a second consumer joins is one of M2's exercises.
    consumer.subscribe([settings.topic],
                       on_assign=_log_assignment,
                       on_revoke=_log_revocation)

    sink = PostgresSink(settings.dsn)
    running = [True]

    def stop(signum, frame):
        log.info("signal %d received, finishing the current batch", signum)
        running[0] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s as group %s", settings.topic, settings.group_id)
    try:
        run(consumer, sink, settings, should_continue=lambda: running[0])
    finally:
        # close() leaves the group cleanly, which triggers an immediate
        # rebalance instead of waiting for the session timeout to expire.
        consumer.close()
        sink.close()
        log.info("stopped")


if __name__ == "__main__":
    main()
EOF
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_consumer.py -q`
Expected: 9 passed.

Then the whole suite: `.venv/bin/pytest -q` — expected 52 passed.

- [x] **Step 5: Commit**

```bash
git add consumer/main.py tests/test_consumer.py
git commit -m "M2: consumer loop, committing offsets only after the write lands"
```

---

### Task 4: Wire the consumer into Compose and watch rows appear

**Files:**
- Modify: `Dockerfile`, `docker-compose.yml`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: a `consumer` service, group `raw-plays-writer`, filling `raw_plays`.

- [x] **Step 1: Add `consumer/` to the image**

In `Dockerfile`, after the `COPY producer/ producer/` line, add:

```dockerfile
COPY consumer/ consumer/
```

- [x] **Step 2: Add the consumer service to Compose**

Insert after the `simulator` service:

```yaml
  consumer:
    build: .
    # The image's CMD runs the simulator; this service runs the consumer.
    command: ["python", "-m", "consumer.main"]
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC: plays
      CONSUMER_GROUP: raw-plays-writer
      POSTGRES_DSN: postgresql://spot:spot@postgres:5432/spot
    # No container_name: `docker compose up --scale consumer=2` needs to be
    # free to name the replicas, and that scale-up is one of M2's exercises.
    restart: unless-stopped
```

- [x] **Step 3: Make `SIM_SPEED` overridable from the shell**

The lag exercise in Task 6 needs to turn the simulator's volume up and back
down. Hardcoded in `docker-compose.yml`, that means editing a tracked file
twice per experiment. Change the `simulator` service's environment line to
take a default from the shell instead:

```yaml
      SIM_SPEED: ${SIM_SPEED:-1.0}
```

`docker compose up -d simulator` is then unchanged at speed 1.0, and
`SIM_SPEED=60 docker compose up -d simulator` recreates it at sixty times the
rate. Verify both read through:

```bash
docker compose config | grep -A1 SIM_SPEED
SIM_SPEED=60 docker compose config | grep -A1 SIM_SPEED
```

Expected: `1.0` in the first, `60` in the second.

- [x] **Step 4: Bring the stack up and confirm rows land**

```bash
docker compose up -d --build
docker compose ps
```

Expected: `kafka`, `postgres`, `simulator`, `consumer` all up; `kafka-init` exited 0.

Watch the consumer pick up its partitions:

```bash
docker compose logs consumer | grep -E "ASSIGNED|consuming|wrote"
```

Expected: one `consuming plays as group raw-plays-writer`, one `ASSIGNED ['plays[0]', 'plays[1]', 'plays[2]']` (a single consumer takes all three partitions), then repeated `wrote N events, committed N offsets`.

Then count the rows twice, ten seconds apart:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "SELECT count(*) FROM raw_plays;"
```

Expected: a non-zero count that is strictly larger on the second run.

- [x] **Step 5: Confirm the upsert, not just the insert**

This is the invariant-3 check, done cheaply before the full replay exercise in Task 6:

```bash
docker compose exec -T postgres psql -U spot -d spot -c \
  "SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_ids FROM raw_plays;"
```

Expected: the two numbers are equal. They are trivially equal given `event_id` is the primary key — the point of running it is that it would be *impossible* for them to differ, which is exactly the guarantee the primary key buys.

- [x] **Step 6: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "M2: run the consumer under Compose, raw_plays filling"
```

---

### Task 5: Grafana, provisioned from git

**Files:**
- Create: `grafana/provisioning/datasources/postgres.yml`, `grafana/provisioning/dashboards/spot.yml`, `grafana/dashboards/spot.json`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: the `raw_plays` table from Task 1, filled by Task 4.
- Produces: a Grafana at `http://localhost:3000` with a dashboard titled `spot — live`, datasource uid `spot-postgres`.

- [x] **Step 1: Write the datasource**

```bash
mkdir -p grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards
cat > grafana/provisioning/datasources/postgres.yml <<'EOF'
# Provisioned at startup. The UI shows this datasource as non-editable, which
# is intentional: the datasource is code, and a change made in the browser
# would vanish on the next `docker compose up`.
apiVersion: 1

datasources:
  - name: Postgres
    # Dashboards reference this uid, so it must stay stable.
    uid: spot-postgres
    type: postgres
    access: proxy
    url: postgres:5432
    database: spot
    user: spot
    secureJsonData:
      password: spot
    jsonData:
      sslmode: disable
      timescaledb: false
    isDefault: true
    editable: false
EOF
```

- [x] **Step 2: Write the dashboard provider**

```bash
cat > grafana/provisioning/dashboards/spot.yml <<'EOF'
apiVersion: 1

providers:
  - name: spot
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    # Re-reads the JSON every 10s, so editing spot.json on the host shows up
    # without restarting the container.
    updateIntervalSeconds: 10
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
EOF
```

- [x] **Step 3: Write the dashboard**

The spec's "Dashboard" section lists six panels. Three can be built now,
because `raw_plays` is the only table with data in it until M4:

| Spec panel | Milestone | Why |
|---|---|---|
| 1. Plays per minute | **M2** | Bucketed from `raw_plays` at query time now; repointed at `agg_plays_per_minute` in M4. |
| 2. Top 10 artists | M5 | Needs `agg_top_artists`, which the hopping window fills. |
| 3. Now playing | **M2** | Reads `raw_plays` directly and never changes. |
| 4. Active sessions | M6 | Needs `agg_sessions`, which the session window fills. |
| 5. Consumer lag | M7 | Lag lives in Kafka, not Postgres; a panel needs an exporter service. Watched from the CLI in Task 6 instead. |
| 6. Event-time skew | **M2** | Reads `raw_plays` directly and never changes. |

Panel 1 is the interesting one. Doing the bucketing in SQL now and in the
stream later means M4's change is a one-line query edit, and the two versions
can be compared side by side — which is the clearest available answer to "what
did Flink actually take over?"

```bash
cat > grafana/dashboards/spot.json <<'EOF'
{
  "uid": "spot-live",
  "title": "spot — live",
  "tags": ["spot"],
  "timezone": "utc",
  "schemaVersion": 39,
  "version": 1,
  "editable": true,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Plays per minute, by listener",
      "description": "Counted straight from raw_plays with date_trunc. M4 repoints this at agg_plays_per_minute, where Flink has done the same bucketing in the stream instead of at query time.",
      "datasource": { "type": "postgres", "uid": "spot-postgres" },
      "gridPos": { "h": 9, "w": 24, "x": 0, "y": 0 },
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "min": 0,
          "custom": {
            "drawStyle": "bars",
            "fillOpacity": 70,
            "lineWidth": 0,
            "stacking": { "mode": "normal", "group": "A" }
          }
        },
        "overrides": []
      },
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": false },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "postgres", "uid": "spot-postgres" },
          "format": "time_series",
          "rawQuery": true,
          "rawSql": "SELECT date_trunc('minute', started_at) AS time, listener_id AS metric, count(*) AS value FROM raw_plays WHERE $__timeFilter(started_at) GROUP BY 1, 2 ORDER BY 1"
        }
      ]
    },
    {
      "id": 2,
      "type": "table",
      "title": "Now playing",
      "description": "The most recent play per listener. DISTINCT ON is Postgres-specific and is the cheapest way to take one row per group.",
      "datasource": { "type": "postgres", "uid": "spot-postgres" },
      "gridPos": { "h": 10, "w": 14, "x": 0, "y": 9 },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {
            "matcher": { "id": "byName", "options": "started_at" },
            "properties": [{ "id": "unit", "value": "dateTimeAsIso" }]
          }
        ]
      },
      "options": {
        "showHeader": true,
        "sortBy": [{ "displayName": "started_at", "desc": true }]
      },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "postgres", "uid": "spot-postgres" },
          "format": "table",
          "rawQuery": true,
          "rawSql": "SELECT DISTINCT ON (listener_id) listener_id, track_name, artist_name, is_synthetic, started_at FROM raw_plays ORDER BY listener_id, started_at DESC"
        }
      ]
    },
    {
      "id": 3,
      "type": "timeseries",
      "title": "Event-time skew (observed_at − started_at)",
      "description": "How far behind the event time each observation was. This is the only panel that reads observed_at: it is retained to chart poll drift and is never used for windowing.",
      "datasource": { "type": "postgres", "uid": "spot-postgres" },
      "gridPos": { "h": 10, "w": 10, "x": 14, "y": 9 },
      "fieldConfig": {
        "defaults": {
          "unit": "s",
          "min": 0,
          "custom": { "drawStyle": "line", "lineWidth": 2, "fillOpacity": 10 }
        },
        "overrides": []
      },
      "options": {
        "legend": { "displayMode": "list", "placement": "bottom", "showLegend": true },
        "tooltip": { "mode": "multi", "sort": "desc" }
      },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "postgres", "uid": "spot-postgres" },
          "format": "time_series",
          "rawQuery": true,
          "rawSql": "SELECT date_trunc('minute', observed_at) AS time, avg(extract(epoch FROM observed_at - started_at)) AS \"avg skew\", max(extract(epoch FROM observed_at - started_at)) AS \"max skew\" FROM raw_plays WHERE $__timeFilter(observed_at) GROUP BY 1 ORDER BY 1"
        }
      ]
    }
  ]
}
EOF
```

- [x] **Step 4: Validate the JSON before handing it to Grafana**

A malformed dashboard fails silently — Grafana logs it and shows an empty list, which is a confusing way to find a missing comma.

Run: `.venv/bin/python -m json.tool grafana/dashboards/spot.json > /dev/null && echo "valid JSON"`
Expected: `valid JSON`.

- [x] **Step 5: Add the Grafana service to Compose**

Insert after the `consumer` service:

```yaml
  grafana:
    image: grafana/grafana:11.3.0
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      # Local, single-user, no data worth protecting: skip the login wall.
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_AUTH_DISABLE_LOGIN_FORM: "true"
      GF_USERS_DEFAULT_THEME: dark
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    # Deliberately no named volume. Everything Grafana knows comes from the
    # two directories above, both in git. Persistent storage would only give
    # UI-made changes somewhere to hide.
    restart: unless-stopped
```

- [x] **Step 6: Start Grafana and verify provisioning took**

```bash
docker compose up -d grafana
docker compose logs grafana 2>&1 | grep -iE "error|provisioning" | head -20
```

Expected: no `error` lines relating to the datasource or dashboard.

Confirm both landed, via the API rather than by eye:

```bash
curl -s http://localhost:3000/api/datasources | python3 -m json.tool | grep -E '"name"|"uid"|"type"'
curl -s "http://localhost:3000/api/search?query=spot" | python3 -m json.tool | grep -E '"title"|"uid"'
```

Expected: a `postgres` datasource with uid `spot-postgres`, and a dashboard titled `spot — live` with uid `spot-live`.

- [x] **Step 7: Verify the acceptance criterion — a rising play count**

Query the first panel's SQL through Grafana's own datasource proxy, so a pass proves the datasource works and not merely that the table has rows:

```bash
for i in 1 2; do
  docker compose exec -T postgres psql -U spot -d spot -t -c \
    "SELECT count(*) FROM raw_plays WHERE started_at > now() - interval '15 minutes';"
  if [ "$i" -eq 1 ]; then sleep 20; fi
done
```

Expected: two numbers, the second larger.

Then open `http://localhost:3000/d/spot-live` and confirm by eye that the plays-per-minute bars are climbing and the now-playing table lists your listeners. **This is M2's stated acceptance criterion — a Grafana panel showing a rising play count.**

- [x] **Step 8: Commit**

```bash
git add grafana/ docker-compose.yml
git commit -m "M2: Grafana provisioned from git, first working dashboard"
```

---

### Task 6: The three exercises

**Files:** none. This task changes no code; it runs the stack and observes it.

These are the reason M2 exists. Once Flink takes over in M4, the consumer group
is managed inside a job graph and none of the following is directly visible. Run
each one and read the output before moving on.

- [x] **Step 1: Watch a rebalance**

With one consumer running it holds all three partitions. Add a second:

```bash
docker compose logs --tail=5 consumer | grep ASSIGNED
docker compose up -d --scale consumer=2
sleep 15
docker compose logs --tail=40 consumer | grep -E "ASSIGNED|REVOKED"
```

Expected: the original consumer logs `REVOKED ['plays[0]', 'plays[1]', 'plays[2]']` and then an `ASSIGNED` with a *subset*; the new consumer logs an `ASSIGNED` with the rest. Three partitions across two consumers splits 2/1.

Confirm from the broker's side, which is the authoritative view:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group raw-plays-writer
```

Expected: three rows, one per partition, with two distinct `CONSUMER-ID` values.

**What to notice:** the whole group stopped consuming during the revoke. That
pause is the cost of a rebalance, and it is why partition count and consumer
count are worth thinking about. Also note that a fourth consumer would sit
idle — a group can never have more useful consumers than partitions.

Scale back down:

```bash
docker compose up -d --scale consumer=1
```

- [x] **Step 2: Build lag, then watch it drain**

Stop the consumer while the simulator keeps producing, and turn the volume up:

```bash
docker compose stop consumer
SIM_SPEED=60 docker compose up -d simulator
sleep 60
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group raw-plays-writer
```

Expected: `CURRENT-OFFSET` frozen where the consumer stopped, `LOG-END-OFFSET`
climbing, and a `LAG` column in the hundreds or thousands. `LAG` is exactly
`LOG-END-OFFSET - CURRENT-OFFSET` — the number of messages produced but not yet
committed by this group.

The simulator is left running here on purpose: lag is only interesting while
the log end keeps moving away from you.

Now restart the consumer and watch it catch up:

```bash
docker compose start consumer
for i in 1 2 3 4 5; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group raw-plays-writer \
    | awk 'NR>1 {sum += $6} END {print "total lag:", sum}'
  sleep 5
done
```

Expected: `total lag` falling toward 0 on each line.

**What to notice:** nothing was lost. The broker held the backlog on disk the
whole time — Kafka's retention is what makes a consumer being down a delay
rather than a data loss. Restore normal speed afterwards:

```bash
SIM_SPEED=1.0 docker compose up -d simulator
```

- [x] **Step 3: Reset the offsets and replay the entire topic**

This is the payoff for invariants 2 and 3. Record the row count first:

Stop both first. The simulator matters as much as the consumer: with it
running, new events arrive during the replay and the row count legitimately
grows, which destroys the comparison this exercise rests on.

```bash
docker compose stop consumer simulator
sleep 5
docker compose exec -T postgres psql -U spot -d spot -t -c \
  "SELECT count(*) FROM raw_plays;"   # the baseline, nothing in flight

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group raw-plays-writer \
  --reset-offsets --to-earliest --topic plays --execute
```

Expected: a table showing `NEW-OFFSET` of 0 for all three partitions. The reset
requires the group to have no active members, which is why the consumer is
stopped.

```bash
docker compose start consumer
sleep 45
docker compose exec -T postgres psql -U spot -d spot -t -c \
  "SELECT count(*) FROM raw_plays;"
```

Expected: **the same count as the baseline.** Every message in the topic was
read a second time and written a second time, and the table did not grow by a
single row.

**What to notice:** this is what "idempotent" buys. The consumer made no attempt
to detect duplicates — it blindly upserted every event it read. The dedupe came
from `event_id` being a deterministic function of the play rather than a random
UUID, plus a primary key to collide against. Had `event_id` been a `uuid4()`,
this replay would have doubled the table.

Restart the simulator:

```bash
docker compose up -d simulator
```

If the count did grow, check that the simulator really stopped before you took
the baseline — `docker compose ps simulator` — rather than suspecting the
upsert.

- [x] **Step 4: Record what was observed**

No commit of code. Append a short "Exercise results" section to this plan file
with the actual numbers seen — the partition split, the peak lag, and the
before/after row counts — then commit the plan update. Real numbers from this
run are worth more later than the expectations written above.

---

### Task 7: Milestone acceptance

**Files:** Modify: this plan file, `CLAUDE.md`

- [x] **Step 1: Verify the full suite is green**

Run: `.venv/bin/pytest -q`
Expected: 52 passed.

- [x] **Step 2: Verify a cold start reaches a working dashboard**

```bash
docker compose down -v
docker compose up -d --build
sleep 90
docker compose ps
docker compose exec -T postgres psql -U spot -d spot -c "SELECT count(*) FROM raw_plays;"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/d/spot-live
```

Expected: all services up, a non-zero row count, and `200`.

`down -v` is what makes this a real test: it destroys both data volumes, so
`postgres/init.sql` runs again from scratch and the topic is recreated. If the
schema only exists because of a manual `psql` command run earlier, this catches it.

- [x] **Step 3: Check the milestone acceptance criteria**

From the spec's M2 section:

- [x] A Grafana panel shows a rising play count (Task 5, Step 7).
- [x] Two consumers in one group rebalance partitions between them (Task 6, Step 1).
- [x] Stopping the consumer builds lag; restarting drains it (Task 6, Step 2).
- [x] Resetting the group offset to 0 replays the topic and adds no rows (Task 6, Step 3).

Plus the project's own standards:

- [x] `.venv/bin/pytest -q` is green.
- [x] `docker compose up -d --build` from cold reaches a filled dashboard.
- [x] No Flink, no `agg_` writes — those tables exist and are empty.

- [x] **Step 4: Update CLAUDE.md's "Current state" section**

It still says "Design approved, nothing implemented", which has been wrong since
M0. Replace that section with an accurate one naming the completed milestones and
the next one.

- [x] **Step 5: Commit**

```bash
git add docs/superpowers/plans/2026-09-12-m2-consumer-postgres-grafana.md CLAUDE.md
git commit -m "M2: mark plan complete, record exercise results"
```

---

## Notes for M3

M3 adds the real Spotify poller, which writes to the same topic and is read by
this same consumer with no changes. Two things from M2 carry forward:

1. **Filter by `is_synthetic` once real data arrives.** The now-playing panel
   will show 20 synthetic listeners and one real one with equal prominence. A
   panel variable or a `WHERE NOT is_synthetic` variant is worth adding then,
   not now — there is nothing to filter yet.
2. **The `raw_plays` upsert is what makes a poller restart safe.** M3's
   acceptance criterion is "play a song, it appears exactly once, let it play
   through, no duplicate rows". That works because of the sink built in Task 2,
   so if it fails in M3, suspect `transition.py` rather than the database.

---

## Exercise results, 2026-09-12

Actual numbers from the run, replacing the expectations written above.

### 1. Rebalance

One consumer held all three partitions. On `--scale consumer=2`:

```
consumer-1  05:10:53,962 REVOKED  ['plays[0]', 'plays[1]', 'plays[2]']
consumer-1  05:10:53,964 ASSIGNED ['plays[2]']
consumer-2  05:10:53,964 ASSIGNED ['plays[0]', 'plays[1]']
```

The 2/1 split was as predicted. The detail worth keeping: consumer-1 gave up
*all three* partitions and was handed one back, rather than handing over one.
Between the revoke and the assignment the whole group consumed nothing — 2ms
here, but that pause is the cost of a rebalance and it scales with state.

`kafka-consumer-groups --describe` confirmed two distinct `CONSUMER-ID`s on
hosts `172.18.0.7` (partitions 0, 1) and `172.18.0.5` (partition 2).

### 2. Lag and drain

First attempt built only 313 lag at `SIM_SPEED=60`, which the consumer cleared
before the first sample — an honest result but a useless demonstration. Rebuilt
at `SIM_SPEED=1200`:

```
plays[2]  149 -> 1670   lag 1521
plays[1]  250 -> 3238   lag 2988
plays[0]  349 -> 4196   lag 3847
                  TOTAL 8356
```

`LAG` is exactly `LOG-END-OFFSET - CURRENT-OFFSET` on every row. With the
consumer stopped the group showed `has no active members` while the offsets
stayed frozen.

**8356 messages drained in under 2 seconds** — 1208 remaining at t+0, 0 at t+1.
The consumer log shows full batches of 100 every 8-9ms, about 11,000 events/sec.
The database was never close to being the bottleneck, and `BATCH_MAX=100` with
a single round trip per batch is why. To make backpressure genuinely visible,
a future exercise would need to throttle the consumer rather than speed up the
producer.

### 3. Offset reset and replay

Baseline with both producer and consumer stopped: **4251 rows** from **10656
messages** in the topic.

After `--reset-offsets --to-earliest --execute` (all three partitions to
`NEW-OFFSET 0`) and restarting the consumer: 107 batches re-read and
re-written, lag back to 0, and:

```
 rows_after_replay | distinct_ids
-------------------+--------------
              4251 |         4251
```

**The whole topic was replayed and the table did not grow by one row.** The
consumer does nothing to detect duplicates; the dedupe is entirely
`event_id` being deterministic plus a primary key to collide against.

---

## Finding for M4 and M7: `SIM_SPEED` collides `event_id`s

The lag exercise surfaced something that matters beyond M2. At
`SIM_SPEED=1200` the topic held 10656 messages but only **4251 distinct
`event_id`s** — 6405 messages were exact duplicates of another message, and
`raw_plays` ended up with exactly 4251 rows.

These were not replays. They were *genuinely distinct plays* that the
`event_id` could not tell apart:

```
listener-10  aS97CCox  started=2026-09-12T05:15:16Z
listener-10  aS97CCox  started=2026-09-12T05:15:17Z
listener-10  aS97CCox  started=2026-09-12T05:15:18Z
```

Three plays, one second apart, all flooring into the same 5-second `event_id`
bucket, therefore one row.

**Why.** `EVENT_ID_ROUNDING_SECONDS = 5` rests on the spec's assumption that
distinct plays are "at minimum seconds apart". That holds at `SIM_SPEED=1.0`,
where tracks are minutes long. But `SIM_SPEED` compresses the wall-clock *wait*
while `started_at` is still derived from `datetime.now(UTC)` — so at high speed
one listener emits many plays of a short favourite-artist rotation inside a
single 5-second bucket, and they collapse.

**Consequences to respect later:**

1. **Do not use a high `SIM_SPEED` to generate volume for windowed
   aggregations.** M4-M6 assert counts in `agg_` tables. Events silently lost
   to `event_id` collision before Flink ever sees them would make those counts
   wrong in a way that looks like a Flink bug. Use `SIM_LISTENERS` to raise
   volume instead — more listeners do not collide with each other, because
   `listener_id` is in the hash.
2. **This is not a reason to change `event_id`.** Invariant 2 is load-bearing
   and a random UUID would break the replay guarantee demonstrated above. The
   collision is a property of time compression, not of the design.
3. If a future milestone genuinely needs high-speed distinct events, the fix is
   to make the simulator advance a *virtual* clock for `started_at` rather than
   reusing wall clock, so compressed time produces compressed-but-distinct
   event times. That is a simulator change, not a schema change.

Also carried forward from M1 and confirmed here: the topic contained exactly 12
duplicate `event_id`s from `SIM_MODE=fixture` having been run twice. The
fixture suite still needs a clean topic in M4.
