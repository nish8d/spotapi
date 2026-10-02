"""The fixture stream through Kafka and Flink into Postgres, rows asserted
exactly.

Runs only with `pytest -m integration`. It brings up its own Compose project,
spot-it, so it never touches the dev stack. Set SPOT_IT_KEEP=1 to leave that
stack running afterwards; its Flink UI is on :18081 and Postgres on :55433.
"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from simulator.main import fixture_events, fixture_stream

pytestmark = pytest.mark.integration

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)
COMPOSE = ["docker", "compose", "-p", "spot-it",
           "-f", "docker-compose.yml", "-f", "docker-compose.it.yml"]
DSN = "postgresql://spot:spot@localhost:55433/spot"
FIXTURE_LISTENERS = ["fixture-a", "fixture-b"]

# The poller never starts here, but Compose interpolates the whole file before
# it looks at which services were asked for, and the poller's
# ${SPOTIFY_CLIENT_ID:?} guard would refuse a clone with no .env.
ENV = {**os.environ,
       "SPOTIFY_CLIENT_ID": "unused-in-integration",
       "SPOTIFY_CLIENT_SECRET": "unused-in-integration"}

# Cold image builds, cluster start, job submission, then 30s of partition
# idleness before the last window can close.
TIMEOUT_SECONDS = 300


def compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], env=ENV, check=True)


def minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def wait_for_count(conn, sql: str, expected: int) -> int:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        count = conn.execute(sql).fetchone()[0]
        if count >= expected or time.monotonic() > deadline:
            return count
        time.sleep(3)


@pytest.fixture(scope="module")
def db():
    # Brings up Kafka, the topic, Postgres and the Flink cluster, and submits
    # every job: flink-submit depends on all of them.
    compose("up", "-d", "--build", "flink-submit")
    compose("run", "--rm", "--build", "-e", "SIM_MODE=fixture", "simulator")
    try:
        with psycopg.connect(DSN, autocommit=True) as conn:
            yield conn
    finally:
        if not os.environ.get("SPOT_IT_KEEP"):
            compose("down", "-v")


@pytest.fixture(scope="module")
def windows(db):
    """Every fixture window, once all six have closed."""
    wait_for_count(db, "SELECT count(*) FROM agg_plays_per_minute "
                       "WHERE listener_id IN ('fixture-a', 'fixture-b')", 6)
    return db.execute(
        "SELECT window_start, window_end, listener_id, play_count "
        "FROM agg_plays_per_minute WHERE listener_id = ANY(%s) "
        "ORDER BY window_start, listener_id",
        (FIXTURE_LISTENERS,)).fetchall()


def test_tumbling_windows_count_each_play_exactly_once(windows):
    # 5 / 4 / 3 plays across three minutes, split by listener. The replayed
    # first play sits in minute 0 for fixture-a: COUNT(*) would make that 4.
    assert windows == [
        (minute(0), minute(1), "fixture-a", 3),
        (minute(0), minute(1), "fixture-b", 2),
        (minute(1), minute(2), "fixture-a", 2),
        (minute(1), minute(2), "fixture-b", 2),
        (minute(2), minute(3), "fixture-a", 1),
        (minute(2), minute(3), "fixture-b", 2),
    ]


def test_a_window_still_open_is_never_written(db, windows):
    # The flush events' minute, +10, has no later event to close it.
    count = db.execute("SELECT count(*) FROM agg_plays_per_minute "
                       "WHERE window_start >= %s", (minute(3),)).fetchone()[0]
    assert count == 0


def test_passthrough_keeps_one_row_per_event_id(db, windows):
    # 15 messages on the topic, 14 distinct event_ids: the replay collapses.
    distinct_ids = {e.event_id for e in fixture_stream()}
    assert len(distinct_ids) == 14
    rows = db.execute("SELECT event_id FROM raw_plays "
                      "WHERE listener_id LIKE 'fixture-%'").fetchall()
    assert {r[0] for r in rows} == distinct_ids
    assert len(rows) == 14


def test_passthrough_preserves_the_instant(db, windows):
    first = fixture_events()[0]
    started_at, observed_at = db.execute(
        "SELECT started_at, observed_at FROM raw_plays WHERE event_id = %s",
        (first.event_id,)).fetchone()
    assert started_at == T0
    assert observed_at == T0 + timedelta(seconds=3)
