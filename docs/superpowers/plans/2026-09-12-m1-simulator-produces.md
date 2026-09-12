# M1 — Simulator Produces: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce well-formed `PlayEvent` JSON onto the `plays` topic from a simulator of N virtual listeners, keyed so that one listener's events always land on one partition.

**Architecture:** `events/schema.py` defines the event and is imported by everything else — it is the single source of truth for the event shape. `producer/kafka_sink.py` wraps `confluent_kafka.Producer` and is the only place that knows the message key is `listener_id`. `simulator/` draws tracks from a cached catalog and drives N listeners on one scheduling loop. The simulator runs as a Compose service built from a shared Python image; unit tests run on the host against a venv.

**Tech Stack:** Python 3.12, `confluent-kafka`, `pytest`, `python:3.12-slim` base image.

**Spec:** `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` (see "Event model", "`events/schema.py`", "`simulator/main.py`", "`simulator/catalog.py`", and "M1 — Simulator produces")

## Global Constraints

- **Partition key is `listener_id`.** Never `track_id`. Kafka orders only within a partition, and session windows (M6) depend on one listener's plays staying ordered on one partition.
- **`event_id` is deterministic:** `sha1(listener_id | track_id | started_at rounded down to 5s)`. Never a random UUID — a poller restart must re-emit an identical ID so the duplicate upserts to a no-op.
- **`started_at` is the event-time column**, derived as `observed_at - progress_ms`. `observed_at` is retained only so poll drift can be charted; never window on it.
- Event JSON field order and names come from the spec's example and must match it exactly:
  `event_id, listener_id, is_synthetic, track_id, track_name, artist_name, album_name, duration_ms, started_at, observed_at`
- Timestamps serialize as `%Y-%m-%dT%H:%M:%SZ` (UTC, second precision, literal `Z`), e.g. `2026-09-11T10:03:22Z`. Flink's `'json.timestamp-format.standard' = 'ISO-8601'` parses this.
- All datetimes crossing a module boundary are **timezone-aware UTC**. Naive datetimes raise rather than being guessed at.
- Synthetic events carry `is_synthetic: true` so real and simulated data separate in any query.
- Broker addresses: `kafka:9092` from inside Compose, `localhost:29092` from the host. Topic is `plays`.
- Host port 5432 is taken by an unrelated local Postgres; when Postgres arrives in M2 it maps to `55432:5432`.
- Do not jump ahead: no Postgres, no consumer, no Grafana, no Flink. M1 ends when JSON lands on the topic correctly keyed.

### Deviations from the spec, and why

1. **The catalog is hand-authored, not fetched.** The spec has `simulator/catalog.py` cache results from the Spotify search API. Spotify credentials do not exist until M3, so `simulator/catalog.json` is written by hand with real artist/track/album names and deterministic Spotify-shaped IDs (22-char base62). The shape is identical to what a fetch would produce, so M3 can regenerate the file without any code change. Track IDs are derived from a hash of `artist|track`, so they are stable and obviously not real Spotify IDs.
2. **Second-precision timestamps.** Everything here derives from a 10-second poll, windows are 60s, and `event_id` rounds to 5s, so sub-second precision carries no information.

---

## File Structure

| File | Responsibility |
|---|---|
| `requirements.txt` | Create: runtime deps (`confluent-kafka`). Installed into the Docker image and the host venv. |
| `requirements-dev.txt` | Create: test deps (`pytest`). Host venv only; never enters the image. |
| `pytest.ini` | Create: puts the repo root on `sys.path` so `events`/`producer`/`simulator` import as top-level packages, and points `pytest` at `tests/`. |
| `Dockerfile` | Create: one shared Python image for every Python service. The simulator uses it in M1; the poller (M3) reuses it unchanged. |
| `.dockerignore` | Create: keeps `.git`, `.venv`, and docs out of the build context. |
| `events/schema.py` | Create: `PlayEvent`, `compute_event_id`, `to_iso`/`from_iso`. Standard library only — no Kafka import, so tests are instant and the module stays importable everywhere. |
| `producer/kafka_sink.py` | Create: the only module that knows the Kafka key is `listener_id`. Wraps `Producer`, serializes via `PlayEvent.to_json()`, logs delivery failures. |
| `simulator/catalog.json` | Create: the cached track catalog, committed to git. |
| `simulator/catalog.py` | Create: `Track` and `load_catalog()`. Reads the JSON; no network. |
| `simulator/main.py` | Create: N virtual listeners on one scheduling loop, `live` and `fixture` modes, env-var configuration. |
| `tests/test_schema.py` | Create: round-trip, field order, `event_id` determinism including the restart case. |
| `tests/test_kafka_sink.py` | Create: key and payload assertions against an injected fake producer — no broker needed. |
| `tests/test_simulator.py` | Create: catalog loading, and that `fixture` mode is byte-identical across runs. |
| `docker-compose.yml` | Modify: add the `simulator` service. |

### Why `events/schema.py` imports nothing but the standard library

Four things import it: both producers, the throwaway M2 consumer, and the tests. If it pulled in `confluent-kafka`, the tests would need a Kafka library to check a JSON round-trip, and the schema would stop being usable as the neutral shared definition. Keeping it stdlib-only is what lets it be the single source of truth rather than one more layer.

---

### Task 1: Project scaffolding and `events/schema.py`

**Files:**
- Create: `requirements.txt`, `requirements-dev.txt`, `pytest.ini`, `.dockerignore`
- Create: `events/__init__.py`, `events/schema.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `PlayEvent` — frozen dataclass, fields in spec order, `started_at`/`observed_at` as aware `datetime`.
  - `PlayEvent.create(*, listener_id, is_synthetic, track_id, track_name, artist_name, album_name, duration_ms, started_at, observed_at) -> PlayEvent` — computes `event_id` for you.
  - `PlayEvent.to_json(self) -> str`, `PlayEvent.from_json(raw: str | bytes) -> PlayEvent`
  - `compute_event_id(listener_id: str, track_id: str, started_at: datetime) -> str` — 40-char sha1 hex.
  - `to_iso(ts: datetime) -> str`, `from_iso(s: str) -> datetime`
  - `EVENT_ID_ROUNDING_SECONDS = 5`

- [ ] **Step 1: Create the venv and dependency manifests**

```bash
cat > requirements.txt <<'EOF'
confluent-kafka==2.6.1
EOF

cat > requirements-dev.txt <<'EOF'
-r requirements.txt
pytest==8.3.4
EOF

cat > pytest.ini <<'EOF'
[pytest]
pythonpath = .
testpaths = tests
EOF

cat > .dockerignore <<'EOF'
.git
.venv
docs
tests
*.md
__pycache__
EOF

python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements-dev.txt
.venv/bin/python -c "import confluent_kafka, pytest; print('deps ok', confluent_kafka.version())"
```
Expected: `deps ok ('2.6.1', ...)`. `.venv/` is already gitignored.

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_schema.py
import json
from datetime import datetime, timedelta, timezone

import pytest

from events.schema import (
    EVENT_ID_ROUNDING_SECONDS,
    PlayEvent,
    compute_event_id,
    from_iso,
    to_iso,
)

UTC = timezone.utc

SPEC_FIELD_ORDER = [
    "event_id", "listener_id", "is_synthetic", "track_id", "track_name",
    "artist_name", "album_name", "duration_ms", "started_at", "observed_at",
]


def make_event(**overrides):
    defaults = dict(
        listener_id="nishad",
        is_synthetic=False,
        track_id="3n3Ppam7vgaVa1iaRUc9Lp",
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        started_at=datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC),
        observed_at=datetime(2026, 9, 11, 10, 3, 31, tzinfo=UTC),
    )
    defaults.update(overrides)
    return PlayEvent.create(**defaults)


def test_round_trip_preserves_every_field():
    event = make_event()
    assert PlayEvent.from_json(event.to_json()) == event


def test_json_has_exactly_the_spec_fields_in_order():
    payload = json.loads(make_event().to_json())
    assert list(payload) == SPEC_FIELD_ORDER


def test_timestamps_serialize_as_utc_with_z_suffix():
    payload = json.loads(make_event().to_json())
    assert payload["started_at"] == "2026-09-11T10:03:22Z"
    assert payload["observed_at"] == "2026-09-11T10:03:31Z"


def test_non_utc_timestamps_are_converted_not_rejected():
    plus_two = timezone(timedelta(hours=2))
    event = make_event(started_at=datetime(2026, 9, 11, 12, 3, 22, tzinfo=plus_two))
    assert json.loads(event.to_json())["started_at"] == "2026-09-11T10:03:22Z"


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError):
        to_iso(datetime(2026, 9, 11, 10, 3, 22))
    with pytest.raises(ValueError):
        compute_event_id("nishad", "t", datetime(2026, 9, 11, 10, 3, 22))


def test_from_iso_round_trips_to_iso():
    ts = datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC)
    assert from_iso(to_iso(ts)) == ts


def test_event_id_is_deterministic():
    ts = datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC)
    assert compute_event_id("nishad", "trk", ts) == compute_event_id("nishad", "trk", ts)


def test_event_id_is_a_sha1_hex_digest():
    ts = datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC)
    event_id = compute_event_id("nishad", "trk", ts)
    assert len(event_id) == 40
    assert set(event_id) <= set("0123456789abcdef")


def test_event_id_ignores_jitter_inside_the_rounding_bucket():
    # 10:03:20 and 10:03:24 both floor to the 10:03:20 bucket.
    base = datetime(2026, 9, 11, 10, 3, 20, tzinfo=UTC)
    jittered = base + timedelta(seconds=EVENT_ID_ROUNDING_SECONDS - 1)
    assert compute_event_id("nishad", "trk", base) == compute_event_id("nishad", "trk", jittered)


def test_event_id_differs_across_rounding_buckets():
    base = datetime(2026, 9, 11, 10, 3, 20, tzinfo=UTC)
    next_bucket = base + timedelta(seconds=EVENT_ID_ROUNDING_SECONDS)
    assert compute_event_id("nishad", "trk", base) != compute_event_id("nishad", "trk", next_bucket)


def test_event_id_separates_listeners_and_tracks():
    ts = datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC)
    assert compute_event_id("a", "trk", ts) != compute_event_id("b", "trk", ts)
    assert compute_event_id("a", "one", ts) != compute_event_id("a", "two", ts)


def test_poller_restart_mid_track_reproduces_the_same_event_id():
    # The poller holds state in memory. After a restart it re-derives
    # started_at as observed_at - progress_ms from a fresh poll, landing a
    # second or two off the original. The duplicate must be a no-op upsert.
    original_started_at = datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC)
    before = make_event(started_at=original_started_at)
    after_restart = make_event(started_at=original_started_at + timedelta(seconds=2))
    assert after_restart.event_id == before.event_id


def test_create_populates_event_id_consistently_with_compute_event_id():
    event = make_event()
    assert event.event_id == compute_event_id(
        event.listener_id, event.track_id, event.started_at
    )
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_schema.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'events'`.

- [ ] **Step 4: Write the implementation**

```python
# events/schema.py
"""The single source of truth for the shape of a play event.

Both producers, the throwaway M2 consumer, and the tests import this module
rather than restating the event shape. It deliberately depends on nothing but
the standard library: a JSON round-trip should not require a Kafka client.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

# started_at is derived as observed_at - progress_ms, and those two are sampled
# a moment apart, so the result jitters by well under a second. Flooring to a
# 5s bucket absorbs that without risking a collision between genuinely distinct
# plays, which are at minimum seconds apart.
EVENT_ID_ROUNDING_SECONDS = 5

_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def to_iso(ts: datetime) -> str:
    """Serialize an aware datetime as UTC, second precision, literal Z."""
    if ts.tzinfo is None:
        raise ValueError(f"timestamp must be timezone-aware, got naive {ts!r}")
    return ts.astimezone(timezone.utc).strftime(_TS_FORMAT)


def from_iso(text: str) -> datetime:
    """Parse what to_iso produced back into an aware UTC datetime."""
    return datetime.strptime(text, _TS_FORMAT).replace(tzinfo=timezone.utc)


def compute_event_id(listener_id: str, track_id: str, started_at: datetime) -> str:
    """sha1(listener_id | track_id | started_at floored to 5s).

    Deterministic on purpose. The poller keeps state in memory, so a restart
    mid-song re-emits the current track; an identical id plus upsert-on-primary
    -key makes that duplicate a no-op. Never replace this with a random UUID.
    """
    if started_at.tzinfo is None:
        raise ValueError(f"started_at must be timezone-aware, got naive {started_at!r}")
    epoch = int(started_at.astimezone(timezone.utc).timestamp())
    floored = epoch - (epoch % EVENT_ID_ROUNDING_SECONDS)
    bucket = datetime.fromtimestamp(floored, tz=timezone.utc)
    material = f"{listener_id}|{track_id}|{to_iso(bucket)}"
    return hashlib.sha1(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PlayEvent:
    """One play. Field order matches the spec's JSON example exactly."""

    event_id: str
    listener_id: str
    is_synthetic: bool
    track_id: str
    track_name: str
    artist_name: str
    album_name: str
    duration_ms: int
    started_at: datetime   # event time: observed_at - progress_ms
    observed_at: datetime  # wall clock of the poll, for charting drift only

    @classmethod
    def create(
        cls,
        *,
        listener_id: str,
        is_synthetic: bool,
        track_id: str,
        track_name: str,
        artist_name: str,
        album_name: str,
        duration_ms: int,
        started_at: datetime,
        observed_at: datetime,
    ) -> "PlayEvent":
        """Build an event, deriving event_id from the identifying fields."""
        return cls(
            event_id=compute_event_id(listener_id, track_id, started_at),
            listener_id=listener_id,
            is_synthetic=is_synthetic,
            track_id=track_id,
            track_name=track_name,
            artist_name=artist_name,
            album_name=album_name,
            duration_ms=duration_ms,
            started_at=started_at,
            observed_at=observed_at,
        )

    def to_json(self) -> str:
        payload = asdict(self)
        payload["started_at"] = to_iso(self.started_at)
        payload["observed_at"] = to_iso(self.observed_at)
        return json.dumps(payload, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | bytes) -> "PlayEvent":
        payload = json.loads(raw)
        return cls(
            event_id=payload["event_id"],
            listener_id=payload["listener_id"],
            is_synthetic=payload["is_synthetic"],
            track_id=payload["track_id"],
            track_name=payload["track_name"],
            artist_name=payload["artist_name"],
            album_name=payload["album_name"],
            duration_ms=payload["duration_ms"],
            started_at=from_iso(payload["started_at"]),
            observed_at=from_iso(payload["observed_at"]),
        )
```

Also create the empty package marker:
```bash
touch events/__init__.py
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_schema.py -q`
Expected: `13 passed`.

- [ ] **Step 6: Commit**

```bash
git add requirements.txt requirements-dev.txt pytest.ini .dockerignore events/ tests/test_schema.py
git commit -m "M1: events/schema.py, the shared event definition

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `producer/kafka_sink.py`

**Files:**
- Create: `producer/__init__.py`, `producer/kafka_sink.py`
- Test: `tests/test_kafka_sink.py`

**Interfaces:**
- Consumes: `PlayEvent` from `events.schema`.
- Produces:
  - `KafkaSink(bootstrap_servers: str, topic: str = "plays", producer_factory=Producer)` — `producer_factory` exists so tests can inject a fake; production never passes it.
  - `KafkaSink.send(event: PlayEvent) -> None` — keys by `listener_id`, serializes with `to_json()`.
  - `KafkaSink.flush(timeout: float = 10.0) -> int` — returns messages still undelivered.
  - `KafkaSink.delivery_failures: int` — count of failed deliveries seen so far.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_kafka_sink.py
import json
from datetime import datetime, timezone

from events.schema import PlayEvent
from producer.kafka_sink import KafkaSink

UTC = timezone.utc


class FakeProducer:
    """Records produce() calls instead of talking to a broker."""

    def __init__(self, config):
        self.config = config
        self.produced = []
        self.flushed = False

    def produce(self, topic, key, value, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value,
                              "on_delivery": on_delivery})

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def make_event(listener_id="nishad"):
    return PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=True,
        track_id="3n3Ppam7vgaVa1iaRUc9Lp",
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        started_at=datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC),
        observed_at=datetime(2026, 9, 11, 10, 3, 31, tzinfo=UTC),
    )


def build_sink():
    captured = {}

    def factory(config):
        captured["producer"] = FakeProducer(config)
        return captured["producer"]

    sink = KafkaSink("kafka:9092", topic="plays", producer_factory=factory)
    return sink, captured["producer"]


def test_message_key_is_the_listener_id():
    sink, fake = build_sink()
    sink.send(make_event(listener_id="listener-07"))
    assert fake.produced[0]["key"] == b"listener-07"


def test_every_event_from_one_listener_shares_a_key():
    # Same key -> same partition -> ordering preserved for that listener.
    sink, fake = build_sink()
    for _ in range(3):
        sink.send(make_event(listener_id="listener-07"))
    assert {m["key"] for m in fake.produced} == {b"listener-07"}


def test_value_is_the_schema_json():
    sink, fake = build_sink()
    event = make_event()
    sink.send(event)
    assert PlayEvent.from_json(fake.produced[0]["value"]) == event


def test_produces_to_the_configured_topic():
    sink, fake = build_sink()
    sink.send(make_event())
    assert fake.produced[0]["topic"] == "plays"


def test_producer_is_configured_for_durability():
    _, fake = build_sink()
    assert fake.config["bootstrap.servers"] == "kafka:9092"
    assert fake.config["acks"] == "all"


def test_delivery_failures_are_counted_not_raised():
    sink, fake = build_sink()
    sink.send(make_event())
    callback = fake.produced[0]["on_delivery"]
    callback("broker went away", None)
    assert sink.delivery_failures == 1


def test_successful_delivery_does_not_count_as_a_failure():
    sink, fake = build_sink()
    sink.send(make_event())
    callback = fake.produced[0]["on_delivery"]
    callback(None, object())
    assert sink.delivery_failures == 0


def test_flush_delegates_to_the_producer():
    sink, fake = build_sink()
    assert sink.flush(timeout=1.0) == 0
    assert fake.flushed is True
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_kafka_sink.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'producer'`.

- [ ] **Step 3: Write the implementation**

```python
# producer/kafka_sink.py
"""The one place that knows a play event is keyed by listener_id.

Kafka guarantees ordering only within a partition, and the partition is chosen
by hashing the message key. Keying by listener_id therefore puts all of one
listener's plays on one partition in order, which is what session windows (M6)
require. Keying by track_id would scatter them and corrupt sessions silently.
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from events.schema import PlayEvent

log = logging.getLogger(__name__)


class KafkaSink:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "plays",
        producer_factory=Producer,
    ) -> None:
        self._topic = topic
        self.delivery_failures = 0
        self._producer = producer_factory(
            {
                "bootstrap.servers": bootstrap_servers,
                # Wait for the broker to persist before considering a write
                # done. With a single broker this is cheap; the habit matters.
                "acks": "all",
                "compression.type": "snappy",
                # Small batching window: throughput without visible latency.
                "linger.ms": 50,
            }
        )

    def _on_delivery(self, err, msg) -> None:
        # Never raise from the callback: a broker hiccup must not kill the
        # producing loop. Kafka's retention is the safety net, and the
        # deterministic event_id makes a re-send harmless.
        if err is not None:
            self.delivery_failures += 1
            log.error("delivery failed: %s", err)

    def send(self, event: PlayEvent) -> None:
        self._producer.produce(
            topic=self._topic,
            key=event.listener_id.encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        # Serve delivery callbacks without blocking.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until queued messages are delivered. Returns the number still
        in the queue, so 0 means everything landed."""
        return self._producer.flush(timeout)
```

Also create the empty package marker:
```bash
touch producer/__init__.py
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_kafka_sink.py -q`
Expected: `8 passed`.

- [ ] **Step 5: Commit**

```bash
git add producer/ tests/test_kafka_sink.py
git commit -m "M1: Kafka sink, keyed by listener_id

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: The track catalog

**Files:**
- Create: `simulator/__init__.py`, `simulator/catalog.py`, `simulator/catalog.json`
- Test: `tests/test_simulator.py` (catalog tests only; Task 4 appends to this file)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Track` — frozen dataclass: `track_id: str`, `track_name: str`, `artist_name: str`, `album_name: str`, `duration_ms: int`.
  - `load_catalog(path: str | Path | None = None) -> list[Track]` — defaults to `simulator/catalog.json`.
  - `artists(tracks: list[Track]) -> list[str]` — unique artist names, sorted, for building listener preferences.

The IDs are generated from `sha1(artist|track)` rendered in base62 and truncated to 22 characters, which is the shape of a real Spotify track ID. They are stable across regeneration and obviously not real IDs.

- [ ] **Step 1: Generate `simulator/catalog.json`**

```bash
mkdir -p simulator
.venv/bin/python - <<'GEN'
import hashlib, json, pathlib

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def spotify_shaped_id(artist, track):
    """22-char base62, deterministic. Same shape as a real Spotify track id."""
    n = int.from_bytes(hashlib.sha1(f"{artist}|{track}".encode()).digest(), "big")
    out = []
    for _ in range(22):
        n, rem = divmod(n, 62)
        out.append(ALPHABET[rem])
    return "".join(out)

ALBUMS = [
    ("The Killers", "Hot Fuss", [
        ("Mr. Brightside", 222075), ("Somebody Told Me", 197120),
        ("All These Things That I've Done", 302000), ("Smile Like You Mean It", 234000)]),
    ("Radiohead", "In Rainbows", [
        ("15 Step", 237000), ("Bodysnatchers", 242000),
        ("Nude", 255000), ("Weird Fishes/Arpeggi", 318000)]),
    ("Fleetwood Mac", "Rumours", [
        ("Dreams", 257000), ("Go Your Own Way", 223000),
        ("The Chain", 270000), ("Second Hand News", 163000)]),
    ("Kendrick Lamar", "good kid, m.A.A.d city", [
        ("Money Trees", 386000), ("Swimming Pools (Drank)", 313000),
        ("Bitch, Don't Kill My Vibe", 310000), ("Poetic Justice", 300000)]),
    ("Daft Punk", "Discovery", [
        ("One More Time", 320357), ("Digital Love", 301000),
        ("Harder, Better, Faster, Stronger", 224000), ("Something About Us", 232000)]),
    ("Nina Simone", "Pastel Blues", [
        ("Sinnerman", 604000), ("Be My Husband", 149000),
        ("Strange Fruit", 170000), ("Tell Me More and More", 218000)]),
    ("Burial", "Untrue", [
        ("Archangel", 240000), ("Near Dark", 235000),
        ("Ghost Hardware", 320000), ("Etched Headplate", 290000)]),
    ("Talking Heads", "Remain in Light", [
        ("Once in a Lifetime", 259000), ("Born Under Punches", 349000),
        ("Crosseyed and Painless", 286000), ("Houses in Motion", 272000)]),
]

tracks = [
    {"track_id": spotify_shaped_id(artist, title), "track_name": title,
     "artist_name": artist, "album_name": album, "duration_ms": ms}
    for artist, album, songs in ALBUMS for title, ms in songs
]

pathlib.Path("simulator/catalog.json").write_text(
    json.dumps({"tracks": tracks}, indent=2) + "\n"
)
print(f"wrote {len(tracks)} tracks across {len(ALBUMS)} albums")
GEN
```
Expected: `wrote 32 tracks across 8 albums`.

- [ ] **Step 2: Write the failing tests**

```python
# tests/test_simulator.py
from simulator.catalog import Track, artists, load_catalog


def test_catalog_loads_every_track():
    tracks = load_catalog()
    assert len(tracks) == 32
    assert all(isinstance(t, Track) for t in tracks)


def test_track_ids_are_unique_and_spotify_shaped():
    tracks = load_catalog()
    ids = [t.track_id for t in tracks]
    assert len(set(ids)) == len(ids)
    assert all(len(i) == 22 and i.isalnum() for i in ids)


def test_every_track_has_a_plausible_duration():
    for track in load_catalog():
        assert 60_000 < track.duration_ms < 900_000


def test_artists_are_unique_and_sorted():
    names = artists(load_catalog())
    assert names == sorted(set(names))
    assert len(names) == 8
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_simulator.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'simulator.catalog'`.

- [ ] **Step 4: Write the implementation**

```python
# simulator/catalog.py
"""Seed tracks for the simulator, read from a JSON file committed to the repo.

The spec has this cached from the Spotify search API. Credentials do not exist
until M3, so the file is hand-authored with the same shape; M3 can regenerate it
without any change here. No network at simulator runtime either way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CATALOG = Path(__file__).with_name("catalog.json")


@dataclass(frozen=True)
class Track:
    track_id: str
    track_name: str
    artist_name: str
    album_name: str
    duration_ms: int


def load_catalog(path: str | Path | None = None) -> list[Track]:
    payload = json.loads(Path(path or DEFAULT_CATALOG).read_text())
    return [Track(**entry) for entry in payload["tracks"]]


def artists(tracks: list[Track]) -> list[str]:
    return sorted({track.artist_name for track in tracks})
```

Also create the empty package marker:
```bash
touch simulator/__init__.py
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_simulator.py -q`
Expected: `4 passed`.

- [ ] **Step 6: Commit**

```bash
git add simulator/__init__.py simulator/catalog.py simulator/catalog.json tests/test_simulator.py
git commit -m "M1: seeded track catalog for the simulator

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `simulator/main.py`, the image, and the Compose service

**Files:**
- Create: `simulator/main.py`, `Dockerfile`
- Modify: `docker-compose.yml` (add the `simulator` service before the `volumes:` block)
- Test: `tests/test_simulator.py` (append)

**Interfaces:**
- Consumes: `Track`/`load_catalog`/`artists` from `simulator.catalog`; `PlayEvent` from `events.schema`; `KafkaSink` from `producer.kafka_sink`.
- Produces:
  - `Settings` — frozen dataclass read from the environment: `listeners: int`, `speed: float`, `late_event_rate: float`, `mode: str`, `seed: int`, `bootstrap_servers: str`, `topic: str`.
  - `settings_from_env(env: Mapping[str, str]) -> Settings`
  - `build_listeners(settings, tracks) -> list[Listener]` — each `Listener` has `listener_id: str` and `preferred: list[Track]`.
  - `fixture_events(base: datetime | None = None) -> list[PlayEvent]` — the deterministic set M4's integration tests assert on.
  - `main() -> None` — entry point, `python -m simulator.main`.

**How `SIM_SPEED` works:** it divides the wall-clock wait between events; it does **not** rescale timestamps. `started_at` stays real wall-clock `now`. So raising it makes events arrive faster in real time — which is exactly the dial for building consumer lag on demand — without distorting event time.

**The `fixture` event set:** 12 events over 3 minutes from `2026-01-01T00:00:00Z`, split 5 / 4 / 3 across the three one-minute tumbling windows, across two listeners. M4 asserts those counts.

- [ ] **Step 1: Write the failing tests (append to `tests/test_simulator.py`)**

```python
from datetime import datetime, timedelta, timezone

from events.schema import PlayEvent
from simulator.catalog import load_catalog
from simulator.main import (
    FIXTURE_BASE,
    build_listeners,
    fixture_events,
    settings_from_env,
)

UTC = timezone.utc


def test_settings_have_spec_defaults():
    settings = settings_from_env({})
    assert settings.listeners == 20
    assert settings.speed == 1.0
    assert settings.late_event_rate == 0.0
    assert settings.mode == "live"
    assert settings.bootstrap_servers == "kafka:9092"
    assert settings.topic == "plays"


def test_settings_read_the_environment():
    settings = settings_from_env({
        "SIM_LISTENERS": "3", "SIM_SPEED": "60", "SIM_LATE_EVENT_RATE": "0.25",
        "SIM_MODE": "fixture", "KAFKA_BOOTSTRAP": "localhost:29092",
    })
    assert settings.listeners == 3
    assert settings.speed == 60.0
    assert settings.late_event_rate == 0.25
    assert settings.mode == "fixture"
    assert settings.bootstrap_servers == "localhost:29092"


def test_listener_ids_are_stable_and_distinct():
    settings = settings_from_env({"SIM_LISTENERS": "4"})
    listeners = build_listeners(settings, load_catalog())
    assert [l.listener_id for l in listeners] == [
        "listener-00", "listener-01", "listener-02", "listener-03"]


def test_listener_preferences_are_seeded_and_reproducible():
    settings = settings_from_env({"SIM_LISTENERS": "5", "SIM_SEED": "7"})
    tracks = load_catalog()
    first = build_listeners(settings, tracks)
    second = build_listeners(settings, tracks)
    assert [l.preferred for l in first] == [l.preferred for l in second]


def test_fixture_emits_twelve_events():
    assert len(fixture_events()) == 12


def test_fixture_is_byte_identical_across_runs():
    assert [e.to_json() for e in fixture_events()] == [
        e.to_json() for e in fixture_events()]


def test_fixture_splits_five_four_three_across_tumbling_minutes():
    counts = [0, 0, 0]
    for event in fixture_events():
        minute = int((event.started_at - FIXTURE_BASE).total_seconds()) // 60
        counts[minute] += 1
    assert counts == [5, 4, 3]


def test_fixture_events_stay_inside_the_three_minute_span():
    for event in fixture_events():
        offset = event.started_at - FIXTURE_BASE
        assert timedelta(0) <= offset < timedelta(minutes=3)


def test_fixture_events_are_marked_synthetic():
    assert all(e.is_synthetic for e in fixture_events())


def test_fixture_observed_at_is_never_before_started_at():
    assert all(e.observed_at >= e.started_at for e in fixture_events())


def test_fixture_event_ids_are_unique():
    ids = [e.event_id for e in fixture_events()]
    assert len(set(ids)) == 12


def test_fixture_uses_two_listeners():
    assert {e.listener_id for e in fixture_events()} == {"fixture-a", "fixture-b"}


def test_fixture_events_round_trip_through_the_schema():
    for event in fixture_events():
        assert PlayEvent.from_json(event.to_json()) == event
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_simulator.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'simulator.main'`.

- [ ] **Step 3: Write the implementation**

```python
# simulator/main.py
"""N virtual listeners producing synthetic plays onto the `plays` topic.

Exists so the pipeline has volume before the real Spotify poller lands in M3,
and so specific behaviours can be provoked on demand: raise SIM_SPEED to build
consumer lag, raise SIM_LATE_EVENT_RATE to push events behind the watermark.
"""

from __future__ import annotations

import heapq
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from events.schema import PlayEvent
from producer.kafka_sink import KafkaSink
from simulator.catalog import Track, artists, load_catalog

UTC = timezone.utc
log = logging.getLogger("simulator")

# Anchor for fixture mode. Fixed, in the past, and never "now": the integration
# tests assert exact window counts, which is only possible with fixed input.
FIXTURE_BASE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

# A poll catches a track somewhere in its first few seconds, so observed_at
# trails started_at by up to one poll interval.
POLL_INTERVAL_SECONDS = 10

# Late events land behind the source table's 30s watermark delay on purpose.
LATE_MIN_SECONDS = 35
LATE_MAX_SECONDS = 120


@dataclass(frozen=True)
class Settings:
    listeners: int
    speed: float
    late_event_rate: float
    mode: str
    seed: int
    bootstrap_servers: str
    topic: str


def settings_from_env(env: Mapping[str, str]) -> Settings:
    return Settings(
        listeners=int(env.get("SIM_LISTENERS", "20")),
        speed=float(env.get("SIM_SPEED", "1.0")),
        late_event_rate=float(env.get("SIM_LATE_EVENT_RATE", "0.0")),
        mode=env.get("SIM_MODE", "live"),
        seed=int(env.get("SIM_SEED", "1337")),
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=env.get("KAFKA_TOPIC", "plays"),
    )


@dataclass(frozen=True)
class Listener:
    listener_id: str
    preferred: list[Track]


def build_listeners(settings: Settings, tracks: list[Track]) -> list[Listener]:
    """Give each listener a couple of favourite artists, deterministically.

    Seeded so a restart reproduces the same population — otherwise every
    restart would look like a different set of people.
    """
    names = artists(tracks)
    listeners = []
    for index in range(settings.listeners):
        rng = random.Random(f"{settings.seed}:{index}")
        favourites = rng.sample(names, k=min(2, len(names)))
        preferred = [t for t in tracks if t.artist_name in favourites]
        listeners.append(Listener(f"listener-{index:02d}", preferred))
    return listeners


def _event(listener_id: str, track: Track, started_at: datetime,
           observed_at: datetime) -> PlayEvent:
    return PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=True,
        track_id=track.track_id,
        track_name=track.track_name,
        artist_name=track.artist_name,
        album_name=track.album_name,
        duration_ms=track.duration_ms,
        started_at=started_at,
        observed_at=observed_at,
    )


# (offset seconds from FIXTURE_BASE, listener) -> 5 in minute 0, 4 in minute 1,
# 3 in minute 2. M4's integration tests assert exactly these counts.
FIXTURE_SCHEDULE = [
    (0, "fixture-a"), (12, "fixture-b"), (25, "fixture-a"),
    (37, "fixture-a"), (51, "fixture-b"),
    (63, "fixture-b"), (75, "fixture-a"), (88, "fixture-b"), (99, "fixture-a"),
    (126, "fixture-a"), (141, "fixture-b"), (158, "fixture-b"),
]


def fixture_events(base: datetime | None = None) -> list[PlayEvent]:
    """A fixed event set with hardcoded timestamps, for integration tests."""
    anchor = base or FIXTURE_BASE
    tracks = load_catalog()
    events = []
    for position, (offset, listener_id) in enumerate(FIXTURE_SCHEDULE):
        track = tracks[position % len(tracks)]
        started_at = anchor + timedelta(seconds=offset)
        events.append(_event(listener_id, track, started_at,
                             started_at + timedelta(seconds=3)))
    return events


def run_fixture(sink: KafkaSink) -> None:
    events = fixture_events()
    for event in events:
        sink.send(event)
    remaining = sink.flush()
    log.info("fixture mode: sent %d events, %d undelivered", len(events), remaining)


def run_live(sink: KafkaSink, settings: Settings) -> None:
    """One scheduling loop for every listener, ordered by next play time.

    A heap rather than a thread each: 20 listeners that mostly sleep do not
    need 20 threads, and one loop keeps the ordering easy to reason about.
    """
    listeners = build_listeners(settings, load_catalog())
    rng = random.Random(settings.seed)
    now = time.monotonic()
    # Stagger the first play so they do not all fire at once. The index is a
    # tiebreaker: Listener holds a list and is not orderable, so two entries
    # landing on the same float would otherwise make the heap compare them.
    queue = [(now + rng.uniform(0, 5), index, listener)
             for index, listener in enumerate(listeners)]
    heapq.heapify(queue)

    log.info("live mode: %d listeners, speed x%.1f, late rate %.2f",
             settings.listeners, settings.speed, settings.late_event_rate)

    while True:
        due_at, index, listener = heapq.heappop(queue)
        wait = due_at - time.monotonic()
        if wait > 0:
            time.sleep(wait)

        track = rng.choice(listener.preferred)
        observed_at = datetime.now(UTC)
        if rng.random() < settings.late_event_rate:
            # Behind the watermark on purpose, to exercise late-data handling.
            started_at = observed_at - timedelta(
                seconds=rng.uniform(LATE_MIN_SECONDS, LATE_MAX_SECONDS))
        else:
            started_at = observed_at - timedelta(
                seconds=rng.uniform(0, POLL_INTERVAL_SECONDS))

        sink.send(_event(listener.listener_id, track, started_at, observed_at))

        # SIM_SPEED compresses the wait, not the timestamps: events arrive
        # faster in real time while event time stays truthful.
        gap = (track.duration_ms / 1000.0) / max(settings.speed, 0.001)
        heapq.heappush(queue, (time.monotonic() + gap, index, listener))


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings_from_env(os.environ)
    sink = KafkaSink(settings.bootstrap_servers, topic=settings.topic)

    if settings.mode == "fixture":
        run_fixture(sink)
        return
    if settings.mode != "live":
        raise SystemExit(f"SIM_MODE must be 'live' or 'fixture', got {settings.mode!r}")

    try:
        run_live(sink, settings)
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        sink.flush()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest -q`
Expected: `38 passed` (13 schema + 8 sink + 17 simulator).

- [ ] **Step 5: Write the Dockerfile**

```bash
cat > Dockerfile <<'EOF'
# One image for every Python service. The simulator uses it in M1; the real
# Spotify poller reuses it unchanged in M3.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY events/ events/
COPY producer/ producer/
COPY simulator/ simulator/

# Unbuffered so container logs appear immediately rather than in blocks.
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "simulator.main"]
EOF
```

- [ ] **Step 6: Add the Compose service**

Insert before the `volumes:` block in `docker-compose.yml`:

```yaml
  simulator:
    build: .
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC: plays
      SIM_LISTENERS: 20
      SIM_SPEED: 1.0
      SIM_LATE_EVENT_RATE: 0.0
      SIM_MODE: live
    restart: unless-stopped
```

- [ ] **Step 7: Build and start it**

Run:
```bash
docker compose up -d --build simulator
docker compose logs --tail 5 simulator
```
Expected: a line like `live mode: 20 listeners, speed x1.0, late rate 0.00` and no traceback.

- [ ] **Step 8: Commit**

```bash
git add simulator/main.py Dockerfile docker-compose.yml tests/test_simulator.py
git commit -m "M1: simulator producing synthetic plays to Kafka

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Verify the acceptance criteria against the running topic

**Files:** none — this is verification against the live stack.

**Interfaces:**
- Consumes: the running `simulator` service and the `plays` topic.
- Produces: evidence for both of M1's acceptance criteria.

- [ ] **Step 1: Confirm the consumer sees well-formed JSON (acceptance criterion 1)**

Run:
```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic plays \
  --property print.key=true --property print.partition=true \
  --max-messages 5 --timeout-ms 90000
```
Expected: five lines, each `Partition:<n>\t<listener-id>\t{...json...}`. Every payload parses as JSON and carries all ten spec fields with `"is_synthetic":true`.

- [ ] **Step 2: Confirm one listener's events all land on one partition (acceptance criterion 2)**

Run:
```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic plays --from-beginning \
  --property print.key=true --property print.partition=true \
  --timeout-ms 30000 2>/dev/null \
  | awk -F'\t' '$2 ~ /^listener-/ {print $2, $1}' | sort -u \
  | awk '{print $1}' | uniq -d
```
Expected: **no output.** The pipeline lists each (listener, partition) pair seen; a listener appearing on two partitions would print its id. Empty means every listener stayed on exactly one partition.

Then confirm the keys really are spread over all three partitions, rather than all sitting on one:
```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic plays
```
Expected: all three of `plays:0`, `plays:1`, `plays:2` at non-zero offsets.

- [ ] **Step 3: Confirm fixture mode is deterministic**

Run:
```bash
docker compose run --rm -e SIM_MODE=fixture simulator | tail -2
```
Expected: `fixture mode: sent 12 events, 0 undelivered`. Running it a second time sends the same 12 `event_id`s — which is the point: replaying the fixture is a no-op once the sinks upsert.

- [ ] **Step 4: Commit nothing; record the result**

Verification only. If both criteria hold, M1 is complete.

---

## Milestone acceptance

From the spec's M1 section:

- [ ] The console consumer shows well-formed JSON events (Task 5, Step 1).
- [ ] Messages for a given `listener_id` all land on the same partition (Task 5, Step 2).

Plus the project's own standards:

- [ ] `.venv/bin/pytest -q` is green (38 tests).
- [ ] `docker compose up -d --build` from cold reaches a producing simulator.
