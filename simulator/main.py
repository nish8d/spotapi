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
