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
