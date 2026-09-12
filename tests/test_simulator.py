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


from datetime import datetime, timedelta, timezone

from events.schema import PlayEvent
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
