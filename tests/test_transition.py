import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from poller.transition import NowPlaying, snapshot

UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures" / "spotify"
OBSERVED = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def test_snapshot_reads_every_field_of_a_playing_track():
    now = snapshot(fixture("playing"), OBSERVED)
    assert now == NowPlaying(
        track_id="3n3Ppam7vgaVa1iaRUc9Lp",
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        progress_ms=9000,
        is_playing=True,
        observed_at=OBSERVED,
    )


def test_snapshot_of_a_204_is_nothing_playing():
    # spotify_client turns 204 No Content into None rather than an error.
    assert snapshot(None, OBSERVED) is None


@pytest.mark.parametrize("name", ["advert", "local_file", "episode"])
def test_snapshot_of_a_non_track_is_nothing_playing(name):
    # An advert has no item, a local file has no id, an episode is not a
    # track. None of the three is an error and none is a play.
    assert snapshot(fixture(name), OBSERVED) is None


def test_snapshot_takes_the_first_artist_of_a_collaboration():
    # The event schema has one artist_name. Spotify lists every credited
    # artist; the first is the primary one.
    payload = fixture("playing")
    payload["item"]["artists"].append({"id": "x", "name": "Guest", "type": "artist"})
    assert snapshot(payload, OBSERVED).artist_name == "The Killers"


def test_snapshot_survives_a_paused_track_with_no_progress():
    payload = fixture("playing")
    payload["is_playing"] = False
    payload["progress_ms"] = None
    now = snapshot(payload, OBSERVED)
    assert now.is_playing is False
    assert now.progress_ms == 0


# --- transition(): the spec's state table, one test per row ----------------

from poller.transition import REPLAY_THRESHOLD_MS, PlayState, transition


def playing(track_id="T", progress_ms=0, at_seconds=0, is_playing=True):
    """A NowPlaying at OBSERVED + at_seconds. Every test builds its input here
    so the only things that vary between tests are the things under test."""
    return NowPlaying(
        track_id=track_id,
        track_name=f"track {track_id}",
        artist_name=f"artist of {track_id}",
        album_name=f"album of {track_id}",
        duration_ms=222075,
        progress_ms=progress_ms,
        is_playing=is_playing,
        observed_at=OBSERVED + timedelta(seconds=at_seconds),
    )


def test_nothing_to_playing_emits_a_new_play():
    event, state = transition(None, playing("T", progress_ms=9000), "nishad")

    assert event is not None
    assert event.track_id == "T"
    assert event.listener_id == "nishad"
    assert event.is_synthetic is False
    # started_at is derived, not observed: a poll that catches a track 9
    # seconds in tells us it actually began 9 seconds ago.
    assert event.started_at == OBSERVED - timedelta(seconds=9)
    assert event.observed_at == OBSERVED
    assert state == PlayState("T", OBSERVED - timedelta(seconds=9), 9000)


def test_same_track_advancing_emits_nothing_and_keeps_the_original_start():
    _, first = transition(None, playing("T", progress_ms=9000), "nishad")
    event, state = transition(first, playing("T", progress_ms=19000, at_seconds=10), "nishad")

    assert event is None
    # The whole point: one play has one started_at, derived once. Recomputing
    # it every poll would drift it across event_id buckets.
    assert state.started_at == first.started_at
    assert state.progress_ms == 19000


def test_a_backwards_jump_over_the_threshold_is_a_replay():
    _, first = transition(None, playing("T", progress_ms=120000), "nishad")
    event, state = transition(first, playing("T", progress_ms=2000, at_seconds=10), "nishad")

    assert event is not None
    assert event.track_id == "T"
    assert state.started_at == OBSERVED + timedelta(seconds=10) - timedelta(seconds=2)


def test_a_backwards_jump_under_the_threshold_is_jitter():
    # progress_ms and the poll timestamp are sampled a moment apart, so a
    # slow response can report slightly less progress than the one before.
    _, first = transition(None, playing("T", progress_ms=120000), "nishad")
    event, state = transition(first, playing("T", progress_ms=117000, at_seconds=1), "nishad")

    assert event is None
    assert state.started_at == first.started_at


def test_the_threshold_is_ten_seconds():
    assert REPLAY_THRESHOLD_MS == 10_000


def test_a_different_track_emits():
    _, first = transition(None, playing("T", progress_ms=9000), "nishad")
    event, state = transition(first, playing("U", progress_ms=1000, at_seconds=60), "nishad")

    assert event.track_id == "U"
    assert state.track_id == "U"


def test_pausing_emits_nothing_and_retains_the_track():
    _, first = transition(None, playing("T", progress_ms=9000), "nishad")
    event, state = transition(first, playing("T", progress_ms=9000, at_seconds=10,
                                             is_playing=False), "nishad")

    assert event is None
    assert state == first


def test_resuming_after_a_pause_emits_nothing():
    _, first = transition(None, playing("T", progress_ms=9000), "nishad")
    _, paused = transition(first, playing("T", progress_ms=9000, at_seconds=10,
                                          is_playing=False), "nishad")
    event, state = transition(paused, playing("T", progress_ms=12000, at_seconds=300), "nishad")

    # Retaining state across the pause is exactly what stops a
    # paused-then-resumed track being counted twice.
    assert event is None
    assert state.started_at == first.started_at


def test_nothing_playing_retains_the_track():
    # A 204, an advert, or a failed request the client turned into None.
    _, first = transition(None, playing("T", progress_ms=9000), "nishad")
    event, state = transition(first, None, "nishad")

    assert event is None
    assert state == first


def test_a_pause_with_no_state_stays_empty():
    # The row the spec's table does not have. Paused emits nothing and leaves
    # state alone, and "alone" is allowed to be None -- so a poller started
    # against a paused track simply stays empty.
    event, state = transition(None, playing("T", progress_ms=45000, is_playing=False), "nishad")

    assert event is None
    assert state is None


def test_pressing_play_on_that_paused_track_is_an_ordinary_first_sighting():
    _, state = transition(None, playing("T", progress_ms=45000, is_playing=False), "nishad")
    event, state = transition(state, playing("T", progress_ms=45000, at_seconds=10), "nishad")

    assert event is not None
    assert event.track_id == "T"


def test_repeat_one_emits_a_second_play():
    # The track ended and started again. progress_ms falls from near the
    # duration to near zero, which is the replay rule earning its keep on a
    # case nobody seeked through.
    _, first = transition(None, playing("T", progress_ms=219000), "nishad")
    event, state = transition(first, playing("T", progress_ms=3000, at_seconds=10), "nishad")

    assert event is not None
    assert state.started_at != first.started_at


def test_a_restart_mid_song_reproduces_the_same_event_id():
    # The poller holds state in memory, so a restart re-emits the current
    # track. Both derivations of started_at land on the same instant, so the
    # ids collide and the upsert in Postgres makes the duplicate a no-op.
    first, _ = transition(None, playing("T", progress_ms=0), "nishad")
    reemitted, _ = transition(None, playing("T", progress_ms=90000, at_seconds=90), "nishad")

    assert reemitted.event_id == first.event_id


def test_the_replay_event_gets_an_id_of_its_own():
    # A replay must NOT collide, or the upsert would swallow it. It cannot:
    # a backwards jump over 10s puts the recomputed started_at at least 10s
    # later than the original, which is a different 5-second bucket.
    first, state = transition(None, playing("T", progress_ms=120000), "nishad")
    replay, _ = transition(state, playing("T", progress_ms=0, at_seconds=30), "nishad")

    assert replay.event_id != first.event_id


def test_a_restart_can_straddle_a_bucket_boundary_and_duplicate():
    """A known and accepted limit, recorded here rather than hidden.

    event_id floors started_at into 5-second buckets, and a restart
    re-derives started_at from a fresh observation. The two derivations
    differ by network jitter, so a play whose true start sits near a bucket
    edge can fall either side of it and write a second row. Roughly one
    restart in twenty. The fix would be a coarser bucket, which would start
    merging genuinely distinct plays -- a worse trade.
    """
    on_the_edge = datetime(2026, 9, 21, 12, 0, 4, tzinfo=UTC)
    first, _ = transition(None, NowPlaying(
        "T", "n", "a", "al", 222075, 0, True, on_the_edge), "nishad")
    # The same play, re-derived a second late after a restart 30s in.
    later, _ = transition(None, NowPlaying(
        "T", "n", "a", "al", 222075, 29000, True,
        on_the_edge + timedelta(seconds=30)), "nishad")

    assert first.event_id != later.event_id
