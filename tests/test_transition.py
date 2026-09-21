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
