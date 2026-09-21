# M3 — Real Spotify Poller: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** A `poller` service polls `/me/player/currently-playing` on the owner's real Spotify account every 10 seconds and produces exactly one event per play onto the `plays` topic, so that playing a song makes it appear in `raw_plays` within 10 seconds and playing it through adds no second row.

**Architecture:** `poller/` is a new top-level package of four modules with one responsibility each. `spotify_auth.py` owns credentials: an interactive authorization-code flow run once on the host, then unattended refresh. `spotify_client.py` owns HTTP and nothing else — it returns a decoded body or `None`, and it does not raise. `transition.py` is pure: it normalises one API response into a `NowPlaying` snapshot and decides whether that snapshot represents a new play. `main.py` is the loop and is the only module that imports Kafka. The split exists so that the one subtle piece — which of the ~24 responses for a 4-minute track is a *play* — can be tested exhaustively with no network, no clock, and no broker.

**Tech Stack:** Python 3.12, `requests` 2.32.3 (new), `confluent-kafka` 2.6.1, `pytest` 8.3.4, the existing single service image.

**Spec:** `docs/superpowers/specs/2026-09-11-spotify-streaming-pipeline-design.md` (see "The Spotify API is a state endpoint, not an event stream", `poller/transition.py`, `poller/spotify_auth.py`, `poller/spotify_client.py`, "Error handling", and "M3 — Real Spotify poller")

## Global Constraints

- **Partition key is `listener_id`.** The poller produces through the existing `producer/kafka_sink.py`, which already keys on `listener_id`. Do not add a second produce path and do not pass a key anywhere else.
- **`event_id` is deterministic** — `sha1(listener_id | track_id | started_at floored to 5s)`, computed by `PlayEvent.create`. The poller holds state in memory, so a restart mid-song re-emits the current track; the deterministic id plus upsert-on-primary-key makes that a no-op. Never construct a `PlayEvent` by hand or generate an id any other way.
- **`started_at` is derived, not observed:** `observed_at - progress_ms`. It is the event-time column that drives every watermark from M4 onwards. `observed_at` is the wall clock of the poll and is used for the skew panel only — never for windowing.
- **`started_at` is derived once per play and then retained**, not recomputed each poll. Recomputing it every 10 seconds would let one play drift across 5-second `event_id` buckets and write duplicate rows.
- **Real events carry `is_synthetic: false`.** The simulator's carry `true`. Nothing else distinguishes them.
- **`events/schema.py` stays the single source of truth.** The poller builds events with `PlayEvent.create` and never restates the field list.
- **The loop never exits.** A `204`, an expired token, a `429`, a dropped connection and a JSON decode failure are all the same thing to the loop: no snapshot this poll, state retained, try again in 10 seconds. An exception that clears state would double-count the track that was playing when it happened.
- **No test contacts the live Spotify API, a broker, or a database.** Recorded responses live in `tests/fixtures/spotify/`. `.venv/bin/pytest -q` must stay green and fast.
- Poll interval is 10s. That is 6 requests/minute against a ceiling of roughly 180/minute, so rate limiting should never be reached in normal operation; the `429` path exists because "should never" is not "cannot".
- Secrets live in `.env` and `.spotify_token.json`, both already gitignored. Neither is ever committed, echoed into a log line, or written into the plan's command output.
- The OAuth scope is exactly `user-read-currently-playing`. Do not request more; a scope this narrow cannot read the user's profile, which is why `listener_id` comes from configuration rather than from the API.
- Do not jump ahead: no Flink, no `flink/sql/`, no changes to `consumer/` beyond leaving it running. The three `agg_` tables stay empty.

### Decisions this plan makes that the spec leaves open

1. **The redirect URI is `http://127.0.0.1:8888/callback`.** Spotify permits plain `http` only for explicit-IP loopback addresses, so `localhost` is *not* interchangeable with `127.0.0.1` here — an app registered with `http://localhost:8888/callback` is rejected at authorize time with `INVALID_CLIENT: Invalid redirect URI`. The capture is a one-request `http.server` on the host rather than a copy-paste of the redirect URL.
2. **Response parsing lives in `transition.py`, not in `spotify_client.py`.** `snapshot(payload, observed_at)` is a pure function, so the recorded-response fixtures are tested without an HTTP object anywhere in sight, and the client stays purely about status codes and retries. The client depends on nothing in `transition.py`; `main.py` composes the two.
3. **`PlayState` is three fields** — `track_id`, `started_at`, `progress_ms`. No track metadata, because an event is only ever emitted on a poll that has a live `NowPlaying` to build it from. State never has to reconstruct a track on its own.
4. **"Nothing → paused T" needs no rule of its own.** Paused emits nothing and leaves state exactly as it was, and "as it was" is allowed to be `None`. A poller started against a paused track therefore stays empty, and pressing play becomes an ordinary first sighting. This is one branch covering both of the spec's pause rows plus the row it does not list, with no extra state flag.
5. **An advert, a podcast episode and a local file are all "nothing playing".** They are not errors and not pauses: `snapshot()` returns `None` for each, so state is retained and no event is emitted. A local file has no Spotify `id`, so there is nothing to key an event on.
6. **`429` sleeps for `Retry-After` and returns `None` rather than retrying inline.** One request per call keeps the loop's behaviour easy to state. The next poll picks up normally.
7. **`401` forces one token refresh and retries once.** If the retry also fails the poll is abandoned until the next tick — never a tight loop against the API.
8. **The poller runs as a Compose service with `.spotify_token.json` bind-mounted read-write.** The token file is rewritten on every refresh and that must survive a restart. The interactive authorization still runs once on the host, before the service is ever started.
9. **`listener_id` comes from the `LISTENER_ID` environment variable, defaulting to `nishad`.** The `user-read-currently-playing` scope cannot read `/me`, and inventing a second scope to learn a name the owner already knows would be a poor trade.

---

## File Structure

| File | Responsibility |
|---|---|
| `poller/__init__.py` | Create: empty, makes `poller` a package. |
| `poller/transition.py` | Create: `NowPlaying`, `PlayState`, `snapshot()`, `transition()`. Pure. Imports `events.schema` and the standard library, nothing else. |
| `poller/spotify_auth.py` | Create: `Tokens`, `TokenStore`, `SpotifyAuth`, and the one-shot interactive flow under `__main__`. The only module that knows the accounts.spotify.com endpoints. |
| `poller/spotify_client.py` | Create: `SpotifyClient.currently_playing()`. The only module that makes an API request. Knows no event shape. |
| `poller/main.py` | Create: `Settings`, `poll_once`, `run`, `main`. The only module that imports `KafkaSink` or calls `datetime.now`. |
| `tests/fixtures/spotify/*.json` | Create: recorded-shape responses — a playing track, an advert, a local file, a podcast episode. |
| `tests/test_transition.py` | Create: every row of the spec's state table, the rows it omits, and the restart-determinism check. Written first. |
| `tests/test_spotify_auth.py` | Create: expiry arithmetic, refresh triggering, refresh-token retention, file round-trip. Injected fakes. |
| `tests/test_spotify_client.py` | Create: 200/204/401/429/5xx/exception paths against a fake getter and a fake sleep. |
| `tests/test_poller_main.py` | Create: settings parsing, and that the loop retains state across a failed poll. |
| `.env.example` | Create: the variable names, committed, with no values. |
| `requirements.txt` | Modify: add `requests==2.32.3`. |
| `Dockerfile` | Modify: add `COPY poller/ poller/`. |
| `docker-compose.yml` | Modify: add the `poller` service. |
| `grafana/dashboards/spot.json` | Modify: add the `synthetic` template variable and a real-account now-playing panel. |

### Why four modules and not one

The poller is about 300 lines and would fit in one file. It gets four because
each has a different reason to be hard: `spotify_auth` is hard because OAuth is
fiddly, `spotify_client` is hard because networks fail, `transition` is hard
because the *logic* is subtle, and `main` is not hard at all. Only the third
deserves exhaustive tests, and it can only get them if it is separable — which
it is only if it has no clock, no socket and no broker in it.

---

### Task 1: Credentials and the dependency

**Files:**
- Create: `.env.example`, `poller/__init__.py`
- Modify: `requirements.txt`

**Interfaces:**
- Consumes: nothing.
- Produces: a gitignored `.env` holding `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET`, a Spotify app whose redirect URI is `http://127.0.0.1:8888/callback`, and `requests` installed in `.venv`.

This is the one task with a manual step in it. Everything after it is code.

- [ ] **Step 1: Register the Spotify app (manual, in a browser)**

1. Open <https://developer.spotify.com/dashboard> and log in with the account whose listening should be tracked.
2. **Create app.** Name and description are free text — `spot` and `local streaming pipeline` will do.
3. **Redirect URI:** enter exactly `http://127.0.0.1:8888/callback` and click **Add**. This must be `127.0.0.1`, not `localhost`: Spotify allows plain `http` only for explicit-IP loopback, and rejects the authorize request with `INVALID_CLIENT: Invalid redirect URI` otherwise.
4. Under **Which API/SDKs are you planning to use**, tick **Web API**.
5. Save, then open **Settings** and copy the **Client ID**, and the **Client secret** from behind *View client secret*.

- [ ] **Step 2: Write `.env.example`, committed**

```bash
cat > .env.example <<'EOF'
# Copy to .env and fill in. .env is gitignored; this file is not.
#
# From https://developer.spotify.com/dashboard -> your app -> Settings.
# The app's redirect URI must be exactly http://127.0.0.1:8888/callback
# (127.0.0.1, not localhost: Spotify permits plain http only for
# explicit-IP loopback).
SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=

# The listener_id real plays are produced under. Also the Kafka partition
# key, so changing it moves this listener to a different partition.
LISTENER_ID=nishad

# Volume dial for the simulator. Leave at 1.0 unless deliberately building
# consumer lag -- see the SIM_SPEED finding at the end of the M2 plan.
SIM_SPEED=1.0
EOF
```

- [ ] **Step 3: Create `.env` from it and fill in the two secrets**

```bash
cp .env.example .env
```

Then edit `.env` and paste the client id and secret. Verify without printing them:

```bash
grep -c '^SPOTIFY_CLIENT_ID=.\+$' .env && grep -c '^SPOTIFY_CLIENT_SECRET=.\+$' .env
git check-ignore -v .env
```

Expected: `1`, `1`, and a line naming `.gitignore` as the reason `.env` is ignored. If `git check-ignore` prints nothing, stop — the secret is about to be committable.

- [ ] **Step 4: Add the HTTP dependency and the package**

```bash
printf 'requests==2.32.3\n' >> requirements.txt
mkdir -p poller tests/fixtures/spotify
touch poller/__init__.py
.venv/bin/pip install -q requests==2.32.3
.venv/bin/python -c "import requests; print(requests.__version__)"
```

Expected: `2.32.3`.

- [ ] **Step 5: Commit (no secrets in the diff)**

```bash
git add .env.example requirements.txt poller/__init__.py
git status --short
git commit -m "M3: Spotify app credentials scaffolding and the requests dependency"
```

`git status --short` must not list `.env`. If it does, stop and fix `.gitignore` before committing.

---

### Task 2: `snapshot()` — one response, normalised

**Files:**
- Create: `tests/fixtures/spotify/playing.json`, `tests/fixtures/spotify/advert.json`, `tests/fixtures/spotify/local_file.json`, `tests/fixtures/spotify/episode.json`
- Create: `poller/transition.py`
- Test: `tests/test_transition.py`

**Interfaces:**
- Consumes: `events.schema` (not yet, but the module will).
- Produces:
  - `NowPlaying(track_id: str, track_name: str, artist_name: str, album_name: str, duration_ms: int, progress_ms: int, is_playing: bool, observed_at: datetime)` — frozen dataclass.
  - `snapshot(payload: dict | None, observed_at: datetime) -> NowPlaying | None`

- [ ] **Step 1: Record the response fixtures**

These are the documented shape of `GET /v1/me/player/currently-playing`, trimmed to the fields that matter. No test contacts the live API, so these files *are* the API as far as the suite is concerned.

```bash
cat > tests/fixtures/spotify/playing.json <<'EOF'
{
  "timestamp": 1789012345678,
  "progress_ms": 9000,
  "is_playing": true,
  "currently_playing_type": "track",
  "item": {
    "id": "3n3Ppam7vgaVa1iaRUc9Lp",
    "name": "Mr. Brightside",
    "duration_ms": 222075,
    "type": "track",
    "uri": "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
    "artists": [
      {"id": "0C0XlULifJtAgn6ZNCW2eu", "name": "The Killers", "type": "artist"}
    ],
    "album": {"id": "4piJq7R3gjUOxnYs6lDCTg", "name": "Hot Fuss", "type": "album"}
  }
}
EOF

# An advert between tracks: the field is present, the item is not.
cat > tests/fixtures/spotify/advert.json <<'EOF'
{
  "timestamp": 1789012355678,
  "progress_ms": 4000,
  "is_playing": true,
  "currently_playing_type": "ad",
  "item": null
}
EOF

# A file added from the listener's own disk. It plays, but it has no
# Spotify id, so there is nothing to key an event on.
cat > tests/fixtures/spotify/local_file.json <<'EOF'
{
  "timestamp": 1789012365678,
  "progress_ms": 15000,
  "is_playing": true,
  "currently_playing_type": "track",
  "item": {
    "id": null,
    "name": "demo-take-3",
    "duration_ms": 180000,
    "type": "track",
    "uri": "spotify:local:::demo-take-3:180",
    "artists": [{"id": null, "name": "", "type": "artist"}],
    "album": {"id": null, "name": "", "type": "album"}
  }
}
EOF

# A podcast. Has an id and a duration, but it is not a track, and this
# pipeline counts plays of tracks.
cat > tests/fixtures/spotify/episode.json <<'EOF'
{
  "timestamp": 1789012375678,
  "progress_ms": 60000,
  "is_playing": true,
  "currently_playing_type": "episode",
  "item": {
    "id": "512ojhOuo1ktJprKbVcKyQ",
    "name": "Some Episode",
    "duration_ms": 2400000,
    "type": "episode",
    "uri": "spotify:episode:512ojhOuo1ktJprKbVcKyQ"
  }
}
EOF
```

- [ ] **Step 2: Write the failing tests**

```bash
cat > tests/test_transition.py <<'EOF'
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
EOF
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_transition.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'poller.transition'`.

- [ ] **Step 4: Write the module**

```bash
cat > poller/transition.py <<'EOF'
"""Turning a state endpoint into an event stream.

/me/player/currently-playing answers "what is playing right now", so polling
every 10 seconds returns a 4-minute track about 24 times. Deciding which of
those 24 answers is a *play* is the only genuinely subtle logic in this
project, so it lives here, with no network, no Kafka and no clock: everything
this module needs arrives as an argument. That is what makes the spec's state
table testable row by row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class NowPlaying:
    """One poll's answer, normalised.

    Only ever built from a response that has a real track in it, so every
    field is known. "Nothing is playing" is represented by None instead --
    see snapshot().
    """

    track_id: str
    track_name: str
    artist_name: str
    album_name: str
    duration_ms: int
    progress_ms: int
    is_playing: bool
    observed_at: datetime


def snapshot(payload: Optional[dict[str, Any]],
             observed_at: datetime) -> Optional[NowPlaying]:
    """Normalise one API response. None means "nothing is playing".

    None covers more than a 204. An advert has no item; a local file has no
    Spotify id to key an event on; a podcast episode is not a track. None of
    the three is an error, and none of them is a play -- the caller treats
    them all the way it treats silence, by leaving state untouched.
    """
    if not payload:
        return None

    item = payload.get("item")
    if not item or payload.get("currently_playing_type") != "track":
        return None

    track_id = item.get("id")
    if not track_id:
        return None

    artists = item.get("artists") or [{}]
    album = item.get("album") or {}
    return NowPlaying(
        track_id=track_id,
        track_name=item.get("name") or "",
        artist_name=artists[0].get("name") or "",
        album_name=album.get("name") or "",
        # `or 0` rather than a default: Spotify sends an explicit null for
        # progress_ms on a paused track often enough that int(None) would
        # crash the loop on an ordinary pause.
        duration_ms=int(item.get("duration_ms") or 0),
        progress_ms=int(payload.get("progress_ms") or 0),
        is_playing=bool(payload.get("is_playing")),
        observed_at=observed_at,
    )
EOF
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_transition.py -q`
Expected: 7 passed (the parametrized case counts as three).

- [ ] **Step 6: Commit**

```bash
git add poller/transition.py tests/test_transition.py tests/fixtures/spotify/
git commit -m "M3: snapshot(), normalising one currently-playing response"
```

---

### Task 3: `transition()` — which poll is a play

**Files:**
- Modify: `poller/transition.py`
- Test: `tests/test_transition.py`

**Interfaces:**
- Consumes: `NowPlaying`, `snapshot()` from Task 2; `PlayEvent.create` from `events/schema.py`.
- Produces:
  - `PlayState(track_id: str, started_at: datetime, progress_ms: int)` — frozen dataclass.
  - `REPLAY_THRESHOLD_MS: int` — `10_000`.
  - `transition(state: PlayState | None, now: NowPlaying | None, listener_id: str) -> tuple[PlayEvent | None, PlayState | None]`

This is the component the spec singles out for strict TDD: every row of its
state table becomes a test, and the tests are written before the function.
Do not write the implementation first and back-fill the tests.

The rows, from the spec, plus the two it does not list:

| Situation | Emit | New state |
|---|---|---|
| Nothing → playing T | new play event for T | T |
| T → T, `progress_ms` advanced | none | T (progress updated) |
| T → T, `progress_ms` jumped backwards >10s | new play event for T (replay) | T |
| T → U | new play event for U | U |
| T → paused | none | T, unchanged |
| paused T → playing T | none | T |
| T → nothing (`204`) | none | T retained |
| *Nothing → paused T* | none | *nothing (stays empty)* |
| *T → T restarted from the top (repeat-one)* | *new play event for T* | *T* |

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_transition.py`:

```bash
cat >> tests/test_transition.py <<'EOF'


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
EOF
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_transition.py -q`
Expected: collection error — `ImportError: cannot import name 'REPLAY_THRESHOLD_MS' from 'poller.transition'`. The Task 2 tests fail with it, because the import is at module level; that is expected and goes green again in Step 4.

- [ ] **Step 3: Write the implementation**

Append to `poller/transition.py`:

```bash
cat >> poller/transition.py <<'EOF'


# A backwards jump smaller than this is jitter, not a seek: progress_ms and
# the poll timestamp are sampled a moment apart, so a slow or redelivered
# response can report slightly less progress than the one before it. Larger
# than this and the listener really did seek backwards, or the track ended
# and started again.
REPLAY_THRESHOLD_MS = 10_000


@dataclass(frozen=True)
class PlayState:
    """What the poller remembers between polls.

    Three fields, deliberately. No track metadata: an event is only ever
    emitted on a poll that has a live NowPlaying to build it from, so state
    never has to reconstruct a track on its own.
    """

    track_id: str
    started_at: datetime  # derived once, at first sight, then left alone
    progress_ms: int      # the previous reading, solely to spot a jump back


def _started_at(now: NowPlaying) -> datetime:
    """observed_at - progress_ms. The event-time column, derived not observed."""
    return now.observed_at - timedelta(milliseconds=now.progress_ms)


def _play(now: NowPlaying, listener_id: str) -> tuple[PlayEvent, PlayState]:
    """Emit an event for this snapshot and make it the new state."""
    started_at = _started_at(now)
    event = PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=False,
        track_id=now.track_id,
        track_name=now.track_name,
        artist_name=now.artist_name,
        album_name=now.album_name,
        duration_ms=now.duration_ms,
        started_at=started_at,
        observed_at=now.observed_at,
    )
    return event, PlayState(now.track_id, started_at, now.progress_ms)


def transition(state: Optional[PlayState], now: Optional[NowPlaying],
               listener_id: str) -> tuple[Optional[PlayEvent], Optional[PlayState]]:
    """Decide whether this poll represents a new play.

    Pure: the same arguments always give the same answer. The caller owns the
    clock, the socket and the producer; this owns the decision.
    """
    if now is None:
        # Nothing playing: a 204, an advert, or a request the client could
        # not complete. Not a reason to forget the current track -- forgetting
        # it is precisely what makes a resumed track count twice.
        return None, state

    if not now.is_playing:
        # Paused emits nothing and leaves state exactly as it was, and "as it
        # was" is allowed to be None. That covers both of the spec's pause
        # rows and the one it omits (a poller started while a track sits
        # paused stays empty, and pressing play falls through below).
        return None, state

    if state is None or state.track_id != now.track_id:
        return _play(now, listener_id)

    if now.progress_ms < state.progress_ms - REPLAY_THRESHOLD_MS:
        # Seeked backwards, or the track ended and restarted on repeat.
        # Either way it is a second play, and it needs an event_id of its
        # own: a jump this large puts the recomputed started_at at least
        # REPLAY_THRESHOLD_MS later, which is a different 5-second bucket.
        return _play(now, listener_id)

    # Same track, still going. Keep the started_at derived at first sight.
    return None, PlayState(state.track_id, state.started_at, now.progress_ms)
EOF
```

The appended code needs three names the Task 2 header does not import. Add them:

```bash
python3 - <<'EOF'
from pathlib import Path
path = Path("poller/transition.py")
text = path.read_text()
text = text.replace(
    "from dataclasses import dataclass\nfrom datetime import datetime\nfrom typing import Any, Optional\n",
    "from dataclasses import dataclass\n"
    "from datetime import datetime, timedelta\n"
    "from typing import Any, Optional\n"
    "\n"
    "from events.schema import PlayEvent\n",
)
path.write_text(text)
EOF
head -20 poller/transition.py
```

Expected: the import block now reads `from datetime import datetime, timedelta` and `from events.schema import PlayEvent`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_transition.py -q`
Expected: 22 passed — the 7 from Task 2 and the 15 added here.

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/pytest -q`
Expected: 74 passed (52 from M2, 22 in tests/test_transition.py).

- [ ] **Step 6: Commit**

```bash
git add poller/transition.py tests/test_transition.py
git commit -m "M3: transition(), the pure state-endpoint-to-events decision"
```

---

### Task 4: `poller/spotify_auth.py` — authorise once, refresh forever

**Files:**
- Create: `poller/spotify_auth.py`
- Test: `tests/test_spotify_auth.py`

**Interfaces:**
- Consumes: `.env` from Task 1; `requests`.
- Produces:
  - `NotAuthorized(RuntimeError)`
  - `Tokens(access_token: str, refresh_token: str, expires_at: datetime)` with `expired(now) -> bool`, `Tokens.from_grant(payload, now, previous=None)`, `to_dict()`, `from_dict(payload)`
  - `TokenStore(path)` with `.path`, `load() -> Tokens | None`, `save(tokens) -> None`
  - `SpotifyAuth(client_id, client_secret, store, poster=None, clock=None)` — `poster` defaults to `requests.post` and `clock` to `datetime.now(UTC)`; has `access_token() -> str`, `force_refresh() -> str`, `exchange_code(code) -> Tokens`
  - `authorize_url(client_id, state) -> str`, `capture_code(expected_state) -> str`, `REDIRECT_URI`
  - A `.spotify_token.json` on the host, holding a working refresh token.

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_spotify_auth.py <<'EOF'
import json
from datetime import datetime, timedelta, timezone

import pytest

from poller.spotify_auth import (
    REDIRECT_URI,
    REFRESH_MARGIN,
    NotAuthorized,
    SpotifyAuth,
    TokenStore,
    Tokens,
    authorize_url,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class FakePoster:
    """Stands in for requests.post. Records what it was asked to send."""

    def __init__(self, *responses):
        self._responses = list(responses) or [FakeResponse()]
        self.calls = []

    def __call__(self, url, data=None, headers=None, timeout=None):
        self.calls.append({"url": url, "data": data, "headers": headers})
        return self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]


def grant(**overrides):
    payload = {"access_token": "access-1", "refresh_token": "refresh-1",
               "expires_in": 3600, "token_type": "Bearer"}
    payload.update(overrides)
    return payload


def make_tokens(**overrides):
    fields = dict(access_token="access-1", refresh_token="refresh-1",
                  expires_at=NOW + timedelta(hours=1))
    fields.update(overrides)
    return Tokens(**fields)


def test_from_grant_turns_expires_in_into_an_absolute_instant():
    tokens = Tokens.from_grant(grant(), NOW)
    assert tokens.expires_at == NOW + timedelta(seconds=3600)


def test_from_grant_keeps_the_previous_refresh_token_when_the_response_omits_it():
    # A refresh grant usually returns no refresh_token, meaning "keep yours".
    # Dropping it would lock the poller out at the next restart.
    previous = make_tokens(refresh_token="the-durable-one")
    payload = grant()
    del payload["refresh_token"]

    tokens = Tokens.from_grant(payload, NOW, previous=previous)

    assert tokens.refresh_token == "the-durable-one"
    assert tokens.access_token == "access-1"


def test_from_grant_refuses_a_response_with_no_refresh_token_at_all():
    payload = grant()
    del payload["refresh_token"]
    with pytest.raises(NotAuthorized):
        Tokens.from_grant(payload, NOW)


def test_a_token_counts_as_expired_inside_the_refresh_margin():
    tokens = make_tokens(expires_at=NOW + REFRESH_MARGIN - timedelta(seconds=1))
    assert tokens.expired(NOW) is True


def test_a_token_outside_the_margin_is_still_good():
    tokens = make_tokens(expires_at=NOW + REFRESH_MARGIN + timedelta(seconds=1))
    assert tokens.expired(NOW) is False


def test_the_token_file_round_trips(tmp_path):
    store = TokenStore(tmp_path / ".spotify_token.json")
    tokens = make_tokens()
    store.save(tokens)
    assert store.load() == tokens


def test_a_missing_token_file_is_not_an_error(tmp_path):
    assert TokenStore(tmp_path / "absent.json").load() is None


def test_a_token_path_that_is_a_directory_says_why(tmp_path):
    # Docker creates a DIRECTORY when it bind-mounts a file that does not
    # exist yet. The message has to name that, or the failure is baffling.
    (tmp_path / ".spotify_token.json").mkdir()
    with pytest.raises(NotAuthorized, match="directory"):
        TokenStore(tmp_path / ".spotify_token.json").load()


def test_access_token_does_not_refresh_a_healthy_token(tmp_path):
    store = TokenStore(tmp_path / "t.json")
    store.save(make_tokens(expires_at=NOW + timedelta(hours=1)))
    poster = FakePoster()
    auth = SpotifyAuth("id", "secret", store, poster=poster, clock=lambda: NOW)

    assert auth.access_token() == "access-1"
    assert poster.calls == []


def test_access_token_refreshes_inside_the_margin_and_persists_the_result(tmp_path):
    store = TokenStore(tmp_path / "t.json")
    store.save(make_tokens(access_token="old", expires_at=NOW + timedelta(minutes=1)))
    poster = FakePoster(FakeResponse(payload=grant(access_token="fresh",
                                                   refresh_token=None)))
    auth = SpotifyAuth("id", "secret", store, poster=poster, clock=lambda: NOW)

    assert auth.access_token() == "fresh"
    assert poster.calls[0]["data"]["grant_type"] == "refresh_token"
    assert poster.calls[0]["data"]["refresh_token"] == "refresh-1"
    # Persisted, so a restart does not have to refresh again immediately.
    assert store.load().access_token == "fresh"


def test_the_client_secret_travels_in_the_authorization_header(tmp_path):
    store = TokenStore(tmp_path / "t.json")
    store.save(make_tokens(expires_at=NOW))
    poster = FakePoster(FakeResponse(payload=grant()))
    auth = SpotifyAuth("id", "secret", store, poster=poster, clock=lambda: NOW)
    auth.access_token()

    assert poster.calls[0]["headers"]["Authorization"].startswith("Basic ")
    assert "secret" not in json.dumps(poster.calls[0]["data"])


def test_force_refresh_refreshes_a_token_that_is_not_due(tmp_path):
    # The 401 path: the token looked fine and the API disagreed.
    store = TokenStore(tmp_path / "t.json")
    store.save(make_tokens(expires_at=NOW + timedelta(hours=1)))
    poster = FakePoster(FakeResponse(payload=grant(access_token="fresh")))
    auth = SpotifyAuth("id", "secret", store, poster=poster, clock=lambda: NOW)

    assert auth.force_refresh() == "fresh"
    assert len(poster.calls) == 1


def test_a_rejected_grant_raises_rather_than_returning_a_broken_token(tmp_path):
    store = TokenStore(tmp_path / "t.json")
    store.save(make_tokens(expires_at=NOW))
    poster = FakePoster(FakeResponse(status_code=400, text="invalid_grant"))
    auth = SpotifyAuth("id", "secret", store, poster=poster, clock=lambda: NOW)

    with pytest.raises(NotAuthorized, match="400"):
        auth.access_token()


def test_an_empty_store_tells_the_operator_what_to_run(tmp_path):
    auth = SpotifyAuth("id", "secret", TokenStore(tmp_path / "absent.json"))
    with pytest.raises(NotAuthorized, match="spotify_auth"):
        auth.access_token()


def test_the_authorize_url_asks_for_exactly_one_scope_on_the_loopback_redirect():
    url = authorize_url("client-123", "state-abc")

    assert url.startswith("https://accounts.spotify.com/authorize?")
    assert "scope=user-read-currently-playing" in url
    assert "state=state-abc" in url
    assert "127.0.0.1" in url and "localhost" not in url
    assert REDIRECT_URI == "http://127.0.0.1:8888/callback"
EOF
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_spotify_auth.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'poller.spotify_auth'`.

- [ ] **Step 3: Write the module**

```bash
cat > poller/spotify_auth.py <<'EOF'
"""Tokens: authorised once by hand, refreshed forever without help.

The authorization-code flow needs a browser exactly once. What it produces is
a refresh token, and that is the durable credential -- it goes into
.spotify_token.json (gitignored) and every access token afterwards is minted
from it with nobody present.

Run the once with `python -m poller.spotify_auth` on the host, before the
poller service is ever started.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import secrets
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import requests

UTC = timezone.utc
log = logging.getLogger("poller.auth")

AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"

# Narrow on purpose. This scope cannot read /me, which is why listener_id is
# configuration rather than something the poller discovers.
SCOPE = "user-read-currently-playing"

# Must match the redirect URI registered on the Spotify app exactly.
# 127.0.0.1 and not localhost: Spotify permits plain http only for
# explicit-IP loopback, and treats the two spellings as different URIs.
REDIRECT_HOST = "127.0.0.1"
REDIRECT_PORT = 8888
REDIRECT_URI = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}/callback"

# Access tokens last an hour. Refreshing with five minutes to spare means a
# slow refresh never races an expiring token in the middle of a poll.
REFRESH_MARGIN = timedelta(minutes=5)


class NotAuthorized(RuntimeError):
    """No usable credential. Someone has to open a browser."""


@dataclass(frozen=True)
class Tokens:
    access_token: str
    refresh_token: str
    expires_at: datetime

    def expired(self, now: datetime) -> bool:
        return now >= self.expires_at - REFRESH_MARGIN

    @classmethod
    def from_grant(cls, payload: dict, now: datetime,
                   previous: Optional["Tokens"] = None) -> "Tokens":
        """Build from a token-endpoint response.

        A refresh grant usually omits refresh_token, which means "keep the one
        you have". Taking the response at face value there would throw away
        the durable credential and lock the poller out at the next restart.
        """
        refresh_token = payload.get("refresh_token") or (
            previous.refresh_token if previous else "")
        if not refresh_token:
            raise NotAuthorized("token response carried no refresh token")
        return cls(
            access_token=payload["access_token"],
            refresh_token=refresh_token,
            expires_at=now + timedelta(seconds=int(payload.get("expires_in", 3600))),
        )

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "Tokens":
        return cls(
            access_token=payload["access_token"],
            refresh_token=payload["refresh_token"],
            expires_at=datetime.fromisoformat(payload["expires_at"]),
        )


class TokenStore:
    """The gitignored JSON file the refresh token lives in."""

    def __init__(self, path) -> None:
        self.path = Path(path)

    def load(self) -> Optional[Tokens]:
        if self.path.is_dir():
            raise NotAuthorized(
                f"{self.path} is a directory. Docker creates a directory when it "
                "bind-mounts a file that does not exist yet -- remove it, run "
                "`python -m poller.spotify_auth` on the host, and start the "
                "service again."
            )
        if not self.path.exists():
            return None
        return Tokens.from_dict(json.loads(self.path.read_text()))

    def save(self, tokens: Tokens) -> None:
        self.path.write_text(json.dumps(tokens.to_dict(), indent=2))
        # The refresh token is the credential. Nobody else on the machine
        # needs to be able to read it.
        os.chmod(self.path, 0o600)


class SpotifyAuth:
    """Hands out a valid access token, refreshing when one is due.

    poster and clock are injected so the tests can exercise the expiry
    arithmetic without a network or a wait.
    """

    def __init__(self, client_id: str, client_secret: str, store: TokenStore,
                 poster=None, clock=None) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._store = store
        self._post = poster or requests.post
        self._clock = clock or (lambda: datetime.now(UTC))
        self._tokens: Optional[Tokens] = None

    def access_token(self) -> str:
        tokens = self._current()
        if tokens.expired(self._clock()):
            tokens = self._refresh(tokens)
        return tokens.access_token

    def force_refresh(self) -> str:
        """Refresh whether or not one is due -- the response to a 401."""
        return self._refresh(self._current()).access_token

    def exchange_code(self, code: str) -> Tokens:
        """Trade the one-time authorization code for the durable credential."""
        return self._grant({
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        })

    def _current(self) -> Tokens:
        if self._tokens is None:
            self._tokens = self._store.load()
        if self._tokens is None:
            raise NotAuthorized(
                f"no token file at {self._store.path}. Run "
                "`python -m poller.spotify_auth` on the host once to authorize."
            )
        return self._tokens

    def _refresh(self, tokens: Tokens) -> Tokens:
        log.info("refreshing the access token")
        return self._grant(
            {"grant_type": "refresh_token", "refresh_token": tokens.refresh_token},
            previous=tokens,
        )

    def _grant(self, form: dict, previous: Optional[Tokens] = None) -> Tokens:
        # Client credentials go in the Authorization header rather than the
        # form body: it keeps the secret out of anything that logs a payload.
        raw = f"{self._client_id}:{self._client_secret}".encode("utf-8")
        headers = {"Authorization": "Basic " + base64.b64encode(raw).decode("ascii")}
        response = self._post(TOKEN_URL, data=form, headers=headers, timeout=10)
        if response.status_code != 200:
            raise NotAuthorized(
                f"token endpoint returned {response.status_code}: {response.text[:200]}")
        tokens = Tokens.from_grant(response.json(), self._clock(), previous)
        self._tokens = tokens
        self._store.save(tokens)
        return tokens


# --- the interactive half: run once, on the host ---------------------------


def authorize_url(client_id: str, state: str) -> str:
    query = urllib.parse.urlencode({
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPE,
        "state": state,
    })
    return f"{AUTHORIZE_URL}?{query}"


class _CallbackHandler(BaseHTTPRequestHandler):
    """Serves the one page Spotify redirects the browser to."""

    result: dict = {}

    def do_GET(self) -> None:
        query = urllib.parse.urlparse(self.path).query
        _CallbackHandler.result = {k: v[0]
                                   for k, v in urllib.parse.parse_qs(query).items()}
        body = (b"<html><body style='font-family:sans-serif'>"
                b"<h1>spot</h1><p>Authorized. Close this tab and go back to "
                b"the terminal.</p></body></html>")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        # The default handler logs the request line, which contains the
        # authorization code. Silence it.
        pass


def capture_code(expected_state: str) -> str:
    """Serve requests on the loopback redirect until the real one arrives."""
    _CallbackHandler.result = {}
    with HTTPServer((REDIRECT_HOST, REDIRECT_PORT), _CallbackHandler) as server:
        # A loop rather than a single handle_request(): browsers sometimes
        # ask for /favicon.ico first, and that must not consume the one shot.
        while not _CallbackHandler.result:
            server.handle_request()

    result = _CallbackHandler.result
    if "error" in result:
        raise NotAuthorized(f"Spotify refused: {result['error']}")
    if result.get("state") != expected_state:
        raise NotAuthorized("state mismatch: that redirect answers a different request")
    code = result.get("code")
    if not code:
        raise NotAuthorized(f"no code in the redirect: {sorted(result)}")
    return code


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    client_id = os.environ.get("SPOTIFY_CLIENT_ID", "")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise SystemExit(
            "SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are not set. Run:\n"
            "  set -a; . ./.env; set +a\n"
            "and try again.")

    store = TokenStore(os.environ.get("SPOTIFY_TOKEN_PATH", ".spotify_token.json"))
    state = secrets.token_urlsafe(16)
    url = authorize_url(client_id, state)

    print("Open this URL and approve access:\n")
    print(f"  {url}\n")
    webbrowser.open(url)
    print(f"Waiting for the redirect on {REDIRECT_URI} ...")

    tokens = SpotifyAuth(client_id, client_secret, store).exchange_code(
        capture_code(state))
    print(f"Authorized. Refresh token written to {store.path}; "
          f"this access token is good until {tokens.expires_at:%H:%M:%S} UTC.")


if __name__ == "__main__":
    main()
EOF
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_spotify_auth.py -q`
Expected: 15 passed.

- [ ] **Step 5: Authorise for real (manual, one time)**

```bash
set -a; . ./.env; set +a
.venv/bin/python -m poller.spotify_auth
```

A browser tab opens on accounts.spotify.com. Approve. The tab should end on a
page saying *Authorized*, and the terminal should print where the token went.

Then confirm the file exists, is ignored, and is not world-readable — without
printing its contents:

```bash
ls -l .spotify_token.json
git check-ignore -v .spotify_token.json
.venv/bin/python -c "
import json; d=json.load(open('.spotify_token.json'))
print('keys:', sorted(d)); print('expires_at:', d['expires_at'])"
```

Expected: mode `-rw-------`, a `.gitignore` line, `keys: ['access_token', 'expires_at', 'refresh_token']`, and a timestamp about an hour ahead.

If the browser shows `INVALID_CLIENT: Invalid redirect URI`, the app's
registered redirect URI does not match `http://127.0.0.1:8888/callback`
character for character — check for `localhost`, a missing `/callback`, or a
trailing slash.

- [ ] **Step 6: Commit (the token file must not appear)**

```bash
git add poller/spotify_auth.py tests/test_spotify_auth.py
git status --short
git commit -m "M3: OAuth authorization-code flow and unattended token refresh"
```

`git status --short` must not list `.spotify_token.json` or `.env`.

---

### Task 5: `poller/spotify_client.py` — HTTP that never raises

**Files:**
- Create: `poller/spotify_client.py`
- Test: `tests/test_spotify_client.py`

**Interfaces:**
- Consumes: `SpotifyAuth.access_token()` and `.force_refresh()` from Task 4.
- Produces:
  - `SpotifyClient(auth, getter=None, sleep=time.sleep, rng=None)` — `getter` defaults to `requests.get` and `rng` to `random.Random()`; has `currently_playing() -> dict | None`
  - `CURRENTLY_PLAYING_URL`, `DEFAULT_RETRY_AFTER_SECONDS`, `INITIAL_BACKOFF_SECONDS`, `MAX_BACKOFF_SECONDS`

The contract is one sentence: **it returns a decoded body or `None`, and it
does not raise.** Every failure mode collapses to `None`, which the loop
reads as "no snapshot", which `transition()` answers by retaining state. That
equivalence is what stops a network blip mid-song from double-counting.

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_spotify_client.py <<'EOF'
import random

import pytest

from poller.spotify_auth import NotAuthorized
from poller.spotify_client import (
    DEFAULT_RETRY_AFTER_SECONDS,
    INITIAL_BACKOFF_SECONDS,
    SpotifyClient,
)


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload


class FakeGetter:
    def __init__(self, *responses):
        self._responses = list(responses)
        self.calls = []

    def __call__(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers})
        if not self._responses:
            raise AssertionError("client made more requests than the test allowed")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeAuth:
    def __init__(self, tokens=("token-1", "token-2"), raises=None):
        self._tokens = list(tokens)
        self.raises = raises
        self.refreshes = 0

    def access_token(self):
        if self.raises:
            raise self.raises
        return self._tokens[0]

    def force_refresh(self):
        self.refreshes += 1
        self._tokens.pop(0)
        return self._tokens[0]


class FakeSleep:
    def __init__(self):
        self.slept = []

    def __call__(self, seconds):
        self.slept.append(seconds)


def make_client(*responses, auth=None, sleep=None):
    # rng seeded so the jittered backoff is a fixed number in assertions.
    return SpotifyClient(auth or FakeAuth(), getter=FakeGetter(*responses),
                         sleep=sleep or FakeSleep(), rng=random.Random(0))


def test_a_200_returns_the_decoded_body_with_a_bearer_token():
    client = make_client(FakeResponse(200, {"is_playing": True}))
    assert client.currently_playing() == {"is_playing": True}
    assert client._get.calls[0]["headers"]["Authorization"] == "Bearer token-1"


def test_a_204_is_nothing_playing_and_not_an_error():
    sleep = FakeSleep()
    client = make_client(FakeResponse(204), sleep=sleep)
    assert client.currently_playing() is None
    assert sleep.slept == []  # nothing went wrong, so nothing backs off


def test_a_401_forces_one_refresh_and_retries_once():
    auth = FakeAuth()
    client = make_client(FakeResponse(401), FakeResponse(200, {"is_playing": True}),
                         auth=auth)

    assert client.currently_playing() == {"is_playing": True}
    assert auth.refreshes == 1
    assert client._get.calls[1]["headers"]["Authorization"] == "Bearer token-2"


def test_a_second_401_gives_up_until_the_next_poll():
    # Never a tight loop against the API: two failures, then wait.
    auth = FakeAuth()
    sleep = FakeSleep()
    client = make_client(FakeResponse(401), FakeResponse(401, text="expired"),
                         auth=auth, sleep=sleep)

    assert client.currently_playing() is None
    assert auth.refreshes == 1
    assert len(sleep.slept) == 1


def test_a_429_honours_retry_after():
    sleep = FakeSleep()
    client = make_client(FakeResponse(429, headers={"Retry-After": "17"}), sleep=sleep)

    assert client.currently_playing() is None
    assert sleep.slept == [17.0]


def test_a_429_without_a_header_still_waits():
    sleep = FakeSleep()
    client = make_client(FakeResponse(429), sleep=sleep)

    assert client.currently_playing() is None
    assert sleep.slept == [DEFAULT_RETRY_AFTER_SECONDS]


def test_a_500_backs_off_and_reports_nothing_playing():
    sleep = FakeSleep()
    client = make_client(FakeResponse(503, text="service unavailable"), sleep=sleep)

    assert client.currently_playing() is None
    assert len(sleep.slept) == 1
    assert INITIAL_BACKOFF_SECONDS <= sleep.slept[0] <= 2 * INITIAL_BACKOFF_SECONDS


def test_a_dead_connection_does_not_escape():
    client = make_client(ConnectionError("connection refused"), sleep=FakeSleep())
    assert client.currently_playing() is None


def test_a_body_that_is_not_json_does_not_escape():
    client = make_client(FakeResponse(200, payload=None), sleep=FakeSleep())
    assert client.currently_playing() is None


def test_a_missing_credential_does_not_escape_either():
    # NotAuthorized from the auth layer is still just "no snapshot": the
    # operator sees it in the log, the loop keeps its state.
    auth = FakeAuth(raises=NotAuthorized("no token file"))
    client = SpotifyClient(auth, getter=FakeGetter(), sleep=FakeSleep(),
                           rng=random.Random(0))
    assert client.currently_playing() is None


def test_the_backoff_doubles_while_failing_and_resets_on_success():
    sleep = FakeSleep()
    client = make_client(FakeResponse(503), FakeResponse(503),
                         FakeResponse(204), FakeResponse(503), sleep=sleep)

    client.currently_playing()
    client.currently_playing()
    assert sleep.slept[1] > sleep.slept[0]

    client.currently_playing()          # a 204 means the API is answering
    client.currently_playing()
    assert sleep.slept[2] < sleep.slept[1]
EOF
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_spotify_client.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'poller.spotify_client'`.

- [ ] **Step 3: Write the module**

```bash
cat > poller/spotify_client.py <<'EOF'
"""The one module that makes a request to the Spotify API.

It knows nothing about play events, and it does not raise. Every way a poll
can fail -- an empty 204, an expired token, a rate limit, a refused
connection, a body that will not parse -- comes back as None. The loop reads
None as "no snapshot this time" and transition() answers that by retaining
state, so a blip in the middle of a song cannot double-count it.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Optional

import requests

log = logging.getLogger("poller.client")

CURRENTLY_PLAYING_URL = "https://api.spotify.com/v1/me/player/currently-playing"

# Used when a 429 arrives with no Retry-After header. Spotify always sends
# one; surviving its absence costs a line.
DEFAULT_RETRY_AFTER_SECONDS = 5.0

INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0


class SpotifyClient:
    """getter, sleep and rng are injected so the tests never wait or connect."""

    def __init__(self, auth, getter=None, sleep=time.sleep, rng=None) -> None:
        self._auth = auth
        self._get = getter or requests.get
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._backoff = INITIAL_BACKOFF_SECONDS

    def currently_playing(self) -> Optional[dict[str, Any]]:
        """The decoded body, or None for "nothing to report this poll"."""
        try:
            response = self._fetch(self._auth.access_token())

            if response.status_code == 401:
                # The token looked fine and the API disagreed -- revoked, or
                # a clock that drifted. One refresh, one retry, then wait for
                # the next poll rather than hammering the endpoint.
                log.info("401 from the API, forcing a token refresh")
                response = self._fetch(self._auth.force_refresh())

            if response.status_code == 204:
                # Nothing is playing. Not an error, and the most common
                # answer there is.
                self._backoff = INITIAL_BACKOFF_SECONDS
                return None

            if response.status_code == 200:
                self._backoff = INITIAL_BACKOFF_SECONDS
                return response.json()

            if response.status_code == 429:
                delay = self._retry_after(response)
                log.warning("rate limited, sleeping %.1fs", delay)
                self._sleep(delay)
                return None

            log.warning("unexpected status %s: %s",
                        response.status_code, response.text[:200])
            self._back_off()
            return None

        except Exception as exc:
            # A refused connection, a DNS failure, a timeout, an unparseable
            # body, a missing credential. The loop must outlive all of them.
            log.warning("poll failed: %s: %s", type(exc).__name__, exc)
            self._back_off()
            return None

    def _fetch(self, token: str):
        return self._get(
            CURRENTLY_PLAYING_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )

    @staticmethod
    def _retry_after(response) -> float:
        try:
            return max(float(response.headers.get("Retry-After", "")), 0.0)
        except (TypeError, ValueError):
            return DEFAULT_RETRY_AFTER_SECONDS

    def _back_off(self) -> None:
        # Sleeping here rather than skipping polls upstairs keeps the loop
        # trivial. Jitter so several restarting services do not synchronise.
        self._sleep(self._backoff * (1.0 + self._rng.random()))
        self._backoff = min(self._backoff * 2, MAX_BACKOFF_SECONDS)
EOF
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_spotify_client.py -q`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add poller/spotify_client.py tests/test_spotify_client.py
git commit -m "M3: Spotify HTTP client — 204, 401, 429 and backoff, never raising"
```

---

### Task 6: `poller/main.py` — the loop

**Files:**
- Create: `poller/main.py`
- Test: `tests/test_poller_main.py`

**Interfaces:**
- Consumes: everything from Tasks 2-5, plus `KafkaSink` from `producer/kafka_sink.py`.
- Produces:
  - `Settings(client_id, client_secret, token_path, listener_id, poll_interval, bootstrap_servers, topic)`
  - `settings_from_env(env: Mapping[str, str]) -> Settings`
  - `poll_once(client, sink, state, listener_id, clock=None) -> PlayState | None`
  - `run(client, sink, settings, should_continue=..., sleep=time.sleep, clock=None) -> None`
  - `main() -> None`, the module entry point

- [ ] **Step 1: Write the failing tests**

```bash
cat > tests/test_poller_main.py <<'EOF'
from datetime import datetime, timezone

from poller.main import poll_once, run, settings_from_env
from poller.transition import PlayState

UTC = timezone.utc
NOW = datetime(2026, 9, 21, 12, 0, 0, tzinfo=UTC)

PLAYING = {
    "progress_ms": 9000,
    "is_playing": True,
    "currently_playing_type": "track",
    "item": {
        "id": "T", "name": "Mr. Brightside", "duration_ms": 222075,
        "artists": [{"name": "The Killers"}], "album": {"name": "Hot Fuss"},
    },
}


class FakeClient:
    """Returns each payload in turn, then None for ever after."""

    def __init__(self, *payloads):
        self._payloads = list(payloads)
        self.polls = 0

    def currently_playing(self):
        self.polls += 1
        return self._payloads.pop(0) if self._payloads else None


class FakeSink:
    def __init__(self):
        self.sent = []

    def send(self, event):
        self.sent.append(event)


def test_settings_default_to_the_compose_environment():
    settings = settings_from_env({})
    assert settings.listener_id == "nishad"
    assert settings.poll_interval == 10
    assert settings.bootstrap_servers == "kafka:9092"
    assert settings.topic == "plays"
    assert settings.token_path == ".spotify_token.json"


def test_settings_read_the_environment_when_it_is_set():
    settings = settings_from_env({
        "LISTENER_ID": "someone", "POLL_INTERVAL": "3", "KAFKA_TOPIC": "other"})
    assert settings.listener_id == "someone"
    assert settings.poll_interval == 3.0
    assert settings.topic == "other"


def test_a_first_sighting_produces_one_event():
    client, sink = FakeClient(PLAYING), FakeSink()

    state = poll_once(client, sink, None, "nishad", clock=lambda: NOW)

    assert len(sink.sent) == 1
    assert sink.sent[0].track_name == "Mr. Brightside"
    assert sink.sent[0].is_synthetic is False
    assert state.track_id == "T"


def test_a_failed_poll_keeps_the_state_it_had():
    # The client turns every failure into None. If that cleared state, the
    # track playing when the network blipped would be counted again.
    client, sink = FakeClient(None), FakeSink()
    before = PlayState("T", NOW, 9000)

    after = poll_once(client, sink, before, "nishad", clock=lambda: NOW)

    assert after == before
    assert sink.sent == []


def test_the_loop_polls_until_it_is_told_to_stop():
    client, sink = FakeClient(PLAYING, PLAYING, PLAYING), FakeSink()
    settings = settings_from_env({"POLL_INTERVAL": "0"})
    ticks = [0]

    def should_continue():
        ticks[0] += 1
        return ticks[0] <= 3

    run(client, sink, settings, should_continue=should_continue,
        sleep=lambda seconds: None, clock=lambda: NOW)

    assert client.polls == 3
    # Three polls of the same track at the same instant: one play, not three.
    assert len(sink.sent) == 1
EOF
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_poller_main.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'poller.main'`.

- [ ] **Step 3: Write the module**

```bash
cat > poller/main.py <<'EOF'
"""The polling loop: ask, normalise, decide, produce.

The only module in poller/ that reads a clock, imports Kafka, or looks at the
environment. Everything interesting happens in the three it composes, which
is why this one is short enough to read in a sitting.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Optional

from poller.spotify_auth import NotAuthorized, SpotifyAuth, TokenStore
from poller.spotify_client import SpotifyClient
from poller.transition import PlayState, snapshot, transition
from producer.kafka_sink import KafkaSink

UTC = timezone.utc
log = logging.getLogger("poller")


@dataclass(frozen=True)
class Settings:
    client_id: str
    client_secret: str
    token_path: str
    listener_id: str
    poll_interval: float
    bootstrap_servers: str
    topic: str


def settings_from_env(env: Mapping[str, str]) -> Settings:
    return Settings(
        client_id=env.get("SPOTIFY_CLIENT_ID", ""),
        client_secret=env.get("SPOTIFY_CLIENT_SECRET", ""),
        token_path=env.get("SPOTIFY_TOKEN_PATH", ".spotify_token.json"),
        # The scope cannot read /me, so the listener names itself. This is
        # also the Kafka partition key: changing it moves this listener to a
        # different partition and starts a new session there.
        listener_id=env.get("LISTENER_ID", "nishad"),
        # 10s sees a 4-minute track about 24 times and spends 6 requests a
        # minute against a ceiling near 180.
        poll_interval=float(env.get("POLL_INTERVAL", "10")),
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=env.get("KAFKA_TOPIC", "plays"),
    )


def poll_once(client, sink, state: Optional[PlayState], listener_id: str,
              clock=None) -> Optional[PlayState]:
    """One turn of the loop. Returns the state to carry into the next."""
    clock = clock or (lambda: datetime.now(UTC))
    payload = client.currently_playing()
    # Stamped the moment the response is in hand. observed_at exists so that
    # observed_at - started_at can be charted as poll drift; it never drives
    # a window.
    now = snapshot(payload, clock())
    event, new_state = transition(state, now, listener_id)
    if event is not None:
        sink.send(event)
        log.info("PLAY  %s — %s  (started %s, event_id %s)",
                 event.track_name, event.artist_name,
                 event.started_at.isoformat(), event.event_id[:12])
    return new_state


def run(client, sink, settings: Settings, should_continue=lambda: True,
        sleep=time.sleep, clock=None) -> None:
    """Poll for ever. State lives here, in memory, and only here.

    That is deliberate and is what the deterministic event_id is for: a
    restart re-emits the current track, and the upsert in Postgres turns the
    duplicate into a no-op.
    """
    state: Optional[PlayState] = None
    while should_continue():
        state = poll_once(client, sink, state, settings.listener_id, clock=clock)
        sleep(settings.poll_interval)


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings_from_env(os.environ)

    if not settings.client_id or not settings.client_secret:
        raise SystemExit(
            "SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are not set. Copy "
            ".env.example to .env, fill in the two values from the Spotify "
            "dashboard, and start this service again.")

    store = TokenStore(settings.token_path)
    try:
        if store.load() is None:
            raise SystemExit(
                f"no token at {settings.token_path}. Run "
                "`python -m poller.spotify_auth` on the host once to "
                "authorize, then start this service again.")
    except NotAuthorized as exc:
        # Covers the bind-mounted-directory case, whose message explains it.
        raise SystemExit(str(exc))

    client = SpotifyClient(SpotifyAuth(settings.client_id, settings.client_secret, store))
    sink = KafkaSink(settings.bootstrap_servers, topic=settings.topic)
    running = [True]

    def stop(signum, frame):
        log.info("signal %d received, stopping after this poll", signum)
        running[0] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("polling every %.0fs as listener %r into topic %r",
             settings.poll_interval, settings.listener_id, settings.topic)
    try:
        run(client, sink, settings, should_continue=lambda: running[0])
    finally:
        # Anything produced but not yet delivered goes now. A shutdown can
        # take up to one poll interval, because the sleep is not interrupted.
        sink.flush()
        log.info("stopped")


if __name__ == "__main__":
    main()
EOF
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_poller_main.py -q`
Expected: 5 passed.

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/pytest -q`
Expected: 105 passed (52 from M2, 22 transition, 15 auth, 11 client, 5 main).

- [ ] **Step 6: Commit**

```bash
git add poller/main.py tests/test_poller_main.py
git commit -m "M3: the polling loop, wiring auth, client, transition and the sink"
```

---

### Task 7: Wire the poller into Compose and see a real play land

**Files:**
- Modify: `Dockerfile`, `docker-compose.yml`

**Interfaces:**
- Consumes: `poller/main.py` from Task 6, `.env` from Task 1, `.spotify_token.json` from Task 4.
- Produces: a running `poller` service producing real events onto `plays`, which the existing M2 consumer writes into `raw_plays` with no change of its own.

- [ ] **Step 1: Add the package to the image**

```bash
python3 - <<'EOF'
from pathlib import Path
path = Path("Dockerfile")
text = path.read_text()
text = text.replace(
    "COPY simulator/ simulator/\n",
    "COPY simulator/ simulator/\nCOPY poller/ poller/\n",
)
path.write_text(text)
EOF
grep -n COPY Dockerfile
```

Expected: a `COPY poller/ poller/` line after the simulator one.

- [ ] **Step 2: Add the service**

Insert this after the `simulator` service and before `consumer` in `docker-compose.yml`:

```yaml
  poller:
    build: .
    # The image's CMD runs the simulator; this service runs the real poller.
    command: ["python", "-m", "poller.main"]
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC: plays
      # Compose reads ./.env automatically for substitution, so these come
      # from the same file the host-side authorization step uses. The :? form
      # fails the `up` with this message rather than starting a service that
      # can only crash-loop.
      SPOTIFY_CLIENT_ID: ${SPOTIFY_CLIENT_ID:?set SPOTIFY_CLIENT_ID in .env (copy .env.example)}
      SPOTIFY_CLIENT_SECRET: ${SPOTIFY_CLIENT_SECRET:?set SPOTIFY_CLIENT_SECRET in .env}
      SPOTIFY_TOKEN_PATH: /app/.spotify_token.json
      LISTENER_ID: ${LISTENER_ID:-nishad}
      POLL_INTERVAL: 10
    volumes:
      # Read-write on purpose: the poller rewrites this file every time it
      # refreshes the access token, and that has to outlive the container.
      #
      # The file must already exist. Docker creates a DIRECTORY in its place
      # if it does not, which is why `python -m poller.spotify_auth` runs on
      # the host first -- Task 4, Step 5.
      - ./.spotify_token.json:/app/.spotify_token.json
    restart: unless-stopped
```

- [ ] **Step 3: Check the file the bind mount needs is a file**

```bash
test -f .spotify_token.json && echo "ok: a file" || echo "STOP: run Task 4 Step 5 first"
docker compose config --quiet && echo "compose config valid"
```

Expected: `ok: a file` and `compose config valid`. If `docker compose config`
complains about `SPOTIFY_CLIENT_ID`, `.env` is missing or empty — Task 1.

- [ ] **Step 4: Build and start**

```bash
docker compose up -d --build poller
docker compose ps
```

Expected: `poller` is `Up`. If it is restarting, read the reason:

```bash
docker compose logs poller | tail -20
```

- [ ] **Step 5: Watch it poll while nothing is playing**

```bash
docker compose logs -f poller
```

Expected: a startup line naming the poll interval and listener, then silence.
Silence is correct — a `204` emits nothing and logs nothing. Leave this
running for the next step.

- [ ] **Step 6: Play a song and watch it appear**

Start any track on Spotify, on any device signed into the same account. Within
10 seconds the log should show a single line:

```
PLAY  <track> — <artist>  (started 2026-09-21T..., event_id 1a2b3c4d5e6f)
```

Then check it reached Postgres, which the M2 consumer did with no changes:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT listener_id, track_name, artist_name, started_at, observed_at
FROM raw_plays WHERE NOT is_synthetic ORDER BY started_at DESC LIMIT 5;"
```

Expected: one row, with `observed_at` a few seconds after `started_at` —
that gap is the poll catching the track already in progress, and it is the
quantity the skew panel charts.

- [ ] **Step 7: Let it play through and confirm it stays one row**

Leave the track playing for a minute, then:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT count(*) AS rows, count(DISTINCT event_id) AS ids
FROM raw_plays WHERE NOT is_synthetic;"
```

Expected: `rows` unchanged from Step 6. Roughly six more polls have seen the
same track and emitted nothing, because `transition()` recognised it as the
play already in flight.

- [ ] **Step 8: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "M3: the poller as a Compose service, token file bind-mounted"
```

---

### Task 8: Tell the real listener apart on the dashboard

**Files:**
- Modify: `grafana/dashboards/spot.json`

**Interfaces:**
- Consumes: `raw_plays.is_synthetic`, already written by both producers.
- Produces: a `synthetic` template variable on the dashboard, applied to all three existing panels, plus a real-account panel at the top.

The M2 plan flagged this: one real listener next to twenty synthetic ones,
all with equal prominence, makes "did my song arrive?" needlessly hard to
read. Now that real data exists, it is worth a variable.

- [ ] **Step 1: Edit the dashboard JSON**

```bash
python3 - <<'EOF'
import copy
import json
from pathlib import Path

path = Path("grafana/dashboards/spot.json")
dashboard = json.loads(path.read_text())

# A custom variable rather than a query one: the two values are the whole
# domain of a boolean, and asking Postgres for them every refresh would be
# a query to learn something already known. `label : value` pairs give the
# dropdown readable options; ${synthetic:sqlstring} renders the selection as
# 'false','true', which is why the column is cast to text to compare.
dashboard["templating"] = {
    "list": [
        {
            "name": "synthetic",
            "label": "Source",
            "type": "custom",
            "query": "real : false, synthetic : true",
            "multi": True,
            "includeAll": True,
            "allValue": "false,true",
            "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
            "options": [],
        }
    ]
}

FILTER = "is_synthetic::text IN (${synthetic:sqlstring})"

SQL = {
    1: ("SELECT date_trunc('minute', started_at) AS time, listener_id AS metric, "
        "count(*) AS value FROM raw_plays "
        f"WHERE $__timeFilter(started_at) AND {FILTER} "
        "GROUP BY 1, 2 ORDER BY 1"),
    2: ("SELECT DISTINCT ON (listener_id) listener_id, track_name, artist_name, "
        "is_synthetic, started_at FROM raw_plays "
        f"WHERE {FILTER} "
        "ORDER BY listener_id, started_at DESC"),
    3: ("SELECT date_trunc('minute', observed_at) AS time, "
        "avg(extract(epoch FROM observed_at - started_at)) AS \"avg skew\", "
        "max(extract(epoch FROM observed_at - started_at)) AS \"max skew\" "
        "FROM raw_plays "
        f"WHERE $__timeFilter(observed_at) AND {FILTER} "
        "GROUP BY 1 ORDER BY 1"),
}

by_id = {panel["id"]: panel for panel in dashboard["panels"]}
for panel_id, sql in SQL.items():
    by_id[panel_id]["targets"][0]["rawSql"] = sql

# The real account goes at the top, where the answer to "did my song arrive"
# should be. Everything else shifts down by its height.
NEW_HEIGHT = 5
for panel in dashboard["panels"]:
    panel["gridPos"]["y"] += NEW_HEIGHT

real = copy.deepcopy(by_id[2])
real["id"] = 4
real["title"] = "Now playing — real account"
real["description"] = ("The last ten plays from the Spotify poller, ignoring the "
                       "Source variable on purpose: this panel is the answer to "
                       "'did my song arrive', so it must not be filterable away.")
real["gridPos"] = {"h": NEW_HEIGHT, "w": 24, "x": 0, "y": 0}
real["options"]["sortBy"] = [{"displayName": "started_at", "desc": True}]
real["targets"][0]["rawSql"] = (
    "SELECT started_at, track_name, artist_name, album_name, observed_at "
    "FROM raw_plays WHERE NOT is_synthetic ORDER BY started_at DESC LIMIT 10")
real["fieldConfig"]["overrides"] = [
    {"matcher": {"id": "byName", "options": name},
     "properties": [{"id": "unit", "value": "dateTimeAsIso"}]}
    for name in ("started_at", "observed_at")
]
dashboard["panels"].insert(0, real)

dashboard["version"] = dashboard.get("version", 1) + 1
path.write_text(json.dumps(dashboard, indent=2) + "\n")
print("panels now:", [(p["id"], p["title"], p["gridPos"]["y"]) for p in dashboard["panels"]])
EOF
```

Expected: four panels, the new one at `y: 0`, the others at 5, 14 and 14.

- [ ] **Step 2: Reload and check every panel still returns data**

Grafana's file provider re-reads the dashboard within about ten seconds, so
no restart is needed.

```bash
sleep 15
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/d/spot-live
```

Expected: `200`. Then open <http://localhost:3000/d/spot-live> and check:

1. A **Source** dropdown appears at the top left, with *All*, *real*, *synthetic*.
2. **Now playing — real account** lists the track played in Task 7 and nothing synthetic.
3. Selecting **real** in the dropdown leaves only your listener in *Plays per minute* and *Now playing*.
4. Selecting **synthetic** removes it and leaves the twenty simulated listeners.

If a panel goes blank on *All*, the `sqlstring` interpolation is the thing
to suspect: check the rendered query under the panel's *Query inspector*,
which should show `is_synthetic::text IN ('false','true')`.

- [ ] **Step 3: Commit**

```bash
git add grafana/dashboards/spot.json
git commit -m "M3: dashboard tells the real account apart from the simulator"
```

---

### Task 9: Milestone acceptance

**Files:** Modify: this plan file, `CLAUDE.md`

- [ ] **Step 1: The restart exercise**

This is the one that proves invariant 2 end to end, and it is worth doing
deliberately rather than trusting the unit test.

With a track playing and already recorded, note where things stand:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT count(*) AS rows FROM raw_plays WHERE NOT is_synthetic;"
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT event_id, track_name, started_at FROM raw_plays
WHERE NOT is_synthetic ORDER BY started_at DESC LIMIT 1;"
```

Then throw the poller's memory away mid-song and let it rediscover the track:

```bash
docker compose restart poller
docker compose logs --tail 20 poller
```

Expected: a `PLAY` line for the track that is *still playing* — the poller
lost its state and re-emitted. Now the point:

```bash
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT count(*) AS rows FROM raw_plays WHERE NOT is_synthetic;"
```

Expected: the same count as before the restart. The re-emitted event derived
the same `started_at`, so it hashed to the same `event_id`, so the sink's
`ON CONFLICT (event_id) DO UPDATE` refreshed the existing row instead of
adding one. At-least-once delivery plus an idempotent write.

**If the count went up by one, that is the bucket-boundary case**, not a bug:
the play's true start sat near a 5-second edge and the two derivations landed
either side of it. Confirm by comparing `started_at` on the two rows — they
will differ by under a second. Record which outcome you got in the results
section below; both are informative.

- [ ] **Step 2: Verify the full suite is green**

Run: `.venv/bin/pytest -q`
Expected: 105 passed.

- [ ] **Step 3: Verify a cold start still reaches a working dashboard**

```bash
docker compose down -v
docker compose up -d --build
sleep 90
docker compose ps
docker compose exec -T postgres psql -U spot -d spot -c "
SELECT is_synthetic, count(*) FROM raw_plays GROUP BY 1;"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/d/spot-live
```

Expected: every service up (`kafka-init` exited 0), synthetic rows present,
`200` from Grafana. Real rows appear only if something is playing — start a
track if the `false` group is missing.

`down -v` destroys both data volumes but not `.spotify_token.json`, which
lives on the host. The poller should come back without any re-authorization.

- [ ] **Step 4: Check the milestone acceptance criteria**

From the spec's M3 section:

- [ ] Play a song on Spotify; within 10 seconds it appears in `raw_plays` exactly once (Task 7, Steps 6-7).
- [ ] Let it play through and confirm no duplicate rows (Task 7, Step 7).
- [ ] `transition.py` was built test-first, one test per row of the state table (Task 3).
- [ ] No test contacts the live Spotify API (Task 2's fixtures are the API).

Plus the project's own standards:

- [ ] `.venv/bin/pytest -q` is green.
- [ ] `docker compose up -d --build` from cold reaches a filled dashboard.
- [ ] No Flink, no `agg_` writes — those tables exist and are still empty.
- [ ] `.env` and `.spotify_token.json` are untracked: `git status --short` lists neither.

- [ ] **Step 5: Update CLAUDE.md's "Current state" section**

It says M0-M2 are complete and M3 is next. Rewrite it for the state after this
milestone: `poller/` exists, the only thing outstanding is `flink/sql/`, the
new test count, and the one manual step a fresh clone now needs
(`cp .env.example .env`, fill it in, run `python -m poller.spotify_auth`).

- [ ] **Step 6: Record what actually happened**

Replace the expected numbers in the "Results" section below with the real
ones, the way M1 and M2 did. Note in particular whether the restart in Step 1
produced a duplicate row.

- [ ] **Step 7: Commit**

```bash
git add docs/superpowers/plans/2026-09-21-m3-real-spotify-poller.md CLAUDE.md
git commit -m "M3: mark plan complete, record the restart-idempotence result"
```

---

## Results

*Filled in during Task 9, Step 6.*

---

## Notes for M4

M4 deletes `consumer/` and lets Flink take over the `raw_plays` write. Three
things from M3 carry forward:

1. **The poller does not change in M4.** It produces to `plays` and has no
   idea what reads it. When the consumer is retired, real plays keep arriving
   because Flink's passthrough statement writes the same table on the same
   primary key.
2. **`started_at` from the poller is derived, and it is what the watermark
   will be built on.** The spec's 30-second watermark delay is comfortably
   wider than the poll interval, so ordinary poll jitter never produces late
   data. The skew panel now has real values in it to confirm that against.
3. **The fixture suite still needs a clean topic**, as recorded at the end of
   the M2 plan. Real plays sitting in `plays` alongside the fixture set do not
   break the `agg_` counts — every fixture assertion can filter on
   `is_synthetic` or on the fixture listeners' ids — but the assertions have to
   be written that way from the start rather than assuming an empty table.
