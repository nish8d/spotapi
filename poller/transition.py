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
from datetime import datetime, timedelta
from typing import Any, Optional

from events.schema import PlayEvent


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
