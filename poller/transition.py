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
