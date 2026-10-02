"""The single source of truth for the shape of a play event.

Both producers and the tests import this module rather than restating the
event shape. It deliberately depends on nothing but the standard library: a
JSON round-trip should not require a Kafka client.
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
