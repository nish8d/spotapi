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
