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
