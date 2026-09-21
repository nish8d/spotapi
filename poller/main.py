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
