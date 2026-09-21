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
