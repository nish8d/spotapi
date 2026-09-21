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
