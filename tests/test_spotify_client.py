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
