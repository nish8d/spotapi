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
