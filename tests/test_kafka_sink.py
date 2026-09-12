import json
from datetime import datetime, timezone

from events.schema import PlayEvent
from producer.kafka_sink import KafkaSink

UTC = timezone.utc


class FakeProducer:
    """Records produce() calls instead of talking to a broker."""

    def __init__(self, config):
        self.config = config
        self.produced = []
        self.flushed = False

    def produce(self, topic, key, value, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value,
                              "on_delivery": on_delivery})

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def make_event(listener_id="nishad"):
    return PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=True,
        track_id="3n3Ppam7vgaVa1iaRUc9Lp",
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        started_at=datetime(2026, 9, 11, 10, 3, 22, tzinfo=UTC),
        observed_at=datetime(2026, 9, 11, 10, 3, 31, tzinfo=UTC),
    )


def build_sink():
    captured = {}

    def factory(config):
        captured["producer"] = FakeProducer(config)
        return captured["producer"]

    sink = KafkaSink("kafka:9092", topic="plays", producer_factory=factory)
    return sink, captured["producer"]


def test_message_key_is_the_listener_id():
    sink, fake = build_sink()
    sink.send(make_event(listener_id="listener-07"))
    assert fake.produced[0]["key"] == b"listener-07"


def test_every_event_from_one_listener_shares_a_key():
    # Same key -> same partition -> ordering preserved for that listener.
    sink, fake = build_sink()
    for _ in range(3):
        sink.send(make_event(listener_id="listener-07"))
    assert {m["key"] for m in fake.produced} == {b"listener-07"}


def test_value_is_the_schema_json():
    sink, fake = build_sink()
    event = make_event()
    sink.send(event)
    assert PlayEvent.from_json(fake.produced[0]["value"]) == event


def test_produces_to_the_configured_topic():
    sink, fake = build_sink()
    sink.send(make_event())
    assert fake.produced[0]["topic"] == "plays"


def test_producer_is_configured_for_durability():
    _, fake = build_sink()
    assert fake.config["bootstrap.servers"] == "kafka:9092"
    assert fake.config["acks"] == "all"


def test_delivery_failures_are_counted_not_raised():
    sink, fake = build_sink()
    sink.send(make_event())
    callback = fake.produced[0]["on_delivery"]
    callback("broker went away", None)
    assert sink.delivery_failures == 1


def test_successful_delivery_does_not_count_as_a_failure():
    sink, fake = build_sink()
    sink.send(make_event())
    callback = fake.produced[0]["on_delivery"]
    callback(None, object())
    assert sink.delivery_failures == 0


def test_flush_delegates_to_the_producer():
    sink, fake = build_sink()
    assert sink.flush(timeout=1.0) == 0
    assert fake.flushed is True
