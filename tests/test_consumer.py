from datetime import datetime, timezone

from consumer.main import (
    Settings,
    decode,
    poll_batch,
    rewind,
    run,
    settings_from_env,
)
from events.schema import PlayEvent

UTC = timezone.utc


def make_event(listener_id="listener-00", track_id="t1", second=0):
    # Defined locally rather than imported from tests/test_pg_sink.py: there is
    # no tests/__init__.py, so a cross-test import loads a second copy of the
    # module under a different name. Each test file standing alone is also the
    # existing convention here -- test_schema.py and test_kafka_sink.py each
    # define their own builder.
    started = datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC)
    return PlayEvent.create(
        listener_id=listener_id,
        is_synthetic=True,
        track_id=track_id,
        track_name="Mr. Brightside",
        artist_name="The Killers",
        album_name="Hot Fuss",
        duration_ms=222075,
        started_at=started,
        observed_at=datetime(2026, 1, 1, 0, 0, second + 3, tzinfo=UTC),
    )


class FakeMessage:
    def __init__(self, value, partition=0, offset=0, error=None, topic="plays"):
        self._value = value
        self._partition = partition
        self._offset = offset
        self._error = error
        self._topic = topic

    def value(self):
        return self._value

    def error(self):
        return self._error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeConsumer:
    """Hands out a scripted sequence of poll() results and records commits."""

    def __init__(self, script):
        self._script = list(script)
        self.commits = 0
        self.seeks = []
        self.closed = False

    def poll(self, timeout):
        if not self._script:
            return None
        return self._script.pop(0)

    def commit(self, asynchronous=False):
        self.commits += 1

    def seek(self, partition):
        self.seeks.append((partition.topic, partition.partition, partition.offset))

    def close(self):
        self.closed = True


class FakeSink:
    def __init__(self, fail_times=0):
        self.batches = []
        self._fail_times = fail_times

    def write_batch(self, events):
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("postgres is down")
        self.batches.append(list(events))
        return len(events)


def encoded(event):
    return event.to_json().encode("utf-8")


SETTINGS = Settings(
    bootstrap_servers="kafka:9092",
    topic="plays",
    group_id="raw-plays-writer",
    dsn="postgresql://unused",
    batch_max=3,
    poll_timeout=0.01,
    # Zero, so the two failure tests do not put a real sleep in the suite.
    retry_backoff=0.0,
)


def test_settings_come_from_the_environment_with_defaults():
    s = settings_from_env({})
    assert s.group_id == "raw-plays-writer"
    assert s.topic == "plays"
    assert s.batch_max == 100
    assert s.retry_backoff == 2.0

    s = settings_from_env({"CONSUMER_GROUP": "second-reader", "BATCH_MAX": "7"})
    assert s.group_id == "second-reader"
    assert s.batch_max == 7


def test_poll_batch_stops_at_the_batch_limit():
    messages = [FakeMessage(b"a", offset=i) for i in range(10)]
    consumer = FakeConsumer(messages)
    assert len(poll_batch(consumer, max_messages=3, timeout=0.01)) == 3


def test_poll_batch_returns_early_when_the_broker_has_nothing_more():
    consumer = FakeConsumer([FakeMessage(b"a"), None, FakeMessage(b"b")])
    # The None ends the batch; the loop must not block waiting to fill it.
    assert len(poll_batch(consumer, max_messages=100, timeout=0.01)) == 1


def test_decode_skips_malformed_messages_and_keeps_the_rest():
    good, other = make_event(second=0), make_event(track_id="t2", second=30)
    messages = [
        FakeMessage(encoded(good)),
        FakeMessage(b"{not json at all"),
        FakeMessage(encoded(other)),
    ]
    events = decode(messages)
    assert [e.event_id for e in events] == [good.event_id, other.event_id]


def test_commit_happens_only_after_the_batch_is_written():
    event = make_event()
    consumer = FakeConsumer([FakeMessage(encoded(event))])
    sink = FakeSink()

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert len(sink.batches) == 1
    assert consumer.commits == 1


def test_a_failed_write_does_not_commit():
    consumer = FakeConsumer([FakeMessage(encoded(make_event()))])
    sink = FakeSink(fail_times=1)

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sink.batches == []
    assert consumer.commits == 0


def test_a_failed_write_rewinds_to_the_start_of_the_batch():
    # Committing nothing protects a restart. It does nothing for the running
    # process, whose position has already advanced -- so the loop must seek.
    messages = [
        FakeMessage(encoded(make_event(second=0)), partition=0, offset=40),
        FakeMessage(encoded(make_event(second=5)), partition=0, offset=41),
        FakeMessage(encoded(make_event(second=10)), partition=2, offset=7),
    ]
    consumer = FakeConsumer(messages)
    sink = FakeSink(fail_times=1)

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sorted(consumer.seeks) == [("plays", 0, 40), ("plays", 2, 7)]


def test_rewind_picks_the_lowest_offset_seen_per_partition():
    consumer = FakeConsumer([])
    rewind(consumer, [
        FakeMessage(b"", partition=1, offset=99),
        FakeMessage(b"", partition=1, offset=95),
        FakeMessage(b"", partition=1, offset=97),
    ])
    assert consumer.seeks == [("plays", 1, 95)]


def test_a_batch_of_only_malformed_messages_still_commits():
    # Otherwise one unparseable byte wedges the partition forever.
    consumer = FakeConsumer([FakeMessage(b"garbage")])
    sink = FakeSink()

    run(consumer, sink, SETTINGS, should_continue=_once())

    assert sum(len(batch) for batch in sink.batches) == 0
    assert consumer.commits == 1


def _once():
    """should_continue that is True for exactly one iteration."""
    remaining = [True]

    def go():
        if remaining:
            remaining.pop()
            return True
        return False

    return go
