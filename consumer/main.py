"""The throwaway M2 consumer: `plays` topic -> Postgres `raw_plays`.

Written with confluent-kafka directly, and with manual offset commits, so that
consumer groups, offsets, lag and rebalancing are visible before M4 replaces
the whole thing with Flink. Deleted in M4. Not built to last, on purpose.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Mapping

from confluent_kafka import Consumer, TopicPartition

from consumer.pg_sink import PostgresSink
from events.schema import PlayEvent

log = logging.getLogger("consumer")


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    topic: str
    group_id: str
    dsn: str
    batch_max: int
    poll_timeout: float
    retry_backoff: float


def settings_from_env(env: Mapping[str, str]) -> Settings:
    return Settings(
        bootstrap_servers=env.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic=env.get("KAFKA_TOPIC", "plays"),
        group_id=env.get("CONSUMER_GROUP", "raw-plays-writer"),
        dsn=env.get("POSTGRES_DSN", "postgresql://spot:spot@postgres:5432/spot"),
        batch_max=int(env.get("BATCH_MAX", "100")),
        poll_timeout=float(env.get("POLL_TIMEOUT", "1.0")),
        # Injectable so the unit tests can set it to 0 instead of sleeping.
        retry_backoff=float(env.get("RETRY_BACKOFF", "2.0")),
    )


def poll_batch(consumer, max_messages: int, timeout: float) -> list:
    """Collect up to max_messages, returning as soon as the broker is drained.

    The first poll waits up to `timeout` for something to arrive; subsequent
    polls use 0 so a quiet topic yields a small batch immediately rather than
    holding rows back waiting to fill one.
    """
    batch: list = []
    # Bounded rather than `while len(batch) < max_messages`: an error message
    # is skipped without being added to the batch, so an unbroken stream of
    # them would otherwise loop forever.
    for _ in range(max_messages * 2):
        if len(batch) >= max_messages:
            break
        message = consumer.poll(timeout if not batch else 0)
        if message is None:
            break
        if message.error() is not None:
            log.warning("kafka error: %s", message.error())
            continue
        batch.append(message)
    return batch


def decode(messages) -> list[PlayEvent]:
    """Deserialize with the shared schema, dropping anything unparseable.

    A malformed message is logged and skipped rather than retried: its offset
    is committed with the rest of the batch, because one bad byte must not
    wedge a partition forever. A database failure is the opposite case and is
    handled by the caller.
    """
    events = []
    for message in messages:
        try:
            events.append(PlayEvent.from_json(message.value()))
        except Exception as exc:
            log.error(
                "skipping malformed message %s[%d]@%d: %s",
                message.topic(), message.partition(), message.offset(), exc,
            )
    return events


def rewind(consumer, messages) -> None:
    """Seek every partition in this batch back to its first offset.

    Declining to commit protects a restart, not this process: the in-memory
    position has already advanced past these messages. Retrying requires an
    explicit seek. This is the difference between the committed offset and
    the current position, and it is easy to get wrong.
    """
    lowest: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        offset = message.offset()
        if key not in lowest or offset < lowest[key]:
            lowest[key] = offset
    for (topic, partition), offset in lowest.items():
        try:
            consumer.seek(TopicPartition(topic, partition, offset))
        except Exception as exc:
            # A rebalance between the poll and the failure means this partition
            # is no longer ours. Whoever holds it now starts from the last
            # committed offset, which is still before this batch, so nothing
            # is lost -- there is simply nothing for us to rewind.
            log.warning("cannot rewind %s[%d]: %s", topic, partition, exc)
            continue
        log.info("rewound %s[%d] to offset %d", topic, partition, offset)


def run(consumer, sink, settings: Settings, should_continue=lambda: True) -> None:
    """Poll, write, then commit -- in that order.

    Committing only after a successful write makes this at-least-once: a crash
    between the write and the commit replays the batch, and the upsert on the
    deterministic event_id turns that replay into a no-op.
    """
    while should_continue():
        messages = poll_batch(consumer, settings.batch_max, settings.poll_timeout)
        if not messages:
            continue

        events = decode(messages)
        try:
            written = sink.write_batch(events)
        except Exception as exc:
            log.error("batch write failed, not committing: %s", exc)
            rewind(consumer, messages)
            time.sleep(settings.retry_backoff)
            continue

        consumer.commit(asynchronous=False)
        log.info("wrote %d events, committed %d offsets", written, len(messages))


def _log_assignment(consumer, partitions) -> None:
    log.info("ASSIGNED %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def _log_revocation(consumer, partitions) -> None:
    log.info("REVOKED  %s", [f"{p.topic}[{p.partition}]" for p in partitions])


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings_from_env(os.environ)

    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        # The whole point of the milestone: offsets move only when this code
        # says so, after the rows are in Postgres.
        "enable.auto.commit": False,
        # A group with no committed offset starts at the beginning, so the
        # backlog the simulator has already produced gets consumed.
        "auto.offset.reset": "earliest",
    })
    # Callbacks purely so a rebalance is visible in the logs. Watching these
    # fire when a second consumer joins is one of M2's exercises.
    consumer.subscribe([settings.topic],
                       on_assign=_log_assignment,
                       on_revoke=_log_revocation)

    sink = PostgresSink(settings.dsn)
    running = [True]

    def stop(signum, frame):
        log.info("signal %d received, finishing the current batch", signum)
        running[0] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("consuming %s as group %s", settings.topic, settings.group_id)
    try:
        run(consumer, sink, settings, should_continue=lambda: running[0])
    finally:
        # close() leaves the group cleanly, which triggers an immediate
        # rebalance instead of waiting for the session timeout to expire.
        consumer.close()
        sink.close()
        log.info("stopped")


if __name__ == "__main__":
    main()
