"""The one place that knows a play event is keyed by listener_id.

Kafka guarantees ordering only within a partition, and the partition is chosen
by hashing the message key. Keying by listener_id therefore puts all of one
listener's plays on one partition in order, which is what session windows (M6)
require. Keying by track_id would scatter them and corrupt sessions silently.
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from events.schema import PlayEvent

log = logging.getLogger(__name__)


class KafkaSink:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "plays",
        producer_factory=Producer,
    ) -> None:
        self._topic = topic
        self.delivery_failures = 0
        self._producer = producer_factory(
            {
                "bootstrap.servers": bootstrap_servers,
                # Wait for the broker to persist before considering a write
                # done. With a single broker this is cheap; the habit matters.
                "acks": "all",
                "compression.type": "snappy",
                # Small batching window: throughput without visible latency.
                "linger.ms": 50,
            }
        )

    def _on_delivery(self, err, msg) -> None:
        # Never raise from the callback: a broker hiccup must not kill the
        # producing loop. Kafka's retention is the safety net, and the
        # deterministic event_id makes a re-send harmless.
        if err is not None:
            self.delivery_failures += 1
            log.error("delivery failed: %s", err)

    def send(self, event: PlayEvent) -> None:
        self._producer.produce(
            topic=self._topic,
            key=event.listener_id.encode("utf-8"),
            value=event.to_json().encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        # Serve delivery callbacks without blocking.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Block until queued messages are delivered. Returns the number still
        in the queue, so 0 means everything landed."""
        return self._producer.flush(timeout)
