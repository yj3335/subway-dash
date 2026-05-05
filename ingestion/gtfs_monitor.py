"""Task A2.3 — GTFS Kafka monitor.

Subscribes to all four GTFS topics plus the DLQ and prints a one-line status
every 60 seconds with messages-per-minute throughput per topic.

Run:
    python -m ingestion.gtfs_monitor
"""
from __future__ import annotations

import logging
import signal
import sys
import time
from collections import defaultdict
from typing import Any

from kafka import KafkaConsumer

from ingestion.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_ALERTS,
    TOPIC_DLQ,
    TOPIC_TRIPS,
    TOPIC_VEHICLE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("gtfs_monitor")

REPORT_INTERVAL_SECS = 60
TOPICS = [TOPIC_VEHICLE, TOPIC_TRIPS, TOPIC_ALERTS, TOPIC_DLQ]

_running = True


def _handle_signal(_signum: int, _frame: Any) -> None:
    global _running
    _running = False


def main() -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="gtfs-monitor",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )

    counts: dict[str, int] = defaultdict(int)
    window_start = time.time()
    log.info("monitor up; topics=%s", TOPICS)

    while _running:
        for record in consumer:
            counts[record.topic] += 1
            if not _running:
                break
        now = time.time()
        if now - window_start >= REPORT_INTERVAL_SECS:
            elapsed_min = (now - window_start) / 60.0
            line = " | ".join(
                f"{t}: {int(counts[t] / elapsed_min)} msgs/min" for t in TOPICS
            )
            log.info(line)
            counts.clear()
            window_start = now

    consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
