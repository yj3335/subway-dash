"""Synthetic delay injector for the end-to-end smoke test.

Publishes one fake TripUpdate-shaped JSON message to `gtfs-trips` for a
chosen stop with a configurable arrival delay. Used to confirm
the marker on the Streamlit map turns yellow/red within 60 seconds.

Run:
    python -m ingestion.inject_synthetic_delay --stop-id 127N --delay 600
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from ingestion.config import KAFKA_BOOTSTRAP, TOPIC_TRIPS, partition_for_route


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject a synthetic GTFS TripUpdate for smoke testing")
    p.add_argument("--stop-id", default="127N", help="GTFS stop_id, e.g. 127N (Times Sq NB)")
    p.add_argument("--route-id", default="1")
    p.add_argument("--trip-id", default="SYNTHETIC-TRIP-1")
    p.add_argument("--delay", type=int, default=600, help="Arrival delay seconds (positive = late)")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(timezone.utc)
    now_unix = int(now.timestamp())
    payload = {
        "feed": "synthetic",
        "feed_timestamp": now_unix,
        "entity_id": f"synthetic-{now_unix}",
        "route_id": args.route_id,
        "trip": {
            "trip_id": args.trip_id,
            "route_id": args.route_id,
            "start_date": now.strftime("%Y%m%d"),
            "start_time": now.strftime("%H:%M:%S"),
        },
        "stop_time_update": [
            {
                "stop_id": args.stop_id,
                "stop_sequence": 1,
                "arrival": {"time": now_unix + args.delay, "delay": args.delay},
                "departure": {"time": now_unix + args.delay + 30, "delay": args.delay},
            }
        ],
        "timestamp": now_unix,
    }

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    partition = partition_for_route(args.route_id)
    start = time.time()
    future = producer.send(
        TOPIC_TRIPS,
        value=payload,
        key=args.route_id,
        partition=partition,
    )
    metadata = future.get(timeout=10)
    producer.flush()
    producer.close()
    elapsed = time.time() - start
    print(
        f"[OK] synthetic delay injected: stop={args.stop_id} delay={args.delay}s "
        f"topic={metadata.topic} partition={metadata.partition} offset={metadata.offset} "
        f"elapsed={elapsed:.2f}s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
