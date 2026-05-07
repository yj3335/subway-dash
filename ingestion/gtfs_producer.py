"""Live MTA GTFS-Realtime → Kafka producer.

Polls all 8 MTA GTFS-Realtime feeds every 15 seconds, deserializes Protobuf
to flat JSON, and emits one Kafka record per entity. Records are routed to
`gtfs-vehicle`, `gtfs-trips`, or `gtfs-alerts` depending on entity kind.
Partition is chosen by `route_id` via `ROUTE_PARTITION_MAP` (with a hash
fallback for unknown routes — see `config.partition_for_route`).

Deserialization failures publish the raw bytes + error message to `gtfs-dlq`.

Run:
    python -m ingestion.gtfs_producer
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from base64 import b64encode
from typing import Any, Iterable

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_ALERTS,
    TOPIC_DLQ,
    TOPIC_TRIPS,
    TOPIC_VEHICLE,
    partition_for_route,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gtfs_producer")

USER_AGENT = "subway-dash/1.0 (academic project, contact@example.com)"
POLL_INTERVAL_SECS = 15
HTTP_TIMEOUT_SECS = 10

FEEDS: dict[str, str] = {
    "ace":  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "bdfm": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "g":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "jz":   "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "nqrw": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "l":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "main": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "si":   "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}


_running = True


def _handle_signal(_signum: int, _frame: Any) -> None:
    global _running
    _running = False
    log.info("shutdown signal received; finishing current cycle")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
        linger_ms=50,
        retries=5,
    )


def fetch_feed(url: str) -> bytes | None:
    """GET the Protobuf payload. Returns bytes or None on failure."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/x-protobuf"},
            timeout=HTTP_TIMEOUT_SECS,
        )
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as exc:
        log.warning("fetch failed for %s: %s", url, exc)
        return None


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(payload)
    return msg


def _route_id_for_entity(entity: Any) -> str | None:
    if entity.HasField("trip_update") and entity.trip_update.trip.route_id:
        return entity.trip_update.trip.route_id
    if entity.HasField("vehicle") and entity.vehicle.trip.route_id:
        return entity.vehicle.trip.route_id
    if entity.HasField("alert"):
        for sel in entity.alert.informed_entity:
            if sel.route_id:
                return sel.route_id
    return None


def _entity_records(
    feed_name: str, msg: gtfs_realtime_pb2.FeedMessage
) -> Iterable[tuple[str, str | None, dict[str, Any]]]:
    """Yield (topic, route_id, json_dict) for every entity in the feed."""
    header_ts = msg.header.timestamp
    for entity in msg.entity:
        route_id = _route_id_for_entity(entity)
        base = {
            "feed": feed_name,
            "feed_timestamp": header_ts,
            "entity_id": entity.id,
            "route_id": route_id,
        }
        if entity.HasField("vehicle"):
            payload = MessageToDict(entity.vehicle, preserving_proto_field_name=True)
            yield TOPIC_VEHICLE, route_id, {**base, **payload}
        if entity.HasField("trip_update"):
            payload = MessageToDict(entity.trip_update, preserving_proto_field_name=True)
            yield TOPIC_TRIPS, route_id, {**base, **payload}
        if entity.HasField("alert"):
            payload = MessageToDict(entity.alert, preserving_proto_field_name=True)
            yield TOPIC_ALERTS, route_id, {**base, **payload}


def _send_to_dlq(
    producer: KafkaProducer, feed_name: str, raw: bytes, error: str
) -> None:
    dlq_msg = {
        "feed": feed_name,
        "error": error,
        "received_at": int(time.time()),
        "raw_b64": b64encode(raw).decode("ascii") if raw else None,
    }
    try:
        producer.send(TOPIC_DLQ, value=dlq_msg, key=feed_name)
    except KafkaError as exc:
        log.error("DLQ send failed: %s", exc)


def process_feed(producer: KafkaProducer, feed_name: str, url: str) -> dict[str, int]:
    counts = {"vehicle": 0, "trips": 0, "alerts": 0, "dlq": 0}
    raw = fetch_feed(url)
    if raw is None:
        _send_to_dlq(producer, feed_name, b"", "fetch_failed")
        counts["dlq"] += 1
        return counts
    try:
        msg = parse_feed(raw)
    except Exception as exc:  # protobuf parse error
        _send_to_dlq(producer, feed_name, raw, f"parse_error: {exc}")
        counts["dlq"] += 1
        log.warning("parse error on %s: %s", feed_name, exc)
        return counts

    for topic, route_id, value in _entity_records(feed_name, msg):
        partition = partition_for_route(route_id)
        # gtfs-alerts has 2 partitions, not 12 — modulo it down
        if topic == TOPIC_ALERTS:
            partition = partition % 2
        try:
            producer.send(
                topic,
                value=value,
                key=route_id or feed_name,
                partition=partition,
            )
            if topic == TOPIC_VEHICLE:
                counts["vehicle"] += 1
            elif topic == TOPIC_TRIPS:
                counts["trips"] += 1
            else:
                counts["alerts"] += 1
        except KafkaError as exc:
            log.error("kafka send error (%s): %s", topic, exc)
            _send_to_dlq(producer, feed_name, b"", f"send_error: {exc}")
            counts["dlq"] += 1
    return counts


def main() -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    producer = build_producer()
    log.info(
        "GTFS producer starting; bootstrap=%s feeds=%d interval=%ds",
        KAFKA_BOOTSTRAP, len(FEEDS), POLL_INTERVAL_SECS,
    )
    while _running:
        cycle_start = time.time()
        totals = {"vehicle": 0, "trips": 0, "alerts": 0, "dlq": 0}
        for feed_name, url in FEEDS.items():
            counts = process_feed(producer, feed_name, url)
            for k, v in counts.items():
                totals[k] += v
        producer.flush()
        elapsed = time.time() - cycle_start
        log.info(
            "cycle complete in %.1fs | vehicle=%d trips=%d alerts=%d dlq=%d",
            elapsed, totals["vehicle"], totals["trips"], totals["alerts"], totals["dlq"],
        )
        sleep_for = max(0.0, POLL_INTERVAL_SECS - elapsed)
        for _ in range(int(sleep_for * 10)):
            if not _running:
                break
            time.sleep(0.1)
    producer.flush()
    producer.close()
    log.info("producer stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
