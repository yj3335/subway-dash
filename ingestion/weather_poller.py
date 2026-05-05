"""Task A3.3 — Real-time weather poller (NWS, not NOAA CDO).

NOAA CDO publishes historical climate data with 1–3 day latency, which is
unusable for the speed layer. The live current-weather signal comes from the
National Weather Service public API at `api.weather.gov` — no API key, only a
descriptive User-Agent header.

Every 15 minutes:
  1. GET /stations/KNYC/observations/latest (KNYC = Central Park ASOS).
  2. Classify into bucket: snow / rain / clear (same thresholds as the
     historical batch path so live and batch buckets stay aligned).
  3. Upsert single-row Cassandra `subway_dash.current_weather` (scope='nyc').
  4. Publish JSON to `weather-feed` Kafka topic for downstream replay.

On total failure we DO NOT overwrite the last-known-good Cassandra row —
the speed layer's cache will keep serving the previous bucket.

Run:
    python -m ingestion.weather_poller
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from cassandra.cluster import Cluster
from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.config import KAFKA_BOOTSTRAP, TOPIC_WEATHER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("weather_poller")

USER_AGENT = "subway-dash/1.0 (academic project, contact@example.com)"
NWS_STATION = os.getenv("NWS_STATION", "KNYC")  # Central Park ASOS
NWS_URL = f"https://api.weather.gov/stations/{NWS_STATION}/observations/latest"
POLL_INTERVAL_SECS = 15 * 60  # 15 minutes
HTTP_TIMEOUT_SECS = 15
MAX_RETRIES = 3

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = "subway_dash"

PRCP_RAIN_THRESHOLD_IN = 0.1   # matches Section 3.2 batch buckets
SNOW_KEYWORDS = ("snow", "sleet", "ice")

_running = True


def _handle_signal(_signum: int, _frame: Any) -> None:
    global _running
    _running = False
    log.info("shutdown signal received")


def fetch_observation() -> dict[str, Any] | None:
    """GET the latest NWS observation, with exponential backoff. Returns None
    on terminal failure (caller must NOT overwrite the last-known-good row)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                NWS_URL,
                headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"},
                timeout=HTTP_TIMEOUT_SECS,
            )
            if resp.status_code >= 500:
                raise requests.HTTPError(f"5xx: {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            wait = 2 ** attempt
            log.warning(
                "NWS fetch attempt %d/%d failed: %s — sleeping %ds",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
    log.error("NWS fetch failed after %d retries; preserving last-known-good", MAX_RETRIES)
    return None


def classify(props: dict[str, Any]) -> dict[str, Any]:
    """Reduce raw NWS properties to (weather_bucket, prcp_in, snow_in, tmax_f)."""
    prcp_mm = (props.get("precipitationLastHour") or {}).get("value")
    temp_c = (props.get("temperature") or {}).get("value")
    text = (props.get("textDescription") or "").lower()

    prcp_in = (prcp_mm / 25.4) if isinstance(prcp_mm, (int, float)) else 0.0
    tmax_f = (temp_c * 9.0 / 5.0 + 32.0) if isinstance(temp_c, (int, float)) else None

    snow_inferred = (
        any(k in text for k in SNOW_KEYWORDS)
        and isinstance(temp_c, (int, float))
        and temp_c < 0.0
    )

    if snow_inferred:
        bucket = "snow"
    elif prcp_in > PRCP_RAIN_THRESHOLD_IN:
        bucket = "rain"
    else:
        bucket = "clear"

    return {
        "weather_bucket": bucket,
        "prcp_in": float(prcp_in),
        "snow_in": float(prcp_in if snow_inferred else 0.0),
        "tmax_f": tmax_f,
    }


class CassandraWriter:
    def __init__(self, hosts: list[str]) -> None:
        # Pin protocol_version=5 to skip the downgrade-negotiation handshake
        # the driver does on each connect against Cassandra 4.1 (which
        # otherwise prints WARN "Downgrading core protocol version from 66
        # to 65 to 5"). 5 is the highest version 4.1 supports.
        self._cluster = Cluster(hosts, protocol_version=5)
        self._session = self._cluster.connect(CASSANDRA_KEYSPACE)
        self._stmt = self._session.prepare(
            "INSERT INTO current_weather "
            "(scope, weather_bucket, prcp_in, snow_in, tmax_f, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )

    def upsert(self, classified: dict[str, Any]) -> None:
        self._session.execute(
            self._stmt,
            (
                "nyc",
                classified["weather_bucket"],
                classified["prcp_in"],
                classified["snow_in"],
                classified["tmax_f"],
                datetime.now(timezone.utc),
            ),
        )

    def close(self) -> None:
        self._cluster.shutdown()


def build_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_to_kafka(producer: KafkaProducer, classified: dict[str, Any]) -> None:
    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **classified,
    }
    try:
        producer.send(TOPIC_WEATHER, key="nyc", value=msg)
        producer.flush(timeout=5)
    except KafkaError as exc:
        log.error("Kafka publish failed: %s", exc)


def main() -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer = build_kafka_producer()
    try:
        cass = CassandraWriter(CASSANDRA_HOSTS)
    except Exception as exc:
        log.error("Cassandra connect failed: %s", exc)
        return 1

    log.info("weather poller running; station=%s interval=%ds", NWS_STATION, POLL_INTERVAL_SECS)

    while _running:
        cycle_start = time.time()
        observation = fetch_observation()
        if observation is not None:
            props = observation.get("properties", {})
            classified = classify(props)
            try:
                cass.upsert(classified)
                log.info(
                    "current_weather → bucket=%s prcp_in=%.3f tmax_f=%s",
                    classified["weather_bucket"],
                    classified["prcp_in"],
                    classified["tmax_f"],
                )
            except Exception as exc:
                log.error("Cassandra upsert failed: %s", exc)
            publish_to_kafka(producer, classified)
        elapsed = time.time() - cycle_start
        sleep_for = max(0.0, POLL_INTERVAL_SECS - elapsed)
        for _ in range(int(sleep_for * 10)):
            if not _running:
                break
            time.sleep(0.1)

    producer.close()
    cass.close()
    log.info("weather poller stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
