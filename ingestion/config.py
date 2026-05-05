"""Shared config for Track A ingestion processes.

Keeps Kafka bootstrap, topic names, and the route → partition map in one place
so producers, monitors, and consumers cannot drift.
"""
from __future__ import annotations

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_VEHICLE = "gtfs-vehicle"
TOPIC_TRIPS = "gtfs-trips"
TOPIC_ALERTS = "gtfs-alerts"
TOPIC_DLQ = "gtfs-dlq"
TOPIC_WEATHER = "weather-feed"

NUM_PARTITIONS_GTFS = 12

# Route → partition map (Task A2.2). One partition per NYC subway line group.
# Unknown routes fall back to hash(route_id) % NUM_PARTITIONS_GTFS so they
# don't all land on a single overflow bucket.
ROUTE_PARTITION_MAP: dict[str, int] = {
    # A/C/E
    "A": 0, "C": 0, "E": 0,
    # B/D/F/M
    "B": 1, "D": 1, "F": 1, "M": 1,
    # G
    "G": 2,
    # J/Z
    "J": 3, "Z": 3,
    # L
    "L": 4,
    # N/Q/R/W
    "N": 5, "Q": 5, "R": 5, "W": 5,
    # 1/2/3
    "1": 6, "2": 6, "3": 6,
    # 4/5/6
    "4": 7, "5": 7, "6": 7,
    # 7
    "7": 8,
    # Shuttles
    "S": 9, "FS": 9, "GS": 9, "H": 9,
    # Staten Island Railway
    "SIR": 10, "SI": 10,
}


def partition_for_route(route_id: str | None) -> int:
    """Return a deterministic partition for a GTFS route_id.

    Unknown routes hash uniformly across partitions to avoid hotspotting one
    overflow bucket (per Task A2.2).
    """
    if route_id and route_id in ROUTE_PARTITION_MAP:
        return ROUTE_PARTITION_MAP[route_id]
    if not route_id:
        return 11
    return hash(route_id) % NUM_PARTITIONS_GTFS
