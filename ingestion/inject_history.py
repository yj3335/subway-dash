"""Inject synthetic 24-hour history into MongoDB speed_layer for dashboard testing.

Inserts one document every N minutes over the past 24 hours, with a realistic
congestion pattern (low overnight, peaks at morning/evening rush).

Usage:
    python -m ingestion.inject_history --station-id 611
    python -m ingestion.inject_history --station-id 611 --interval-mins 30
    python -m ingestion.inject_history --station-id 611 --interval-mins 15 --clear
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
from pymongo import MongoClient

from processing.config import BRIDGE_PARQUET


def _congestion_at_hour(hour: float) -> float:
    """Simulate a realistic congestion curve: low overnight, peaks at 8am and 6pm."""
    morning_peak = math.exp(-0.5 * ((hour - 8.0) / 1.5) ** 2) * 0.75
    evening_peak = math.exp(-0.5 * ((hour - 18.0) / 1.5) ** 2) * 0.65
    noise = (hash(int(hour * 100)) % 100) / 1000.0  # deterministic jitter
    return min(round(morning_peak + evening_peak + noise, 3), 1.0)


def _alert_level(score: float) -> str:
    if score >= 0.50:
        return "SEVERE"
    if score >= 0.20:
        return "MODERATE"
    return "NORMAL"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--station-id", default="611", help="station_complex_id (default: 611 = Times Sq-42 St)")
    p.add_argument("--interval-mins", type=int, default=30, help="Minutes between synthetic docs (default: 30)")
    p.add_argument("--hours", type=int, default=24, help="How many hours of history to generate (default: 24)")
    p.add_argument("--clear", action="store_true", help="Delete existing docs for this station before inserting")
    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    args = p.parse_args()

    bridge = pd.read_parquet(BRIDGE_PARQUET, columns=["station_complex_id", "complex_name"])
    match = bridge[bridge["station_complex_id"] == args.station_id].iloc[:1]
    complex_name = match["complex_name"].values[0] if len(match) else args.station_id

    client = MongoClient(args.mongo_uri)
    col = client["subway_dash"]["speed_layer"]

    if args.clear:
        deleted = col.delete_many({"station_complex_id": args.station_id}).deleted_count
        print(f"[cleared] removed {deleted} existing docs for station {args.station_id}")

    now = datetime.now(timezone.utc)
    interval = timedelta(minutes=args.interval_mins)
    total_points = (args.hours * 60) // args.interval_mins
    start = now - timedelta(hours=args.hours)

    docs = []
    for i in range(total_points):
        ts = start + interval * i
        hour_float = ts.hour + ts.minute / 60.0
        score = _congestion_at_hour(hour_float)
        delay = score * 300.0
        docs.append({
            "station_complex_id":   args.station_id,
            "complex_name":         complex_name,
            "event_timestamp":      ts,
            "avg_arrival_delay_secs": round(delay, 1),
            "service_deficit":      score,
            "demand_intensity":     1.0,
            "congestion_score":     score,
            "predicted_delay_mins": round(delay / 60, 2),
            "alert_level":          _alert_level(score),
            "weather_bucket":       "clear",
            "inserted_at":          ts,
        })

    col.insert_many(docs)
    print(f"[OK] inserted {len(docs)} docs for station {args.station_id} ({complex_name})")
    print(f"     interval={args.interval_mins}m  span={args.hours}h  range={start.strftime('%H:%M')} → {now.strftime('%H:%M')} UTC")
    print(f"     Select '{complex_name}' in the 24h History tab to see the chart.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
