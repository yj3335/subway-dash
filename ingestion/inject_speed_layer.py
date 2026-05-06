"""C6.3 — Inject a synthetic speed_layer document directly into MongoDB.

Used to verify the Streamlit dashboard color-codes markers correctly
without running the full Kafka → Spark → MongoDB pipeline.

Run:
    python -m ingestion.inject_speed_layer --station-id 611 --level SEVERE
    # Streamlit refreshes every 30s — check http://localhost:8501:
    # the Times Sq marker should turn red within one refresh cycle.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient

from processing.config import BRIDGE_PARQUET

_DEFAULTS_BY_LEVEL = {
    "SEVERE":   {"score": 0.80, "delay": 300.0, "pred_mins": 10.0},
    "MODERATE": {"score": 0.30, "delay": 120.0, "pred_mins": 4.0},
    "NORMAL":   {"score": 0.10, "delay": 30.0,  "pred_mins": 0.5},
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Insert a synthetic speed_layer document into MongoDB")
    p.add_argument("--station-id", default="611", help="station_complex_id (default: 611 = Times Sq-42 St)")
    p.add_argument("--level", choices=["SEVERE", "MODERATE", "NORMAL"], default="SEVERE")
    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    return p.parse_args()


def main() -> int:
    args = parse_args()

    bridge = pd.read_parquet(BRIDGE_PARQUET, columns=["station_complex_id", "complex_name"])
    match = bridge[bridge["station_complex_id"] == args.station_id].iloc[:1]
    complex_name = match["complex_name"].values[0] if len(match) else args.station_id

    vals = _DEFAULTS_BY_LEVEL[args.level]
    now = datetime.now(timezone.utc)
    doc = {
        "station_complex_id": args.station_id,
        "complex_name":       complex_name,
        "event_timestamp":    now,
        "avg_arrival_delay_secs": vals["delay"],
        "service_deficit":    vals["score"],
        "demand_intensity":   1.0,
        "congestion_score":   vals["score"],
        "predicted_delay_mins": vals["pred_mins"],
        "alert_level":        args.level,
        "weather_bucket":     "clear",
        "inserted_at":        now,
    }

    client = MongoClient(args.mongo_uri)
    result = client["subway_dash"]["speed_layer"].insert_one(doc)
    print(f"[OK] station={args.station_id} ({complex_name}) level={args.level} _id={result.inserted_id}")
    print("     Dashboard refreshes every 30s — verify at http://localhost:8501")
    return 0


if __name__ == "__main__":
    sys.exit(main())
