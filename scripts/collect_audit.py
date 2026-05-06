"""Collect Track A staging metrics for the Phase 3 / Week 8 audit.

Reads the staging Parquet directories and produces a JSON metrics dump:
- staging row counts per topic + per partition date
- Kafka offset deltas vs. baseline (logs/audit_baseline.txt)
- partition distribution for vehicle_positions (skew analysis, A8.3)
- station coverage / null_station_complex_id rate (A4.2)
- arrival_delay_secs distribution (A5.2 / A-16)
- top-20 busiest stations (input for skew salting decision A-12)

Usage: python -m scripts.collect_audit > docs/phase3_arjun_audit.md
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pyarrow.dataset as ds

REPO = Path(__file__).resolve().parents[1]
STAGING = REPO / "data" / "staging"
LOGS = REPO / "logs"


def kafka_offset_total(topic: str) -> int:
    out = subprocess.check_output(
        [
            "docker", "exec", "subway_kafka",
            "kafka-run-class", "kafka.tools.GetOffsetShell",
            "--broker-list", "localhost:9092",
            "--topic", topic, "--time", "-1",
        ],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    total = 0
    for line in out.splitlines():
        if not line.strip():
            continue
        # format: topic:partition:offset
        total += int(line.rsplit(":", 1)[-1])
    return total


def kafka_offsets_by_partition(topic: str) -> dict[int, int]:
    out = subprocess.check_output(
        [
            "docker", "exec", "subway_kafka",
            "kafka-run-class", "kafka.tools.GetOffsetShell",
            "--broker-list", "localhost:9092",
            "--topic", topic, "--time", "-1",
        ],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    result: dict[int, int] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        _, p, off = line.rsplit(":", 2)
        result[int(p)] = int(off)
    return result


def read_staging(name: str):
    path = STAGING / name
    if not path.exists():
        return None
    return (
        ds.dataset(path, format="parquet", partitioning="hive")
        .to_table()
        .to_pandas()
    )


def baseline_offsets() -> dict[str, int]:
    path = LOGS / "audit_baseline.txt"
    if not path.exists():
        return {}
    out: dict[str, int] = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.startswith("="):
            t, n = line.split(":", 1)
            try:
                out[t.strip()] = int(n.strip())
            except ValueError:
                continue
    return out


def main() -> None:
    metrics: dict = {}

    # Kafka totals + delta vs baseline
    base = baseline_offsets()
    kafka_now = {
        t: kafka_offset_total(t)
        for t in ("gtfs-vehicle", "gtfs-trips", "gtfs-alerts", "gtfs-dlq", "weather-feed")
    }
    metrics["kafka_offsets_total"] = kafka_now
    metrics["kafka_offsets_delta"] = {
        t: kafka_now[t] - base.get(t, 0) for t in kafka_now
    }
    metrics["kafka_vehicle_partition_offsets"] = kafka_offsets_by_partition("gtfs-vehicle")
    metrics["kafka_trips_partition_offsets"] = kafka_offsets_by_partition("gtfs-trips")

    # Staging — vehicle_positions
    veh = read_staging("vehicle_positions")
    if veh is not None and len(veh) > 0:
        null_sci = int(veh["station_complex_id"].isna().sum())
        top_stations = (
            veh.dropna(subset=["station_complex_id"])
               .groupby("station_complex_id").size()
               .sort_values(ascending=False).head(20)
        )
        counts = veh.dropna(subset=["station_complex_id"]).groupby("station_complex_id").size()
        skew_max = int(counts.max())
        skew_med = int(counts.median())
        metrics["vehicle_positions"] = {
            "rows": int(len(veh)),
            "distinct_stations": int(veh["station_complex_id"].nunique()),
            "distinct_routes": int(veh["route_id"].nunique()),
            "null_station_complex_id": null_sci,
            "null_station_complex_id_pct": round(100 * null_sci / len(veh), 2),
            "skew_max_station_count": skew_max,
            "skew_median_station_count": skew_med,
            "skew_ratio_max_over_median": round(skew_max / skew_med, 2) if skew_med else None,
            "top20_stations": [
                {"station_complex_id": str(k), "count": int(v)} for k, v in top_stations.items()
            ],
            "earliest_event": str(veh["event_timestamp"].min()),
            "latest_event": str(veh["event_timestamp"].max()),
        }

    # Staging — trip_delays
    tr = read_staging("trip_delays")
    if tr is not None and len(tr) > 0:
        d = tr["arrival_delay_secs"]
        null_d = int(d.isna().sum())
        metrics["trip_delays"] = {
            "rows": int(len(tr)),
            "distinct_trips": int(tr["trip_id"].nunique()),
            "distinct_stops": int(tr["stop_id"].nunique()),
            "null_arrival_delay_secs": null_d,
            "null_arrival_delay_secs_pct": round(100 * null_d / len(tr), 2),
            "delay_min": int(d.min()) if null_d < len(tr) else None,
            "delay_p50": int(d.quantile(0.50)) if null_d < len(tr) else None,
            "delay_p95": int(d.quantile(0.95)) if null_d < len(tr) else None,
            "delay_p99": int(d.quantile(0.99)) if null_d < len(tr) else None,
            "delay_max": int(d.max()) if null_d < len(tr) else None,
            "earliest_event": str(tr["event_timestamp"].min()),
            "latest_event": str(tr["event_timestamp"].max()),
        }

    print(json.dumps(metrics, indent=2, default=str))


if __name__ == "__main__":
    main()
