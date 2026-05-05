from __future__ import annotations

import argparse
import csv
from pathlib import Path

from processing.transforms import (
    compute_congestion_score,
    compute_demand_intensity,
    compute_service_deficit,
    classify_alert_level,
)


def backtest_events(events_csv: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with events_csv.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            service_deficit = compute_service_deficit(row["avg_arrival_delay_secs"])
            demand_intensity = compute_demand_intensity(row["avg_entries"], row["max_hourly_entries"])
            score = compute_congestion_score(service_deficit, demand_intensity)
            alert = classify_alert_level(score)
            rows.append(
                {
                    **row,
                    "service_deficit": round(service_deficit, 4),
                    "demand_intensity": round(demand_intensity, 4),
                    "congestion_score": round(score, 4),
                    "alert_level": alert,
                    "passes_expected": not row.get("expected_level") or alert == row.get("expected_level"),
                }
            )
    return rows


def render_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# Phase 2 Validation",
        "",
        "## Threshold Back-Test",
        "",
        "| Event | Station | Delay Secs | Demand | Score | Alert | Expected | Pass |",
        "|---|---|---:|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {event} | {station} | {delay} | {demand} | {score} | {alert} | {expected} | {passed} |".format(
                event=row.get("event_name", ""),
                station=row.get("station_complex_id", ""),
                delay=row.get("avg_arrival_delay_secs", ""),
                demand=row.get("demand_intensity", ""),
                score=row.get("congestion_score", ""),
                alert=row.get("alert_level", ""),
                expected=row.get("expected_level", ""),
                passed="yes" if row.get("passes_expected") else "no",
            )
        )
    lines.extend(
        [
            "",
            "Final threshold changes should be made in `processing/transforms.py`, which is imported by `lambda_merge.py`.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Back-test congestion thresholds against documented historical events.")
    parser.add_argument("--events-csv", required=True)
    parser.add_argument("--output", default="docs/phase2_validation.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = backtest_events(Path(args.events_csv))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(rows), encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()

