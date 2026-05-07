from __future__ import annotations

import argparse
from pathlib import Path


ASSUMPTIONS = [
    ("A-01", "Tiered bridge table coverage"),
    ("A-02", "Levenshtein threshold <= 4"),
    ("A-03", "Haversine proximity radius"),
    ("A-04", "Baseline lookback window"),
    ("A-05", "Weather bucket thresholds"),
    ("A-06", "MODERATE/SEVERE alert thresholds"),
    ("A-07", "Service deficit 60s/300s endpoints"),
    ("A-08", "Demand intensity self-max normalization"),
    ("A-09", "Single NYC-wide weather bucket"),
    ("A-10", "30-second micro-batch interval"),
    ("A-11", "Kafka maxOffsetsPerTrigger"),
    ("A-12", "Spark skew/salted join need"),
    ("A-13", "Station count"),
    ("A-14", "Dashboard severe alert threshold"),
    ("A-15", "Daily NOAA weather granularity"),
    ("A-16", "GTFS arrival.delay population"),
]


def render() -> str:
    lines = [
        "# Assumptions Registry Audit",
        "",
        "| ID | Assumption | Final Status | Evidence / Notes |",
        "|---|---|---|---|",
    ]
    for assumption_id, text in ASSUMPTIONS:
        lines.append(f"| {assumption_id} | {text} | UNRESOLVED | Fill after validation run |")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create final assumptions registry audit template.")
    parser.add_argument("--output", default="docs/assumptions_audit.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render(), encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
