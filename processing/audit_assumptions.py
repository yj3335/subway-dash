from __future__ import annotations

import argparse
from pathlib import Path


ASSUMPTIONS = [
    ("A-01", "Tiered bridge table coverage", "Preyansh"),
    ("A-02", "Levenshtein threshold <= 4", "Preyansh"),
    ("A-03", "Haversine proximity radius", "Preyansh"),
    ("A-04", "Baseline lookback window", "Preyansh"),
    ("A-05", "Weather bucket thresholds", "Preyansh"),
    ("A-06", "MODERATE/SEVERE alert thresholds", "Preyansh"),
    ("A-07", "Service deficit 60s/300s endpoints", "Preyansh"),
    ("A-08", "Demand intensity self-max normalization", "Preyansh"),
    ("A-09", "Single NYC-wide weather bucket", "Arjun"),
    ("A-10", "30-second micro-batch interval", "Arjun"),
    ("A-11", "Kafka maxOffsetsPerTrigger", "Arjun"),
    ("A-12", "Spark skew/salted join need", "Arjun"),
    ("A-13", "Station count", "Preyansh"),
    ("A-14", "Dashboard severe alert threshold", "Yash"),
    ("A-15", "Daily NOAA weather granularity", "Preyansh"),
    ("A-16", "GTFS arrival.delay population", "Arjun"),
]


def render() -> str:
    lines = [
        "# Assumptions Registry Audit",
        "",
        "| ID | Assumption | Owner | Final Status | Evidence / Notes |",
        "|---|---|---|---|---|",
    ]
    for assumption_id, text, owner in ASSUMPTIONS:
        lines.append(f"| {assumption_id} | {text} | {owner} | UNRESOLVED | Fill after validation run |")
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

