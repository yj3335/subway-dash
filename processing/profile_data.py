from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from processing.config import RAW_RIDERSHIP_2020_2024, RAW_RIDERSHIP_2025, RAW_STATIONS, RAW_STOPS, RAW_WEATHER, REPO_ROOT, ensure_parent
from processing.transforms import find_column


@dataclass
class Profile:
    path: Path
    row_count: int
    columns: list[str]
    null_counts: dict[str, int]
    numeric_min: dict[str, float]
    numeric_max: dict[str, float]
    samples: list[dict[str, str]]


def profile_csv(path: Path, *, sample_rows: int = 5) -> Profile:
    if not path.exists():
        raise FileNotFoundError(path)

    row_count = 0
    samples: list[dict[str, str]] = []
    null_counts: dict[str, int] = {}
    numeric_min: dict[str, float] = {}
    numeric_max: dict[str, float] = {}
    non_numeric: set[str] = set()

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        null_counts = {column: 0 for column in columns}
        for row in reader:
            row_count += 1
            if len(samples) < sample_rows:
                samples.append(dict(row))
            for column in columns:
                value = (row.get(column) or "").strip()
                if value == "":
                    null_counts[column] += 1
                    continue
                if column in non_numeric:
                    continue
                try:
                    numeric_value = float(value)
                except ValueError:
                    non_numeric.add(column)
                    numeric_min.pop(column, None)
                    numeric_max.pop(column, None)
                    continue
                numeric_min[column] = min(numeric_min.get(column, numeric_value), numeric_value)
                numeric_max[column] = max(numeric_max.get(column, numeric_value), numeric_value)

    return Profile(path, row_count, columns, null_counts, numeric_min, numeric_max, samples)


def render_profiles(profiles: Iterable[Profile]) -> str:
    lines = ["# Data Profiling Results", ""]
    for profile in profiles:
        lines.extend(
            [
                f"## `{profile.path}`",
                "",
                f"- Rows: `{profile.row_count}`",
                f"- Columns: `{', '.join(profile.columns)}`",
                "",
                "### Null Counts",
                "",
                "| Column | Nulls |",
                "|---|---:|",
            ]
        )
        for column in profile.columns:
            lines.append(f"| `{column}` | {profile.null_counts[column]} |")
        if profile.numeric_min:
            lines.extend(["", "### Numeric Ranges", "", "| Column | Min | Max |", "|---|---:|---:|"])
            for column in sorted(profile.numeric_min):
                lines.append(f"| `{column}` | {profile.numeric_min[column]} | {profile.numeric_max[column]} |")
        lines.extend(["", "### Sample Rows", ""])
        for row in profile.samples:
            lines.append(f"- `{row}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_join_key_map(stations_profile: Profile, ridership_profile: Profile, weather_profile: Profile) -> str:
    stations_columns = stations_profile.columns
    ridership_columns = ridership_profile.columns
    weather_columns = weather_profile.columns
    gtfs_col = find_column(stations_columns, ["GTFS Stop ID", "gtfs_stop_id"])
    complex_col = find_column(stations_columns, ["Station Complex ID", "Complex ID", "Complex MRN"])
    routes_col = find_column(stations_columns, ["Daytime Routes", "daytime_routes"], required=False)
    ridership_station_col = find_column(ridership_columns, ["station_complex_id", "Station Complex ID", "Complex ID"])
    timestamp_col = find_column(ridership_columns, ["transit_timestamp", "Transit Timestamp", "timestamp"])
    entries_col = find_column(ridership_columns, ["entries", "ridership", "Ridership"])
    date_col = find_column(weather_columns, ["DATE", "date"])

    return f"""# Join Key Map

## Confirmed Columns

| Dataset | Purpose | Column |
|---|---|---|
| MTA Stations | GTFS stop key | `{gtfs_col}` |
| MTA Stations | Station complex key | `{complex_col}` |
| MTA Stations | Route labels | `{routes_col or 'not available'}` |
| MTA Hourly Ridership | Station complex key | `{ridership_station_col}` |
| MTA Hourly Ridership | Timestamp | `{timestamp_col}` |
| MTA Hourly Ridership | Entry/ridership count | `{entries_col}` |
| NOAA Daily Weather | Daily join key | `{date_col}` |

## Conventions

- GTFS-Realtime stop IDs such as `127N` and `127S` are normalized to base stop ID `127`.
- Spark `dayofweek()` is used in batch outputs: Sunday = 1, Wednesday = 4, Saturday = 7.
- NOAA historical weather is daily. Ridership joins to weather on `date` only, and the same bucket is applied to all 24 hours.
- Local defaults are under `data/`; every CLI accepts path overrides for Docker or S3 later.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile raw Subway Dash datasets and document join keys.")
    parser.add_argument("--stops", default=str(RAW_STOPS))
    parser.add_argument("--stations", default=str(RAW_STATIONS))
    parser.add_argument("--ridership", default=str(RAW_RIDERSHIP_2020_2024))
    parser.add_argument("--ridership-2025", default=str(RAW_RIDERSHIP_2025))
    parser.add_argument("--weather", default=str(RAW_WEATHER))
    parser.add_argument("--profile-output", default=str(REPO_ROOT / "docs" / "data_profile.md"))
    parser.add_argument("--join-key-output", default=str(REPO_ROOT / "docs" / "join_key_map.md"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = [args.stops, args.stations]
    ridership_paths = []
    for path in [args.ridership, args.ridership_2025]:
        resolved = Path(path)
        if resolved.exists() and resolved not in ridership_paths:
            ridership_paths.append(resolved)
    if not ridership_paths:
        raise FileNotFoundError(
            f"No ridership CSV found. Checked {args.ridership} and {args.ridership_2025}."
        )
    paths.extend(str(path) for path in ridership_paths)
    paths.append(args.weather)
    profiles = [profile_csv(Path(path)) for path in paths]
    profile_output = ensure_parent(args.profile_output)
    profile_output.write_text(render_profiles(profiles), encoding="utf-8")
    join_key_output = ensure_parent(args.join_key_output)
    join_key_output.write_text(render_join_key_map(profiles[1], profiles[2], profiles[-1]), encoding="utf-8")
    print(f"wrote {profile_output}")
    print(f"wrote {join_key_output}")


if __name__ == "__main__":
    main()
