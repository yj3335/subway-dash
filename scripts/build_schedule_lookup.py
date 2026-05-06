"""Build a (trip_id_suffix, stop_id) → scheduled_arrival_secs lookup table.

The MTA only populates `arrival.delay` on the L-line GTFS-Realtime feed.
For every other route, the realtime feed publishes `arrival.time` (predicted
unix epoch) but leaves the `delay` field null. To get a delay value, we
join against the GTFS *static* schedule's `stop_times.txt` on
`(trip_id_suffix, stop_id)`.

trip_id mapping:
  static:    AFA25GEN-1038-Sunday-00_000600_1..S03R
  realtime:  000600_1..S03R   (everything after the first underscore in static)

Output: data/schedule/schedule_lookup.parquet
  trip_id_suffix  STRING  — realtime-style trip identifier
  stop_id         STRING  — direction-suffixed stop_id (e.g. 127N)
  sched_arr_secs  INT     — scheduled arrival, seconds since service-date midnight
                            (can exceed 86400 for after-midnight runs)

Re-run nightly when MTA publishes a new static feed (rare — typically every
schedule change, ~quarterly).
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.request
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
RAW_ZIP = REPO / "data" / "raw" / "google_transit.zip"
OUT_PARQUET = REPO / "data" / "schedule" / "schedule_lookup.parquet"
GTFS_STATIC_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"


def download_static_feed(dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(GTFS_STATIC_URL, headers={"User-Agent": "subway-dash/1.0"})
    print(f"Downloading {GTFS_STATIC_URL} ...")
    data = urllib.request.urlopen(req, timeout=60).read()
    dest.write_bytes(data)
    print(f"  {dest} ({len(data)/1024/1024:.1f} MB)")


def hms_to_secs(hms: str) -> int:
    """HH:MM:SS → seconds since service-date midnight. Tolerates >24h values."""
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def realtime_suffix(static_trip_id: str) -> str:
    """Strip the static schedule prefix, leaving the realtime-equivalent suffix.

    Static IDs always look like '<schedule>_<originSecs>_<route>..<run>'. The
    `<schedule>` half (e.g. 'AFA25GEN-1038-Sunday-00') uses dashes internally
    and contains no underscore, so splitting on the first underscore is safe.
    """
    idx = static_trip_id.find("_")
    return static_trip_id[idx + 1 :] if idx >= 0 else static_trip_id


def trip_prefix(trip_id_suffix: str) -> str:
    """Strip the run-id from a realtime trip_id suffix.

    Some routes (L, 7, SI, FX, 7X) publish realtime trip_ids WITHOUT the run-id:
        088750_L..S          ← realtime
        088750_L..S03R       ← static  (run-id `03R`)
    Most other routes publish the full thing. We need a prefix-only key for
    fallback matching on the routes that strip it.

    Pattern: `<originSecs>_<route>..<directionLetter><run>`. Keep up to and
    including the direction letter; drop everything after. Direction letter
    is the first character after `..`.

    Returns the input unchanged if it doesn't match the expected shape.
    """
    idx = trip_id_suffix.find("..")
    if idx < 0 or idx + 2 >= len(trip_id_suffix):
        return trip_id_suffix
    return trip_id_suffix[: idx + 3]


def build(zip_path: Path, out_path: Path) -> None:
    full_suffixes: list[str] = []
    prefixes: list[str] = []
    stop_ids: list[str] = []
    sched_secs: list[int] = []

    with zipfile.ZipFile(zip_path) as z:
        with z.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            row_count = 0
            for row in reader:
                trip_id = row["trip_id"]
                stop_id = row["stop_id"]
                arr = row["arrival_time"]
                if not arr:
                    # GTFS allows arrival_time to be empty for non-timepoint stops;
                    # those don't have a defined schedule, skip.
                    continue
                suffix = realtime_suffix(trip_id)
                full_suffixes.append(suffix)
                prefixes.append(trip_prefix(suffix))
                stop_ids.append(stop_id)
                sched_secs.append(hms_to_secs(arr))
                row_count += 1

    print(f"Parsed {row_count:,} stop_times rows")

    # Two outputs:
    # 1) full lookup — keyed on (full_suffix, stop_id), used for routes that
    #    publish the full trip_id (most of them)
    # 2) prefix lookup — collapsed by (prefix, stop_id) using the median
    #    sched_arr_secs across all matching runs. Used as a fallback when
    #    the realtime feed strips the run-id (L, 7, SI, FX, 7X). The median
    #    is intentionally a coarse approximation — it picks a "typical"
    #    scheduled time for trips of that route+direction at that stop.
    table_full = pa.table(
        {
            "trip_id_suffix": pa.array(full_suffixes, type=pa.string()),
            "stop_id": pa.array(stop_ids, type=pa.string()),
            "sched_arr_secs": pa.array(sched_secs, type=pa.int32()),
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table_full, out_path)

    # Build the prefix lookup in pandas so we can group + median cleanly
    import pandas as pd

    df = pd.DataFrame({
        "prefix": prefixes,
        "stop_id": stop_ids,
        "sched_arr_secs": sched_secs,
    })
    grouped = (
        df.groupby(["prefix", "stop_id"], as_index=False)["sched_arr_secs"]
          .median()
    )
    grouped["sched_arr_secs"] = grouped["sched_arr_secs"].astype(int)
    prefix_path = out_path.parent / (out_path.stem + "_prefix.parquet")
    pq.write_table(pa.Table.from_pandas(grouped, preserve_index=False), prefix_path)

    print(f"Wrote {out_path}")
    print(f"  full (trip_id_suffix, stop_id) rows : {row_count:,}")
    print(f"  unique trip_id_suffix               : {len(set(full_suffixes)):,}")
    print(f"  unique stop_id                      : {len(set(stop_ids)):,}")
    over_24h = sum(1 for s in sched_secs if s >= 86400)
    print(f"  rows with arrival_time >= 24:00:00  : {over_24h:,} (after-midnight runs)")
    print(f"Wrote {prefix_path}")
    print(f"  prefix (prefix, stop_id) rows       : {len(grouped):,}")
    print(f"  unique prefix                       : {grouped['prefix'].nunique():,}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build GTFS static schedule lookup parquet")
    p.add_argument("--zip", default=str(RAW_ZIP))
    p.add_argument("--out", default=str(OUT_PARQUET))
    p.add_argument("--download", action="store_true", help="Download a fresh static feed first")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    zip_path = Path(args.zip)
    if args.download or not zip_path.exists():
        download_static_feed(zip_path)
    build(zip_path, Path(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
