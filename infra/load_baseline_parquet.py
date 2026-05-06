"""Load a pre-computed station_capacity_baseline Parquet directory into Cassandra.

Usage:
    python infra/load_baseline_parquet.py --input <path-to-parquet-dir>

Reads all part-*.parquet files in the directory and bulk-inserts into
subway_dash.station_capacity_baseline using concurrent prepared statements.
"""
import argparse
import glob
import os
import sys

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input", required=True,
        help="Directory containing part-*.parquet files (build_baseline.py --sink parquet output)"
    )
    p.add_argument("--cassandra-host", default=os.environ.get("CASSANDRA_HOSTS", "localhost"))
    p.add_argument("--concurrency", type=int, default=50)
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.input, "part-*.parquet")))
    if not files:
        sys.exit(f"No part-*.parquet files found in {args.input!r}")

    print(f"Reading {len(files)} Parquet file(s)…")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"  {len(df):,} rows, {df['station_complex_id'].nunique()} stations")

    hosts = args.cassandra_host.split(",")
    cluster = Cluster(hosts)
    session = cluster.connect()

    stmt = session.prepare(
        "INSERT INTO subway_dash.station_capacity_baseline "
        "(station_complex_id, weather_bucket, day_of_week, hour_of_day, "
        " avg_entries, std_entries, p95_entries) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    params = [
        (
            str(row.station_complex_id),
            str(row.weather_bucket),
            int(row.day_of_week),
            int(row.hour_of_day),
            float(row.avg_entries),
            float(row.std_entries),
            float(row.p95_entries),
        )
        for row in df.itertuples(index=False)
    ]

    print(f"Inserting {len(params):,} rows (concurrency={args.concurrency})…")
    results = execute_concurrent_with_args(session, stmt, params, concurrency=args.concurrency)

    errors = [r for r in results if not r.success]
    if errors:
        print(f"  ⚠ {len(errors)} errors")
        for e in errors[:5]:
            print(f"    {e.result_or_exc}")
    else:
        print(f"  ✓ All {len(params):,} rows inserted successfully")

    cluster.shutdown()


if __name__ == "__main__":
    main()
