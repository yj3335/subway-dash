from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import BRIDGE_PARQUET, RAW_RIDERSHIP, RAW_RIDERSHIP_2020_2024, RAW_RIDERSHIP_2025, RIDERSHIP_CLEAN_DIR
from processing.spark_utils import get_spark
from processing.transforms import find_column


TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
]


def clean_ridership_dataframe(df, *, source_timezone: str = "America/New_York"):
    from functools import reduce

    from pyspark.sql import functions as F

    columns = df.columns
    timestamp_col = find_column(columns, ["transit_timestamp", "Transit Timestamp", "timestamp"])
    station_col = find_column(columns, ["station_complex_id", "Station Complex ID", "Complex ID"])
    entries_col = find_column(columns, ["entries", "ridership", "Ridership"])
    complex_name_col = find_column(columns, ["station_complex", "Station Complex", "complex_name"], required=False)

    timestamp_exprs = [F.to_timestamp(F.col(timestamp_col), fmt) for fmt in TIMESTAMP_FORMATS]
    timestamp_exprs.append(F.to_timestamp(F.col(timestamp_col)))
    parsed_ts = reduce(lambda left, right: F.coalesce(left, right), timestamp_exprs)

    selected = df.select(
        F.col(station_col).cast("string").alias("station_complex_id"),
        F.col(complex_name_col).cast("string").alias("station_complex_name")
        if complex_name_col
        else F.lit(None).cast("string").alias("station_complex_name"),
        F.col(entries_col).cast("double").alias("entries"),
        F.to_utc_timestamp(parsed_ts, source_timezone).alias("transit_timestamp"),
    )

    return (
        selected.filter(F.col("transit_timestamp").isNotNull())
        .filter(F.col("station_complex_id").isNotNull())
        .filter(F.col("entries").isNotNull() & (F.col("entries") >= 0))
        .withColumn("date", F.to_date("transit_timestamp"))
        .withColumn("hour_of_day", F.hour("transit_timestamp"))
        .withColumn("day_of_week", F.dayofweek("transit_timestamp"))
    )


def validate_against_bridge(clean_df, bridge_path: str):
    from pyspark.sql import functions as F

    spark = clean_df.sparkSession
    bridge_ids = spark.read.parquet(bridge_path).filter(F.col("station_complex_id").isNotNull()).select("station_complex_id").distinct()
    return clean_df.join(bridge_ids.withColumn("bridge_resolved", F.lit(True)), on="station_complex_id", how="left").fillna(
        {"bridge_resolved": False}
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean MTA hourly ridership CSV into partitioned Parquet.")
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input ridership CSV. Repeat for multiple files. Defaults to both 2020-2024 and Beginning 2025 files when present.",
    )
    parser.add_argument("--output", default=str(RIDERSHIP_CLEAN_DIR))
    parser.add_argument("--bridge-path", default=str(BRIDGE_PARQUET))
    parser.add_argument("--source-timezone", default="America/New_York")
    parser.add_argument("--skip-bridge-validation", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-clean-ridership")
    inputs = args.input or [str(path) for path in [RAW_RIDERSHIP_2020_2024, RAW_RIDERSHIP_2025, RAW_RIDERSHIP] if Path(path).exists()]
    if not inputs:
        raise FileNotFoundError(
            "No ridership CSV found. Expected data/raw/MTA_Hourly_Ridership_2020_2024.csv "
            "and/or data/raw/MTA_Hourly_Ridership_Beginning_2025.csv."
        )
    raw_df = spark.read.option("mergeSchema", "true").csv(inputs, header=True, inferSchema=False)
    before = raw_df.count()
    clean_df = clean_ridership_dataframe(raw_df, source_timezone=args.source_timezone)
    if not args.skip_bridge_validation and Path(args.bridge_path).exists():
        clean_df = validate_against_bridge(clean_df, args.bridge_path)
    after = clean_df.count()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    clean_df.write.mode("overwrite").partitionBy("date").parquet(args.output)
    print(f"ridership rows before={before:,} after={after:,} output={args.output}")
    if "bridge_resolved" in clean_df.columns:
        clean_df.groupBy("bridge_resolved").count().show()


if __name__ == "__main__":
    main()
