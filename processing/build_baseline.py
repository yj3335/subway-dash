from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import DEFAULT_DATA_DIR, RIDERSHIP_CLEAN_DIR, WEATHER_CLEAN_DIR
from processing.cassandra_writer import write_to_cassandra
from processing.spark_utils import get_spark


BASELINE_OUTPUT_DIR = DEFAULT_DATA_DIR / "batch" / "station_capacity_baseline"
STATION_MAX_OUTPUT_DIR = DEFAULT_DATA_DIR / "batch" / "station_max_entries"


def build_baseline_dataframes(ridership_df, weather_df=None, *, lookback_days: int = 90):
    from pyspark.sql import functions as F

    ridership = ridership_df
    if "date" not in ridership.columns:
        ridership = ridership.withColumn("date", F.to_date("transit_timestamp"))
    if "hour_of_day" not in ridership.columns:
        ridership = ridership.withColumn("hour_of_day", F.hour("transit_timestamp"))
    if "day_of_week" not in ridership.columns:
        ridership = ridership.withColumn("day_of_week", F.dayofweek("transit_timestamp"))

    if "weather_bucket" not in ridership.columns:
        if weather_df is None:
            ridership = ridership.withColumn("weather_bucket", F.lit("clear"))
        else:
            weather_daily = weather_df.select("date", "weather_bucket").dropDuplicates(["date"])
            ridership = ridership.join(weather_daily, on="date", how="left").fillna({"weather_bucket": "clear"})

    if lookback_days and lookback_days > 0:
        max_date = ridership.agg(F.max("date").alias("max_date")).collect()[0]["max_date"]
        if max_date:
            ridership = ridership.filter(F.col("date") >= F.date_sub(F.lit(max_date), int(lookback_days)))

    prepared = ridership.select(
        F.col("station_complex_id").cast("string").alias("station_complex_id"),
        F.col("day_of_week").cast("int").alias("day_of_week"),
        F.col("hour_of_day").cast("int").alias("hour_of_day"),
        F.col("weather_bucket").cast("string").alias("weather_bucket"),
        F.col("entries").cast("double").alias("entries"),
    ).filter(F.col("station_complex_id").isNotNull() & F.col("entries").isNotNull())

    baseline = prepared.groupBy("station_complex_id", "day_of_week", "hour_of_day", "weather_bucket").agg(
        F.avg("entries").alias("avg_entries"),
        F.stddev("entries").alias("std_entries"),
        F.percentile_approx("entries", 0.95).alias("p95_entries"),
    )
    station_max = prepared.groupBy("station_complex_id").agg(F.max("entries").alias("max_hourly_entries"))
    return baseline, station_max


def write_outputs(baseline_df, station_max_df, *, sink: str, baseline_output: str, station_max_output: str) -> None:
    sinks = {part.strip() for part in sink.split(",") if part.strip()}
    if "parquet" in sinks:
        Path(baseline_output).mkdir(parents=True, exist_ok=True)
        Path(station_max_output).mkdir(parents=True, exist_ok=True)
        baseline_df.write.mode("overwrite").parquet(baseline_output)
        station_max_df.write.mode("overwrite").parquet(station_max_output)
    if "cassandra" in sinks:
        write_to_cassandra(baseline_df, "station_capacity_baseline", mode="overwrite")
        write_to_cassandra(station_max_df, "station_max_entries", mode="overwrite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build station capacity baseline and station max tables.")
    parser.add_argument("--ridership-input", default=str(RIDERSHIP_CLEAN_DIR))
    parser.add_argument("--weather-input", default=str(WEATHER_CLEAN_DIR))
    parser.add_argument("--ridership-weather-input", default=None)
    parser.add_argument("--baseline-output", default=str(BASELINE_OUTPUT_DIR))
    parser.add_argument("--station-max-output", default=str(STATION_MAX_OUTPUT_DIR))
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--sink", default="parquet", help="Comma-separated sinks: parquet,cassandra")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-build-baseline")
    if args.ridership_weather_input:
        ridership_df = spark.read.parquet(args.ridership_weather_input)
        weather_df = None
    else:
        ridership_df = spark.read.parquet(args.ridership_input)
        weather_df = spark.read.parquet(args.weather_input)
    baseline_df, station_max_df = build_baseline_dataframes(ridership_df, weather_df, lookback_days=args.lookback_days)
    write_outputs(
        baseline_df,
        station_max_df,
        sink=args.sink,
        baseline_output=args.baseline_output,
        station_max_output=args.station_max_output,
    )
    print(f"baseline rows={baseline_df.count():,} station_max rows={station_max_df.count():,} sink={args.sink}")


if __name__ == "__main__":
    main()

