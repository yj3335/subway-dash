from __future__ import annotations

import argparse
from pathlib import Path

from processing.cassandra_writer import write_to_cassandra
from processing.config import DEFAULT_DATA_DIR, RIDERSHIP_WEATHER_DIR
from processing.spark_utils import get_spark


WEATHER_COEFFICIENTS_DIR = DEFAULT_DATA_DIR / "batch" / "weather_coefficients"


def compute_weather_coefficients(df):
    from pyspark.sql import functions as F

    bucket_avg = df.groupBy("station_complex_id", "weather_bucket").agg(F.avg("entries").alias("avg_entries"))
    clear_avg = (
        bucket_avg.filter(F.col("weather_bucket") == "clear")
        .select("station_complex_id", F.col("avg_entries").alias("clear_avg_entries"))
    )
    return (
        bucket_avg.join(clear_avg, on="station_complex_id", how="left")
        .withColumn(
            "multiplier",
            F.when(F.col("clear_avg_entries") > 0, F.col("avg_entries") / F.col("clear_avg_entries")).otherwise(F.lit(None)),
        )
        .select("station_complex_id", "weather_bucket", "multiplier")
        .filter(F.col("multiplier").isNotNull())
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute rain/snow ridership multipliers from ridership-weather baseline.")
    parser.add_argument("--input", default=str(RIDERSHIP_WEATHER_DIR))
    parser.add_argument("--output", default=str(WEATHER_COEFFICIENTS_DIR))
    parser.add_argument("--sink", default="parquet", help="Comma-separated sinks: parquet,cassandra")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-weather-coefficients")
    df = spark.read.parquet(args.input)
    coefficients = compute_weather_coefficients(df)
    sinks = {part.strip() for part in args.sink.split(",") if part.strip()}
    if "parquet" in sinks:
        Path(args.output).mkdir(parents=True, exist_ok=True)
        coefficients.write.mode("overwrite").parquet(args.output)
    if "cassandra" in sinks:
        write_to_cassandra(coefficients, "weather_coefficients", mode="overwrite")
    print(f"weather coefficient rows={coefficients.count():,} sink={args.sink}")


if __name__ == "__main__":
    main()

