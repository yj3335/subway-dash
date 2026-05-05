from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import RIDERSHIP_CLEAN_DIR, RIDERSHIP_WEATHER_DIR, WEATHER_CLEAN_DIR
from processing.spark_utils import get_spark


def join_ridership_weather(ridership_df, weather_df):
    from pyspark.sql import functions as F

    weather_daily = weather_df.select("date", "weather_bucket").dropDuplicates(["date"])
    joined = ridership_df.join(weather_daily, on="date", how="left").withColumn(
        "weather_join_missing", F.col("weather_bucket").isNull()
    )
    return joined.fillna({"weather_bucket": "clear"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join cleaned ridership to daily NOAA weather on date.")
    parser.add_argument("--ridership-input", default=str(RIDERSHIP_CLEAN_DIR))
    parser.add_argument("--weather-input", default=str(WEATHER_CLEAN_DIR))
    parser.add_argument("--output", default=str(RIDERSHIP_WEATHER_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-join-ridership-weather")
    ridership_df = spark.read.parquet(args.ridership_input)
    weather_df = spark.read.parquet(args.weather_input)
    joined = join_ridership_weather(ridership_df, weather_df)
    null_count = joined.filter("weather_bucket IS NULL").count()
    total = joined.count()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    joined.write.mode("overwrite").partitionBy("date").parquet(args.output)
    print(f"joined rows={total:,} missing_weather_rows={null_count:,} output={args.output}")


if __name__ == "__main__":
    main()
