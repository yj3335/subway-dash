from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import BRIDGE_PARQUET, CHECKPOINT_DIR, DEFAULT_DATA_DIR, STAGING_DIR
from processing.spark_utils import get_spark
from processing.transforms import (
    MODERATE_THRESHOLD,
    SERVICE_GRACE_SECS,
    SERVICE_SATURATION_SECS,
    SEVERE_THRESHOLD,
)


SPEED_LAYER_DELAYS_DIR = STAGING_DIR / "speed_layer_delays"
LAMBDA_DEBUG_OUTPUT_DIR = DEFAULT_DATA_DIR / "debug" / "lambda_merge"
BASELINE_PARQUET_DIR = DEFAULT_DATA_DIR / "batch" / "station_capacity_baseline"
STATION_MAX_PARQUET_DIR = DEFAULT_DATA_DIR / "batch" / "station_max_entries"
MONGO_URI = "mongodb://localhost:27017"


def delay_signal_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("station_complex_id", T.StringType()),
            T.StructField("event_timestamp", T.TimestampType()),
            T.StructField("avg_arrival_delay_secs", T.DoubleType()),
        ]
    )


def load_batch_views(spark, *, source: str, baseline_path: str, station_max_path: str):
    if source == "cassandra":
        from processing.cassandra_writer import read_from_cassandra

        return read_from_cassandra(spark, "station_capacity_baseline"), read_from_cassandra(spark, "station_max_entries")
    if source == "parquet":
        return spark.read.parquet(baseline_path), spark.read.parquet(station_max_path)
    raise ValueError(f"Unsupported batch source: {source}")


def score_batch(batch_df, baseline_df, station_max_df, bridge_df, *, weather_bucket: str):
    from pyspark.sql import functions as F

    baseline = F.broadcast(baseline_df)
    station_max = F.broadcast(station_max_df)
    bridge = F.broadcast(
        bridge_df.filter(F.col("station_complex_id").isNotNull())
        .select("station_complex_id", "complex_name")
        .dropDuplicates(["station_complex_id"])
    )

    enriched = (
        batch_df.withColumn("hour_of_day", F.hour("event_timestamp"))
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))
        .withColumn("weather_bucket", F.lit(weather_bucket))
        .join(baseline, on=["station_complex_id", "day_of_week", "hour_of_day", "weather_bucket"], how="left")
        .join(station_max, on="station_complex_id", how="left")
        .join(bridge, on="station_complex_id", how="left")
    )

    return (
        enriched.withColumn(
            "service_deficit",
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (F.col("avg_arrival_delay_secs") - F.lit(float(SERVICE_GRACE_SECS)))
                    / F.lit(float(SERVICE_SATURATION_SECS - SERVICE_GRACE_SECS)),
                ),
            ),
        )
        .withColumn(
            "demand_intensity",
            F.when(F.col("max_hourly_entries") > 0, F.col("avg_entries") / F.col("max_hourly_entries")).otherwise(F.lit(0.0)),
        )
        .withColumn("demand_intensity", F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("demand_intensity"))))
        .withColumn("congestion_score", F.col("service_deficit") * F.col("demand_intensity"))
        .withColumn(
            "predicted_delay_mins",
            (F.col("avg_arrival_delay_secs") / F.lit(60.0)) * (F.lit(1.0) + F.col("demand_intensity")),
        )
        .withColumn(
            "alert_level",
            F.when(F.col("congestion_score") >= F.lit(float(SEVERE_THRESHOLD)), F.lit("SEVERE"))
            .when(F.col("congestion_score") >= F.lit(float(MODERATE_THRESHOLD)), F.lit("MODERATE"))
            .otherwise(F.lit("NORMAL")),
        )
        .withColumn("inserted_at", F.current_timestamp())
        .select(
            "station_complex_id",
            "complex_name",
            "event_timestamp",
            "avg_arrival_delay_secs",
            "service_deficit",
            "demand_intensity",
            "congestion_score",
            "predicted_delay_mins",
            "alert_level",
            "weather_bucket",
            "inserted_at",
        )
    )


def write_scored(scored_df, *, sink: str, mongo_uri: str, debug_output: str) -> None:
    if sink == "mongodb":
        (
            scored_df.write.format("mongodb")
            .option("spark.mongodb.connection.uri", mongo_uri)
            .option("spark.mongodb.database", "subway_dash")
            .option("spark.mongodb.collection", "speed_layer")
            .mode("append")
            .save()
        )
    elif sink == "parquet":
        scored_df.write.mode("append").parquet(debug_output)
    elif sink == "console":
        scored_df.show(truncate=False)
    else:
        raise ValueError(f"Unsupported sink: {sink}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge live delay signal with batch baseline and write congestion documents.")
    parser.add_argument("--input", default=str(SPEED_LAYER_DELAYS_DIR))
    parser.add_argument("--checkpoint", default=str(CHECKPOINT_DIR / "lambda_merge"))
    parser.add_argument("--batch-source", choices=["parquet", "cassandra"], default="parquet")
    parser.add_argument("--baseline-path", default=str(BASELINE_PARQUET_DIR))
    parser.add_argument("--station-max-path", default=str(STATION_MAX_PARQUET_DIR))
    parser.add_argument("--bridge-path", default=str(BRIDGE_PARQUET))
    parser.add_argument("--sink", choices=["mongodb", "parquet", "console"], default="parquet")
    parser.add_argument("--mongo-uri", default=MONGO_URI)
    parser.add_argument("--debug-output", default=str(LAMBDA_DEBUG_OUTPUT_DIR))
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=None,
        help="Optional file-source backpressure limit for speed-layer input.",
    )
    parser.add_argument(
        "--latest-first",
        action="store_true",
        help="Process newest speed-layer files first when catching up from an existing backlog.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-lambda-merge")
    baseline_df, station_max_df = load_batch_views(
        spark,
        source=args.batch_source,
        baseline_path=args.baseline_path,
        station_max_path=args.station_max_path,
    )
    bridge_df = spark.read.parquet(args.bridge_path)

    from infra.weather_cache import get_current_weather_bucket

    def process_micro_batch(batch_df, batch_id: int) -> None:
        bucket = get_current_weather_bucket()
        scored = score_batch(batch_df, baseline_df, station_max_df, bridge_df, weather_bucket=bucket)
        write_scored(scored, sink=args.sink, mongo_uri=args.mongo_uri, debug_output=args.debug_output)
        print(f"processed lambda batch_id={batch_id} weather_bucket={bucket} rows={batch_df.count()}")

    Path(args.debug_output).mkdir(parents=True, exist_ok=True)
    stream_reader = spark.readStream.schema(delay_signal_schema())
    if args.max_files_per_trigger is not None:
        stream_reader = stream_reader.option("maxFilesPerTrigger", args.max_files_per_trigger)
    if args.latest_first:
        stream_reader = stream_reader.option("latestFirst", "true")
    live_stream = stream_reader.parquet(args.input)
    query = (
        live_stream.writeStream.foreachBatch(process_micro_batch)
        .trigger(processingTime=args.trigger)
        .option("checkpointLocation", args.checkpoint)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
