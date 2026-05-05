"""Task A5.1 — PySpark Structured Streaming consumer for `gtfs-trips`.

Reads JSON `TripUpdate` records from Kafka, explodes the `stop_time_update`
array to one row per stop, and extracts `arrival.delay` directly (per the
plan — do NOT compute deltas manually).

Output: `data/staging/trip_delays/` partitioned by `event_date`.
Checkpoint: `checkpoints/trip_delays/`.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        -m processing.gtfs_trips_consumer
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ingestion.config import KAFKA_BOOTSTRAP, TOPIC_TRIPS
from processing.config import CHECKPOINT_DIR, STAGING_DIR, ensure_dir, path_str
from processing.spark_utils import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gtfs_trips_consumer")

# JSON shape produced by gtfs_producer for TripUpdate entities.
# The producer flattens via MessageToDict(preserving_proto_field_name=True),
# so `stop_time_update` is the proto field name (not camelCase).
STOP_TIME_UPDATE_SCHEMA = StructType([
    StructField("stop_id", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("arrival", StructType([
        StructField("time", LongType()),
        StructField("delay", IntegerType()),
    ])),
    StructField("departure", StructType([
        StructField("time", LongType()),
        StructField("delay", IntegerType()),
    ])),
])

TRIP_UPDATE_SCHEMA = StructType([
    StructField("feed", StringType()),
    StructField("feed_timestamp", LongType()),
    StructField("entity_id", StringType()),
    StructField("route_id", StringType()),
    StructField("trip", StructType([
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
    ])),
    StructField("stop_time_update", ArrayType(STOP_TIME_UPDATE_SCHEMA)),
    StructField("timestamp", LongType()),
])


def build_pipeline(args: argparse.Namespace) -> None:
    spark = get_spark(
        "gtfs-trips-consumer",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"],
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC_TRIPS)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
        .select(F.from_json("json_value", TRIP_UPDATE_SCHEMA).alias("u"), "kafka_ts")
        .select(
            F.col("u.trip.trip_id").alias("trip_id"),
            F.coalesce(F.col("u.route_id"), F.col("u.trip.route_id")).alias("route_id"),
            F.col("u.timestamp").alias("event_unix_ts"),
            F.col("kafka_ts"),
            F.col("u.stop_time_update").alias("stop_time_update"),
        )
        .withColumn("stu", F.explode_outer("stop_time_update"))
        .select(
            "trip_id",
            "route_id",
            F.col("stu.stop_id").alias("stop_id"),
            F.col("stu.stop_sequence").alias("stop_sequence"),
            F.col("stu.arrival.delay").alias("arrival_delay_secs"),
            F.col("stu.departure.delay").alias("departure_delay_secs"),
            F.col("stu.arrival.time").alias("arrival_time_epoch"),
            F.col("event_unix_ts"),
            F.col("kafka_ts"),
        )
        .withColumn(
            "event_timestamp",
            F.when(
                F.col("event_unix_ts").isNotNull(),
                F.to_timestamp(F.from_unixtime(F.col("event_unix_ts"))),
            ).otherwise(F.col("kafka_ts")),
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
        .filter(F.col("stop_id").isNotNull())
        .filter(F.col("arrival_delay_secs").isNotNull())
    )

    output_path = ensure_dir(STAGING_DIR / "trip_delays")
    checkpoint = ensure_dir(CHECKPOINT_DIR / "trip_delays")

    log.info(
        "starting stream: kafka=%s topic=%s output=%s checkpoint=%s",
        args.bootstrap, TOPIC_TRIPS, output_path, checkpoint,
    )

    query = (
        parsed.writeStream.format("parquet")
        .option("path", path_str(output_path))
        .option("checkpointLocation", path_str(checkpoint))
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GTFS trips Kafka → Parquet streaming consumer")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    return p.parse_args()


if __name__ == "__main__":
    build_pipeline(parse_args())
