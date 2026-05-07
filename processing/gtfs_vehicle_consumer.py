"""PySpark Structured Streaming consumer for `gtfs-vehicle`.

Reads JSON records produced by `ingestion.gtfs_producer`, parses the
VehiclePosition fields, strips the direction suffix from `stop_id`, and
attaches `station_complex_id` via a broadcast join against the bridge table.

Output: `data/staging/vehicle_positions/` partitioned by `event_date`.
Checkpoint: `checkpoints/vehicle_positions/`.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        -m processing.gtfs_vehicle_consumer
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ingestion.config import KAFKA_BOOTSTRAP, TOPIC_VEHICLE
from processing.config import (
    BRIDGE_PARQUET,
    CHECKPOINT_DIR,
    STAGING_DIR,
    ensure_dir,
    path_str,
)
from processing.spark_utils import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gtfs_vehicle_consumer")

# Schema of the JSON value emitted by gtfs_producer for VehiclePosition entities.
# Producer flattens via MessageToDict(preserving_proto_field_name=True), so
# fields like trip.trip_id and stop_id are nested under their proto names.
#
# IMPORTANT: protobuf canonical JSON encodes int64/uint64 as STRINGS. The
# `feed_timestamp` and `timestamp` fields are uint64 → declared StringType
# and cast to long after parsing. `current_stop_sequence` is uint32 →
# JSON number, IntegerType is fine.
VEHICLE_SCHEMA = StructType([
    StructField("feed", StringType()),
    StructField("feed_timestamp", StringType()),  # uint64 → JSON string
    StructField("entity_id", StringType()),
    StructField("route_id", StringType()),
    StructField("trip", StructType([
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
    ])),
    StructField("vehicle", StructType([StructField("id", StringType())])),
    StructField("stop_id", StringType()),
    StructField("current_status", StringType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("timestamp", StringType()),  # uint64 → JSON string
])


def build_pipeline(args: argparse.Namespace) -> None:
    spark = get_spark(
        "gtfs-vehicle-consumer",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"],
    )
    spark.sparkContext.setLogLevel("WARN")

    bridge_path = path_str(args.bridge or BRIDGE_PARQUET)
    bridge = (
        spark.read.parquet(bridge_path)
        .select(
            F.col("gtfs_stop_id").cast("string").alias("gtfs_stop_id"),
            F.col("station_complex_id").cast("string").alias("station_complex_id"),
            F.col("complex_name"),
            F.col("lat"),
            F.col("lon"),
        )
        .dropDuplicates(["gtfs_stop_id"])
    )
    log.info("loaded bridge table: %d rows", bridge.count())

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC_VEHICLE)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        # Group ID prefix so this consumer shows up under the documented
        # name in `kafka-consumer-groups.sh --list` (per docs/kafka_config.md).
        # Spark composes `<prefix>-<UUID>` internally; using kafka.group.id
        # directly clashes with Spark's offset management and produces a WARN.
        .option("groupIdPrefix", "spark-vehicle-consumer")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
        .select(F.from_json("json_value", VEHICLE_SCHEMA).alias("v"), "kafka_ts")
        .select(
            F.col("v.trip.trip_id").alias("trip_id"),
            F.coalesce(F.col("v.route_id"), F.col("v.trip.route_id")).alias("route_id"),
            F.col("v.vehicle.id").alias("vehicle_id"),
            F.col("v.stop_id").alias("stop_id_raw"),
            F.col("v.current_status").alias("current_status"),
            F.col("v.current_stop_sequence").alias("current_stop_sequence"),
            # Cast string→long for the int64 protobuf timestamp
            F.col("v.timestamp").cast(LongType()).alias("event_unix_ts"),
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
        .withColumn(
            "stop_id_base",
            F.regexp_replace(F.col("stop_id_raw"), "[NS]$", ""),
        )
    )

    enriched = parsed.join(
        F.broadcast(bridge),
        parsed.stop_id_base == bridge.gtfs_stop_id,
        how="left",
    ).select(
        "trip_id",
        "route_id",
        "vehicle_id",
        F.col("stop_id_raw").alias("stop_id"),
        "stop_id_base",
        "current_status",
        "current_stop_sequence",
        "event_timestamp",
        "event_date",
        "station_complex_id",
        "complex_name",
        "lat",
        "lon",
    )

    output_path = ensure_dir(STAGING_DIR / "vehicle_positions")
    checkpoint = ensure_dir(CHECKPOINT_DIR / "vehicle_positions")

    log.info(
        "starting stream: kafka=%s topic=%s output=%s checkpoint=%s",
        args.bootstrap, TOPIC_VEHICLE, output_path, checkpoint,
    )

    query = (
        enriched.writeStream.format("parquet")
        .option("path", path_str(output_path))
        .option("checkpointLocation", path_str(checkpoint))
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GTFS vehicle Kafka → Parquet streaming consumer")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--bridge", default=None, help="Override path to station_bridge.parquet")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    return p.parse_args()


if __name__ == "__main__":
    build_pipeline(parse_args())
