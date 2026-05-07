"""PySpark Structured Streaming consumer for `gtfs-trips`.

Reads JSON `TripUpdate` records from Kafka, explodes the `stop_time_update`
array to one row per stop, and resolves `arrival_delay_secs` for every
NYCT route — not just the L line.

The MTA only populates `arrival.delay` on the L-line GTFS-Realtime feed.
For every other route the field is null but `arrival.time` (predicted unix
epoch) is populated. We compute the delay ourselves by joining against
the GTFS *static* schedule:

    delay = arrival.time - (service_date_midnight_NY_utc + sched_arr_secs)

The static schedule is built by `scripts/build_schedule_lookup.py` and
broadcast at consumer startup. trip_id mapping: realtime trip_id is the
suffix of the static trip_id after the first underscore (the MTA static
prefix uses dashes, not underscores).

Output: `data/staging/trip_delays/` partitioned by `event_date`.
Checkpoint: `checkpoints/trip_delays/`.

Run:
    python -m scripts.build_schedule_lookup           # one-time / nightly
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        -m processing.gtfs_trips_consumer
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

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
from processing.config import CHECKPOINT_DIR, REPO_ROOT, STAGING_DIR, ensure_dir, path_str
from processing.spark_utils import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gtfs_trips_consumer")

DEFAULT_SCHEDULE_PARQUET = REPO_ROOT / "data" / "schedule" / "schedule_lookup.parquet"
DEFAULT_SCHEDULE_PREFIX_PARQUET = (
    REPO_ROOT / "data" / "schedule" / "schedule_lookup_prefix.parquet"
)

# JSON shape produced by gtfs_producer for TripUpdate entities.
# The producer flattens via MessageToDict(preserving_proto_field_name=True),
# so `stop_time_update` is the proto field name (not camelCase).
#
# IMPORTANT: protobuf canonical JSON encodes int64/uint64 fields as STRINGS
# (because JSON numbers can't safely carry 64-bit precision). So
# `arrival.time`, `departure.time`, `feed_timestamp`, and `timestamp` are
# declared as StringType here and cast to long after parsing. int32 fields
# (delay, stop_sequence) are JSON numbers and decode as IntegerType cleanly.
# This caught us once: declaring `arrival.time` as LongType produces silent
# nulls because Spark's from_json refuses string-to-long coercion in
# PERMISSIVE mode.
STOP_TIME_UPDATE_SCHEMA = StructType([
    StructField("stop_id", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("arrival", StructType([
        StructField("time", StringType()),  # int64 → JSON string
        StructField("delay", IntegerType()),
    ])),
    StructField("departure", StructType([
        StructField("time", StringType()),  # int64 → JSON string
        StructField("delay", IntegerType()),
    ])),
])

TRIP_UPDATE_SCHEMA = StructType([
    StructField("feed", StringType()),
    StructField("feed_timestamp", StringType()),  # int64 → JSON string
    StructField("entity_id", StringType()),
    StructField("route_id", StringType()),
    StructField("trip", StructType([
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
    ])),
    StructField("stop_time_update", ArrayType(STOP_TIME_UPDATE_SCHEMA)),
    StructField("timestamp", StringType()),  # int64 → JSON string
])


def build_pipeline(args: argparse.Namespace) -> None:
    spark = get_spark(
        "gtfs-trips-consumer",
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"],
    )
    spark.sparkContext.setLogLevel("WARN")

    schedule_path = path_str(args.schedule or DEFAULT_SCHEDULE_PARQUET)
    schedule_prefix_path = path_str(args.schedule_prefix or DEFAULT_SCHEDULE_PREFIX_PARQUET)

    # Full lookup: keyed on (trip_id_suffix, stop_id). Used for routes that
    # publish the full realtime trip_id with run-id intact (most routes).
    schedule = (
        spark.read.parquet(schedule_path)
        .select(
            F.col("trip_id_suffix").alias("sched_trip_id"),
            F.col("stop_id").alias("sched_stop_id"),
            F.col("sched_arr_secs"),
        )
        .dropDuplicates(["sched_trip_id", "sched_stop_id"])
    )
    log.info("loaded schedule lookup: %d rows from %s", schedule.count(), schedule_path)

    # Prefix lookup: keyed on (prefix, stop_id). Used as fallback for routes
    # where the realtime feed strips the run-id (L, 7, SI, FX, 7X). Median
    # sched_arr_secs across all matching runs gives a "typical" scheduled
    # arrival — coarse but better than no signal.
    schedule_prefix = (
        spark.read.parquet(schedule_prefix_path)
        .select(
            F.col("prefix").alias("sched_prefix"),
            F.col("stop_id").alias("sched_prefix_stop_id"),
            F.col("sched_arr_secs").alias("sched_arr_secs_prefix"),
        )
    )
    log.info(
        "loaded schedule prefix lookup: %d rows from %s",
        schedule_prefix.count(),
        schedule_prefix_path,
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC_TRIPS)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        # Group ID prefix so this consumer shows up under the documented
        # name in `kafka-consumer-groups.sh --list` (per docs/kafka_config.md).
        # Spark composes `<prefix>-<UUID>` internally; using kafka.group.id
        # directly clashes with Spark's offset management and produces a WARN.
        .option("groupIdPrefix", "spark-trips-consumer")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
        .select(F.from_json("json_value", TRIP_UPDATE_SCHEMA).alias("u"), "kafka_ts")
        .select(
            F.col("u.trip.trip_id").alias("trip_id"),
            F.coalesce(F.col("u.route_id"), F.col("u.trip.route_id")).alias("route_id"),
            F.col("u.trip.start_date").alias("start_date"),
            # Cast string→long for the int64 protobuf timestamp fields
            F.col("u.timestamp").cast(LongType()).alias("event_unix_ts"),
            F.col("kafka_ts"),
            F.col("u.stop_time_update").alias("stop_time_update"),
        )
        .withColumn("stu", F.explode_outer("stop_time_update"))
        .select(
            "trip_id",
            "route_id",
            "start_date",
            F.col("stu.stop_id").alias("stop_id"),
            F.col("stu.stop_sequence").alias("stop_sequence"),
            F.col("stu.arrival.delay").alias("arrival_delay_native"),
            F.col("stu.departure.delay").alias("departure_delay_secs"),
            # Cast string→long for the int64 arrival.time
            F.col("stu.arrival.time").cast(LongType()).alias("arrival_time_epoch"),
            F.col("event_unix_ts"),
            F.col("kafka_ts"),
        )
        # Derive a trip_id prefix (drop the run-id, keep through direction
        # letter) so we can fall back to a coarser match for L/7/SI/FX/7X
        # which strip the run-id from the realtime feed.
        # Pattern matched: <originSecs>_<route>..<dirLetter><runId>
        # We capture <originSecs>_<route>..<dirLetter>.
        .withColumn(
            "trip_id_prefix",
            F.regexp_extract(F.col("trip_id"), r"^(.+\.\.[NSEW])", 1),
        )
        # First join: full trip_id match (most routes have run-id in realtime).
        .join(F.broadcast(schedule),
              (F.col("trip_id") == F.col("sched_trip_id")) &
              (F.col("stop_id") == F.col("sched_stop_id")),
              how="left")
        .drop("sched_trip_id", "sched_stop_id")
        # Second join: prefix match (fallback for routes that strip run-id).
        .join(F.broadcast(schedule_prefix),
              (F.col("trip_id_prefix") == F.col("sched_prefix")) &
              (F.col("stop_id") == F.col("sched_prefix_stop_id")),
              how="left")
        .drop("sched_prefix", "sched_prefix_stop_id")
        # Coalesce: prefer full match, fall back to prefix match.
        .withColumn(
            "sched_arr_secs",
            F.coalesce(F.col("sched_arr_secs"), F.col("sched_arr_secs_prefix")),
        )
        .drop("sched_arr_secs_prefix")
        .withColumn(
            # Service date midnight in NY local → unix epoch (UTC).
            # to_utc_timestamp interprets the input as being in the named TZ.
            "service_date_unix",
            F.unix_timestamp(
                F.to_utc_timestamp(
                    F.to_timestamp(F.col("start_date"), "yyyyMMdd"),
                    "America/New_York",
                )
            ),
        )
        .withColumn(
            "scheduled_arrival_unix",
            F.col("service_date_unix") + F.col("sched_arr_secs"),
        )
        .withColumn(
            "arrival_delay_computed",
            F.when(
                F.col("arrival_time_epoch").isNotNull()
                & F.col("scheduled_arrival_unix").isNotNull(),
                (F.col("arrival_time_epoch") - F.col("scheduled_arrival_unix")).cast(IntegerType()),
            ),
        )
        .withColumn(
            # Coalesce: prefer the MTA-published delay (L line), fall back
            # to our computed delay (every other route).
            "arrival_delay_secs",
            F.coalesce(F.col("arrival_delay_native"), F.col("arrival_delay_computed")),
        )
        .withColumn(
            "delay_source",
            F.when(F.col("arrival_delay_native").isNotNull(), F.lit("native"))
             .when(F.col("arrival_delay_computed").isNotNull(), F.lit("computed"))
             .otherwise(F.lit("missing")),
        )
        .withColumn(
            "event_timestamp",
            F.when(
                F.col("event_unix_ts").isNotNull(),
                F.to_timestamp(F.from_unixtime(F.col("event_unix_ts"))),
            ).otherwise(F.col("kafka_ts")),
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
        .select(
            "trip_id",
            "route_id",
            "start_date",
            "stop_id",
            "stop_sequence",
            "arrival_delay_secs",
            "departure_delay_secs",
            "arrival_time_epoch",
            "scheduled_arrival_unix",
            "delay_source",
            "event_unix_ts",
            "kafka_ts",
            "event_timestamp",
            "event_date",
        )
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
    p.add_argument("--schedule", default=None,
                   help="Override path to data/schedule/schedule_lookup.parquet")
    p.add_argument("--schedule-prefix", default=None,
                   help="Override path to data/schedule/schedule_lookup_prefix.parquet")
    return p.parse_args()


if __name__ == "__main__":
    build_pipeline(parse_args())
