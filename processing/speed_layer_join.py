from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import CHECKPOINT_DIR, STAGING_DIR
from processing.spark_utils import get_spark


VEHICLE_POSITIONS_DIR = STAGING_DIR / "vehicle_positions"
TRIP_DELAYS_DIR = STAGING_DIR / "trip_delays"
SPEED_LAYER_DELAYS_DIR = STAGING_DIR / "speed_layer_delays"


def vehicle_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("trip_id", T.StringType()),
            T.StructField("route_id", T.StringType()),
            T.StructField("vehicle_id", T.StringType()),
            T.StructField("stop_id", T.StringType()),
            T.StructField("stop_id_base", T.StringType()),
            T.StructField("current_status", T.StringType()),
            T.StructField("current_stop_sequence", T.LongType()),
            T.StructField("event_timestamp", T.TimestampType()),
            T.StructField("station_complex_id", T.StringType()),
            T.StructField("complex_name", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("event_date", T.DateType()),
        ]
    )


def trip_delay_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("trip_id", T.StringType()),
            T.StructField("route_id", T.StringType()),
            T.StructField("stop_id", T.StringType()),
            T.StructField("stop_sequence", T.IntegerType()),
            T.StructField("arrival_delay_secs", T.IntegerType()),
            T.StructField("departure_delay_secs", T.IntegerType()),
            T.StructField("arrival_time_epoch", T.LongType()),
            T.StructField("event_unix_ts", T.LongType()),
            T.StructField("kafka_ts", T.TimestampType()),
            T.StructField("event_timestamp", T.TimestampType()),
            T.StructField("event_date", T.DateType()),
        ]
    )


def build_delay_stream(vehicle_stream, trip_delay_stream):
    from pyspark.sql import functions as F

    vehicles = vehicle_stream.select(
        "trip_id",
        "stop_id",
        "station_complex_id",
        F.col("event_timestamp").alias("vehicle_timestamp"),
    ).filter(F.col("station_complex_id").isNotNull())

    delays = trip_delay_stream.select(
        "trip_id",
        "stop_id",
        F.col("arrival_delay_secs").cast("double").alias("arrival_delay_secs"),
        F.col("event_timestamp").alias("trip_timestamp"),
    ).filter(F.col("arrival_delay_secs").isNotNull())

    joined = vehicles.withWatermark("vehicle_timestamp", "5 minutes").join(
        delays.withWatermark("trip_timestamp", "5 minutes"),
        on=[
            vehicles.trip_id == delays.trip_id,
            vehicles.stop_id == delays.stop_id,
            delays.trip_timestamp.between(
                vehicles.vehicle_timestamp - F.expr("INTERVAL 5 MINUTES"),
                vehicles.vehicle_timestamp + F.expr("INTERVAL 5 MINUTES"),
            ),
        ],
        how="inner",
    )

    return (
        joined.groupBy(F.window(F.col("vehicle_timestamp"), "10 minutes", "30 seconds"), F.col("station_complex_id"))
        .agg(F.avg("arrival_delay_secs").alias("avg_arrival_delay_secs"))
        .select(
            F.col("station_complex_id"),
            F.col("window.end").alias("event_timestamp"),
            F.col("avg_arrival_delay_secs"),
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join vehicle positions to trip delays and aggregate rolling station delays.")
    parser.add_argument("--vehicle-input", default=str(VEHICLE_POSITIONS_DIR))
    parser.add_argument("--trip-delay-input", default=str(TRIP_DELAYS_DIR))
    parser.add_argument("--output", default=str(SPEED_LAYER_DELAYS_DIR))
    parser.add_argument("--checkpoint", default=str(CHECKPOINT_DIR / "speed_layer_delays"))
    parser.add_argument("--trigger", default="30 seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-speed-layer-join")
    vehicles = spark.readStream.schema(vehicle_schema()).parquet(args.vehicle_input)
    delays = spark.readStream.schema(trip_delay_schema()).parquet(args.trip_delay_input)
    enriched = build_delay_stream(vehicles, delays)
    Path(args.output).mkdir(parents=True, exist_ok=True)
    query = (
        enriched.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start(args.output)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
