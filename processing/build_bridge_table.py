from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import BRIDGE_PARQUET, RAW_STATIONS, RAW_STOPS, REPO_ROOT, ensure_parent
from processing.spark_utils import get_spark
from processing.transforms import find_column, haversine_meters, normalize_station_name, normalize_stop_id


def _udfs():
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    return {
        "normalize_stop_id": F.udf(normalize_stop_id, T.StringType()),
        "normalize_station_name": F.udf(normalize_station_name, T.StringType()),
        "haversine_meters": F.udf(
            lambda lat1, lon1, lat2, lon2: None
            if None in (lat1, lon1, lat2, lon2)
            else float(haversine_meters(lat1, lon1, lat2, lon2)),
            T.DoubleType(),
        ),
    }


def build_bridge(stations_df, stops_df, *, levenshtein_threshold: int = 4, radius_m: float = 100.0):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    udfs = _udfs()
    station_columns = stations_df.columns
    stop_columns = stops_df.columns

    station_gtfs_col = find_column(station_columns, ["GTFS Stop ID", "gtfs_stop_id"])
    station_complex_col = find_column(station_columns, ["Station Complex ID", "Complex ID", "Complex MRN"])
    complex_name_col = find_column(station_columns, ["Complex Name", "Stop Name", "Station Name"])
    lat_col = find_column(station_columns, ["GTFS Latitude", "Latitude", "lat"])
    lon_col = find_column(station_columns, ["GTFS Longitude", "Longitude", "lon", "lng"])
    borough_col = find_column(station_columns, ["Borough"], required=False)
    daytime_routes_col = find_column(station_columns, ["Daytime Routes", "daytime_routes"], required=False)

    stop_id_col = find_column(stop_columns, ["stop_id"])
    stop_name_col = find_column(stop_columns, ["stop_name"])
    stop_lat_col = find_column(stop_columns, ["stop_lat", "GTFS Latitude", "Latitude"])
    stop_lon_col = find_column(stop_columns, ["stop_lon", "GTFS Longitude", "Longitude"])

    bridge_seed = (
        stations_df.select(
            F.col(station_gtfs_col).cast("string").alias("gtfs_stop_id_raw"),
            F.col(station_complex_col).cast("string").alias("station_complex_id"),
            F.col(complex_name_col).cast("string").alias("complex_name"),
            F.col(lat_col).cast("double").alias("lat"),
            F.col(lon_col).cast("double").alias("lon"),
            F.col(borough_col).cast("string").alias("borough") if borough_col else F.lit(None).cast("string").alias("borough"),
            F.col(daytime_routes_col).cast("string").alias("daytime_routes")
            if daytime_routes_col
            else F.lit(None).cast("string").alias("daytime_routes"),
        )
        .withColumn("gtfs_stop_id", udfs["normalize_stop_id"](F.col("gtfs_stop_id_raw")))
        .filter(F.col("gtfs_stop_id") != "")
        .dropDuplicates(["gtfs_stop_id", "station_complex_id"])
    )

    stops_base = (
        stops_df.select(
            F.col(stop_id_col).cast("string").alias("source_stop_id"),
            F.col(stop_name_col).cast("string").alias("stop_name"),
            F.col(stop_lat_col).cast("double").alias("stop_lat"),
            F.col(stop_lon_col).cast("double").alias("stop_lon"),
        )
        .withColumn("gtfs_stop_id", udfs["normalize_stop_id"](F.col("source_stop_id")))
        .filter(F.col("gtfs_stop_id") != "")
        .dropDuplicates(["gtfs_stop_id"])
    )

    joined = stops_base.join(bridge_seed, on="gtfs_stop_id", how="left")
    output_cols = [
        "gtfs_stop_id",
        "source_stop_id",
        "station_complex_id",
        "complex_name",
        "lat",
        "lon",
        "borough",
        "daytime_routes",
        "match_tier",
        "match_distance",
    ]

    tier1 = (
        joined.filter(F.col("station_complex_id").isNotNull())
        .withColumn("match_tier", F.lit("TIER1_DIRECT"))
        .withColumn("match_distance", F.lit(None).cast("double"))
        .select(*output_cols)
    )

    unmatched = joined.filter(F.col("station_complex_id").isNull()).select(
        "gtfs_stop_id", "source_stop_id", "stop_name", "stop_lat", "stop_lon"
    )

    bridge_norm = bridge_seed.drop("gtfs_stop_id").withColumn("norm_complex_name", udfs["normalize_station_name"](F.col("complex_name")))
    unmatched_norm = unmatched.withColumn("norm_stop_name", udfs["normalize_station_name"](F.col("stop_name")))
    tier2_candidates = (
        unmatched_norm.crossJoin(bridge_norm)
        .withColumn("name_dist", F.levenshtein(F.col("norm_stop_name"), F.col("norm_complex_name")))
        .filter(F.col("name_dist") <= levenshtein_threshold)
        .withColumn("rn", F.row_number().over(Window.partitionBy("gtfs_stop_id").orderBy("name_dist", "station_complex_id")))
        .filter(F.col("rn") == 1)
    )
    tier2 = (
        tier2_candidates.withColumn("match_tier", F.lit("TIER2_NAME"))
        .withColumn("match_distance", F.col("name_dist").cast("double"))
        .select(*output_cols)
    )

    tier2_ids = tier2.select("gtfs_stop_id").distinct()
    tier3_input = unmatched.join(tier2_ids, on="gtfs_stop_id", how="left_anti")
    tier3_candidates = (
        tier3_input.crossJoin(
            bridge_seed.select(
                "gtfs_stop_id",
                "station_complex_id",
                "complex_name",
                "lat",
                "lon",
                "borough",
                "daytime_routes",
            ).withColumnRenamed("gtfs_stop_id", "bridge_gtfs_stop_id")
        )
        .withColumn("dist_m", udfs["haversine_meters"](F.col("stop_lat"), F.col("stop_lon"), F.col("lat"), F.col("lon")))
        .filter(F.col("dist_m") < F.lit(float(radius_m)))
        .withColumn("rn", F.row_number().over(Window.partitionBy("gtfs_stop_id").orderBy("dist_m", "station_complex_id")))
        .filter(F.col("rn") == 1)
    )
    tier3 = (
        tier3_candidates.withColumn("match_tier", F.lit("TIER3_GEO"))
        .withColumn("match_distance", F.col("dist_m").cast("double"))
        .select(*output_cols)
    )

    matched_ids = tier1.select("gtfs_stop_id").unionByName(tier2_ids).unionByName(tier3.select("gtfs_stop_id")).distinct()
    unresolved = (
        unmatched.join(matched_ids, on="gtfs_stop_id", how="left_anti")
        .select(
            "gtfs_stop_id",
            "source_stop_id",
            F.lit(None).cast("string").alias("station_complex_id"),
            F.col("stop_name").alias("complex_name"),
            F.col("stop_lat").alias("lat"),
            F.col("stop_lon").alias("lon"),
            F.lit(None).cast("string").alias("borough"),
            F.lit(None).cast("string").alias("daytime_routes"),
            F.lit("UNRESOLVED").alias("match_tier"),
            F.lit(None).cast("double").alias("match_distance"),
        )
    )

    return tier1.unionByName(tier2).unionByName(tier3).unionByName(unresolved)


def write_summary(bridge_df, output_path: str) -> None:
    from pyspark.sql import functions as F

    counts = {row["match_tier"]: row["count"] for row in bridge_df.groupBy("match_tier").count().collect()}
    total = sum(counts.values())
    unresolved = counts.get("UNRESOLVED", 0)
    null_rate = unresolved / total if total else 0.0
    lines = [
        "# Phase 1 Validation",
        "",
        "## Bridge Table",
        "",
        f"- Total normalized GTFS stops: `{total}`",
        f"- Tier 1 direct matches: `{counts.get('TIER1_DIRECT', 0)}`",
        f"- Tier 2 name matches: `{counts.get('TIER2_NAME', 0)}`",
        f"- Tier 3 geospatial matches: `{counts.get('TIER3_GEO', 0)}`",
        f"- Unresolved stops: `{unresolved}`",
        f"- Unresolved rate: `{null_rate:.2%}`",
        "",
        "Assumption A-01 is confirmed only if this unresolved rate is acceptable after manual review.",
    ]
    path = ensure_parent(output_path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GTFS stop ID to station_complex_id bridge table.")
    parser.add_argument("--stations", default=str(RAW_STATIONS))
    parser.add_argument("--stops", default=str(RAW_STOPS))
    parser.add_argument("--output", default=str(BRIDGE_PARQUET))
    parser.add_argument("--summary-output", default=str(REPO_ROOT / "docs" / "phase1_validation.md"))
    parser.add_argument("--levenshtein-threshold", type=int, default=4)
    parser.add_argument("--radius-m", type=float, default=100.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-build-bridge")
    stations_df = spark.read.csv(args.stations, header=True, inferSchema=False)
    stops_df = spark.read.csv(args.stops, header=True, inferSchema=False)
    bridge_df = build_bridge(
        stations_df,
        stops_df,
        levenshtein_threshold=args.levenshtein_threshold,
        radius_m=args.radius_m,
    )
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    bridge_df.write.mode("overwrite").parquet(args.output)
    write_summary(bridge_df, args.summary_output)
    bridge_df.groupBy("match_tier").count().show(truncate=False)


if __name__ == "__main__":
    main()
