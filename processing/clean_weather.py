from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import RAW_WEATHER, WEATHER_CLEAN_DIR
from processing.spark_utils import get_spark
from processing.transforms import find_column


def clean_weather_dataframe(df, *, prcp_scale: float = 1.0, snow_scale: float = 1.0):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    columns = df.columns
    date_col = find_column(columns, ["DATE", "date"])

    if find_column(columns, ["DATATYPE", "datatype"], required=False) and find_column(columns, ["VALUE", "value"], required=False):
        datatype_col = find_column(columns, ["DATATYPE", "datatype"])
        value_col = find_column(columns, ["VALUE", "value"])
        prepared = (
            df.select(
                F.to_date(F.col(date_col)).alias("date"),
                F.upper(F.col(datatype_col)).alias("datatype"),
                F.col(value_col).cast("double").alias("value"),
            )
            .groupBy("date")
            .pivot("datatype", ["PRCP", "SNOW", "TMAX", "TMIN"])
            .agg(F.max("value"))
        )
    else:
        prcp_col = find_column(columns, ["PRCP", "prcp"], required=False)
        snow_col = find_column(columns, ["SNOW", "snow"], required=False)
        tmax_col = find_column(columns, ["TMAX", "tmax"], required=False)
        tmin_col = find_column(columns, ["TMIN", "tmin"], required=False)
        prepared = df.select(
            F.to_date(F.col(date_col)).alias("date"),
            F.col(prcp_col).cast("double").alias("PRCP") if prcp_col else F.lit(None).cast("double").alias("PRCP"),
            F.col(snow_col).cast("double").alias("SNOW") if snow_col else F.lit(None).cast("double").alias("SNOW"),
            F.col(tmax_col).cast("double").alias("TMAX") if tmax_col else F.lit(None).cast("double").alias("TMAX"),
            F.col(tmin_col).cast("double").alias("TMIN") if tmin_col else F.lit(None).cast("double").alias("TMIN"),
        )

    daily = prepared.groupBy("date").agg(
        F.max("PRCP").alias("prcp_raw"),
        F.max("SNOW").alias("snow_raw"),
        F.avg("TMAX").alias("tmax_raw"),
        F.avg("TMIN").alias("tmin_raw"),
    )
    window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    return (
        daily.filter(F.col("date").isNotNull())
        .withColumn("prcp_in", F.coalesce(F.col("prcp_raw") * F.lit(float(prcp_scale)), F.lit(0.0)))
        .withColumn("snow_in", F.coalesce(F.col("snow_raw") * F.lit(float(snow_scale)), F.lit(0.0)))
        .withColumn("tmax_f", F.last("tmax_raw", ignorenulls=True).over(window))
        .withColumn("tmin_f", F.last("tmin_raw", ignorenulls=True).over(window))
        .withColumn(
            "weather_bucket",
            F.when(F.col("snow_in") > 0.1, F.lit("snow"))
            .when(F.col("prcp_in") > 0.1, F.lit("rain"))
            .otherwise(F.lit("clear")),
        )
        .select("date", "prcp_in", "snow_in", "tmax_f", "tmin_f", "weather_bucket")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean NOAA daily weather CSV into Parquet.")
    parser.add_argument("--input", default=str(RAW_WEATHER))
    parser.add_argument("--output", default=str(WEATHER_CLEAN_DIR))
    parser.add_argument("--prcp-scale", type=float, default=1.0)
    parser.add_argument("--snow-scale", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-clean-weather")
    raw_df = spark.read.csv(args.input, header=True, inferSchema=False)
    clean_df = clean_weather_dataframe(raw_df, prcp_scale=args.prcp_scale, snow_scale=args.snow_scale)
    Path(args.output).mkdir(parents=True, exist_ok=True)
    clean_df.write.mode("overwrite").parquet(args.output)
    print(f"weather rows={clean_df.count():,} output={args.output}")
    clean_df.groupBy("weather_bucket").count().show()


if __name__ == "__main__":
    main()

