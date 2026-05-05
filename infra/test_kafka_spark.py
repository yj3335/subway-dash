"""Task A1.3 — Verify Kafka + Spark connectivity.

Runs a 10-line PySpark Structured Streaming job that reads from `gtfs-vehicle`
and prints the count of received messages every 10 seconds. Run for ~2 minutes
against manually produced test messages and confirm non-zero counts.

Submit with:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        infra/test_kafka_spark.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "gtfs-vehicle"


def main() -> None:
    spark = (
        SparkSession.builder.appName("kafka-spark-connectivity-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    counts = (
        raw.withWatermark("timestamp", "30 seconds")
        .groupBy(window("timestamp", "10 seconds"))
        .agg(count("*").alias("msg_count"))
    )

    query = (
        counts.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
