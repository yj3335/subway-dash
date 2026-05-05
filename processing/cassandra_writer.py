from __future__ import annotations


def write_to_cassandra(df, table_name: str, mode: str = "append", keyspace: str = "subway_dash") -> None:
    writer = df.write.format("org.apache.spark.sql.cassandra").option("keyspace", keyspace).option("table", table_name)
    if mode == "overwrite":
        writer = writer.option("confirm.truncate", "true")
    writer.mode(mode).save()


def read_from_cassandra(spark, table_name: str, keyspace: str = "subway_dash"):
    return spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", keyspace).option("table", table_name).load()
