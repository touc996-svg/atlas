import json
import os

import psycopg
from psycopg.conninfo import make_conninfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, hour
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

def get_database_url():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing. Load .env.")

    return make_conninfo(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "atlas"),
        user=os.getenv("POSTGRES_USER", "atlas"),
        password=password,
    )


DATABASE_URL = get_database_url()

OUTPUT_PATH = "data/processed/events"

def main():
    with psycopg.connect(DATABASE_URL) as connection:
        records = connection.execute("""
            SELECT event_id, event_type, source,
                   event_timestamp::TEXT, payload::TEXT
            FROM atlas_events
            ORDER BY event_timestamp
        """).fetchall()

    if not records:
        raise RuntimeError("No events found in PostgreSQL.")

    spark = (
        SparkSession.builder
        .appName("ATLAS-Event-Processing")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("payload_json", StringType(), True),
        ])

        rows = [
            (r[0], r[1], r[2], r[3], r[4])
            for r in records
        ]

        raw_df = spark.createDataFrame(rows, schema)

        payload_schema = StructType([
            StructField("sequence", IntegerType(), True),
            StructField("message", StringType(), True),
        ])

        processed_df = (
            raw_df
            .withColumn(
                "event_timestamp",
                col("event_timestamp").cast("timestamp")
            )
            .withColumn(
                "payload",
                from_json(col("payload_json"), payload_schema)
            )
            .withColumn("sequence", col("payload.sequence"))
            .withColumn("message", col("payload.message"))
            .withColumn("event_date", to_date("event_timestamp"))
            .withColumn("event_hour", hour("event_timestamp"))
            .drop("payload_json", "payload")
        )

        processed_df.coalesce(1).write \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .parquet(OUTPUT_PATH)

        print("\n=== ATLAS SPARK PROCESSING RESULTS ===")
        print(f"Input records: {raw_df.count()}")
        print(f"Output records: {processed_df.count()}")
        processed_df.select(
            "event_id", "sequence", "event_date", "event_hour", "message"
        ).show(truncate=False)

        print(f"Parquet output: {OUTPUT_PATH}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
