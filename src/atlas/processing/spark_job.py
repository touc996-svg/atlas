"""ATLAS Spark transformation using Spark-native PostgreSQL JDBC reads.

The source is read by Spark rather than fetched into the Python driver process.
Parquet publication remains staged and atomic, and every run emits a manifest.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import quote

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pyspark import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    from_json,
    hour,
    sum as spark_sum,
    to_date,
    when,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from atlas.processing.incremental_state import (
    ProcessingCheckpointStore,
    build_source_query,
    calculate_window_start,
    has_parquet_files,
    no_new_data_check,
    should_bootstrap,
)

DEFAULT_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.14"
SOURCE_QUERY = """(
    SELECT
        event_id,
        event_type,
        source,
        event_timestamp,
        ingested_at,
        payload::text AS payload_json
    FROM public.atlas_events
) AS atlas_events_source"""


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    password = os.getenv("POSTGRES_PASSWORD")
    user = os.getenv("POSTGRES_USER")
    if not password or not user:
        raise RuntimeError(
            "Set DATABASE_URL or configure POSTGRES_USER and POSTGRES_PASSWORD."
        )
    return make_conninfo(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "atlas"),
        user=user,
        password=password,
    )


def build_jdbc_options(database_url: str) -> dict[str, str]:
    """Convert a libpq connection string into Spark JDBC options safely.

    Credentials are passed as JDBC properties, never interpolated into the URL
    or SQL statement. The returned mapping must not be logged.
    """
    params = conninfo_to_dict(database_url)
    host = (params.get("host") or "localhost").split(",", 1)[0].strip()
    port = (params.get("port") or "5432").split(",", 1)[0].strip()
    database = params.get("dbname")
    user = params.get("user")
    password = params.get("password")

    missing = [
        name for name, value in (
            ("host", host),
            ("database", database),
            ("user", user),
            ("password", password),
        ) if not value
    ]
    if missing:
        raise RuntimeError(
            "Spark JDBC configuration is missing: " + ", ".join(missing)
        )

    jdbc_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    jdbc_url = f"jdbc:postgresql://{jdbc_host}:{port}/{quote(database, safe='')}"
    options = {
        "url": jdbc_url,
        "driver": "org.postgresql.Driver",
        "user": user,
        "password": password,
        "fetchsize": os.getenv("ATLAS_SPARK_JDBC_FETCH_SIZE", "10000"),
    }
    sslmode = params.get("sslmode")
    if sslmode:
        options["sslmode"] = sslmode
    return options


def get_source_bounds(
    database_url: str, ingested_after: datetime | None = None
) -> tuple[int, datetime | None, datetime | None]:
    """Read row count and ingestion-time bounds for a full or incremental JDBC read."""
    sql = """
        SELECT COUNT(*), MIN(ingested_at), MAX(ingested_at)
        FROM public.atlas_events
    """
    params: tuple = ()
    if ingested_after is not None:
        sql += " WHERE ingested_at >= %s"
        params = (ingested_after,)
    with psycopg.connect(database_url, connect_timeout=10) as connection:
        row = connection.execute(sql, params).fetchone()
    return int(row[0]), row[1], row[2]


def configure_jdbc_reader(
    spark: SparkSession,
    database_url: str,
    *,
    source_count: int,
    lower_bound: datetime | None,
    upper_bound: datetime | None,
    source_query: str = SOURCE_QUERY,
    partition_column: str = "ingested_at",
) -> DataFrame:
    """Create a Spark JDBC DataFrame with optional ingestion-time partitioning."""
    options = build_jdbc_options(database_url)
    reader = (
        spark.read.format("jdbc")
        .options(**options)
        .option("dbtable", source_query)
    )

    requested_partitions = int(os.getenv("ATLAS_SPARK_JDBC_PARTITIONS", "1"))
    if requested_partitions < 1:
        raise ValueError("ATLAS_SPARK_JDBC_PARTITIONS must be at least 1")

    if (
        requested_partitions > 1
        and source_count > 1
        and lower_bound is not None
        and upper_bound is not None
        and lower_bound < upper_bound
    ):
        num_partitions = min(requested_partitions, source_count)
        reader = (
            reader
            .option("partitionColumn", partition_column)
            .option("lowerBound", lower_bound.isoformat())
            .option("upperBound", upper_bound.isoformat())
            .option("numPartitions", str(num_partitions))
        )

    return reader.load()


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    temporary.replace(path)


def evaluate_quality(
    *, input_rows: int, output_rows: int, unique_ids: int,
    null_required_fields: int, invalid_payload_rows: int,
) -> list[dict[str, Any]]:
    return [
        {
            "check_name": "source_not_empty",
            "status": "PASS" if input_rows > 0 else "FAIL",
            "observed_value": input_rows,
            "expected_value": "> 0",
            "details": "Source PostgreSQL table must contain at least one event.",
        },
        {
            "check_name": "row_count_preserved",
            "status": "PASS" if input_rows == output_rows else "FAIL",
            "observed_value": f"input={input_rows}, output={output_rows}",
            "expected_value": "input count equals output count",
            "details": "Spark transformation must preserve the source record count.",
        },
        {
            "check_name": "unique_event_ids",
            "status": "PASS" if input_rows == unique_ids else "FAIL",
            "observed_value": unique_ids,
            "expected_value": input_rows,
            "details": "Every source event_id must be unique.",
        },
        {
            "check_name": "required_fields_not_null",
            "status": "PASS" if null_required_fields == 0 else "FAIL",
            "observed_value": null_required_fields,
            "expected_value": 0,
            "details": "Required identifiers, source fields and parsed timestamps must not be null.",
        },
        {
            "check_name": "payload_json_valid",
            "status": "PASS" if invalid_payload_rows == 0 else "FAIL",
            "observed_value": invalid_payload_rows,
            "expected_value": 0,
            "details": "Every payload must parse as a JSON object in Spark.",
        },
    ]


def publish_dataset(staging: Path, target: Path, run_id: str) -> None:
    """Swap a completed staging dataset into place; restore prior data on error."""
    target.parent.mkdir(parents=True, exist_ok=True)
    backup = target.with_name(f".{target.name}.previous-{run_id}")
    if backup.exists():
        shutil.rmtree(backup)
    moved_old = False
    try:
        if target.exists():
            target.replace(backup)
            moved_old = True
        staging.replace(target)
    except Exception:
        if moved_old and backup.exists() and not target.exists():
            backup.replace(target)
        raise
    else:
        if backup.exists():
            shutil.rmtree(backup)


def run_job(
    run_id: str,
    output_path: str = "data/processed/events",
    manifest_path: str | None = None,
    *,
    mode: str = "full",
    lookback_hours: float = 48.0,
) -> dict[str, Any]:
    """Run a full snapshot or an overlap-window incremental transformation.

    Incremental mode uses PostgreSQL ingested_at as its durable watermark. The
    overlap window catches late arrivals; event_id anti-join filters previously
    published rows. New Parquet files are appended to a staged copy of the prior
    dataset, then the whole directory is swapped atomically. The checkpoint only
    advances after publication succeeds.
    """
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be 'full' or 'incremental'")
    if lookback_hours < 0:
        raise ValueError("lookback_hours cannot be negative")

    started = monotonic()
    started_at = datetime.now(timezone.utc)
    target = Path(output_path).resolve()
    manifest_file = Path(
        manifest_path or f"data/processed/manifests/{run_id}.json"
    ).resolve()
    staging = target.with_name(f".{target.name}.staging-{run_id}")
    if staging.exists():
        shutil.rmtree(staging)

    spark: SparkSession | None = None
    source_df: DataFrame | None = None
    new_source_df: DataFrame | None = None
    processed_df: DataFrame | None = None
    checkpoint_store: ProcessingCheckpointStore | None = None
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "status": "FAILED",
        "started_at": started_at.isoformat(),
        "output_path": str(target),
        "source_reader": "spark_jdbc",
        "processing_mode": mode,
        "watermark_column": "ingested_at",
        "lookback_hours": lookback_hours if mode == "incremental" else None,
        "input_rows": 0,
        "output_rows": 0,
        "quality_passed": False,
        "quality_checks": [],
    }
    try:
        database_url = get_database_url()
        checkpoint_store = ProcessingCheckpointStore(database_url)
        checkpoint_store.initialize()
        checkpoint_store.acquire_lock()
        checkpoint_store.ensure_source_index()

        watermark_before = checkpoint_store.get_watermark()
        target_has_data = has_parquet_files(target)
        bootstrap = mode == "incremental" and should_bootstrap(
            watermark_before, target_has_data
        )
        incremental_active = mode == "incremental" and not bootstrap
        window_start = (
            calculate_window_start(watermark_before, lookback_hours)
            if incremental_active and watermark_before is not None
            else None
        )
        source_count, lower_bound, upper_bound = get_source_bounds(
            database_url, ingested_after=window_start
        )
        manifest.update({
            "checkpoint_before": watermark_before.isoformat() if watermark_before else None,
            "incremental_window_start": window_start.isoformat() if window_start else None,
            "checkpoint_bootstrap": bootstrap,
            "source_rows_scanned": source_count,
        })

        if source_count == 0:
            if mode == "full" or bootstrap:
                manifest.update({
                    "quality_passed": False,
                    "quality_checks": [{
                        "check_name": "source_not_empty",
                        "status": "FAIL",
                        "observed_value": 0,
                        "expected_value": "> 0",
                        "details": "A full baseline snapshot requires at least one source event.",
                    }],
                    "error": "No events found in PostgreSQL; baseline snapshot was not published.",
                })
                raise RuntimeError(manifest["error"])

            manifest.update({
                "status": "SUCCESS",
                "no_op": True,
                "input_rows": 0,
                "output_rows": 0,
                "quality_passed": True,
                "quality_checks": no_new_data_check(0),
                "checkpoint_after": watermark_before.isoformat() if watermark_before else None,
                "duration_seconds": round(monotonic() - started, 3),
            })
            write_manifest(manifest_file, manifest)
            print(json.dumps({
                "run_id": run_id,
                "status": "SUCCESS",
                "processing_mode": mode,
                "no_op": True,
                "input_rows": 0,
                "output_rows": 0,
                "manifest_path": str(manifest_file),
            }, indent=2))
            return manifest

        jdbc_package = os.getenv(
            "ATLAS_POSTGRES_JDBC_PACKAGE", DEFAULT_JDBC_PACKAGE
        )
        spark = (
            SparkSession.builder
            .appName(f"ATLAS-Event-Processing-{run_id}")
            .master(os.getenv("SPARK_MASTER", "local[2]"))
            .config("spark.jars.packages", jdbc_package)
            .config(
                "spark.sql.shuffle.partitions",
                os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"),
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

        source_df = configure_jdbc_reader(
            spark,
            database_url,
            source_count=source_count,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            source_query=build_source_query(window_start),
            partition_column="ingested_at",
        ).persist(StorageLevel.MEMORY_AND_DISK)
        source_rows_scanned = source_df.count()

        if incremental_active and target_has_data:
            existing_ids = (
                spark.read.parquet(str(target))
                .select("event_id")
                .dropDuplicates(["event_id"])
            )
            new_source_df = (
                source_df.join(existing_ids, on="event_id", how="left_anti")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
        else:
            new_source_df = source_df

        input_rows = new_source_df.count()
        manifest["source_rows_scanned"] = source_rows_scanned
        manifest["input_rows"] = input_rows
        manifest["new_event_rows"] = input_rows

        if input_rows == 0 and incremental_active:
            if upper_bound is not None:
                checkpoint_store.advance_watermark(
                    watermark=upper_bound,
                    run_id=run_id,
                    rows_processed=0,
                )
            checkpoint_after = checkpoint_store.get_watermark()
            manifest.update({
                "status": "SUCCESS",
                "no_op": True,
                "output_rows": 0,
                "quality_passed": True,
                "quality_checks": no_new_data_check(source_rows_scanned),
                "checkpoint_after": checkpoint_after.isoformat() if checkpoint_after else None,
                "duration_seconds": round(monotonic() - started, 3),
            })
            write_manifest(manifest_file, manifest)
            print(json.dumps({
                "run_id": run_id,
                "status": "SUCCESS",
                "processing_mode": mode,
                "no_op": True,
                "source_rows_scanned": source_rows_scanned,
                "input_rows": 0,
                "output_rows": 0,
                "manifest_path": str(manifest_file),
            }, indent=2))
            return manifest

        payload_schema = StructType([
            StructField("sequence", IntegerType(), True),
            StructField("message", StringType(), True),
        ])
        processed_df = (
            new_source_df
            .withColumn("payload", from_json(col("payload_json"), payload_schema))
            .withColumn("sequence", col("payload.sequence"))
            .withColumn("message", col("payload.message"))
            .withColumn("event_date", to_date("event_timestamp"))
            .withColumn("event_hour", hour("event_timestamp"))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        required_fields = [
            "event_id", "event_type", "source", "event_timestamp", "event_date"
        ]
        null_exprs = [
            spark_sum(when(col(name).isNull(), 1).otherwise(0)).alias(f"null_{name}")
            for name in required_fields
        ]
        metrics = processed_df.agg(
            count("*").alias("output_rows"),
            countDistinct("event_id").alias("unique_ids"),
            spark_sum(
                when(col("payload").isNull(), 1).otherwise(0)
            ).alias("invalid_payload_rows"),
            *null_exprs,
        ).first().asDict()

        output_rows = int(metrics.get("output_rows") or 0)
        unique_ids = int(metrics.get("unique_ids") or 0)
        invalid_payload_rows = int(metrics.get("invalid_payload_rows") or 0)
        null_required_fields = sum(
            int(metrics.get(f"null_{name}") or 0) for name in required_fields
        )
        checks = evaluate_quality(
            input_rows=input_rows,
            output_rows=output_rows,
            unique_ids=unique_ids,
            null_required_fields=null_required_fields,
            invalid_payload_rows=invalid_payload_rows,
        )
        quality_passed = all(item["status"] == "PASS" for item in checks)
        manifest.update({
            "input_rows": input_rows,
            "output_rows": output_rows,
            "unique_event_ids": unique_ids,
            "quality_passed": quality_passed,
            "quality_checks": checks,
            "jdbc_partitions_requested": int(
                os.getenv("ATLAS_SPARK_JDBC_PARTITIONS", "1")
            ),
            "jdbc_driver_package": jdbc_package,
        })
        if not quality_passed:
            raise RuntimeError(
                "Spark data-quality gate failed; previous Parquet dataset was preserved."
            )

        output_df = processed_df.drop("payload_json", "payload", "ingested_at")
        if incremental_active and target_has_data:
            # Copy old partition files into a unique staging location, then append
            # only newly accepted event IDs. Existing dataset remains untouched
            # until the full staged directory is atomically swapped into place.
            shutil.copytree(target, staging)
            output_df.write.mode("append").partitionBy("event_date").parquet(str(staging))
        else:
            output_df.write.mode("errorifexists").partitionBy("event_date").parquet(str(staging))
        publish_dataset(staging, target, run_id)

        if upper_bound is not None:
            checkpoint_store.advance_watermark(
                watermark=upper_bound,
                run_id=run_id,
                rows_processed=input_rows,
            )
        checkpoint_after = checkpoint_store.get_watermark()
        manifest.update({
            "status": "SUCCESS",
            "no_op": False,
            "checkpoint_after": checkpoint_after.isoformat() if checkpoint_after else None,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(monotonic() - started, 3),
        })
        write_manifest(manifest_file, manifest)
        print(json.dumps({
            "run_id": run_id,
            "status": "SUCCESS",
            "source_reader": "spark_jdbc",
            "processing_mode": mode,
            "checkpoint_bootstrap": bootstrap,
            "source_rows_scanned": source_rows_scanned,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "quality_passed": quality_passed,
            "output_path": str(target),
            "manifest_path": str(manifest_file),
        }, indent=2))
        return manifest
    except Exception as exc:
        manifest.update({
            "status": "FAILED",
            "error": f"{type(exc).__name__}: {exc}",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(monotonic() - started, 3),
        })
        manifest.setdefault("quality_checks", [])
        write_manifest(manifest_file, manifest)
        raise
    finally:
        if processed_df is not None:
            processed_df.unpersist(blocking=False)
        if new_source_df is not None and new_source_df is not source_df:
            new_source_df.unpersist(blocking=False)
        if source_df is not None:
            source_df.unpersist(blocking=False)
        if spark is not None:
            spark.stop()
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        if checkpoint_store is not None:
            checkpoint_store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the ATLAS Spark transformation job using PostgreSQL JDBC."
    )
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    parser.add_argument(
        "--output-path",
        default=os.getenv("ATLAS_PARQUET_OUTPUT_PATH", "data/processed/events"),
    )
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument(
        "--lookback-hours", type=float,
        default=float(os.getenv("ATLAS_INCREMENTAL_LOOKBACK_HOURS", "48")),
    )
    args = parser.parse_args(argv)
    run_job(
        args.run_id, args.output_path, args.manifest_path,
        mode=args.mode, lookback_hours=args.lookback_hours,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
