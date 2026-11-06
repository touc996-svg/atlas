# ATLAS Spark JDBC Source Optimization

## Why this change

The previous Spark job used `psycopg.fetchall()` to materialize every source event in the Python driver before constructing a Spark DataFrame. That creates a driver-memory bottleneck and prevents Spark from controlling source reads.

This upgrade reads PostgreSQL through Spark's JDBC data source. Rows are loaded into Spark partitions, and the job continues to apply the same five quality gates and staged Parquet publication contract.

## Install

```bash
cd ~/Projects/atlas
unzip -o ~/Downloads/atlas_spark_jdbc_upgrade.zip -d /tmp
bash /tmp/atlas_spark_jdbc_upgrade/install_upgrade.sh ~/Projects/atlas
```

The installer backs up only `src/atlas/processing/spark_job.py` before replacing it. It adds a focused test and this document. It does not modify `.env`, PostgreSQL tables, Docker configuration, volumes or existing Parquet data.

## Runtime dependency

The Spark job resolves the PostgreSQL JDBC driver through Maven coordinates:

`org.postgresql:postgresql:42.7.14`

The default is pinned in code. Change it with `ATLAS_POSTGRES_JDBC_PACKAGE` only when deliberately upgrading or matching a managed Spark runtime. The first Spark run needs Maven Central access unless the driver is already cached.

## Configuration

Optional environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `ATLAS_POSTGRES_JDBC_PACKAGE` | `org.postgresql:postgresql:42.7.14` | Pinned JDBC driver coordinate |
| `ATLAS_SPARK_JDBC_FETCH_SIZE` | `10000` | Rows fetched per JDBC round-trip |
| `ATLAS_SPARK_JDBC_PARTITIONS` | `1` | JDBC read partitions; values above 1 partition by `event_timestamp` when bounds allow |
| `SPARK_MASTER` | `local[2]` | Spark master |
| `SPARK_SHUFFLE_PARTITIONS` | `4` | Spark SQL shuffle partitions |

Keep JDBC partitions modest. Each partition opens a database connection; increasing this value should be based on database capacity and workload size, not set arbitrarily high.

## Run

```bash
cd ~/Projects/atlas
set -a && source .env && set +a
PYTHONPATH=src python -m atlas.processing.pipeline_runner
```

The orchestrator remains the entry point. It invokes the upgraded Spark job, then the existing S3 and dbt stages.

## Operational contract

- Spark reads PostgreSQL through JDBC; Python no longer fetches the entire event table into a driver-side list.
- JDBC credentials are supplied as driver properties, not embedded in the JDBC URL or SQL.
- The DataFrame is persisted with `MEMORY_AND_DISK` while quality aggregates and Parquet output are computed.
- The five existing quality checks remain: non-empty source, row-count preservation, unique event IDs, required fields not null, and valid JSON payloads.
- The output is written to a unique staging path and swapped into place only after the quality gate passes and the Parquet write completes.
- The pipeline still performs a full snapshot read. Incremental/watermarked processing is a separate future change and is not claimed by this upgrade.

## Tests

```bash
PYTHONPATH=src pytest -q tests/test_spark_jdbc.py
```
