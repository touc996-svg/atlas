# ATLAS — Spark Pipeline Orchestration & Data Quality

This upgrade introduces a persisted processing control plane around the current ATLAS stages.

## What it adds

- A repeatable orchestration entry point for Spark transformation, S3 publication, dbt materialization and dbt tests.
- Bounded per-stage retries with exponential backoff and per-attempt execution records.
- Durable pipeline run, stage-attempt and data-quality history in PostgreSQL.
- Five Spark quality gates: non-empty source, row-count preservation, unique event IDs, required fields/timestamps, and parseable JSON-object payloads.
- A staged Parquet write. The active dataset is replaced only after the new Spark output has been fully written; the previous dataset is restored if the directory swap fails.
- FastAPI routes for recent runs and run details, including stage attempts and quality results.
- A Streamlit Pipeline Operations page for run history, execution stages and quality checks.

## Install

From the ATLAS repository root:

```bash
unzip -o ~/Downloads/atlas_pipeline_orchestration_upgrade.zip -d /tmp
bash /tmp/atlas_pipeline_orchestration_upgrade/install_upgrade.sh "$HOME/Projects/atlas"
```

The installer backs up the API, dashboard and Spark job in `.atlas-upgrade-backups/`. It does not modify `.env`, Docker configuration, database volumes, raw events or existing published Parquet data.

## Run the full pipeline

Load the same `.env` values used by the local services, then execute:

```bash
cd ~/Projects/atlas
set -a && source .env && set +a
PYTHONPATH=src python -m atlas.processing.pipeline_runner
```

Stages run in this order:

1. Spark transformation and quality gate
2. S3-compatible object-storage publication
3. dbt model build
4. dbt data-quality tests

A failed stage is retried twice by default. Each attempt is persisted. The entire run is marked failed if a stage exhausts retries or the Spark quality gate fails.

For a local Spark-only build while dbt/S3 credentials are being configured:

```bash
PYTHONPATH=src python -m atlas.processing.pipeline_runner --skip-s3 --skip-dbt
```

Useful options:

- `--max-attempts 3`
- `--retry-delay 2`
- `--timeout 1800`
- `--output-path data/processed/events`
- `--skip-s3`
- `--skip-dbt`

## API

- `GET /api/v1/pipeline/runs?limit=50`
- `GET /api/v1/pipeline/runs/{run_id}`

The run detail response contains `run`, `steps` and `quality_checks`.

## Dashboard

Rebuild the API and dashboard after installation:

```bash
docker compose up -d --build api dashboard
```

Open `http://localhost:18501` and select **Pipeline Operations**.

## Control-plane tables

- `public.atlas_pipeline_runs`
- `public.atlas_pipeline_steps`
- `public.atlas_data_quality_results`

Tables are initialized by the runner on its first execution. The raw `public.atlas_events` table and ingestion offsets are not changed by this upgrade.

## Delivery and recovery semantics

The Spark output is a deterministic snapshot of the currently ingested raw-event table. A completed staging output is promoted into the active Parquet directory. S3 publication and dbt are safe to retry as snapshot/object replacement operations for the current project layout; this is not a distributed transaction across PostgreSQL, S3 and dbt. If a downstream stage fails, the run is recorded as failed and can be rerun.

The current Spark source read still materializes PostgreSQL rows on the driver before creating a Spark DataFrame. This is appropriate for the current local demonstration but is a known scaling limit; the next optimization should use Spark JDBC with a pinned PostgreSQL JDBC driver and partitioned reads.
