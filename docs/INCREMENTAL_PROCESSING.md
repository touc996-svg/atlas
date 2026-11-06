# ATLAS Incremental Processing

## Design

ATLAS now supports full snapshots and incremental Spark transformations. Incremental mode uses PostgreSQL `atlas_events.ingested_at` as its durable watermark rather than business event time. A 48-hour overlap is applied by default to capture late-arriving records. Stable `event_id` values are used to eliminate already-published records from the overlap.

The checkpoint table `public.atlas_processing_checkpoints` is created automatically. ATLAS also creates `idx_atlas_events_ingested_at` concurrently so the bounded time-window source scan can use an index without the stronger write lock of a regular index build.

## Run commands

Full snapshot (backward-compatible default):

```bash
PYTHONPATH=src python -m atlas.processing.pipeline_runner --mode full
```

Incremental processing with a 48-hour late-arrival overlap:

```bash
PYTHONPATH=src python -m atlas.processing.pipeline_runner --mode incremental --lookback-hours 48
```

Develop only the Spark stage while skipping the downstream S3 and dbt stages:

```bash
PYTHONPATH=src python -m atlas.processing.pipeline_runner --mode incremental --skip-s3 --skip-dbt
```

## Processing contract

1. Acquire a PostgreSQL advisory lock for the ATLAS event pipeline.
2. Load the last committed `ingested_at` checkpoint.
3. Query the overlap window via Spark JDBC. The SQL filter uses `ingested_at >= watermark - lookback`.
4. Anti-join IDs already present in the current Parquet dataset.
5. Apply the five Spark data-quality gates to new records.
6. Copy the current Parquet dataset to a unique staging directory and append only new rows.
7. Atomically swap the staged dataset into the published path.
8. Advance the watermark only after the filesystem publish succeeds.

A missing checkpoint or missing Parquet dataset triggers a full baseline snapshot. A subsequent no-new-data run is a successful no-op. If a process fails after Parquet publication but before checkpoint advancement, the next run re-reads the overlap and anti-joins the IDs already published. This provides at-least-once source reads with idempotent dataset effects, not distributed exactly-once processing.

## Tradeoffs and limitations

- The 48-hour lookback can be increased for sources that deliver later, at the cost of a larger overlap scan.
- This captures late business event timestamps because the cursor uses database ingestion time. It cannot detect rows inserted with a backdated `ingested_at` older than the configured overlap; a CDC log or monotonically increasing source sequence would be needed for that guarantee.
- The local staging publish copies existing Parquet files before appending new ones. Historical rows are not re-read from PostgreSQL or re-transformed, but the file copy still consumes local I/O proportional to dataset size.
- S3 publication and dbt still run as the downstream orchestration stages and are not incremental in this release.
- `event_id` must remain stable and unique. PostgreSQL's primary key and the Parquet anti-join are the idempotency contract.
- Checkpoint advancement follows Parquet publication. The filesystem and PostgreSQL cannot share one atomic transaction; retries are made safe by the event-ID anti-join.

## Tests

```bash
PYTHONPATH=src pytest -q tests/test_incremental_processing.py tests/test_incremental_runner_cli.py
```
