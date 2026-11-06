# ATLAS — Data Intelligence & Lakehouse Platform

An end-to-end local data engineering platform demonstrating event streaming, database ingestion, distributed processing, object storage, analytics transformations, data quality, API delivery, and operational monitoring.

## Architecture

Python Producer → Redpanda/Kafka → PostgreSQL → Spark JDBC / PySpark → Partitioned Parquet → SeaweedFS S3 Data Lake

PostgreSQL → dbt staging/fact/dimension models → FastAPI → Streamlit/Plotly Dashboard

## Technology

- Python 3.12
- Redpanda / Kafka
- PostgreSQL 16
- Apache Spark / PySpark
- Parquet / Snappy
- SeaweedFS S3-compatible storage
- dbt-postgres
- FastAPI
- Streamlit / Plotly
- Docker Compose

## Verified local demonstration — November 6, 2026

Latest successful automated macOS LaunchAgent execution:

| Metric | Verified result |
|---|---:|
| End-to-end pipeline | SUCCESS |
| Total execution time | 65.449 seconds |
| Spark transformation | SUCCESS |
| Spark execution time | 43.083 seconds |
| S3 Parquet files discovered | 2 |
| New S3 objects uploaded | 0 |
| Existing S3 objects skipped | 2 |
| dbt models | 3/3 successful |
| dbt data-quality tests | 9/9 passed |
| Incremental fact rows inserted | 45 |
| Pipeline exit code | 0 |

The S3 publisher skipped existing objects, demonstrating idempotent
publishing on the verified run. Spark reads PostgreSQL through JDBC,
and dbt builds staging, dimension, and incremental fact models.

The pipeline supports persisted run metadata, stage-level execution
tracking, retries, data-quality gates, incremental processing, and
failure status reporting.

## Automated execution

A per-user macOS LaunchAgent runs the incremental pipeline immediately
at load and every 30 minutes thereafter. Its environment explicitly
provides Java 17, the project virtual environment, and Python path.

Execution status and logs are stored locally under
`data/processed/scheduler/`. The machine-specific LaunchAgent plist
is not part of the repository.

## Local setup

1. Install Python 3.12, Docker Compose and Java 17.
2. Copy `.env.example` to `.env` and replace the placeholder passwords.
3. Start infrastructure:

   `docker compose up -d`

4. Create and activate a virtual environment, then install:

   `pip install -r requirements.txt`

5. Load environment variables:

   `set -a && source .env && set +a`

6. Start the PostgreSQL sink:

   `PYTHONPATH=src python -m atlas.ingestion.postgres_sink`

7. Publish events from another terminal:

   `PYTHONPATH=src python -m atlas.ingestion.producer --count 20 --interval 0.1`

8. Run Spark with Java 17:

   `PYTHONPATH=src python -m atlas.processing.spark_job`

9. Upload Parquet:

   `PYTHONPATH=src python -m atlas.storage.s3_lake`

10. Run dbt:

    `dbt run --project-dir atlas_analytics`

    `dbt test --project-dir atlas_analytics`

11. Start the API:

    `PYTHONPATH=src uvicorn atlas.api.main:app --host 127.0.0.1 --port 8000`

12. Start the dashboard:

    `PYTHONPATH=src streamlit run src/atlas/dashboard/app.py --server.port 8501`

## API endpoints

- `/health` — API, PostgreSQL, Redpanda and object-storage health
- `/api/v1/analytics/summary` — aggregate event metrics
- `/api/v1/analytics/telemetry` — recent event counts, partitions, latest offset and event age
- `/api/v1/analytics/event-types` — event-type aggregates
- `/api/v1/analytics/daily` — daily aggregates
- `/api/v1/events` — recent events

API documentation: `http://localhost:8000/docs`

Dashboard: `http://localhost:8501`

## Current limitations

- Demo events are synthetic.
- Dashboard refresh is user-triggered and briefly cached; continuous live streaming updates are not yet implemented.
- Throughput metrics are derived from PostgreSQL event timestamps, not native broker measurements.
- Spark execution history and host resource utilization are not yet monitored.
- Authentication, role-based access control, TLS, production secrets management and cloud deployment remain future work.

## Security

Keep `.env`, virtual environments, generated datasets and credentials out of Git. Use managed secrets and least-privilege credentials before external deployment.
