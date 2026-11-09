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
- `/api/v1/ingestion/summary` — accepted/rejected totals, recent rejection counts and rejection rate
- `/api/v1/ingestion/rejections` — recent quarantine metadata (raw payloads are never returned)
- `/api/v1/ingestion/rejections/by-reason` — rejection counts grouped by validation reason
- `/api/v1/ingestion/replay-summary` — replay totals by status
- `/api/v1/ingestion/replays` — replay audit history and Kafka coordinates
- `/api/v1/analytics/event-types` — event-type aggregates
- `/api/v1/analytics/daily` — daily aggregates
- `/api/v1/events` — recent events

API documentation: `http://localhost:8000/docs`

Dashboard: `http://localhost:8501`

## Current limitations

- Demo events are synthetic.
- Dashboard analytics refresh automatically every 30 seconds while the browser session is active.
- Throughput metrics are derived from PostgreSQL event timestamps, not native broker measurements.
- Spark execution history and host resource utilization are not yet monitored.
- API key authentication protects private API endpoints. Role-based access control, TLS, managed production secrets and cloud deployment remain future work.

## Ingestion observability

The dashboard includes an Ingestion Health view backed by PostgreSQL quarantine telemetry. It reports accepted and rejected counts, rejection rates, recent failures and grouped rejection reasons. The API deliberately omits raw quarantined message bodies; it exposes only Kafka coordinates, the validation reason, timestamp and raw-value byte length.

## Dead-letter replay and historical recovery

Quarantined Kafka records can be replayed without changing or deleting the original DLQ row. The replay worker publishes the original message bytes, writes a PostgreSQL audit record for each attempt, captures destination Kafka partition/offset, and skips records already marked PUBLISHED. Replay is at-least-once across Kafka and PostgreSQL; downstream event persistence remains idempotent by `event_id`.

Preview eligible records before publishing:

```bash
PYTHONPATH=src python -m atlas.ingestion.replay --dry-run --limit 100
```

Replay a bounded batch after reviewing the preview:

```bash
PYTHONPATH=src python -m atlas.ingestion.replay --limit 100
```

Replay records rejected within a historical UTC window (the lower bound is inclusive; upper bound is exclusive):

```bash
PYTHONPATH=src python -m atlas.ingestion.replay --since 2026-11-01T00:00:00Z --until 2026-11-08T00:00:00Z --dry-run
```

The dashboard displays replay status and audit metadata. Raw quarantined message bodies are not returned by the API. The replay CLI requires the same PostgreSQL and Kafka environment configuration as the ingestion pipeline.

## API authentication

ATLAS accepts an `X-API-Key` header on all `/api/v1/*` endpoints.

- Local development without a key remains supported.
- When a key is configured, private API endpoints require the matching header.
- Production mode requires a key of at least 32 characters.
- The dashboard sends the configured key automatically.
- `/`, `/health`, and API documentation remain public for health checks and diagnostics.

Generate a key with `openssl rand -hex 32` and store it in the local `.env` file as `ATLAS_API_KEY`. Set `ATLAS_ENV=production` for production. Never commit `.env` or expose the key in source code or logs.

## Security

Keep `.env`, virtual environments, generated datasets and credentials out of Git. Use managed secrets and least-privilege credentials before external deployment.

---

## Engineering Portfolio

This repository is part of a four-project engineering portfolio spanning data engineering, machine learning operations, cloud infrastructure automation, and applied AI engineering.

| Project | Engineering Focus | Technology |
|---|---|---|
| [ATLAS](https://github.com/touc996-svg/atlas) | Event-driven data engineering and lakehouse analytics | Python, Kafka, PostgreSQL, Spark, Parquet, dbt, FastAPI |
| [APEX MLOps](https://github.com/touc996-svg/apex-mlops) | Reproducible ML lifecycle, model governance and monitoring | Python, scikit-learn, MLflow, FastAPI, Docker |
| [ORBIT](https://github.com/touc996-svg/orbit-cloud-automation) | AWS infrastructure automation and security validation | Terraform, AWS, Python, GitHub Actions |
| [NEXUS AI](https://github.com/touc996-svg/nexus-ai) | Multi-agent AI, RAG architecture and intelligent orchestration | FastAPI, Qwen3, Ollama, Qdrant, Nomic Embeddings, SQL |

### Portfolio Verification Highlights

- **ATLAS:** Verified local end-to-end pipeline execution, 3/3 dbt models and 9/9 dbt data-quality tests.
- **APEX MLOps 0.3.0:** Model registry, artifact integrity, controlled retraining, protected inference API and drift monitoring.
- **ORBIT:** Terraform validation, security scanning, infrastructure preflight and CI. This is an infrastructure baseline; no live AWS deployment is claimed.
- **NEXUS AI 0.7.0:** 70 passing local tests, 12/12 reported live integration checks, successful Qwen3 inference, 768-dimensional Nomic embeddings, and successful GitHub CI for commit `5c6a949`. RAG endpoints passed HTTP smoke tests; retrieval quality remains a separate validation item.

Each project has its own architecture, setup instructions, tests and documented limitations. The repositories can be reviewed and run independently.
