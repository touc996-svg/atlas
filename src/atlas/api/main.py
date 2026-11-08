import os
import time
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Query


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    password = os.getenv("POSTGRES_PASSWORD")
    if not password or not os.getenv("POSTGRES_USER"):
        raise RuntimeError(
            "Set DATABASE_URL or configure POSTGRES_USER and POSTGRES_PASSWORD."
        )

    return make_conninfo(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "atlas"),
        user=os.environ["POSTGRES_USER"],
        password=password,
    )

DATABASE_URL = get_database_url()
REDPANDA_ADMIN_URL = os.getenv(
    "REDPANDA_ADMIN_URL", "http://localhost:9644"
)
S3_HEALTH_URL = os.getenv(
    "S3_HEALTH_URL", "http://localhost:9000/minio/health/live"
)

app = FastAPI(
    title="ATLAS Analytics API",
    description="Analytics and operational telemetry API for ATLAS.",
    version="1.2.0",
)


def query_db(sql: str, params: tuple = ()):
    try:
        with psycopg.connect(
            DATABASE_URL, row_factory=dict_row, connect_timeout=3
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Analytics database unavailable",
        ) from exc


def check_postgres():
    started = time.perf_counter()
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return {
            "status": "online",
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception:
        return {
            "status": "offline",
            "latency_ms": None,
            "detail": "Database connectivity check failed",
        }


def check_http_service(url: str, expected_status: str | None = None):
    started = time.perf_counter()
    try:
        request = Request(url, headers={"User-Agent": "ATLAS-Health-Check"})
        with urlopen(request, timeout=2) as response:
            body = response.read(4096)
            if response.status != 200:
                raise RuntimeError("Unexpected HTTP status")

        if expected_status:
            payload = json.loads(body.decode("utf-8"))
            if payload.get("status") != expected_status:
                raise RuntimeError("Service is not ready")

        return {
            "status": "online",
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception:
        return {
            "status": "offline",
            "latency_ms": None,
            "detail": "Service health check failed",
        }


@app.get("/")
def root():
    return {
        "platform": "ATLAS",
        "service": "Analytics API",
        "status": "online",
        "version": app.version,
    }


@app.get("/health")
def health():
    services = {
        "api": {"status": "online"},
        "postgres": check_postgres(),
        "redpanda": check_http_service(
            f"{REDPANDA_ADMIN_URL.rstrip('/')}/v1/status/ready",
            expected_status="ready",
        ),
        "object_storage": check_http_service(S3_HEALTH_URL),
    }

    dependencies_online = all(
        services[name]["status"] == "online"
        for name in ("postgres", "redpanda", "object_storage")
    )

    return {
        "status": "healthy" if services["postgres"]["status"] == "online"
                  else "degraded",
        "platform_status": "healthy" if dependencies_online else "degraded",
        "database": (
            "connected"
            if services["postgres"]["status"] == "online"
            else "unavailable"
        ),
        "services": services,
        "checked_at": datetime.now(timezone.utc),
    }


@app.get("/api/v1/pipeline/runs")
def pipeline_runs(limit: int = Query(default=20, ge=1, le=100)):
    """Return recent persisted pipeline runs."""
    return query_db("""
        SELECT run_id, status, started_at, finished_at,
               input_records, output_records, quality_passed, error_message
        FROM public.atlas_pipeline_runs
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/v1/pipeline/runs/{run_id}")
def pipeline_run_detail(run_id: str):
    """Return one run with its stage attempts and data-quality checks."""
    from uuid import UUID

    try:
        parsed_run_id = str(UUID(run_id))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid pipeline run ID") from exc

    runs = query_db("""
        SELECT run_id, status, started_at, finished_at,
               input_records, output_records, quality_passed, error_message
        FROM public.atlas_pipeline_runs
        WHERE run_id = %s
    """, (parsed_run_id,))
    if not runs:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    steps = query_db("""
        SELECT step_name, attempt, status, started_at, finished_at,
               duration_ms, exit_code, output_tail
        FROM public.atlas_pipeline_steps
        WHERE run_id = %s
        ORDER BY step_id
    """, (parsed_run_id,))
    quality = query_db("""
        SELECT check_name, status, observed_value, expected_value, details, created_at
        FROM public.atlas_data_quality_results
        WHERE run_id = %s
        ORDER BY result_id
    """, (parsed_run_id,))
    return {"run": runs[0], "steps": steps, "quality_checks": quality}


@app.get("/api/v1/analytics/summary")
def analytics_summary():
    rows = query_db("""
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT event_id) AS unique_events,
            COUNT(DISTINCT source) AS total_sources,
            COUNT(DISTINCT event_type) AS total_event_types,
            MIN(event_timestamp) AS first_event_at,
            MAX(event_timestamp) AS latest_event_at
        FROM analytics.fct_atlas_events
    """)
    return rows[0]


@app.get("/api/v1/analytics/event-types")
def event_types():
    return query_db("""
        SELECT
            event_type, source, total_events,
            first_event_at, last_event_at, active_days
        FROM analytics.dim_atlas_event_types
        ORDER BY total_events DESC
    """)


@app.get("/api/v1/analytics/daily")
def daily_trends():
    return query_db("""
        SELECT
            event_date,
            COUNT(*) AS total_events,
            COUNT(DISTINCT event_type) AS event_types,
            COUNT(DISTINCT source) AS sources
        FROM analytics.fct_atlas_events
        GROUP BY event_date
        ORDER BY event_date
    """)


@app.get("/api/v1/analytics/telemetry")
def analytics_telemetry():
    rows = query_db("""
        SELECT
            COUNT(*) AS total_events,
            COUNT(*) FILTER (
                WHERE event_timestamp >= NOW() - INTERVAL '1 minute'
            ) AS events_last_minute,
            COUNT(*) FILTER (
                WHERE event_timestamp >= NOW() - INTERVAL '5 minutes'
            ) AS events_last_5_minutes,
            COUNT(DISTINCT kafka_partition) AS active_partitions,
            MAX(kafka_offset) AS latest_kafka_offset,
            MAX(event_timestamp) AS latest_event_at,
            EXTRACT(
                EPOCH FROM (NOW() - MAX(event_timestamp))
            )::double precision AS seconds_since_latest_event
        FROM public.atlas_events
    """)
    return rows[0]


@app.get("/api/v1/events")
def recent_events(limit: int = Query(default=20, ge=1, le=100)):
    return query_db("""
        SELECT
            event_id, event_type, source, event_timestamp,
            payload, kafka_partition, kafka_offset, ingested_at
        FROM analytics.fct_atlas_events
        ORDER BY event_timestamp DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/v1/ingestion/summary")
def ingestion_summary():
    """Return accepted/rejected ingestion totals and recent quarantine activity."""
    rows = query_db("""
        WITH accepted AS (
            SELECT COUNT(*)::BIGINT AS accepted_events
            FROM public.atlas_events
        ),
        rejected AS (
            SELECT
                COUNT(*)::BIGINT AS rejected_events,
                COUNT(*) FILTER (
                    WHERE rejected_at >= NOW() - INTERVAL '1 hour'
                )::BIGINT AS rejected_last_hour,
                COUNT(*) FILTER (
                    WHERE rejected_at >= NOW() - INTERVAL '24 hours'
                )::BIGINT AS rejected_last_24_hours,
                MAX(rejected_at) AS latest_rejection_at
            FROM public.atlas_event_rejections
        )
        SELECT
            accepted.accepted_events,
            rejected.rejected_events,
            rejected.rejected_last_hour,
            rejected.rejected_last_24_hours,
            rejected.latest_rejection_at,
            CASE
                WHEN accepted.accepted_events + rejected.rejected_events = 0
                    THEN 0.0
                ELSE ROUND(
                    rejected.rejected_events * 100.0
                    / (accepted.accepted_events + rejected.rejected_events),
                    2
                )
            END AS rejection_rate_percent
        FROM accepted
        CROSS JOIN rejected
    """)
    return rows[0]


@app.get("/api/v1/ingestion/rejections")
def ingestion_rejections(limit: int = Query(default=50, ge=1, le=100)):
    """List quarantined records without returning their raw message payloads."""
    return query_db("""
        SELECT
            topic,
            kafka_partition,
            kafka_offset,
            error_reason,
            rejected_at,
            octet_length(raw_value) AS raw_value_bytes
        FROM public.atlas_event_rejections
        ORDER BY rejected_at DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/v1/ingestion/rejections/by-reason")
def ingestion_rejections_by_reason(
    limit: int = Query(default=20, ge=1, le=100)
):
    """Aggregate quarantined records by validation or processing reason."""
    return query_db("""
        SELECT
            error_reason,
            COUNT(*)::BIGINT AS rejected_events,
            MAX(rejected_at) AS latest_rejection_at
        FROM public.atlas_event_rejections
        GROUP BY error_reason
        ORDER BY rejected_events DESC, latest_rejection_at DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/v1/ingestion/replay-summary")
def ingestion_replay_summary():
    """Return replay outcome counts without exposing quarantined event bodies."""
    rows = query_db("""
        SELECT
            COUNT(*)::BIGINT AS total_attempts,
            COUNT(*) FILTER (WHERE status = 'PUBLISHED')::BIGINT AS published,
            COUNT(*) FILTER (WHERE status = 'FAILED')::BIGINT AS failed,
            COUNT(*) FILTER (WHERE status = 'PUBLISHING')::BIGINT AS publishing,
            MAX(finished_at) AS latest_finished_at
        FROM public.atlas_event_replay_audit
    """)
    return rows[0]


@app.get("/api/v1/ingestion/replays")
def ingestion_replays(limit: int = Query(default=50, ge=1, le=100)):
    """List replay audit metadata; raw Kafka values are never returned."""
    return query_db("""
        SELECT
            replay_id, source_topic, source_partition, source_offset,
            target_topic, target_partition, target_offset,
            status, attempt, error_message, started_at, finished_at
        FROM public.atlas_event_replay_audit
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))
