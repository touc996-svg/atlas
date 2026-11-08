"""Controlled, auditable replay of quarantined ATLAS Kafka events.

Replay is at-least-once across PostgreSQL and Kafka. The downstream PostgreSQL
sink remains idempotent by event_id, while this module records every replay
attempt and never deletes or modifies the original quarantine record.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg
from kafka import KafkaProducer
from psycopg.conninfo import make_conninfo

TOPIC = "atlas.events"
AUDIT_TABLE = "public.atlas_event_replay_audit"
LOCK_NAME = "atlas-dead-letter-replay-v1"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
logger = logging.getLogger("atlas.ingestion.replay")


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    if not user or not password:
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


def parse_datetime_bound(value: str | None) -> datetime | None:
    """Parse an ISO-8601 bound and require an explicit timezone."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 datetime: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Replay date bounds must include a timezone offset.")
    return parsed.astimezone(timezone.utc)


def validate_date_window(
    since: datetime | None, until: datetime | None
) -> tuple[datetime | None, datetime | None]:
    if since is not None and until is not None and since >= until:
        raise ValueError("--since must be earlier than --until")
    return since, until


def build_candidate_query(
    since: datetime | None = None,
    until: datetime | None = None,
) -> tuple[str, tuple[Any, ...]]:
    """Return a parameterized query for replayable quarantine records."""
    clauses = [
        "r.raw_value IS NOT NULL",
        "NOT EXISTS (SELECT 1 FROM public.atlas_event_replay_audit a "
        "WHERE a.source_topic=r.topic "
        "AND a.source_partition=r.kafka_partition "
        "AND a.source_offset=r.kafka_offset AND a.status='PUBLISHED')",
        "NOT EXISTS (SELECT 1 FROM public.atlas_event_replay_audit active "
        "WHERE active.source_topic=r.topic "
        "AND active.source_partition=r.kafka_partition "
        "AND active.source_offset=r.kafka_offset "
        "AND active.status='PUBLISHING' "
        "AND active.started_at > NOW() - INTERVAL '10 minutes')",
    ]
    params: list[Any] = []
    if since is not None:
        clauses.append("r.rejected_at >= %s")
        params.append(since)
    if until is not None:
        clauses.append("r.rejected_at < %s")
        params.append(until)
    sql = (
        "SELECT r.topic, r.kafka_partition, r.kafka_offset, r.raw_value "
        "FROM public.atlas_event_rejections r WHERE "
        + " AND ".join(clauses)
        + " ORDER BY r.rejected_at ASC, r.topic, r.kafka_partition, r.kafka_offset "
        " LIMIT %s"
    )
    return sql, tuple(params)


def initialize_replay_schema(db: Any) -> None:
    """Create replay audit storage without changing the original DLQ table."""
    with db.transaction():
        with db.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                    replay_id UUID PRIMARY KEY,
                    source_topic TEXT NOT NULL,
                    source_partition INTEGER NOT NULL,
                    source_offset BIGINT NOT NULL,
                    target_topic TEXT NOT NULL,
                    target_partition INTEGER,
                    target_offset BIGINT,
                    status TEXT NOT NULL CHECK (
                        status IN ('PUBLISHING', 'PUBLISHED', 'FAILED')
                    ),
                    attempt INTEGER NOT NULL CHECK (attempt > 0),
                    error_message TEXT,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
            """)
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_atlas_replay_started "
                f"ON {AUDIT_TABLE} (started_at DESC)"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_atlas_replay_status "
                f"ON {AUDIT_TABLE} (status, started_at DESC)"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_atlas_replay_source "
                f"ON {AUDIT_TABLE} (source_topic, source_partition, source_offset)"
            )


def acquire_replay_lock(db: Any) -> None:
    row = db.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (LOCK_NAME,)
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError("Another ATLAS dead-letter replay process is already running.")


def publish_raw_value(
    producer: Any,
    *,
    target_topic: str,
    raw_value: bytes,
    headers: list[tuple[str, bytes]],
    max_attempts: int = 3,
    retry_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Publish original bytes with bounded retries; do not parse or mutate payload."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if retry_delay < 0:
        raise ValueError("retry_delay cannot be negative")
    for attempt in range(1, max_attempts + 1):
        try:
            return producer.send(
                target_topic, value=raw_value, headers=headers
            ).get(timeout=30)
        except Exception:
            if attempt == max_attempts:
                raise
            delay = min(retry_delay * (2 ** (attempt - 1)), 8.0)
            logger.warning("Kafka replay publish failed; retry %s/%s in %.1fs", attempt, max_attempts - 1, delay)
            sleep(delay)
    raise RuntimeError("Kafka replay retry loop exited unexpectedly")


def run_replay(
    db: Any,
    producer: Any,
    *,
    limit: int = 100,
    target_topic: str = TOPIC,
    since: datetime | None = None,
    until: datetime | None = None,
    dry_run: bool = False,
    max_attempts: int = 3,
    retry_delay: float = 1.0,
) -> dict[str, Any]:
    if limit < 1 or limit > 10000:
        raise ValueError("limit must be between 1 and 10000")
    if not target_topic.strip():
        raise ValueError("target_topic must not be empty")
    validate_date_window(since, until)
    initialize_replay_schema(db)
    acquire_replay_lock(db)

    sql, params = build_candidate_query(since, until)
    rows = db.execute(sql, (*params, limit)).fetchall()
    if dry_run:
        return {
            "status": "DRY_RUN",
            "candidates": len(rows),
            "published": 0,
            "failed": 0,
            "target_topic": target_topic,
        }

    totals = {"status": "COMPLETED", "candidates": len(rows), "published": 0, "failed": 0}
    for source_topic, source_partition, source_offset, raw_value in rows:
        replay_id = str(uuid.uuid4())
        attempt_row = db.execute(
            f"SELECT COALESCE(MAX(attempt), 0) + 1 FROM {AUDIT_TABLE} "
            "WHERE source_topic=%s AND source_partition=%s AND source_offset=%s",
            (source_topic, source_partition, source_offset),
        ).fetchone()
        attempt = int(attempt_row[0])
        with db.transaction():
            db.execute(
                f"""INSERT INTO {AUDIT_TABLE}
                    (replay_id, source_topic, source_partition, source_offset,
                     target_topic, status, attempt)
                    VALUES (%s, %s, %s, %s, %s, 'PUBLISHING', %s)""",
                (replay_id, source_topic, source_partition, source_offset, target_topic, attempt),
            )
        headers = [
            ("atlas-replay-id", replay_id.encode("utf-8")),
            ("atlas-original-topic", source_topic.encode("utf-8")),
            ("atlas-original-partition", str(source_partition).encode("ascii")),
            ("atlas-original-offset", str(source_offset).encode("ascii")),
        ]
        try:
            metadata = publish_raw_value(
                producer,
                target_topic=target_topic,
                raw_value=bytes(raw_value),
                headers=headers,
                max_attempts=max_attempts,
                retry_delay=retry_delay,
            )
            with db.transaction():
                db.execute(
                    f"""UPDATE {AUDIT_TABLE}
                        SET status='PUBLISHED', target_partition=%s, target_offset=%s,
                            finished_at=NOW(), error_message=NULL
                        WHERE replay_id=%s""",
                    (metadata.partition, metadata.offset, replay_id),
                )
            totals["published"] += 1
            logger.info(
                "Replayed quarantined record %s/%s/%s to %s/%s",
                source_topic, source_partition, source_offset,
                metadata.partition, metadata.offset,
            )
        except Exception as exc:
            safe_error = f"{type(exc).__name__}: {exc}"[:500]
            with db.transaction():
                db.execute(
                    f"""UPDATE {AUDIT_TABLE}
                        SET status='FAILED', finished_at=NOW(), error_message=%s
                        WHERE replay_id=%s""",
                    (safe_error, replay_id),
                )
            totals["failed"] += 1
            logger.exception(
                "Replay failed for quarantined record %s/%s/%s",
                source_topic, source_partition, source_offset,
            )
    if totals["failed"]:
        totals["status"] = "COMPLETED_WITH_FAILURES"
    totals["target_topic"] = target_topic
    return totals


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay quarantined ATLAS Kafka events with an audit trail."
    )
    parser.add_argument("--limit", type=int, default=100, help="Maximum records per run (1-10000).")
    parser.add_argument("--target-topic", default=os.getenv("ATLAS_REPLAY_TOPIC", TOPIC))
    parser.add_argument("--since", help="Inclusive rejection-time bound, ISO-8601 with timezone.")
    parser.add_argument("--until", help="Exclusive rejection-time bound, ISO-8601 with timezone.")
    parser.add_argument("--dry-run", action="store_true", help="Count eligible records without publishing.")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    args = parser.parse_args(argv)
    if args.limit < 1 or args.limit > 10000 or args.max_attempts < 1 or args.retry_delay < 0:
        parser.error("limit must be 1-10000, max-attempts positive, retry-delay non-negative")
    try:
        since = parse_datetime_bound(args.since)
        until = parse_datetime_bound(args.until)
        validate_date_window(since, until)
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=os.getenv("ATLAS_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    database_url = get_database_url()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS).split(",")
    producer = None
    try:
        with psycopg.connect(database_url, connect_timeout=10, autocommit=True) as db:
            if not args.dry_run:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    acks="all",
                    retries=5,
                    max_in_flight_requests_per_connection=1,
                )
            else:
                # The run_replay signature accepts a producer but dry-run never uses it.
                producer = object()
            result = run_replay(
                db,
                producer,
                limit=args.limit,
                target_topic=args.target_topic,
                since=since,
                until=until,
                dry_run=args.dry_run,
                max_attempts=args.max_attempts,
                retry_delay=args.retry_delay,
            )
            print(json.dumps(result, indent=2, default=str))
    finally:
        if producer is not None and hasattr(producer, "flush"):
            producer.flush(timeout=30)
            producer.close(timeout=30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
