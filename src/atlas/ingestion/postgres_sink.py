"""Reliable Kafka-to-PostgreSQL sink for ATLAS.

Delivery contract:
- Kafka offsets are committed only after the corresponding database transaction.
- Valid events are idempotent by event_id.
- Invalid events are durably quarantined by topic/partition/offset.
- Database connectivity failures are retried with bounded exponential backoff.

This is at-least-once ingestion with idempotent database effects, not a distributed
Kafka/PostgreSQL exactly-once transaction.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import psycopg
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

TOPIC = "atlas.events"
GROUP_ID = "atlas-postgres-sink-recovery-v1"
MAX_DB_ATTEMPTS = 5
BASE_RETRY_SECONDS = 1.0
MAX_RETRY_SECONDS = 8.0
TRANSIENT_DATABASE_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

logger = logging.getLogger("atlas.ingestion.postgres_sink")


class EventValidationError(ValueError):
    """Raised when a Kafka message does not conform to the ATLAS event contract."""


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


def parse_event(raw_value: bytes | None) -> tuple[dict[str, Any], datetime]:
    """Decode and validate one event without allowing poison messages to halt the sink."""
    if raw_value is None:
        raise EventValidationError("message value is null")

    try:
        decoded = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EventValidationError("message value is not valid UTF-8") from exc

    def reject_non_finite_number(value: str) -> None:
        raise EventValidationError(
            f"JSON contains unsupported non-finite number: {value}"
        )

    try:
        event = json.loads(decoded, parse_constant=reject_non_finite_number)
    except EventValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise EventValidationError("message value is not valid JSON") from exc

    if not isinstance(event, dict):
        raise EventValidationError("event must be a JSON object")

    for field in ("event_id", "event_type", "source"):
        value = event.get(field)
        if not isinstance(value, str) or not value.strip():
            raise EventValidationError(f"{field} must be a non-empty string")

    if len(event["event_id"]) > 255:
        raise EventValidationError("event_id exceeds 255 characters")

    timestamp_value = event.get("timestamp")
    if not isinstance(timestamp_value, str) or not timestamp_value.strip():
        raise EventValidationError("timestamp must be a non-empty ISO-8601 string")

    try:
        event_timestamp = datetime.fromisoformat(
            timestamp_value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise EventValidationError("timestamp is not a valid ISO-8601 value") from exc

    if event_timestamp.tzinfo is None or event_timestamp.utcoffset() is None:
        raise EventValidationError("timestamp must include a timezone offset")

    if not isinstance(event.get("payload"), dict):
        raise EventValidationError("payload must be a JSON object")

    return event, event_timestamp.astimezone(timezone.utc)


def initialize_schema(db: psycopg.Connection) -> None:
    """Create the raw-event and quarantine tables if they do not already exist."""
    with db.transaction():
        with db.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS atlas_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    event_timestamp TIMESTAMPTZ NOT NULL,
                    payload JSONB NOT NULL,
                    raw_event JSONB NOT NULL,
                    kafka_partition INTEGER,
                    kafka_offset BIGINT,
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS atlas_event_rejections (
                    topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset BIGINT NOT NULL,
                    raw_value BYTEA,
                    error_reason TEXT NOT NULL,
                    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (topic, kafka_partition, kafka_offset)
                )
                """
            )


def store_event(
    db: psycopg.Connection,
    message: Any,
    event: dict[str, Any],
    event_timestamp: datetime,
) -> bool:
    """Persist one valid event; return False when its event_id was already stored."""
    with db.transaction():
        with db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO atlas_events (
                    event_id, event_type, source, event_timestamp,
                    payload, raw_event, kafka_partition, kafka_offset
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["event_type"],
                    event["source"],
                    event_timestamp,
                    Jsonb(event["payload"]),
                    Jsonb(event),
                    message.partition,
                    message.offset,
                ),
            )
            inserted = cursor.rowcount == 1
    return inserted


def store_rejection(db: psycopg.Connection, message: Any, reason: str) -> bool:
    """Durably quarantine an invalid Kafka record; return False if already recorded."""
    with db.transaction():
        with db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO atlas_event_rejections (
                    topic, kafka_partition, kafka_offset, raw_value, error_reason
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
                """,
                (
                    message.topic,
                    message.partition,
                    message.offset,
                    message.value,
                    reason[:1000],
                ),
            )
            inserted = cursor.rowcount == 1
    return inserted


def process_message(db: psycopg.Connection, message: Any) -> str:
    """Persist a valid event or quarantine an invalid one."""
    try:
        event, event_timestamp = parse_event(message.value)
    except EventValidationError as exc:
        store_rejection(db, message, str(exc))
        return "rejected"

    if store_event(db, message, event, event_timestamp):
        return "stored"
    return "duplicate"


def initialize_schema_with_retries(
    database_url: str,
    max_attempts: int = MAX_DB_ATTEMPTS,
) -> psycopg.Connection:
    """Connect and initialize the sink schema with bounded startup retries."""
    for attempt in range(1, max_attempts + 1):
        db: psycopg.Connection | None = None
        try:
            db = psycopg.connect(database_url, connect_timeout=5)
            initialize_schema(db)
            return db
        except TRANSIENT_DATABASE_ERRORS:
            if db is not None:
                try:
                    db.close()
                except psycopg.Error:
                    pass
            if attempt == max_attempts:
                raise
            delay = min(BASE_RETRY_SECONDS * (2 ** (attempt - 1)), MAX_RETRY_SECONDS)
            logger.warning(
                "PostgreSQL unavailable during sink startup; retry %s/%s in %.1fs",
                attempt,
                max_attempts - 1,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError("Database initialization retry loop exited unexpectedly")


def process_message_with_retries(
    db: psycopg.Connection | None,
    database_url: str,
    message: Any,
    max_attempts: int = MAX_DB_ATTEMPTS,
) -> tuple[psycopg.Connection, str]:
    """Retry transient database/connection failures without advancing Kafka offsets."""
    for attempt in range(1, max_attempts + 1):
        try:
            if db is None or db.closed:
                db = psycopg.connect(database_url, connect_timeout=5)
            return db, process_message(db, message)
        except TRANSIENT_DATABASE_ERRORS:
            if db is not None:
                try:
                    db.close()
                except psycopg.Error:
                    pass
            db = None
            if attempt == max_attempts:
                raise
            delay = min(BASE_RETRY_SECONDS * (2 ** (attempt - 1)), MAX_RETRY_SECONDS)
            logger.warning(
                "PostgreSQL unavailable while processing %s/%s/%s; "
                "retry %s/%s in %.1fs",
                message.topic,
                message.partition,
                message.offset,
                attempt,
                max_attempts - 1,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError("Database retry loop exited unexpectedly")


def commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """Commit only the processed record's partition offset, after its DB transaction."""
    partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        {partition: OffsetAndMetadata(message.offset + 1, "")}
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume ATLAS events with durable, replay-safe PostgreSQL ingestion."
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=30.0,
        help="Stop after this many idle seconds; use 0 to run continuously (default: 30).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after processing this many Kafka records (default: no limit).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.idle_timeout < 0:
        raise SystemExit("--idle-timeout must be zero or greater")
    if args.max_events is not None and args.max_events < 1:
        raise SystemExit("--max-events must be greater than zero")

    logging.basicConfig(
        level=os.getenv("ATLAS_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    database_url = get_database_url()
    db: psycopg.Connection | None = None
    consumer: KafkaConsumer | None = None

    try:
        db = initialize_schema_with_retries(database_url)

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ).split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=GROUP_ID,
            max_poll_records=1,
        )

        totals = {"processed": 0, "stored": 0, "duplicate": 0, "rejected": 0}
        last_message_at = time.monotonic()
        logger.info(
            "PostgreSQL sink active for topic %s; idle timeout=%s seconds",
            TOPIC,
            args.idle_timeout if args.idle_timeout else "disabled",
        )

        while True:
            records = consumer.poll(timeout_ms=1000, max_records=1)
            if not records:
                if (
                    args.idle_timeout > 0
                    and time.monotonic() - last_message_at >= args.idle_timeout
                ):
                    logger.info("Idle timeout reached; stopping sink")
                    break
                continue

            for messages in records.values():
                for message in messages:
                    db, outcome = process_message_with_retries(
                        db, database_url, message
                    )

                    # The database transaction has completed before this offset commit.
                    # If the commit fails, replay is safe because both writes are idempotent.
                    commit_message(consumer, message)

                    totals["processed"] += 1
                    totals[outcome] += 1
                    last_message_at = time.monotonic()
                    logger.info(
                        "Processed %s/%s/%s outcome=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                        outcome,
                    )

                    if (
                        args.max_events is not None
                        and totals["processed"] >= args.max_events
                    ):
                        break

                if (
                    args.max_events is not None
                    and totals["processed"] >= args.max_events
                ):
                    break

            if (
                args.max_events is not None
                and totals["processed"] >= args.max_events
            ):
                break

        logger.info(
            "Sink stopped: processed=%s stored=%s duplicates=%s rejected=%s",
            totals["processed"],
            totals["stored"],
            totals["duplicate"],
            totals["rejected"],
        )
    finally:
        if consumer is not None:
            consumer.close()
        if db is not None and not db.closed:
            db.close()


if __name__ == "__main__":
    main()
