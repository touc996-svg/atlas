"""Durable, PostgreSQL-backed incremental-processing checkpoints for ATLAS."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from typing import Any


CHECKPOINT_TABLE = "public.atlas_processing_checkpoints"
DEFAULT_PIPELINE_NAME = "atlas_events"


def normalize_utc(value: datetime) -> datetime:
    """Return an aware UTC datetime; treat legacy naive timestamps as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def calculate_window_start(watermark: datetime, lookback_hours: float) -> datetime:
    """Move the read cursor backwards to capture late-arriving source rows."""
    if lookback_hours < 0:
        raise ValueError("lookback_hours cannot be negative")
    return normalize_utc(watermark) - timedelta(hours=lookback_hours)


def should_bootstrap(watermark: datetime | None, target_has_parquet: bool) -> bool:
    """A first run or missing local dataset requires a complete baseline snapshot."""
    return watermark is None or not target_has_parquet


def has_parquet_files(path: Path) -> bool:
    return path.is_dir() and next(path.rglob("*.parquet"), None) is not None


def build_source_query(ingested_after: datetime | None = None) -> str:
    """Build a read-only JDBC subquery, filtered by the ingestion-time cursor."""
    where_clause = ""
    if ingested_after is not None:
        cutoff = normalize_utc(ingested_after).isoformat(sep=" ")
        # The value is an internally generated ISO timestamp, not user SQL.
        where_clause = f"\n    WHERE ingested_at >= TIMESTAMPTZ '{cutoff}'"
    return f"""(
    SELECT
        event_id,
        event_type,
        source,
        event_timestamp,
        ingested_at,
        payload::text AS payload_json
    FROM public.atlas_events{where_clause}
) AS atlas_events_source"""


def no_new_data_check(source_rows_scanned: int) -> list[dict[str, Any]]:
    if source_rows_scanned:
        details = (
            "No new event IDs were found after checkpoint anti-join; "
            f"{source_rows_scanned} overlap-window rows were already published."
        )
    else:
        details = "No source rows were ingested within the incremental overlap window."
    return [{
        "check_name": "no_unprocessed_event_ids",
        "status": "PASS",
        "observed_value": 0,
        "expected_value": 0,
        "details": details,
    }]


class ProcessingCheckpointStore:
    """A session-level advisory lock plus monotonic checkpoint storage.

    The lock remains held until close(). The filesystem publish and checkpoint
    commit cannot be one distributed transaction; event_id anti-joins make a
    retry safe if publication succeeds but checkpoint advancement fails.
    """

    def __init__(self, database_url: str, *, lock_timeout_seconds: float = 0.0) -> None:
        self.database_url = database_url
        self.lock_timeout_seconds = lock_timeout_seconds
        self._connection: Any | None = None
        self._lock_acquired = False

    def initialize(self) -> None:
        import psycopg

        self._connection = psycopg.connect(
            self.database_url, connect_timeout=10, autocommit=True
        )
        self._connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                pipeline_name TEXT PRIMARY KEY,
                watermark TIMESTAMPTZ NOT NULL,
                last_run_id UUID,
                rows_processed BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    def ensure_source_index(self) -> None:
        if self._connection is None:
            raise RuntimeError("Checkpoint store is not initialized")
        # Keep the bounded ingestion-time scan indexable without taking the
        # stronger write lock used by a regular CREATE INDEX. autocommit=True
        # is required for CREATE INDEX CONCURRENTLY. The processing advisory
        # lock is acquired first so concurrent pipeline starts cannot race here.
        self._connection.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_atlas_events_ingested_at "
            "ON public.atlas_events (ingested_at)"
        )

    def acquire_lock(self, pipeline_name: str = DEFAULT_PIPELINE_NAME) -> None:
        if self._connection is None:
            raise RuntimeError("Checkpoint store must be initialized before locking")
        deadline = time.monotonic() + self.lock_timeout_seconds
        while True:
            row = self._connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (f"atlas-processing:{pipeline_name}",),
            ).fetchone()
            if row and row[0]:
                self._lock_acquired = True
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Another ATLAS processing run holds the '{pipeline_name}' lock."
                )
            time.sleep(0.25)

    def get_watermark(self, pipeline_name: str = DEFAULT_PIPELINE_NAME) -> datetime | None:
        if self._connection is None:
            raise RuntimeError("Checkpoint store is not initialized")
        row = self._connection.execute(
            f"SELECT watermark FROM {CHECKPOINT_TABLE} WHERE pipeline_name=%s",
            (pipeline_name,),
        ).fetchone()
        return normalize_utc(row[0]) if row else None

    def advance_watermark(
        self,
        *,
        watermark: datetime,
        run_id: str,
        rows_processed: int,
        pipeline_name: str = DEFAULT_PIPELINE_NAME,
    ) -> None:
        if self._connection is None:
            raise RuntimeError("Checkpoint store is not initialized")
        value = normalize_utc(watermark)
        self._connection.execute(
            f"""
            INSERT INTO {CHECKPOINT_TABLE} AS existing
                (pipeline_name, watermark, last_run_id, rows_processed, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (pipeline_name) DO UPDATE SET
                watermark = GREATEST(existing.watermark, EXCLUDED.watermark),
                last_run_id = EXCLUDED.last_run_id,
                rows_processed = EXCLUDED.rows_processed,
                updated_at = NOW()
            WHERE EXCLUDED.watermark >= existing.watermark
            """,
            (pipeline_name, value, run_id, rows_processed),
        )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()  # session close releases advisory lock
            self._connection = None
            self._lock_acquired = False

    def __enter__(self) -> "ProcessingCheckpointStore":
        self.initialize()
        self.acquire_lock()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
