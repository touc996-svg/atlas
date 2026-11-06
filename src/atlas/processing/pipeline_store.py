"""PostgreSQL control-plane storage for ATLAS processing runs."""
from __future__ import annotations

import os
from typing import Any

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb


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


class PipelineStore:
    """Small repository layer; every write is committed independently."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or get_database_url()

    def _connect(self):
        return psycopg.connect(self.database_url, connect_timeout=5)

    def initialize(self) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.atlas_pipeline_runs (
                    run_id UUID PRIMARY KEY,
                    status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    input_records BIGINT NOT NULL DEFAULT 0,
                    output_records BIGINT NOT NULL DEFAULT 0,
                    quality_passed BOOLEAN,
                    error_message TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.atlas_pipeline_steps (
                    step_id BIGSERIAL PRIMARY KEY,
                    run_id UUID NOT NULL REFERENCES public.atlas_pipeline_runs(run_id)
                        ON DELETE CASCADE,
                    step_name TEXT NOT NULL,
                    attempt INTEGER NOT NULL CHECK (attempt > 0),
                    status TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    duration_ms BIGINT,
                    exit_code INTEGER,
                    output_tail TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.atlas_data_quality_results (
                    result_id BIGSERIAL PRIMARY KEY,
                    run_id UUID NOT NULL REFERENCES public.atlas_pipeline_runs(run_id)
                        ON DELETE CASCADE,
                    check_name TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('PASS','FAIL','WARN')),
                    observed_value TEXT,
                    expected_value TEXT,
                    details TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (run_id, check_name)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_atlas_pipeline_runs_started "
                "ON public.atlas_pipeline_runs (started_at DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_atlas_pipeline_steps_run "
                "ON public.atlas_pipeline_steps (run_id, step_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_atlas_quality_run "
                "ON public.atlas_data_quality_results (run_id, result_id)"
            )

    def start_run(self, run_id: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.atlas_pipeline_runs (run_id, status) VALUES (%s, 'RUNNING')",
                (run_id,),
            )

    def start_step(self, run_id: str, step_name: str, attempt: int) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.atlas_pipeline_steps
                    (run_id, step_name, attempt, status)
                VALUES (%s, %s, %s, 'RUNNING')
                RETURNING step_id
                """,
                (run_id, step_name, attempt),
            )
            return int(cur.fetchone()[0])

    def finish_step(
        self,
        step_id: int,
        *,
        status: str,
        duration_ms: int,
        exit_code: int | None,
        output_tail: str,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.atlas_pipeline_steps
                SET status=%s, finished_at=NOW(), duration_ms=%s,
                    exit_code=%s, output_tail=%s
                WHERE step_id=%s
                """,
                (status, duration_ms, exit_code, output_tail[-4000:], step_id),
            )

    def save_quality_results(self, run_id: str, checks: list[dict[str, Any]]) -> None:
        if not checks:
            return
        rows = [
            (
                run_id,
                check["check_name"],
                check["status"],
                str(check.get("observed_value", "")),
                str(check.get("expected_value", "")),
                check.get("details", ""),
            )
            for check in checks
        ]
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO public.atlas_data_quality_results
                    (run_id, check_name, status, observed_value, expected_value, details)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, check_name) DO UPDATE SET
                    status=EXCLUDED.status,
                    observed_value=EXCLUDED.observed_value,
                    expected_value=EXCLUDED.expected_value,
                    details=EXCLUDED.details,
                    created_at=NOW()
                """,
                rows,
            )

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        input_records: int = 0,
        output_records: int = 0,
        quality_passed: bool | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.atlas_pipeline_runs
                SET status=%s, finished_at=NOW(), input_records=%s,
                    output_records=%s, quality_passed=%s, error_message=%s
                WHERE run_id=%s
                """,
                (
                    status,
                    input_records,
                    output_records,
                    quality_passed,
                    error_message[:2000] if error_message else None,
                    run_id,
                ),
            )
