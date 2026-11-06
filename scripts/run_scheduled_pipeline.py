#!/usr/bin/env python3
"""Launch one non-overlapping incremental ATLAS pipeline run."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.request import Request, urlopen

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEDULER_ROOT = PROJECT_ROOT / "data" / "processed" / "scheduler"
RUNS_ROOT = SCHEDULER_ROOT / "runs"
LOCK_PATH = SCHEDULER_ROOT / "runner.lock"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def runtime_config(environ: dict[str, str] | None = None) -> tuple[int, int]:
    env = os.environ if environ is None else environ
    try:
        lookback = int(env.get("ATLAS_INCREMENTAL_LOOKBACK_HOURS", "48"))
        interval = int(env.get("ATLAS_SCHEDULE_INTERVAL_SECONDS", "1800"))
    except ValueError as exc:
        raise ValueError("Lookback hours and schedule interval must be integers.") from exc

    if lookback < 1:
        raise ValueError("ATLAS_INCREMENTAL_LOOKBACK_HOURS must be >= 1.")
    if interval < 60:
        raise ValueError("ATLAS_SCHEDULE_INTERVAL_SECONDS must be >= 60.")
    return lookback, interval


def build_pipeline_command(python_executable: str, lookback_hours: int) -> list[str]:
    return [
        python_executable,
        "-m",
        "atlas.processing.pipeline_runner",
        "--mode",
        "incremental",
        "--lookback-hours",
        str(lookback_hours),
    ]


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    temp_path.replace(path)


@contextmanager
def run_lock(path: Path) -> Iterator[bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def cleanup_old_runs(directory: Path, keep: int = 30) -> None:
    if keep < 1:
        raise ValueError("keep must be >= 1")
    logs = sorted(
        directory.glob("run-*.log"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for old_log in logs[keep:]:
        old_log.unlink(missing_ok=True)


def notify_failure(webhook_url: str | None, payload: dict) -> None:
    """Send a small failure notification; never include secrets or raw logs."""
    if not webhook_url:
        return
    body = json.dumps({
        "text": (
            f"ATLAS scheduled pipeline {payload.get('status')}: "
            f"run_id={payload.get('run_id')}, "
            f"exit_code={payload.get('exit_code')}"
        )
    }).encode("utf-8")
    request = Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            response.read(256)
    except Exception as exc:
        print(f"Failure webhook could not be delivered: {type(exc).__name__}", file=sys.stderr)


def run_once(*, project_root: Path = PROJECT_ROOT, dry_run: bool = False) -> int:
    project_root = project_root.resolve()
    load_dotenv(project_root / ".env", override=False)

    try:
        lookback_hours, _ = runtime_config()
    except ValueError as exc:
        print(f"Invalid scheduler configuration: {exc}", file=sys.stderr)
        return 2

    python_executable = sys.executable
    command = build_pipeline_command(python_executable, lookback_hours)

    if dry_run:
        print("ATLAS SCHEDULED PIPELINE (DRY RUN)")
        print(f"Project: {project_root}")
        print(f"Command: {' '.join(command)}")
        print("S3 publishing and dbt stages are enabled.")
        return 0

    src_path = str(project_root / "src")
    env = os.environ.copy()
    env["PYTHONPATH"] = src_path + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    env["PYTHONUNBUFFERED"] = "1"

    scheduler_root = project_root / "data" / "processed" / "scheduler"
    runs_root = scheduler_root / "runs"
    lock_path = scheduler_root / "runner.lock"
    runs_root.mkdir(parents=True, exist_ok=True)

    with run_lock(lock_path) as acquired:
        if not acquired:
            print("ATLAS scheduled run skipped: another pipeline run holds the lock.")
            return 0

        run_id = str(uuid.uuid4())
        started_at = utc_now()
        started = time.monotonic()
        log_path = runs_root / f"run-{run_id}.log"
        status_path = runs_root / f"run-{run_id}.json"
        status = {
            "run_id": run_id,
            "status": "RUNNING",
            "started_at": started_at,
            "finished_at": None,
            "lookback_hours": lookback_hours,
            "command": command,
            "exit_code": None,
            "duration_seconds": None,
            "log_file": str(log_path),
        }
        atomic_write_json(status_path, status)
        atomic_write_json(scheduler_root / "latest.json", status)

        exit_code = 127
        with log_path.open("w", encoding="utf-8") as log_handle:
            log_handle.write(
                f"ATLAS scheduled run {run_id}\n"
                f"Started: {started_at}\n"
                f"Command: {' '.join(command)}\n\n"
            )
            log_handle.flush()
            try:
                completed = subprocess.run(
                    command,
                    cwd=project_root,
                    env=env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
                exit_code = completed.returncode
            except OSError as exc:
                log_handle.write(f"\nUnable to launch pipeline: {type(exc).__name__}: {exc}\n")
                exit_code = 127

        finished_at = utc_now()
        status.update({
            "status": "SUCCESS" if exit_code == 0 else "FAILED",
            "finished_at": finished_at,
            "exit_code": exit_code,
            "duration_seconds": round(time.monotonic() - started, 3),
        })
        atomic_write_json(status_path, status)
        atomic_write_json(scheduler_root / "latest.json", status)
        cleanup_old_runs(runs_root, keep=30)

        print(
            f"ATLAS scheduled pipeline {status['status']} — "
            f"run_id={run_id} — duration={status['duration_seconds']}s"
        )
        print(f"Run log: {log_path}")
        if exit_code != 0:
            notify_failure(os.getenv("ATLAS_ALERT_WEBHOOK_URL"), status)
        return 0 if exit_code == 0 else exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return run_once(dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
