"""Retryable ATLAS orchestration for Spark, S3 publication and dbt."""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any



@dataclass
class CommandResult:
    exit_code: int
    output: str
    duration_ms: int


def validate_manifest(manifest: dict[str, Any]) -> tuple[bool, str]:
    if manifest.get("status") != "SUCCESS":
        return False, str(manifest.get("error", "Spark job did not complete successfully"))
    failed = [
        item.get("check_name", "unnamed_check")
        for item in manifest.get("quality_checks", [])
        if item.get("status") == "FAIL"
    ]
    if failed:
        return False, "Data-quality gate failed: " + ", ".join(failed)
    if int(manifest.get("input_rows", -1)) != int(manifest.get("output_rows", -2)):
        return False, "Source and output record counts do not match"
    return True, "Spark manifest and quality checks are valid"


def redact_output(value: str) -> str:
    """Avoid persisting common secret assignments in pipeline step output."""
    return re.sub(
        r"(?i)(password|secret|token|aws_access_key_id)(\s*[:=]\s*)[^\s,;]+",
        r"\1\2[REDACTED]",
        value,
    )


def execute_stage(
    *,
    store: PipelineStore,
    run_id: str,
    name: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    max_attempts: int,
    retry_delay: float,
    timeout: int,
) -> None:
    for attempt in range(1, max_attempts + 1):
        step_id = store.start_step(run_id, name, attempt)
        started = time.monotonic()
        exit_code: int | None = None
        output = ""
        try:
            result = subprocess.run(
                command,
                cwd=str(cwd),
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            exit_code = result.returncode
            output = "\n".join(part for part in (result.stdout, result.stderr) if part).strip()
        except subprocess.TimeoutExpired as exc:
            exit_code = 124
            output = f"Stage timed out after {timeout} seconds."
            if exc.stdout:
                output += "\n" + str(exc.stdout)[-1000:]
            if exc.stderr:
                output += "\n" + str(exc.stderr)[-1000:]
        except OSError as exc:
            exit_code = 127
            output = f"Could not start stage: {type(exc).__name__}: {exc}"

        duration_ms = int((time.monotonic() - started) * 1000)
        status = "SUCCESS" if exit_code == 0 else "FAILED"
        safe_output = redact_output(output)
        store.finish_step(
            step_id,
            status=status,
            duration_ms=duration_ms,
            exit_code=exit_code,
            output_tail=safe_output[-4000:],
        )

        print(f"[{status}] {name} attempt {attempt}/{max_attempts} ({duration_ms} ms)")
        if output:
            print(safe_output[-2500:])
        if exit_code == 0:
            return
        if attempt < max_attempts:
            delay = retry_delay * (2 ** (attempt - 1))
            print(f"Retrying {name} in {delay:.1f}s...")
            time.sleep(delay)

    raise RuntimeError(f"Pipeline stage '{name}' failed after {max_attempts} attempts.")


def run_pipeline(
    *,
    skip_s3: bool = False,
    skip_dbt: bool = False,
    max_attempts: int = 2,
    retry_delay: float = 2.0,
    timeout: int = 1800,
    output_path: str = "data/processed/events",
    mode: str = "full",
    lookback_hours: float = 48.0,
) -> str:
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be 'full' or 'incremental'")
    if lookback_hours < 0:
        raise ValueError("lookback_hours cannot be negative")

    # Import the database adapter only for an actual orchestration run; this keeps
    # manifest/retry utility tests independent of the local PostgreSQL driver.
    from atlas.processing.pipeline_store import PipelineStore

    project_root = Path(__file__).resolve().parents[3]
    run_id = str(uuid.uuid4())
    manifest_path = project_root / "data/processed/manifests" / f"{run_id}.json"
    output_path_abs = str((project_root / output_path).resolve())
    store = PipelineStore()
    store.initialize()
    store.start_run(run_id)
    input_rows = output_rows = 0
    quality_passed: bool | None = None
    env = os.environ.copy()
    src_path = str(project_root / "src")
    env["PYTHONPATH"] = src_path + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PYTHONUNBUFFERED"] = "1"

    stages: list[tuple[str, list[str]]] = [
        (
            "spark_transform",
            [
                sys.executable, "-m", "atlas.processing.spark_job",
                "--run-id", run_id,
                "--output-path", output_path_abs,
                "--manifest-path", str(manifest_path),
                "--mode", mode,
                "--lookback-hours", str(lookback_hours),
            ],
        )
    ]
    if not skip_s3:
        stages.append(("publish_s3", [sys.executable, "-m", "atlas.storage.s3_lake"]))
    if not skip_dbt:
        stages.extend([
            ("dbt_run", ["dbt", "run", "--project-dir", "atlas_analytics"]),
            ("dbt_test", ["dbt", "test", "--project-dir", "atlas_analytics"]),
        ])

    try:
        for name, command in stages:
            try:
                execute_stage(
                    store=store,
                    run_id=run_id,
                    name=name,
                    command=command,
                    cwd=project_root,
                    env=env,
                    max_attempts=max_attempts,
                    retry_delay=retry_delay,
                    timeout=timeout,
                )
            except Exception:
                # A Spark quality-gate failure still writes a FAILED manifest.
                # Persist its checks before marking the orchestration run failed.
                if name == "spark_transform" and manifest_path.is_file():
                    failed_manifest = json.loads(manifest_path.read_text())
                    input_rows = int(failed_manifest.get("input_rows", 0))
                    output_rows = int(failed_manifest.get("output_rows", 0))
                    quality_passed = bool(failed_manifest.get("quality_passed", False))
                    store.save_quality_results(
                        run_id, failed_manifest.get("quality_checks", [])
                    )
                raise

            if name == "spark_transform":
                if not manifest_path.is_file():
                    raise RuntimeError("Spark completed without producing its run manifest.")
                manifest = json.loads(manifest_path.read_text())
                input_rows = int(manifest.get("input_rows", 0))
                output_rows = int(manifest.get("output_rows", 0))
                quality_passed = bool(manifest.get("quality_passed"))
                checks = manifest.get("quality_checks", [])
                store.save_quality_results(run_id, checks)
                valid, reason = validate_manifest(manifest)
                if not valid:
                    raise RuntimeError(reason)

        store.finish_run(
            run_id,
            status="SUCCESS",
            input_records=input_rows,
            output_records=output_rows,
            quality_passed=quality_passed,
        )
        print(f"ATLAS PIPELINE SUCCEEDED — run_id={run_id}")
        return run_id
    except Exception as exc:
        store.finish_run(
            run_id,
            status="FAILED",
            input_records=input_rows,
            output_records=output_rows,
            quality_passed=quality_passed,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        print(f"ATLAS PIPELINE FAILED — run_id={run_id} — {type(exc).__name__}: {exc}")
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Orchestrate the ATLAS processing pipeline.")
    parser.add_argument("--skip-s3", action="store_true", help="Skip object-storage publication.")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt run and test stages.")
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    parser.add_argument("--timeout", type=int, default=1800, help="Timeout per stage in seconds.")
    parser.add_argument("--output-path", default="data/processed/events")
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument(
        "--lookback-hours", type=float,
        default=float(os.getenv("ATLAS_INCREMENTAL_LOOKBACK_HOURS", "48")),
        help="Overlap window used to discover late-arriving events in incremental mode.",
    )
    args = parser.parse_args(argv)
    if args.max_attempts < 1 or args.retry_delay < 0 or args.timeout < 1:
        parser.error("max-attempts and timeout must be positive; retry-delay cannot be negative")
    if args.lookback_hours < 0:
        parser.error("lookback-hours cannot be negative")
    run_pipeline(
        skip_s3=args.skip_s3,
        skip_dbt=args.skip_dbt,
        max_attempts=args.max_attempts,
        retry_delay=args.retry_delay,
        timeout=args.timeout,
        output_path=args.output_path,
        mode=args.mode,
        lookback_hours=args.lookback_hours,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
