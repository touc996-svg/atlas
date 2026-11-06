"""Unit tests for ATLAS orchestration contracts (no Spark cluster required)."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from atlas.processing.pipeline_runner import (
    execute_stage,
    redact_output,
    validate_manifest,
)


def test_manifest_passes_when_counts_and_quality_checks_are_valid():
    manifest = {
        "status": "SUCCESS",
        "input_rows": 12,
        "output_rows": 12,
        "quality_checks": [
            {"check_name": "row_count_preserved", "status": "PASS"},
            {"check_name": "unique_event_ids", "status": "PASS"},
        ],
    }
    valid, reason = validate_manifest(manifest)
    assert valid is True
    assert "valid" in reason


def test_manifest_fails_when_a_quality_gate_fails():
    manifest = {
        "status": "SUCCESS",
        "input_rows": 12,
        "output_rows": 12,
        "quality_checks": [
            {"check_name": "unique_event_ids", "status": "FAIL"},
        ],
    }
    valid, reason = validate_manifest(manifest)
    assert valid is False
    assert "unique_event_ids" in reason


def test_manifest_fails_when_row_count_changes():
    valid, reason = validate_manifest({
        "status": "SUCCESS",
        "input_rows": 12,
        "output_rows": 11,
        "quality_checks": [],
    })
    assert valid is False
    assert "record counts" in reason


def test_manifest_fails_for_incomplete_spark_run():
    valid, reason = validate_manifest({"status": "FAILED", "error": "Spark error"})
    assert valid is False
    assert reason == "Spark error"


def test_redact_output_removes_common_secret_assignments():
    output = "POSTGRES_PASSWORD=do-not-store AWS_ACCESS_KEY_ID: ABC123"
    redacted = redact_output(output)
    assert "do-not-store" not in redacted
    assert "ABC123" not in redacted
    assert redacted.count("[REDACTED]") == 2


def test_execute_stage_retries_then_succeeds(monkeypatch):
    store = Mock()
    store.start_step.side_effect = [101, 102]
    run_results = [
        SimpleNamespace(returncode=1, stdout="temporary failure", stderr=""),
        SimpleNamespace(returncode=0, stdout="stage complete", stderr=""),
    ]
    run = Mock(side_effect=run_results)
    monkeypatch.setattr("atlas.processing.pipeline_runner.subprocess.run", run)
    sleep = Mock()
    monkeypatch.setattr("atlas.processing.pipeline_runner.time.sleep", sleep)

    execute_stage(
        store=store,
        run_id="run-1",
        name="example",
        command=["python", "-c", "pass"],
        cwd=__import__("pathlib").Path("."),
        env={},
        max_attempts=2,
        retry_delay=0.01,
        timeout=10,
    )

    assert run.call_count == 2
    assert store.finish_step.call_count == 2
    assert store.finish_step.call_args_list[0].kwargs["status"] == "FAILED"
    assert store.finish_step.call_args_list[1].kwargs["status"] == "SUCCESS"
    sleep.assert_called_once_with(0.01)


def test_execute_stage_fails_after_attempt_limit(monkeypatch):
    store = Mock()
    store.start_step.return_value = 201
    monkeypatch.setattr(
        "atlas.processing.pipeline_runner.subprocess.run",
        Mock(return_value=SimpleNamespace(returncode=2, stdout="broken", stderr="")),
    )
    monkeypatch.setattr("atlas.processing.pipeline_runner.time.sleep", Mock())

    with pytest.raises(RuntimeError, match="failed after 1 attempts"):
        execute_stage(
            store=store,
            run_id="run-2",
            name="broken-stage",
            command=["missing"],
            cwd=__import__("pathlib").Path("."),
            env={},
            max_attempts=1,
            retry_delay=0,
            timeout=10,
        )

    store.finish_step.assert_called_once()
    assert store.finish_step.call_args.kwargs["status"] == "FAILED"
