import importlib.util
import json
from pathlib import Path

SCRIPT = Path("scripts/run_scheduled_pipeline.py")
SPEC = importlib.util.spec_from_file_location("atlas_scheduler", SCRIPT)
scheduler = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(scheduler)


def test_scheduler_always_runs_incremental_mode_and_keeps_s3_dbt_enabled():
    command = scheduler.build_pipeline_command("/venv/bin/python", 48)
    assert command == [
        "/venv/bin/python",
        "-m",
        "atlas.processing.pipeline_runner",
        "--mode",
        "incremental",
        "--lookback-hours",
        "48",
    ]
    assert "--skip-s3" not in command
    assert "--skip-dbt" not in command


def test_runtime_config_defaults_and_overrides():
    assert scheduler.runtime_config({}) == (48, 1800)
    assert scheduler.runtime_config({
        "ATLAS_INCREMENTAL_LOOKBACK_HOURS": "24",
        "ATLAS_SCHEDULE_INTERVAL_SECONDS": "900",
    }) == (24, 900)


def test_runtime_config_rejects_invalid_values():
    import pytest

    with pytest.raises(ValueError):
        scheduler.runtime_config({"ATLAS_INCREMENTAL_LOOKBACK_HOURS": "0"})
    with pytest.raises(ValueError):
        scheduler.runtime_config({"ATLAS_SCHEDULE_INTERVAL_SECONDS": "5"})


def test_status_json_is_written_atomically(tmp_path):
    path = tmp_path / "scheduler" / "latest.json"
    payload = {"run_id": "test", "status": "SUCCESS"}
    scheduler.atomic_write_json(path, payload)

    assert json.loads(path.read_text()) == payload
    assert not path.with_suffix(".json.tmp").exists()


def test_run_lock_prevents_overlapping_processes(tmp_path):
    lock_path = tmp_path / "runner.lock"
    with scheduler.run_lock(lock_path) as first:
        assert first is True
        with scheduler.run_lock(lock_path) as second:
            assert second is False


def test_old_run_logs_are_pruned(tmp_path):
    logs = []
    for index in range(5):
        path = tmp_path / f"run-{index}.log"
        path.write_text(str(index))
        logs.append(path)

    scheduler.cleanup_old_runs(tmp_path, keep=2)
    assert len(list(tmp_path.glob("run-*.log"))) == 2


def test_build_command_preserves_virtualenv_python_path():
    executable = "/workspace/atlas/.venv/bin/python"
    command = scheduler.build_pipeline_command(executable, 48)
    assert command[0] == executable
