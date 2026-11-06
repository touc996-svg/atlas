import json
from pathlib import Path

import pytest

from scripts.dbt_run_report import load_report, render_report

MODEL = Path("atlas_analytics/models/marts/fct_atlas_events.sql")


def test_incremental_model_uses_two_hour_default_and_ingestion_timestamp():
    sql = MODEL.read_text().lower()
    assert "atlas_incremental_lookback_hours', 2" in sql
    assert "where ingested_at >=" in sql
    assert "max(ingested_at)" in sql
    assert "event_timestamp >=" not in sql


def test_incremental_model_has_source_and_target_indexes():
    sql = MODEL.read_text().lower()
    assert "idx_atlas_events_ingested_at" in sql
    assert "idx_fct_atlas_events_event_id" in sql
    assert "create unique index if not exists" in sql


def test_incremental_model_preserves_idempotent_key_and_strategy():
    sql = " ".join(MODEL.read_text().lower().split())
    assert "unique_key='event_id'" in sql
    assert "incremental_strategy='delete+insert'" in sql


def test_report_formats_execution_metrics(tmp_path):
    result_file = tmp_path / "run_results.json"
    result_file.write_text(json.dumps({
        "results": [{
            "unique_id": "model.atlas_analytics.fct_atlas_events",
            "status": "success",
            "execution_time": 1.25,
            "adapter_response": {"rows_affected": 5},
        }]
    }))

    results = load_report(result_file)
    report = render_report(results)

    assert "fct_atlas_events" in report
    assert "success" in report
    assert "5" in report
    assert "1.25s" in report


def test_report_rejects_missing_results_array(tmp_path):
    result_file = tmp_path / "run_results.json"
    result_file.write_text('{"metadata": {}}')

    with pytest.raises(ValueError):
        load_report(result_file)


def test_reconciliation_script_runs_full_refresh_and_tests():
    script = Path("scripts/dbt_reconcile.sh").read_text()
    assert "--full-refresh" in script
    assert "dbt test" in script
    assert script.index("dbt_run_report.py") < script.index("\ndbt test")
