from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from atlas.processing.incremental_state import (
    build_source_query,
    calculate_window_start,
    has_parquet_files,
    no_new_data_check,
    should_bootstrap,
)


def test_incremental_window_uses_ingestion_time_and_overlap():
    watermark = datetime(2026, 11, 6, 15, 0, tzinfo=timezone.utc)
    assert calculate_window_start(watermark, 48) == watermark - timedelta(hours=48)


def test_window_normalizes_naive_watermark_to_utc():
    watermark = datetime(2026, 11, 6, 15, 0)
    result = calculate_window_start(watermark, 24)
    assert result == datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)


def test_negative_overlap_is_rejected():
    with pytest.raises(ValueError, match="cannot be negative"):
        calculate_window_start(datetime.now(timezone.utc), -1)


def test_bootstrap_when_no_checkpoint_exists():
    assert should_bootstrap(None, target_has_parquet=True) is True


def test_bootstrap_when_local_dataset_is_missing():
    watermark = datetime(2026, 11, 6, tzinfo=timezone.utc)
    assert should_bootstrap(watermark, target_has_parquet=False) is True


def test_incremental_mode_can_run_when_checkpoint_and_dataset_exist():
    watermark = datetime(2026, 11, 6, tzinfo=timezone.utc)
    assert should_bootstrap(watermark, target_has_parquet=True) is False


def test_has_parquet_files_checks_for_real_parquet_objects(tmp_path: Path):
    dataset = tmp_path / "events"
    dataset.mkdir()
    assert has_parquet_files(dataset) is False
    (dataset / "part-00000.parquet").write_bytes(b"parquet-placeholder")
    assert has_parquet_files(dataset) is True


def test_incremental_jdbc_query_filters_on_ingested_at_not_event_time():
    cutoff = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)
    query = " ".join(build_source_query(cutoff).lower().split())
    assert "from public.atlas_events where ingested_at >= timestamptz" in query
    assert "event_timestamp >=" not in query
    assert "ingested_at" in query
    assert "payload::text as payload_json" in query


def test_full_snapshot_query_has_no_where_clause():
    query = " ".join(build_source_query().lower().split())
    assert "from public.atlas_events ) as atlas_events_source" in query
    assert " where " not in query


def test_empty_incremental_window_has_explicit_noop_quality_result():
    check = no_new_data_check(17)
    assert len(check) == 1
    assert check[0]["status"] == "PASS"
    assert check[0]["observed_value"] == 0
    assert "17 overlap-window rows" in check[0]["details"]
