from pathlib import Path

MODEL = Path("atlas_analytics/models/marts/fct_atlas_events.sql")


def test_fact_model_is_incremental():
    sql = " ".join(MODEL.read_text().lower().split())

    assert "materialized='incremental'" in sql
    assert "unique_key='event_id'" in sql
    assert "incremental_strategy='delete+insert'" in sql


def test_incremental_filter_uses_ingestion_time():
    sql = MODEL.read_text().lower()

    assert "{% if is_incremental() %}" in sql
    assert "where ingested_at >=" in sql
    assert "max(ingested_at)" in sql


def test_overlap_window_defaults_to_2_hours():
    sql = MODEL.read_text().lower()

    assert "atlas_incremental_lookback_hours', 2" in sql
    assert "make_interval" in sql
    assert "lookback_hours" in sql


def test_existing_fact_columns_are_preserved():
    sql = MODEL.read_text().lower()

    columns = [
        "event_id",
        "event_type",
        "source",
        "event_timestamp",
        "event_date",
        "event_hour",
        "sequence_number",
        "event_message",
        "payload",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    ]

    for column in columns:
        assert column in sql
