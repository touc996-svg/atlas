from atlas.api import main as api


def test_ingestion_summary_returns_accepted_and_rejected_metrics(monkeypatch):
    expected = {
        "accepted_events": 125,
        "rejected_events": 5,
        "rejected_last_hour": 2,
        "rejected_last_24_hours": 4,
        "latest_rejection_at": "2026-11-08T12:00:00Z",
        "rejection_rate_percent": 3.85,
    }
    captured = {}

    def fake_query(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [expected]

    monkeypatch.setattr(api, "query_db", fake_query)
    assert api.ingestion_summary() == expected
    assert "public.atlas_events" in captured["sql"]
    assert "public.atlas_event_rejections" in captured["sql"]
    assert "rejected_last_24_hours" in captured["sql"]


def test_recent_rejections_are_limited_and_do_not_expose_raw_payload(monkeypatch):
    captured = {}

    def fake_query(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [{"topic": "atlas.events", "kafka_partition": 1, "kafka_offset": 9}]

    monkeypatch.setattr(api, "query_db", fake_query)
    result = api.ingestion_rejections(limit=25)

    assert result[0]["topic"] == "atlas.events"
    assert captured["params"] == (25,)
    assert "octet_length(raw_value) AS raw_value_bytes" in captured["sql"]
    assert "SELECT raw_value" not in captured["sql"]


def test_rejection_reason_endpoint_aggregates_and_applies_limit(monkeypatch):
    captured = {}

    def fake_query(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [{"error_reason": "invalid JSON", "rejected_events": 3}]

    monkeypatch.setattr(api, "query_db", fake_query)
    result = api.ingestion_rejections_by_reason(limit=10)

    assert result[0]["rejected_events"] == 3
    assert captured["params"] == (10,)
    assert "GROUP BY error_reason" in captured["sql"]
