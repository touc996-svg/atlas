import os

os.environ.setdefault("POSTGRES_USER", "atlas_ci")
os.environ.setdefault("POSTGRES_PASSWORD", "ci-only-password")

from atlas.api import main as api


def test_replay_summary_returns_status_totals_without_raw_values(monkeypatch):
    expected = {
        "total_attempts": 9,
        "published": 7,
        "failed": 1,
        "publishing": 1,
        "latest_finished_at": "2026-11-08T12:00:00Z",
    }
    captured = {}

    def fake_query(sql, params=()):
        captured["sql"] = sql
        return [expected]

    monkeypatch.setattr(api, "query_db", fake_query)
    assert api.ingestion_replay_summary() == expected
    assert "atlas_event_replay_audit" in captured["sql"]
    assert "raw_value" not in captured["sql"]


def test_replay_history_is_bounded_and_does_not_select_raw_payload(monkeypatch):
    captured = {}

    def fake_query(sql, params=()):
        captured["sql"] = sql
        captured["params"] = params
        return [{"status": "PUBLISHED", "source_offset": 12}]

    monkeypatch.setattr(api, "query_db", fake_query)
    result = api.ingestion_replays(limit=25)

    assert result[0]["status"] == "PUBLISHED"
    assert captured["params"] == (25,)
    assert "raw_value" not in captured["sql"]
    assert "source_offset" in captured["sql"]
