from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from atlas.ingestion import replay


def test_parse_datetime_bound_normalizes_timezone_to_utc():
    result = replay.parse_datetime_bound("2026-11-08T07:00:00-05:00")
    assert result == datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


def test_parse_datetime_bound_rejects_timezone_naive_values():
    with pytest.raises(ValueError, match="include a timezone"):
        replay.parse_datetime_bound("2026-11-08T07:00:00")


def test_date_window_requires_since_before_until():
    bound = datetime(2026, 11, 8, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="earlier than"):
        replay.validate_date_window(bound, bound)


def test_candidate_query_excludes_published_and_recently_active_replays():
    since = datetime(2026, 11, 1, tzinfo=timezone.utc)
    until = datetime(2026, 11, 8, tzinfo=timezone.utc)
    sql, params = replay.build_candidate_query(since, until)

    assert "atlas_event_rejections" in sql
    assert "status='PUBLISHED'" in sql
    assert "status='PUBLISHING'" in sql
    assert "INTERVAL '10 minutes'" in sql
    assert "r.rejected_at >= %s" in sql
    assert "r.rejected_at < %s" in sql
    assert params == (since, until)


def test_publish_raw_value_preserves_original_bytes_and_replay_headers():
    producer = Mock()
    metadata = SimpleNamespace(partition=2, offset=900)
    producer.send.return_value.get.return_value = metadata
    original = b'{"event_id":"evt-1","payload":{"message":"retained"}}'
    headers = [("atlas-replay-id", b"replay-1")]

    result = replay.publish_raw_value(
        producer,
        target_topic="atlas.events",
        raw_value=original,
        headers=headers,
        max_attempts=1,
    )

    assert result is metadata
    producer.send.assert_called_once_with(
        "atlas.events", value=original, headers=headers
    )


def test_publish_raw_value_retries_then_succeeds():
    producer = Mock()
    metadata = SimpleNamespace(partition=1, offset=45)
    future = Mock()
    future.get.return_value = metadata
    producer.send.side_effect = [RuntimeError("transient"), future]
    sleep = Mock()

    result = replay.publish_raw_value(
        producer,
        target_topic="atlas.events",
        raw_value=b"{}",
        headers=[],
        max_attempts=2,
        retry_delay=0.25,
        sleep=sleep,
    )

    assert result is metadata
    assert producer.send.call_count == 2
    sleep.assert_called_once_with(0.25)


def test_publish_raw_value_stops_at_attempt_limit():
    producer = Mock()
    producer.send.side_effect = RuntimeError("broker unavailable")
    sleep = Mock()

    with pytest.raises(RuntimeError, match="broker unavailable"):
        replay.publish_raw_value(
            producer,
            target_topic="atlas.events",
            raw_value=b"{}",
            headers=[],
            max_attempts=2,
            retry_delay=0,
            sleep=sleep,
        )

    assert producer.send.call_count == 2
    sleep.assert_called_once_with(0)


def test_replay_schema_creates_audit_table_and_indexes():
    class Cursor:
        def __init__(self, statements):
            self.statements = statements

        def execute(self, statement, params=None):
            self.statements.append(" ".join(statement.split()))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    class Transaction:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    class Database:
        def __init__(self):
            self.statements = []

        def transaction(self):
            return Transaction()

        def cursor(self):
            return Cursor(self.statements)

    db = Database()
    replay.initialize_replay_schema(db)
    combined = " ".join(db.statements)

    assert "CREATE TABLE IF NOT EXISTS public.atlas_event_replay_audit" in combined
    assert "PUBLISHING" in combined and "PUBLISHED" in combined and "FAILED" in combined
    assert "idx_atlas_replay_started" in combined
    assert "idx_atlas_replay_status" in combined
    assert "idx_atlas_replay_source" in combined
