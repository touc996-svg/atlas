import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from atlas.ingestion import postgres_sink as sink


def valid_event(**overrides):
    event = {
        "event_id": "evt-001",
        "event_type": "atlas.demo",
        "source": "unit-test",
        "timestamp": "2026-11-06T12:00:00Z",
        "payload": {"sequence": 1},
    }
    event.update(overrides)
    return event


class FakeCursor:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.statement = None
        self.params = None

    def execute(self, statement, params=None):
        self.statement = statement
        self.params = params

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class FakeConnection:
    closed = False

    def __init__(self, rowcount=1):
        self.fake_cursor = FakeCursor(rowcount=rowcount)

    @contextmanager
    def transaction(self):
        yield

    @contextmanager
    def cursor(self):
        yield self.fake_cursor

    def close(self):
        self.closed = True


def message(value, *, topic=sink.TOPIC, partition=2, offset=41):
    return SimpleNamespace(
        topic=topic,
        partition=partition,
        offset=offset,
        value=value,
    )


def test_parse_event_normalizes_timezone_to_utc():
    event, timestamp = sink.parse_event(json.dumps(valid_event()).encode("utf-8"))

    assert event["event_id"] == "evt-001"
    assert timestamp == datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
    assert timestamp.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    ("raw_value", "expected_reason"),
    [
        (None, "message value is null"),
        (b"\xff", "message value is not valid UTF-8"),
        (b"{broken", "message value is not valid JSON"),
        (b'{"event_id":"x","event_type":"x","source":"x","timestamp":"2026-11-06T12:00:00Z","payload":{"value":NaN}}', "JSON contains unsupported non-finite number"),
        (b"[]", "event must be a JSON object"),
        (
            json.dumps(valid_event(timestamp="not-a-date")).encode(),
            "timestamp is not a valid ISO-8601 value",
        ),
        (
            json.dumps(valid_event(timestamp="2026-11-06T12:00:00")).encode(),
            "timestamp must include a timezone offset",
        ),
        (json.dumps(valid_event(payload=[])).encode(), "payload must be a JSON object"),
        (json.dumps(valid_event(event_id=" ")).encode(), "event_id must be a non-empty string"),
        (json.dumps(valid_event(event_id="x" * 256)).encode(), "event_id exceeds 255 characters"),
    ],
)
def test_parse_event_rejects_invalid_records(raw_value, expected_reason):
    with pytest.raises(sink.EventValidationError, match=expected_reason):
        sink.parse_event(raw_value)


def test_store_event_uses_event_id_for_idempotency():
    db = FakeConnection(rowcount=1)
    record = message(b"{}")
    timestamp = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)

    inserted = sink.store_event(db, record, valid_event(), timestamp)

    assert inserted is True
    assert "ON CONFLICT (event_id) DO NOTHING" in db.fake_cursor.statement
    assert db.fake_cursor.params[0] == "evt-001"
    assert db.fake_cursor.params[3] == timestamp


def test_store_event_reports_duplicate_without_inserting_again():
    db = FakeConnection(rowcount=0)
    record = message(b"{}")
    timestamp = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)

    inserted = sink.store_event(db, record, valid_event(), timestamp)

    assert inserted is False


def test_invalid_message_is_sent_to_quarantine_table():
    db = FakeConnection(rowcount=1)

    outcome = sink.process_message(db, message(b"not-json"))

    assert outcome == "rejected"
    assert "INSERT INTO atlas_event_rejections" in db.fake_cursor.statement
    assert "ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING" in db.fake_cursor.statement
    assert db.fake_cursor.params[0:3] == (sink.TOPIC, 2, 41)
    assert db.fake_cursor.params[3] == b"not-json"


def test_valid_message_is_persisted():
    db = FakeConnection(rowcount=1)
    raw_value = json.dumps(valid_event()).encode("utf-8")

    outcome = sink.process_message(db, message(raw_value))

    assert outcome == "stored"
    assert "INSERT INTO atlas_events" in db.fake_cursor.statement


def test_duplicate_message_is_reported_as_duplicate():
    db = FakeConnection(rowcount=0)
    raw_value = json.dumps(valid_event()).encode("utf-8")

    outcome = sink.process_message(db, message(raw_value))

    assert outcome == "duplicate"


def test_kafka_offset_advances_only_past_the_processed_record():
    consumer = Mock()
    record = message(b"{}", partition=3, offset=41)

    sink.commit_message(consumer, record)

    offsets = consumer.commit.call_args.args[0]
    partition = TopicPartition(sink.TOPIC, 3)
    assert offsets[partition] == OffsetAndMetadata(42, "")


def test_database_retry_reconnects_before_reprocessing(monkeypatch):
    failed_connection = FakeConnection()
    failed_connection.closed = False
    recovered_connection = FakeConnection(rowcount=1)
    attempts = iter([failed_connection, recovered_connection])
    calls = {"connect": 0, "process": 0}

    def fake_connect(*args, **kwargs):
        calls["connect"] += 1
        return next(attempts)

    def fake_process(db, record):
        calls["process"] += 1
        if calls["process"] == 1:
            raise sink.psycopg.OperationalError("temporary database disconnect")
        return "stored"

    monkeypatch.setattr(sink.psycopg, "connect", fake_connect)
    monkeypatch.setattr(sink, "process_message", fake_process)
    monkeypatch.setattr(sink.time, "sleep", lambda _: None)

    db, outcome = sink.process_message_with_retries(
        None, "postgresql://unused", message(b"{}"), max_attempts=2
    )

    assert db is recovered_connection
    assert db.closed is False
    assert failed_connection.closed is True
    assert outcome == "stored"
    assert calls == {"connect": 2, "process": 2}


def test_startup_schema_initialization_uses_retry(monkeypatch):
    recovered_connection = FakeConnection()
    attempts = iter([
        sink.psycopg.OperationalError("database starting"),
        recovered_connection,
    ])
    calls = {"connect": 0, "initialize": 0}

    def fake_connect(*args, **kwargs):
        calls["connect"] += 1
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    def fake_initialize(db):
        calls["initialize"] += 1

    monkeypatch.setattr(sink.psycopg, "connect", fake_connect)
    monkeypatch.setattr(sink, "initialize_schema", fake_initialize)
    monkeypatch.setattr(sink.time, "sleep", lambda _: None)

    result = sink.initialize_schema_with_retries("postgresql://unused", max_attempts=2)

    assert result is recovered_connection
    assert calls == {"connect": 2, "initialize": 1}
