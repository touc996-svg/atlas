# ATLAS — Reliable Ingestion Upgrade

This upgrade hardens the Kafka-to-PostgreSQL ingestion boundary.

## Delivery and recovery guarantees

- **At-least-once delivery:** Kafka offsets are committed only after the matching PostgreSQL transaction completes. If the process stops between the database commit and the Kafka offset commit, Kafka can replay the message.
- **Idempotent valid-event writes:** `atlas_events.event_id` remains the primary key. Replayed events are handled with `ON CONFLICT (event_id) DO NOTHING`.
- **Durable poison-message quarantine:** invalid UTF-8, invalid JSON, malformed event objects, missing required fields, invalid timestamps, and non-object payloads are written to `atlas_event_rejections`. The rejection key is `(topic, kafka_partition, kafka_offset)`, so replay does not create duplicate quarantine records.
- **Partition-scoped offset commits:** the sink commits only `message.offset + 1` for the partition whose record was persisted, rather than committing the consumer's broad current position.
- **Bounded database recovery:** transient connection failures trigger up to five attempts with exponential backoff. If PostgreSQL remains unavailable, the sink exits without committing the in-flight Kafka record; it can be replayed after the sink is restarted.

This is not a distributed exactly-once transaction between Kafka and PostgreSQL. It is an at-least-once design with idempotent database effects and durable quarantine.

## Run the sink

From the ATLAS project root, with dependencies installed and environment variables loaded:

```bash
PYTHONPATH=src python -m atlas.ingestion.postgres_sink
```

The default behavior preserves the existing 30-second idle shutdown. To run continuously:

```bash
PYTHONPATH=src python -m atlas.ingestion.postgres_sink --idle-timeout 0
```

To process at most 500 records and then stop:

```bash
PYTHONPATH=src python -m atlas.ingestion.postgres_sink --max-events 500
```

You can override the broker address with `KAFKA_BOOTSTRAP_SERVERS`, using comma-separated host:port values. Log verbosity can be changed with `ATLAS_LOG_LEVEL`.

## Inspect rejected records

```sql
SELECT
    topic,
    kafka_partition,
    kafka_offset,
    error_reason,
    rejected_at
FROM atlas_event_rejections
ORDER BY rejected_at DESC
LIMIT 100;
```

The raw Kafka value is retained in `raw_value` for investigation and controlled replay.

## Automated tests

The new unit tests cover event validation, UTC timestamp normalization, idempotent writes, malformed-message quarantine, and partition-specific offset advancement. Run them with:

```bash
PYTHONPATH=src pytest -q tests/test_postgres_sink.py
```
