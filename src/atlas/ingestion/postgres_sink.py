import json
import os
import time

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from kafka import KafkaConsumer


TOPIC = "atlas.events"


def get_database_url():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is missing. Load your .env file.")

    return make_conninfo(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "atlas"),
        user=os.getenv("POSTGRES_USER", "atlas"),
        password=password,
    )


def main():
    database_url = get_database_url()

    with psycopg.connect(database_url) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS atlas_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                raw_event JSONB NOT NULL,
                kafka_partition INTEGER,
                kafka_offset BIGINT,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        db.commit()

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=["localhost:9092"],
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id="atlas-postgres-sink-recovery-v1",
        )

        stored = 0
        processed = 0
        last_message_at = time.monotonic()

        try:
            print("ATLAS PostgreSQL sink active. Waiting for events...")

            while time.monotonic() - last_message_at < 30:
                records = consumer.poll(timeout_ms=1000, max_records=1)

                for messages in records.values():
                    for message in messages:
                        event = message.value

                        with db.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO atlas_events (
                                    event_id, event_type, source,
                                    event_timestamp, payload, raw_event,
                                    kafka_partition, kafka_offset
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING
                            """, (
                                event["event_id"],
                                event["event_type"],
                                event["source"],
                                event["timestamp"],
                                Jsonb(event["payload"]),
                                Jsonb(event),
                                message.partition,
                                message.offset,
                            ))
                            inserted = cursor.rowcount

                        db.commit()
                        consumer.commit()

                        processed += 1
                        stored += inserted
                        last_message_at = time.monotonic()

                        if inserted:
                            print(
                                f"Stored event at partition "
                                f"{message.partition}, offset {message.offset}"
                            )

            print(f"Sink finished. Messages processed: {processed}")
            print(f"New database records stored: {stored}")

        finally:
            consumer.close()


if __name__ == "__main__":
    main()
