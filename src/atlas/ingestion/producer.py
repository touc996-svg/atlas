import argparse
import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "atlas.events"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    try:
        for number in range(1, args.count + 1):
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "atlas.demo",
                "source": "atlas-python-producer",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": {
                    "sequence": number,
                    "message": f"ATLAS streaming event {number}",
                },
            }

            metadata = producer.send(TOPIC, value=event).get(timeout=10)
            print(
                f"Published event {number}/{args.count} "
                f"to partition {metadata.partition}, offset {metadata.offset}"
            )
            time.sleep(args.interval)
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
