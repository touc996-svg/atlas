import argparse
import json

from kafka import KafkaConsumer

TOPIC = "atlas.events"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=20000,
        group_id="atlas-demo-consumer",
    )

    received = 0
    try:
        print(f"Listening for events on {TOPIC}...")
        for message in consumer:
            print(json.dumps(message.value, indent=2))
            received += 1
            if received >= args.count:
                break
        print(f"\nReceived {received} events.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
