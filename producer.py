"""
producer.py
===========
Kafka Producer — Key-Based Partitioning
Topic   : events_topic
Keys    : user_1, user_2, user_3
Interval: setiap 5 detik
"""

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "events_topic"
USERS             = ["user_1", "user_2", "user_3"]
INTERVAL_SECONDS  = 5

# ── Event types ───────────────────────────────────────────────────────────────

EVENT_TYPES = ["login", "purchase", "logout", "view_product", "add_to_cart"]

# ── Producer ──────────────────────────────────────────────────────────────────

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    # Serialize key dan value ke bytes
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def build_event(user: str) -> dict:
    """Buat JSON payload event untuk user tertentu."""
    return {
        "user_id":    user,
        "event_type": random.choice(EVENT_TYPES),
        "amount":     round(random.uniform(10.0, 500.0), 2),
        "timestamp":  datetime.utcnow().isoformat(),
    }


def main():
    print(f"Producer started → topic: {TOPIC}")
    print(f"Sending event every {INTERVAL_SECONDS} seconds...\n")

    event_count = 0
    try:
        while True:
            # Pilih user secara berurutan (round-robin) agar tiap user kebagian
            user  = USERS[event_count % len(USERS)]
            event = build_event(user)

            future = producer.send(TOPIC, key=user, value=event)
            record  = future.get(timeout=10)  # block sampai terkirim

            event_count += 1
            print(
                f"[{event_count:04d}] key={user:8s} | "
                f"partition={record.partition} | "
                f"offset={record.offset} | "
                f"event={event['event_type']}"
            )

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total events sent: {event_count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
