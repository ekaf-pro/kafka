"""
consumer.py
===========
Kafka Consumer — Consumer Group
Topic        : events_topic
Group ID     : events_consumer_group
Processing   : hitung jumlah event per user & per event type

Cara jalankan 2 consumer dalam 1 group (buka 2 terminal berbeda):
  Terminal 1: python consumer.py --consumer-id 1
  Terminal 2: python consumer.py --consumer-id 2
"""

import argparse
import json
from collections import defaultdict

from kafka import KafkaConsumer

# ── Config ────────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "events_topic"
GROUP_ID          = "events_consumer_group"

# ── Consumer ──────────────────────────────────────────────────────────────────

def main(consumer_id: str):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        # Deserialize value dari bytes ke dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print(f"Consumer [{consumer_id}] started")
    print(f"Group ID : {GROUP_ID}")
    print(f"Topic    : {TOPIC}")
    print("-" * 60)

    # Statistik
    event_count      = 0
    count_per_user   = defaultdict(int)
    count_per_type   = defaultdict(int)
    partitions_seen  = set()

    try:
        for message in consumer:
            event_count += 1
            key   = message.key
            value = message.value
            part  = message.partition
            off   = message.offset

            partitions_seen.add(part)
            count_per_user[key] += 1
            count_per_type[value.get("event_type", "unknown")] += 1

            print(
                f"[Consumer {consumer_id}] "
                f"partition={part} | offset={off} | "
                f"key={key:8s} | event={value.get('event_type'):15s} | "
                f"amount={value.get('amount')}"
            )

            # Tampilkan ringkasan setiap 10 event
            if event_count % 10 == 0:
                print(f"\n{'='*60}")
                print(f"  [Consumer {consumer_id}] SUMMARY — {event_count} events processed")
                print(f"  Partitions handled : {sorted(partitions_seen)}")
                print(f"  Events per user    : {dict(count_per_user)}")
                print(f"  Events per type    : {dict(count_per_type)}")
                print(f"{'='*60}\n")

    except KeyboardInterrupt:
        print(f"\n[Consumer {consumer_id}] Stopped.")
        print(f"Total events processed : {event_count}")
        print(f"Partitions handled     : {sorted(partitions_seen)}")
        print(f"Events per user        : {dict(count_per_user)}")
        print(f"Events per type        : {dict(count_per_type)}")
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument(
        "--consumer-id", default="1",
        help="ID consumer untuk identifikasi di log (default: 1)"
    )
    args = parser.parse_args()
    main(args.consumer_id)
