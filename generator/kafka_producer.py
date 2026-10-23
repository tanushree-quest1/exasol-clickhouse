import argparse
import json
import logging
import time
from typing import Optional
import yaml

from generator.data_generator import FinTechDataGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaProducer")


def get_kafka_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            linger_ms=10,
            batch_size=65536,
            acks=1,
        )
    except Exception as e:
        logger.error(f"Failed to create KafkaProducer: {e}")
        return None


def stream_events_to_kafka(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "paylens.auth_events",
    num_events: int = 10000,
    target_rate: Optional[int] = None,  # events per second
):
    """Streams generated auth events into Kafka topic."""
    producer = get_kafka_producer(bootstrap_servers)
    if not producer:
        raise RuntimeError("Kafka is unreachable; producer does not report dry-run results as a live stream.")

    generator = FinTechDataGenerator()
    generator.generate_dimensions(num_accounts=1000, num_cards=2000, num_merchants=200, num_devices=500)
    logger.info(f"Generating {num_events} payment auth events with labeled fraud rings...")
    # generate_events(num_events) adds its fraud-ring fixtures on top of the
    # requested normal events. Include those fixtures within the requested
    # total so the benchmark producer and both sinks handle the same row count.
    fraud_fixtures = generator.generate_events(num_events=0)
    fixture_count = len(fraud_fixtures)
    events = generator.generate_events(num_events=max(0, num_events - fixture_count))
    if len(events) > num_events:
        # Keep small smoke tests exact when the fixed fraud fixtures exceed
        # their requested row count.
        events = events[:num_events]

    logger.info(f"Publishing to Kafka topic '{topic}' (target rate: {target_rate or 'unthrottled'})...")
    start_time = time.perf_counter()
    sent_count = 0

    sleep_interval = 1.0 / target_rate if target_rate else 0.0

    for event in events:
        event_dict = {
            "event_id": event.event_id,
            "event_time": event.event_time.isoformat(),
            "event_date": event.event_date.isoformat(),
            "card_id": event.card_id,
            "merchant_id": event.merchant_id,
            "device_id": event.device_id,
            "amount": float(event.amount),
            "currency": event.currency,
            "auth_status": event.auth_status,
            "decline_reason": event.decline_reason,
            "pos_entry_mode": event.pos_entry_mode,
            "raw_payload": event.raw_payload,
            "is_fraud": event.is_fraud,
            "fraud_pattern": event.fraud_pattern,
            # Propagated only through the benchmark topic; sinks ignore this
            # field when writing the database row.
            "_benchmark_sent_at_ms": time.time() * 1000,
        }
        # Partition by merchant_id or card_id
        producer.send(topic, key=str(event.card_id), value=event_dict)
        sent_count += 1

        if sleep_interval > 0 and sent_count % 50 == 0:
            time.sleep(sleep_interval * 50)

    producer.flush()
    elapsed = time.perf_counter() - start_time
    throughput = sent_count / elapsed if elapsed > 0 else 0
    logger.info(f"Successfully published {sent_count} events in {elapsed:.2f}s ({throughput:.1f} events/s).")
    return sent_count, elapsed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PayLens Kafka Event Streamer")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="paylens.auth_events", help="Kafka topic")
    parser.add_argument("--count", type=int, default=10000, help="Number of events to publish")
    parser.add_argument("--rate", type=int, default=0, help="Target events/sec (0 = unthrottled)")
    args = parser.parse_args()

    stream_events_to_kafka(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        num_events=args.count,
        target_rate=args.rate if args.rate > 0 else None,
    )
