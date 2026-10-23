import json
import logging
import time
import uuid
from datetime import date
from typing import List, Optional

from consumers.timestamps import parse_utc_timestamp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("ClickHouseConsumer")

COLUMNS = [
    "event_id",
    "event_time",
    "event_date",
    "card_id",
    "merchant_id",
    "device_id",
    "amount",
    "currency",
    "auth_status",
    "decline_reason",
    "pos_entry_mode",
    "raw_payload",
]


class ClickHouseConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "paylens.auth_events",
        group_id: str = "paylens-clickhouse-group",
        ch_host: str = "localhost",
        ch_port: int = 8123,
        ch_user: str = "default",
        ch_password: str = "",
        ch_database: str = "paylens",
        batch_size: int = 10000,
        flush_interval_sec: float = 1.0,
        table: str = "auth_events",
        ready_event=None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.table = table
        self.ready_event = ready_event

        self.ch_params = {
            "host": ch_host,
            "port": ch_port,
            "username": ch_user,
            "password": ch_password,
            "database": ch_database,
        }
        self.client = None
        self._init_client()

    def _init_client(self):
        try:
            import clickhouse_connect
            self.client = clickhouse_connect.get_client(**self.ch_params)
            logger.info("Connected to ClickHouse successfully.")
        except Exception as e:
            raise RuntimeError(f"Live ClickHouse connection required for Kafka sink: {e}") from e

    def insert_batch(self, batch: List[dict]) -> float:
        """Inserts a batch of event dicts into ClickHouse and returns insertion time in seconds."""
        if not batch:
            return 0.0

        rows = []
        for msg in batch:
            rows.append((
                uuid.UUID(msg["event_id"]) if isinstance(msg["event_id"], str) else msg["event_id"],
                parse_utc_timestamp(msg["event_time"]),
                date.fromisoformat(msg["event_date"]),
                msg["card_id"],
                msg["merchant_id"],
                msg["device_id"],
                msg["amount"],
                msg["currency"],
                msg["auth_status"],
                msg["decline_reason"],
                msg["pos_entry_mode"],
                msg["raw_payload"],
            ))

        start = time.perf_counter()
        self.client.insert(self.table, rows, column_names=COLUMNS)
        elapsed = time.perf_counter() - start
        return elapsed

    def start_consuming(self, max_messages: Optional[int] = None):
        """Streams from Kafka and flushes in batches."""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=120000,
            )
            logger.info(f"Subscribed to topic '{self.topic}'. Starting consumption loop...")
        except Exception as e:
            logger.error(f"Cannot connect to Kafka broker: {e}")
            raise RuntimeError("Live Kafka connection required; no dry-run consumption is reported.") from e

        if self.ready_event:
            self.ready_event.set()
        batch = []
        total_processed = 0
        last_flush = time.time()
        insert_seconds = 0.0
        batch_durations = []
        end_to_end_ms = []
        first_message_at = None
        last_message_at = None
        try:
            for msg in consumer:
                if max_messages and total_processed + len(batch) >= max_messages:
                    break
                batch.append(msg.value)
                first_message_at = first_message_at or time.perf_counter()
                last_message_at = time.perf_counter()
                is_limit = bool(max_messages and total_processed + len(batch) >= max_messages)
                if len(batch) >= self.batch_size or is_limit or (time.time() - last_flush) >= self.flush_interval_sec:
                    duration = self.insert_batch(batch)
                    inserted_at_ms = time.time() * 1000
                    sent_times = [row.get("_benchmark_sent_at_ms") for row in batch]
                    end_to_end_ms.extend(inserted_at_ms - ts for ts in sent_times if isinstance(ts, (int, float)))
                    total_processed += len(batch)
                    insert_seconds += duration
                    batch_durations.append(duration)
                    consumer.commit()
                    logger.info(f"Inserted batch of {len(batch)} rows in {duration*1000:.1f}ms ({len(batch)/duration if duration else 0:.0f} rows/s). Total: {total_processed}")
                    batch.clear()
                    last_flush = time.time()
                    if max_messages and total_processed >= max_messages:
                        break
        finally:
            if batch:
                duration = self.insert_batch(batch)
                inserted_at_ms = time.time() * 1000
                end_to_end_ms.extend(inserted_at_ms - row["_benchmark_sent_at_ms"] for row in batch if isinstance(row.get("_benchmark_sent_at_ms"), (int, float)))
                total_processed += len(batch)
                insert_seconds += duration
                batch_durations.append(duration)
                consumer.commit()
            consumer.close()
            self.client.close()

        def percentile(values, p):
            if not values:
                return None
            ordered = sorted(values)
            return round(ordered[min(len(ordered)-1, int((len(ordered)-1)*p))], 3)
        return {
            "rows_consumed": total_processed,
            "batch_count": len(batch_durations),
            "sink_insert_seconds": round(insert_seconds, 6),
            "sink_rows_per_second": round(total_processed / insert_seconds, 2) if insert_seconds else None,
            "consume_window_seconds": round(last_message_at - first_message_at, 6) if first_message_at and last_message_at else None,
            "end_to_end_latency_ms": {"p50": percentile(end_to_end_ms, .50), "p95": percentile(end_to_end_ms, .95), "p99": percentile(end_to_end_ms, .99)},
            "errors": 0,
        }
