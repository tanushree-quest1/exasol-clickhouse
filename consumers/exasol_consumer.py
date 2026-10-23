import json
import logging
import os
import ssl
import time
from typing import List, Optional

from consumers.timestamps import exasol_timestamp_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("ExasolConsumer")

EXASOL_COLUMNS = [
    "EVENT_ID",
    "EVENT_TIME",
    "EVENT_DATE",
    "CARD_ID",
    "MERCHANT_ID",
    "DEVICE_ID",
    "AMOUNT",
    "CURRENCY",
    "AUTH_STATUS",
    "DECLINE_REASON",
    "POS_ENTRY_MODE",
    "RAW_PAYLOAD",
]


class ExasolConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "paylens.auth_events",
        group_id: str = "paylens-exasol-group",
        dsn: str = "localhost:8563",
        user: str = "sys",
        password: Optional[str] = None,
        schema: str = "PAYLENS",
        batch_size: int = 10000,
        flush_interval_sec: float = 1.0,
        table: str = "AUTH_EVENTS",
        ready_event=None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.table = table
        self.ready_event = ready_event

        self.exasol_params = {
            "dsn": dsn,
            "user": user,
            "password": password if password is not None else os.environ.get("EXASOL_PASSWORD", ""),
            "schema": schema,
            "autocommit": True,
        }
        self.conn = None
        self._init_connection()

    def _init_connection(self):
        try:
            import pyexasol
            options = {}
            if self.exasol_params["dsn"].startswith(("localhost:", "127.0.0.1:")):
                options["websocket_sslopt"] = {"cert_reqs": ssl.CERT_NONE}
            self.conn = pyexasol.connect(**self.exasol_params, **options)
            logger.info("Connected to Exasol successfully.")
        except Exception as e:
            raise RuntimeError(f"Live Exasol connection required for Kafka sink: {e}") from e

    def insert_batch(self, batch: List[dict]) -> float:
        """Inserts batch into Exasol using high-performance HTTP transport (import_from_iterable)."""
        if not batch:
            return 0.0

        rows = []
        for msg in batch:
            rows.append((
                str(msg["event_id"]),
                exasol_timestamp_text(msg["event_time"]),
                str(msg["event_date"]),
                msg["card_id"],
                msg["merchant_id"],
                msg["device_id"],
                float(msg["amount"]),
                msg["currency"],
                msg["auth_status"],
                msg["decline_reason"],
                msg["pos_entry_mode"],
                msg["raw_payload"],
            ))

        start = time.perf_counter()
        # High-performance parallel HTTP import
        self.conn.import_from_iterable(rows, self.table)
        elapsed = time.perf_counter() - start
        return elapsed

    def start_consuming(self, max_messages: Optional[int] = None):
        """Streams from Kafka and flushes in batches to Exasol."""
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
                    end_to_end_ms.extend(inserted_at_ms - row["_benchmark_sent_at_ms"] for row in batch if isinstance(row.get("_benchmark_sent_at_ms"), (int, float)))
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
            self.conn.close()

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
