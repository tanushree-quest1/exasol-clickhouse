"""Paired Kafka-to-ClickHouse/Exasol ingest benchmark on isolated tables."""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from benchmark.physical_design_bench import connect, exa_rows
from consumers.ch_consumer import ClickHouseConsumer
from consumers.exasol_consumer import ExasolConsumer
from generator.kafka_producer import stream_events_to_kafka


def run(rows: int, batch_size: int, bootstrap: str = "localhost:9092", target_rate: int | None = None) -> dict[str, Any]:
    from kafka.admin import KafkaAdminClient, NewTopic

    run_key = uuid.uuid4().hex[:10]
    topic = f"paylens.kafka_bench.{run_key}"
    ch_table = f"kafka_bench_{run_key}"
    exa_table = f"KAFKA_BENCH_{run_key.upper()}"
    ch, exa = connect()
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id=f"paylens-bench-{run_key}")
    ch_ready, exa_ready = threading.Event(), threading.Event()
    executor = ThreadPoolExecutor(max_workers=2)
    futures = []
    try:
        # kafka-python-ng's AdminClient waits for the controller response and
        # returns the protocol response directly. Some kafka-python versions
        # returned a mapping of futures here; don't assume that shape.
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
            timeout_ms=30000,
        )
        ch.command(f"""CREATE TABLE paylens.{ch_table} (
            event_id UUID, event_time DateTime64(3, 'UTC'), event_date Date,
            card_id UInt64, merchant_id UInt64, device_id UInt64, amount Decimal64(2),
            currency LowCardinality(String), auth_status LowCardinality(String),
            decline_reason LowCardinality(String), pos_entry_mode LowCardinality(String), raw_payload String
        ) ENGINE=MergeTree PARTITION BY event_date
          ORDER BY (card_id, event_time)""")
        exa_rows(exa, f"""CREATE TABLE PAYLENS.{exa_table} (
            EVENT_ID VARCHAR(36), EVENT_TIME TIMESTAMP, EVENT_DATE DATE,
            CARD_ID DECIMAL(18,0), MERCHANT_ID DECIMAL(18,0), DEVICE_ID DECIMAL(18,0),
            AMOUNT DECIMAL(18,2), CURRENCY CHAR(3), AUTH_STATUS VARCHAR(20),
            DECLINE_REASON VARCHAR(50), POS_ENTRY_MODE VARCHAR(20), RAW_PAYLOAD VARCHAR(2000000),
            DISTRIBUTE BY CARD_ID,
            PARTITION BY EVENT_DATE)""")

        ch_consumer = ClickHouseConsumer(
            bootstrap_servers=bootstrap, topic=topic,
            group_id=f"paylens-ch-bench-{run_key}", batch_size=batch_size,
            table=ch_table, ready_event=ch_ready,
        )
        exa_consumer = ExasolConsumer(
            bootstrap_servers=bootstrap, topic=topic,
            group_id=f"paylens-exa-bench-{run_key}", batch_size=batch_size,
            table=exa_table, ready_event=exa_ready,
        )
        futures = [
            executor.submit(ch_consumer.start_consuming, rows),
            executor.submit(exa_consumer.start_consuming, rows),
        ]
        if not ch_ready.wait(45) or not exa_ready.wait(45):
            raise TimeoutError("Kafka consumers did not subscribe within 45 seconds")

        producer_started = time.perf_counter()
        sent, producer_elapsed = stream_events_to_kafka(
            bootstrap_servers=bootstrap, topic=topic, num_events=rows, target_rate=target_rate,
        )
        producer_elapsed = max(producer_elapsed, time.perf_counter() - producer_started)
        ch_metrics, exa_metrics = [f.result(timeout=300) for f in futures]
        ch_count = int(ch.query(f"SELECT count() FROM paylens.{ch_table}").result_rows[0][0])
        exa_count = int(exa_rows(exa, f"SELECT COUNT(*) FROM PAYLENS.{exa_table}")[0][0])
        return {
            "run_type": "live",
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows_requested": rows,
            "batch_size": batch_size,
            "target_rate_rows_per_second": target_rate,
            "topic": topic,
            "producer": {"rows_sent": sent, "elapsed_seconds": round(producer_elapsed, 6),
                         "rows_per_second": round(sent / producer_elapsed, 2) if producer_elapsed else None,
                         "target_rate_rows_per_second": target_rate},
            "clickhouse_sink": {**ch_metrics, "row_count": ch_count},
            "exasol_sink": {**exa_metrics, "row_count": exa_count},
            "correctness": {"producer_sent_requested": sent == rows,
                            "clickhouse_count_matches": ch_count == rows,
                            "exasol_count_matches": exa_count == rows,
                            "sink_counts_match": ch_count == exa_count},
            "notes": [
                "Both consumers independently read the same single-partition topic using separate consumer groups.",
                "End-to-end latency is measured from producer enqueue timestamp to sink batch completion.",
                "Sink throughput is rows divided by database insert time; consume window excludes producer setup and includes broker delivery.",
                "Single broker, single-node Exasol Nano, and one partition; not a distributed scaling result.",
            ],
        }
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        try:
            ch.command(f"DROP TABLE IF EXISTS paylens.{ch_table}")
        except Exception:
            pass
        try:
            exa_rows(exa, f"DROP TABLE IF EXISTS PAYLENS.{exa_table}")
        except Exception:
            pass
        try:
            admin.delete_topics([topic], timeout_ms=10000)
        except Exception:
            pass
        admin.close()
        ch.close()
        exa.close()


def parse_sweep_values(value: str, *, allow_zero: bool = False) -> list[int]:
    try:
        values = [int(item.strip()) for item in value.split(",")]
    except ValueError as exc:
        raise ValueError("Sweep values must be comma-separated integers") from exc
    minimum = 0 if allow_zero else 1
    if not values or any(item < minimum for item in values):
        raise ValueError(f"Sweep values must be at least {minimum}")
    if len(set(values)) != len(values):
        raise ValueError("Sweep values must not contain duplicates")
    return values


def run_sweep(
    rows: int,
    batch_sizes: list[int],
    rates: list[int],
    bootstrap: str = "localhost:9092",
) -> dict[str, Any]:
    scenarios = []
    for batch_size in batch_sizes:
        for rate in rates:
            result = run(rows, batch_size, bootstrap, rate or None)
            result["scenario"] = {"batch_size": batch_size, "target_rate_rows_per_second": rate}
            scenarios.append(result)
    return {
        "run_type": "live_sweep",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows_per_scenario": rows,
        "batch_sizes": batch_sizes,
        "target_rates_rows_per_second": rates,
        "scenarios": scenarios,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Paired Kafka sink benchmark")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=0, help="producer target rows/s; 0 is unthrottled")
    parser.add_argument("--batch-sizes", default=None, help="comma-separated Kafka sink batch-size sweep")
    parser.add_argument("--rates", default=None, help="comma-separated producer rows/s sweep (0 means unthrottled)")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.rows < 1 or args.batch_size < 1 or args.rate < 0:
        parser.error("rows and batch-size must be positive; rate must be nonnegative")
    if args.batch_sizes or args.rates:
        try:
            batch_sizes = parse_sweep_values(args.batch_sizes or str(args.batch_size))
            rates = parse_sweep_values(args.rates or str(args.rate), allow_zero=True)
        except ValueError as exc:
            parser.error(str(exc))
        result = run_sweep(args.rows, batch_sizes, rates, args.bootstrap)
    else:
        result = run(args.rows, args.batch_size, args.bootstrap, args.rate or None)
    out = args.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR", "results"), "kafka_benchmark_results.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"[+] Kafka benchmark results saved to {out}")


if __name__ == "__main__":
    main()
