import argparse
import json
import os
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from consumers.timestamps import exasol_timestamp_text, parse_utc_timestamp
from generator.data_generator import FinTechDataGenerator


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.yaml")
    try:
        import yaml
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except ImportError:
        # Fallback default configuration when pyyaml is not yet installed
        config = {
            "scale": {"tier": "test", "test_rows": {"auth_events": 100000}},
            "clickhouse": {
                "host": "localhost",
                "port": 8123,
                "user": "default",
                "password": "",
                "database": "paylens",
            },
            "exasol": {
                "dsn": "localhost:8563",
                "user": "sys",
                "password": "",
                "schema": "PAYLENS",
                "allow_local_self_signed": True,
            },
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "topics": {"auth_events": "paylens.auth_events"},
            },
        }
    config["exasol"]["password"] = os.environ.get(
        "EXASOL_PASSWORD", config["exasol"]["password"]
    )
    return config


class InsertBenchmarkRunner:
    def __init__(self, config: dict):
        self.config = config
        self.ch_client = None
        self.exasol_conn = None
        self._init_connections()

    def _init_connections(self):
        # ClickHouse
        try:
            import clickhouse_connect
            ch_cfg = self.config["clickhouse"]
            self.ch_client = clickhouse_connect.get_client(
                host=ch_cfg["host"],
                port=ch_cfg["port"],
                username=ch_cfg["user"],
                password=ch_cfg["password"],
                database=ch_cfg["database"],
            )
            print("[+] Connected to ClickHouse.")
        except Exception as e:
            print(f"[-] ClickHouse connection failed: {e}", file=sys.stderr)
            raise RuntimeError("Live ClickHouse connection is required; no simulated metrics are generated.") from e

        # Exasol
        try:
            import pyexasol
            ex_cfg = self.config["exasol"]
            connect_options = {}
            if (
                ex_cfg.get("allow_local_self_signed")
                and ex_cfg["dsn"].startswith(("localhost:", "127.0.0.1:"))
            ):
                # Exasol Personal's local deployment uses a self-signed cert.
                # Never disable certificate validation for a remote DSN.
                connect_options["websocket_sslopt"] = {"cert_reqs": ssl.CERT_NONE}
            self.exasol_conn = pyexasol.connect(
                dsn=ex_cfg["dsn"],
                user=ex_cfg["user"],
                password=ex_cfg["password"],
                schema=ex_cfg["schema"],
                autocommit=True,
                **connect_options,
            )
            print("[+] Connected to Exasol.")
        except Exception as e:
            print(f"[-] Exasol connection failed: {e}", file=sys.stderr)
            raise RuntimeError("Live Exasol connection is required; no simulated metrics are generated.") from e

    def _load_dimensions(self, generator: FinTechDataGenerator) -> None:
        """Load the generated dimensions before timing the fact-table ingest."""
        if self.ch_client:
            self.ch_client.insert(
                "accounts",
                [(
                    row.account_id, row.customer_id, row.account_type, row.currency,
                    row.credit_limit, row.current_balance, row.status, row.created_at,
                ) for row in generator.accounts],
                column_names=["account_id", "customer_id", "account_type", "currency", "credit_limit", "current_balance", "status", "created_at"],
            )
            self.ch_client.insert(
                "cards",
                [(
                    row.card_id, row.account_id, row.card_number_hash, row.card_type,
                    row.expiry_date, row.status, row.is_contactless, row.created_at,
                ) for row in generator.cards],
                column_names=["card_id", "account_id", "card_number_hash", "card_type", "expiry_date", "status", "is_contactless", "created_at"],
            )
            self.ch_client.insert(
                "merchants",
                [(
                    row.merchant_id, row.merchant_name, row.mcc, row.risk_tier,
                    row.city, row.country, row.created_at,
                ) for row in generator.merchants],
                column_names=["merchant_id", "merchant_name", "mcc", "risk_tier", "city", "country", "created_at"],
            )
            self.ch_client.insert(
                "devices",
                [(
                    row.device_id, row.device_fingerprint, row.device_type,
                    row.os, row.ip_address, row.first_seen_at,
                ) for row in generator.devices],
                column_names=["device_id", "device_fingerprint", "device_type", "os", "ip_address", "first_seen_at"],
            )

        if self.exasol_conn:
            self.exasol_conn.import_from_iterable(
                [(
                    row.account_id, row.customer_id, row.account_type, row.currency,
                    row.credit_limit, row.current_balance, row.status, row.created_at,
                ) for row in generator.accounts], "ACCOUNTS"
            )
            self.exasol_conn.import_from_iterable(
                [(
                    row.card_id, row.account_id, row.card_number_hash, row.card_type,
                    row.expiry_date, row.status, row.is_contactless, row.created_at,
                ) for row in generator.cards], "CARDS"
            )
            self.exasol_conn.import_from_iterable(
                [(
                    row.merchant_id, row.merchant_name, row.mcc, row.risk_tier,
                    row.city, row.country, row.created_at,
                ) for row in generator.merchants], "MERCHANTS"
            )
            self.exasol_conn.import_from_iterable(
                [(
                    row.device_id, row.device_fingerprint, row.device_type,
                    row.os, row.ip_address, row.first_seen_at,
                ) for row in generator.devices], "DEVICES"
            )

    def benchmark_bulk_insert(
        self,
        total_rows: int = 100000,
        batch_size: int = 10000,
    ) -> Dict[str, dict]:
        """Runs bulk insert test on both engines across batches."""
        print(f"\n==================================================================")
        print(f"  BULK INSERT BENCHMARK: {total_rows:,} rows (Batch Size: {batch_size:,})")
        print(f"==================================================================")

        generator = FinTechDataGenerator(seed=42)
        generator.generate_dimensions(num_accounts=1000, num_cards=2000, num_merchants=200, num_devices=500)
        self._load_dimensions(generator)
        events = generator.generate_events(
            num_events=total_rows,
            start_time=datetime.now(timezone.utc) - timedelta(hours=12),
        )
        actual_rows = len(events)
        print(f"Loading {actual_rows:,} total events, including injected fraud patterns.")

        num_batches = (actual_rows + batch_size - 1) // batch_size
        batches = [events[i * batch_size : (i + 1) * batch_size] for i in range(num_batches)]

        # 1. Benchmarking ClickHouse
        ch_latencies = []
        ch_start = time.perf_counter()
        for batch in batches:
            b_start = time.perf_counter()
            rows = []
            for event in batch:
                row = list(event.to_ch_row())
                row[1] = parse_utc_timestamp(event.event_time)
                rows.append(tuple(row))
            self.ch_client.insert(
                "auth_events",
                rows,
                column_names=[
                    "event_id", "event_time", "event_date", "card_id", "merchant_id",
                    "device_id", "amount", "currency", "auth_status", "decline_reason",
                    "pos_entry_mode", "raw_payload"
                ]
            )
            ch_latencies.append((time.perf_counter() - b_start) * 1000.0)
        ch_total_time = time.perf_counter() - ch_start
        ch_throughput = actual_rows / ch_total_time if ch_total_time > 0 else 0

        # 2. Benchmarking Exasol
        ex_latencies = []
        ex_start = time.perf_counter()
        for batch in batches:
            b_start = time.perf_counter()
            rows = []
            for event in batch:
                row = list(event.to_exasol_row())
                row[1] = exasol_timestamp_text(event.event_time)
                rows.append(tuple(row))
            self.exasol_conn.import_from_iterable(rows, "AUTH_EVENTS")
            ex_latencies.append((time.perf_counter() - b_start) * 1000.0)
        ex_total_time = time.perf_counter() - ex_start
        ex_throughput = actual_rows / ex_total_time if ex_total_time > 0 else 0

        # Calculations
        def calc_stats(latencies):
            s = sorted(latencies)
            p50 = s[int(len(s) * 0.5)] if s else 0
            p95 = s[int(len(s) * 0.95)] if s else 0
            return p50, p95

        ch_p50, ch_p95 = calc_stats(ch_latencies)
        ex_p50, ex_p95 = calc_stats(ex_latencies)

        results = {
            "batch_size": batch_size,
            "requested_normal_rows": total_rows,
            "total_rows": actual_rows,
            "clickhouse": {
                "total_time_sec": round(ch_total_time, 3),
                "throughput_rows_per_sec": round(ch_throughput, 1),
                "batch_p50_ms": round(ch_p50, 2),
                "batch_p95_ms": round(ch_p95, 2),
            },
            "exasol": {
                "total_time_sec": round(ex_total_time, 3),
                "throughput_rows_per_sec": round(ex_throughput, 1),
                "batch_p50_ms": round(ex_p50, 2),
                "batch_p95_ms": round(ex_p95, 2),
            },
        }

        self.print_comparison_table(results)
        return results

    def print_comparison_table(self, res: dict):
        ch = res["clickhouse"]
        ex = res["exasol"]
        speedup = ch["throughput_rows_per_sec"] / ex["throughput_rows_per_sec"] if ex["throughput_rows_per_sec"] > 0 else 1.0
        winner = "ClickHouse" if speedup >= 1.0 else "Exasol"
        ratio = speedup if speedup >= 1.0 else (1.0 / speedup)

        print("\n| Metric | ClickHouse | Exasol | Winner |")
        print("|---|---|---|---|")
        print(f"| **Throughput (rows/s)** | **{ch['throughput_rows_per_sec']:,}** | **{ex['throughput_rows_per_sec']:,}** | 🏆 **{winner}** ({ratio:.2f}x) |")
        print(f"| **Total Ingest Time** | {ch['total_time_sec']}s | {ex['total_time_sec']}s | {winner} |")
        print(f"| **Batch Latency (p50)** | {ch['batch_p50_ms']} ms | {ex['batch_p50_ms']} ms | - |")
        print(f"| **Batch Latency (p95)** | {ch['batch_p95_ms']} ms | {ex['batch_p95_ms']} ms | - |")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PayLens Ingestion Benchmark")
    parser.add_argument("--rows", type=int, default=100000, help="Total rows to benchmark")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size")
    args = parser.parse_args()

    cfg = load_config()
    runner = InsertBenchmarkRunner(cfg)
    results = runner.benchmark_bulk_insert(total_rows=args.rows, batch_size=args.batch_size)

    # Save to results
    os.makedirs("results", exist_ok=True)
    results_dir = os.environ.get("PAYLENS_RESULTS_DIR", "results")
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "insert_benchmark_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[+] Benchmark results saved to {results_dir}/insert_benchmark_results.json")
