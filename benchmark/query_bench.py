import argparse
import datetime
import decimal
import json
import math
import os
import platform
import ssl
import sys
import time
from importlib.metadata import PackageNotFoundError, version as package_version
from typing import Any, Dict, List, Tuple


# =====================================================================
# Benchmark Queries for Both Dialects
# =====================================================================

QUERIES = {
    # -----------------------------------------------------------------
    # Q1: Card Velocity (Point lookup + rolling 24h window aggregation)
    # -----------------------------------------------------------------
    "Q1_card_velocity": {
        "description": "Point lookup: rolling 24h spend & swipe count for a single card",
        "clickhouse": """
            SELECT
                card_id,
                count() AS swipe_count_24h,
                sum(amount) AS total_spend_24h,
                avg(amount) AS avg_spend_24h,
                max(event_time) AS latest_swipe
            FROM paylens.auth_events
            WHERE card_id = {card_id}
              AND event_time >= now() - INTERVAL 1 DAY
            GROUP BY card_id;
        """,
        "exasol": """
            SELECT
                card_id,
                count(*) AS swipe_count_24h,
                sum(amount) AS total_spend_24h,
                avg(amount) AS avg_spend_24h,
                max(event_time) AS latest_swipe
            FROM PAYLENS.auth_events
            WHERE card_id = {card_id}
              AND event_time >= ADD_DAYS(CURRENT_TIMESTAMP, -1)
            GROUP BY card_id;
        """,
    },

    # -----------------------------------------------------------------
    # Q2: Merchant Analytics Rollup (Large aggregation over millions of rows)
    # -----------------------------------------------------------------
    "Q2_merchant_rollup": {
        "description": "Heavy aggregation: volume, decline rates & avg spend grouped by MCC & Country",
        "clickhouse": """
            SELECT
                m.country,
                m.mcc,
                count() AS total_txns,
                sum(e.amount) AS total_volume,
                round(avg(e.amount), 2) AS avg_ticket_size,
                countIf(e.auth_status = 'DECLINED') * 100.0 / count() AS decline_rate_pct
            FROM paylens.auth_events AS e
            JOIN paylens.merchants AS m ON e.merchant_id = m.merchant_id
            GROUP BY m.country, m.mcc
            ORDER BY total_volume DESC
            LIMIT 20;
        """,
        "exasol": """
            SELECT
                m.country,
                m.mcc,
                count(*) AS total_txns,
                sum(e.amount) AS total_volume,
                round(avg(e.amount), 2) AS avg_ticket_size,
                sum(CASE WHEN e.auth_status = 'DECLINED' THEN 1.0 ELSE 0.0 END) * 100.0 / count(*) AS decline_rate_pct
            FROM PAYLENS.auth_events AS e
            JOIN PAYLENS.merchants AS m ON e.merchant_id = m.merchant_id
            GROUP BY m.country, m.mcc
            ORDER BY total_volume DESC
            LIMIT 20;
        """,
    },

    # -----------------------------------------------------------------
    # Q3: 5-Table Star Join (Complex Fraud-Ring Detection)
    # Identifies devices shared across > 5 different cards with high volume
    # -----------------------------------------------------------------
    "Q3_fraud_ring_5table_join": {
        "description": "5-Table Star Join: Shared device fraud ring detection across events, cards, accounts, merchants, devices",
        "clickhouse": """
            SELECT
                d.device_id,
                d.device_fingerprint,
                d.ip_address,
                count(DISTINCT e.card_id) AS distinct_cards_used,
                count() AS total_attempts,
                sum(e.amount) AS total_exposed_volume,
                m.merchant_name,
                m.city
            FROM paylens.auth_events AS e
            JOIN paylens.devices AS d ON e.device_id = d.device_id
            JOIN paylens.cards AS c ON e.card_id = c.card_id
            JOIN paylens.accounts AS a ON c.account_id = a.account_id
            JOIN paylens.merchants AS m ON e.merchant_id = m.merchant_id
            WHERE e.auth_status = 'APPROVED'
            GROUP BY d.device_id, d.device_fingerprint, d.ip_address, m.merchant_name, m.city
            HAVING count(DISTINCT e.card_id) >= 5
            ORDER BY distinct_cards_used DESC, total_exposed_volume DESC
            LIMIT 50;
        """,
        "exasol": """
            SELECT
                d.device_id,
                d.device_fingerprint,
                d.ip_address,
                count(DISTINCT e.card_id) AS distinct_cards_used,
                count(*) AS total_attempts,
                sum(e.amount) AS total_exposed_volume,
                m.merchant_name,
                m.city
            FROM PAYLENS.auth_events AS e
            JOIN PAYLENS.devices AS d ON e.device_id = d.device_id
            JOIN PAYLENS.cards AS c ON e.card_id = c.card_id
            JOIN PAYLENS.accounts AS a ON c.account_id = a.account_id
            JOIN PAYLENS.merchants AS m ON e.merchant_id = m.merchant_id
            WHERE e.auth_status = 'APPROVED'
            GROUP BY d.device_id, d.device_fingerprint, d.ip_address, m.merchant_name, m.city
            HAVING count(DISTINCT e.card_id) >= 5
            ORDER BY distinct_cards_used DESC, total_exposed_volume DESC
            LIMIT 50;
        """,
    },

    # Q3 split controls: isolate the five-table join from the high-cardinality
    # string-keyed aggregation in the legacy fraud-ring query above.
    "Q3a_five_table_join_only": {
        "description": "Five-table join with scalar aggregates and no high-cardinality GROUP BY",
        "clickhouse": """
            SELECT count(), sum(e.amount), sum(d.device_id), sum(c.account_id), sum(a.customer_id), sum(m.merchant_id)
            FROM paylens.auth_events AS e
            JOIN paylens.devices AS d ON e.device_id = d.device_id
            JOIN paylens.cards AS c ON e.card_id = c.card_id
            JOIN paylens.accounts AS a ON c.account_id = a.account_id
            JOIN paylens.merchants AS m ON e.merchant_id = m.merchant_id
            WHERE e.auth_status = 'APPROVED';
        """,
        "exasol": """
            SELECT count(*), sum(e.amount), sum(d.device_id), sum(c.account_id), sum(a.customer_id), sum(m.merchant_id)
            FROM PAYLENS.auth_events AS e
            JOIN PAYLENS.devices AS d ON e.device_id = d.device_id
            JOIN PAYLENS.cards AS c ON e.card_id = c.card_id
            JOIN PAYLENS.accounts AS a ON c.account_id = a.account_id
            JOIN PAYLENS.merchants AS m ON e.merchant_id = m.merchant_id
            WHERE e.auth_status = 'APPROVED';
        """,
    },
    "Q3b_integer_id_aggregation": {
        "description": "High-cardinality fraud aggregation grouped by integer device and merchant IDs",
        "clickhouse": """
            SELECT e.device_id, e.merchant_id,
                uniqExact(e.card_id) AS distinct_cards_used,
                count() AS total_attempts,
                sum(e.amount) AS total_exposed_volume
            FROM paylens.auth_events AS e
            WHERE e.auth_status = 'APPROVED'
            GROUP BY e.device_id, e.merchant_id
            HAVING distinct_cards_used >= 5
            ORDER BY distinct_cards_used DESC, total_exposed_volume DESC
            LIMIT 50;
        """,
        "exasol": """
            SELECT e.device_id, e.merchant_id,
                count(DISTINCT e.card_id) AS distinct_cards_used,
                count(*) AS total_attempts,
                sum(e.amount) AS total_exposed_volume
            FROM PAYLENS.auth_events AS e
            WHERE e.auth_status = 'APPROVED'
            GROUP BY e.device_id, e.merchant_id
            HAVING count(DISTINCT e.card_id) >= 5
            ORDER BY distinct_cards_used DESC, total_exposed_volume DESC
            LIMIT 50;
        """,
    },

    # -----------------------------------------------------------------
    # Q4: Nested JSON Path Extraction
    # Evaluates extraction speed from nested ISO 8583 JSON payload
    # -----------------------------------------------------------------
    "Q4_json_extraction": {
        "description": "Semi-structured JSON extraction from raw ISO 8583 payload",
        "clickhouse": """
            SELECT
                JSONExtractString(raw_payload, 'network') AS card_network,
                count() AS txn_count,
                round(sum(amount), 2) AS total_amount
            FROM paylens.auth_events
            WHERE raw_payload != '{}'
            GROUP BY card_network
            ORDER BY total_amount DESC;
        """,
        "exasol": """
            SELECT
                JSON_VALUE(raw_payload, '$.network') AS card_network,
                count(*) AS txn_count,
                round(sum(amount), 2) AS total_amount
            FROM PAYLENS.auth_events
            WHERE raw_payload != '{}'
            GROUP BY JSON_VALUE(raw_payload, '$.network')
            ORDER BY total_amount DESC;
        """,
    },
}


def _sort_value(value: Any) -> tuple[str, str]:
    if isinstance(value, (decimal.Decimal, int, float)) and not isinstance(value, bool):
        return ("number", decimal.Decimal(str(value)))
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return ("datetime", value.isoformat())
    return (type(value).__name__, repr(value))


def _json_value(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return value


def _installed_version(package_name: str) -> str | None:
    try:
        return package_version(package_name)
    except PackageNotFoundError:
        return None


def compare_result_rows(
    clickhouse_rows: list[tuple],
    exasol_rows: list[tuple],
    *,
    abs_tol: float = 1e-6,
    rel_tol: float = 1e-9,
) -> dict[str, Any]:
    """Compare unordered result sets while allowing connector numeric precision drift."""
    ch_sorted = sorted(clickhouse_rows, key=lambda row: tuple(_sort_value(value) for value in row))
    exa_sorted = sorted(exasol_rows, key=lambda row: tuple(_sort_value(value) for value in row))
    mismatches = []
    for row_index in range(max(len(ch_sorted), len(exa_sorted))):
        if row_index >= len(ch_sorted) or row_index >= len(exa_sorted):
            mismatches.append({
                "row_index": row_index,
                "clickhouse": [_json_value(value) for value in ch_sorted[row_index]] if row_index < len(ch_sorted) else None,
                "exasol": [_json_value(value) for value in exa_sorted[row_index]] if row_index < len(exa_sorted) else None,
            })
            continue
        ch_row, exa_row = ch_sorted[row_index], exa_sorted[row_index]
        if len(ch_row) != len(exa_row):
            mismatches.append({"row_index": row_index,
                               "clickhouse": [_json_value(value) for value in ch_row],
                               "exasol": [_json_value(value) for value in exa_row]})
            continue
        for column_index, (ch_value, exa_value) in enumerate(zip(ch_row, exa_row)):
            if isinstance(ch_value, (decimal.Decimal, int, float)) and not isinstance(ch_value, bool):
                if not isinstance(exa_value, (decimal.Decimal, int, float)) or isinstance(exa_value, bool):
                    equal = False
                else:
                    equal = math.isclose(float(ch_value), float(exa_value), abs_tol=abs_tol, rel_tol=rel_tol)
            else:
                equal = type(ch_value) is type(exa_value) and ch_value == exa_value
                if not equal and isinstance(ch_value, (datetime.datetime, datetime.date, datetime.time)):
                    equal = isinstance(exa_value, type(ch_value)) and ch_value.isoformat() == exa_value.isoformat()
            if not equal:
                mismatches.append({
                    "row_index": row_index,
                    "column_index": column_index,
                    "clickhouse": _json_value(ch_value),
                    "exasol": _json_value(exa_value),
                })
                break
        if len(mismatches) >= 10:
            break
    return {
        "row_counts_match": len(clickhouse_rows) == len(exasol_rows),
        "values_match": len(mismatches) == 0 and len(clickhouse_rows) == len(exasol_rows),
        "mismatch_count_sampled": len(mismatches),
        "mismatch_sample": mismatches,
        "numeric_tolerance": {"absolute": abs_tol, "relative": rel_tol},
        "comparison_note": "Result row order is ignored; numeric columns use the reported absolute and relative tolerances.",
    }


class QueryBenchmarkRunner:
    def __init__(self):
        self.ch_client = None
        self.exasol_conn = None
        self._init_connections()

    def _init_connections(self):
        try:
            import clickhouse_connect
            self.ch_client = clickhouse_connect.get_client(
                host="localhost", port=8123, username="default", password="", database="paylens"
            )
            print("[+] Connected to ClickHouse.")
        except Exception as e:
            raise RuntimeError("Live ClickHouse connection is required; no simulated metrics are generated.") from e

        try:
            import pyexasol
            connect_options = {"websocket_sslopt": {"cert_reqs": ssl.CERT_NONE}}
            self.exasol_conn = pyexasol.connect(
                dsn="localhost:8563", user="sys", password=os.environ.get("EXASOL_PASSWORD", ""), schema="PAYLENS",
                **connect_options,
            )
            print("[+] Connected to Exasol.")
        except Exception as e:
            print(f"[-] Exasol connection failed: {e}", file=sys.stderr)
            raise RuntimeError("Live Exasol connection is required; no simulated metrics are generated.") from e

    def run_query_benchmarks(self, iterations: int = 5, warmups: int = 2) -> Dict[str, Any]:
        if iterations < 1 or warmups < 0:
            raise ValueError("iterations must be >= 1 and warmups must be >= 0")
        print(f"\n==================================================================")
        print(f"  QUERY SPEED & JOIN BENCHMARK ({warmups} warmups, {iterations} measured runs)")
        print(f"==================================================================")

        results = {
            "metadata": {
                "run_id": os.environ.get("PAYLENS_RUN_ID"),
                "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "python_version": platform.python_version(),
                "clickhouse_connect_version": _installed_version("clickhouse-connect"),
                "pyexasol_version": _installed_version("pyexasol"),
                "comparison": "Returned values are compared after numeric tolerance normalization; mismatches are sampled.",
            }
        }

        for q_id, q_data in QUERIES.items():
            print(f"\nRunning {q_id}: {q_data['description']}")
            ch_sql = q_data["clickhouse"].replace("{card_id}", "42")
            ex_sql = q_data["exasol"].replace("{card_id}", "42")

            def run_clickhouse() -> list[tuple]:
                return self.ch_client.query(ch_sql).result_rows

            def run_exasol() -> list[tuple]:
                # Fetch rows directly; export_to_pandas adds dataframe/HTTP-export work.
                return self.exasol_conn.execute(ex_sql).fetchall()

            # Warm both engines before measuring to avoid comparing cold-start
            # and warmed runs. Alternate which engine runs first each iteration.
            for _ in range(warmups):
                run_clickhouse()
                run_exasol()

            # ClickHouse Timing
            ch_times = []
            ex_times = []
            ch_result = exa_result = []
            for iteration in range(iterations):
                engine_runs = [("clickhouse", run_clickhouse), ("exasol", run_exasol)]
                if iteration % 2:
                    engine_runs.reverse()
                for engine, run_query in engine_runs:
                    start = time.perf_counter()
                    result_rows = run_query()
                    elapsed_ms = (time.perf_counter() - start) * 1000.0
                    if engine == "clickhouse":
                        ch_times.append(elapsed_ms)
                        ch_result = result_rows
                    else:
                        ex_times.append(elapsed_ms)
                        exa_result = result_rows

            ch_sorted = sorted(ch_times)
            ex_sorted = sorted(ex_times)
            ch_p50 = ch_sorted[len(ch_sorted) // 2]
            ex_p50 = ex_sorted[len(ex_sorted) // 2]
            ch_p95 = ch_sorted[max(0, int(len(ch_sorted) * 0.95 + 0.999) - 1)]
            ex_p95 = ex_sorted[max(0, int(len(ex_sorted) * 0.95 + 0.999) - 1)]

            speedup = ex_p50 / ch_p50 if ch_p50 > 0 else 1.0
            winner = "ClickHouse" if ch_p50 < ex_p50 else "Exasol"
            margin = (ex_p50 / ch_p50) if ch_p50 < ex_p50 else (ch_p50 / ex_p50)
            correctness = compare_result_rows(ch_result, exa_result)

            results[q_id] = {
                "sql": {
                    "clickhouse": ch_sql.strip(),
                    "exasol": ex_sql.strip(),
                },
                "clickhouse_p50_ms": round(ch_p50, 2),
                "clickhouse_p95_ms": round(ch_p95, 2),
                "exasol_p50_ms": round(ex_p50, 2),
                "exasol_p95_ms": round(ex_p95, 2),
                "clickhouse_result_rows": len(ch_result),
                "exasol_result_rows": len(exa_result),
                "result_row_counts_match": correctness["row_counts_match"],
                "result_values_match": correctness["values_match"],
                "result_diff": correctness,
                "winner": winner,
                "margin": f"{margin:.2f}x",
            }

        self.print_summary_table(results)
        return results

    def print_summary_table(self, results: Dict[str, Any]):
        print("\n| Query ID | ClickHouse (p50) | Exasol (p50) | Winner & Margin |")
        print("|---|---|---|---|")
        for q_id, data in results.items():
            if q_id == "metadata":
                continue
            print(f"| **{q_id}** | {data['clickhouse_p50_ms']} ms | {data['exasol_p50_ms']} ms | 🏆 **{data['winner']}** ({data['margin']}) |")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PayLens Query Benchmark")
    parser.add_argument("--iterations", type=int, default=5, help="Number of benchmark iterations")
    parser.add_argument("--warmups", type=int, default=2, help="Warmup runs per engine and query")
    args = parser.parse_args()

    runner = QueryBenchmarkRunner()
    results = runner.run_query_benchmarks(iterations=args.iterations, warmups=args.warmups)

    results_dir = os.environ.get("PAYLENS_RESULTS_DIR", "results")
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, "query_benchmark_results.json"), "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[+] Query benchmark results saved to {results_dir}/query_benchmark_results.json")
