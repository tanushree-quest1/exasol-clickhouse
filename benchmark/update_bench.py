"""Live ledger mutation probes isolated to temporary benchmark tables."""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from benchmark.physical_design_bench import connect, exa_rows


def run(update_count: int) -> dict[str, Any]:
    if update_count < 1:
        raise ValueError("--rows must be positive")
    ch, exa = connect()
    suffix = uuid.uuid4().hex[:10].upper()
    ch_alter = f"paylens.ledger_alter_{suffix.lower()}"
    ch_rmt = f"paylens.ledger_rmt_{suffix.lower()}"
    ex_table = f"PAYLENS.LEDGER_UPDATE_{suffix}"
    ids = list(range(1, update_count + 1))
    initial = [(i, i % 2000, "AUTH", 1) for i in ids]
    updates = [(i, i % 2000, "SETTLED", 2) for i in ids]
    try:
        ch.command(f"CREATE TABLE {ch_alter} (id UInt64, account_id UInt64, state String, version UInt64) ENGINE=MergeTree ORDER BY id")
        ch.command(f"CREATE TABLE {ch_rmt} (id UInt64, account_id UInt64, state String, version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY id")
        exa_rows(exa, f"CREATE TABLE {ex_table} (ID DECIMAL(18,0), ACCOUNT_ID DECIMAL(18,0), TXN_STATE VARCHAR(20), VERSION DECIMAL(18,0))")
        ch.insert(ch_alter, initial, column_names=["id", "account_id", "state", "version"])
        ch.insert(ch_rmt, initial, column_names=["id", "account_id", "state", "version"])
        exa.import_from_iterable(initial, ex_table.split(".")[1])

        started = time.perf_counter()
        ch.command(f"ALTER TABLE {ch_alter} UPDATE state='SETTLED', version=2 WHERE state='AUTH'", settings={"mutations_sync": 1})
        ch_alter_ms = (time.perf_counter() - started) * 1000
        ch_alter_count = int(ch.query(f"SELECT count() FROM {ch_alter} WHERE state='SETTLED'").result_rows[0][0])

        started = time.perf_counter()
        exa.execute(f"UPDATE {ex_table} SET TXN_STATE='SETTLED', VERSION=2 WHERE TXN_STATE='AUTH'")
        ex_update_ms = (time.perf_counter() - started) * 1000
        ex_update_count = int(exa_rows(exa, f"SELECT COUNT(*) FROM {ex_table} WHERE TXN_STATE='SETTLED'")[0][0])

        started = time.perf_counter()
        ch.insert(ch_rmt, updates, column_names=["id", "account_id", "state", "version"])
        rmt_insert_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        rmt_count = int(ch.query(f"SELECT count() FROM {ch_rmt} FINAL WHERE state='SETTLED'").result_rows[0][0])
        rmt_visibility_ms = (time.perf_counter() - started) * 1000

        return {
            "run_type": "live",
            "rows_updated": update_count,
            "clickhouse_alter_update": {"elapsed_ms": round(ch_alter_ms, 3), "settled_rows": ch_alter_count,
                "completed_before_return": True, "sync_setting": "mutations_sync=1"},
            "clickhouse_replacing_merge_tree": {"append_elapsed_ms": round(rmt_insert_ms, 3),
                "deduplicating_read_elapsed_ms": round(rmt_visibility_ms, 3), "settled_rows_after_final": rmt_count,
                "append_rows_written": update_count},
            "exasol_update": {"elapsed_ms": round(ex_update_ms, 3), "settled_rows": ex_update_count,
                "commit_mode": "PyExasol autocommit"},
            "correctness": {"alter_update_matches_requested": ch_alter_count == update_count,
                "replacing_merge_tree_final_matches_requested": rmt_count == update_count,
                "exasol_update_matches_requested": ex_update_count == update_count},
            "caveat": "Uses equal synthetic scratch rows and synchronous ClickHouse mutation completion. Does not establish transaction isolation or production ledger correctness.",
        }
    finally:
        for table in (ch_alter, ch_rmt):
            try:
                ch.command(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass
        try:
            exa_rows(exa, f"DROP TABLE IF EXISTS {ex_table}")
        finally:
            ch.close()
            exa.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live ClickHouse and Exasol ledger mutation probe")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    result = run(args.rows)
    out = args.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR", "results"), "update_benchmark_results.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"[+] Live mutation results saved to {out}")


if __name__ == "__main__":
    main()
