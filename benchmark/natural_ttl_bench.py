"""Observe natural ClickHouse TTL cleanup against an Exasol scheduled DELETE."""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from benchmark.physical_design_bench import connect, exa_rows


def run(retention_seconds: int = 10, max_wait_seconds: int = 90, poll_seconds: float = 1.0) -> dict[str, Any]:
    if retention_seconds < 1 or max_wait_seconds < 1 or poll_seconds <= 0:
        raise ValueError("retention, max wait, and polling interval must be positive")
    suffix = uuid.uuid4().hex[:10].lower()
    ch_table = f"paylens.ttl_watch_{suffix}"
    exa_table = f"PAYLENS.TTL_WATCH_{suffix.upper()}"
    ch, exa = connect()
    def _to_dt(val: Any) -> datetime:
        if isinstance(val, datetime):
            return val
        s = str(val).split(".")[0]
        return datetime.fromisoformat(s)

    ch_now = _to_dt(ch.query("SELECT now64(3, 'UTC')").result_rows[0][0])
    if ch_now.tzinfo is None:
        ch_now = ch_now.replace(tzinfo=timezone.utc)
    exa_now = _to_dt(exa_rows(exa, "SELECT CURRENT_TIMESTAMP")[0][0])

    # Keep candidate rows fresh during table creation and inserts, then let them
    # cross the expiry boundary while polling. Synchronize with each engine's
    # server clock to avoid host/container timezone offsets.
    lead_seconds = 0
    ch_candidate_time = ch_now + timedelta(seconds=lead_seconds)
    ch_fresh_time = ch_now + timedelta(seconds=max_wait_seconds + retention_seconds)
    exa_candidate_time = exa_now + timedelta(seconds=lead_seconds)
    exa_fresh_time = exa_now + timedelta(seconds=max_wait_seconds + retention_seconds)
    ch_expiry_time = ch_candidate_time + timedelta(seconds=retention_seconds)
    exa_expiry_time = exa_candidate_time + timedelta(seconds=retention_seconds)
    exa_candidate_literal = exa_candidate_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    exa_fresh_literal = exa_fresh_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    try:
        ch.command(f"""CREATE TABLE {ch_table} (
          id UInt32, event_time DateTime64(3, 'UTC')
        ) ENGINE=MergeTree ORDER BY id
          TTL event_time + INTERVAL {retention_seconds} SECOND DELETE
          SETTINGS merge_with_ttl_timeout=1""")
        exa_rows(exa, f"CREATE TABLE {exa_table} (ID DECIMAL(10,0), EVENT_TIME TIMESTAMP)")
        ch_create = ch.query(f"SHOW CREATE TABLE {ch_table}").result_rows[0][0]

        # Several small parts give background merge selection an opportunity
        # to process expired rows. No OPTIMIZE/FINAL or forced merge is issued.
        for batch in range(8):
            old_id = batch * 2 + 1
            fresh_id = old_id + 1
            ch.insert(ch_table, [(old_id, ch_candidate_time), (fresh_id, ch_fresh_time)], column_names=["id", "event_time"])
            exa_rows(exa, f"INSERT INTO {exa_table} VALUES ({old_id}, TIMESTAMP '{exa_candidate_literal}'), ({fresh_id}, TIMESTAMP '{exa_fresh_literal}')")

        expected_expired = 8
        started = time.monotonic()
        exa_first_cleanup_ms = None
        ch_first_cleanup_ms = None
        exa_cleanup_lag_ms = None
        ch_cleanup_lag_ms = None
        observations = []
        deadline = started + max_wait_seconds
        while time.monotonic() <= deadline:
            elapsed_ms = (time.monotonic() - started) * 1000
            ch_server_now = _to_dt(ch.query("SELECT now('UTC')").result_rows[0][0])
            if ch_server_now.tzinfo is None:
                ch_server_now = ch_server_now.replace(tzinfo=timezone.utc)
            exa_server_now = _to_dt(exa_rows(exa, "SELECT CURRENT_TIMESTAMP")[0][0])
            if exa_server_now.tzinfo is None and exa_now.tzinfo is not None:
                exa_server_now = exa_server_now.replace(tzinfo=exa_now.tzinfo)
            ch_cutoff_reached = ch_server_now >= ch_candidate_time + timedelta(seconds=retention_seconds)
            exa_cutoff_reached = exa_server_now >= exa_candidate_time + timedelta(seconds=retention_seconds)
            ch_expired = int(ch.query(f"SELECT count() FROM {ch_table} WHERE id % 2 = 1 AND event_time < now('UTC') - INTERVAL {retention_seconds} SECOND").result_rows[0][0])
            ch_total_now = int(ch.query(f"SELECT count() FROM {ch_table}").result_rows[0][0])
            exa_expired_before = int(exa_rows(exa, f"SELECT COUNT(*) FROM {exa_table} WHERE MOD(ID, 2)=1 AND EVENT_TIME < ADD_SECONDS(CURRENT_TIMESTAMP, -{retention_seconds})")[0][0])
            if exa_expired_before:
                exa_started = time.perf_counter()
                exa_rows(exa, f"DELETE FROM {exa_table} WHERE MOD(ID, 2)=1 AND EVENT_TIME < ADD_SECONDS(CURRENT_TIMESTAMP, -{retention_seconds})")
                exa_rows(exa, "COMMIT")
                if exa_first_cleanup_ms is None:
                    exa_first_cleanup_ms = round(elapsed_ms, 3)
                    exa_cleanup_lag_ms = max(
                        0.0, (exa_server_now - exa_expiry_time).total_seconds() * 1000
                    )
                exa_delete_ms = round((time.perf_counter() - exa_started) * 1000, 3)
            else:
                exa_delete_ms = None
            exa_expired = int(exa_rows(exa, f"SELECT COUNT(*) FROM {exa_table} WHERE MOD(ID, 2)=1 AND EVENT_TIME < ADD_SECONDS(CURRENT_TIMESTAMP, -{retention_seconds})")[0][0])
            observations.append({"elapsed_ms": round(elapsed_ms, 3), "clickhouse_cutoff_reached": ch_cutoff_reached,
                "exasol_cutoff_reached": exa_cutoff_reached, "clickhouse_expired_visible": ch_expired,
                "clickhouse_total_rows": ch_total_now, "exasol_expired_visible": exa_expired})
            if ch_cutoff_reached and ch_expired == 0 and ch_total_now == 8 and ch_first_cleanup_ms is None:
                ch_first_cleanup_ms = round(elapsed_ms, 3)
                ch_cleanup_lag_ms = max(
                    0.0, (ch_server_now - ch_expiry_time).total_seconds() * 1000
                )
            if (ch_cutoff_reached and exa_cutoff_reached and ch_expired == 0
                    and exa_expired == 0 and ch_total_now == 8):
                break
            time.sleep(poll_seconds)

        ch_remaining = int(ch.query(f"SELECT count() FROM {ch_table} WHERE id % 2 = 1 AND event_time < now('UTC') - INTERVAL {retention_seconds} SECOND").result_rows[0][0])
        exa_remaining = int(exa_rows(exa, f"SELECT COUNT(*) FROM {exa_table} WHERE MOD(ID, 2)=1 AND EVENT_TIME < ADD_SECONDS(CURRENT_TIMESTAMP, -{retention_seconds})")[0][0])
        ch_total = int(ch.query(f"SELECT count() FROM {ch_table}").result_rows[0][0])
        exa_total = int(exa_rows(exa, f"SELECT COUNT(*) FROM {exa_table}")[0][0])
        return {
            "run_type": "live",
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "retention_seconds": retention_seconds,
            "max_wait_seconds": max_wait_seconds,
            "poll_interval_seconds": poll_seconds,
            "rows_per_engine": 16,
            "expired_rows_per_engine": expected_expired,
            "clickhouse": {"mechanism": "MergeTree background TTL; no forced optimize/merge", "cleanup_observed_ms": ch_first_cleanup_ms,
                "expiry_time_utc": ch_expiry_time.isoformat(),
                "cleanup_lag_since_expiry_ms": round(ch_cleanup_lag_ms, 3) if ch_cleanup_lag_ms is not None else None,
                "effective_table_settings": {"merge_with_ttl_timeout_seconds": 1},
                "create_table_sql": ch_create,
                "expired_rows_remaining": ch_remaining, "total_rows_remaining": ch_total,
                "fresh_rows_remaining": ch_total - ch_remaining,
                "fresh_rows_match_expected": ch_total - ch_remaining == 8,
                "cleanup_observed": ch_cutoff_reached and ch_remaining == 0 and ch_total - ch_remaining == 8},
            "exasol": {"mechanism": "application-side scheduled DELETE + COMMIT polled at the configured interval", "cleanup_observed_ms": exa_first_cleanup_ms,
                "expiry_time_server_local": exa_expiry_time.isoformat(),
                "cleanup_lag_since_expiry_ms": round(exa_cleanup_lag_ms, 3) if exa_cleanup_lag_ms is not None else None,
                "expired_rows_remaining": exa_remaining, "total_rows_remaining": exa_total,
                "fresh_rows_remaining": exa_total - exa_remaining,
                "fresh_rows_match_expected": exa_total - exa_remaining == 8,
                "cleanup_observed": exa_cutoff_reached and exa_remaining == 0 and exa_total - exa_remaining == 8},
            "observations": observations,
            "caveat": "Short synthetic retention and a polled Exasol DELETE workflow estimate behavior only in this local setup. ClickHouse background merges are nondeterministic; a timeout is recorded as not observed, not forced into success. Visibility is measured, not physical disk reclamation.",
        }
    finally:
        try:
            ch.command(f"DROP TABLE IF EXISTS {ch_table}")
        finally:
            try:
                exa_rows(exa, f"DROP TABLE IF EXISTS {exa_table}")
            finally:
                ch.close()
                exa.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Observe natural TTL visibility and scheduled Exasol cleanup")
    parser.add_argument("--retention-seconds", type=int, default=10)
    parser.add_argument("--max-wait-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    result = run(args.retention_seconds, args.max_wait_seconds, args.poll_seconds)
    out = args.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR", "results"), "natural_ttl_results.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"[+] Natural-retention results saved to {out}")


if __name__ == "__main__":
    main()
