"""Live-only probes for layout, pruning, indexes, storage, and retention.

This script never falls back to fabricated timings. It reads current PayLens
data and uses temporary tables for the short TTL behavior probe.
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from benchmark.query_bench import QUERIES


def connect():
    import clickhouse_connect
    import pyexasol

    ch = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database="paylens",
    )
    dsn = os.getenv("EXASOL_DSN", "localhost:8563")
    options = {}
    if dsn.startswith(("localhost:", "127.0.0.1:")):
        options["websocket_sslopt"] = {"cert_reqs": ssl.CERT_NONE}
    exa_pass = os.getenv("EXASOL_PASSWORD", "")
    if not exa_pass:
        secrets_path = os.getenv("EXASOL_SECRETS_FILE") or os.path.expanduser("~/.exasol/personal/deployments/default/secrets.json")
        if os.path.exists(secrets_path):
            try:
                with open(secrets_path) as f:
                    exa_pass = json.load(f).get("dbPassword", "")
            except Exception:
                pass
    exa = pyexasol.connect(
        dsn=dsn,
        user=os.getenv("EXASOL_USER", "sys"),
        password=exa_pass,
        schema="PAYLENS",
        autocommit=True,
        **options,
    )
    return ch, exa


def exa_rows(conn, sql: str):
    stmt = conn.execute(sql)
    # PyExasol statements for DDL/DML have no result set; fetchall() raises.
    if getattr(stmt, "result_type", None) != "resultSet":
        return []
    return stmt.fetchall()


def date_text(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def get_exa_profile(conn, before_stmt_id: int) -> list[dict[str, Any]]:
    exa_rows(conn, "ALTER SESSION SET PROFILE='OFF'")
    exa_rows(conn, "FLUSH STATISTICS")
    rows = exa_rows(
        conn,
        """SELECT STMT_ID, PART_NAME, PART_INFO, OBJECT_NAME, OBJECT_ROWS,
                  OUT_ROWS, DURATION, CPU, TEMP_DB_RAM_PEAK, LOCAL_READ_SIZE, REMARKS
           FROM EXA_USER_PROFILE_LAST_DAY
           WHERE SESSION_ID = CURRENT_SESSION
             AND STMT_ID > {before_stmt_id}
           ORDER BY STMT_ID, PART_ID""".format(before_stmt_id=int(before_stmt_id)),
    )
    columns = ["stmt_id", "part_name", "part_info", "object_name", "object_rows",
               "out_rows", "duration_sec", "cpu_pct", "temp_db_ram_peak_mb",
               "local_read_size_mb", "remarks"]
    return [dict(zip(columns, row)) for row in rows]


def profiled_exasol_query(conn, sql: str) -> tuple[float, int, list[dict[str, Any]]]:
    prior = exa_rows(
        conn,
        "SELECT COALESCE(MAX(STMT_ID), 0) FROM EXA_USER_PROFILE_LAST_DAY "
        "WHERE SESSION_ID = CURRENT_SESSION",
    )[0][0]
    exa_rows(conn, "ALTER SESSION SET PROFILE='ON'")
    started = time.perf_counter()
    result = exa_rows(conn, sql)
    elapsed_ms = (time.perf_counter() - started) * 1000
    profile = get_exa_profile(conn, int(prior or 0))
    return elapsed_ms, len(result), profile


def clickhouse_profiled_query(ch, sql: str, query_id: str) -> tuple[float, int, dict[str, Any] | None]:
    started = time.perf_counter()
    # clickhouse-connect exposes query IDs through its context/settings APIs in
    # version-specific ways. `log_comment` is a supported server setting and
    # gives us a stable key to retrieve this query's system.query_log row.
    result = ch.query(sql, settings={"log_comment": query_id})
    elapsed_ms = (time.perf_counter() - started) * 1000
    try:
        ch.command("SYSTEM FLUSH LOGS")
        rows = ch.query(
            """SELECT query_duration_ms, read_rows, read_bytes, result_rows,
                      memory_usage, ProfileEvents
               FROM system.query_log
               WHERE type = 'QueryFinish' AND log_comment = {query_id:String}
               ORDER BY event_time_microseconds DESC LIMIT 1""",
            parameters={"query_id": query_id},
        ).result_rows
        row = rows[0] if rows else None
        log = None if row is None else {
            "query_duration_ms": row[0], "read_rows": row[1], "read_bytes": row[2],
            "result_rows": row[3], "memory_bytes": row[4], "profile_events": row[5],
        }
    except Exception as exc:
        log = {"query_log_unavailable": str(exc)}
    return elapsed_ms, len(result.result_rows), log


def storage_metrics(ch, exa) -> dict[str, Any]:
    ch_row = ch.query(
        """SELECT sum(data_uncompressed_bytes), sum(data_compressed_bytes),
                  sum(rows), countIf(active)
           FROM system.parts
           WHERE database='paylens' AND table='auth_events' AND active"""
    ).result_rows[0]
    exa_row = exa_rows(
        exa,
        """SELECT SUM(RAW_OBJECT_SIZE), SUM(MEM_OBJECT_SIZE)
           FROM EXA_DBA_OBJECT_SIZES WHERE ROOT_NAME='PAYLENS'
             AND OBJECT_TYPE='TABLE' AND OBJECT_NAME='AUTH_EVENTS'""",
    )[0]
    exa_rows_count = int(exa_rows(exa, "SELECT COUNT(*) FROM PAYLENS.AUTH_EVENTS")[0][0])
    def values(raw, compressed):
        raw = int(raw or 0)
        compressed = int(compressed or 0)
        return {
            "raw_bytes": raw,
            "compressed_or_mem_bytes": compressed,
            "compression_ratio": round(raw / compressed, 3) if compressed else None,
        }
    ch_data = values(ch_row[0], ch_row[1])
    exa_data = values(exa_row[0], exa_row[1])
    ch_data.update({"rows": int(ch_row[2] or 0), "active_parts": int(ch_row[3] or 0),
                    "size_semantics": "compressed data-part bytes on disk"})
    exa_data.update({"rows": exa_rows_count,
                     "size_semantics": "compressed in-memory object size at last commit"})
    return {"clickhouse": ch_data, "exasol": exa_data,
            "scope": "PAYLENS.AUTH_EVENTS only",
            "comparability_note": "Physical size columns have different engine semantics; compression ratios are indicative, not identical accounting measures."}


def partition_and_index_metrics(ch, exa) -> dict[str, Any]:
    ch_date = ch.query("SELECT max(event_date) FROM paylens.auth_events").result_rows[0][0]
    exa_date = exa_rows(exa, "SELECT MAX(EVENT_DATE) FROM PAYLENS.AUTH_EVENTS")[0][0]
    if not ch_date or not exa_date:
        raise RuntimeError("PAYLENS.AUTH_EVENTS must contain rows in both databases")
    ch_day = date_text(ch_date)
    exa_day = date_text(exa_date)
    ch_count = int(ch.query("SELECT count() FROM paylens.auth_events").result_rows[0][0])
    exa_count = int(exa_rows(exa, "SELECT COUNT(*) FROM PAYLENS.AUTH_EVENTS")[0][0])

    ch_filter = f"SELECT count() FROM paylens.auth_events WHERE event_date = toDate('{ch_day}')"
    exa_filter = f"SELECT COUNT(*) FROM PAYLENS.AUTH_EVENTS WHERE EVENT_DATE = DATE '{exa_day}'"
    ch_explain = [str(r[0]) for r in ch.query("EXPLAIN indexes=1 " + ch_filter).result_rows]
    exa_ms, exa_result_rows, exa_profile = profiled_exasol_query(exa, exa_filter)
    ch_ms, ch_result_rows, ch_log = clickhouse_profiled_query(
        ch, ch_filter, "paylens-prune-" + uuid.uuid4().hex,
    )

    card = int(ch.query("SELECT any(card_id) FROM paylens.auth_events").result_rows[0][0])
    ch_lookup = f"SELECT count() FROM paylens.auth_events WHERE card_id = {card}"
    ch_lookup_explain = [str(r[0]) for r in ch.query("EXPLAIN indexes=1 " + ch_lookup).result_rows]
    ch_lookup_ms, ch_lookup_rows, ch_lookup_log = clickhouse_profiled_query(
        ch, ch_lookup, "paylens-card-index-" + uuid.uuid4().hex,
    )
    # Demonstrate ClickHouse's explicit secondary-index workflow on an
    # isolated capped copy: index added after existing rows, then materialized.
    idx_suffix = uuid.uuid4().hex[:10].lower()
    idx_table = f"paylens.index_probe_{idx_suffix}"
    clickhouse_index_probe: dict[str, Any]
    try:
        ch.command(f"CREATE TABLE {idx_table} (id UUID, card_id UInt64) ENGINE=MergeTree ORDER BY id")
        ch.command(f"INSERT INTO {idx_table} SELECT event_id, card_id FROM paylens.auth_events LIMIT 1000000")
        copied_rows = int(ch.query(f"SELECT count() FROM {idx_table}").result_rows[0][0])
        probe_sql = f"SELECT count() FROM {idx_table} WHERE card_id = {card}"
        before_index = [str(r[0]) for r in ch.query("EXPLAIN indexes=1 " + probe_sql).result_rows]
        ch.command(f"ALTER TABLE {idx_table} ADD INDEX idx_card_bloom card_id TYPE bloom_filter(0.01) GRANULARITY 4")
        before_materialize = [str(r[0]) for r in ch.query("EXPLAIN indexes=1 " + probe_sql).result_rows]
        started = time.perf_counter()
        ch.command(f"ALTER TABLE {idx_table} MATERIALIZE INDEX idx_card_bloom", settings={"mutations_sync": 1})
        materialize_ms = (time.perf_counter() - started) * 1000
        after_materialize = [str(r[0]) for r in ch.query("EXPLAIN indexes=1 " + probe_sql).result_rows]
        clickhouse_index_probe = {
            "copied_rows": copied_rows,
            "index_added_after_data_load": True,
            "explicit_materialize_elapsed_ms": round(materialize_ms, 3),
            "explain_before_index": before_index,
            "explain_after_add_before_materialize": before_materialize,
            "explain_after_materialize": after_materialize,
        }
    finally:
        ch.command(f"DROP TABLE IF EXISTS {idx_table}")
    # The five-table join is used to expose Exasol's automatic join-index path;
    # the profile records INDEX CREATE/INDEX usage if the optimizer chooses it.
    exa_join = QUERIES["Q3_fraud_ring_5table_join"]["exasol"].strip().rstrip(";")
    exa_join_ms, exa_join_rows, exa_join_profile = profiled_exasol_query(exa, exa_join)
    exa_join_warm_ms, exa_join_warm_rows, exa_join_warm_profile = profiled_exasol_query(exa, exa_join)
    exa_indices = exa_rows(
        exa,
        "SELECT * FROM EXA_ALL_INDICES WHERE INDEX_SCHEMA='PAYLENS' "
        "AND INDEX_TABLE IN ('AUTH_EVENTS','CARDS','ACCOUNTS','MERCHANTS','DEVICES')",
    )
    return {
        "row_counts": {"clickhouse": ch_count, "exasol": exa_count,
                       "equal": ch_count == exa_count},
        "date_filter": {"clickhouse_date": ch_day, "exasol_date": exa_day,
            "clickhouse": {"client_elapsed_ms": round(ch_ms, 3), "result_rows": ch_result_rows,
                           "explain_indexes": ch_explain, "query_log": ch_log},
            "exasol": {"client_elapsed_ms": round(exa_ms, 3), "result_rows": exa_result_rows,
                       "profile": exa_profile}},
        "card_lookup_index_probe": {
            "card_id": card,
            "clickhouse": {"client_elapsed_ms": round(ch_lookup_ms, 3),
                "result_rows": ch_lookup_rows, "explain_indexes": ch_lookup_explain,
                "query_log": ch_lookup_log,
                "declared_index": "idx_card_bloom (card_id), bloom_filter(0.01), granularity 4",
                "add_then_materialize_probe": clickhouse_index_probe},
            "exasol_auto_index": {"join_probe_elapsed_ms": round(exa_join_ms, 3),
                "join_probe_result_rows": exa_join_rows,
                "warm_join_elapsed_ms": round(exa_join_warm_ms, 3),
                "warm_join_result_rows": exa_join_warm_rows,
                "indices_metadata_rows": [list(r) for r in exa_indices],
                "first_join_profile": exa_join_profile,
                "warm_join_profile": exa_join_warm_profile}},
        "five_table_join_probe": {
            "matches_query_benchmark": True,
            "query_id": "Q3_fraud_ring_5table_join",
            "exasol_sql": exa_join,
            "result_limit": 50,
            "first_run_elapsed_ms": round(exa_join_ms, 3),
            "first_run_result_rows": exa_join_rows,
            "warm_run_elapsed_ms": round(exa_join_warm_ms, 3),
            "warm_run_result_rows": exa_join_warm_rows,
            "first_run_profile": exa_join_profile,
            "warm_run_profile": exa_join_warm_profile,
        },
        "interpretation": "Use plans/profiles and rows/bytes read; elapsed time alone does not prove index or pruning use.",
    }


def ttl_probe(ch, exa) -> dict[str, Any]:
    suffix = uuid.uuid4().hex[:10].upper()
    ch_table = f"paylens.ttl_probe_{suffix.lower()}"
    exa_table = f"PAYLENS.TTL_PROBE_{suffix}"
    ch.command(f"CREATE TABLE {ch_table} (id UInt8, event_time DateTime) ENGINE=MergeTree ORDER BY id TTL event_time + INTERVAL 90 DAY DELETE")
    exa_rows(exa, f"CREATE TABLE {exa_table} (ID DECIMAL(3,0), EVENT_TIME TIMESTAMP)")
    try:
        ch.command(f"INSERT INTO {ch_table} VALUES (1, now() - INTERVAL 180 DAY), (2, now() - INTERVAL 1 DAY)")
        exa_rows(exa, f"INSERT INTO {exa_table} VALUES (1, ADD_DAYS(CURRENT_TIMESTAMP, -180)), (2, ADD_DAYS(CURRENT_TIMESTAMP, -1))")

        ch_started = time.perf_counter()
        ch.command(f"OPTIMIZE TABLE {ch_table} FINAL")
        ch_cleanup_ms = (time.perf_counter() - ch_started) * 1000
        ch_remaining = int(ch.query(f"SELECT count() FROM {ch_table}").result_rows[0][0])

        ex_started = time.perf_counter()
        exa_rows(exa, f"DELETE FROM {exa_table} WHERE EVENT_TIME < ADD_DAYS(CURRENT_TIMESTAMP, -90)")
        exa_rows(exa, "COMMIT")
        ex_cleanup_ms = (time.perf_counter() - ex_started) * 1000
        ex_remaining = int(exa_rows(exa, f"SELECT COUNT(*) FROM {exa_table}")[0][0])
        return {
            "clickhouse": {"mechanism": "TTL DELETE evaluated during forced OPTIMIZE FINAL on isolated 2-row probe",
                "cleanup_elapsed_ms": round(ch_cleanup_ms, 3), "remaining_rows": ch_remaining,
                "expired_rows_removed": ch_remaining == 1},
            "exasol": {"mechanism": "explicit DELETE plus COMMIT on isolated 2-row probe; scheduler not included",
                "cleanup_elapsed_ms": round(ex_cleanup_ms, 3), "remaining_rows": ex_remaining,
                "expired_rows_removed": ex_remaining == 1},
            "caveat": "This measures a forced cleanup operation on scratch tables; it does not measure natural ClickHouse TTL lag or the operational cost of scheduling Exasol retention.",
        }
    finally:
        try:
            ch.command(f"DROP TABLE IF EXISTS {ch_table}")
        finally:
            exa_rows(exa, f"DROP TABLE IF EXISTS {exa_table}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Live ClickHouse/Exasol physical-design probes")
    parser.add_argument("--output", default=None, help="Output JSON path (default PAYLENS_RESULTS_DIR or results)")
    args = parser.parse_args()
    ch, exa = connect()
    try:
        probes = {
            "storage": lambda: storage_metrics(ch, exa),
            "partition_and_indexing": lambda: partition_and_index_metrics(ch, exa),
            "ttl_probe": lambda: ttl_probe(ch, exa),
        }
        data = {}
        for name, probe in probes.items():
            try:
                data[name] = {"status": "measured", "metrics": probe()}
            except Exception as exc:
                data[name] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        result = {
            "run_type": "live",
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
    finally:
        ch.close()
        exa.close()
    output_path = args.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR", "results"), "physical_design_results.json")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print(json.dumps(result, indent=2, default=str))
    print(f"\n[+] Live physical-design results saved to {output_path}")


if __name__ == "__main__":
    main()
