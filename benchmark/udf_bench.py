"""Live benchmark comparing equivalent scoring logic in ClickHouse and Exasol."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import time
from typing import Any

from benchmark.physical_design_bench import connect, exa_rows

MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "udf", "model_artifact", "fraud_model.json")


def load_model() -> tuple[dict[str, Any], str]:
    with open(MODEL_PATH, "rb") as f:
        artifact_bytes = f.read()
    with open(MODEL_PATH, encoding="utf-8") as f:
        model = json.load(f)
    if model.get("model_type") != "LogisticFraudScorer" or len(model.get("weights", [])) != 5:
        raise ValueError("Expected the repository's five-feature LogisticFraudScorer artifact")
    digest = hashlib.sha256(artifact_bytes).hexdigest()
    return model, digest


def make_udf_ddl(model: dict[str, Any]) -> tuple[str, str]:
    w = [repr(float(v)) for v in model["weights"]]
    bias = repr(float(model["bias"]))
    threshold = repr(float(model["threshold"]))
    z = f"(amount*{w[0]} + mcc*{w[1]} + hour_val*{w[2]} + is_cnp*{w[3]} + is_contactless*{w[4]} + {bias})"
    ch_ddl = f"""CREATE OR REPLACE FUNCTION score_fraud_artifact AS (amount, mcc, hour_val, is_cnp, is_contactless) ->
      round(1.0 / (1.0 + exp(-greatest(-20.0, least(20.0, {z})))), 4)"""
    exa_ddl = f'''CREATE OR REPLACE PYTHON3 SCALAR SCRIPT score_fraud_artifact(
      amount DOUBLE, mcc DOUBLE, hour_val DOUBLE, is_cnp DOUBLE, is_contactless DOUBLE
    ) RETURNS DOUBLE AS
import math
WEIGHTS = [{", ".join(w)}]
BIAS = {bias}
THRESHOLD = {threshold}
def run(ctx):
    values = [ctx.amount or 0.0, ctx.mcc or 0.0, ctx.hour_val or 0.0,
              ctx.is_cnp or 0.0, ctx.is_contactless or 0.0]
    z = sum(x * weight for x, weight in zip(values, WEIGHTS)) + BIAS
    score = round(1.0 / (1.0 + math.exp(-max(min(z, 20.0), -20.0))), 4)
    return score
/'''
    return ch_ddl, exa_ddl


def make_exasol_sql_expression(model: dict[str, Any]) -> str:
    weights = [repr(float(value)) for value in model["weights"]]
    bias = repr(float(model["bias"]))
    z = (
        f"(CAST(amount AS DOUBLE)*{weights[0]} + "
        f"CAST(MOD(merchant_id, 1000) AS DOUBLE)*{weights[1]} + "
        f"CAST(HOUR(event_time) AS DOUBLE)*{weights[2]} + "
        f"CASE WHEN pos_entry_mode='CNP' THEN {weights[3]} ELSE 0.0 END + "
        f"CASE WHEN pos_entry_mode='CONTACTLESS' THEN {weights[4]} ELSE 0.0 END + {bias})"
    )
    return (
        f"CASE WHEN {z} >= 0 THEN "
        f"ROUND(1.0 / (1.0 + EXP(-LEAST(20.0, {z}))), 4) "
        f"ELSE ROUND(EXP(GREATEST(-20.0, {z})) / "
        f"(1.0 + EXP(GREATEST(-20.0, {z}))), 4) END"
    )


def make_exasol_pandas_set_ddl(model: dict[str, Any]) -> str:
    weights = [repr(float(value)) for value in model["weights"]]
    bias = repr(float(model["bias"]))
    return f'''CREATE OR REPLACE PYTHON3 SET SCRIPT score_fraud_pandas(
      amount DOUBLE, mcc DOUBLE, hour_val DOUBLE, is_cnp DOUBLE, is_contactless DOUBLE
    ) EMITS (score DOUBLE) AS
import numpy as np
import pandas as pd
WEIGHTS = np.array([{", ".join(weights)}], dtype=np.float64)
BIAS = {bias}
BATCH_SIZE = 4096
def emit_batch(ctx, batch):
    frame = pd.DataFrame(batch, columns=["amount", "mcc", "hour", "cnp", "contactless"])
    values = frame.to_numpy(dtype=np.float64, copy=False)
    margins = values @ WEIGHTS + BIAS
    scores = 1.0 / (1.0 + np.exp(-np.clip(margins, -20.0, 20.0)))
    ctx.emit(pd.DataFrame({{"score": np.round(scores, 4)}}))
def run(ctx):
    batch = []
    while True:
        batch.append([ctx.amount or 0.0, ctx.mcc or 0.0, ctx.hour_val or 0.0,
                      ctx.is_cnp or 0.0, ctx.is_contactless or 0.0])
        if len(batch) == BATCH_SIZE:
            emit_batch(ctx, batch)
            batch.clear()
        if not ctx.next():
            break
    if batch:
        emit_batch(ctx, batch)
/'''


def _aggregate_stats(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    p50 = statistics.median(ordered)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return {"p50_ms": round(p50, 3), "p95_ms": round(p95, 3)}


def reference_score(features: tuple[float, ...], model: dict[str, Any]) -> float:
    z = sum(float(x) * float(w) for x, w in zip(features, model["weights"])) + float(model["bias"])
    return round(1.0 / (1.0 + math.exp(-max(min(z, 20.0), -20.0))), 4)


def benchmark(rows: int, iterations: int, warmups: int) -> dict[str, Any]:
    model, model_sha256 = load_model()
    ch_ddl, exa_ddl = make_udf_ddl(model)
    exa_pandas_ddl = make_exasol_pandas_set_ddl(model)
    ch, exa = connect()
    try:
        ch.command(ch_ddl)
        exa_rows(exa, "OPEN SCHEMA PAYLENS")
        exa_rows(exa, exa_ddl)
        exa_rows(exa, exa_pandas_ddl)
        ch_sql = f"""SELECT event_id, score_fraud_artifact(
            toFloat64(amount), toFloat64(merchant_id % 1000),
            toFloat64(toHour(event_time)), toFloat64(pos_entry_mode='CNP'),
            toFloat64(pos_entry_mode='CONTACTLESS')), toFloat64(amount),
            toFloat64(merchant_id % 1000), toFloat64(toHour(event_time)),
            toFloat64(pos_entry_mode='CNP'), toFloat64(pos_entry_mode='CONTACTLESS')
          FROM (SELECT event_id, amount, merchant_id, event_time, pos_entry_mode
                FROM paylens.auth_events ORDER BY toString(event_id) LIMIT {rows})"""
        exa_sql = f"""SELECT event_id, PAYLENS.score_fraud_artifact(
            CAST(amount AS DOUBLE), CAST(MOD(merchant_id, 1000) AS DOUBLE),
            CAST(HOUR(event_time) AS DOUBLE),
            CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
            CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END),
            CAST(amount AS DOUBLE), CAST(MOD(merchant_id, 1000) AS DOUBLE),
            CAST(HOUR(event_time) AS DOUBLE),
            CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
            CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END
          FROM (SELECT event_id, amount, merchant_id, event_time, pos_entry_mode
                FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows})"""
        ch_aggregate_sql = f"""SELECT COUNT(*), SUM(score_fraud_artifact(
            toFloat64(amount), toFloat64(merchant_id % 1000), toFloat64(toHour(event_time)),
            toFloat64(pos_entry_mode='CNP'), toFloat64(pos_entry_mode='CONTACTLESS')))
          FROM (SELECT amount, merchant_id, event_time, pos_entry_mode FROM paylens.auth_events
                ORDER BY toString(event_id) LIMIT {rows})"""
        ch_selection_baseline_sql = f"""SELECT COUNT(*), SUM(toFloat64(amount))
          FROM (SELECT amount FROM paylens.auth_events
                ORDER BY toString(event_id) LIMIT {rows})"""
        exa_source = f"""SELECT amount, merchant_id, event_time, pos_entry_mode
            FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows}"""
        exa_python_aggregate_sql = f"""SELECT COUNT(*), SUM(PAYLENS.score_fraud_artifact(
            CAST(amount AS DOUBLE), CAST(MOD(merchant_id, 1000) AS DOUBLE), CAST(HOUR(event_time) AS DOUBLE),
            CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
            CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END))
          FROM ({exa_source})"""
        exa_sql_aggregate_sql = f"""SELECT COUNT(*), SUM({make_exasol_sql_expression(model)})
          FROM ({exa_source})"""
        exa_selection_baseline_sql = f"""SELECT COUNT(*), SUM(CAST(amount AS DOUBLE))
          FROM (SELECT amount FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows})"""
        exa_pandas_aggregate_sql = f"""SELECT COUNT(*), SUM(score) FROM (
            SELECT PAYLENS.score_fraud_pandas(
                CAST(amount AS DOUBLE), CAST(MOD(merchant_id, 1000) AS DOUBLE),
                CAST(HOUR(event_time) AS DOUBLE),
                CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
                CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END)
            FROM (SELECT amount, merchant_id, event_time, pos_entry_mode
                  FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows}))"""

        def run_ch():
            result = ch.query(ch_sql).result_rows
            return result

        def run_exa():
            return exa_rows(exa, exa_sql)

        for _ in range(warmups):
            run_ch()
            run_exa()
        ch_times: list[float] = []
        exa_times: list[float] = []
        ch_result = exa_result = []
        for i in range(iterations):
            sequence = [("ch", run_ch), ("exa", run_exa)]
            if i % 2:
                sequence.reverse()
            for engine, fn in sequence:
                started = time.perf_counter()
                result = fn()
                elapsed = (time.perf_counter() - started) * 1000
                if engine == "ch":
                    ch_times.append(elapsed)
                    ch_result = result
                else:
                    exa_times.append(elapsed)
                    exa_result = result

        aggregate_queries = {
            "clickhouse_selection_baseline": lambda: ch.query(ch_selection_baseline_sql).result_rows[0],
            "clickhouse_sql_udf": lambda: ch.query(ch_aggregate_sql).result_rows[0],
            "exasol_selection_baseline": lambda: exa_rows(exa, exa_selection_baseline_sql)[0],
            "exasol_python_scalar_udf": lambda: exa_rows(exa, exa_python_aggregate_sql)[0],
            "exasol_sql_case": lambda: exa_rows(exa, exa_sql_aggregate_sql)[0],
            "exasol_vectorized_pandas_udf": lambda: exa_rows(exa, exa_pandas_aggregate_sql)[0],
        }
        for _ in range(warmups):
            for query in aggregate_queries.values():
                query()
        aggregate_latencies = {name: [] for name in aggregate_queries}
        aggregate_results = {}
        for iteration in range(iterations):
            calls = list(aggregate_queries.items())
            if iteration % 2:
                calls.reverse()
            for name, query in calls:
                started = time.perf_counter()
                aggregate_results[name] = query()
                aggregate_latencies[name].append((time.perf_counter() - started) * 1000)

        ch_by_id = {str(row[0]): (float(row[1]), tuple(float(v) for v in row[2:])) for row in ch_result}
        exa_by_id = {str(row[0]): (float(row[1]), tuple(float(v) for v in row[2:])) for row in exa_result}
        common_ids = ch_by_id.keys() & exa_by_id.keys()
        max_engine_error = max((abs(ch_by_id[i][0] - exa_by_id[i][0]) for i in common_ids), default=None)
        feature_errors = [
            max(abs(ch_by_id[i][1][j] - exa_by_id[i][1][j]) for j in range(5))
            for i in common_ids
        ]
        feature_mismatch_rows = sum(
            ch_by_id[i][1] != exa_by_id[i][1] for i in common_ids
        )
        hour_deltas = [
            ((exa_by_id[i][1][2] - ch_by_id[i][1][2] + 12) % 24) - 12
            for i in common_ids
        ]
        max_ch_reference_error = max((abs(ch_by_id[i][0] - reference_score(ch_by_id[i][1], model)) for i in common_ids), default=None)
        max_exa_reference_error = max((abs(exa_by_id[i][0] - reference_score(exa_by_id[i][1], model)) for i in common_ids), default=None)
        sample_ids = sorted(common_ids)[:3]
        sample_list = ", ".join("'" + key.replace("'", "''") + "'" for key in sample_ids)
        ch_time_samples = ch.query(
            "SELECT toString(event_id), toString(event_time) FROM paylens.auth_events "
            f"WHERE toString(event_id) IN ({sample_list}) ORDER BY toString(event_id)"
        ).result_rows if sample_ids else []
        exa_time_samples = exa_rows(
            exa,
            "SELECT EVENT_ID, CAST(EVENT_TIME AS VARCHAR(30)) FROM PAYLENS.AUTH_EVENTS "
            f"WHERE EVENT_ID IN ({sample_list}) ORDER BY EVENT_ID",
        ) if sample_ids else []
        p50_ch = statistics.median(ch_times)
        p50_exa = statistics.median(exa_times)
        return {
            "run_type": "live",
            "scorer": model["model_type"],
            "artifact": {"path": "udf/model_artifact/fraud_model.json", "version": model.get("version"), "sha256": model_sha256,
                         "feature_names": model["feature_names"]},
            "requested_rows": rows,
            "actual_rows": {"clickhouse": len(ch_result), "exasol": len(exa_result)},
            "warmups_per_engine": warmups,
            "iterations": iterations,
            "server_side_aggregate": {
                name: {
                    **_aggregate_stats(samples),
                    "result_count": int(aggregate_results[name][0]),
                    "sum_score": float(aggregate_results[name][1] or 0),
                }
                for name, samples in aggregate_latencies.items()
            },
            "clickhouse_sql_udf": {"p50_ms": round(p50_ch, 3), "p95_ms": round(sorted(ch_times)[max(0, int(iterations * .95 + .999) - 1)], 3),
                                   "predictions_per_second": round(len(ch_result) / (p50_ch / 1000), 2)},
            "exasol_python_udf": {"p50_ms": round(p50_exa, 3), "p95_ms": round(sorted(exa_times)[max(0, int(iterations * .95 + .999) - 1)], 3),
                                  "predictions_per_second": round(len(exa_result) / (p50_exa / 1000), 2)},
            "correctness": {"row_count_match": len(ch_result) == len(exa_result) == rows,
                "event_ids_match": ch_by_id.keys() == exa_by_id.keys(),
                "feature_vectors_match": feature_mismatch_rows == 0,
                "feature_mismatch_rows": feature_mismatch_rows,
                "max_abs_feature_error_for_event": max(feature_errors, default=None),
                "exasol_minus_clickhouse_hour_delta_range": [min(hour_deltas), max(hour_deltas)] if hour_deltas else None,
                "max_abs_engine_score_error": max_engine_error,
                "max_abs_clickhouse_vs_artifact_error": max_ch_reference_error,
                "max_abs_exasol_vs_artifact_error": max_exa_reference_error,
                "scores_match_artifact": max_ch_reference_error == 0 and max_exa_reference_error == 0},
            "timestamp_samples": {"clickhouse": [list(row) for row in ch_time_samples],
                "exasol": [list(row) for row in exa_time_samples]},
            "timestamp_normalization": {
                "contract": "Naive source timestamps are UTC; offset-aware timestamps are converted to UTC before loading."
            },
            "caveat": "This repository artifact is LogisticFraudScorer, not XGBoost. The same artifact weights and scoring behavior are applied in ClickHouse SQL and Exasol Python UDFs. Client-observed latency includes query and result transfer.",
        }
    finally:
        ch.close()
        exa.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live SQL/Python UDF scoring comparison")
    parser.add_argument("--rows", type=int, default=50000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.rows < 1 or args.iterations < 1 or args.warmups < 0:
        parser.error("rows and iterations must be positive; warmups must be nonnegative")
    result = benchmark(args.rows, args.iterations, args.warmups)
    out = args.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR", "results"), "udf_benchmark_results.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    print(f"[+] Live UDF results saved to {out}")


if __name__ == "__main__":
    main()
