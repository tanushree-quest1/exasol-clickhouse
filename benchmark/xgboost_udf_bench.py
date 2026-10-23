"""Train one deterministic XGBoost artifact and score it in both databases."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import struct
import time
from typing import Any

from benchmark.physical_design_bench import connect, exa_rows
from generator.data_generator import FinTechDataGenerator

FEATURE_NAMES = ["amount", "mcc", "hour_of_day", "is_cnp", "is_contactless"]
ARTIFACT_NAME = "xgboost_fraud_model.json"


def features(event) -> list[float]:
    return [float(event.amount), float(event.merchant_id % 1000), float(event.event_time.hour),
            float(event.pos_entry_mode == "CNP"), float(event.pos_entry_mode == "CONTACTLESS")]


def train_artifact(train_rows: int, artifact_path: str) -> tuple[dict[str, Any], Any, float]:
    import xgboost as xgb

    started = time.perf_counter()
    gen = FinTechDataGenerator(seed=999)
    gen.generate_dimensions(num_accounts=500, num_cards=1000, num_merchants=100, num_devices=200)
    events = gen.generate_events(num_events=train_rows, fraud_ratio=0.08)
    X = [features(event) for event in events]
    y = [int(event.is_fraud) for event in events]
    if len(set(y)) != 2:
        raise RuntimeError("Generated training rows must contain both fraud labels")
    model = xgb.XGBClassifier(
        n_estimators=24, max_depth=3, learning_rate=0.15, objective="binary:logistic",
        eval_metric="logloss", base_score=0.5, tree_method="hist", n_jobs=1,
        random_state=999, subsample=1.0, colsample_bytree=1.0,
    )
    model.fit(X, y)
    booster = model.get_booster()
    trees = [json.loads(tree) for tree in booster.get_dump(dump_format="json")]
    artifact = {"model_type": "XGBoostClassifier", "xgboost_version": xgb.__version__,
                "objective": "binary:logistic", "base_margin": 0.0,
                "feature_names": FEATURE_NAMES, "requested_training_rows": train_rows,
                "actual_training_rows": len(events),
                "parameters": {"n_estimators": 24, "max_depth": 3, "learning_rate": 0.15,
                    "tree_method": "hist", "random_state": 999, "n_jobs": 1, "base_score": 0.5},
                "trees": trees}
    os.makedirs(os.path.dirname(artifact_path) or ".", exist_ok=True)
    with open(artifact_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, separators=(",", ":"))
    return artifact, model, time.perf_counter() - started


def score_tree(node: dict[str, Any], row: list[float], *, float32: bool = True) -> float:
    leaf = tree_leaf_node(node, row, float32=float32)
    return float(leaf["leaf"])


def engine_split_threshold(value: float) -> float:
    """Return a double threshold that reproduces XGBoost's float32 comparison.

    XGBoost evaluates feature values and split thresholds as float32. The SQL
    and Exasol exports evaluate decimal input values as doubles, so exporting
    the printed threshold directly can flip values exactly on a float32
    boundary. A midpoint between the adjacent float32 values preserves the
    float32 branch for the two-decimal benchmark inputs.
    """
    import numpy as np

    threshold = np.float32(value)
    lower = np.nextafter(threshold, np.float32(-np.inf))
    return (float(lower) + float(threshold)) / 2.0


def tree_leaf_node(node: dict[str, Any], row: list[float], *, float32: bool = False) -> dict[str, Any]:
    while "leaf" not in node:
        feature = int(str(node["split"]).removeprefix("f"))
        value = float(row[feature])
        if float32:
            value = struct.unpack("f", struct.pack("f", value))[0]
        if math.isnan(value):
            child_id = node["missing"]
        else:
            child_id = node["yes"] if value < engine_split_threshold(node["split_condition"]) else node["no"]
        node = next(child for child in node["children"] if child["nodeid"] == child_id)
    return node


def diagnose_leaf_routing(
    artifact: dict[str, Any],
    model: Any,
    event_ids: list[str],
    feature_rows: list[list[float]],
    observed_scores: list[float],
    limit: int = 10,
) -> dict[str, Any]:
    import numpy as np
    import xgboost as xgb

    if not feature_rows:
        return {"rows_examined": 0, "worst_error_rows": []}
    matrix = np.asarray(feature_rows, dtype=np.float32)
    reference_scores = model.predict_proba(matrix)[:, 1]
    booster_leaf_ids = model.get_booster().predict(xgb.DMatrix(matrix), pred_leaf=True)
    if booster_leaf_ids.ndim == 1:
        booster_leaf_ids = booster_leaf_ids.reshape((-1, 1))
    errors = [abs(float(observed) - float(reference)) for observed, reference in zip(observed_scores, reference_scores)]
    worst = sorted(range(len(errors)), key=lambda index: errors[index], reverse=True)[:limit]
    records = []
    for index in worst:
        raw_mismatches = []
        float32_mismatches = []
        for tree_index, tree in enumerate(artifact["trees"]):
            expected_leaf_id = int(booster_leaf_ids[index][tree_index])
            raw_leaf = tree_leaf_node(tree, feature_rows[index])
            float32_leaf = tree_leaf_node(tree, feature_rows[index], float32=True)
            if int(raw_leaf["nodeid"]) != expected_leaf_id:
                raw_mismatches.append({
                    "tree_index": tree_index,
                    "xgboost_leaf_id": expected_leaf_id,
                    "exported_leaf_id": raw_leaf["nodeid"],
                    "exported_leaf_value": raw_leaf["leaf"],
                })
            if int(float32_leaf["nodeid"]) != expected_leaf_id:
                float32_mismatches.append({
                    "tree_index": tree_index,
                    "xgboost_leaf_id": expected_leaf_id,
                    "exported_leaf_id": float32_leaf["nodeid"],
                    "exported_leaf_value": float32_leaf["leaf"],
                })
        records.append({
            "event_id": event_ids[index],
            "features": feature_rows[index],
            "observed_score": observed_scores[index],
            "xgboost_library_score": float(reference_scores[index]),
            "absolute_error": errors[index],
            "nan_features": [
                FEATURE_NAMES[i] for i, value in enumerate(feature_rows[index])
                if math.isnan(value)
            ],
            "float64_routing_leaf_mismatches": raw_mismatches,
            "float32_routing_leaf_mismatches": float32_mismatches,
        })
    return {"rows_examined": len(feature_rows), "worst_error_rows": records}


def predict(artifact: dict[str, Any], row: list[float]) -> float:
    margin = float(artifact["base_margin"]) + sum(
        score_tree(tree, row, float32=True) for tree in artifact["trees"]
    )
    return 1.0 / (1.0 + math.exp(-max(min(margin, 40.0), -40.0)))


def _latency_stats(samples: list[float], rows: int) -> dict[str, float]:
    if not samples:
        raise ValueError("At least one latency sample is required")
    ordered = sorted(samples)
    p50 = statistics.median(ordered)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return {
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "predictions_per_second": round(rows / (p50 / 1000), 2),
    }


def tree_sql(node: dict[str, Any], args: list[str]) -> str:
    if "leaf" in node:
        return repr(float(node["leaf"]))
    feature = int(str(node["split"]).removeprefix("f"))
    yes = next(child for child in node["children"] if child["nodeid"] == node["yes"])
    no = next(child for child in node["children"] if child["nodeid"] == node["no"])
    threshold = engine_split_threshold(node["split_condition"])
    return f"if({args[feature]} < {threshold!r}, {tree_sql(yes, args)}, {tree_sql(no, args)})"


def tree_exasol_case_sql(node: dict[str, Any], args: list[str]) -> str:
    if "leaf" in node:
        return repr(float(node["leaf"]))
    feature = int(str(node["split"]).removeprefix("f"))
    children = {child["nodeid"]: child for child in node["children"]}
    yes = tree_exasol_case_sql(children[node["yes"]], args)
    no = tree_exasol_case_sql(children[node["no"]], args)
    missing = tree_exasol_case_sql(children[node["missing"]], args)
    threshold = engine_split_threshold(node["split_condition"])
    return (
        f"(CASE WHEN {args[feature]} IS NULL THEN {missing} "
        f"WHEN {args[feature]} < {threshold!r} THEN {yes} "
        f"ELSE {no} END)"
    )


def create_exasol_pandas_set_script(artifact: dict[str, Any]) -> str:
    trees = json.dumps(artifact["trees"], separators=(",", ":"))
    return f'''CREATE OR REPLACE PYTHON3 SET SCRIPT score_xgboost_pandas(
      amount DOUBLE, mcc DOUBLE, hour_val DOUBLE, is_cnp DOUBLE, is_contactless DOUBLE
    ) EMITS (score DOUBLE) AS
import json
import numpy as np
import pandas as pd
TREES = json.loads({trees!r})
BATCH_SIZE = 4096
def tree_values(node, frame):
    if "leaf" in node:
        return pd.Series(float(node["leaf"]), index=frame.index, dtype="float64")
    feature_index = int(node["split"][1:])
    values = frame.iloc[:, feature_index]
    missing = values.isna()
    yes_mask = (~missing) & (values < float(node["split_condition"]))
    no_mask = (~missing) & (~yes_mask)
    children = {{child["nodeid"]: child for child in node["children"]}}
    result = pd.Series(index=frame.index, dtype="float64")
    if missing.any():
        result.loc[missing] = tree_values(children[node["missing"]], frame.loc[missing])
    if yes_mask.any():
        result.loc[yes_mask] = tree_values(children[node["yes"]], frame.loc[yes_mask])
    if no_mask.any():
        result.loc[no_mask] = tree_values(children[node["no"]], frame.loc[no_mask])
    return result
def emit_batch(ctx, batch):
    frame = pd.DataFrame(batch, columns=["amount", "mcc", "hour_val", "is_cnp", "is_contactless"])
    margin = sum((tree_values(tree, frame) for tree in TREES), pd.Series(0.0, index=frame.index))
    scores = 1.0 / (1.0 + np.exp(-np.clip(margin.to_numpy(), -40.0, 40.0)))
    ctx.emit(pd.DataFrame({{"score": scores}}))
def run(ctx):
    batch = []
    while True:
        batch.append([ctx.amount, ctx.mcc, ctx.hour_val, ctx.is_cnp, ctx.is_contactless])
        if len(batch) == BATCH_SIZE:
            emit_batch(ctx, batch)
            batch.clear()
        if not ctx.next():
            break
    if batch:
        emit_batch(ctx, batch)
/'''


def create_udf_sql(artifact: dict[str, Any]) -> tuple[str, str]:
    args = ["amount", "mcc", "hour_val", "is_cnp", "is_contactless"]
    margin = " + ".join(f"({tree_sql(tree, args)})" for tree in artifact["trees"])
    ch = f"""CREATE OR REPLACE FUNCTION score_xgboost AS ({', '.join(args)}) ->
      1.0 / (1.0 + exp(-greatest(-40.0, least(40.0, ({margin})))))"""
    trees_json = json.dumps(artifact["trees"], separators=(",", ":"))
    py_body = f'''import json, math
TREES = json.loads({trees_json!r})
def leaf(node, row):
    while "leaf" not in node:
        i = int(node["split"][1:])
        if row[i] is None or math.isnan(row[i]):
            child_id = node["missing"]
        else:
            child_id = node["yes"] if row[i] < float(node["split_condition"]) else node["no"]
        node = next(c for c in node["children"] if c["nodeid"] == child_id)
    return float(node["leaf"])
def run(ctx):
    row = [ctx.amount, ctx.mcc, ctx.hour_val, ctx.is_cnp, ctx.is_contactless]
    margin = sum(leaf(tree, row) for tree in TREES)
    return 1.0 / (1.0 + math.exp(-max(min(margin, 40.0), -40.0)))
/'''
    exa = f'''CREATE OR REPLACE PYTHON3 SCALAR SCRIPT score_xgboost(
      amount DOUBLE, mcc DOUBLE, hour_val DOUBLE, is_cnp DOUBLE, is_contactless DOUBLE
    ) RETURNS DOUBLE AS
{py_body}'''
    return ch, exa


def benchmark(rows: int, train_rows: int, iterations: int, warmups: int, results_dir: str) -> dict[str, Any]:
    artifact_path = os.path.join(results_dir, ARTIFACT_NAME)
    artifact, model, training_seconds = train_artifact(train_rows, artifact_path)
    artifact_bytes = open(artifact_path, "rb").read()
    ch_ddl, exa_ddl = create_udf_sql(artifact)
    ch, exa = connect()
    try:
        ch.command(ch_ddl)
        exa_rows(exa, "OPEN SCHEMA PAYLENS")
        exa_rows(exa, exa_ddl)
        exa_rows(exa, create_exasol_pandas_set_script(artifact))
        ch_sql = f"""SELECT event_id, score_xgboost(toFloat64(amount), toFloat64(merchant_id % 1000),
          toFloat64(toHour(event_time)), toFloat64(pos_entry_mode='CNP'), toFloat64(pos_entry_mode='CONTACTLESS')),
          toFloat64(amount), toFloat64(merchant_id % 1000), toFloat64(toHour(event_time)),
          toFloat64(pos_entry_mode='CNP'), toFloat64(pos_entry_mode='CONTACTLESS')
          FROM (SELECT event_id, amount, merchant_id, event_time, pos_entry_mode FROM paylens.auth_events ORDER BY toString(event_id) LIMIT {rows})"""
        exa_sql = f"""SELECT event_id, PAYLENS.score_xgboost(CAST(amount AS DOUBLE), CAST(MOD(merchant_id,1000) AS DOUBLE),
          CAST(HOUR(event_time) AS DOUBLE), CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
          CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END), CAST(amount AS DOUBLE),
          CAST(MOD(merchant_id,1000) AS DOUBLE), CAST(HOUR(event_time) AS DOUBLE),
          CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
          CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END
          FROM (SELECT event_id, amount, merchant_id, event_time, pos_entry_mode FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows})"""
        ch_aggregate_sql = f"""SELECT COUNT(*), SUM(score_xgboost(
            toFloat64(amount), toFloat64(merchant_id % 1000), toFloat64(toHour(event_time)),
            toFloat64(pos_entry_mode='CNP'), toFloat64(pos_entry_mode='CONTACTLESS')))
          FROM (SELECT amount, merchant_id, event_time, pos_entry_mode FROM paylens.auth_events
                ORDER BY toString(event_id) LIMIT {rows})"""
        ch_selection_baseline_sql = f"""SELECT COUNT(*), SUM(toFloat64(amount))
          FROM (SELECT amount FROM paylens.auth_events
                ORDER BY toString(event_id) LIMIT {rows})"""
        exa_source = f"""SELECT amount, MOD(merchant_id,1000) AS mcc, HOUR(event_time) AS hour_val,
            CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END AS is_cnp,
            CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END AS is_contactless
            FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows}"""
        exa_python_aggregate_sql = f"""SELECT COUNT(*), SUM(PAYLENS.score_xgboost(
            CAST(amount AS DOUBLE), CAST(mcc AS DOUBLE), CAST(hour_val AS DOUBLE), is_cnp, is_contactless))
          FROM ({exa_source})"""
        exa_selection_baseline_sql = f"""SELECT COUNT(*), SUM(CAST(amount AS DOUBLE))
          FROM (SELECT amount FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows})"""
        exa_margin_sql = " + ".join(
            tree_exasol_case_sql(tree, ["CAST(amount AS DOUBLE)", "CAST(mcc AS DOUBLE)",
                                        "CAST(hour_val AS DOUBLE)", "is_cnp", "is_contactless"])
            for tree in artifact["trees"]
        )
        exa_case_sql = f"""SELECT COUNT(*), SUM(1.0 / (1.0 + EXP(-GREATEST(-40.0, LEAST(40.0, ({exa_margin_sql}))))))
          FROM ({exa_source})"""
        exa_pandas_sql = f"""SELECT COUNT(*), SUM(score) FROM (
            SELECT PAYLENS.score_xgboost_pandas(
                CAST(amount AS DOUBLE), CAST(MOD(merchant_id,1000) AS DOUBLE),
                CAST(HOUR(event_time) AS DOUBLE),
                CASE WHEN pos_entry_mode='CNP' THEN 1.0 ELSE 0.0 END,
                CASE WHEN pos_entry_mode='CONTACTLESS' THEN 1.0 ELSE 0.0 END)
            FROM (SELECT amount, merchant_id, event_time, pos_entry_mode
                  FROM PAYLENS.auth_events ORDER BY event_id LIMIT {rows}))"""

        def run_ch(): return ch.query(ch_sql).result_rows
        def run_exa(): return exa_rows(exa, exa_sql)
        for _ in range(warmups):
            run_ch(); run_exa()
        ch_times, exa_times = [], []
        ch_result = exa_result = []
        for i in range(iterations):
            calls = [("ch", run_ch), ("exa", run_exa)]
            if i % 2: calls.reverse()
            for engine, call in calls:
                started = time.perf_counter(); result = call(); elapsed = (time.perf_counter()-started)*1000
                if engine == "ch": ch_times.append(elapsed); ch_result = result
                else: exa_times.append(elapsed); exa_result = result
        aggregate_queries = {
            "clickhouse_selection_baseline": lambda: ch.query(ch_selection_baseline_sql).result_rows[0],
            "clickhouse_sql_udf": lambda: ch.query(ch_aggregate_sql).result_rows[0],
            "exasol_selection_baseline": lambda: exa_rows(exa, exa_selection_baseline_sql)[0],
            "exasol_python_scalar_udf": lambda: exa_rows(exa, exa_python_aggregate_sql)[0],
            "exasol_sql_case": lambda: exa_rows(exa, exa_case_sql)[0],
            "exasol_vectorized_pandas_udf": lambda: exa_rows(exa, exa_pandas_sql)[0],
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
                aggregate_latencies[name].append((time.perf_counter()-started)*1000)
        ch_map = {str(row[0]): (float(row[1]), [float(v) for v in row[2:]]) for row in ch_result}
        exa_map = {str(row[0]): (float(row[1]), [float(v) for v in row[2:]]) for row in exa_result}
        common = ch_map.keys() & exa_map.keys()
        common_ids = sorted(common)
        feature_mismatch_rows = sum(ch_map[k][1] != exa_map[k][1] for k in common_ids)
        feature_error_by_position = [
            max((abs(ch_map[k][1][j] - exa_map[k][1][j]) for k in common_ids), default=None)
            for j in range(len(FEATURE_NAMES))
        ]
        hour_deltas = [
            ((exa_map[k][1][2] - ch_map[k][1][2] + 12) % 24) - 12
            for k in common_ids
        ]
        if common_ids:
            ch_features = [ch_map[k][1] for k in common_ids]
            exa_features = [exa_map[k][1] for k in common_ids]
            ch_reference_probs = model.predict_proba(ch_features)[:, 1]
            exa_reference_probs = model.predict_proba(exa_features)[:, 1]
            ch_reference = {key: float(prob) for key, prob in zip(common_ids, ch_reference_probs)}
            exa_reference = {key: float(prob) for key, prob in zip(common_ids, exa_reference_probs)}
            ch_error = max((abs(ch_map[k][0]-ch_reference[k]) for k in common_ids), default=None)
            exa_error = max((abs(exa_map[k][0]-exa_reference[k]) for k in common_ids), default=None)
            engine_error = max((abs(ch_map[k][0]-exa_map[k][0]) for k in common), default=None)
            dump_error = max((abs(predict(artifact, ch_map[k][1])-ch_reference[k]) for k in common_ids), default=None)
            worst_ids = sorted(
                common_ids,
                key=lambda key: abs(ch_map[key][0] - ch_reference[key]),
                reverse=True,
            )[:10]
            leaf_diagnostics = diagnose_leaf_routing(
                artifact, model, worst_ids,
                [ch_map[key][1] for key in worst_ids],
                [ch_map[key][0] for key in worst_ids],
                limit=len(worst_ids),
            )
        else:
            ch_error = exa_error = engine_error = dump_error = None
            leaf_diagnostics = {"rows_examined": 0, "worst_error_rows": []}
        return {"run_type":"live", "model_type":artifact["model_type"], "requested_training_rows":train_rows,
            "actual_training_rows":artifact["actual_training_rows"],
            "model_training_seconds":round(training_seconds,3), "scored_rows":rows, "warmups":warmups, "iterations":iterations,
            "artifact":os.path.basename(artifact_path), "artifact_sha256":hashlib.sha256(artifact_bytes).hexdigest(),
            "xgboost_version":artifact["xgboost_version"], "percentile_method":"nearest_rank",
            "clickhouse_sql_udf":_latency_stats(ch_times, rows),
            "exasol_python_udf":_latency_stats(exa_times, rows),
            "server_side_aggregate": {
                name: {
                    **_latency_stats(samples, rows),
                    "result_count": int(aggregate_results[name][0]),
                    "sum_score": float(aggregate_results[name][1] or 0),
                }
                for name, samples in aggregate_latencies.items()
            },
            "correctness":{"row_counts_match":len(ch_result)==len(exa_result)==rows,"event_ids_match":ch_map.keys()==exa_map.keys(),
                "feature_vectors_match":feature_mismatch_rows == 0,
                "feature_mismatch_rows":feature_mismatch_rows,
                "max_abs_feature_error_by_feature":dict(zip(FEATURE_NAMES, feature_error_by_position)),
                "exasol_minus_clickhouse_hour_delta_range":[min(hour_deltas),max(hour_deltas)] if hour_deltas else None,
                "max_abs_engine_error":engine_error,"max_abs_clickhouse_vs_model":ch_error,"max_abs_exasol_vs_model":exa_error,
                "max_abs_tree_dump_vs_xgboost":dump_error,
                "xgboost_leaf_routing_diagnostics": leaf_diagnostics,
                "scores_within_1e_6":all(v is not None and v <= 1e-6 for v in (engine_error,ch_error,exa_error,dump_error))},
            "caveat":"Model is trained deterministically from generated synthetic labels for this run. SQL and Python UDFs evaluate the exported XGBoost tree dump; client-observed timing includes result transfer. Single-node local systems only."}
    finally:
        ch.close(); exa.close()


def main() -> None:
    p=argparse.ArgumentParser(description="Train and compare XGBoost UDF scoring in ClickHouse and Exasol")
    p.add_argument("--rows",type=int,default=50000); p.add_argument("--train-rows",type=int,default=10000)
    p.add_argument("--iterations",type=int,default=5); p.add_argument("--warmups",type=int,default=2)
    p.add_argument("--output",default=None); a=p.parse_args()
    if min(a.rows,a.train_rows,a.iterations)<1 or a.warmups<0: p.error("row counts and iterations must be positive; warmups nonnegative")
    out=a.output or os.path.join(os.getenv("PAYLENS_RESULTS_DIR","results"),"xgboost_udf_results.json")
    os.makedirs(os.path.dirname(out) or ".",exist_ok=True)
    result=benchmark(a.rows,a.train_rows,a.iterations,a.warmups,os.path.dirname(out) or ".")
    with open(out,"w",encoding="utf-8") as f: json.dump(result,f,indent=2)
    print(json.dumps(result,indent=2)); print(f"[+] XGBoost UDF results saved to {out}")


if __name__ == "__main__": main()
