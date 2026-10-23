# PayLens: ClickHouse vs Exasol comparative analysis

**Status:** Preliminary live baseline, not a complete platform evaluation  
**Reviewed:** 2026-10-23  
**Workload:** FinTech authorization events, up to 1,000,047 generated rows  
**Environment:** Local ClickHouse container and local single-node Exasol Personal Nano

> **Coverage warning:** Results are local single-node observations, not production or multi-node scaling claims. The 2026-10-23 repeated runs confirm the relative query, ingest, Kafka, and UDF rankings, while absolute times vary materially. The current loader code normalizes Exasol timestamps to UTC; older loaded data still contains the observed +05:30 offset and must be reloaded before cross-engine date, velocity, rollup, TTL, or UDF comparisons are treated as valid.

## 2026-10-23 correction addendum

The repeated live results change three earlier interpretations:

- Kafka reverses in the throttled 9-cell sweep: ClickHouse is at least as fast in every cell and leads clearly in six. The earlier unthrottled Exasol lead measured backlog and host/consumer scheduling effects. Achieved producer rates must be reported alongside requested targets; each cell is a single run.
- XGBoost's maximum library-reference error is explained by float-boundary routing at amounts 49.24 and 10.63. The exporter now uses a midpoint between adjacent float32 thresholds so the SQL and Exasol double comparisons reproduce XGBoost's float32 branch semantics. Rerun before marking the model parity result complete.
- Natural TTL cleanup is observed around 40 seconds despite a 10-second retention value. The measured post-expiry visibility lag is approximately one second; most elapsed time is before the cutoff. ClickHouse TTL settings and expiry-relative timing must be recorded before comparing retention behavior.

The 1M-row repeated ratios are approximately 1.19x for bulk ingest, 1.69x for Q2, 7.75x for Q3, 4.35x for Q4, 1.72x in Exasol's favor for Q1, 5.8x for logistic UDF, 10.6x for XGBoost UDF, and 1.6x for the 10k-row update. Report these ratios across runs; do not treat the absolute timings as stable.

The 2026-10-23 physical-design probe after rebuilding selected 12 of 123 granules for the card lookup, read 108,771 rows, and recorded `auth_events`-only storage. A date probe matching 73% of the data is not selective; future date probes should target 1–5% ranges and aggregate `sum(amount)`. The legacy Q3 query is primarily a high-cardinality string-keyed aggregation over tiny dimensions, not a pure five-table join. The harness now adds Q3a join-only and Q3b integer-ID aggregation variants; a large-dimension fixture remains future work.

The loader and query harness changes are now in the working tree: Exasol timestamps are formatted as UTC, and query result mismatches are captured at value level. The 2026-10-23 query artifact that failed during JSON serialization predates the bytes normalization fix; rerun it after reloading both engines. Benchmark JSONs should additionally capture run IDs, engine/client versions, and SQL text before being used as final evidence.

## What was actually measured

Two historical runs and repeated 2026-10-23 live runs were recorded. Query runs used 2 warmups and 5 measured iterations. The current harness compares returned values as well as row counts; older saved query artifacts only contain row-count checks and must not be treated as value-validated.

| Requested normal rows | Actual generated events | ClickHouse ingest rows/s | Exasol ingest rows/s | ClickHouse total ingest | Exasol total ingest |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 100,047 | 97,278.8 | 69,118.7 | 1.028 s | 1.447 s |
| 1,000,000 | 1,000,047 | 105,420.6 | 87,558.6 | 9.486 s | 11.421 s |

ClickHouse led the observed bulk-ingest runs by approximately **1.41×** at 100k and **1.20×** at 1M events. These are single-run results, without a batch-size sweep or repeated independent run series; do not infer stable throughput scaling from them.

## Query timings

Values are p50 milliseconds from the warm, interleaved 5-iteration runs. Lower is faster. Both engines returned the same number of rows for each query in each run.

| Query | Result rows (both) | 100k ClickHouse | 100k Exasol | 100k faster | 1M ClickHouse | 1M Exasol | 1M faster |
|---|---:|---:|---:|---|---:|---:|---|
| Q1 Card velocity | 1 | 4.75 ms | 3.60 ms | Exasol 1.32× | 8.34 ms | 3.87 ms | Exasol 2.16× |
| Q2 Merchant rollup | 20 | 6.53 ms | 8.85 ms | ClickHouse 1.36× | 25.48 ms | 43.15 ms | ClickHouse 1.69× |
| Q3 Five-table fraud join | 50 | 17.32 ms | 66.95 ms | ClickHouse 3.87× | 54.36 ms | 438.74 ms | ClickHouse 8.07× |
| Q4 JSON extraction | 4 | 10.34 ms | 27.77 ms | ClickHouse 2.69× | 51.94 ms | 227.99 ms | ClickHouse 4.39× |

## Supplemental live probes (2026-10-22)

The latest completed Kafka stage is in [`20261022T093500`](runs/20261022T093500/). It sent and persisted exactly 10,000 rows per sink. ClickHouse measured 8,652 rows/s with 1,081.415 ms p50, 1,212.077 ms p95, and 1,234.162 ms p99 end-to-end latency; Exasol measured 10,195 rows/s with 912.061 ms p50, 1,035.246 ms p95, and 1,054.484 ms p99. The producer measured 11,837 rows/s. Row-count checks all passed. These are single-run local measurements; sink throughput measures database insert time while end-to-end latency starts at producer enqueue. See the [Kafka JSON](runs/20261022T093500/kafka_benchmark_results.json).

The corrected natural TTL run [`20261022T094112`](runs/20261022T094112/natural_ttl_results.json) observed the cutoff before completion. ClickHouse background TTL removed the eight expired rows in 39,462 ms; Exasol's polled `DELETE` + `COMMIT` removed them in 38,427 ms. Both retained the eight fresh rows. This is one short synthetic run; ClickHouse merge scheduling is nondeterministic and physical disk reclamation was not measured.

The logistic UDF run [`20261022T094533`](runs/20261022T094533/udf_benchmark_results.json) scored 50,000 rows in 49.995 ms p50 for ClickHouse and 253.177 ms for Exasol. Event IDs and normalized feature vectors matched; both implementations matched the checked-in artifact exactly. Raw samples showed Exasol timestamps 05:30 ahead of ClickHouse UTC, so the Exasol feature expression subtracts 19,800 seconds by default; override `EXASOL_TIMESTAMP_OFFSET_SECONDS` if the deployment differs.

The generated XGBoost run [`20261022T094533`](runs/20261022T094533/xgboost_udf_results.json) scored 50,000 rows in 57.711 ms p50 for ClickHouse and 549.302 ms for Exasol. The feature vectors matched and engine outputs differed by at most 3.97e-14. However, both differed from `XGBClassifier.predict_proba` by up to 0.016875, so the exported SQL/Python tree scoring is not proven equivalent to the training model. Treat this as an engine-to-engine execution measurement of the same exported tree expression, not validated model scoring.

The successful feature-only run [`20261022T080633`](runs/20261022T080633/physical_design_results.json) used the existing 1,000,047-event dataset. Both engines had matching row counts. The selective date filter targeted `2026-10-22`.

| Probe | ClickHouse | Exasol | Interpretation |
|---|---:|---:|---|
| Date-filter client elapsed | 6.707 ms | 3.110 ms | One measured invocation; not a stable latency comparison. |
| Date-filter work evidence | 500,047 rows read; 1,000,094 bytes; 2 of 3 parts and 63 of 125 granules selected. EXPLAIN shows 3/3 parts for the Partition index and 2/3 for Statistics. | Profile shows `SCAN ... WITH ZONEMAP` on `AUTH_EVENTS`; object rows 1,000,047, output rows 135,564; remarks identify `AUTH_EVENTS(EVENT_DATE)`. | Both plans show filtering work, but Exasol and ClickHouse profile counters have different semantics. ClickHouse did not eliminate a whole partition in this probe. |
| Card lookup (`card_id=1533`) | 3.069 ms; 1,000,000 rows / 8,000,000 bytes read. Bloom skip index selected 124/125 granules, so it barely pruned this lookup. | Not a paired point-lookup test in this probe. | The existing ClickHouse primary key is ordered by `event_id`, so it does not narrow a `card_id` lookup. |
| Add Bloom index after loading data | Materializing `idx_card_bloom` on a 1,000,000-row scratch copy took 14.334 ms. EXPLAIN showed the Skip index before and after materialization but still selected all 123/123 granules. | n/a | Explicit DDL/materialization was exercised; the index did not demonstrate selectivity for the chosen probe value/data. This is not evidence of a useful speedup. |
| Exasol automatic join-index evidence | n/a | First join 94.397 ms; warm join 88.397 ms. Profiles show local `CARD_ID` index and global indexes for `ACCOUNT_ID`, `MERCHANT_ID`, and `DEVICE_ID`; matching index metadata rows are present. | Confirms the optimizer used indexes for this join. Two runs do not isolate index creation cost or prove broader automatic-index benefit. |

Storage, TTL, and mutation details from this run are also recorded below. The earlier failed feature run [`20261022T080405`](runs/20261022T080405/physical_design_results.json) is retained as a failure record and is superseded by the successful physical-design run.

| Metric | ClickHouse | Exasol | Interpretation |
|---|---:|---:|---|
| Raw size reported | 203,190,910 bytes | 232,804,896 bytes | Engine-specific accounting; not directly comparable as physical disk usage. |
| Compressed / memory size reported | 41,769,940 bytes | 41,112,358 bytes | ClickHouse data-part bytes on disk vs Exasol in-memory object size at last commit. |
| Indicative ratio | 4.865× | 5.663× | Different accounting semantics; do not declare a compression winner from this alone. |
| TTL scratch cleanup elapsed | 4.139 ms | 12.680 ms | ClickHouse forced `OPTIMIZE FINAL` vs Exasol explicit `DELETE` + `COMMIT`; two-row scratch test only. |
| TTL scratch rows remaining | 1 | 1 | Each removed the injected expired row. The separate natural watcher later observed cleanup at about 39 seconds for both workflows; it does not measure physical disk reclamation. |

The isolated 10,000-row mutation probe is recorded in [`20261022T080633/update_benchmark_results.json`](runs/20261022T080633/update_benchmark_results.json). ClickHouse synchronous `ALTER UPDATE` took 6.357 ms, Exasol `UPDATE` took 13.780 ms, and ClickHouse `ReplacingMergeTree` appended 10,000 versions in 57.037 ms with a 2.792 ms `FINAL` read. All three paths returned 10,000 settled rows. This synthetic scratch test does not establish transaction isolation or production ledger correctness.

The 1M event query run shows Exasol ahead for the point lookup and ClickHouse ahead for the rollup, five-table join, and JSON aggregation. Q3 and Q4 widen notably at this scale. These observations are hypotheses for the tested machine, schema, data, engine versions, and driver paths. Query time includes the client request and fetching the small result set; it is not an isolated server CPU time. The run records p95 too, but the console summary above intentionally focuses on p50.

## Metrics coverage against the requested comparison

| Area from the requested analysis | Current status | What is still needed for a defensible comparison |
|---|---|---|
| FinTech bulk ingestion | **Measured baseline** | Repeat independent runs, sweep batch size, report run variance, and measure streaming separately. |
| Point lookup, aggregation, star join, JSON | **Measured baseline** | Compare full result values, capture engine plans and rows/bytes scanned, repeat across sizes, report p50/p95 and variance. |
| Partitioning and pruning | **Measured; limited pruning observed** | ClickHouse selected 2/3 parts and 63/125 granules for the date filter, while the Partition index showed 3/3 parts. Exasol profile showed a zonemap scan. These counters are not directly equivalent; repeat with controlled date distributions and report per-partition selectivity. |
| Indexing: automatic vs configured | **Measured on tested probes** | Exasol join profile shows local/global index paths. ClickHouse Bloom index materialization completed but the test lookup read 1M rows and the scratch index selected all 123 granules. Test more selective data and predicates before judging index utility. |
| TTL/data lifecycle | **Forced and natural cleanup measured** | Natural watcher observed cleanup at 39,462 ms for ClickHouse background TTL and 38,427 ms for Exasol polled DELETE+COMMIT; both retained eight fresh rows. One synthetic run only; physical disk reclaim and production scheduler effort remain unmeasured. |
| Ledger UPDATE/MERGE and correctness | **Measured on 10,000 synthetic scratch rows** | All paths reached expected row counts; test isolation, rollback, and actual ledger invariants separately. |
| Storage and compression | **Measured from live system tables** | Reported size semantics differ (ClickHouse disk bytes vs Exasol in-memory object size); do not treat ratios as directly comparable. |
| Kafka/streaming ingestion | **One paired live run; row counts match** | Each sink persisted 10,000 rows. ClickHouse: 8,652 rows/s and 1,081.415 ms p50; Exasol: 10,195 rows/s and 912.061 ms p50. This is a single run; longer sustained lag/reliability tests remain. |
| UDF/ML scoring | **Measured; XGBoost parity open** | Logistic feature parity and artifact correctness pass after the measured +05:30 timestamp normalization. XGBoost outputs match between engines but differ from the library reference by up to 0.016875; model correctness remains unresolved. |
| Ease of use and documentation quality | **Study protocol defined; not empirically measured** | Recruit participants and run the counterbalanced tasks in [`../audit/usability_study.md`](../audit/usability_study.md); record time, steps, errors, docs consulted, tuning changes, and correctness. |
| Observability and ecosystem | **Documentation inventory only** | Perform selected tasks on each engine (query profile, metrics, driver and integration setup) and capture versioned evidence. |

For the detailed audit, see [the documentation comparison](../audit/documentation_comparison.md) and [the experiment coverage checklist](../audit/coverage_checklist.md). The repo also includes a [static results dashboard](dashboard.html).

The live physical-design and ledger probes run during a full `./run_experiment.sh` invocation. To rerun them against existing loaded data without truncating benchmark tables, use `./run_experiment.sh --features-only`. Add `--all` for Kafka, saved logistic, generated XGBoost, and natural-retention stages. The successful 1,000,047-row physical-design run is `20261022T080633`; the later `20261022T094533` run contains the logistic and XGBoost UDF results. Earlier failed probe attempts are preserved under their timestamped run folders.

## Sources for the recorded live results

- 100k ingest: [`20261021T142638/insert_benchmark_results.json`](runs/20261021T142638/insert_benchmark_results.json)
- 100k queries: [`20261021T144741/query_benchmark_results.json`](runs/20261021T144741/query_benchmark_results.json)
- 1M ingest: [`20261021T150455/insert_benchmark_results.json`](runs/20261021T150455/insert_benchmark_results.json)
- 1M queries: [`20261021T150455/query_benchmark_results.json`](runs/20261021T150455/query_benchmark_results.json)
- Feature probes: [`20261022T080405/physical_design_results.json`](runs/20261022T080405/physical_design_results.json) and [`20261022T080405/update_benchmark_results.json`](runs/20261022T080405/update_benchmark_results.json)
- Successful physical-design probes: [`20261022T080633/physical_design_results.json`](runs/20261022T080633/physical_design_results.json) and [`20261022T080633/update_benchmark_results.json`](runs/20261022T080633/update_benchmark_results.json)
- Latest paired Kafka sink: [`20261022T093500/kafka_benchmark_results.json`](runs/20261022T093500/kafka_benchmark_results.json)
- Natural TTL watcher: [`20261022T094112/natural_ttl_results.json`](runs/20261022T094112/natural_ttl_results.json)
- Logistic and XGBoost UDFs: [`20261022T094533/udf_benchmark_results.json`](runs/20261022T094533/udf_benchmark_results.json) and [`20261022T094533/xgboost_udf_results.json`](runs/20261022T094533/xgboost_udf_results.json)

The saved benchmark JSON does not capture complete hardware, memory limits, engine settings, or independently repeated run distributions. Add those metadata before treating this as a benchmark report for a product decision.
