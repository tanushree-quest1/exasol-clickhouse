# PayLens Experiment Coverage Checklist

This checklist records what the repository can currently support and what still needs a live, repeatable experiment. “Documentation-only” means a claim is not backed by a measurement from this project. The live run records for 100k and 1M are summarized in [comparative_analysis.md](../results/comparative_analysis.md); older top-level JSON remains legacy material.

## Venn view

| Hands-on experiments | Verified by both code and docs | Documentation and usability audit |
|---|---|---|
| Live bulk ingest and four query timings are recorded at 100k and 1M; storage, date-filter profiles, index behavior, TTL, and synthetic mutations have supplemental results. | Live plans/profiles now show date-filter and index behavior, though the engine counters differ and the tested ClickHouse skip index barely pruned. | A scoped, dated vendor documentation comparison exists; a task-based documentation and usability study is still needed. |
| Older query artifacts compare row counts only; the current query harness compares returned values and records sampled mismatches. | Re-run after the UTC data reload because historical Exasol timestamps are offset by +05:30. | Older feature/usability matrices contain draft claims; consult the scoped source-linked comparison and do not treat draft opinions as findings. |
| Storage, partition/index behavior, forced and natural TTL, synthetic ledger mutations, Kafka sinks, logistic UDF, and XGBoost UDF have live results. Logistic artifact parity passes after timestamp normalization; XGBoost outputs match across engines but differ from the library reference by up to 0.016875. | Core harnesses produced live measurements, with XGBoost model parity still open and repeat-run/scale coverage outstanding. | Observability inventory and task-based usability remain documentation/manual work. |

## Feature status

| Feature | Hands-on status | Documentation status | Work needed before treating as a finding |
|---|---|---|---|
| Shared schemas and generated FinTech workload | **Partial** — schemas and deterministic generator exist; live baseline seeds dimensions and authorization events. | **Partial** | Record engine versions, hardware/runtime constraints, generated row counts, and validate equal data/results. |
| Bulk ingestion | **Measured live baseline** at 100k and 1M generated events. | **Partial** | Add independent repeat runs, batch-size sweep, environment metadata, and variance. |
| Streaming/Kafka | **Measured live once** — paired consumers each persisted exactly 10,000 rows; sink throughput, enqueue-to-sink p50/p95/p99, and row-count parity were captured. | **Partial** | Repeat for variance and run a longer sustained stream to measure lag and reliability. |
| Point lookup, aggregation, joins, JSON extraction | **Measured live baseline** at 100k and repeated 1M runs; two warmups, five measured runs, p50/p95. The current harness compares returned values and row counts. | **Partial** | Reload Exasol with UTC timestamps, capture run IDs/versions/SQL, and retain independent repeat distributions. |
| Ledger UPDATE/MERGE | **Measured live on 10,000 synthetic scratch rows** — ClickHouse synchronous mutation and versioned append/read against Exasol UPDATE reached expected row counts. | **Partial** | Separately test transaction isolation, rollback, and application-level ledger correctness before any ACID recommendation. |
| Storage/compression | **Measured live** — ClickHouse parts and Exasol object-size statistics captured. | **Partial** | Size meanings differ (disk bytes vs in-memory object size); record codecs, merge state, and accounting before treating ratios as comparable. |
| Partitioning and pruning | **Measured live on 1,000,047 rows** — ClickHouse selected 2/3 parts and 63/125 granules for the date filter, but its Partition index showed 3/3 parts; Exasol profile showed a `WITH ZONEMAP` scan. | **Partial** | Repeat with controlled date distributions and report per-partition selectivity; engine counters differ and Nano is single-node. |
| Indexing | **Measured live on tested probes** — Exasol join profile shows local `CARD_ID` and global `ACCOUNT_ID`, `MERCHANT_ID`, `DEVICE_ID` index paths. ClickHouse Bloom materialization took 14.334 ms on 1M copied rows but selected all 123 granules for the tested lookup. | **Documented in comparison** | Test more selective data/predicates and write overhead; do not infer general index effectiveness from this probe. |
| TTL/retention | **Forced and natural visibility measured** — the natural watcher observed eight expired rows removed and eight fresh rows retained by each engine. | **Partial** | One short synthetic run only; repeat across runs, separately measure physical disk reclamation, and implement/measure a production scheduler. |
| UDF/ML scoring | **Both UDF benchmarks ran live.** Logistic features and artifact outputs match after timestamp normalization. XGBoost outputs match engine-to-engine but differ from the XGBoost library reference by up to 0.016875. | **Partial** | Resolve XGBoost model parity before treating it as correct scoring; repeat with a production-trained artifact. Python SLC and macOS OpenMP setup are prerequisites. |
| Documentation completeness and feature gaps | **Not an empirical test** | **Scoped comparison exists** in `documentation_comparison.md`, reviewed 2026-10-22. | Extend version-specific source review to every feature and perform a controlled documentation task study; absence from this review is not proof of unsupported functionality. |
| Ease of use / developer ergonomics | **Not measured** | **Opinion-only scorecard exists; controlled study protocol added** in `usability_study.md`. | Recruit participants and run the same tasks in counterbalanced order; record time, commands, errors, docs use, and correctness. |
| Observability and ecosystem | **Not measured** | **Prose only** | Inventory official integrations and execute selected tasks (metrics/query profile, driver, Kafka/BI connector) with version and source evidence. |

## Scope and interpretation

## Repository scan (2026-10-22)

| Repository area | What exists now | Measurement status |
|---|---|---|
| `benchmark/insert_bench.py` | Paired bulk insert into ClickHouse and Exasol, with batch latency and throughput. | Live baseline exists at 100k and 1M. Prior silent simulated fallback has been removed. |
| `benchmark/query_bench.py` | Point lookup, merchant rollup, five-table join, and JSON aggregation; warmups, interleaved timing, p50/p95, row counts. | Live results exist at 100k and 1M; values are not compared row by row and scan/profile metrics are separate. |
| `benchmark/physical_design_bench.py`, `benchmark/natural_ttl_bench.py` | Storage snapshots, selective-date EXPLAIN/profile probes, card index evidence, Exasol join-index evidence, forced TTL probe, and separate natural cleanup watcher. | `physical_design_bench.py` has live results; natural cleanup was observed in `results/runs/20261022T094112/natural_ttl_results.json`, retaining eight fresh rows per engine. |
| `benchmark/update_bench.py` | Scratch-table ClickHouse `ALTER UPDATE`, `ReplacingMergeTree` append plus `FINAL`, and Exasol `UPDATE`; checks requested row counts. | Live run completed for 10,000 rows with expected counts. This does not establish full ledger isolation/correctness. |
| `benchmark/udf_bench.py` | Scores with ClickHouse SQL and Exasol Python UDFs using the saved LogisticFraudScorer JSON weights, and checks outputs against the artifact formula. | Live result at `20261022T094533`; feature and artifact parity pass after normalizing Exasol's observed +05:30 timestamp offset. |
| `benchmark/xgboost_udf_bench.py` | Trains a deterministic XGBoost model on generated synthetic labels, exports its JSON tree artifact, and benchmarks equivalent SQL-tree and Exasol Python-tree scoring. | Live result at `20261022T094533`; engine outputs match but both differ from the XGBoost library reference by up to 0.016875. Model parity remains unresolved. |
| `benchmark/kafka_bench.py`, `generator/kafka_producer.py`, `consumers/` | Paired producer/two-sink orchestration, fresh topic and scratch tables, sink throughput, enqueue-to-sink p50/p95/p99 latency, and row-count parity. | Live result at `20261022T093500`; exact 10,000-row counts matched. Longer sustained lag/reliability runs remain future work. |
| `config/settings.yaml` | Declares scale tiers, concurrency levels, and batch-size candidates. | These settings are not all consumed by the current runner; configured values are not measurements. |
| `audit/spec_matrix_and_capabilities.md`, `audit/usability_scorecard.md` | Broad feature prose and a numeric draft scorecard. | Treat as hypotheses; `documentation_comparison.md` is the narrower cited source review. No controlled usability study exists. |

The `--features-only` runner mode reuses loaded data and runs physical-design and mutation probes without truncating the PayLens tables. Add `--kafka`, `--udf`, `--xgboost`, and `--natural-ttl` to include those optional stages; `--all` enables all of them alongside the full load/query/physical-design baseline. `--queries-only` remains available for query timings alone.

The current local Exasol target reports a Nano build. Results from this setup can compare SQL behavior and single-node runtime on this machine; they do not establish distributed MPP scaling, node-to-node data distribution, or production concurrency behavior. Record hardware, memory, architecture, database versions, and all relevant settings with each run.

The experiment is **not ready for final comparative conclusions**. The current runner is a useful live baseline for ingest and query timing. The remaining feature tests and source audit must be completed before the report can claim comprehensive coverage.
