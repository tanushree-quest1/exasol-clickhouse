# PayLens: Exasol vs ClickHouse Comparative Benchmark & Audit

A preliminary comparative benchmark and documentation audit evaluating **ClickHouse** and **Exasol** for FinTech card payments, mutable ledgers, star schema analytics, and fraud scoring.

---

## Deliverables & Documentation

* **Coverage checklist (measured vs. missing):** [`audit/coverage_checklist.md`](audit/coverage_checklist.md)
* **Current comparative results:** [`results/comparative_analysis.md`](results/comparative_analysis.md)
* **Results UI:** [`results/dashboard.html`](results/dashboard.html) (open in a browser)
* **Documentation comparison:** [`audit/documentation_comparison.md`](audit/documentation_comparison.md)
* **Feature matrix (draft; verify every claim):** [`audit/spec_matrix_and_capabilities.md`](audit/spec_matrix_and_capabilities.md)
* **Usability scorecard (opinion-only draft):** [`audit/usability_scorecard.md`](audit/usability_scorecard.md)
* **Final MD report:** [`results/final_report.md`](results/final_report.md)

## Requested metrics: measured vs. outstanding

| Requested comparison area | Current evidence |
|---|---|
| Bulk ingestion | Live paired measurements at 100k and 1M events; no independent repeat series or batch-size sweep. |
| FinTech queries | Live p50/p95 for point lookup, rollup, five-table join, and JSON extraction at both scales; the current harness compares returned values as well as row counts. Older artifacts may contain row-count checks only. |
| Partitioning / pruning | Live date filter/profile probe completed on 1,000,047 rows. ClickHouse selected 2/3 parts and 63/125 granules but no full partition pruning; Exasol profile showed zonemap scan evidence. |
| Indexing and automatic behavior | Live probe completed. Exasol join profile showed local/global index paths. ClickHouse Bloom index materialized, but the tested card lookup still read 1M rows and the scratch index selected all 123 granules. |
| TTL / retention | Forced scratch cleanup is measured; a natural ClickHouse background-TTL vs polled Exasol DELETE watcher is implemented and runs with `--all`, but needs a live result. |
| Ledger mutations | Live 10,000-row scratch probe passed row-count checks for ClickHouse `ALTER UPDATE` (6.357 ms), `ReplacingMergeTree` append/read, and Exasol `UPDATE` (13.780 ms). |
| Storage/compression | Live system-table snapshots; size accounting differs across engines and must be interpreted cautiously. |
| Kafka | Paired benchmark harness added. It starts isolated consumers for both engines, publishes one unique topic, records sink throughput, end-to-end p50/p95/p99, and row-count parity. Repeated runs show Exasol ahead on this unthrottled client path; use producer-rate and batch-size sweeps before making an engine-wide streaming claim. |
| ML/UDF | `--udf` compares the checked-in LogisticFraudScorer artifact; `--xgboost` trains a deterministic model on generated fraud labels and compares exported-tree scoring through ClickHouse SQL and Exasol Python UDFs. Both need live runs for measurements. |
| Documentation gaps and ease of use | Scoped vendor source comparison exists; no controlled task-based documentation/usability study has been conducted. |

**Conclusion:** The repository has live evidence for ingestion, the four query workloads, storage snapshots, date-filter and index behavior, forced and natural TTL cleanup, synthetic ledger mutations, paired Kafka ingestion, and logistic/XGBoost UDF execution. Logistic scoring matches its artifact after normalizing the measured Exasol timestamp offset. XGBoost engines match each other, but their tree outputs differ from the XGBoost library reference by up to 0.016875, so model parity remains open. These are local single-node observations. Task-based ease of use requires human participants and is documented in the study protocol. See the [comparative analysis](results/comparative_analysis.md), [documentation comparison](audit/documentation_comparison.md), and [usability study protocol](audit/usability_study.md).

---

## Project Structure

```
exasol-clickhouse/
├── docker-compose.yml            # ClickHouse and Kafka (KRaft); Exasol runs via Exasol Personal
├── pyproject.toml                # Project metadata & dependencies
├── config/
│   └── settings.yaml             # Centralized scale knobs & connection configs
├── schema/
│   ├── clickhouse.sql            # ClickHouse DDL (MergeTree, Partitioning, Bloom Filter, TTL)
│   └── exasol.sql                # Exasol DDL (distribution and partitioning)
├── generator/
│   ├── models.py                 # Fast entity models (Accounts, Cards, Events, Ledger)
│   ├── fraud_patterns.py         # Ground-truth fraud rings (Shared device, Impossible travel, Collusion)
│   ├── data_generator.py         # Deterministic FinTech transaction stream generator
│   └── kafka_producer.py         # Streaming producer to Kafka topic paylens.auth_events
├── consumers/
│   ├── ch_consumer.py            # Kafka -> ClickHouse streaming consumer
│   └── exasol_consumer.py        # Kafka -> Exasol streaming consumer (HTTP transport)
├── benchmark/
│   ├── insert_bench.py           # Bulk load & streaming ingestion throughput
│   ├── query_bench.py            # Point lookup, aggregation, 5-table join, JSON extraction
│   ├── physical_design_bench.py  # Live storage, pruning, index and TTL probes
│   ├── storage_and_partition_bench.py # Compatibility entry point for physical probes
│   ├── update_bench.py           # Live mutation probes on scratch tables
│   ├── natural_ttl_bench.py      # Background TTL vs polled Exasol DELETE watcher
│   ├── kafka_bench.py            # Paired Kafka sink benchmark
│   ├── indexing_bench.py         # Compatibility entry point for physical probes
│   ├── udf_bench.py              # Checked-in logistic artifact SQL/Python UDF comparison
│   └── xgboost_udf_bench.py      # Generated XGBoost SQL/Python UDF comparison
├── udf/
│   ├── train_model.py            # Trains fraud classifier on ground-truth dataset
│   ├── ch_executable_score.py    # ClickHouse executable UDF script
│   ├── ch_udf.py                 # ClickHouse SQL vectorized & executable UDF registration
│   ├── exasol_udf.py             # Exasol distributed Python container UDF registration
│   └── model_artifact/           # Exported portable fraud model artifact
├── audit/
│   ├── spec_matrix_and_capabilities.md # Feature parity & documented capabilities
│   └── usability_scorecard.md    # Developer ergonomics, debugging, observability, BI
└── results/
    ├── final_report.md           # Master executive report & architectural decisions
    └── *.json                    # Benchmark metric JSON outputs
```

---

## Run the Live Baseline

The complete runner performs paired bulk-ingest/query measurements, physical-design and mutation probes, a Kafka sink comparison, a short natural-retention observation, and both the saved logistic-model and generated XGBoost UDF comparisons. Kafka, logistic UDF, XGBoost UDF, and natural-retention stages can be selected individually, or enabled together by `--all`. Scratch probes use isolated tables; the baseline load resets the PayLens benchmark tables. It requires Python 3.10 or newer, Docker, and a running Exasol Personal instance. The Exasol Python SLC must be configured for both UDF stages.

```bash
./run_experiment.sh --all --rows 1000000 --batch-size 10000 --iterations 5
```

The runner reads the sys password from Exasol Personal's default deployment secrets file. Set `EXASOL_DEPLOYMENT_DIR` for a non-default deployment, or `EXASOL_PASSWORD` to override. A full `--all` run asks for `RESET` because it clears and reloads the benchmark tables. Results go to a timestamped directory under `results/runs/`. Use `--features-only --all` to reuse existing data while running physical-design, mutation, Kafka, both model UDFs, and natural TTL stages; use `--queries-only` for query timings alone. The full reset rebuilds ClickHouse `auth_events` with day partitions and `ORDER BY (card_id, event_time)`; query-only and feature-only runs preserve the existing schema and data. Query benchmarks compare returned values as well as row counts. The TTL watcher reports cleanup lag after expiry and records the table TTL setting. The full run sweeps Kafka at 2k, 5k, and 8k rows/s with batches of 500, 1,000, and 5,000 (10k rows per scenario); override these using `KAFKA_ROWS`, `KAFKA_RATES`, and `KAFKA_BATCH_SIZES`. The checked-in model is logistic; the XGBoost stage trains and records a separate model artifact in that run folder and reports per-row/tree routing diagnostics for the largest library-score errors. UDF stages require a usable Exasol Python SLC with NumPy and pandas for the vectorized SET-script comparison; on macOS, XGBoost training also requires the OpenMP runtime (`libomp`). If either stage fails, its JSON records that no metrics were produced and the runner continues to the next stage. No benchmark command falls back to simulated timings. Ease-of-use tasks are a separate human study described in `audit/usability_study.md`.

Event timestamps use UTC throughout the loaders: timezone-naive generated timestamps are interpreted as UTC, and offset-aware timestamps are converted to UTC before loading. Rerun the baseline after this change to reload existing rows; feature-only runs reuse existing data and do not repair timestamps already stored in either database.
