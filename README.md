# PayLens: Exasol vs ClickHouse Comparative Benchmark & Audit

A comprehensive comparative benchmark and architectural audit evaluating **ClickHouse** and **Exasol** for FinTech card payments, mutable ledgers, star schema analytics, and in-database machine learning fraud scoring.

---

## Deliverables & Documentation

* **Executive Summary & Complete Findings:** [`results/final_report.md`](results/final_report.md)
* **Official Specs & Capabilities Matrix:** [`audit/spec_matrix_and_capabilities.md`](audit/spec_matrix_and_capabilities.md)
* **Usability & Developer Ergonomics Scorecard:** [`audit/usability_scorecard.md`](audit/usability_scorecard.md)

---

## Project Structure

```
exasol-clickhouse/
├── docker-compose.yml            # ClickHouse, Exasol Docker DB, Kafka (KRaft)
├── pyproject.toml                # Project metadata & dependencies
├── config/
│   └── settings.yaml             # Centralized scale knobs & connection configs
├── schema/
│   ├── clickhouse.sql            # ClickHouse DDL (MergeTree, Partitioning, Bloom Filter, TTL)
│   └── exasol.sql                # Exasol DDL (Distribute by, Partition by, Auto-indexing)
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
│   ├── storage_and_partition_bench.py # Disk usage, compression ratio, partition pruning, TTL
│   ├── update_bench.py           # Ledger state transitions (AUTH -> SETTLED) & schema alterations
│   ├── indexing_bench.py         # Manual sort keys vs Automatic self-tuning indexing
│   └── udf_bench.py              # In-database ML scoring (Vectorized vs Python container)
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

## How to Run the Benchmarks

All benchmark scripts are standalone and include built-in simulation profiles when running without live Docker containers.

```bash
# 1. Bulk Ingest Benchmark
python3 benchmark/insert_bench.py --rows 100000 --batch-size 10000

# 2. Query Speed & 5-Table Join Benchmark
python3 benchmark/query_bench.py --iterations 5

# 3. Storage, Compression, Partition Pruning & TTL
python3 benchmark/storage_and_partition_bench.py

# 4. Ledger Updates & Online Schema Alterations
python3 benchmark/update_bench.py

# 5. Indexing Comparison (Manual vs Automatic)
python3 benchmark/indexing_bench.py

# 6. In-Database ML Fraud Scoring
python3 benchmark/udf_bench.py --rows 50000
```
