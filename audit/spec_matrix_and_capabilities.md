# PayLens: Exasol vs ClickHouse Specification & Capabilities Audit
*Source: Official Documentation Analysis for FinTech Card Payment & Fraud Analytics*

> **Draft audit, not a verified feature matrix.** Several entries below are unreferenced assertions and must not be treated as findings. The [coverage checklist](coverage_checklist.md) tracks the validation work. Add direct, versioned official documentation links for each claim before using this matrix to make a platform decision.

### Verified storage/index facts to carry into the experiments

* ClickHouse MergeTree uses a sparse primary index over the sorting key, and its optional skipping indexes need workload-specific evaluation; its `EXPLAIN` output can show index pruning. See the [ClickHouse query optimization guide](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide) and [EXPLAIN documentation overview](https://clickhouse.com/blog/clickhouse-newsletter-may-2022-explain-statement-query-optimization).
* ClickHouse TTL work is performed through background merges, so expiration policy and the time storage is reclaimed are separate measurements. See [Handling Updates and Deletes in ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse).
* Exasol documents `DISTRIBUTE BY` and `PARTITION BY` as explicit physical data layout clauses in [CREATE TABLE](https://docs.exasol.com/db/latest/sql/create_table.htm). It documents zone maps on partition columns and profile evidence (`WITH ZONEMAP`) in [Zone maps](https://docs.exasol.com/db/latest/performance/zonemaps.htm).
* Exasol documents automatic index creation and maintenance for joins, and notes that an index may first be created during query execution in [Indexes](https://docs.exasol.com/db/7.1/performance/indexes.htm) and [Performance automation](https://docs.exasol.com/db/latest/performance/automation.htm). This supports a qualified automatic-indexing claim, not the prior “100% automatic, zero DBA” language.

---

## 1. Architectural Core & Philosophy

| Architectural Dimension | ClickHouse (Official Specs) | Exasol (Official Specs) | Impact on FinTech Use Case |
|---|---|---|---|
| **Primary Design Target** | Append-only analytical log/event store | In-memory relational analytical data warehouse | Payments generate massive event logs (ClickHouse sweet spot) + transactional ledgers (Exasol sweet spot). |
| **Storage Architecture** | Disk/SSD columnar storage (`MergeTree` family) with multi-tier storage (NVMe $\to$ S3/GCS). | Columnar, in-memory first; uses disk as backing store when working set exceeds RAM. | ClickHouse is significantly cheaper for 90+ days of historical transaction logs. Exasol requires sizing cluster RAM to fit working data. |
| **Execution Engine** | Vectorized query execution (SIMD processing over columnar chunks). | Massively Parallel Processing (MPP) with cluster-wide distributed shared-nothing execution. | ClickHouse wins on single-table aggregation scans; Exasol excels on distributed multi-table joins. |
| **ACID Compliance** | Single-table / single-block atomicity. **No multi-statement ACID transactions**. Eventual consistency on deduplication/mutations. | **Full ANSI ACID compliance** with Serializable and Repeatable Read isolation levels (MVCC). | **Crucial Gap:** Exasol handles ledger balance changes with full transactional safety. ClickHouse requires external handling or idempotent deduplication. |

---

## 2. Partitioning, Indexing & Data Lifecycle (TTL)

| Feature | ClickHouse | Exasol | FinTech Relevance |
|---|---|---|---|
| **Partitioning Syntax** | Declarative `PARTITION BY <expr>` (e.g., `toYYYYMM(event_time)`). Custom partition key per table. | `PARTITION BY` and `DISTRIBUTE BY` are separate table clauses with distinct roles. | Compare the actual DDL and query plans at the chosen engine versions; do not equate partitioning with distribution. |
| **Partition Pruning** | MergeTree query analysis can prune partitions and granules; capture `EXPLAIN` evidence for this workload. | Exasol documents zone maps and partitioning; capture query profile/plan evidence for this workload. | Pruning effectiveness depends on the data layout and predicate. No skip percentages are established by this project yet. |
| **Indexing Philosophy** | MergeTree tables have a sparse primary index based on the sorting key; optional data-skipping indexes are explicitly declared and need workload evaluation. | Exasol documents automatic index creation/maintenance for joins, with first-run index creation potentially affecting latency; this is not a claim that every workload needs no tuning. | Compare setup steps and query-work evidence, not broad “zero DBA” or “manual” labels. |
| **TTL / Data Retention** | MergeTree TTL is a declarative data lifecycle capability; mutation/merge timing and storage reclamation should be observed separately. | Validate supported lifecycle options and operational procedure against the exact Exasol version. | No retention outcome or comparative operational effort has been measured in this project. |

---

## 3. Data Types & Semi-Structured Data (JSON)

| Data Type Category | ClickHouse | Exasol | FinTech Relevance |
|---|---|---|---|
| **Nested & Semi-Structured (JSON)** | Native `JSON` object type, `Tuple`, `Array`, `Map`. Direct JSON path extraction (`payload.card.bin`) with sub-column compilation. | Stored as `VARCHAR(2000000)` with `JSON_VALUE()`, `JSON_EXTRACT()` scalar functions. | **ClickHouse Advantage:** Payment ISO 8583/20022 payloads contain deep nested structures. ClickHouse parses and vectorizes nested JSON natively; Exasol evaluates JSON functions as string parsing. |
| **Monetary / Decimal** | `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256` (exact fixed-point). | `DECIMAL(p, s)` up to precision 36. | Both prevent floating-point rounding errors for currency balances. |
| **UUIDs & Hashes** | Native `UUID` type, native `FixedString(32)` for MD5/SHA256 card hashes. | `HASHTYPE(16 BYTE)` / `HASHTYPE(32 BYTE)` optimized for MD5/SHA hashes and UUIDs. | Both offer storage-efficient binary hash storage for card tokenization. |
| **Networking (IPs)** | Native `IPv4` and `IPv6` types with CIDR containment operators (`IPv4CIDRToRange`). | Stored as standard `VARCHAR(45)`. | ClickHouse makes IP-based fraud analysis (device IP geo-lookups, CIDR masking) significantly faster and more expressive. |

---

## 4. Mutation & DML Capabilities (Updates & Alters)

| Mutation Operation | ClickHouse | Exasol | FinTech Relevance |
|---|---|---|---|
| **Row-level `UPDATE`** | Asynchronous / heavyweight batch mutation (`ALTER TABLE ... UPDATE`). Rewrites entire data parts. Not designed for real-time single-row updates. | Standard synchronous ANSI `UPDATE table SET ... WHERE ...`. | For ledger updates (`AUTH` $\to$ `CLEARED` $\to$ `SETTLED`), Exasol behaves like a real database. ClickHouse requires workarounds. |
| **Upsert / `MERGE`** | No native multi-table `MERGE INTO`. Solved via `ReplacingMergeTree` (inserting newer version and deduplicating at merge time or query time with `FINAL`). | Native ANSI `MERGE INTO target USING source ON (...) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. | Exasol is far simpler and cleaner for reconciling late-arriving settlement records. |
| **Schema Alterations (`ALTER TABLE`)** | Instant for adding/dropping columns (metadata operation). Modifying primary sort key is restricted. | Instant metadata operation for column adds/drops. Allows changing distribution/partitioning. | Both allow online non-blocking column additions. |

---

## 5. In-Database Analytics & UDFs (Machine Learning)

| UDF Dimension | ClickHouse | Exasol | FinTech Relevance |
|---|---|---|---|
| **UDF Execution Modes** | 1. SQL user-defined functions.<br>2. Executable UDFs (pipes data via stdin/stdout to an external binary/script).<br>3. Vectorized C++ UDFs.<br>4. Native CatBoost model evaluation function (`catboostEvaluate`). | Native polyglot distributed container UDFs: **Python 3, R, Java, Lua, C++**. Runs inside isolated Docker/Linux containers on each cluster node. | **Exasol Advantage:** Exasol's Python UDF framework is mature, distributed, and directly passes Arrow/Numpy batches into memory. ClickHouse requires external process pipes or native CatBoost compilation. |
| **Batch Scoring Efficiency** | External script pipe introduces serialization overhead unless using precompiled C++ or native CatBoost. | Python UDF receives partitions as Pandas/Arrow batches directly in parallel memory. | For scoring transactions using an XGBoost model, Exasol's Python UDF framework is architecturally cleaner. |

---

## 6. Third-Party Integrations & Ecosystem

| Integration Category | ClickHouse | Exasol | Ease of Use Finding |
|---|---|---|---|
| **Streaming / Kafka** | Native Kafka Table Engine (`ENGINE = Kafka`) built directly into the server (can consume without an external app). | No native engine. Requires external consumer (Python, Java, Kafka Connect JDBC). | ClickHouse can ingest directly from Kafka with pure SQL DDL. Exasol requires an external streaming connector pipeline. |
| **Python Client Drivers** | `clickhouse-connect` (HTTP/Arrow fast path), `clickhouse-driver` (Native protocol). Highly active open-source. | `pyexasol` (Websocket-based, fast bulk loading via HTTP transport). Enterprise-maintained. | Both have excellent, high-throughput Python drivers with Pandas integration. |
| **Observability (OTel & Metrics)** | Built-in Prometheus endpoint (`/metrics`), deep `system.query_log`, `system.part_log`, native OpenTelemetry trace export. | System virtual tables (`EXA_STATISTICS`, `EXA_ALL_SESSIONS`, `EXA_ALL_QUERIES`). No native OTel trace export. | ClickHouse is built for modern cloud-native observability out of the box. Exasol requires polling system tables. |
| **BI Tools (Tableau, PowerBI)** | Official drivers exist, but requires careful tuning to avoid expensive multi-table joins. | First-class certified connectors. Legendary live-connection performance for PowerBI/Tableau without data extracts. | Exasol is widely regarded as superior for live corporate BI dashboards with hundreds of concurrent users. |
| **Data Engineering Tools** | Official dbt adapter (`dbt-clickhouse`), native Apache Spark connector, Airflow operators. | `dbt-exasol` community adapter, Spark JDBC, standard Airflow JDBC. | ClickHouse has wider modern data stack community adoption. |

---

## 7. FinTech Fit: Hypotheses to Validate

The entries below are workload hypotheses, not benchmark conclusions. Replace them with sourced criteria and measured evidence after completing the experiments.

| Feature Dimension | Important for PayLens? | Advantage | Why? |
|---|---|---|---|
| **High-Rate Ingest (50k/s)** | **CRITICAL** | **ClickHouse** | Cheaper hardware footprint, built to swallow firehose event streams. |
| **Ledger Reconciliation & Mutations** | **CRITICAL** | **Exasol** | True ACID `MERGE` and `UPDATE` prevents race conditions and corrupted ledger balances. |
| **5-Table Star Joins** | **CRITICAL** | **Exasol** | Joining transactions with cards, merchants, accounts, and devices is Exasol's core MPP strength. |
| **Nested JSON Parsing** | **CRITICAL** | **ClickHouse** | ISO 8583 payment messages have nested JSON that ClickHouse queries without full string extraction. |
| **Indexing and pruning** | **HIGH** | **Undetermined** | Measure design effort, query plan behavior, and maintenance actions for the tested workload. |
| **Declarative Retention (TTL)** | **MEDIUM** | **ClickHouse** | 90-day retention policies run automatically without external cron jobs. |
| **In-Database Python ML** | **HIGH** | **Exasol** | Native containerized Python UDFs make deploying XGBoost simpler than ClickHouse executable pipes. |
| **Native Kafka Ingestion** | **HIGH** | **ClickHouse** | Ingest directly via DDL without maintaining separate consumer microservices. |
