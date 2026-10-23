# ClickHouse and Exasol documentation comparison

**Review date:** 2026-10-22  
**Scope:** FinTech event analytics, data layout, indexing/pruning, lifecycle, mutations, streaming, ML, observability, and developer workflow.

> This is a documentation-based capability comparison, not a benchmark. “Documented” means the cited vendor source describes the feature. It does not mean this repository tested it. “Not verified” means the cited sources do not establish an absence of a capability; it must not be read as “unsupported.”

## Documentation findings

| Topic | ClickHouse documentation | Exasol documentation | PayLens experiment status / gap |
|---|---|---|---|
| Table layout | MergeTree `PARTITION BY` organizes parts; `ORDER BY` defines sorting and sparse-primary-index behavior. Skipping indexes are separately declared and workload-sensitive. | `DISTRIBUTE BY` and `PARTITION BY` are explicit, distinct physical layout clauses. | DDL is present, but these settings differ and are not an apples-to-apples construct. No matched pruning or layout experiment yet. |
| Pruning | Primary and skipping indexes may avoid granules; effectiveness depends on ordering, predicate, and data correlation. | Zone maps record segment min/max and can skip segments for supported predicates; profile evidence can identify zone-map use. | Capture EXPLAIN/profile, parts/segments and rows/bytes read under matched predicates. No skip percentages are measured. |
| Indexing | The sorting key provides a sparse primary index; optional skipping indexes are explicitly specified and should be evaluated against workload and write cost. | Join indexes are automatically created/maintained by the optimizer; index creation can affect first execution and profile output. Manual controls are documented for special cases. | Do not characterize ClickHouse as simply “manual” or Exasol as “zero DBA.” Compare first-run vs warmed plans, index metadata, write effects, and tuning steps. |
| TTL / retention | MergeTree TTL rules express data lifecycle actions; expiration and physical cleanup occur through background processing and should be observed separately. | The sources reviewed document partition management and DELETE/reorganization behavior, but this review did not establish an equivalent declarative TTL clause. | ClickHouse DDL has a 90-day TTL; no expiration test was run. Define and test an Exasol operational retention workflow before comparing effort. |
| Mutations / ledger | ClickHouse supports mutations/updates with engine-specific execution and visibility tradeoffs; replacing/versioned patterns are another design choice. | Standard SQL `UPDATE`/`MERGE` and transaction behavior are documented. | No equivalent update workload has been measured or correctness checked. Avoid broad ACID/performance conclusions until tested on deployed versions. |
| Streaming | ClickHouse has Kafka-related integration options; exact architecture depends on deployment (Kafka engine, Kafka Connect, or external pipeline). | Exasol documentation includes loading/integration paths and UDF-based ETL; this review did not verify a native Kafka table engine. | Repo producer/consumers are scaffolding only. Compare end-to-end pipelines, not the presence/absence of one integration label. |
| UDF / ML | ClickHouse documents SQL and executable/user-defined extension paths; setup and runtime depend on implementation and deployment. | Exasol documents parallel UDF scripts in languages including Python, Java, Lua and R, with execution and environment details. | No paired XGBoost scoring has been run. Need same model, input, output tolerance, setup record, and throughput. |
| Observability | ClickHouse exposes system tables and query/profile information; documentation includes query logs and table/part internals. | Exasol profiling provides per-operation execution data and system tables, including index creation and distribution/partition operations. | Inventory exists in prose only. Implement matching tasks and capture the result artifacts. |
| Documentation usability | Official documentation offers engine concepts, optimization guides, and feature-specific references. | Official documentation offers SQL references, performance topics, profiles, and feature-specific examples. | We have not done a controlled documentation usability study. No “easier to use” winner is justified. |

## FinTech relevance and open questions

1. **Authorization event analytics:** Compare throughput and query scaling against row count, but also hold the card filter selective and capture actual scan work. Current p50s alone do not explain why the engines differ.
2. **Fraud ring joins:** The current ClickHouse advantage in Q3 grows at 1M events on this Nano/local setup. Verify same query semantics and inspect plans, memory, scanned rows, and repeat variance before generalizing.
3. **Card velocity lookup:** Exasol was faster at both tested sizes. The ClickHouse sorting key begins with `merchant_id`, while the point query filters by `card_id`; test alternative sort keys/index options before interpreting this as a product-level outcome.
4. **Retention:** The 90-day ClickHouse TTL is in the DDL, but the dataset is recent and no expiry was observed. Exasol's comparative process, cleanup delay, and storage recovery are still open.
5. **Ledger correctness:** The project’s benchmark must model transaction state changes and settlement correctness before recommending either engine for the ledger of record.
6. **Ease of use:** Score tasks, not impressions. Use setup from a clean environment, create the schema, diagnose a slow query, alter retention, ingest a stream, and run a UDF. Record docs used, elapsed time, commands, errors, and manual tuning.

## Source register

Reviewed on 2026-10-22. These are vendor documentation pages; release/version coverage can change, and the test environment’s exact engine versions should be captured alongside future runs.

### ClickHouse

- [MergeTree table engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [Data skipping indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes)
- [TTL for tables](https://clickhouse.com/docs/en/guides/developer/ttl)
- [Kafka table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [System tables](https://clickhouse.com/docs/en/operations/system-tables)
- [Updates and deletes](https://clickhouse.com/docs/en/sql-reference/statements/alter)
- [Executable user-defined functions](https://clickhouse.com/docs/en/sql-reference/functions/udf)

### Exasol

- [CREATE TABLE: distribution and partitioning clauses](https://docs.exasol.com/db/latest/sql/create_table.htm)
- [Zone maps](https://docs.exasol.com/db/latest/performance/zonemaps.htm)
- [Performance automation](https://docs.exasol.com/db/latest/performance/automation.htm)
- [Indexes](https://docs.exasol.com/db/7.1/performance/indexes.htm)
- [Profiling](https://docs.exasol.com/db/latest/database_concepts/profiling.htm)
- [UDF overview](https://docs.exasol.com/db/latest/database_concepts/udf_scripts/udf_overview.htm)
- [Python UDFs](https://docs.exasol.com/db/latest/database_concepts/udf_scripts/python3.htm)
- [User-defined import/export using UDFs](https://docs.exasol.com/db/latest/loading_data/user_defined_import_export_using_udfs.htm)

## Audit limits

This is a scoped source review, not a complete line-by-line documentation quality score. Before a formal platform selection, capture exact product build/version and deployment model, link claims to the matching release documentation, and perform the task-based usability study described above. Vendor feature availability can vary across self-managed, cloud, and Nano deployments.
