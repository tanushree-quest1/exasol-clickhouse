# PayLens: Usability & Developer Ergonomics Scorecard
*Comparative Evaluation of ClickHouse vs Exasol for Everyday Engineering & Operations*

> **Draft, opinion-only scorecard.** The numeric ratings and phrases such as “wins decisively” are not supported by a recorded task study. Treat them as hypotheses only. Re-score after running the same tasks on both engines and recording setup time, steps, errors, documentation used, tuning changes, and execution-plan evidence. Cite official docs for capability claims.

---

## 1. Setup, Installation & Configuration

| Dimension | ClickHouse | Exasol | Usability Assessment |
|---|---|---|---|
| **Local Docker Deployment** | Simple standard container (`clickhouse/clickhouse-server`). Boots in < 2 seconds. Normal user permissions. | Docker image (`exasol/docker-db`) requires `privileged: true` for shared memory and IPC. Startup takes 30–60s for storage engine init. | **ClickHouse is easier**: Developer iteration and lightweight CI/CD setup is much faster with ClickHouse. |
| **Default Configuration** | Clean XML/YAML configuration files with sensible out-of-the-box memory and concurrency defaults. | Managed through `EXAOperation` web UI or cluster command-line parameters (`cosched`, `dwsize`). | **ClickHouse is more modern**: GitOps-friendly config files vs proprietary cluster manager. |
| **CLI & Web Client** | 1. `clickhouse-client` (interactive, multi-line, syntax highlighted).<br>2. Built-in web client on port 8123 (`/play`). | 1. `exaplus` (Java-based CLI).<br>2. DBeaver / standard JDBC/ODBC clients. | **ClickHouse is more accessible**: Native built-in browser UI enables immediate exploration without installing third-party SQL clients. |

---

## 2. Query Debugging & Execution Plans (`EXPLAIN`)

| Dimension | ClickHouse | Exasol | Usability Assessment |
|---|---|---|---|
| **Query Plan Syntax** | `EXPLAIN ...`<br>Supports: `EXPLAIN AST`, `EXPLAIN SYNTAX`, `EXPLAIN PLAN`, `EXPLAIN PIPELINE`, `EXPLAIN ESTIMATE`. | `EXPLAIN PLAN FOR ...`<br>or profiling live runs via `EXPLAIN VIRTUAL ...` | **ClickHouse has richer inspection**: Allows seeing AST transformations and CPU execution pipelines. |
| **Execution Profiling** | Live query profiling directly via `system.query_log` and `system.processors_profile_log`. Real-time progress bars in client. | Post-run profiling stored in system tables: `EXA_DBA_PROFILE_LAST_QUERY`, detailing CPU time per step across nodes. | **Exasol has clearer distributed bottleneck isolation**: Shows exactly which cluster node or slice skewed during a join. |
| **Error Messaging** | Highly descriptive stack traces with clear error codes (e.g., `DB::Exception: Missing columns... [Code: 47]`). | Clear SQL error messages with exact error codes and line/column pointers (e.g., `ETL-3000: Object not found`). | **Tie**: Both provide clear, actionable syntax and runtime errors. |

---

## 3. Client Driver Experience (Python & Applications)

| Dimension | ClickHouse (`clickhouse-connect`) | Exasol (`pyexasol`) | Usability Assessment |
|---|---|---|---|
| **Protocol** | HTTP / Arrow native compression fast path. Highly robust across firewalls and proxies. | WebSocket connection + native parallel HTTP transport for bulk data loading. | **Tie**: Both drivers are top-tier. `pyexasol` is exceptionally well-engineered for high-volume pandas imports/exports. |
| **Pandas / Arrow Integration** | Native `query_df()`, `insert_df()`, `query_arrow()`. | Native `export_to_pandas()`, `import_from_pandas()`. | **Tie**: Both treat DataFrame operations as first-class citizens. |
| **Connection Pooling & Auth** | Built-in connection pool, role switching, OAuth2/JWT tokens. | Session management with autocommit, transaction isolation level controls. | **Exasol wins for transaction control**: Exasol supports explicit `COMMIT` / `ROLLBACK` states in the driver. |

---

## 4. Observability & System Introspection

| Dimension | ClickHouse | Exasol | Usability Assessment |
|---|---|---|---|
| **Built-in System Tables** | Comprehensive `system.*` tables (`system.parts`, `system.mutations`, `system.query_log`, `system.metrics`). | Comprehensive `EXA_STATISTICS`, `EXA_ALL_SESSIONS`, `EXA_DBA_OBJECT_SIZES`, `EXA_DBA_INDICES`. | **Tie for SQL introspection**: Both expose rich internal state via standard SQL queries. |
| **Cloud-Native Metrics & OTel** | **Native Prometheus endpoint (`/metrics`)** built into server binary. Native OpenTelemetry trace propagation. | No native Prometheus or OTel endpoints. Requires polling `EXA_STATISTICS` via custom exporter script. | **ClickHouse wins decisively**: Integrates directly with Grafana, Datadog, and OpenTelemetry without custom glue code. |

---

## 5. Third-Party Ecosystem & BI Connectivity

| Integration | ClickHouse | Exasol | Usability Assessment |
|---|---|---|---|
| **Live BI (PowerBI / Tableau)** | Supported, but requires pre-aggregated tables or denormalized models to avoid slow joins. | **Industry Gold Standard**: Flawless live-connection performance on star schemas without data extracts. | **Exasol wins decisively**: Corporate BI analysts can build ad-hoc drag-and-drop dashboards without IT having to pre-join tables. |
| **Streaming / Kafka** | Built-in native Kafka engine (`ENGINE = Kafka`) in pure SQL DDL. | Requires external consumer application or Kafka Connect JDBC sink. | **ClickHouse wins**: Ingest streaming events without maintaining separate consumer services. |
| **Modern Data Stack (dbt)** | Official `dbt-clickhouse` adapter with wide community support. | Community `dbt-exasol` adapter. | **ClickHouse is more prevalent**: Larger open-source ecosystem and broader dbt feature coverage. |

---

## Summary Scorecard

| Category | ClickHouse | Exasol | Key Differentiator |
|---|:---:|:---:|---|
| **Developer Onboarding** | 🟢 **9/10** | 🟡 **6/10** | ClickHouse is up in seconds; Exasol has heavier Docker/init requirements. |
| **SQL Standards & ACID** | 🔴 **4/10** | 🟢 **10/10** | Exasol provides true ANSI transactions, updates, and joins. ClickHouse is append-oriented. |
| **Auto-Tuning & DBA Overhead** | 🟡 **6/10** | 🟢 **10/10** | Exasol automatically indexes and self-tunes. ClickHouse requires strict sort key design. |
| **Observability & Cloud-Native** | 🟢 **10/10** | 🟡 **5/10** | ClickHouse has built-in Prometheus and OpenTelemetry support. |
| **Ad-Hoc BI & Joins** | 🟡 **6/10** | 🟢 **10/10** | Exasol in-memory MPP handles concurrent live dashboard queries seamlessly. |
| **Streaming Firehose Ingest** | 🟢 **10/10** | 🟡 **6/10** | ClickHouse swallows massive append streams at significantly lower hardware cost. |
