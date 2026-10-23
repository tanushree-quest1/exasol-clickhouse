# ClickHouse vs Exasol: Consolidated Comparison Report

**Project:** PayLens FinTech analytics benchmark  
**Report date:** 2026-10-23  
**Scope:** Physical design, UDF/ML execution, documentation, and developer usability  
**Environment:** Local ClickHouse container and local single-node Exasol Personal Nano

## Executive conclusion

The evidence points to different strengths rather than a universal winner:

- **ClickHouse** is the simpler choice for SQL-native scoring, append-heavy event analytics, declarative retention, Kafka-oriented ingestion, and engine-level observability.
- **Exasol** is the more flexible choice for general-purpose, distributed Python and polyglot UDFs, conventional relational mutations, and database-hosted ML workflows.
- **ClickHouse showed stronger demonstrated date pruning** in the latest physical-design probe and used slightly less measured storage, although the storage metrics are not directly equivalent.
- **The UDF benchmark favored ClickHouse on the tested implementation**, but Exasol has the cleaner architecture for arbitrary Python/ML logic.
- **Documentation usability has not been experimentally measured.** The differences below are documented feature emphases, not a formal usability ranking.

## 1. Physical design and query behavior

Source: [latest physical-design run](runs/20261023T094259/physical_design_results.json).

| Area | ClickHouse | Exasol | Conclusion |
|---|---|---|---|
| Row-count parity | 10,000,047 rows | 10,000,047 rows | The compared `AUTH_EVENTS` datasets are aligned. |
| Measured physical size | 363.7 MB compressed data-part bytes | 395.7 MB compressed in-memory object size | ClickHouse is approximately 8% smaller in this reading, but the accounting semantics differ. |
| Date predicate | Read 2 rows / 52 bytes through `_minmax_count_projection` | Profile showed a zonemap scan over the `AUTH_EVENTS` object | ClickHouse has the clearest demonstrated pruning advantage in this probe. |
| `card_id` lookup | Primary key reduced the read to about 684k rows and 80 granules | Automatic local indexes are present in metadata | ClickHouse’s primary-key ordering did useful work; the test did not prove additional Bloom-filter benefit. |
| Five-table join | 50 rows returned | 50 rows returned | The join result is functionally aligned, but the profile was cache-served and should not be used to attribute performance to indexes. |
| Retention probe | Forced TTL cleanup removed the expired row in 2.3 ms | Explicit `DELETE` plus `COMMIT` removed it in 13.5 ms | Both mechanisms worked; this was a scratch probe, not a production lifecycle comparison. |

### Physical-design interpretation

ClickHouse currently has the stronger direct evidence for selective date filtering. Exasol’s zonemaps and automatic indexes are visible, but the available evidence does not establish a general performance advantage. Client elapsed time alone should not be treated as proof of pruning or index use; plans, profiles, and rows/bytes read are more meaningful.

## 2. UDF functionality

Source: [UDF capability matrix](../audit/spec_matrix_and_capabilities.md) and the repository UDF implementations in [`../udf/`](../udf/).

| UDF requirement | ClickHouse | Exasol | Practical winner |
|---|---|---|---|
| Simple deterministic formula | SQL user-defined functions run directly in the query engine | Can use a UDF, but Python is generally more machinery than needed | ClickHouse |
| Arbitrary Python or ML code | Executable UDFs can invoke external scripts; deployment and serialization are required | Native distributed Python UDFs run in isolated containers | Exasol |
| Multiple implementation languages | SQL, executable scripts, and native C++ extension paths are documented in the review | Python, R, Java, Lua, and C++ UDF languages are documented | Exasol for language breadth |
| Batch ML scoring | External process pipes can introduce serialization overhead | UDFs receive distributed batches in the database runtime | Exasol architecturally |
| Native model support | Native CatBoost evaluation is documented | General-purpose UDF framework is the main extension route | Depends on model; ClickHouse for supported native paths, Exasol for arbitrary libraries |
| UDF deployment footprint | SQL UDFs are lightweight; executable UDFs require server-side scripts/configuration | Requires a configured Python SLC and runtime packages | ClickHouse for SQL; Exasol after SLC setup for Python |

### UDF ease of use

- **ClickHouse is easiest for a small SQL expression.** The repository’s logistic scorer can be registered as a SQL lambda without a Python runtime.
- **Exasol is easiest for a real Python/ML UDF once the runtime is configured.** The implementation is a database-managed `PYTHON3` script rather than a separate process connected through stdin/stdout.
- **ClickHouse executable UDFs are more operationally involved** for complex models because scripts, permissions, formats, and server deployment must be managed.
- **Exasol has higher initial setup cost.** The project requires an Exasol Python SLC with NumPy and pandas for the vectorized UDF stage.

## 3. UDF benchmark results

The recorded 50,000-row live measurements were:

| Workload | ClickHouse | Exasol | Interpretation |
|---|---:|---:|---|
| Logistic scoring p50 | 49.995 ms | 253.177 ms | ClickHouse was approximately 5.1× faster in this run. |
| XGBoost scoring p50 | 57.711 ms | 549.302 ms | ClickHouse was approximately 9.5× faster in this run. |

These numbers are implementation-specific. The ClickHouse side used SQL/tree scoring while the Exasol side used Python UDF scoring, so they demonstrate measured execution performance—not equal UDF architecture or equal flexibility.

The logistic implementation matched the checked-in artifact after correcting the measured Exasol timestamp offset. The XGBoost implementations matched each other, but both differed from the XGBoost library reference by up to **0.016875**. Therefore, XGBoost model correctness remains unresolved.

## 4. Documentation differences

Source: [documentation comparison](../audit/documentation_comparison.md).

### ClickHouse documents more prominently

- MergeTree parts, `ORDER BY`, sparse primary indexes, and granule-level skipping indexes.
- Declarative TTL rules and background cleanup behavior.
- Kafka table-engine ingestion options.
- Query logs, system tables, part internals, and processor-level execution details.
- SQL and executable UDF extension paths.

### Exasol documents more prominently

- Separate `DISTRIBUTE BY` and `PARTITION BY` controls for MPP physical layout.
- Zone maps and automatic/local/global join-index behavior.
- Performance automation and per-operation distributed profiling.
- Parallel UDF scripts with documented Python, R, Java, Lua, and C++ support.
- User-defined import/export workflows using UDFs.
- Conventional SQL `UPDATE`, `MERGE`, and transaction-oriented workflows.

These are documented differentiators, not proof that a feature is absent from the other product. Product versions and deployment models can change the result.

## 5. Developer usability comparison

| Developer task | Easier platform | Reason |
|---|---|---|
| Start a local analytical database | ClickHouse | Lightweight container startup and simpler local iteration. |
| Write a scalar scoring formula | ClickHouse | SQL function is concise and native. |
| Deploy a Python model inside the database | Exasol, after setup | Native distributed Python UDF model is more direct. |
| Avoid external UDF processes | Exasol for Python; ClickHouse for SQL | Exasol embeds Python UDF execution; ClickHouse SQL UDFs avoid processes entirely. |
| Inspect low-level query execution | ClickHouse | Rich `EXPLAIN` variants and detailed query/processor logs. |
| Diagnose distributed join bottlenecks | Exasol | Per-operation profiling exposes distributed execution steps and index activity. |
| Maintain streaming ingestion with SQL | ClickHouse | Kafka table-engine options reduce external pipeline code. |
| Use standard relational updates and merges | Exasol | Conventional `UPDATE`/`MERGE` semantics are simpler for mutable records. |

The repository’s usability scorecard is explicitly opinion-only and recommends a controlled task study before assigning a final usability winner: [usability_scorecard.md](../audit/usability_scorecard.md).

## 6. Project setup implications

The project declares both database clients and ML libraries in [pyproject.toml](../pyproject.toml): `clickhouse-connect`, `pyexasol`, pandas, NumPy, scikit-learn, and XGBoost. These dependencies support the benchmark and client-side model work; they do **not** automatically install packages into an Exasol UDF container or deploy executable scripts to ClickHouse servers.

The README separately requires an Exasol Python SLC with NumPy and pandas for the UDF stages. This makes Exasol’s UDF model more capable, but also introduces an environment prerequisite that a SQL-only ClickHouse UDF does not need.

## 7. Recommendation by workload

Choose **ClickHouse** when the main workload is:

- High-volume append-only event analytics.
- SQL-native scoring and transformations.
- Kafka-connected ingestion.
- Declarative retention and cloud-native observability.
- Minimal UDF runtime setup for simple formulas.

Choose **Exasol** when the main workload is:

- Python, R, Java, Lua, or C++ logic executed close to distributed data.
- General-purpose ML libraries and batch scoring inside the database.
- Mutable relational records requiring standard `UPDATE`, `MERGE`, and transaction semantics.
- MPP join optimization with automatic index and profiling support.

## 8. Evidence limits and next steps

The conclusions are preliminary because the environment is local and single-node, the UDF timings use different implementation styles, storage metrics have different semantics, and documentation usability was not tested with participants.

For a stronger decision, the next comparison should:

1. Run the same model implementation strategy in both engines where possible.
2. Measure cold and warm UDF runs with cache behavior recorded.
3. Test a real Python dependency package inside the Exasol SLC and a real executable UDF deployment in ClickHouse.
4. Repeat physical-design probes at several selectivities and data sizes.
5. Run a controlled usability study recording setup time, commands, documentation consulted, errors, and tuning changes.

