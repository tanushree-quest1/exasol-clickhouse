# PayLens ClickHouse vs Exasol: experiment report

**Reviewed:** 2026-10-22  
**Workload:** FinTech authorization and fraud-detection data  
**Environment:** Local ClickHouse container and single-node Exasol Personal Nano

## Findings

- At 1M generated events, ClickHouse ingested at 105,420.6 rows/s versus Exasol at 87,558.6 rows/s. ClickHouse was faster on the merchant rollup, five-table fraud join, and JSON query; Exasol was faster on the card-velocity point lookup. These are local observations from one measured run series, not production capacity claims.
- The physical-design run had 1,000,047 rows in both databases. The date-filter profiles showed some work reduction, but not complete partition elimination. ClickHouse selected 2/3 parts and 63/125 granules; its Partition index still showed 3/3 parts. Exasol reported a zonemap scan.
- Index behavior differed in the tested probes. Exasol join profiles used local and global indexes. The ClickHouse Bloom index was added and materialized on a 1M-row copy, but EXPLAIN still selected every granule for that test. The card lookup also read 1M rows, so this probe did not show a useful Bloom-index reduction.
- Both systems removed the expired row in a two-row retention probe. Forced ClickHouse TTL cleanup took 4.139 ms; Exasol DELETE+COMMIT took 12.680 ms. The test does not measure natural TTL delay or Exasol scheduling effort.
- On 10,000 synthetic scratch rows, ClickHouse synchronous `ALTER UPDATE` took 6.357 ms and Exasol `UPDATE` took 13.780 ms; both returned the expected row count. ClickHouse `ReplacingMergeTree` append took 57.037 ms, followed by a 2.792 ms `FINAL` read. This does not test isolation, rollback, or production ledger invariants.
- Storage snapshots reported indicative compression ratios of 4.865× for ClickHouse and 5.663× for Exasol. The underlying size metrics have different meanings: ClickHouse compressed part bytes on disk versus Exasol in-memory object size. Treat them as separate engine-specific readings, not a direct compression ranking.

## Remaining coverage gaps

The latest Kafka run sent and persisted 10,000 rows per sink: ClickHouse measured 8,652 rows/s and 1,081.415 ms p50 end-to-end latency; Exasol measured 10,195 rows/s and 912.061 ms. The natural TTL watcher observed cleanup after 39,462 ms for ClickHouse and 38,427 ms for Exasol; both retained eight fresh rows. The logistic UDF scored 50,000 rows and matched the same artifact exactly in both engines after normalizing Exasol's measured +05:30 timestamp offset. XGBoost engine outputs also match each other, but both differ from the XGBoost library reference by up to 0.016875; model-scoring correctness remains open. These are single local runs. Repeated independent benchmark series, a controlled documentation/usability study, and production-style concurrency remain unmeasured. Query result row counts match, but the query harness does not compare all returned values.

## Detailed materials

- [Comparative analysis and all live measurements](comparative_analysis.md)
- [Documentation comparison](../audit/documentation_comparison.md)
- [Experiment coverage checklist](../audit/coverage_checklist.md)
- [Usability study protocol](../audit/usability_study.md)
- [Interactive comparison dashboard](dashboard.html)
- [Successful physical-design run JSON](runs/20261022T080633/physical_design_results.json)
- [Successful mutation run JSON](runs/20261022T080633/update_benchmark_results.json)
- [Latest paired Kafka sink run JSON](runs/20261022T093500/kafka_benchmark_results.json)
- [Natural TTL run JSON](runs/20261022T094112/natural_ttl_results.json)
- [Logistic UDF result JSON](runs/20261022T094533/udf_benchmark_results.json)
- [XGBoost UDF result JSON (model parity open)](runs/20261022T094533/xgboost_udf_results.json)

The comparison remains preliminary and is limited to the software versions, generated data, local machine, and single-node Exasol Nano environment used in these runs.
