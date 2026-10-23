#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ROWS="${ROWS:-10000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
ITERATIONS="${ITERATIONS:-3}"
WARMUPS="${WARMUPS:-2}"
ASSUME_YES=0
QUERIES_ONLY=0
FEATURES_ONLY=0
RUN_UDF=0
RUN_KAFKA=0
RUN_NATURAL_TTL=0
RUN_XGBOOST=0

usage() {
  cat <<'EOF'
Usage: ./run_experiment.sh [--rows N] [--batch-size N] [--iterations N] [--yes] [--queries-only] [--features-only] [--udf] [--xgboost] [--kafka] [--natural-ttl] [--all]

Runs a live ClickHouse vs Exasol ingestion and query baseline. The script starts
ClickHouse, applies both schemas, resets PayLens benchmark tables, loads the
same generated dimensions and events into both engines, and records results in
results/runs/<timestamp>/.

By default the script reads the Exasol Personal sys password from the active
deployment's secrets.json. Set EXASOL_DEPLOYMENT_DIR for a non-default deployment,
or EXASOL_PASSWORD to override. --yes skips the table-reset confirmation; use it
only if you are okay clearing PayLens tables.
--queries-only reuses the existing data and skips reset and ingestion.
--features-only reuses existing data and runs live storage, pruning, index,
retention, and ledger mutation probes without resetting benchmark tables.
--kafka starts the local Kafka broker and measures paired Kafka-to-database sinks.
--udf measures the saved logistic fraud model artifact in both engines.
--xgboost trains a deterministic XGBoost model and measures its UDF scoring in both engines.
--natural-ttl observes background ClickHouse TTL and a polled Exasol DELETE workflow.
--all runs the full baseline plus Kafka, both model UDFs, and natural-retention stages.
EOF
}

while (($#)); do
  case "$1" in
    --rows)
      if (($# < 2)); then printf 'Missing value for --rows.\n' >&2; usage >&2; exit 2; fi
      ROWS="$2"
      shift 2
      ;;
    --batch-size)
      if (($# < 2)); then printf 'Missing value for --batch-size.\n' >&2; usage >&2; exit 2; fi
      BATCH_SIZE="$2"
      shift 2
      ;;
    --iterations)
      if (($# < 2)); then printf 'Missing value for --iterations.\n' >&2; usage >&2; exit 2; fi
      ITERATIONS="$2"
      shift 2
      ;;
    --yes)
      ASSUME_YES=1
      shift
      ;;
    --queries-only)
      QUERIES_ONLY=1
      shift
      ;;
    --features-only)
      FEATURES_ONLY=1
      shift
      ;;
    --udf)
      RUN_UDF=1
      shift
      ;;
    --kafka)
      RUN_KAFKA=1
      shift
      ;;
    --natural-ttl)
      RUN_NATURAL_TTL=1
      shift
      ;;
    --xgboost)
      RUN_XGBOOST=1
      shift
      ;;
    --all)
      RUN_UDF=1
      RUN_XGBOOST=1
      RUN_KAFKA=1
      RUN_NATURAL_TTL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ((QUERIES_ONLY == 1 && FEATURES_ONLY == 1)); then
  printf 'Choose either --queries-only or --features-only, not both.\n' >&2
  exit 2
fi

for value in "$ROWS" "$BATCH_SIZE" "$ITERATIONS"; do
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf 'Rows, batch size, and iterations must be positive integers.\n' >&2
    exit 2
  fi
done

for command_name in docker exasol; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "$command_name" >&2
    exit 1
  fi
done

PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]] || ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' >/dev/null 2>&1; then
  BASE_PYTHON=""
  for candidate in python3.14 python3.13 python3.12 python3.11 python3.10 python3; do
    candidate_path="$(command -v "$candidate" || true)"
    if [[ -n "$candidate_path" ]] && "$candidate_path" -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' >/dev/null 2>&1; then
      BASE_PYTHON="$candidate_path"
      break
    fi
  done
  if [[ -z "$BASE_PYTHON" ]]; then
    printf 'Python 3.10 or newer is required. Install a current Python 3 release, then rerun this script.\n' >&2
    exit 1
  fi
  if [[ -d .venv ]]; then
    "$BASE_PYTHON" -m venv --clear .venv
  else
    "$BASE_PYTHON" -m venv .venv
  fi
fi
"$PYTHON_BIN" -m pip install --disable-pip-version-check -e .

# On macOS arm64/Intel, xgboost wheel expects libomp. If Homebrew's libomp is not present,
# borrow the bundled libomp from scikit-learn inside .venv.
if [[ "$(uname -s)" == "Darwin" ]] && [[ ! -f /opt/homebrew/opt/libomp/lib/libomp.dylib ]]; then
  for sk_omp in "$ROOT_DIR"/.venv/lib/python*/site-packages/sklearn/.dylibs/libomp.dylib; do
    if [[ -f "$sk_omp" ]]; then
      for xdir in "$ROOT_DIR"/.venv/lib/python*/site-packages/xgboost/lib; do
        if [[ -d "$xdir" && ! -f "$xdir/libomp.dylib" ]]; then
          cp "$sk_omp" "$xdir/libomp.dylib"
          install_name_tool -add_rpath @loader_path "$xdir/libxgboost.dylib" 2>/dev/null || true
        fi
      done
    fi
  done
fi

# Exasol Personal stores the database password in its deployment secrets file.
# Read it without printing it; allow an explicit env var or prompt to override.
if [[ -z "${EXASOL_PASSWORD:-}" ]]; then
  EXASOL_DEPLOYMENT_DIR="${EXASOL_DEPLOYMENT_DIR:-${HOME}/.exasol/personal/deployments/default}"
  EXASOL_SECRETS_FILE="$EXASOL_DEPLOYMENT_DIR/secrets.json"
  if [[ -r "$EXASOL_SECRETS_FILE" ]]; then
    EXASOL_PASSWORD="$("$PYTHON_BIN" -c 'import json,sys
try:
    with open(sys.argv[1]) as f:
        value=json.load(f).get("dbPassword", "")
    print(value if isinstance(value, str) else "", end="")
except (OSError, ValueError, TypeError):
    pass' "$EXASOL_SECRETS_FILE")"
  fi
fi
if [[ -z "${EXASOL_PASSWORD:-}" ]]; then
  if [[ ! -t 0 ]]; then
    printf 'Could not read dbPassword from Exasol Personal secrets.json. Set EXASOL_PASSWORD or run interactively.\n' >&2
    exit 1
  fi
  read -r -s -p 'Exasol Personal sys password: ' EXASOL_PASSWORD
  printf '\n'
fi
if [[ -z "$EXASOL_PASSWORD" ]]; then
  printf 'Exasol password cannot be empty.\n' >&2
  exit 1
fi
export EXASOL_PASSWORD
export PAYLENS_REQUIRE_LIVE=1

printf '\nStarting ClickHouse...\n'
docker compose up -d --remove-orphans clickhouse

CLICKHOUSE_HEALTH="starting"
for _ in {1..60}; do
  CLICKHOUSE_HEALTH="$(docker inspect --format '{{.State.Health.Status}}' paylens-clickhouse 2>/dev/null || true)"
  if [[ "$CLICKHOUSE_HEALTH" == "healthy" ]]; then
    break
  fi
  if [[ "$CLICKHOUSE_HEALTH" == "unhealthy" ]]; then
    docker logs --tail 40 paylens-clickhouse >&2 || true
    printf 'ClickHouse is unhealthy. See the container logs above.\n' >&2
    exit 1
  fi
  sleep 2
done
if [[ "$CLICKHOUSE_HEALTH" != "healthy" ]]; then
  printf 'Timed out waiting for ClickHouse health (last state: %s).\n' "$CLICKHOUSE_HEALTH" >&2
  exit 1
fi

if ((RUN_KAFKA == 1)); then
  printf '\nStarting Kafka...\n'
  docker compose up -d kafka
  KAFKA_HEALTH="starting"
  for _ in {1..60}; do
    KAFKA_HEALTH="$(docker inspect --format '{{.State.Health.Status}}' paylens-kafka 2>/dev/null || true)"
    if [[ "$KAFKA_HEALTH" == "healthy" ]]; then break; fi
    if [[ "$KAFKA_HEALTH" == "unhealthy" ]]; then
      docker logs --tail 40 paylens-kafka >&2 || true
      printf 'Kafka is unhealthy. See the container logs above.\n' >&2
      exit 1
    fi
    sleep 2
  done
  if [[ "$KAFKA_HEALTH" != "healthy" ]]; then
    printf 'Timed out waiting for Kafka health (last state: %s).\n' "$KAFKA_HEALTH" >&2
    exit 1
  fi
fi

EXASOL_STATUS="$(exasol status)"
printf '%s\n' "$EXASOL_STATUS"
if ! grep -q 'database_ready' <<<"$EXASOL_STATUS"; then
  printf 'Exasol Personal is not ready. Start it with `exasol start` and retry.\n' >&2
  exit 1
fi

printf '\nApplying database schemas...\n'
exasol connect --file schema/exasol.sql
docker exec -i paylens-clickhouse clickhouse-client --multiquery < schema/clickhouse.sql

if ((QUERIES_ONLY == 1 || FEATURES_ONLY == 1)); then
  printf '\nReusing existing PayLens data; skipping reset and ingestion.\n'
else
if ((ASSUME_YES == 0)); then
  if [[ ! -t 0 ]]; then
    printf 'This run clears the PayLens tables. Re-run interactively or pass --yes to confirm.\n' >&2
    exit 1
  fi
  printf '\nThis clears rows from the PayLens tables in both databases. Type RESET to continue: '
  read -r confirmation
  if [[ "$confirmation" != "RESET" ]]; then
    printf 'Cancelled; no tables were cleared.\n'
    exit 1
  fi
fi

printf '\nResetting PayLens benchmark tables...\n'
exasol connect --command "TRUNCATE TABLE PAYLENS.AUTH_EVENTS; TRUNCATE TABLE PAYLENS.ACCOUNTS; TRUNCATE TABLE PAYLENS.CARDS; TRUNCATE TABLE PAYLENS.MERCHANTS; TRUNCATE TABLE PAYLENS.DEVICES; TRUNCATE TABLE PAYLENS.TXN_LEDGER; TRUNCATE TABLE PAYLENS.FRAUD_SCORES;"
docker exec paylens-clickhouse clickhouse-client --multiquery --query "DROP TABLE paylens.auth_events; TRUNCATE TABLE paylens.accounts; TRUNCATE TABLE paylens.cards; TRUNCATE TABLE paylens.merchants; TRUNCATE TABLE paylens.devices; TRUNCATE TABLE paylens.txn_ledger; TRUNCATE TABLE paylens.fraud_scores;"
docker exec -i paylens-clickhouse clickhouse-client --multiquery < schema/clickhouse.sql
fi

RUN_ID="$(date +%Y%m%dT%H%M%S)"
export PAYLENS_RESULTS_DIR="$ROOT_DIR/results/runs/$RUN_ID"
export PAYLENS_RUN_ID="$RUN_ID"
mkdir -p "$PAYLENS_RESULTS_DIR"

if ((QUERIES_ONLY == 0 && FEATURES_ONLY == 0)); then
  printf '\nRunning live bulk-ingest benchmark (%s rows, batch size %s)...\n' "$ROWS" "$BATCH_SIZE"
  "$PYTHON_BIN" benchmark/insert_bench.py --rows "$ROWS" --batch-size "$BATCH_SIZE"
fi

if ((FEATURES_ONLY == 0)); then
  printf '\nRunning live query benchmark (%s iterations)...\n' "$ITERATIONS"
  "$PYTHON_BIN" benchmark/query_bench.py --iterations "$ITERATIONS" --warmups "$WARMUPS"
fi

if ((QUERIES_ONLY == 0)); then
  printf '\nRunning live storage, partition, indexing, and retention probes...\n'
  "$PYTHON_BIN" benchmark/physical_design_bench.py
  printf '\nRunning live ledger mutation probe on isolated scratch tables...\n'
  "$PYTHON_BIN" benchmark/update_bench.py --rows 10000
fi

if ((RUN_KAFKA == 1)); then
  KAFKA_ROWS="${KAFKA_ROWS:-10000}"
  KAFKA_RATES="${KAFKA_RATES:-2000,5000,8000}"
  KAFKA_BATCH_SIZES="${KAFKA_BATCH_SIZES:-500,1000,5000}"
  printf '\nRunning paired Kafka sink rate/batch sweep (%s rows, rates %s, batches %s)...\n' "$KAFKA_ROWS" "$KAFKA_RATES" "$KAFKA_BATCH_SIZES"
  "$PYTHON_BIN" benchmark/kafka_bench.py --rows "$KAFKA_ROWS" --rates "$KAFKA_RATES" --batch-sizes "$KAFKA_BATCH_SIZES"
fi

if ((RUN_NATURAL_TTL == 1)); then
  printf '\nRunning natural TTL visibility and scheduled Exasol cleanup probe...\n'
  "$PYTHON_BIN" benchmark/natural_ttl_bench.py
fi

if ((RUN_UDF == 1)); then
  printf '\nRunning live saved-model SQL/Python UDF benchmark...\n'
  if ! "$PYTHON_BIN" benchmark/udf_bench.py; then
    printf '{"status":"failed","reason":"Inspect console output for the prerequisite or runtime error.","metrics_available":false}\n' > "$PAYLENS_RESULTS_DIR/udf_benchmark_results.json"
    printf 'Saved-model UDF stage failed; recorded status and continuing.\n' >&2
  fi
fi

if ((RUN_XGBOOST == 1)); then
  printf '\nTraining XGBoost on generated fraud labels and benchmarking both UDFs...\n'
  if ! "$PYTHON_BIN" benchmark/xgboost_udf_bench.py; then
    printf '{"status":"failed","reason":"Inspect console output for the prerequisite or runtime error.","metrics_available":false}\n' > "$PAYLENS_RESULTS_DIR/xgboost_udf_benchmark_results.json"
    printf 'XGBoost UDF stage failed; recorded status and continuing.\n' >&2
  fi
fi

printf '\nExperiment finished. Results: %s\n' "$PAYLENS_RESULTS_DIR"
