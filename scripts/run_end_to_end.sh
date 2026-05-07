#!/usr/bin/env bash
set -euo pipefail

# End-to-end local launcher for Subway Dash.
#
# Starts:
#   Docker infra -> DB init -> optional Cassandra baseline load ->
#   GTFS/weather producers -> Spark staging consumers -> Track B speed layer ->
#   Lambda merge -> FastAPI -> Streamlit
#
# Stop with Ctrl+C. The script terminates background processes it launched,
# but leaves Docker containers running so data remains available for inspection.
#
# Common overrides (set before running):
#   RESET_KAFKA_VOLUME=1   -- delete infra_kafka_data volume before start
#                             (fixes InconsistentClusterIdException on restart)
#   BASELINE_PARQUET_DIR=/path/to/station_capacity_baseline
#                          -- load pre-computed parquet into Cassandra
#                             (use when data/ridership_weather_baseline is absent)
#   API_PORT=8003          -- change port if 8002 is occupied
#   LOAD_BASELINE=0        -- skip baseline loading entirely

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${LOG_DIR:-$ROOT/logs/e2e_$RUN_ID}"
PID_FILE="$LOG_DIR/pids.txt"
TAIL_PID=""
MONITOR_PID=""

# ---------------------------------------------------------------------------
# Python — prefer .venv; Windows uses Scripts/, Unix uses bin/
# ---------------------------------------------------------------------------
PY="${PY:-}"
if [[ -z "$PY" ]]; then
  if [[ -x "$ROOT/.venv/Scripts/python" ]]; then
    PY="$ROOT/.venv/Scripts/python"
  elif [[ -x "$ROOT/.venv/bin/python" ]]; then
    PY="$ROOT/.venv/bin/python"
  else
    PY="python"
  fi
fi

# ---------------------------------------------------------------------------
# Java — use JAVA_HOME if set, otherwise rely on PATH
# ---------------------------------------------------------------------------
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

export PYSPARK_PACKAGES="${PYSPARK_PACKAGES:-org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1}"
KAFKA_SPARK_PACKAGE="${KAFKA_SPARK_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0}"
export SPARK_CONF_spark__sql__adaptive__enabled="${SPARK_CONF_spark__sql__adaptive__enabled:-true}"
export SPARK_CONF_spark__sql__adaptive__skewJoin__enabled="${SPARK_CONF_spark__sql__adaptive__skewJoin__enabled:-true}"

LOAD_BASELINE="${LOAD_BASELINE:-1}"
CLEAN_TRACK_B="${CLEAN_TRACK_B:-1}"
START_DASHBOARD="${START_DASHBOARD:-1}"
START_API="${START_API:-1}"
START_WEATHER="${START_WEATHER:-1}"
START_MONITOR="${START_MONITOR:-0}"
ENABLE_PROCESS_MONITOR="${ENABLE_PROCESS_MONITOR:-0}"
EXIT_ON_CRITICAL_FAILURE="${EXIT_ON_CRITICAL_FAILURE:-1}"
STARTING_OFFSETS="${STARTING_OFFSETS:-latest}"
STAGING_WARMUP_SECS="${STAGING_WARMUP_SECS:-90}"
SPEED_LAYER_MAX_FILES_PER_TRIGGER="${SPEED_LAYER_MAX_FILES_PER_TRIGGER:-20}"
LAMBDA_MAX_FILES_PER_TRIGGER="${LAMBDA_MAX_FILES_PER_TRIGGER:-20}"
SPEED_LAYER_LATEST_FIRST="${SPEED_LAYER_LATEST_FIRST:-0}"
LAMBDA_LATEST_FIRST="${LAMBDA_LATEST_FIRST:-0}"
RESET_KAFKA_VOLUME="${RESET_KAFKA_VOLUME:-0}"
TAIL_LOGS="${TAIL_LOGS:-0}"

# Port defaults: 8002 because 8000/8001 are commonly occupied on this machine
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8002}"
DASHBOARD_HOST="${DASHBOARD_HOST:-127.0.0.1}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8501}"

# Pre-computed baseline parquet — set this if data/ridership_weather_baseline is absent
BASELINE_PARQUET_DIR="${BASELINE_PARQUET_DIR:-}"

mkdir -p "$LOG_DIR"
: > "$PID_FILE"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

start_bg() {
  local name="$1"
  shift
  local log_file="$LOG_DIR/${name}.log"
  log "starting $name -> $log_file"
  "$@" >"$log_file" 2>&1 &
  local pid=$!
  printf '%s %s\n' "$pid" "$name" >> "$PID_FILE"
}

kill_tree() {
  local pid="$1"
  if [[ -z "${pid:-}" ]]; then
    return
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return
  fi

  local children
  children="$(pgrep -P "$pid" 2>/dev/null || true)"
  local child
  for child in $children; do
    kill_tree "$child"
  done
  kill "$pid" >/dev/null 2>&1 || true
}

print_log_tail() {
  local name="$1"
  local log_file="$LOG_DIR/${name}.log"
  if [[ -f "$log_file" ]]; then
    log "last 80 lines from $log_file"
    tail -80 "$log_file" || true
  fi
}

stop_bg() {
  if [[ ! -f "$PID_FILE" ]]; then
    return
  fi
  log "stopping launched processes"
  if [[ -n "${TAIL_PID:-}" ]]; then
    kill_tree "$TAIL_PID"
  fi
  if [[ -n "${MONITOR_PID:-}" ]]; then
    kill_tree "$MONITOR_PID"
  fi
  while read -r pid name; do
    if [[ -n "${pid:-}" && "$pid" != "${TAIL_PID:-}" && "$pid" != "${MONITOR_PID:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      log "stopping $name pid=$pid"
      kill_tree "$pid"
    fi
  done < "$PID_FILE"

  sleep 3
  while read -r pid name; do
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      log "force stopping $name pid=$pid"
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done < "$PID_FILE"
}

monitor_critical() {
  local status=0
  local pid name
  while true; do
    sleep 10
    while read -r pid name; do
      case "$name" in
        vehicle_consumer|trips_consumer|speed_layer|lambda_merge)
          if [[ -n "${pid:-}" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            wait "$pid" >/dev/null 2>&1 || status=$?
            log "critical process exited: $name pid=$pid status=$status"
            print_log_tail "$name"
            if [[ "$EXIT_ON_CRITICAL_FAILURE" == "1" ]]; then
              log "EXIT_ON_CRITICAL_FAILURE=1; stopping stack"
              stop_bg
              kill_tree "$$"
            fi
          fi
          ;;
      esac
    done < "$PID_FILE"
  done
}

wait_for_cassandra() {
  log "waiting for Cassandra"
  for _ in {1..60}; do
    if docker exec subway_cassandra cqlsh -e 'describe keyspaces' >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  log "Cassandra did not become ready in time"
  return 1
}

wait_for_mongo() {
  log "waiting for MongoDB"
  for _ in {1..40}; do
    if docker exec subway_mongo mongosh --quiet --eval 'db.adminCommand("ping").ok' >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  log "MongoDB did not become ready in time"
  return 1
}

trap stop_bg INT TERM EXIT

log "repo:   $ROOT"
log "logs:   $LOG_DIR"
log "python: $PY"
log "api:    http://$API_HOST:$API_PORT"
log "dash:   http://$DASHBOARD_HOST:$DASHBOARD_PORT"

# ---------------------------------------------------------------------------
# Optional: reset Kafka volume to fix InconsistentClusterIdException
# ---------------------------------------------------------------------------
if [[ "$RESET_KAFKA_VOLUME" == "1" ]]; then
  log "resetting Kafka volume (RESET_KAFKA_VOLUME=1)"
  (cd infra && docker compose down) || true
  docker volume rm infra_kafka_data 2>/dev/null || log "kafka volume not found or already removed"
fi

# ---------------------------------------------------------------------------
# Docker infra
# ---------------------------------------------------------------------------
log "starting Docker infra"
(cd infra && docker compose up -d)
wait_for_cassandra
wait_for_mongo

# ---------------------------------------------------------------------------
# Schema + index init
# ---------------------------------------------------------------------------
log "applying Cassandra schema"
docker cp infra/cassandra_schema.cql subway_cassandra:/schema.cql
docker exec subway_cassandra cqlsh -f /schema.cql

log "initializing MongoDB"
"$PY" infra/init_mongo.py | tee "$LOG_DIR/init_mongo.log"

log "verifying DB connections"
"$PY" infra/verify_connections.py | tee "$LOG_DIR/verify_connections.log"

# ---------------------------------------------------------------------------
# Baseline load (Cassandra station_capacity_baseline)
# ---------------------------------------------------------------------------
if [[ "$LOAD_BASELINE" == "1" ]]; then
  if [[ -d "data/ridership_weather_baseline" ]]; then
    log "loading Cassandra baseline from data/ridership_weather_baseline"
    "$PY" -m processing.build_baseline \
      --ridership-weather-input data/ridership_weather_baseline \
      --lookback-days 0 \
      --sink cassandra \
      >"$LOG_DIR/load_baseline.log" 2>&1
  elif [[ -n "$BASELINE_PARQUET_DIR" && -d "$BASELINE_PARQUET_DIR" ]]; then
    log "loading Cassandra baseline from pre-computed parquet: $BASELINE_PARQUET_DIR"
    "$PY" infra/load_baseline_parquet.py --input "$BASELINE_PARQUET_DIR" \
      >"$LOG_DIR/load_baseline.log" 2>&1
  else
    log "skipping baseline load: set BASELINE_PARQUET_DIR to load pre-computed parquet"
    log "  e.g. BASELINE_PARQUET_DIR='/path/to/station_capacity_baseline' bash scripts/run_end_to_end.sh"
  fi
else
  log "skipping baseline load: LOAD_BASELINE=$LOAD_BASELINE"
fi

# ---------------------------------------------------------------------------
# Clean Track B outputs
# ---------------------------------------------------------------------------
if [[ "$CLEAN_TRACK_B" == "1" ]]; then
  log "cleaning Track B generated outputs"
  rm -rf data/staging/speed_layer_delays checkpoints/speed_layer_delays checkpoints/lambda_merge data/debug/lambda_merge
fi

# ---------------------------------------------------------------------------
# Pre-warm Spark packages
# ---------------------------------------------------------------------------
log "pre-resolving Spark connector packages"
"$PY" - <<PY >"$LOG_DIR/prewarm_spark_packages.log" 2>&1
from processing.spark_utils import get_spark
spark = get_spark("subway-dash-package-prewarm", extra_packages=["$KAFKA_SPARK_PACKAGE"])
spark.stop()
PY

# ---------------------------------------------------------------------------
# Producers
# ---------------------------------------------------------------------------
start_bg producer "$PY" -m ingestion.gtfs_producer
if [[ "$START_WEATHER" == "1" ]]; then
  start_bg weather "$PY" -m ingestion.weather_poller
fi
if [[ "$START_MONITOR" == "1" ]]; then
  start_bg gtfs_monitor "$PY" -m ingestion.gtfs_monitor
fi

# ---------------------------------------------------------------------------
# Track B consumers
# ---------------------------------------------------------------------------
start_bg vehicle_consumer "$PY" -m processing.gtfs_vehicle_consumer --starting-offsets "$STARTING_OFFSETS"
start_bg trips_consumer    "$PY" -m processing.gtfs_trips_consumer   --starting-offsets "$STARTING_OFFSETS"

log "warming staging for ${STAGING_WARMUP_SECS}s before Track B starts"
sleep "$STAGING_WARMUP_SECS"

SPEED_LAYER_ARGS=(--max-files-per-trigger "$SPEED_LAYER_MAX_FILES_PER_TRIGGER")
if [[ "$SPEED_LAYER_LATEST_FIRST" == "1" ]]; then
  SPEED_LAYER_ARGS+=(--latest-first)
fi
start_bg speed_layer "$PY" -m processing.speed_layer_join "${SPEED_LAYER_ARGS[@]}"
sleep 20

LAMBDA_ARGS=(
  --input data/staging/speed_layer_delays \
  --batch-source cassandra \
  --sink mongodb \
  --mongo-uri "mongodb://127.0.0.1:27017" \
  --max-files-per-trigger "$LAMBDA_MAX_FILES_PER_TRIGGER"
)
if [[ "$LAMBDA_LATEST_FIRST" == "1" ]]; then
  LAMBDA_ARGS+=(--latest-first)
fi
start_bg lambda_merge "$PY" -m processing.lambda_merge "${LAMBDA_ARGS[@]}"

# ---------------------------------------------------------------------------
# API + Dashboard
# ---------------------------------------------------------------------------
if [[ "$START_API" == "1" ]]; then
  start_bg api "$PY" -m uvicorn serving.main:app \
    --host "$API_HOST" --port "$API_PORT" --reload
fi

if [[ "$START_DASHBOARD" == "1" ]]; then
  export SUBWAY_DASH_API_URL="${SUBWAY_DASH_API_URL:-http://127.0.0.1:$API_PORT}"
  start_bg dashboard "$PY" -m streamlit run dashboard/app.py \
    --server.address "$DASHBOARD_HOST" \
    --server.port "$DASHBOARD_PORT"
fi

log "stack is running"
log "  API:       http://127.0.0.1:$API_PORT/health"
log "  Dashboard: http://127.0.0.1:$DASHBOARD_PORT"
log "  Logs:      $LOG_DIR"
log "  PIDs:      $PID_FILE"
log "  Ctrl+C to stop local processes. Docker containers are left running."
log "  Set TAIL_LOGS=1 to stream all service logs in this terminal."
log "  Set ENABLE_PROCESS_MONITOR=1 to stop the stack if a critical job exits."

if [[ "$ENABLE_PROCESS_MONITOR" == "1" ]]; then
  monitor_critical &
  MONITOR_PID=$!
  printf '%s %s\n' "$MONITOR_PID" process_monitor >> "$PID_FILE"
fi

if [[ "$TAIL_LOGS" == "1" ]]; then
  tail -f "$LOG_DIR"/*.log &
  TAIL_PID=$!
  printf '%s %s\n' "$TAIL_PID" log_tail >> "$PID_FILE"
  wait "$TAIL_PID"
else
  while true; do
    sleep 3600
  done
fi
