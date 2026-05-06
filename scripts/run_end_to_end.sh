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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${LOG_DIR:-$ROOT/logs/e2e_$RUN_ID}"
PID_FILE="$LOG_DIR/pids.txt"

PY="${PY:-}"
if [[ -z "$PY" ]]; then
  if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PY="$ROOT/.venv/bin/python"
  else
    PY="python"
  fi
fi

if [[ -d "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]]; then
  export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
fi
export PATH="${JAVA_HOME:+$JAVA_HOME/bin:}$PATH"

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
STARTING_OFFSETS="${STARTING_OFFSETS:-latest}"
STAGING_WARMUP_SECS="${STAGING_WARMUP_SECS:-90}"
API_HOST="${API_HOST:-0.0.0.0}"
API_PORT="${API_PORT:-8000}"
DASHBOARD_HOST="${DASHBOARD_HOST:-0.0.0.0}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8501}"

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

stop_bg() {
  if [[ ! -f "$PID_FILE" ]]; then
    return
  fi
  log "stopping launched processes"
  while read -r pid name; do
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      log "stopping $name pid=$pid"
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done < "$PID_FILE"
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

log "repo: $ROOT"
log "logs: $LOG_DIR"
log "python: $PY"
java -version 2>&1 | tee "$LOG_DIR/java_version.log"

log "starting Docker infra"
(cd infra && docker compose up -d)
wait_for_cassandra
wait_for_mongo

log "applying Cassandra schema"
docker cp infra/cassandra_schema.cql subway_cassandra:/schema.cql
docker exec subway_cassandra cqlsh -f /schema.cql

log "initializing MongoDB"
"$PY" infra/init_mongo.py | tee "$LOG_DIR/init_mongo.log"

log "verifying DB connections"
"$PY" infra/verify_connections.py | tee "$LOG_DIR/verify_connections.log"

if [[ "$LOAD_BASELINE" == "1" ]]; then
  if [[ -d "data/ridership_weather_baseline" ]]; then
    log "loading Cassandra baseline from data/ridership_weather_baseline"
    "$PY" -m processing.build_baseline \
      --ridership-weather-input data/ridership_weather_baseline \
      --lookback-days 0 \
      --sink cassandra \
      >"$LOG_DIR/load_baseline.log" 2>&1
  else
    log "skipping baseline load: data/ridership_weather_baseline is missing"
  fi
else
  log "skipping baseline load: LOAD_BASELINE=$LOAD_BASELINE"
fi

if [[ "$CLEAN_TRACK_B" == "1" ]]; then
  log "cleaning Track B generated outputs"
  rm -rf data/staging/speed_layer_delays checkpoints/speed_layer_delays checkpoints/lambda_merge data/debug/lambda_merge
fi

log "pre-resolving Spark connector packages"
"$PY" - <<PY >"$LOG_DIR/prewarm_spark_packages.log" 2>&1
from processing.spark_utils import get_spark
spark = get_spark("subway-dash-package-prewarm", extra_packages=["$KAFKA_SPARK_PACKAGE"])
spark.stop()
PY

start_bg producer "$PY" -m ingestion.gtfs_producer
if [[ "$START_WEATHER" == "1" ]]; then
  start_bg weather "$PY" -m ingestion.weather_poller
fi
if [[ "$START_MONITOR" == "1" ]]; then
  start_bg gtfs_monitor "$PY" -m ingestion.gtfs_monitor
fi

start_bg vehicle_consumer "$PY" -m processing.gtfs_vehicle_consumer --starting-offsets "$STARTING_OFFSETS"
start_bg trips_consumer "$PY" -m processing.gtfs_trips_consumer --starting-offsets "$STARTING_OFFSETS"

log "warming staging for ${STAGING_WARMUP_SECS}s before Track B starts"
sleep "$STAGING_WARMUP_SECS"

start_bg speed_layer "$PY" -m processing.speed_layer_join
sleep 20

start_bg lambda_merge "$PY" -m processing.lambda_merge \
  --input data/staging/speed_layer_delays \
  --batch-source cassandra \
  --sink mongodb \
  --mongo-uri mongodb://localhost:27017

if [[ "$START_API" == "1" ]]; then
  start_bg api "$PY" -m uvicorn serving.main:app --host "$API_HOST" --port "$API_PORT" --reload
fi

if [[ "$START_DASHBOARD" == "1" ]]; then
  export SUBWAY_DASH_API_URL="${SUBWAY_DASH_API_URL:-http://localhost:$API_PORT}"
  start_bg dashboard "$PY" -m streamlit run dashboard/app.py \
    --server.address "$DASHBOARD_HOST" \
    --server.port "$DASHBOARD_PORT"
fi

log "end-to-end stack is running"
log "API:       http://localhost:$API_PORT/health"
log "Dashboard: http://localhost:$DASHBOARD_PORT"
log "PID file:  $PID_FILE"
log "Use Ctrl+C to stop launched local processes. Docker containers are left running."

tail -f "$LOG_DIR"/*.log
