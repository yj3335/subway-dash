#!/usr/bin/env bash
# Create the final-shape Kafka topics. Idempotent.
# Usage: ./infra/create_topics.sh [bootstrap_server]
#   default bootstrap = localhost:9092
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
KAFKA_EXEC="docker exec subway_kafka kafka-topics"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found; falling back to local kafka-topics binary"
    KAFKA_EXEC="kafka-topics.sh"
fi

create() {
    local topic="$1" partitions="$2" extra="${3:-}"
    echo ">> creating topic ${topic} (partitions=${partitions}) ${extra}"
    # shellcheck disable=SC2086
    ${KAFKA_EXEC} --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
        --topic "${topic}" --partitions "${partitions}" --replication-factor 1 ${extra}
}

create gtfs-vehicle 12
create gtfs-trips   12
create gtfs-alerts  2
create gtfs-dlq     2
create weather-feed 1 "--config cleanup.policy=compact"

echo
echo "All topics:"
${KAFKA_EXEC} --bootstrap-server "${BOOTSTRAP}" --list
