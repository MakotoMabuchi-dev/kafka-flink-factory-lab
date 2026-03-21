#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Kafka に学習用 topic をまとめて作成する
#
# Do:
#   - sensor-a
#   - sensor-b
#   - process-events
#   の3 topic を作成する
#
# Don't:
#   - 既存 topic の削除はしない
#   - Kafka コンテナ自体の起動はしない
#
# Usage:
#   ./create_topics.sh

BOOTSTRAP_SERVER="localhost:29092"
KAFKA_CONTAINER="kafka"

TOPICS=(
    "sensor-a"
    "sensor-b"
    "process-events"
)

for topic in "${TOPICS[@]}"; do
    echo "[INFO] Creating topic: ${topic}"
    docker exec -i "${KAFKA_CONTAINER}" kafka-topics \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --bootstrap-server "${BOOTSTRAP_SERVER}"
done

echo "[INFO] Current topic list:"
docker exec -i "${KAFKA_CONTAINER}" kafka-topics \
    --list \
    --bootstrap-server "${BOOTSTRAP_SERVER}"
