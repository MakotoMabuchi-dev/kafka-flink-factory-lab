#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Kafka / Flink / MinIO / Iceberg REST の Docker Compose 環境を停止する
#
# Usage:
#   ./stop_all.sh

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DIR="${BASE_DIR}/kafka-test"
FLINK_DIR="${BASE_DIR}/flink-test"

echo "========================================"
echo "[INFO] Stopping local data platform stack"
echo "========================================"

cd "${FLINK_DIR}"
echo "[INFO] Stopping Flink, MinIO, and Iceberg REST stack..."
docker compose down || true

cd "${KAFKA_DIR}"
echo "[INFO] Stopping Kafka stack..."
docker compose down || true

echo "[INFO] Stop completed."
echo "[INFO] MinIO data under ${FLINK_DIR}/minio-data is preserved."
echo "[INFO] Iceberg REST metadata under ${FLINK_DIR}/iceberg-rest-data is preserved."
