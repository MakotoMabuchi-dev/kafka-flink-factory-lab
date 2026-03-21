#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Kafka / Flink の Docker Compose 環境を停止する
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
docker compose down || true

cd "${KAFKA_DIR}"
docker compose down || true

echo "[INFO] Stop completed."
