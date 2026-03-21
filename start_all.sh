#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Mac 再起動後に Kafka / Flink / 補助スクリプト実行環境を復旧する
#
# Do:
#   - Docker Desktop の起動待ち
#   - Kafka compose 起動
#   - Flink compose 起動
#   - topic 作成
#
# Don't:
#   - Python producer の自動起動はしない
#   - Flink job の自動投入はしない
#
# Usage:
#   ./start_all.sh

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DIR="${BASE_DIR}/kafka-test"
FLINK_DIR="${BASE_DIR}/flink-test"

echo "========================================"
echo "[INFO] Starting local data platform stack"
echo "========================================"

# Docker Desktop が起動しているか待つ
echo "[INFO] Waiting for Docker daemon..."
until docker info >/dev/null 2>&1; do
    sleep 2
done
echo "[INFO] Docker daemon is ready."

# Kafka 起動
echo "[INFO] Starting Kafka stack..."
cd "${KAFKA_DIR}"
docker compose up -d

# Flink 起動
echo "[INFO] Starting Flink stack..."
cd "${FLINK_DIR}"
docker compose up -d --build

# topic 作成
if [[ -f "${BASE_DIR}/create_topics.sh" ]]; then
    echo "[INFO] Creating Kafka topics..."
    "${BASE_DIR}/create_topics.sh"
else
    echo "[WARN] create_topics.sh not found. Skipping topic creation."
fi

echo
echo "========================================"
echo "[INFO] Startup completed."
echo "[INFO] Check containers with: docker ps"
echo "[INFO] Start Python producer manually if needed:"
echo "       cd ${BASE_DIR}/python-producer && python send_factory_data.py"
echo "[INFO] Apply Flink job manually if needed:"
echo "       ${FLINK_DIR}/apply_job.sh"
echo "========================================"
