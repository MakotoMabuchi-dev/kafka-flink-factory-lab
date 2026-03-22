#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Mac 再起動後に Kafka / Flink / MinIO / Iceberg REST を含む
#   ローカルデータ基盤を復旧する
#
# Do:
#   - Docker Desktop の起動待ち
#   - Kafka compose 起動
#   - Flink compose 起動
#   - MinIO / Iceberg REST の起動確認
#   - warehouse bucket の存在確認
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
SHARED_NETWORK="stream-shared"

wait_for_container() {
    local container_name="$1"
    local max_retries="${2:-30}"
    local retry=1

    until docker ps --format '{{.Names}}' | grep -qx "${container_name}"; do
        if (( retry > max_retries )); then
            echo "[ERROR] Container '${container_name}' did not start in time." >&2
            return 1
        fi
        sleep 2
        ((retry++))
    done
}

echo "========================================"
echo "[INFO] Starting local data platform stack"
echo "========================================"

# Docker Desktop が起動しているか待つ
echo "[INFO] Waiting for Docker daemon..."
until docker info >/dev/null 2>&1; do
    sleep 2
done
echo "[INFO] Docker daemon is ready."

if ! docker network inspect "${SHARED_NETWORK}" >/dev/null 2>&1; then
    echo "[INFO] Creating shared Docker network: ${SHARED_NETWORK}"
    docker network create "${SHARED_NETWORK}" >/dev/null
fi

# Kafka 起動
echo "[INFO] Starting Kafka stack..."
cd "${KAFKA_DIR}"
docker compose up -d

# Flink 起動
echo "[INFO] Starting Flink stack..."
cd "${FLINK_DIR}"
docker compose up -d --build

echo "[INFO] Waiting for core containers..."
wait_for_container "kafka"
wait_for_container "flink-jobmanager"
wait_for_container "flink-taskmanager"
wait_for_container "minio"
wait_for_container "mc"
wait_for_container "iceberg-rest"

echo "[INFO] Verifying MinIO warehouse bucket..."
if docker exec mc /usr/bin/mc ls local/warehouse >/dev/null 2>&1; then
    echo "[INFO] MinIO bucket 'warehouse' is ready."
else
    echo "[ERROR] MinIO bucket 'warehouse' is not available." >&2
    exit 1
fi

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
echo "[INFO] MinIO Console: http://localhost:9001"
echo "[INFO] Iceberg REST catalog: http://localhost:8181"
echo "[INFO] Start Python producer manually if needed:"
echo "       cd ${BASE_DIR}/python-producer && python3 send_factory_data.py"
echo "[INFO] Apply Flink job manually if needed:"
echo "       ${FLINK_DIR}/apply_job.sh"
echo "========================================"
