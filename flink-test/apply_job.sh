#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Flink の SQL ジョブ定義を flink-jobmanager コンテナへ反映し、
#   sql-client.sh -f で実行する。
#
# Do:
#   - 指定した SQL ファイルの存在確認
#   - 引数未指定時は「このスクリプトと同じディレクトリ」の job.sql を使う
#   - flink-jobmanager コンテナの起動確認
#   - Kafka connector JAR の存在確認
#   - SQL ファイルをコンテナへコピー
#   - Flink SQL Client でファイル実行
#
# Don't:
#   - Kafka / Flink コンテナ自体の起動は行わない
#   - 既存ジョブの停止は自動では行わない
#
# Usage:
#   ./apply_job.sh
#   ./apply_job.sh ./job.sql
#   ./apply_job.sh /full/path/to/job_summary.sql

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SQL_FILE="${SCRIPT_DIR}/job.sql"
SQL_FILE="${1:-$DEFAULT_SQL_FILE}"

CONTAINER_NAME="flink-jobmanager"
CONTAINER_SQL_PATH="/opt/flink/job.sql"
REQUIRED_KAFKA_CONNECTOR="flink-sql-connector-kafka-3.2.0-1.18.jar"

echo "========================================"
echo "Apply Flink SQL Job"
echo "========================================"
echo "[INFO] Script dir: ${SCRIPT_DIR}"
echo "[INFO] Local SQL file: ${SQL_FILE}"
echo "[INFO] Target container: ${CONTAINER_NAME}"
echo "[INFO] Container path: ${CONTAINER_SQL_PATH}"
echo

if [[ ! -f "${SQL_FILE}" ]]; then
    echo "[ERROR] SQL file not found: ${SQL_FILE}" >&2
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    echo "[ERROR] Container '${CONTAINER_NAME}' is not running." >&2
    echo "        Start Flink first, for example:" >&2
    echo "        cd ${SCRIPT_DIR} && docker compose up -d --build" >&2
    exit 1
fi

if ! docker exec "${CONTAINER_NAME}" sh -c "ls /opt/flink/lib | grep -q '${REQUIRED_KAFKA_CONNECTOR}'"; then
    echo "[ERROR] Kafka connector jar is missing in ${CONTAINER_NAME}." >&2
    echo "        Required: ${REQUIRED_KAFKA_CONNECTOR}" >&2
    echo "        Rebuild Flink image first:" >&2
    echo "        cd ${SCRIPT_DIR} && docker compose build --no-cache && docker compose up -d" >&2
    exit 1
fi

echo "[INFO] Copying SQL file to container..."
docker cp "${SQL_FILE}" "${CONTAINER_NAME}:${CONTAINER_SQL_PATH}"

echo "[INFO] Executing SQL file with Flink SQL Client..."
docker exec -it "${CONTAINER_NAME}" ./bin/sql-client.sh -f "${CONTAINER_SQL_PATH}"

echo
echo "========================================"
echo "[INFO] SQL file execution finished."
echo "[INFO] To inspect running jobs:"
echo "docker exec -it ${CONTAINER_NAME} ./bin/flink list"
echo
echo "[INFO] To inspect TaskManager logs:"
echo "docker logs -f flink-taskmanager"
echo "========================================"