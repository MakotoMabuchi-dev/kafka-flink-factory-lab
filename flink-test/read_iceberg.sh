#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Iceberg に保存された summary table を
#   Flink SQL Client で読み出す。
#
# Do:
#   - 指定した SQL ファイルの存在確認
#   - 引数未指定時はこのディレクトリの read_iceberg_summary.sql を使う
#   - flink-jobmanager コンテナの起動確認
#   - SQL ファイルをコンテナへコピー
#   - sql-client.sh -f で実行
#
# Usage:
#   ./read_iceberg.sh
#   ./read_iceberg.sh ./read_iceberg_summary.sql

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SQL_FILE="${SCRIPT_DIR}/read_iceberg_summary.sql"
SQL_FILE="${1:-$DEFAULT_SQL_FILE}"

CONTAINER_NAME="flink-jobmanager"
CONTAINER_SQL_PATH="/opt/flink/read_iceberg_summary.sql"

echo "========================================"
echo "Read Iceberg Summary"
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

echo "[INFO] Copying SQL file to container..."
docker cp "${SQL_FILE}" "${CONTAINER_NAME}:${CONTAINER_SQL_PATH}"

echo "[INFO] Executing SQL file with Flink SQL Client..."
docker exec -i "${CONTAINER_NAME}" ./bin/sql-client.sh -f "${CONTAINER_SQL_PATH}"
