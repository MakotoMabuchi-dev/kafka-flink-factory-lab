#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -gt 0 ]]; then
    SQL_FILE="$1"
else
    SQL_FILE="${SCRIPT_DIR}/sql/job.sql"
fi

exec python3 "${BASE_DIR}/scripts/lab_cli.py" apply-sql --file "${SQL_FILE}"
