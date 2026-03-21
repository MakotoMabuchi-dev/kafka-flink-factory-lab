#!/usr/bin/env bash
set -euo pipefail

# Role:
#   Kafka の topic を指定して内容を確認する consumer 起動スクリプト
#
# Do:
#   - 引数で topic を指定して consumer を起動する
#   - all 指定時は利用可能な topic 一覧を表示する
#   - --from-beginning を付けると先頭から読む
#
# Don't:
#   - Kafka コンテナの起動はしない
#   - topic の作成はしない
#
# Usage:
#   ./watch_topics.sh process-events
#   ./watch_topics.sh sensor-a
#   ./watch_topics.sh sensor-b
#   ./watch_topics.sh all
#   ./watch_topics.sh process-events --from-beginning

BOOTSTRAP_SERVER="localhost:29092"
KAFKA_CONTAINER="kafka"

TOPIC="${1:-all}"
FROM_BEGINNING="${2:-}"

show_usage() {
    echo "Usage:"
    echo "  ./watch_topics.sh process-events"
    echo "  ./watch_topics.sh sensor-a"
    echo "  ./watch_topics.sh sensor-b"
    echo "  ./watch_topics.sh all"
    echo "  ./watch_topics.sh <topic> --from-beginning"
}

list_topics() {
    echo "========================================"
    echo "Available topics"
    echo "========================================"
    docker exec -i "${KAFKA_CONTAINER}" kafka-topics \
        --list \
        --bootstrap-server "${BOOTSTRAP_SERVER}"
}

watch_topic() {
    local topic_name="$1"

    echo "[INFO] Watching topic: ${topic_name}"

    if [[ "${FROM_BEGINNING}" == "--from-beginning" ]]; then
        docker exec -it "${KAFKA_CONTAINER}" kafka-console-consumer \
            --topic "${topic_name}" \
            --from-beginning \
            --bootstrap-server "${BOOTSTRAP_SERVER}"
    else
        docker exec -it "${KAFKA_CONTAINER}" kafka-console-consumer \
            --topic "${topic_name}" \
            --bootstrap-server "${BOOTSTRAP_SERVER}"
    fi
}

case "${TOPIC}" in
    sensor-a|sensor-b|process-events)
        watch_topic "${TOPIC}"
        ;;
    all)
        list_topics
        echo
        echo "Select one of the topics above, for example:"
        echo "  ./watch_topics.sh process-events"
        echo "  ./watch_topics.sh sensor-a --from-beginning"
        ;;
    *)
        echo "[ERROR] Unknown topic: ${TOPIC}" >&2
        echo >&2
        show_usage
        exit 1
        ;;
esac