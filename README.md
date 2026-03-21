# Kafka + Flink Factory Streaming Lab

この README は、Kafka + Flink + Python のローカル検証環境について、
構成、通信方式、利用手順、トラブルシュートをまとめたものです。

## 構成
- Python (Mac) → Kafka (Docker) → Flink (Docker)
- 将来拡張: Iceberg / Trino / Superset

## 接続ルール
- Python → Kafka: localhost:29092
- Flink → Kafka: kafka:9092

## 主要ファイル
- kafka-test/docker-compose.yml
- flink-test/docker-compose.yml
- flink-test/Dockerfile
- flink-test/apply_job.sh
- flink-test/job.sql
- flink-test/job_summary.sql
- python-producer/send_factory_data.py

## 起動
- ./start_all.sh
- ./create_topics.sh
- python send_factory_data.py

## Flink ジョブ反映
- ./flink-test/apply_job.sh ./flink-test/job.sql
- ./flink-test/apply_job.sh ./flink-test/job_summary.sql

## Kafka 確認
- ./watch_topics.sh process-events
- ./watch_topics.sh sensor-a
- ./watch_topics.sh sensor-b

## 代表的なハマりどころ
- Kafka advertised.listeners の不整合
- 別 compose / 別 network による名前解決失敗
- Flink Kafka connector JAR 未導入
- SQL 予約語 current の使用
- 古い Flink ジョブの残存
