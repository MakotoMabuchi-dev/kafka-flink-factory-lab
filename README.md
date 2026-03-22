# Kafka + Flink Factory Streaming Lab

Kafka + Flink + Python で、工場設備を模したストリーミング処理をローカルで検証するためのリポジトリです。
Python producer が Kafka に 3 種類のイベントを送り、Flink SQL が明細ジョブと summary ジョブを実行します。

## 全体構成

- Python (Mac) -> Kafka (Docker) -> Flink (Docker)
- Python から Kafka へは `localhost:29092`
- Flink コンテナから Kafka へは `kafka:9092`
- 将来拡張候補: Iceberg / Trino / Superset

## ディレクトリと役割

- `python-producer/send_factory_data.py`
  - 工場設備を模した擬似データを Kafka に送る producer
- `kafka-test/docker-compose.yml`
  - Kafka / Zookeeper の compose
- `flink-test/docker-compose.yml`
  - Flink JobManager / TaskManager の compose
- `flink-test/Dockerfile`
  - Kafka connector JAR を含んだ Flink イメージ
- `flink-test/job.sql`
  - process interval と sensor イベントを紐付ける明細ジョブ
- `flink-test/job_summary.sql`
  - 製品シリアル単位の 1 行 summary を作るジョブ
- `start_all.sh`
  - Kafka / Flink 起動と topic 作成
- `stop_all.sh`
  - Kafka / Flink 停止
- `create_topics.sh`
  - 学習用 topic を作成
- `watch_topics.sh`
  - Kafka topic を監視

## Kafka topic

- `sensor-a`
  - 工程 1 側センサー
  - 温度 `value_temp`、振動 `value_vibration` を送る
- `sensor-b`
  - 工程 2 側センサー
  - 圧力 `pressure`、電流 `current` を送る
- `process-events`
  - 製品シリアル単位の工程開始 / 終了イベント
  - `process_1` と `process_2` の start/end を送る

## 前提条件

- Docker Desktop が起動していること
- `docker compose` が使えること
- `python3` が使えること
- 利用ポート `2181` `29092` `8081` が空いていること
- Docker external network `stream-shared` が存在すること

初回のみ、shared network が未作成なら先に作成します。

```bash
docker network create stream-shared
```

## Python セットアップ

producer 実行前に依存パッケージを入れます。`kafka-python` が入っていないと `ModuleNotFoundError: No module named 'kafka'` で失敗します。

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## クイックスタート

リポジトリのルートで以下を実行します。

```bash
./start_all.sh
python3 python-producer/send_factory_data.py
./flink-test/apply_job.sh ./flink-test/job.sql
```

summary ジョブを見たい場合は次を使います。

```bash
./flink-test/apply_job.sh ./flink-test/job_summary.sql
```

## 標準的な実行順序

1. `docker network create stream-shared` を必要なら実行
2. Python 仮想環境を作成し、`pip install -r requirements.txt`
3. `./start_all.sh` で Kafka / Flink を起動
4. `python3 python-producer/send_factory_data.py` でデータ送信開始
5. `./flink-test/apply_job.sh ./flink-test/job.sql` または `./flink-test/apply_job.sh ./flink-test/job_summary.sql`
6. `http://localhost:8081` で Flink UI を確認

## 各ジョブの意味

### `job.sql`

- `process-events` から工程区間を作成
- `sensor-a` を `process_1` 区間に join
- `sensor-b` を `process_2` 区間に join
- print sink に明細イベントを出力
- Python 側の ISO 文字列からミリ秒精度で時刻を保持して処理

### `job_summary.sql`

- completed interval を基準に製品シリアル単位の summary を作成
- センサーイベントが 0 件でも製品行自体は残す
- `cnt_sensor_a` / `cnt_sensor_b` で欠損状況を判定できる
- disturbance は停止理由とセンサーステータスから算出
- Python 側の時刻をミリ秒精度で保持したまま集計

## 動作確認

Kafka topic の一覧確認:

```bash
./watch_topics.sh all
```

特定 topic の監視:

```bash
./watch_topics.sh process-events
./watch_topics.sh sensor-a
./watch_topics.sh sensor-b
./watch_topics.sh process-events --from-beginning
```

Flink ジョブ一覧:

```bash
docker exec -it flink-jobmanager ./bin/flink list
```

TaskManager ログ確認:

```bash
docker logs -f flink-taskmanager
```

## 停止

```bash
./stop_all.sh
```

producer は別ターミナルで `Ctrl + C` で停止します。

## よくあるトラブル

### `ModuleNotFoundError: No module named 'kafka'`

- 仮想環境が有効か確認
- `python3 -m pip install -r requirements.txt` を再実行

### `Cannot connect to the Docker daemon`

- Docker Desktop が起動しているか確認
- `docker ps` が成功する状態で再実行

### `network stream-shared declared as external, but could not be found`

- 次を実行して network を作成

```bash
docker network create stream-shared
```

### Flink SQL ジョブ投入時に Kafka connector が見つからない

- `flink-test/Dockerfile` で connector JAR を `/opt/flink/lib/` にコピーしているか確認
- 必要なら再 build

```bash
cd flink-test
docker compose build --no-cache
docker compose up -d
```

### summary に値が出ない、または一部列が NULL

- producer が動いているか確認
- `process-events` に start/end が両方出ているか確認
- summary は completed interval を基準に作るため、工程終了前は未確定
- センサーが欠損した場合でも行は残るが、平均値列は `NULL`、件数列は `0` になる

### `current` が SQL 予約語と衝突する

- `sensor-b` の電流列は SQL 内で `` `current` `` として参照している

## 補足

- `start_all.sh` は Kafka / Flink 起動後に `create_topics.sh` も実行します
- 既存の Flink ジョブは自動停止しないため、切り替え時は必要に応じて明示的に cancel してください
