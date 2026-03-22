# Kafka + Flink + Iceberg Factory Streaming Lab

Kafka、Flink、MinIO、Iceberg REST Catalog をローカル Docker 上で動かし、
工場設備を模したストリーミング処理を学ぶためのリポジトリです。

この README は、Apache Kafka / Flink / Iceberg を初めて触る人でも、
このリポジトリの起動、停止、動作確認、基本的な用語理解まで進められることを目標にしています。

## これは何をするリポジトリか

- Python producer が擬似工場データを Kafka topic に送る
- Flink SQL が Kafka からイベントを読み、工程ごとの明細や製品単位 summary を作る
- Iceberg を使うと、その結果をテーブルとして永続保存できる
- MinIO は Iceberg の実ファイル保存先として使う
- Iceberg REST Catalog は、Iceberg テーブルの metadata を管理する

## 重要な用語

- Kafka
  - イベントをためるメッセージ基盤です
  - このリポジトリでは `sensor-a` `sensor-b` `process-events` の 3 topic を使います
- Flink
  - ストリーミング処理エンジンです
  - Kafka から継続的にデータを読み、SQL で加工します
- Iceberg
  - データレイク向けのテーブル形式です
  - Parquet などのファイルを「テーブル」として管理しやすくします
- MinIO
  - S3 互換のオブジェクトストレージです
  - Iceberg の Parquet や metadata JSON の保存先です
- Catalog
  - Iceberg テーブルの台帳です
  - 「どのテーブルがどの metadata を使っているか」を管理します
- Iceberg REST Catalog
  - Catalog を HTTP API として公開したものです
  - Flink や将来の Trino が共通で同じ Iceberg テーブルを参照できます

## 全体構成

```text
Python producer
  -> Kafka
  -> Flink SQL
  -> Iceberg REST Catalog
  -> MinIO
```

もう少し正確に書くと、役割はこう分かれます。

- Python -> Kafka
  - 擬似センサーデータを送る
- Flink -> Kafka
  - データを読む
- Flink -> Iceberg REST Catalog
  - テーブル定義や snapshot 情報を問い合わせる
- Flink -> MinIO
  - 実際の Parquet / metadata file を書く

## 現在の実装範囲

- Kafka / Zookeeper は Docker Compose で起動する
- Flink JobManager / TaskManager は Docker Compose で起動する
- MinIO と Iceberg REST Catalog も Docker Compose で起動する
- `job.sql` と `job_summary.sql` は現在 `print` sink へ出力する
- Iceberg への最小疎通確認は、Flink SQL Client から手動で行う

重要:

- `job.sql` と `job_summary.sql` は、まだ Iceberg へ保存するようにはしていません
- まずは `sample_events` のような小さなテーブルで Iceberg 疎通を確認し、その後に本番用 SQL を広げる想定です

## ディレクトリ構成

- `python-producer/send_factory_data.py`
  - 工場設備を模した擬似データを Kafka に送る producer
- `kafka-test/docker-compose.yml`
  - Kafka / Zookeeper の compose
- `flink-test/docker-compose.yml`
  - Flink / MinIO / Iceberg REST Catalog の compose
- `flink-test/Dockerfile`
  - Kafka connector、Iceberg、Hadoop client JAR を含んだ Flink イメージ
- `flink-test/job.sql`
  - 工程区間とセンサーイベントを join して明細を作る SQL
- `flink-test/job_summary.sql`
  - 製品シリアル単位の summary を作る SQL
- `start_all.sh`
  - Kafka / Flink / MinIO / Iceberg REST を起動し、bucket と topic を確認する
- `stop_all.sh`
  - Kafka / Flink / MinIO / Iceberg REST を停止する
- `create_topics.sh`
  - 学習用 Kafka topic を作成する
- `watch_topics.sh`
  - Kafka topic を監視する

## 前提条件

- Docker Desktop が起動していること
- `docker compose` が使えること
- `python3` が使えること
- 次のポートが空いていること
  - `2181` Zookeeper
  - `29092` Kafka
  - `8081` Flink UI
  - `8181` Iceberg REST Catalog
  - `9000` MinIO API
  - `9001` MinIO Console

## Python セットアップ

producer を実行する前に依存を入れます。

```bash
cd /Users/makoto/docker-lab
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## 一番重要な起動と停止

### 起動

```bash
cd /Users/makoto/docker-lab
./start_all.sh
```

`start_all.sh` は次を行います。

- Docker daemon が使えるまで待つ
- `stream-shared` network がなければ作る
- Kafka stack を起動する
- Flink / MinIO / Iceberg REST stack を起動する
- `warehouse` bucket が存在するか確認する
- Kafka topic を作成する

起動後に確認できる URL:

- Flink UI: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`
- Iceberg REST Catalog: `http://localhost:8181`

### 停止

```bash
cd /Users/makoto/docker-lab
./stop_all.sh
```

`stop_all.sh` はコンテナを停止しますが、次のデータは残ります。

- `flink-test/minio-data`
  - MinIO の保存データ
- `flink-test/iceberg-rest-data`
  - Iceberg REST Catalog の metadata SQLite

つまり、`stop_all.sh` のあとに `start_all.sh` で再開しても、
MinIO 上のファイルと Iceberg Catalog の metadata は引き継がれます。

注意:

- Flink SQL Client で実行した `CREATE CATALOG lakehouse ...` はセッション定義です
- Iceberg の table metadata 自体は保存されますが、Flink SQL Client を開き直したら `CREATE CATALOG` は再度実行してください

## クイックスタート

最短で流れをつかむなら、次の順番です。

1. 依存を入れる

```bash
cd /Users/makoto/docker-lab
source .venv/bin/activate
```

2. 基盤を起動する

```bash
./start_all.sh
```

3. producer を流す

```bash
python3 python-producer/send_factory_data.py
```

4. 別ターミナルで Flink の既存 SQL を試す

```bash
./flink-test/apply_job.sh ./flink-test/job.sql
```

summary を見たい場合:

```bash
./flink-test/apply_job.sh ./flink-test/job_summary.sql
```

5. Iceberg 疎通を試す
   - 下の「Iceberg の最小確認」を実行する

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

## まず動作確認したい人向けの重要コマンド

### コンテナ一覧

```bash
docker ps
```

### Kafka topic 一覧

```bash
./watch_topics.sh all
```

### Kafka topic の中身を見る

```bash
./watch_topics.sh process-events
./watch_topics.sh sensor-a
./watch_topics.sh sensor-b
./watch_topics.sh process-events --from-beginning
```

### Flink の実行中ジョブを見る

```bash
docker exec -it flink-jobmanager ./bin/flink list
```

### Flink ジョブを止める

```bash
docker exec -it flink-jobmanager ./bin/flink cancel <JOB_ID>
```

### Flink TaskManager ログを見る

```bash
docker logs -f flink-taskmanager
```

### Iceberg REST Catalog ログを見る

```bash
docker logs -f iceberg-rest
```

### MinIO 上の bucket を見る

```bash
docker exec mc /usr/bin/mc ls local
```

### MinIO 上の Iceberg ファイルを辿る

```bash
docker exec mc /usr/bin/mc find local/warehouse
```

## Iceberg の最小確認

この手順では、既存ジョブを変更せずに、
Flink から Iceberg に 1 テーブル書けることだけを確認します。

### 1. Flink SQL Client に入る

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

### 2. Iceberg REST Catalog を登録する

Flink SQL Client の中で実行します。

```sql
CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  's3.path-style-access' = 'true'
);
```

### 3. catalog と database を使う

```sql
USE CATALOG lakehouse;
CREATE DATABASE IF NOT EXISTS lab;
USE lab;
SHOW TABLES;
```

### 4. テスト用テーブルを作る

```sql
CREATE TABLE sample_events (
  id BIGINT,
  name STRING,
  created_at TIMESTAMP(3)
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);
```

### 5. データを書いて読み返す

```sql
INSERT INTO sample_events VALUES
  (1, 'first', CURRENT_TIMESTAMP);

SELECT * FROM sample_events;
```

### 6. MinIO 側でファイルを確認する

別ターミナルで実行します。

```bash
docker exec mc /usr/bin/mc find local/warehouse
```

ここで `metadata` や `data` 配下のファイルが見えれば、
`Flink -> Iceberg REST Catalog -> MinIO` の経路が動いています。

## 既存 SQL ジョブの意味

### `flink-test/job.sql`

- `process-events` から工程区間を作る
- `sensor-a` を `process_1` 区間に join する
- `sensor-b` を `process_2` 区間に join する
- 明細イベントを `print` sink に出す
- まずはイベントの紐付けが正しいか確認するための SQL

### `flink-test/job_summary.sql`

- 工程区間を基に製品シリアル単位の summary を作る
- センサーイベントが 0 件でも製品行自体は残す
- `cnt_sensor_a` / `cnt_sensor_b` で欠損状況を判断できる
- disturbance は停止理由とセンサーステータスから算出する
- 現在は `print` sink に出す

## Iceberg に既存 summary を保存したいときの考え方

今の `job_summary.sql` は最終的に `product_summary_print` へ出力しています。
Iceberg に保存したいときは、最後の sink を Iceberg table に置き換えます。

考え方は次のとおりです。

1. 既存の view `product_summary` はそのまま使う
2. Iceberg table を別名で作る
3. `INSERT INTO iceberg_table SELECT ... FROM product_summary` に変える

例:

```sql
CREATE TABLE product_summary_iceberg (
  serial_no STRING,
  equipment_id STRING,
  process_1_start TIMESTAMP(3),
  process_1_end TIMESTAMP(3),
  process_1_duration_sec BIGINT,
  avg_temp_a DOUBLE,
  max_temp_a DOUBLE,
  avg_vibration_a DOUBLE,
  cnt_sensor_a BIGINT,
  process_2_start TIMESTAMP(3),
  process_2_end TIMESTAMP(3),
  process_2_duration_sec BIGINT,
  avg_pressure_b DOUBLE,
  max_pressure_b DOUBLE,
  avg_current_b DOUBLE,
  cnt_sensor_b BIGINT,
  has_disturbance BOOLEAN
) WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet'
);

INSERT INTO product_summary_iceberg
SELECT
  serial_no,
  equipment_id,
  process_1_start,
  process_1_end,
  process_1_duration_sec,
  avg_temp_a,
  max_temp_a,
  avg_vibration_a,
  cnt_sensor_a,
  process_2_start,
  process_2_end,
  process_2_duration_sec,
  avg_pressure_b,
  max_pressure_b,
  avg_current_b,
  cnt_sensor_b,
  has_disturbance
FROM product_summary;
```

## 典型的な作業フロー

### 1. 基盤だけ起動して疎通を見る

```bash
./start_all.sh
docker ps
docker exec mc /usr/bin/mc ls local
```

### 2. producer を流して Kafka を見る

```bash
python3 python-producer/send_factory_data.py
./watch_topics.sh process-events
```

### 3. Flink SQL を実行してログを見る

```bash
./flink-test/apply_job.sh ./flink-test/job.sql
docker logs -f flink-taskmanager
```

### 4. Iceberg のテストテーブルを作る

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

その後、上の `CREATE CATALOG` と `CREATE TABLE sample_events` を実行します。

## よくあるトラブル

### `ModuleNotFoundError: No module named 'kafka'`

- 仮想環境が有効か確認する
- `python3 -m pip install -r requirements.txt` を再実行する

### `Cannot connect to the Docker daemon`

- Docker Desktop が起動しているか確認する
- `docker ps` が成功する状態で再実行する

### `network stream-shared declared as external, but could not be found`

- 通常は `start_all.sh` が自動作成する
- 手動で作るなら次を実行する

```bash
docker network create stream-shared
```

### Flink SQL で `org.apache.hadoop.conf.Configuration` が見つからない

- Flink イメージに Hadoop client JAR が入っていない
- `flink-test/Dockerfile` に Hadoop client の `COPY` があるか確認する
- 必要なら再 build する

```bash
cd /Users/makoto/docker-lab/flink-test
docker compose build --no-cache
docker compose up -d
```

### Iceberg 作成時に `UnknownHostException: warehouse.minio` が出る

- REST catalog 側の path-style 設定が効いていない
- `flink-test/docker-compose.yml` の `iceberg-rest` に
  `CATALOG_S3_PATH__STYLE__ACCESS=true` があるか確認する
- 変更後は `iceberg-rest` を再作成する

```bash
cd /Users/makoto/docker-lab/flink-test
docker compose up -d --force-recreate iceberg-rest
```

### `warehouse` bucket が見つからない

- `mc` コンテナが起動しているか確認する
- `docker exec mc /usr/bin/mc ls local` を実行する
- 通常は `start_all.sh` が bucket 存在確認まで行う

### summary に値が出ない、または一部列が `NULL`

- producer が動いているか確認する
- `process-events` に start/end が両方出ているか確認する
- summary は completed interval を基準に作るため、工程終了前は未確定になる
- センサーが欠損した場合でも行は残るが、平均値列は `NULL`、件数列は `0` になる

### `current` が SQL 予約語と衝突する

- `sensor-b` の電流列は SQL 内で `` `current` `` として参照している

## 補足

- `start_all.sh` は毎回 `create_topics.sh` も実行する
- 既存の Flink ジョブは自動停止しない
- ジョブを切り替えるときは `flink list` で job id を見て明示的に cancel する
- MinIO の認証情報 `minioadmin / minioadmin` は学習用の固定値であり、ローカル用途前提
- 将来 Superset までつなぐときは、Trino を追加して Superset から Trino 経由で Iceberg table を読む構成が自然
