# Kafka + Flink + Iceberg Factory Streaming Lab

Kafka、Flink、MinIO、Iceberg REST Catalog をローカル Docker 上で動かし、
工場設備を模したリアルタイム処理を学ぶためのリポジトリです。

現在は Kafka / Flink / MinIO / Iceberg REST Catalog に加えて、
`trino-test/` の Trino 設定も含み、`lab_console` や `scripts/lab_cli.py` から
Trino を起動して Iceberg を読める状態です。
Superset 本体は同梱しませんが、手動追加用の手順と overlay 雛形を含めています。

この README では、背景説明より先に、
すぐ試せる導線、`lab_console` の全コマンド説明、永続データの扱い、
Iceberg と Trino までの確認手順を追えるようにしています。

## 目次

- [クイックスタート](#quickstart)
- [lab_console コマンド一覧](#lab-console-commands)
- [システム構成図](#system-architecture)
- [現在の実装範囲](#implementation-status)
- [ディレクトリ構成](#directory-layout)
- [前提条件](#prerequisites)
- [起動と停止](#start-and-stop)
- [Kafka topic とサンプルデータ](#kafka-and-sample-data)
- [Iceberg に summary を保存して読む](#iceberg-read-write)
- [Trino で Iceberg を読む](#trino-read)
- [Superset を手動で追加する](#superset-add)
- [典型的な作業フロー](#typical-workflow)
- [よくあるトラブル](#troubleshooting)

<a id="quickstart"></a>
## クイックスタート

最短で理解するには、次の 2 通りがあります。

### 1. 永続データを残しながら進める手順

Trino やあとで Superset までつなぐ学習には、こちらを推奨します。
`lab_console` は開始時と終了時に保存データを消すので、継続確認には向きません。

macOS / Linux:

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py start
python3 scripts/lab_cli.py trino-start

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 python-producer/send_factory_data.py
```

別ターミナル:

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
python3 scripts/lab_cli.py inspect-iceberg
python3 scripts/lab_cli.py read-iceberg
```

Trino まで見るなら:

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py trino-status
python3 scripts/lab_cli.py trino-history
python3 scripts/lab_cli.py trino-snapshots
python3 scripts/lab_cli.py trino-read
```

対話的に触るなら:

```bash
python3 scripts/lab_cli.py trino-shell
```

Superset まで進めるなら、ここまで確認したあとに
[Superset を手動で追加する](#superset-add) の手順へ進みます。

### 2. まず全部まとめて触ってみる手順

`lab_console` はワンコマンドで起動し、終了時に環境を掃除します。
学習用に毎回まっさらな状態へ戻したいときに向いています。

macOS / Linux:

```bash
cd kafka-flink-factory-lab
./lab_console.sh
```

Windows:

```powershell
cd kafka-flink-factory-lab
.\lab_console.cmd
```

コンソールに入ったら、まず次を打てば流れが見えます。

```text
help
status
run iceberg
inspect-iceberg
read-iceberg
trino-snapshots
trino-read
exit
```

補足:

- `lab_console` は開始時に既存の Trino も停止し、`flink-test/minio-data` と `flink-test/iceberg-rest-data` を消します
- 起動後は Trino も自動で立ち上がるので、`trino-status` や `trino-shell` をすぐ使えます
- `exit` 時には Trino を含む起動済み環境を停止し、同じ保存データを削除します
- producer 用 Python 依存の取得に失敗しても、console 自体は開きます
- producer が起動できていない状態では、`run iceberg` のあとでも table が空のままになる

<a id="system-architecture"></a>
## システム構成図

```mermaid
flowchart LR
    A["工場の設備・センサー<br/>温度 / 振動 / 圧力 / 電流 / 工程開始終了"] --> B["Python Producer<br/>擬似データ送信"]
    B --> C["Kafka<br/>イベントを受け取って一時的に保持"]
    C --> D["Flink SQL<br/>リアルタイムに集計・結合・判定"]
    D --> E["Iceberg REST Catalog<br/>テーブル情報の管理"]
    D --> F["MinIO<br/>Parquet / metadata の保存"]
    E --> F
    F --> G["将来の分析基盤<br/>Trino / Superset / BI ダッシュボード"]
```

## このシステムが必要な背景

工場では、設備が止まったあとに原因を調べるだけでは遅いことがあります。
品質不良、微妙な温度上昇、振動の増加、停止理由の偏りは、
現場では少しずつ起きていても、表計算の手集計では見落としやすいです。

そこで、設備やセンサーから出る情報をリアルタイムに集め、
その場で整理し、あとから分析しやすい形でためる仕組みが必要になります。

このリポジトリは、その縮小版です。

- センサー値が流れ続ける
- 工程の開始と終了イベントが流れる
- それらを時刻で結び付ける
- 製品単位の summary を作る
- 将来は BI で見られる形にする

という一連の流れを、ローカル PC 上で再現できます。

## 今回のシチュエーション

このリポジトリでは、次のような工場を想定しています。

- 1つの製品が `process_1` と `process_2` を順番に通る
- 工程 1 では `sensor-a` が温度と振動を送る
- 工程 2 では `sensor-b` が圧力と電流を送る
- さらに `process-events` が、各製品の工程開始と工程終了を送る

これによって、単なる「時系列のセンサー値」ではなく、
「どの製品が、どの工程にいた時に、どんな状態だったか」を追えるようになります。

## 実際の工場で同じ考え方を使うと何ができるか

- ライン停止の兆候を早く見つける
  - 温度上昇や振動増加を、停止前のパターンとして見つけやすくなります
- 品質不良と設備状態の関係を見る
  - 不良が多い製品群で、圧力や電流の傾向が違うかを分析できます
- 工程ごとの滞留時間や処理時間を追う
  - どこで時間がかかっているかを製品単位で見られます
- 停止理由とセンサー異常を結び付ける
  - 人が入力した停止理由と実際の設備状態を同時に扱えます
- BI ダッシュボードにつなげる
  - 現場の稼働状況、異常件数、平均温度、工程時間などを可視化できます

## このリポジトリで学べること

- イベントデータを Kafka で受ける考え方
- Flink SQL でリアルタイムに結合と集計をする方法
- Iceberg で結果をテーブルとして残す考え方
- MinIO を S3 互換ストレージとして使う方法
- 将来 Trino や Superset につなげるための土台の作り方

## これは何をするリポジトリか

- Python producer が擬似工場データを Kafka topic に送る
- Flink SQL が Kafka からイベントを読み、工程ごとの明細や製品単位 summary を作る
- Iceberg を使うと、その結果をテーブルとして永続保存できる
- MinIO は Iceberg の実ファイル保存先として使う
- Iceberg REST Catalog は、Iceberg テーブルの metadata を管理する
- 将来的には Trino や Superset から BI 用に参照できる構成へ広げやすい

## 重要な用語

- Apache Kafka
  - 何をするものか
    - 工場から流れてくるイベントを受け取り、順番を保ちながらためておく仕組みです
    - たとえると、現場から届く情報をいったん受け止める「高速な受付窓口」です
  - このリポジトリでの役割
    - `sensor-a` `sensor-b` `process-events` の 3 topic を受け持ちます
  - 誰が開発しているか
    - 現在は Apache Software Foundation のオープンソースプロジェクトです
    - もともとは LinkedIn で Jay Kreps、Jun Rao、Neha Narkhede らが作り始めました

- Apache Flink
  - 何をするものか
    - 流れてきたデータを止めずに、その場で計算する仕組みです
    - たとえると、受付に届いた情報をリアルタイムで見て判断する「現場の分析係」です
  - このリポジトリでの役割
    - Kafka のイベントを読み、工程区間を作り、センサーデータを結合し、summary を作ります
  - 誰が開発しているか
    - 現在は Apache Software Foundation のオープンソースプロジェクトです
    - もともとは Stratosphere という研究プロジェクトとして始まり、その後 Apache Flink になりました

- Apache Iceberg
  - 何をするものか
    - 加工したデータを、あとで SQL で安全に読みやすい「テーブル」として保存する仕組みです
    - 単なるファイル置き場ではなく、更新履歴や schema 変更も扱いやすくします
  - このリポジトリでの役割
    - Flink が作った結果を、分析しやすいテーブルとして保存するために使います
  - 誰が開発しているか
    - 現在は Apache Software Foundation のオープンソースプロジェクトです

- MinIO
  - 何をするものか
    - 大きなデータファイルを保存するためのオブジェクトストレージです
    - AWS S3 と似た使い方ができるので、ローカル学習環境でよく使われます
  - このリポジトリでの役割
    - Iceberg の Parquet や metadata JSON の保存先です
  - 誰が開発しているか
    - MinIO 社が開発しているオープンソース製品です
    - 2014 年に Garima Kapoor、Anand Babu "AB" Periasamy、Harshavardhana らが創業しました

- Catalog
  - 何をするものか
    - Iceberg テーブルの台帳です
    - 「どのテーブルが、どの metadata を使っているか」を管理します
  - たとえ
    - MinIO が倉庫なら、Catalog は倉庫の在庫台帳です

- Iceberg REST Catalog
  - 何をするものか
    - Catalog を HTTP API として公開したものです
  - このリポジトリでの役割
    - Flink や将来の Trino が、同じ Iceberg テーブルを共通認識で扱えるようにします

- ZooKeeper
  - 何をするものか
    - 分散システムの設定や状態をそろえるための仕組みです
  - このリポジトリでの役割
    - いまの Kafka 構成では、Kafka の補助コンポーネントとして使っています
  - 補足
    - 新しい Kafka では ZooKeeper を使わない構成も増えていますが、この学習環境では従来構成を使っています

## 全体構成

もう少し正確に書くと、役割はこう分かれます。

- Python -> Kafka
  - 擬似センサーデータを送る
- Flink -> Kafka
  - データを読む
- Flink -> Iceberg REST Catalog
  - テーブル定義や snapshot 情報を問い合わせる
- Flink -> MinIO
  - 実際の Parquet / metadata file を書く
- Trino
  - `trino-test/` の設定で、同じ Iceberg table を SQL で読む
- Superset
  - Superset 本体は vendoring しない
  - `Superset -> Trino -> Iceberg` の順に手動で接続する想定
  - `superset-local/` に shared network 参加用の overlay 雛形を置く

<a id="implementation-status"></a>
## 現在の実装範囲

- Kafka / Zookeeper は `kafka-test/docker-compose.yml` で起動する
- Flink / MinIO / Iceberg REST Catalog は `flink-test/docker-compose.yml` で起動する
- `flink-test/sql/job.sql` と `flink-test/sql/job_summary.sql` は確認用に `print` sink へ出力する
- `flink-test/sql/job_summary_iceberg.sql` は summary を Iceberg table `product_summary_iceberg` に保存する
- `scripts/lab_cli.py read-iceberg` で Flink SQL Client 経由の読み取り確認ができる
- `scripts/lab_cli.py inspect-iceberg` で catalog DB / snapshot / MinIO object 一覧を確認できる
- `scripts/lab_cli.py trino-start` `trino-read` `trino-query` などで Trino を扱える
- `scripts/lab_cli.py console` で対話コンソールを起動でき、Trino も自動起動する
- `trino-test/` に Trino Compose と Iceberg catalog 設定がある
- `superset-local/` に Superset 手動追加用の overlay 雛形がある

重要:

- `scripts/lab_cli.py console` は開始時と終了時に `flink-test/minio-data` と `flink-test/iceberg-rest-data` を削除する
- 永続データを残して Trino や将来の Superset まで確認したい場合は `start` / `stop` を使う
- `flink-test/sql/job_summary_iceberg.sql` は毎回 `DROP TABLE IF EXISTS lakehouse.lab.product_summary_iceberg` を実行する
- Kafka source の `group.id` は固定なので、job を再実行しても過去 topic を再読しないことがある

<a id="directory-layout"></a>
## ディレクトリ構成

- `scripts/lab_cli.py`
  - 共通 CLI
  - `start` `stop` `status` `watch-topic` `apply-sql` `read-iceberg` `inspect-iceberg` `trino-start` `trino-read` `trino-query` `console` などをまとめて扱う
- `lab_console.sh`
  - macOS / Linux から対話コンソールを起動するラッパー
- `lab_console.cmd`
  - Windows から対話コンソールを起動するラッパー
- `python-producer/send_factory_data.py`
  - 工場設備を模した擬似データを Kafka に送る送信スクリプト
- `kafka-test/docker-compose.yml`
  - Kafka / Zookeeper の Compose 定義
- `flink-test/docker-compose.yml`
  - Flink / MinIO / Iceberg REST Catalog の Compose 定義
- `flink-test/Dockerfile`
  - Kafka connector、Iceberg、Hadoop client JAR を含んだ Flink イメージ
- `flink-test/sql/`
  - Flink SQL ジョブ群
- `flink-test/sql/job.sql`
  - 工程区間とセンサーイベントを結合して明細を作る SQL
- `flink-test/sql/job_summary.sql`
  - 製品シリアル単位の summary を作る SQL
- `flink-test/sql/job_summary_iceberg.sql`
  - summary を Iceberg table `product_summary_iceberg` に保存する SQL
- `flink-test/sql/read_iceberg_summary.sql`
  - Iceberg table を読み取るための SQL
- `trino-test/docker-compose.yml`
  - Trino 単体を `stream-shared` network に参加させる Compose 定義
  - `scripts/lab_cli.py trino-start` と `lab_console` はこの Compose を使う
- `trino-test/etc/catalog/iceberg.properties`
  - Trino から Iceberg REST Catalog と MinIO を読むための catalog 設定
- `superset-local/docker-compose.lab.yml`
  - 手動で clone した Superset を `stream-shared` network に参加させる overlay
- `superset-local/README.md`
  - Superset 手動追加の最小メモ
- `start_all.sh` / `start_all.cmd`
  - 起動処理ラッパー
- `stop_all.sh` / `stop_all.cmd`
  - 停止処理ラッパー
- `create_topics.sh` / `create_topics.cmd`
  - topic 作成ラッパー
- `watch_topics.sh` / `watch_topics.cmd`
  - topic 確認ラッパー

<a id="prerequisites"></a>
## 前提条件

- Docker Desktop が起動していること
- `docker compose` が使えること
- Python 3.10 以上が使えること
- 次のポートが空いていること
  - `2181` Zookeeper
  - `29092` Kafka
  - `8080` Trino
  - `8081` Flink UI
  - `8181` Iceberg REST Catalog
  - `9000` MinIO API
  - `9001` MinIO Console

コマンド例について:

- macOS / Linux では `python3` と `./xxx.sh` を使います
- Windows では `py -3` と `.\xxx.cmd`、または `py -3 scripts\lab_cli.py ...` を使います

## Python セットアップ

手動で producer を実行する場合だけ、あらかじめ依存を入れます。

macOS / Linux:

```bash
cd kafka-flink-factory-lab
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Windows PowerShell:

```powershell
cd kafka-flink-factory-lab
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -3 -m pip install --upgrade pip
py -3 -m pip install -r requirements.txt
```

<a id="start-and-stop"></a>
## 一番重要な起動と停止

### 起動

共通:

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py start
```

Windows PowerShell:

```powershell
cd kafka-flink-factory-lab
py -3 scripts\lab_cli.py start
```

Windows では、次のラッパーでも起動できます。

```powershell
.\start_all.cmd
```

macOS / Linux では、互換ラッパーとして次も使えます。

```bash
./start_all.sh
```

この起動処理は次を行います。

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

共通:

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py stop
```

Windows PowerShell:

```powershell
cd kafka-flink-factory-lab
py -3 scripts\lab_cli.py stop
```

Windows では、次のラッパーでも停止できます。

```powershell
.\stop_all.cmd
```

macOS / Linux では、互換ラッパーとして次も使えます。

```bash
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

<a id="lab-console-commands"></a>
## lab_console コマンド一覧

対話コンソールは `./lab_console.sh` `.\lab_console.cmd` `python3 scripts/lab_cli.py console` のいずれでも起動できます。

起動時の挙動:

- 既存の Kafka / Flink / MinIO / Iceberg REST / Trino を一度停止する
- `flink-test/minio-data` `flink-test/iceberg-rest-data` `.lab-runtime` を削除する
- stack を起動する
- Trino を起動する
- producer の起動を試みる
- その後に `lab>` プロンプトを出す

終了時の挙動:

- `exit` または `quit` で終了できる
- 起動した Kafka / Flink / MinIO / Iceberg REST / Trino を停止する
- `flink-test/minio-data` `flink-test/iceberg-rest-data` `.lab-venv` `.lab-runtime` を削除する

重要:

- 永続データを残したい学習では `console` ではなく `start` / `stop` を使う
- producer 用依存の取得に失敗した場合でも、console 自体は起動する
- producer が起動していない場合、`run iceberg` 自体は成功しても結果行数は 0 のままになり得る

各コマンド:

- `help`
  - 使えるコマンド一覧を表示する
- `status`
  - Docker コンテナ一覧、Flink jobs、producer の稼働状態、Trino の ready 状態をまとめて表示する
- `topics`
  - Kafka topic 一覧を表示する
- `watch <topic> [count]`
  - 指定 topic を先頭から読む
  - `topic` を省略すると `process-events` を対象にする
  - `count` を省略すると 5 件まで表示する
  - 例: `watch process-events 5`
- `run detail`
  - 実行中 Flink job を cancel してから `flink-test/sql/job.sql` を流す
  - 明細の確認用で、出力は `flink-log` に出る
- `run summary`
  - 実行中 Flink job を cancel してから `flink-test/sql/job_summary.sql` を流す
  - 製品単位の summary を `print` sink で確認する
- `run iceberg`
  - 実行中 Flink job を cancel してから `flink-test/sql/job_summary_iceberg.sql` を流す
  - summary を `lakehouse.lab.product_summary_iceberg` に保存する
- `flink-log`
  - TaskManager の最新ログを表示する
  - `print` sink の結果確認に使う
- `producer-log`
  - Python producer の最新ログを表示する
- `producer-start [difficulty]`
  - producer を起動する
  - 難易度を付けるとその値で起動する
  - 例: `producer-start REALISTIC`
- `producer-stop`
  - producer を停止する
- `difficulty`
  - 現在の producer 難易度を表示する
- `difficulty <level>`
  - 難易度を変更し、producer をその設定で再起動する
  - `IDEAL` `BASIC_DISTURBANCE` `REALISTIC` `HARSH` が使える
- `jobs`
  - Flink の実行中 job 一覧を表示する
- `cancel-jobs`
  - 実行中 Flink job をすべて cancel する
- `iceberg-files`
  - MinIO 上の `local/warehouse` 配下を一覧する
  - Iceberg metadata や data file の配置確認に使う
- `iceberg-log`
  - Iceberg REST Catalog コンテナの最新ログを表示する
- `inspect-iceberg`
  - `flink-test/iceberg-rest-data/catalog.db` の登録情報を見る
  - 最新 metadata JSON から snapshot / record 数 / data file 数を見る
  - MinIO 上の object 一覧もまとめて確認する
- `read-iceberg`
  - Flink SQL Client で `SHOW TABLES` と `SELECT * FROM product_summary_iceberg` を実行する
- `trino-status`
  - Trino が `STOPPED` `STARTING` `RUNNING` のどれかを表示する
  - `RUNNING` は Trino CLI の `SELECT 1` が通る状態を意味する
- `trino-start`
  - `trino-test/docker-compose.yml` を使って Trino を起動する
  - すでに起動済みでも再利用される
- `trino-stop`
  - Trino コンテナを停止する
- `trino-shell`
  - `docker exec -it trino trino --catalog iceberg --schema lab` を開く
  - metadata と本表の確認を対話的に進めたいときに使う
- `trino-read`
  - Trino 経由で `SELECT * FROM iceberg.lab.product_summary_iceberg LIMIT 20` を実行する
- `trino-history`
  - Trino 経由で `SELECT * FROM iceberg.lab."product_summary_iceberg$history"` を実行する
- `trino-snapshots`
  - Trino 経由で `SELECT * FROM iceberg.lab."product_summary_iceberg$snapshots"` を実行する
- `trino-query <SQL>`
  - 任意の SQL を Trino で実行する
  - 空白や `$history` を含む SQL は全体を引用して渡すと安全
  - 例: `trino-query "SELECT count(*) FROM iceberg.lab.product_summary_iceberg"`
- `exit`
  - console を終了し、起動した環境と保存データを削除する

最初に試しやすい流れ:

- `help`
- `status`
- `watch process-events 5`
- `run iceberg`
- `inspect-iceberg`
- `read-iceberg`
- `trino-snapshots`
- `trino-read`
- `exit`

<a id="kafka-and-sample-data"></a>
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

## サンプルデータ

このリポジトリでは、3つの topic に次のような JSON が流れます。

### `sensor-a`

工程 1 側のセンサー値です。

```json
{
  "sensor_id": "A",
  "event_time_str": "2026-03-22T10:00:01.123456",
  "value_temp": 31.2,
  "value_vibration": 0.42,
  "unit": "machine-01",
  "status": "RUNNING"
}
```

意味:

- `value_temp`
  - 温度
- `value_vibration`
  - 振動
- `unit`
  - どの設備で出た値か
- `status`
  - 稼働状態

### `sensor-b`

工程 2 側のセンサー値です。

```json
{
  "sensor_code": "B-01",
  "ts_str": "2026-03-22T10:00:02.456789",
  "pressure": 102.4,
  "current": 8.7,
  "line": "machine-01",
  "status": "RUNNING"
}
```

意味:

- `pressure`
  - 圧力
- `current`
  - 電流
- `line`
  - どの設備で出た値か
- `status`
  - 稼働状態

### `process-events`

製品単位の工程開始 / 工程終了イベントです。

```json
{
  "serial_no": "SN00000123",
  "process": "process_1",
  "event_type": "start",
  "event_time_str": "2026-03-22T10:00:00.000000",
  "equipment_id": "machine-01",
  "reason": null
}
```

意味:

- `serial_no`
  - 製品シリアル番号
- `process`
  - どの工程か
- `event_type`
  - `start` または `end`
- `equipment_id`
  - どの設備で処理したか
- `reason`
  - 停止や乱れが含まれた場合の補助情報

## Flink でどう結合されるか

`sensor-a` と `sensor-b` は、製品シリアル番号を直接持っていません。
そのため、Flink はまず `process-events` を基準にして、
「どの製品が、いつからいつまで、その設備で処理されていたか」という工程区間を作ります。

流れはこうです。

1. `process-events` から `start` と `end` を集めて工程区間を作る
2. `process_1` の区間には `sensor-a` を結び付ける
3. `process_2` の区間には `sensor-b` を結び付ける
4. 時刻がその区間の中に入っているセンサー値だけを採用する
5. それを製品単位の明細や summary にまとめる

イメージ:

```text
process-events:
  SN00000123 / process_1 / start 10:00:00
  SN00000123 / process_1 / end   10:00:10

sensor-a:
  machine-01 / 10:00:01 / temp=31.2
  machine-01 / 10:00:05 / temp=31.5
  machine-01 / 10:00:11 / temp=31.8

Flink が結び付ける結果:
  10:00:01 と 10:00:05 は process_1 の区間内なので採用
  10:00:11 は区間外なので採用しない
```

この仕組みによって、
「ただの時系列センサーデータ」だったものを、
「ある製品が工程を通っていた間の設備状態」に変換できます。

## このシミュレーターが再現する現実

このリポジトリの producer は、理想状態だけでなく、
現実の工場で起きるような乱れも段階的に再現できます。

`python-producer/send_factory_data.py` には難易度設定があり、
`CURRENT_DIFFICULTY` を切り替えるだけでシナリオを変えられます。

用意している段階:

- `IDEAL`
  - 停止なし、欠損なし、遅延なしの理想状態
- `BASIC_DISTURBANCE`
  - 軽い停止、軽いドロップ、軽い遅延
- `REALISTIC`
  - 現実的な停止、欠損、ノイズ、送信遅延
- `HARSH`
  - 乱れがかなり大きい厳しい状態

設定によって変わるもの:

- 設備停止の発生確率
- 停止時間の長さ
- センサーイベントの欠損率
- センサー値のノイズ量
- 送信遅延の大きさ

つまり、このシミュレーターは
「理想的に動く工場」から
「停止や欠損やノイズがある現実的な工場」までを切り替えて試せます。

これにより、
正常時の処理だけでなく、
異常や乱れが混ざったときに summary や disturbance 判定がどう変わるかも学べます。

## まず動作確認したい人向けの重要コマンド

### コンテナ一覧

```bash
docker ps
```

### Kafka topic 一覧

```bash
python3 scripts/lab_cli.py watch-topic all
```

### Kafka topic の中身を見る

```bash
python3 scripts/lab_cli.py watch-topic process-events --from-beginning --max-messages 10
python3 scripts/lab_cli.py watch-topic sensor-a --from-beginning --max-messages 10
python3 scripts/lab_cli.py watch-topic sensor-b --from-beginning --max-messages 10
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

### summary を Iceberg に保存する

```bash
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
```

### Iceberg に保存した summary を読む

```bash
python3 scripts/lab_cli.py read-iceberg
```

### Iceberg の保存状態をまとめて見る

```bash
python3 scripts/lab_cli.py inspect-iceberg
```

このコマンドは次をまとめて表示します。

- `flink-test/iceberg-rest-data/catalog.db` の table 登録情報
- 最新 metadata JSON から見える snapshot / record 数 / data file 数
- MinIO 上の `warehouse/lab/product_summary_iceberg` 配下の object 一覧

<a id="iceberg-read-write"></a>
## Iceberg に summary を保存して読む

このリポジトリでは、`flink-test/sql/job_summary_iceberg.sql` と `scripts/lab_cli.py read-iceberg` を使うと、
summary の保存と読み取りをそのまま試せます。

### 1. producer を動かす

```bash
cd kafka-flink-factory-lab
source .venv/bin/activate
python3 python-producer/send_factory_data.py
```

Windows PowerShell:

```powershell
cd kafka-flink-factory-lab
.\.venv\Scripts\Activate.ps1
py -3 python-producer\send_factory_data.py
```

### 2. summary を Iceberg に保存する

別ターミナルで実行します。

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
```

Windows では、次のラッパーでも実行できます。

```powershell
cd kafka-flink-factory-lab
.\flink-test\apply_job.cmd .\flink-test\sql\job_summary_iceberg.sql
```

この SQL は次を行います。

- Kafka から `sensor-a` `sensor-b` `process-events` を読む
- 工程区間を作る
- 製品単位の `product_summary` を作る
- `lakehouse.lab.product_summary_iceberg` に書き込む

### 3. 少し待ってから読み取る

`product_summary` は `process_1` と `process_2` の両方が終わってから 1 行になります。
producer を流した直後は空でも正常です。

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py read-iceberg
```

Windows では、次のラッパーでも読み取れます。

```powershell
cd kafka-flink-factory-lab
.\flink-test\read_iceberg.cmd
```

このスクリプトは次を自動で行います。

- Iceberg REST Catalog を登録する
- `lab` database を使う
- `SHOW TABLES`
- `SELECT * FROM product_summary_iceberg`

### 4. MinIO 側で実ファイルを確認する

```bash
docker exec mc /usr/bin/mc find local/warehouse/lab/product_summary_iceberg
```

`metadata` だけでなく `data` 配下のファイルも見えれば、
Iceberg への書き込みまで完了しています。

## 既存 SQL ジョブの意味

### `flink-test/sql/job.sql`

- `process-events` から工程区間を作る
- `sensor-a` を `process_1` 区間に結合する
- `sensor-b` を `process_2` 区間に結合する
- 明細イベントを `print` sink に出す
- まずはイベントの紐付けが正しいか確認するための SQL

### `flink-test/sql/job_summary.sql`

- 工程区間を基に製品シリアル単位の summary を作る
- センサーイベントが 0 件でも製品行自体は残す
- `cnt_sensor_a` / `cnt_sensor_b` で欠損状況を判断できる
- disturbance は停止理由とセンサーステータスから算出する
- 現在は `print` sink に出す

### `flink-test/sql/job_summary_iceberg.sql`

- `flink-test/sql/job_summary.sql` と同じ summary を作る
- Iceberg REST Catalog と MinIO を使って `product_summary_iceberg` に保存する
- チェックポイントを有効にして、Iceberg へのコミットが進むようにしている
- `client.region` を含め、MinIO への書き込みに必要な設定を入れている

### `flink-test/sql/read_iceberg_summary.sql` と `flink-test/read_iceberg.sh`

- `product_summary_iceberg` を確認するための読み取り用ファイル
- `read_iceberg.sh` は SQL をコンテナへコピーして実行する

<a id="trino-read"></a>
## Trino で Iceberg を読む

このリポジトリには `trino-test/` が含まれており、
Iceberg REST Catalog と MinIO を読むための Trino 設定が入っています。
通常は `scripts/lab_cli.py` 経由で扱い、必要なら `trino-test/` を直接触れます。

### 1. 先に Flink 側で table を作る

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py start
python3 scripts/lab_cli.py trino-start
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
```

producer も流して実データが必要なら、別ターミナルで次を実行します。

```bash
cd kafka-flink-factory-lab
source .venv/bin/activate
python3 python-producer/send_factory_data.py
```

### 2. Trino を起動する

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py trino-start
```

内部では `trino-test/docker-compose.yml` を使って Trino を `stream-shared` network に参加させ、
`trino-test/etc/catalog/iceberg.properties` で Iceberg REST Catalog と MinIO を参照します。

Compose を直接触りたい場合の等価コマンドは次です。

```bash
cd kafka-flink-factory-lab/trino-test
docker compose up -d
```

### 3. Trino CLI で確認する

```bash
python3 scripts/lab_cli.py trino-shell
```

まずは metadata 系から確認します。

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.lab;
SELECT * FROM iceberg.lab."product_summary_iceberg$history";
SELECT * FROM iceberg.lab."product_summary_iceberg$snapshots";
```

次に本表を読みます。

```sql
SELECT count(*) FROM iceberg.lab.product_summary_iceberg;
SELECT * FROM iceberg.lab.product_summary_iceberg LIMIT 20;
```

非対話で十分なら、次のショートカットでも確認できます。

```bash
python3 scripts/lab_cli.py trino-status
python3 scripts/lab_cli.py trino-history
python3 scripts/lab_cli.py trino-snapshots
python3 scripts/lab_cli.py trino-read
python3 scripts/lab_cli.py trino-query "SELECT count(*) FROM iceberg.lab.product_summary_iceberg"
```

補足:

- `$history` や `$snapshots` が読めて本表が失敗する場合は、Trino から MinIO への S3 設定不足を疑う
- 本表が `0 rows` の場合は接続エラーではなく、current snapshot に data file がない可能性が高い
- `job_summary_iceberg.sql` は table を作り直すので、再実行直後は空 table に戻ることがある

<a id="superset-add"></a>
## Superset を手動で追加する

Superset はこの repo に同梱しません。
教育目的では、公式 repo を別ディレクトリに clone し、最小限のローカル差分だけ手で足す方が構成を理解しやすいです。

### 1. 先にこの repo 側を起動する

```bash
cd kafka-flink-factory-lab
python3 scripts/lab_cli.py start
python3 scripts/lab_cli.py trino-start
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
python3 scripts/lab_cli.py trino-read
```

`python3 scripts/lab_cli.py trino-read` で結果が見えない状態では、Superset 側へ進んでも切り分けしづらいです。

### 2. 公式 Superset を別ディレクトリへ clone する

```bash
cd /Users/makoto/docker-lab
git clone --depth=1 https://github.com/apache/superset.git superset-test
cd superset-test
git fetch --depth=1 origin tag 6.0.0
git checkout 6.0.0
export TAG=6.0.0
```

この repo では `superset-test/` 自体は追跡しません。
親 repo には local overlay の雛形だけ置いてあります。

### 3. Trino ドライバを手で追加する

`superset-test/docker/requirements-local.txt` を自分で作り、次だけ入れます。

```text
trino
```

### 4. shared network 参加用 overlay を置く

この repo の [superset-local/docker-compose.lab.yml](superset-local/docker-compose.lab.yml) を
`superset-test/docker-compose.lab.yml` として置きます。

この overlay は `superset` `superset-worker` `superset-worker-beat` `superset-init` を
`stream-shared` network に参加させ、Trino コンテナ名 `trino` を名前解決できる状態にします。

### 5. Superset を起動する

```bash
cd /Users/makoto/docker-lab/superset-test
docker compose -f docker-compose-image-tag.yml -f docker-compose.lab.yml up -d
```

初期化状況は次で確認します。

```bash
docker compose -f docker-compose-image-tag.yml -f docker-compose.lab.yml ps
docker logs superset_init
```

通常は `http://localhost:8088` にアクセスできます。

### 6. Superset UI で Trino を追加する

`Settings -> Data: Database Connections -> + DATABASE` から追加します。

接続 URI の最小例:

```text
trino://trino@trino:8080/iceberg
```

その後、SQL Lab で次を確認します。

```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.lab;
SELECT * FROM iceberg.lab.product_summary_iceberg LIMIT 20;
SELECT * FROM iceberg.lab."product_summary_iceberg$history";
```

### 7. Dataset と chart を作る

まずは `lab.product_summary_iceberg` を Dataset として追加し、
`has_disturbance` 件数や `equipment_id` 別件数を chart にするのが理解しやすいです。

<a id="typical-workflow"></a>
## 典型的な作業フロー

### 1. 基盤だけ起動して疎通を見る

```bash
python3 scripts/lab_cli.py start
python3 scripts/lab_cli.py trino-start
docker ps
docker exec mc /usr/bin/mc ls local
```

### 2. producer を流して Kafka を見る

```bash
python3 python-producer/send_factory_data.py
python3 scripts/lab_cli.py watch-topic process-events --from-beginning --max-messages 10
```

### 3. Flink SQL を実行してログを見る

```bash
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job.sql
docker logs -f flink-taskmanager
```

### 4. summary を Iceberg に保存して読む

```bash
python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql
python3 scripts/lab_cli.py read-iceberg
python3 scripts/lab_cli.py trino-snapshots
python3 scripts/lab_cli.py trino-read
```

<a id="troubleshooting"></a>
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
cd kafka-flink-factory-lab/flink-test
docker compose build --no-cache
docker compose up -d
```

### Iceberg 作成時に `UnknownHostException: warehouse.minio` が出る

- REST catalog 側の path-style 設定が効いていない
- `flink-test/docker-compose.yml` の `iceberg-rest` に
  `CATALOG_S3_PATH__STYLE__ACCESS=true` があるか確認する
- 変更後は `iceberg-rest` を再作成する

```bash
cd kafka-flink-factory-lab/flink-test
docker compose up -d --force-recreate iceberg-rest
```

### `warehouse` bucket が見つからない

- `mc` コンテナが起動しているか確認する
- `docker exec mc /usr/bin/mc ls local` を実行する
- 通常は `start_all.sh` が bucket 存在確認まで行う

### Trino で `$history` は読めるが本表が読めない

- Trino から Iceberg REST Catalog には届いているが、MinIO への S3 設定が不足している可能性が高い
- `trino-test/etc/catalog/iceberg.properties` に次があるか確認する
  - `fs.native-s3.enabled=true`
  - `s3.endpoint=http://minio:9000`
  - `s3.path-style-access=true`
  - `s3.aws-access-key=minioadmin`
  - `s3.aws-secret-key=minioadmin`
- 変更後は Trino を再作成する

```bash
python3 scripts/lab_cli.py trino-stop
python3 scripts/lab_cli.py trino-start
```

### Trino で本表が `0 rows` になる

- 接続失敗ではなく、current snapshot に data file がない状態のことがある
- `python3 scripts/lab_cli.py inspect-iceberg` で snapshot と object 一覧を確認する
- `product_summary_iceberg` は job 実行時に作り直されるため、再実行直後は空 table に戻ることがある
- producer を流したまましばらく待ってから再確認する

```bash
python3 scripts/lab_cli.py inspect-iceberg
python3 scripts/lab_cli.py trino-query "SELECT count(*) FROM iceberg.lab.product_summary_iceberg"
```

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
- このリポジトリには Trino の学習用設定と Superset 手動追加用の雛形が含まれる
- Superset までつなぐときは、Trino を介して Superset から Iceberg table を読む構成が自然
