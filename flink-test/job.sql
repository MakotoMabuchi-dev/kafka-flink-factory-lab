-- ============================================
-- Role:
--   3つの Kafka topic からデータを読み取り、
--   process-events の start/end 区間に対して
--   sensor-a / sensor-b を時刻で紐付ける。
--
-- Do:
--   - Kafka source table を作成する
--   - process-events から工程区間を作成する
--   - sensor-a / sensor-b を工程区間へ join する
--   - 製品シリアル単位で解釈可能な明細イベントとして print sink へ出力する
--
-- Don't:
--   - Iceberg への保存はまだ行わない
--   - Superset 向け summary テーブルはまだ作らない
--
-- Design Policy:
--   - まずは「シリアルとセンサー値が紐付く」ことを確認する
--   - event_time は Python 側の ISO 文字列から明示的に作る
--   - process_1 は sensor-a、process_2 は sensor-b に対応させる
-- ============================================

SET 'sql-client.execution.result-mode' = 'TABLEAU';

DROP TABLE IF EXISTS sensor_a_raw;
DROP TABLE IF EXISTS sensor_b_raw;
DROP TABLE IF EXISTS process_events_raw;
DROP TABLE IF EXISTS product_sensor_events_print;

DROP VIEW IF EXISTS process_intervals;
DROP VIEW IF EXISTS process1_sensor_events;
DROP VIEW IF EXISTS process2_sensor_events;
DROP VIEW IF EXISTS product_sensor_events;

-- ============================================
-- 1. Kafka source tables
-- ============================================

-- sensor-a sample:
-- {
--   "sensor_id": "A",
--   "event_time_str": "2026-03-22T10:00:01.123456",
--   "value_temp": 31.2,
--   "value_vibration": 0.42,
--   "unit": "machine-01",
--   "status": "RUNNING"
-- }

CREATE TABLE sensor_a_raw (
    sensor_id STRING,
    event_time_str STRING,
    value_temp DOUBLE,
    value_vibration DOUBLE,
    unit STRING,
    status STRING,

    event_time AS CAST(REPLACE(SUBSTRING(event_time_str, 1, 19), 'T', ' ') AS TIMESTAMP(3)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-a',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'sensor-a-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- sensor-b sample:
-- {
--   "sensor_code": "B-01",
--   "ts_str": "2026-03-22T10:00:02.456789",
--   "pressure": 102.4,
--   "current": 8.7,
--   "line": "machine-01",
--   "status": "RUNNING"
-- }

CREATE TABLE sensor_b_raw (
    sensor_code STRING,
    ts_str STRING,
    pressure DOUBLE,
    `current` DOUBLE,
    line STRING,
    status STRING,

    event_time AS CAST(REPLACE(SUBSTRING(ts_str, 1, 19), 'T', ' ') AS TIMESTAMP(3)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-b',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'sensor-b-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- process-events sample:
-- {
--   "serial_no": "SN00000123",
--   "process": "process_1",
--   "event_type": "start",
--   "event_time_str": "2026-03-22T10:00:00.000000",
--   "equipment_id": "machine-01",
--   "reason": null
-- }

CREATE TABLE process_events_raw (
    serial_no STRING,
    process STRING,
    event_type STRING,
    event_time_str STRING,
    equipment_id STRING,
    reason STRING,

    event_time AS CAST(REPLACE(SUBSTRING(event_time_str, 1, 19), 'T', ' ') AS TIMESTAMP(3)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'process-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'process-events-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- ============================================
-- 2. 工程 start/end から区間を作る
-- ============================================

CREATE VIEW process_intervals AS
SELECT
    serial_no,
    process,
    equipment_id,
    MIN(CASE WHEN event_type = 'start' THEN event_time END) AS process_start,
    MAX(CASE WHEN event_type = 'end' THEN event_time END) AS process_end
FROM process_events_raw
GROUP BY
    serial_no,
    process,
    equipment_id;

-- ============================================
-- 3. process_1 区間に sensor-a を紐付ける
-- ============================================

CREATE VIEW process1_sensor_events AS
SELECT
    p.serial_no,
    p.process,
    'A' AS sensor_type,
    p.equipment_id,
    p.process_start,
    p.process_end,
    a.event_time AS sensor_time,
    a.value_temp AS temp,
    a.value_vibration AS vibration,
    CAST(NULL AS DOUBLE) AS pressure,
    CAST(NULL AS DOUBLE) AS current_value,
    a.status AS sensor_status
FROM process_intervals p
JOIN sensor_a_raw a
ON p.process = 'process_1'
AND a.unit = p.equipment_id
AND a.event_time BETWEEN p.process_start AND p.process_end;

-- ============================================
-- 4. process_2 区間に sensor-b を紐付ける
-- ============================================

CREATE VIEW process2_sensor_events AS
SELECT
    p.serial_no,
    p.process,
    'B' AS sensor_type,
    p.equipment_id,
    p.process_start,
    p.process_end,
    b.event_time AS sensor_time,
    CAST(NULL AS DOUBLE) AS temp,
    CAST(NULL AS DOUBLE) AS vibration,
    b.pressure AS pressure,
    b.`current` AS current_value,
    b.status AS sensor_status
FROM process_intervals p
JOIN sensor_b_raw b
ON p.process = 'process_2'
AND b.line = p.equipment_id
AND b.event_time BETWEEN p.process_start AND p.process_end;

-- ============================================
-- 5. A/B を統合
-- ============================================

CREATE VIEW product_sensor_events AS
SELECT * FROM process1_sensor_events
UNION ALL
SELECT * FROM process2_sensor_events;

-- ============================================
-- 6. 確認用 print sink
-- ============================================

CREATE TABLE product_sensor_events_print (
    serial_no STRING,
    process STRING,
    sensor_type STRING,
    equipment_id STRING,
    process_start TIMESTAMP(3),
    process_end TIMESTAMP(3),
    sensor_time TIMESTAMP(3),
    temp DOUBLE,
    vibration DOUBLE,
    pressure DOUBLE,
    current_value DOUBLE,
    sensor_status STRING
) WITH (
    'connector' = 'print'
);

-- ============================================
-- 7. 最終出力
-- ============================================

INSERT INTO product_sensor_events_print
SELECT
    serial_no,
    process,
    sensor_type,
    equipment_id,
    process_start,
    process_end,
    sensor_time,
    temp,
    vibration,
    pressure,
    current_value,
    sensor_status
FROM product_sensor_events;