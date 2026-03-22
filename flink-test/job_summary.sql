SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- ============================================
-- Role:
--   Kafka から受け取った process-events / sensor-a / sensor-b を使い、
--   製品シリアル単位の summary を作成する。
--
-- Do:
--   - process-events から工程区間を作る
--   - 工程1に sensor-a、工程2に sensor-b を紐付ける
--   - 1製品1行の product_summary を作る
--   - まずは print sink に出力する
--
-- Don't:
--   - Iceberg への保存はまだ行わない
--   - Superset 用の可視化設定までは行わない
--
-- Design Policy:
--   - product_sensor_events の次段として summary を作る
--   - Superset で扱いやすい横持ちテーブルにする
-- ============================================

DROP TABLE IF EXISTS sensor_a_raw;
DROP TABLE IF EXISTS sensor_b_raw;
DROP TABLE IF EXISTS process_events_raw;
DROP TABLE IF EXISTS product_summary_print;

DROP VIEW IF EXISTS process_intervals;
DROP VIEW IF EXISTS process1_intervals;
DROP VIEW IF EXISTS process2_intervals;
DROP VIEW IF EXISTS process1_sensor_events;
DROP VIEW IF EXISTS process2_sensor_events;
DROP VIEW IF EXISTS process1_summary;
DROP VIEW IF EXISTS process2_summary;
DROP VIEW IF EXISTS product_summary;

-- ============================================
-- Kafka source tables
-- ============================================

CREATE TABLE sensor_a_raw (
    sensor_id STRING,
    event_time_str STRING,
    value_temp DOUBLE,
    value_vibration DOUBLE,
    unit STRING,
    status STRING,

    event_time AS CAST(REPLACE(event_time_str, 'T', ' ') AS TIMESTAMP(6)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-a',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'sensor-a-summary-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE sensor_b_raw (
    sensor_code STRING,
    ts_str STRING,
    pressure DOUBLE,
    `current` DOUBLE,
    line STRING,
    status STRING,

    event_time AS CAST(REPLACE(ts_str, 'T', ' ') AS TIMESTAMP(6)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sensor-b',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'sensor-b-summary-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE process_events_raw (
    serial_no STRING,
    process STRING,
    event_type STRING,
    event_time_str STRING,
    equipment_id STRING,
    reason STRING,

    event_time AS CAST(REPLACE(event_time_str, 'T', ' ') AS TIMESTAMP(6)),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'process-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'process-events-summary-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- ============================================
-- 工程区間
-- ============================================

CREATE VIEW process_intervals AS
SELECT
    serial_no,
    process,
    equipment_id,
    MIN(CASE WHEN event_type = 'start' THEN event_time END) AS process_start,
    MAX(CASE WHEN event_type = 'end' THEN event_time END) AS process_end,
    MAX(CASE WHEN reason IS NOT NULL THEN 1 ELSE 0 END) AS has_reason_flag
FROM process_events_raw
GROUP BY
    serial_no,
    process,
    equipment_id;

CREATE VIEW process1_intervals AS
SELECT
    serial_no,
    equipment_id,
    process_start,
    process_end,
    has_reason_flag
FROM process_intervals
WHERE process = 'process_1'
AND process_start IS NOT NULL
AND process_end IS NOT NULL;

CREATE VIEW process2_intervals AS
SELECT
    serial_no,
    equipment_id,
    process_start,
    process_end,
    has_reason_flag
FROM process_intervals
WHERE process = 'process_2'
AND process_start IS NOT NULL
AND process_end IS NOT NULL;

-- ============================================
-- 工程1 × sensor-a
-- ============================================

CREATE VIEW process1_sensor_events AS
SELECT
    p.serial_no,
    p.equipment_id,
    p.process_start,
    p.process_end,
    a.event_time AS sensor_time,
    a.value_temp,
    a.value_vibration,
    a.status AS sensor_status,
    p.has_reason_flag
FROM process1_intervals p
LEFT JOIN sensor_a_raw a
ON a.unit = p.equipment_id
AND a.event_time BETWEEN p.process_start AND p.process_end;

-- ============================================
-- 工程2 × sensor-b
-- ============================================

CREATE VIEW process2_sensor_events AS
SELECT
    p.serial_no,
    p.equipment_id,
    p.process_start,
    p.process_end,
    b.event_time AS sensor_time,
    b.pressure,
    b.`current` AS current_value,
    b.status AS sensor_status,
    p.has_reason_flag
FROM process2_intervals p
LEFT JOIN sensor_b_raw b
ON b.line = p.equipment_id
AND b.event_time BETWEEN p.process_start AND p.process_end;

-- ============================================
-- 工程1 summary
-- ============================================

CREATE VIEW process1_summary AS
SELECT
    serial_no,
    equipment_id,
    MIN(process_start) AS process_1_start,
    MAX(process_end) AS process_1_end,
    TIMESTAMPDIFF(SECOND, MIN(process_start), MAX(process_end)) AS process_1_duration_sec,
    AVG(value_temp) AS avg_temp_a,
    MAX(value_temp) AS max_temp_a,
    AVG(value_vibration) AS avg_vibration_a,
    COUNT(sensor_time) AS cnt_sensor_a,
    MAX(CASE WHEN sensor_status IS NOT NULL AND sensor_status <> 'RUNNING' THEN 1 ELSE 0 END) AS process_1_has_non_running_status,
    MAX(has_reason_flag) AS process_1_has_reason_flag
FROM process1_sensor_events
GROUP BY
    serial_no,
    equipment_id;

-- ============================================
-- 工程2 summary
-- ============================================

CREATE VIEW process2_summary AS
SELECT
    serial_no,
    equipment_id,
    MIN(process_start) AS process_2_start,
    MAX(process_end) AS process_2_end,
    TIMESTAMPDIFF(SECOND, MIN(process_start), MAX(process_end)) AS process_2_duration_sec,
    AVG(pressure) AS avg_pressure_b,
    MAX(pressure) AS max_pressure_b,
    AVG(current_value) AS avg_current_b,
    COUNT(sensor_time) AS cnt_sensor_b,
    MAX(CASE WHEN sensor_status IS NOT NULL AND sensor_status <> 'RUNNING' THEN 1 ELSE 0 END) AS process_2_has_non_running_status,
    MAX(has_reason_flag) AS process_2_has_reason_flag
FROM process2_sensor_events
GROUP BY
    serial_no,
    equipment_id;

-- ============================================
-- 最終 summary
-- ============================================

CREATE VIEW product_summary AS
SELECT
    p1.serial_no,
    p1.equipment_id,

    p1.process_1_start,
    p1.process_1_end,
    p1.process_1_duration_sec,
    p1.avg_temp_a,
    p1.max_temp_a,
    p1.avg_vibration_a,
    p1.cnt_sensor_a,

    p2.process_2_start,
    p2.process_2_end,
    p2.process_2_duration_sec,
    p2.avg_pressure_b,
    p2.max_pressure_b,
    p2.avg_current_b,
    p2.cnt_sensor_b,

    CASE
        WHEN COALESCE(p1.process_1_has_non_running_status, 0) = 1
          OR COALESCE(p2.process_2_has_non_running_status, 0) = 1
          OR COALESCE(p1.process_1_has_reason_flag, 0) = 1
          OR COALESCE(p2.process_2_has_reason_flag, 0) = 1
        THEN TRUE
        ELSE FALSE
    END AS has_disturbance
FROM process1_summary p1
LEFT JOIN process2_summary p2
ON p1.serial_no = p2.serial_no
AND p1.equipment_id = p2.equipment_id;

-- ============================================
-- 確認用 print sink
-- ============================================

CREATE TABLE product_summary_print (
    serial_no STRING,
    equipment_id STRING,

    process_1_start TIMESTAMP(6),
    process_1_end TIMESTAMP(6),
    process_1_duration_sec BIGINT,
    avg_temp_a DOUBLE,
    max_temp_a DOUBLE,
    avg_vibration_a DOUBLE,
    cnt_sensor_a BIGINT,

    process_2_start TIMESTAMP(6),
    process_2_end TIMESTAMP(6),
    process_2_duration_sec BIGINT,
    avg_pressure_b DOUBLE,
    max_pressure_b DOUBLE,
    avg_current_b DOUBLE,
    cnt_sensor_b BIGINT,

    has_disturbance BOOLEAN
) WITH (
    'connector' = 'print'
);

INSERT INTO product_summary_print
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
