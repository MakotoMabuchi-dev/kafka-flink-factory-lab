SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- ============================================
-- Role:
--   Iceberg に保存された product_summary_iceberg を読むための
--   手動確認用 SQL。
--
-- Do:
--   - Iceberg REST catalog を登録する
--   - lab database を使う
--   - table 一覧を確認する
--   - product_summary_iceberg を SELECT する
--
-- Usage:
--   python3 scripts/lab_cli.py read-iceberg
-- ============================================

CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1'
);

USE CATALOG lakehouse;
USE lab;

SHOW TABLES;

SELECT *
FROM product_summary_iceberg;
