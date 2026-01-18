-- Flink SQL script to generate Iceberg tables for compatibility testing

-- Set up catalog
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 'file:///opt/flink-warehouse'
);

USE CATALOG iceberg_catalog;

-- Create database
CREATE DATABASE IF NOT EXISTS flink_db;
USE flink_db;

-- 1. Simple table
CREATE TABLE flink_simple (
  id BIGINT,
  name STRING,
  value DOUBLE,
  created_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'format-version' = '2'
);

INSERT INTO flink_simple VALUES
  (1, 'Alice', 100.5, TIMESTAMP '2024-01-01 10:00:00'),
  (2, 'Bob', 200.75, TIMESTAMP '2024-01-02 11:00:00'),
  (3, 'Charlie', 300.25, TIMESTAMP '2024-01-03 12:00:00'),
  (4, NULL, 400.0, TIMESTAMP '2024-01-04 13:00:00'),
  (5, 'Eve', NULL, TIMESTAMP '2024-01-05 14:00:00');

-- 2. Partitioned table
CREATE TABLE flink_partitioned (
  id BIGINT,
  event_date DATE,
  category STRING,
  amount DOUBLE,
  PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (event_date, category) WITH (
  'format-version' = '2'
);

INSERT INTO flink_partitioned VALUES
  (1, DATE '2024-01-01', 'A', 10.5),
  (2, DATE '2024-01-01', 'B', 20.5),
  (3, DATE '2024-01-02', 'A', 30.5),
  (4, DATE '2024-01-02', 'C', 40.5),
  (5, DATE '2024-01-03', 'B', 50.5);

-- 3. Table with complex types
CREATE TABLE flink_complex (
  id BIGINT,
  metadata MAP<STRING, STRING>,
  tags ARRAY<STRING>,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'format-version' = '2'
);

INSERT INTO flink_complex VALUES
  (1, MAP['key1', 'value1', 'key2', 'value2'], ARRAY['tag1', 'tag2']),
  (2, MAP['foo', 'bar'], ARRAY['tag3']),
  (3, MAP[], ARRAY[]);

-- 4. Table for time travel (multiple snapshots)
CREATE TABLE flink_time_travel (
  id BIGINT,
  value INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'format-version' = '2'
);

-- Snapshot 1
INSERT INTO flink_time_travel VALUES (1, 100), (2, 200);

-- Snapshot 2
INSERT INTO flink_time_travel VALUES (3, 300), (4, 400);

-- Snapshot 3 (update creates new snapshot in Iceberg)
INSERT OVERWRITE flink_time_travel
SELECT id, value * 2 as value FROM flink_time_travel WHERE id = 1
UNION ALL
SELECT id, value FROM flink_time_travel WHERE id != 1;

-- 5. Table with deletes (Iceberg v2 feature)
CREATE TABLE flink_deletes (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read'
);

INSERT INTO flink_deletes SELECT id, CONCAT('row_', CAST(id AS STRING))
FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(id);

-- This should create delete files in Iceberg v2
DELETE FROM flink_deletes WHERE id IN (3, 6, 9);
