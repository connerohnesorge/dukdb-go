# Table Formats Specification

## Requirements

### Requirement: Iceberg Table Discovery

The system SHALL discover and list accessible Iceberg tables in a given location.

#### Scenario: List Iceberg tables in local directory

- GIVEN a local directory with Iceberg tables (metadata.json files)
- WHEN executing `SELECT * FROM duckdb_iceberg_tables('/path/to/iceberg/')`
- THEN all Iceberg tables SHALL be discovered
- AND returned with columns: table_name, table_location, current_snapshot_id, last_updated_ms

#### Scenario: List Iceberg tables in S3

- GIVEN an S3 bucket with Iceberg tables
- WHEN executing `SELECT * FROM duckdb_iceberg_tables('s3://bucket/iceberg/')`
- THEN Iceberg tables SHALL be discovered via S3 API
- AND table metadata SHALL be returned

#### Scenario: No Iceberg tables found

- GIVEN a directory with no Iceberg tables
- WHEN executing `duckdb_iceberg_tables()`
- THEN an empty result set SHALL be returned
- AND no error SHALL be raised

### Requirement: Iceberg Table Reading

The system SHALL read data from Iceberg tables using Parquet infrastructure.

#### Scenario: Read current snapshot

- GIVEN an Iceberg table at path '/iceberg/table'
- WHEN executing `SELECT * FROM iceberg_scan('/iceberg/table')`
- THEN the current snapshot SHALL be read
- AND all data files SHALL be parsed via Parquet reader
- AND all rows SHALL be returned

#### Scenario: Read with column projection

- GIVEN an Iceberg table with 50 columns
- WHEN executing `SELECT id, name FROM iceberg_scan('/iceberg/table')`
- THEN only id and name columns SHALL be read
- AND other columns SHALL be skipped (I/O optimization)

#### Scenario: Read with partition pruning

- GIVEN an Iceberg table partitioned by date
- WHEN executing `SELECT * FROM iceberg_scan('/iceberg/table') WHERE date = '2024-01-15'`
- THEN manifests with non-matching partition values SHALL be skipped
- AND only relevant data files SHALL be read

### Requirement: Iceberg Time Travel

The system SHALL support querying historical snapshots of Iceberg tables.

#### Scenario: Time travel by timestamp

- GIVEN an Iceberg table with snapshots at timestamps T1, T2, T3
- WHEN executing `SELECT * FROM iceberg_scan('/iceberg/table') AS OF TIMESTAMP '2024-01-15 10:00:00'`
- THEN the snapshot closest to and not after the timestamp SHALL be read
- AND historical data SHALL be returned

#### Scenario: Time travel by snapshot ID

- GIVEN an Iceberg table with snapshot ID 1234567890
- WHEN executing `SELECT * FROM iceberg_scan('/iceberg/table') AS OF SNAPSHOT 1234567890`
- THEN exactly that snapshot SHALL be read
- AND data from that point in time SHALL be returned

#### Scenario: Invalid timestamp returns error

- GIVEN an Iceberg table
- WHEN executing `SELECT * FROM iceberg_scan('/iceberg/table') AS OF TIMESTAMP '2020-01-01'`
- AND no snapshot exists at or before that time
- THEN an error SHALL be returned
- AND available timestamp range SHALL be indicated

### Requirement: Iceberg Schema Evolution Handling

The system SHALL correctly handle Iceberg schema evolution.

#### Scenario: Query with added columns

- GIVEN an Iceberg table where column 'email' was added after snapshot S1
- WHEN querying snapshot S1 (before column was added)
- THEN the column 'email' SHALL be NULL for all rows
- AND the column SHALL be present in result schema

#### Scenario: Query with dropped columns

- GIVEN an Iceberg table where column 'old_col' was dropped
- WHEN querying current snapshot (after drop)
- THEN the column 'old_col' SHALL not be in result schema
- AND query SHALL succeed without error

#### Scenario: Query with renamed columns

- GIVEN an Iceberg table where 'name' was renamed to 'customer_name'
- WHEN querying older snapshot
- THEN the result SHALL use the original column name 'name'
- AND the mapping SHALL be correct

### Requirement: Iceberg Partition Specification

The system SHALL use Iceberg partition specs for efficient filtering.

#### Scenario: Identity partition pruning

- GIVEN Iceberg table partitioned by `region (identity)`
- WHEN querying with `WHERE region = 'US'`
- THEN only manifests with region='US' partition SHALL be read
- AND other manifests SHALL be skipped

#### Scenario: Bucket partition pruning

- GIVEN Iceberg table partitioned by `hash(id, 16)`
- WHEN querying with `WHERE id = 123`
- THEN appropriate bucket SHALL be computed
- AND only matching bucket manifests SHALL be read

#### Scenario: Temporal partition pruning

- GIVEN Iceberg table partitioned by `order_date (year, month, day)`
- WHEN querying with `WHERE order_date = '2024-01-15'`
- THEN year=2024, month=1, day=15 manifests SHALL be identified
- AND only matching manifests SHALL be read

### Requirement: Iceberg Type Mapping

The system SHALL correctly map Iceberg types to DuckDB types.

#### Scenario: Primitive type mapping

- GIVEN Iceberg columns with types: boolean, int, long, float, double, string, binary, date, timestamp, time, uuid
- WHEN reading the Iceberg table
- THEN types SHALL map to: BOOLEAN, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, BLOB, DATE, TIMESTAMP, TIME, UUID

#### Scenario: Nested type mapping - struct

- GIVEN Iceberg column `address STRUCT<street STRING, city STRING, zip INTEGER>`
- WHEN reading the Iceberg table
- THEN type SHALL map to: STRUCT(street VARCHAR, city VARCHAR, zip INTEGER)

#### Scenario: Nested type mapping - list

- GIVEN Iceberg column `tags ARRAY<STRING>`
- WHEN reading the Iceberg table
- THEN type SHALL map to: LIST(VARCHAR)

#### Scenario: Nested type mapping - map

- GIVEN Iceberg column `properties MAP<STRING, INTEGER>`
- WHEN reading the Iceberg table
- THEN type SHALL map to: MAP(VARCHAR, INTEGER)

### Requirement: Iceberg Cloud Storage Access

The system SHALL read Iceberg tables from cloud storage providers.

#### Scenario: Read Iceberg from S3

- GIVEN an Iceberg table at 's3://bucket/iceberg/table'
- WHEN executing `SELECT * FROM iceberg_scan('s3://bucket/iceberg/table')`
- THEN metadata and data files SHALL be fetched via S3 API
- AND table SHALL be read correctly

#### Scenario: Read Iceberg from GCS

- GIVEN an Iceberg table at 'gs://bucket/iceberg/table'
- WHEN executing `SELECT * FROM iceberg_scan('gs://bucket/iceberg/table')`
- THEN metadata and data files SHALL be fetched via GCS API
- AND table SHALL be read correctly

#### Scenario: Read Iceberg with secret

- GIVEN an Iceberg table at 's3://bucket/table' requiring credentials
- AND a secret is created: `CREATE SECRET s3_secret (TYPE S3, ...)`
- WHEN executing `SELECT * FROM iceberg_scan('s3://bucket/table')`
- THEN the secret SHALL be used for authentication
- AND table SHALL be read correctly

