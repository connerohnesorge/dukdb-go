# Arrow Api Specification

## Requirements

### Requirement: Arrow View Registration

The package SHALL allow external Arrow data to be registered as queryable views.

#### Scenario: Register Arrow view with primitive types
- GIVEN Arrow RecordReader with schema (id INT32, name STRING)
- WHEN RegisterView(reader, "external_data") is called
- THEN view "external_data" is registered in catalog
- AND release function is returned
- AND no error is returned

#### Scenario: Query registered Arrow view
- GIVEN registered Arrow view "external_data" with 3 rows
- WHEN SELECT * FROM external_data is executed
- THEN all 3 rows are returned
- AND data matches Arrow RecordReader contents

#### Scenario: Release unregisters view
- GIVEN registered Arrow view "external_data"
- WHEN release function is called
- THEN view is unregistered from catalog
- AND Arrow RecordReader is released
- AND subsequent SELECT fails with "table not found" error

#### Scenario: Duplicate view name error
- GIVEN registered Arrow view "data"
- WHEN RegisterView(another_reader, "data") is called
- THEN error is returned
- AND error message is "view data already exists"

#### Scenario: View name conflicts with table
- GIVEN existing table "t"
- WHEN RegisterView(reader, "t") is called
- THEN error is returned
- AND existing table is not affected

### Requirement: Arrow Type Conversion

The package SHALL convert all Arrow types to corresponding DuckDB types.

#### Scenario: Primitive Arrow types
- GIVEN Arrow schema with INT32, INT64, FLOAT32, DOUBLE, BOOLEAN
- WHEN RegisterView is called
- THEN DuckDB types INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN are created
- AND queries return correct typed values

#### Scenario: String Arrow types
- GIVEN Arrow schema with STRING and BINARY
- WHEN RegisterView is called
- THEN DuckDB types VARCHAR and BLOB are created

#### Scenario: Temporal Arrow types
- GIVEN Arrow schema with DATE32, TIME64[us], TIMESTAMP[us]
- WHEN RegisterView is called
- THEN DuckDB types DATE, TIME, TIMESTAMP are created
- AND temporal values are correctly converted

#### Scenario: LIST Arrow type
- GIVEN Arrow schema with LIST<INT32>
- WHEN RegisterView is called
- THEN DuckDB type LIST(INTEGER) is created
- AND nested values are accessible via SQL

#### Scenario: STRUCT Arrow type
- GIVEN Arrow schema with STRUCT<id: INT32, name: STRING>
- WHEN RegisterView is called
- THEN DuckDB type STRUCT(id INTEGER, name VARCHAR) is created
- AND struct field access works in SQL

#### Scenario: Unsupported Arrow type error
- GIVEN Arrow schema with extension type (not supported)
- WHEN RegisterView is called
- THEN error is returned
- AND error message indicates unsupported type

### Requirement: Virtual Table Lazy Evaluation

The package SHALL scan Arrow data lazily without upfront materialization.

#### Scenario: Large Arrow dataset streaming
- GIVEN Arrow RecordReader with 100,000 rows in 50 batches
- WHEN RegisterView and SELECT LIMIT 10 are called
- THEN only first batch is read from Arrow RecordReader
- AND query completes without reading all batches

#### Scenario: Arrow view exhaustion
- GIVEN registered Arrow view scanned to completion
- WHEN second SELECT query is executed on same view
- THEN error or empty result is returned
- AND behavior matches Arrow RecordReader one-shot semantics

### Requirement: NULL Handling

The package SHALL correctly handle Arrow null bitmaps.

#### Scenario: NULL values in Arrow data
- GIVEN Arrow RecordReader with NULL values in validity bitmap
- WHEN RegisterView and SELECT are called
- THEN NULL values are correctly returned as NULL in DuckDB
- AND IS NULL predicates work correctly

#### Scenario: All-null Arrow column
- GIVEN Arrow column where all rows are NULL
- WHEN querying with WHERE column IS NOT NULL
- THEN zero rows are returned

### Requirement: SQL Query Support

The package SHALL allow standard SQL queries against Arrow views.

#### Scenario: WHERE clause on Arrow view
- GIVEN Arrow view with 100 rows
- WHEN SELECT * FROM view WHERE id > 50 is executed
- THEN only matching rows are returned
- AND Arrow batches are filtered during scan

#### Scenario: JOIN with Arrow view
- GIVEN Arrow view "external" and regular table "local"
- WHEN SELECT * FROM local JOIN external ON local.id = external.id is executed
- THEN join is computed correctly
- AND Arrow data participates in join

#### Scenario: Aggregation on Arrow view
- GIVEN Arrow view with numeric column
- WHEN SELECT COUNT(*), SUM(value) FROM view is executed
- THEN aggregation is computed correctly
- AND all Arrow batches are consumed

### Requirement: Concurrent View Management

The package SHALL handle concurrent view operations safely.

#### Scenario: Concurrent view registration (deterministic)
- GIVEN two goroutines attempting RegisterView with different names
- WHEN both execute using quartz.Mock with traps
- THEN trap := mClock.Trap().Now("Arrow", "register") coordinates
- AND both views are registered successfully
- AND no race conditions occur

#### Scenario: View query during registration
- GIVEN view registration in progress
- WHEN another connection attempts to query same view
- THEN query either succeeds (if registration complete) or fails (if not found)
- AND no undefined behavior occurs

### Requirement: Deterministic Testing Compliance

The package SHALL support deterministic testing per deterministic-testing spec.

#### Scenario: Zero time.Sleep in tests
- GIVEN all Arrow view tests
- WHEN searching for time.Sleep
- THEN zero occurrences found
- AND all timing uses quartz.Clock

#### Scenario: All timing operations tagged
- GIVEN Arrow view registration and scan operations
- WHEN using quartz clock
- THEN mClock.Now("Arrow", "register", "start") tags registration
- AND mClock.Now("Arrow", "view", "scan") tags view scans
- AND tags enable precise trap filtering

