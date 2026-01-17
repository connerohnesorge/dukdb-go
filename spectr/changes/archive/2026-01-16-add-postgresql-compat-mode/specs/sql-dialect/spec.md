## ADDED Requirements

### Requirement: PostgreSQL Type Compatibility

The system SHALL support PostgreSQL data types with automatic mapping to DuckDB types.

#### Scenario: Serial type mapping

- GIVEN a PostgreSQL CREATE TABLE statement with `id SERIAL`
- WHEN creating the table in DuckDB
- THEN the type SHALL map to INTEGER with auto-increment
- AND sequence SHALL be created implicitly

#### Scenario: Character type mapping

- GIVEN PostgreSQL types: VARCHAR(100), CHAR(10), TEXT
- WHEN creating columns
- THEN VARCHAR SHALL map to VARCHAR(100)
- AND CHAR SHALL map to CHAR(10)
- AND TEXT SHALL map to VARCHAR (unlimited)

#### Scenario: Numeric type mapping

- GIVEN PostgreSQL types: NUMERIC(10,2), DECIMAL, REAL, DOUBLE PRECISION
- WHEN creating columns
- THEN types SHALL map to DECIMAL(10,2), DECIMAL, FLOAT, DOUBLE respectively

#### Scenario: Temporal type mapping

- GIVEN PostgreSQL types: TIMESTAMP, TIMESTAMPTZ, DATE, TIME, TIMETZ
- WHEN creating columns
- THEN TIMESTAMP SHALL map to TIMESTAMP
- AND TIMESTAMPTZ SHALL map to TIMESTAMPTZ
- AND DATE SHALL map to DATE
- AND TIME SHALL map to TIME
- AND TIMETZ SHALL map to TIMETZ

#### Scenario: JSON type mapping

- GIVEN a PostgreSQL JSON column
- WHEN creating the column
- THEN type SHALL map to JSON
- AND JSON operations SHALL work

### Requirement: PostgreSQL Function Compatibility

The system SHALL support PostgreSQL function names as aliases for DuckDB functions.

#### Scenario: now() function

- GIVEN a query with `now()`
- WHEN executing the query
- THEN `now()` SHALL resolve to `current_timestamp`
- AND return current timestamp

#### Scenario: current_date and current_time

- GIVEN queries with `current_date` or `current_time`
- WHEN executing
- THEN functions SHALL resolve to DuckDB equivalents
- AND return correct values

#### Scenario: concat and coalesce

- GIVEN queries with PostgreSQL `concat()`, `concat_ws()`, `coalesce()`, `nullif()`
- WHEN executing
- THEN functions SHALL resolve to DuckDB equivalents
- AND return correct results

#### Scenario: generate_series to range

- GIVEN a query with `generate_series(start, stop)`
- WHEN executing
- THEN `generate_series` SHALL be aliased to `range`
- AND return the same results

### Requirement: PostgreSQL Syntax Compatibility

The system SHALL support common PostgreSQL syntax variations.

#### Scenario: DISTINCT ON

- GIVEN query `SELECT DISTINCT ON (col1) col1, col2 FROM table`
- WHEN executing
- THEN DISTINCT ON SHALL be supported
- AND return first row per col1 group

#### Scenario: LIMIT ALL

- GIVEN query `SELECT * FROM table LIMIT ALL`
- WHEN executing
- THEN LIMIT ALL SHALL be equivalent to no LIMIT
- AND return all rows

#### Scenario: ILIKE operator

- GIVEN query `SELECT * FROM table WHERE name ILIKE '%test%'`
- WHEN executing
- THEN ILIKE SHALL be case-insensitive LIKE
- AND return matching rows

#### Scenario: Type cast with ::

- GIVEN query `SELECT '123'::integer`
- WHEN executing
- THEN :: cast SHALL be equivalent to CAST()
- AND return integer 123

#### Scenario: COMMENT syntax

- GIVEN `COMMENT ON TABLE table_name IS 'description'`
- WHEN executing
- THEN comment SHALL be stored in system metadata
- AND retrievable via information_schema

### Requirement: PostgreSQL Wire Protocol

The system SHALL support PostgreSQL wire protocol for client connections.

#### Scenario: PostgreSQL client connection

- GIVEN a PostgreSQL client (psql, pgx, libpq)
- WHEN connecting to DuckDB PostgreSQL compatibility port
- THEN connection SHALL be established using PostgreSQL wire protocol
- AND authentication SHALL complete

#### Scenario: Simple query execution

- GIVEN a simple query message
- WHEN executing
- THEN query SHALL be processed by DuckDB
- AND results SHALL be returned in PostgreSQL format

#### Scenario: Extended query with prepared statements

- GIVEN a prepared statement query
- WHEN executing
- THEN statement SHALL be prepared and executed
- AND results SHALL be returned correctly

#### Scenario: Row description

- GIVEN a query returning columns
- WHEN sending row data
- THEN row description SHALL include column names and types
- AND client SHALL interpret data correctly

### Requirement: System Views Compatibility

The system SHALL provide PostgreSQL-compatible system views.

#### Scenario: information_schema.tables

- GIVEN a query to `information_schema.tables`
- WHEN executing
- THEN table metadata SHALL be returned in PostgreSQL format
- AND include table_name, table_type, etc.

#### Scenario: information_schema.columns

- GIVEN a query to `information_schema.columns`
- WHEN executing
- THEN column metadata SHALL be returned
- AND include column_name, data_type, ordinal_position

#### Scenario: pg_catalog.pg_tables

- GIVEN a query to `pg_catalog.pg_tables`
- WHEN executing
- THEN table information SHALL be returned
- AND include schemaname, tablename, tableowner
