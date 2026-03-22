# System Views Specification

## Requirements

### Requirement: duckdb_schemas SHALL list all schemas

The duckdb_schemas() table function SHALL return a row for each schema in the database. It MUST include built-in schemas (main, information_schema, pg_catalog) and user-created schemas.

#### Scenario: Default schemas

Given a running database
When the user executes `SELECT schema_name FROM duckdb_schemas() WHERE internal = true ORDER BY schema_name`
Then the result MUST include 'information_schema', 'main', and 'pg_catalog'

#### Scenario: User-created schema

Given a running database
When the user executes `CREATE SCHEMA test_s; SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 'test_s'`
Then the result MUST return one row with schema_name 'test_s'

### Requirement: duckdb_types SHALL list all types

The duckdb_types() table function SHALL return a row for each type available in the database. It MUST include all built-in types and user-created types (ENUMs).

#### Scenario: Built-in numeric types

Given a running database
When the user executes `SELECT type_name FROM duckdb_types() WHERE type_category = 'NUMERIC' ORDER BY type_name`
Then the result MUST include 'BIGINT', 'DOUBLE', 'FLOAT', 'INTEGER', 'SMALLINT', 'TINYINT'

#### Scenario: User-created enum type

Given a running database
When the user executes `CREATE TYPE mood AS ENUM('happy', 'sad'); SELECT type_name FROM duckdb_types() WHERE type_name = 'mood'`
Then the result MUST return one row

