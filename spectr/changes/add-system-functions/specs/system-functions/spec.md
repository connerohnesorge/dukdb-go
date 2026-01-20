# System Functions Specification

## ADDED Requirements

### Requirement: System Functions Table Functions

The system SHALL provide 15 table functions that return metadata about the database, callable as `SELECT * FROM duckdb_*()` with no parameters.

Each function returns a read-only result set with columns as specified per function below.

#### Scenario: duckdb_settings() function

- WHEN user executes `SELECT * FROM duckdb_settings()`
- THEN result contains columns: `name VARCHAR`, `value VARCHAR`, `description VARCHAR`
- AND includes all system settings (e.g., 'threads', 'memory_limit', 'default_null_order')
- AND result is read-only (no INSERT/UPDATE/DELETE)

#### Scenario: duckdb_functions() function

- WHEN user executes `SELECT * FROM duckdb_functions()`
- THEN result contains columns: `function_name VARCHAR`, `parameters VARCHAR`, `return_type VARCHAR`, `function_type VARCHAR`, `description VARCHAR`
- AND includes all scalar functions, aggregate functions, table functions
- AND `function_type` values include: 'scalar', 'aggregate', 'table', 'window'
- AND includes built-in functions and user-defined functions

#### Scenario: duckdb_tables() function

- WHEN user executes `SELECT * FROM duckdb_tables()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `table_name VARCHAR`, `table_type VARCHAR`, `comment VARCHAR`
- AND `table_type` is 'BASE TABLE' for permanent tables
- AND returns all tables across all schemas in 'main' database
- AND excludes views (use duckdb_views() for views)

#### Scenario: duckdb_columns() function

- WHEN user executes `SELECT * FROM duckdb_columns()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `table_name VARCHAR`, `column_name VARCHAR`, `column_index INTEGER`, `data_type VARCHAR`, `is_nullable BOOLEAN`, `default_value VARCHAR`, `comment VARCHAR`
- AND returns all columns from all tables
- AND `column_index` is 1-based position in table
- AND `is_nullable` reflects NULL constraint

#### Scenario: duckdb_constraints() function

- WHEN user executes `SELECT * FROM duckdb_constraints()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `table_name VARCHAR`, `constraint_name VARCHAR`, `constraint_type VARCHAR`, `constraint_text VARCHAR`
- AND `constraint_type` includes: 'PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY'
- AND returns all constraints from all tables

#### Scenario: duckdb_databases() function

- WHEN user executes `SELECT * FROM duckdb_databases()`
- THEN result contains columns: `database_name VARCHAR`, `path VARCHAR`, `comment VARCHAR`
- AND returns database 'main' as primary database
- AND includes memory statistics if available

#### Scenario: duckdb_views() function

- WHEN user executes `SELECT * FROM duckdb_views()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `view_name VARCHAR`, `view_definition VARCHAR`, `comment VARCHAR`
- AND returns all views across all schemas
- AND `view_definition` contains the original SELECT query
- AND excludes tables (use duckdb_tables() for tables)

#### Scenario: duckdb_indexes() function

- WHEN user executes `SELECT * FROM duckdb_indexes()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `table_name VARCHAR`, `index_name VARCHAR`, `index_columns VARCHAR`, `is_unique BOOLEAN`, `comment VARCHAR`
- AND `index_columns` is comma-separated list of column names
- AND returns all indexes

#### Scenario: duckdb_sequences() function

- WHEN user executes `SELECT * FROM duckdb_sequences()`
- THEN result contains columns: `database_name VARCHAR`, `schema_name VARCHAR`, `sequence_name VARCHAR`, `start_value BIGINT`, `increment BIGINT`, `min_value BIGINT`, `max_value BIGINT`, `cycle BOOLEAN`, `comment VARCHAR`
- AND returns all sequences

#### Scenario: duckdb_dependencies() function

- WHEN user executes `SELECT * FROM duckdb_dependencies()`
- THEN result contains columns: `dependent_name VARCHAR`, `dependent_type VARCHAR`, `dependency_name VARCHAR`, `dependency_type VARCHAR`
- AND `dependent_type` and `dependency_type` include: 'TABLE', 'VIEW', 'INDEX', 'SEQUENCE'
- AND shows view → table dependencies, index → table dependencies, etc.

#### Scenario: duckdb_optimizers() function

- WHEN user executes `SELECT * FROM duckdb_optimizers()`
- THEN result contains columns: `optimizer_name VARCHAR`, `enabled BOOLEAN`, `description VARCHAR`
- AND includes optimizer settings like 'pushdown', 'index_scan', 'cte_inlining'

#### Scenario: duckdb_keywords() function

- WHEN user executes `SELECT * FROM duckdb_keywords()`
- THEN result contains columns: `keyword VARCHAR`, `reserved BOOLEAN`
- AND returns SQL reserved keywords (SELECT, FROM, WHERE, etc.)
- AND includes all keywords used by DuckDB parser

#### Scenario: duckdb_extensions() function

- WHEN user executes `SELECT * FROM duckdb_extensions()`
- THEN result contains columns: `extension_name VARCHAR`, `loaded BOOLEAN`, `install_path VARCHAR`, `description VARCHAR`
- AND initially returns empty result (no-op, but extensible)

#### Scenario: duckdb_memory_usage() function

- WHEN user executes `SELECT * FROM duckdb_memory_usage()`
- THEN result contains columns: `category VARCHAR`, `bytes_used BIGINT`, `bytes_limit BIGINT`
- AND categories include: 'total', 'table_data', 'index_data', 'temporary', 'cache'

#### Scenario: duckdb_temp_directory() function

- WHEN user executes `SELECT * FROM duckdb_temp_directory()`
- THEN result contains columns: `path VARCHAR`, `available_space BIGINT`, `used_space BIGINT`
- AND returns system temporary directory configuration

### Requirement: PRAGMA Statements

The system SHALL support 9 PRAGMA statements for metadata access and configuration queries.

#### Scenario: PRAGMA database_size

- WHEN user executes `PRAGMA database_size`
- THEN returns single row with columns: `database_size BIGINT` (in bytes)

#### Scenario: PRAGMA table_info('table_name')

- WHEN user executes `PRAGMA table_info('my_table')`
- THEN returns columns: `cid INTEGER`, `name VARCHAR`, `type VARCHAR`, `notnull BOOLEAN`, `dflt_value VARCHAR`, `pk BOOLEAN`
- AND `cid` is column index (0-based)
- AND `pk` is TRUE for primary key columns

#### Scenario: PRAGMA database_list

- WHEN user executes `PRAGMA database_list`
- THEN returns columns: `seq INTEGER`, `name VARCHAR`, `file VARCHAR`
- AND lists 'main' database

#### Scenario: PRAGMA version

- WHEN user executes `PRAGMA version`
- THEN returns single row: `version VARCHAR` (e.g., "1.4.3")
- AND matches DuckDB v1.4.3

#### Scenario: PRAGMA platform

- WHEN user executes `PRAGMA platform`
- THEN returns single row: `platform VARCHAR`, `os VARCHAR`, `arch VARCHAR`
- AND OS values: 'linux', 'darwin', 'windows'
- AND arch values: 'amd64', 'arm64', etc.

#### Scenario: PRAGMA functions

- WHEN user executes `PRAGMA functions`
- THEN returns function metadata similar to duckdb_functions()

#### Scenario: PRAGMA collations

- WHEN user executes `PRAGMA collations`
- THEN returns columns: `collation_name VARCHAR`, `collate_default BOOLEAN`

#### Scenario: PRAGMA table_storage_info('table_name')

- WHEN user executes `PRAGMA table_storage_info('my_table')`
- THEN returns columns: `segment_type VARCHAR`, `column_index INTEGER`, `column_name VARCHAR`, `compressed_size BIGINT`, `uncompressed_size BIGINT`, `compression BOOLEAN`

#### Scenario: PRAGMA storage_info('table_name')

- WHEN user executes `PRAGMA storage_info('my_table')`
- THEN returns storage breakdown by column similar to table_storage_info

### Requirement: Metadata Infrastructure

The system SHALL provide internal helper functions for consistent metadata extraction.

#### Scenario: Helper functions exist in internal/metadata package

- WHEN duckdb system functions are called
- THEN they use internal/metadata package helpers
- AND helpers provide consistent catalog querying
- AND include functions: GetAllTables, GetTableColumns, GetTableConstraints, GetAllViews, GetAllIndexes, GetAllSequences

#### Scenario: Consistent column ordering and types

- WHEN any metadata function returns results
- THEN column order and types match DuckDB v1.4.3 exactly
- AND NULL values are returned for missing fields
- AND string columns use VARCHAR type

#### Scenario: Schema visibility

- WHEN metadata functions return objects
- THEN they return objects from visible schemas
- AND respect catalog search paths
- AND include 'main' schema by default
- AND include 'pg_catalog' schema by default
- AND include 'information_schema' schema by default
