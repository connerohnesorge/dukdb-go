# Query Execution Specification - Database Maintenance Commands

## ADDED Requirements

### Requirement: PRAGMA Statement Execution

The system SHALL support PRAGMA statements for database introspection and configuration.

#### Scenario: PRAGMA database_size returns size information
- GIVEN a database with tables and data
- WHEN executing "PRAGMA database_size"
- THEN result contains columns: database_name, database_size, block_size, total_blocks, used_blocks, free_blocks, wal_size, memory_usage, memory_limit

#### Scenario: PRAGMA table_info returns column metadata
- GIVEN a table "users" with columns (id INTEGER, name VARCHAR, email VARCHAR)
- WHEN executing "PRAGMA table_info('users')"
- THEN result contains columns: cid, name, type, notnull, dflt_value, pk for each column

#### Scenario: PRAGMA functions returns all available functions
- GIVEN a database connection
- WHEN executing "PRAGMA functions"
- THEN result contains columns: name, type, arguments, return_type for each function

#### Scenario: PRAGMA tables returns all tables
- GIVEN a database with tables "users" and "orders"
- WHEN executing "PRAGMA tables"
- THEN result contains table names and optional schema information

#### Scenario: PRAGMA max_memory setting
- GIVEN a database connection
- WHEN executing "PRAGMA max_memory"
- THEN returns current max memory setting in bytes

#### Scenario: PRAGMA max_memory = value sets memory limit
- WHEN executing "PRAGMA max_memory = 1073741824" (1GB)
- THEN subsequent queries are limited to 1GB memory
- AND returns new memory setting

#### Scenario: PRAGMA threads returns/-sets thread count
- GIVEN a database connection
- WHEN executing "PRAGMA threads"
- THEN returns current thread count
- AND "PRAGMA threads = 4" sets thread count to 4

#### Scenario: PRAGMA enable_profiling activates profiling
- WHEN executing "PRAGMA enable_profiling"
- THEN subsequent query execution collects profiling data
- AND profiling output can be retrieved via PRAGMA disable_profiling or query results

#### Scenario: PRAGMA disable_profiling stops profiling
- GIVEN profiling is enabled
- WHEN executing "PRAGMA disable_profiling"
- THEN profiling data collection stops
- AND query results no longer include profiling output

#### Scenario: PRAGMA profiling_mode sets detail level
- WHEN executing "PRAGMA profiling_mode = 'detailed'"
- THEN detailed profiling mode is activated
- AND "PRAGMA profiling_mode = 'standard'" sets standard mode

#### Scenario: PRAGMA storage_info returns table storage details
- GIVEN a table "users" with data
- WHEN executing "PRAGMA storage_info('users')"
- THEN result contains columns: row_group_id, column_name, column_id, segment_type, count, compression, stats

### Requirement: EXPLAIN Statement Execution

The system SHALL support EXPLAIN statements to show query execution plans.

#### Scenario: EXPLAIN SELECT returns query plan
- GIVEN a query "SELECT * FROM users WHERE id = 1"
- WHEN executing "EXPLAIN SELECT * FROM users WHERE id = 1"
- THEN result contains a single column "explain" with plan text
- AND plan shows operator tree with HASH_JOIN, FILTER, SCAN nodes

#### Scenario: EXPLAIN with JOIN shows join plan
- GIVEN tables "users" and "orders"
- WHEN executing "EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id"
- THEN result shows HASH_JOIN or NESTED_LOOP_JOIN operator
- AND shows which tables are on left/right sides

#### Scenario: EXPLAIN with AGGREGATE shows aggregation plan
- GIVEN a query "SELECT COUNT(*) FROM users GROUP BY status"
- WHEN executing "EXPLAIN SELECT COUNT(*) FROM users GROUP BY status"
- THEN result shows GROUP_BY AGGREGATE operator
- AND shows group by expressions

#### Scenario: EXPLAIN output format is tree-based
- WHEN executing any EXPLAIN
- THEN output uses ASCII tree format:
  ```
  ────────────────────────────────
  ┌─────────────────────────────┐
  │      OPERATOR NAME          │
  │   Details on operator       │
  └─────────────────────────────┘
  ────────────────────────────────
  │        CHILD OPERATOR       │
  └─────────────────────────────┘
  ```

### Requirement: EXPLAIN ANALYZE Execution

The system SHALL support EXPLAIN ANALYZE to execute queries and show actual performance metrics.

#### Scenario: EXPLAIN ANALYZE executes query
- GIVEN tables with data
- WHEN executing "EXPLAIN ANALYZE SELECT * FROM users WHERE id > 100"
- THEN query is actually executed
- AND result contains plan with actual row counts and timing

#### Scenario: EXPLAIN ANALYZE shows actual row counts
- GIVEN a query that returns 50 rows
- WHEN executing "EXPLAIN ANALYZE SELECT ..."
- THEN plan shows actual_rows: 50 at relevant operators
- AND estimated vs actual comparison is visible

#### Scenario: EXPLAIN ANALYZE shows timing per operator
- WHEN executing "EXPLAIN ANALYZE"
- THEN plan shows wall_time for each operator
- AND shows cumulative and per-operator timing

#### Scenario: EXPLAIN ANALYZE with aggregate shows metrics
- WHEN executing "EXPLAIN ANALYZE SELECT COUNT(*) FROM large_table"
- THEN shows actual rows scanned
- AND shows aggregation timing

### Requirement: VACUUM Statement Execution

The system SHALL support VACUUM statements to reclaim storage space.

#### Scenario: VACUUM reclaims space from deleted rows
- GIVEN a table with 1000 rows
- AND DELETE FROM table WHERE id > 500 deletes 500 rows
- WHEN executing "VACUUM"
- THEN storage space is reclaimed
- AND table size is reduced

#### Scenario: VACUUM on specific table
- GIVEN tables "users" and "orders"
- WHEN executing "VACUUM users"
- THEN only "users" table is vacuumed
- AND "orders" table is unchanged

#### Scenario: VACUUM returns affected rows
- WHEN executing "VACUUM"
- THEN returns rows_affected count of reclaimed rows
- AND returns successfully even if no deleted rows exist

#### Scenario: VACUUM on non-existent table returns error
- WHEN executing "VACUUM non_existent_table"
- THEN error of type ErrorTypeCatalog is returned
- AND error message indicates table not found

### Requirement: ANALYZE Statement Execution

The system SHALL support ANALYZE statements to collect column statistics.

#### Scenario: ANALYZE collects column statistics
- GIVEN a table with various data types
- WHEN executing "ANALYZE"
- THEN column statistics are computed and stored
- AND statistics include min, max, null_count for each column

#### Scenario: ANALYZE on specific table
- GIVEN tables "users" and "orders"
- WHEN executing "ANALYZE users"
- THEN only "users" table statistics are updated
- AND "orders" statistics remain unchanged

#### Scenario: ANALYZE updates statistics in catalog
- WHEN executing "ANALYZE"
- THEN statistics are stored in catalog at column level
- AND statistics can be retrieved for query optimization

#### Scenario: ANALYZE on non-existent table returns error
- WHEN executing "ANALYZE non_existent_table"
- THEN error of type ErrorTypeCatalog is returned
- AND error message indicates table not found

#### Scenario: ANALYZE collects null counts
- GIVEN a table with nullable columns containing NULL values
- WHEN executing "ANALYZE"
- THEN null_count is recorded for each column
- AND null counts are accurate

#### Scenario: ANALYZE collects min/max for numeric columns
- GIVEN a numeric column with range 1-1000
- WHEN executing "ANALYZE"
- THEN min_value = 1 and max_value = 1000 are stored

### Requirement: CHECKPOINT Statement Execution

The system SHALL support CHECKPOINT statements for full database checkpoint.

#### Scenario: CHECKPOINT writes all pending changes
- GIVEN pending WAL entries from recent operations
- WHEN executing "CHECKPOINT"
- THEN all pending changes are written to storage
- AND WAL is flushed to main file

#### Scenario: CHECKPOINT merges row groups
- GIVEN a table with multiple small row groups
- WHEN executing "CHECKPOINT"
- THEN row groups are merged for efficiency
- AND storage layout is optimized

#### Scenario: CHECKPOINT truncates WAL
- GIVEN WAL has accumulated entries
- WHEN executing "CHECKPOINT"
- THEN after successful checkpoint, WAL is truncated
- AND recovery point is updated

#### Scenario: CHECKPOINT creates valid checkpoint header
- WHEN executing "CHECKPOINT"
- THEN new checkpoint header is written
- AND database can recover from this checkpoint

### Requirement: System Tables

The system SHALL support duckdb_*() table-valued functions for introspection.

#### Scenario: duckdb_tables returns all tables
- GIVEN a database with tables "users" and "orders"
- WHEN executing "SELECT * FROM duckdb_tables"
- THEN result contains schema_name, table_name, table_type for each table

#### Scenario: duckdb_columns returns all columns
- GIVEN a table "users" with columns (id, name, email)
- WHEN executing "SELECT * FROM duckdb_columns WHERE table_name = 'users'"
- THEN result contains column_name, data_type, is_nullable for each column

#### Scenario: duckdb_functions returns all functions
- GIVEN registered functions in the database
- WHEN executing "SELECT * FROM duckdb_functions"
- THEN result contains function_name, return_type, arguments

#### Scenario: duckdb_settings returns all settings
- WHEN executing "SELECT * FROM duckdb_settings"
- THEN result contains name, value, description for each setting

#### Scenario: duckdb_types returns all available types
- WHEN executing "SELECT * FROM duckdb_types"
- THEN result contains type_name, type_id, type_size

#### Scenario: duckdb_views returns all views
- GIVEN views created in the database
- WHEN executing "SELECT * FROM duckdb_views"
- THEN result contains view_name, view_definition for each view

#### Scenario: System tables appear in duckdb_tables
- GIVEN system tables like "duckdb_tables" exist
- WHEN querying "PRAGMA tables" or "SELECT * FROM duckdb_tables"
- THEN system tables are included in results
