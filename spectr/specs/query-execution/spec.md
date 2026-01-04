# Query Execution Specification

## Requirements

### Requirement: Parameter Binding

The package SHALL support positional and named parameter binding with SQL injection prevention.

#### Scenario: Positional parameters
- GIVEN query "SELECT $1 + $2 AS sum"
- WHEN executed with args []any{10, 20}
- THEN result contains {"sum": 30}

#### Scenario: Named parameters
- GIVEN query "SELECT @value * 2 AS doubled"
- WHEN executed with driver.NamedValue{Name: "value", Value: 5}
- THEN result contains {"doubled": 10}

#### Scenario: NULL parameter
- GIVEN query "SELECT $1 IS NULL AS is_null"
- WHEN executed with args []any{nil}
- THEN result contains {"is_null": true}

#### Scenario: String escaping single quote
- GIVEN query "SELECT $1 AS text"
- WHEN executed with args []any{"O'Brien"}
- THEN result contains {"text": "O'Brien"}
- AND the formatted SQL contains `'O''Brien'`

#### Scenario: String SQL injection prevention
- GIVEN query "SELECT $1 AS text"
- WHEN executed with args []any{"'; DROP TABLE users; --"}
- THEN result contains {"text": "'; DROP TABLE users; --"}
- AND no table is dropped

#### Scenario: BLOB parameter
- GIVEN query "SELECT $1 AS data"
- WHEN executed with args []any{[]byte{0x48, 0x45, 0x4C, 0x4C, 0x4F}}
- THEN the formatted SQL contains `X'48454C4C4F'`

#### Scenario: Timestamp parameter
- GIVEN query "SELECT $1 AS ts"
- WHEN executed with args []any{time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC)}
- THEN the formatted SQL contains `'2024-01-15 10:30:45.123456'`

#### Scenario: UUID parameter
- GIVEN query "SELECT $1 AS id"
- WHEN executed with UUID value
- THEN the formatted SQL contains hyphenated string format

#### Scenario: Decimal parameter
- GIVEN query "SELECT $1 AS amount"
- WHEN executed with Decimal{Scale: 2, Value: big.NewInt(12345)}
- THEN the formatted SQL contains `123.45` (unquoted)

#### Scenario: Interval parameter
- GIVEN query "SELECT $1 AS duration"
- WHEN executed with Interval{Months: 1, Days: 2, Micros: 3000000}
- THEN the formatted SQL contains `INTERVAL '1 months 2 days 3000000 microseconds'`

#### Scenario: Parameter count mismatch - too few args
- GIVEN query "SELECT $1, $2"
- WHEN executed with only one argument
- THEN error of type ErrorTypeInvalid is returned
- AND error message indicates missing parameter $2

#### Scenario: Parameter count mismatch - too many args
- GIVEN query "SELECT $1"
- WHEN executed with two arguments
- THEN error of type ErrorTypeInvalid is returned
- AND error message indicates extra parameters

#### Scenario: Named parameter not found
- GIVEN query "SELECT @foo"
- WHEN executed with driver.NamedValue{Name: "bar", Value: 1}
- THEN error of type ErrorTypeInvalid is returned
- AND error message indicates @foo not found in args

#### Scenario: Float infinity not supported
- GIVEN query "SELECT $1"
- WHEN executed with math.Inf(1)
- THEN error of type ErrorTypeInvalid is returned
- AND error message indicates infinity not supported

#### Scenario: Float NaN not supported
- GIVEN query "SELECT $1"
- WHEN executed with math.NaN()
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "NaN values not supported as parameters"

#### Scenario: Placeholder inside string literal preserved
- GIVEN query "SELECT '$1 is literal' AS text, $1 AS actual"
- WHEN executed with args []any{42}
- THEN result contains {"text": "$1 is literal", "actual": 42}
- AND only the placeholder OUTSIDE the string is replaced

#### Scenario: $0 is not a valid placeholder
- GIVEN query "SELECT $0, $1"
- WHEN executed with args []any{42}
- THEN $0 is treated as literal text
- AND $1 is replaced with 42

#### Scenario: Gap in positional placeholders
- GIVEN query "SELECT $1, $3"
- WHEN executed with args []any{10, 20, 30}
- THEN $1 is replaced with 10, $3 is replaced with 30
- AND arg[1] (20) is not used but required

#### Scenario: Duplicate positional placeholders
- GIVEN query "SELECT $1, $1, $1"
- WHEN executed with args []any{42}
- THEN all three $1 placeholders are replaced with 42
- AND only one argument is required

#### Scenario: Mixed positional and named parameters
- GIVEN query "SELECT $1, @foo"
- WHEN executed with any args
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "cannot mix positional ($N) and named (@name) parameters"

### Requirement: Query Execution

The package SHALL execute SQL queries via the backend and return results.

#### Scenario: Exec returns affected rows
- GIVEN a table with 5 rows
- WHEN executing "DELETE FROM t WHERE id < 3"
- THEN result.RowsAffected() returns (2, nil)

#### Scenario: Exec LastInsertId not supported
- GIVEN INSERT statement executed
- WHEN calling result.LastInsertId()
- THEN returns (0, nil)

#### Scenario: Query returns rows
- GIVEN a table with data
- WHEN executing "SELECT * FROM t ORDER BY id"
- THEN Rows contains all matching records in order

#### Scenario: Multiple statement execution
- GIVEN query "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"
- WHEN executing via ExecContext
- THEN both statements execute
- AND result.RowsAffected() reflects last statement (1)

#### Scenario: Multiple statement partial failure
- GIVEN query "INSERT INTO t VALUES (1); INVALID_SQL"
- WHEN executing via ExecContext
- THEN error is returned for second statement
- AND first statement is already committed (not atomic)

### Requirement: Transaction Support

The package SHALL support database transactions with BEGIN, COMMIT, and ROLLBACK semantics.

#### Scenario: Successful commit
- GIVEN a transaction started with BeginTx
- WHEN INSERT is executed and Commit() is called
- THEN changes are persisted

#### Scenario: Rollback discards changes
- GIVEN a transaction started with BeginTx
- WHEN INSERT is executed and Rollback() is called
- THEN changes are discarded

#### Scenario: Transaction isolation
- GIVEN two concurrent connections
- WHEN connection A inserts a row in transaction
- AND connection B queries before A commits
- THEN connection B does not see uncommitted changes

#### Scenario: Unsupported isolation level - Serializable
- GIVEN BeginTx with opts.Isolation = sql.LevelSerializable
- WHEN attempting to start transaction
- THEN error of type ErrorTypeSettings is returned
- AND error message is "only sql.LevelDefault isolation is supported"

#### Scenario: Unsupported isolation level - ReadCommitted
- GIVEN BeginTx with opts.Isolation = sql.LevelReadCommitted
- WHEN attempting to start transaction
- THEN error of type ErrorTypeSettings is returned

#### Scenario: Read-only not supported
- GIVEN BeginTx with opts.ReadOnly = true
- WHEN attempting to start transaction
- THEN error of type ErrorTypeSettings is returned
- AND error message is "read-only transactions are not supported"

#### Scenario: Double commit
- GIVEN a transaction that has been committed
- WHEN Commit() is called again
- THEN error of type ErrorTypeTransaction is returned
- AND error message is "transaction already completed"

#### Scenario: Double rollback
- GIVEN a transaction that has been rolled back
- WHEN Rollback() is called again
- THEN error of type ErrorTypeTransaction is returned

#### Scenario: Commit after rollback
- GIVEN a transaction that has been rolled back
- WHEN Commit() is called
- THEN error of type ErrorTypeTransaction is returned

### Requirement: Context Handling

The package SHALL respect context cancellation and deadlines during query execution.

#### Scenario: Pre-cancelled context
- GIVEN context that is already cancelled
- WHEN executing query
- THEN context.Canceled is returned immediately
- AND no backend call is made

#### Scenario: Context deadline shorter than backend timeout
- GIVEN context with 100ms deadline
- AND backend with 30s default timeout
- WHEN executing query that takes 200ms
- THEN context.DeadlineExceeded is returned

#### Scenario: Context deadline longer than backend timeout
- GIVEN context with 60s deadline
- AND backend with 30s default timeout
- WHEN executing query that takes 45s
- THEN backend timeout (30s) is used

#### Scenario: Context with no deadline
- GIVEN context.Background()
- WHEN executing query
- THEN query runs with backend default timeout

#### Scenario: Query completes before deadline
- GIVEN context with 1s deadline
- WHEN executing query that takes 10ms
- THEN query completes successfully
- AND no timeout error

### Requirement: Statement Interface

The package SHALL implement driver.Stmt interface for prepared statement support.

#### Scenario: NumInput returns parameter count
- GIVEN query "SELECT $1, $2, $3"
- WHEN calling stmt.NumInput()
- THEN returns 3

#### Scenario: NumInput with named parameters
- GIVEN query "SELECT @foo, @bar"
- WHEN calling stmt.NumInput()
- THEN returns 2

#### Scenario: Close marks statement closed
- GIVEN an open statement
- WHEN Close() is called
- THEN subsequent Exec/Query returns error of type ErrorTypeClosed
- AND error message is "statement is closed"

#### Scenario: Statement reuse
- GIVEN a statement
- WHEN executing multiple times with different parameters
- THEN each execution works correctly

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
