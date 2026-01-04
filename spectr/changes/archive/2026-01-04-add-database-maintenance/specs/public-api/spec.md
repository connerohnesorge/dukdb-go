# Public Api Specification - Database Maintenance Commands

## ADDED Requirements

### Requirement: PRAGMA Statement Type Detection

The system SHALL detect and parse PRAGMA statements correctly.

#### Scenario: PRAGMA statement returns STATEMENT_TYPE_PRAGMA
- GIVEN query "PRAGMA database_size"
- WHEN calling `dukdb.StmtType(query)` or using statement detection
- THEN returns `dukdb.STATEMENT_TYPE_PRAGMA`

#### Scenario: PRAGMA with arguments detected
- GIVEN query "PRAGMA table_info('users')"
- WHEN parsing
- THEN statement type is STATEMENT_TYPE_PRAGMA
- AND arguments are parsed correctly

#### Scenario: SET PRAGMA detected
- GIVEN query "PRAGMA max_memory = 1073741824"
- WHEN parsing
- THEN statement type is STATEMENT_TYPE_PRAGMA
- AND assignment is captured

### Requirement: EXPLAIN Statement Type Detection

The system SHALL detect and parse EXPLAIN statements correctly.

#### Scenario: EXPLAIN SELECT returns STATEMENT_TYPE_EXPLAIN
- GIVEN query "EXPLAIN SELECT * FROM users"
- WHEN calling statement detection
- THEN returns `dukdb.STATEMENT_TYPE_EXPLAIN`

#### Scenario: EXPLAIN ANALYZE detected
- GIVEN query "EXPLAIN ANALYZE SELECT * FROM users"
- WHEN parsing
- THEN statement type is STATEMENT_TYPE_EXPLAIN
- AND Analyze flag is set to true

#### Scenario: EXPLAIN with complex query
- GIVEN query "EXPLAIN SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
- WHEN parsing
- THEN statement type is STATEMENT_TYPE_EXPLAIN
- AND inner query is fully parsed

### Requirement: VACUUM Statement Type Detection

The system SHALL detect and parse VACUUM statements correctly.

#### Scenario: VACUUM returns STATEMENT_TYPE_VACUUM
- GIVEN query "VACUUM"
- WHEN calling statement detection
- THEN returns `dukdb.STATEMENT_TYPE_VACUUM`

#### Scenario: VACUUM table_name detected
- GIVEN query "VACUUM users"
- WHEN parsing
- THEN statement type is STATEMENT_TYPE_VACUUM
- AND table name is captured

### Requirement: ANALYZE Statement Type Detection

The system SHALL detect and parse ANALYZE statements correctly.

#### Scenario: ANALYZE returns statement type
- GIVEN query "ANALYZE"
- WHEN calling statement detection
- THEN returns appropriate statement type for ANALYZE

#### Scenario: ANALYZE table_name detected
- GIVEN query "ANALYZE users"
- WHEN parsing
- THEN table name is captured correctly

### Requirement: CHECKPOINT Statement Type Detection

The system SHALL detect and parse CHECKPOINT statements correctly.

#### Scenario: CHECKPOINT detected
- GIVEN query "CHECKPOINT"
- WHEN calling statement detection
- THEN returns appropriate statement type for CHECKPOINT

### Requirement: PRAGMA Result Format Compatibility

The system SHALL return PRAGMA results in a format compatible with duckdb-go.

#### Scenario: PRAGMA database_size column names
- WHEN executing "PRAGMA database_size"
- THEN column names match: database_name, database_size, block_size, total_blocks, used_blocks, free_blocks, wal_size, memory_usage, memory_limit

#### Scenario: PRAGMA table_info column names
- WHEN executing "PRAGMA table_info('users')"
- THEN column names match: cid, name, type, notnull, dflt_value, pk

#### Scenario: PRAGMA functions column names
- WHEN executing "PRAGMA functions"
- THEN column names include: name, type, arguments, return_type

#### Scenario: PRAGMA results are queryable
- GIVEN PRAGMA result with multiple rows
- WHEN treating result as a regular result set
- THEN rows can be iterated
- AND column values can be accessed by index or name

### Requirement: EXPLAIN Result Format Compatibility

The system SHALL return EXPLAIN results in a text format compatible with duckdb-go.

#### Scenario: EXPLAIN returns single column named "explain"
- WHEN executing "EXPLAIN SELECT * FROM users"
- THEN result has one column named "explain"
- AND column type is VARCHAR

#### Scenario: EXPLAIN output contains operator tree
- WHEN executing "EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id"
- THEN "explain" column contains HASH_JOIN operator
- AND contains left/right child operators

#### Scenario: EXPLAIN output matches DuckDB format
- WHEN executing "EXPLAIN SELECT COUNT(*) FROM users GROUP BY status"
- THEN output format is similar to DuckDB text format
- AND contains GROUP_BY AGGREGATE description

#### Scenario: EXPLAIN ANALYZE returns same format with metrics
- WHEN executing "EXPLAIN ANALYZE SELECT * FROM users"
- THEN result still has "explain" column
- AND contains actual row counts and timing information

### Requirement: System Table Function Compatibility

The system SHALL support duckdb_*() table functions compatible with duckdb-go.

#### Scenario: duckdb_tables is queryable as table
- WHEN executing "SELECT * FROM duckdb_tables"
- THEN returns result set with table metadata
- AND columns include: schema_name, table_name, table_type

#### Scenario: duckdb_columns is queryable
- WHEN executing "SELECT * FROM duckdb_columns"
- THEN returns result set with column metadata
- AND columns include: schema_name, table_name, column_name, column_type

#### Scenario: duckdb_functions is queryable
- WHEN executing "SELECT * FROM duckdb_functions"
- THEN returns result set with function metadata
- AND columns include: function_name, return_type

#### Scenario: duckdb_settings is queryable
- WHEN executing "SELECT * FROM duckdb_settings"
- THEN returns result set with setting metadata
- AND columns include: name, value, description

#### Scenario: System tables support WHERE clause
- WHEN executing "SELECT * FROM duckdb_tables WHERE table_name = 'users'"
- THEN results are filtered correctly
- AND query optimization is applied

#### Scenario: System tables support ORDER BY
- WHEN executing "SELECT * FROM duckdb_tables ORDER BY table_name"
- THEN results are sorted correctly
- AND sorting is efficient

### Requirement: PRAGMA Configuration Persistence

The system SHALL handle PRAGMA configuration changes appropriately.

#### Scenario: PRAGMA max_memory changes session setting
- WHEN executing "PRAGMA max_memory = 2147483648"
- THEN current session uses new memory limit
- AND subsequent queries respect the limit

#### Scenario: PRAGMA threads changes parallelism
- WHEN executing "PRAGMA threads = 8"
- THEN subsequent queries use up to 8 threads
- AND parallel operators scale accordingly

#### Scenario: PRAGMA enable_profiling activates collection
- WHEN executing "PRAGMA enable_profiling"
- THEN profiling data is collected for subsequent queries
- AND can be retrieved via profiling API

### Requirement: VACUUM and ANALYZE Impact on Query Planning

The system SHALL use ANALYZE statistics for query optimization.

#### Scenario: ANALYZE updates statistics used by planner
- GIVEN table "users" with 100000 rows
- WHEN executing "ANALYZE users"
- THEN column statistics are stored
- AND subsequent query plans may use statistics

#### Scenario: VACUUM does not affect statistics
- GIVEN ANALYZE has collected statistics
- WHEN executing "VACUUM"
- THEN statistics remain unchanged
- AND query plans continue to use existing statistics

#### Scenario: Large table ANALYZE completes in reasonable time
- GIVEN table with 10 million rows
- WHEN executing "ANALYZE"
- THEN operation completes without timeout
- AND statistics are approximate (sampling allowed)

### Requirement: CHECKPOINT and Recovery

The system SHALL support CHECKPOINT for durability.

#### Scenario: CHECKPOINT enables faster recovery
- GIVEN database has accumulated WAL entries
- WHEN executing "CHECKPOINT"
- THEN recovery point is updated
- AND subsequent recovery starts from checkpoint

#### Scenario: CHECKPOINT during active transactions
- GIVEN open transaction inserting data
- WHEN executing "CHECKPOINT"
- THEN checkpoint waits for transaction commit
- OR checkpoint includes uncommitted data appropriately

#### Scenario: Multiple CHECKPOINTs create valid history
- GIVEN multiple CHECKPOINTs over time
- WHEN recovering database
- THEN starts from most recent valid checkpoint
- AND replays WAL from checkpoint onwards

### Requirement: Error Handling for Maintenance Commands

The system SHALL provide appropriate error messages for maintenance commands.

#### Scenario: PRAGMA on unknown pragma name
- GIVEN query "PRAGMA unknown_pragma"
- WHEN executing
- THEN error indicates unknown pragma
- AND suggests valid pragma names if possible

#### Scenario: PRAGMA with invalid argument type
- GIVEN query "PRAGMA max_memory = 'invalid'"
- WHEN executing
- THEN error indicates type mismatch
- AND expected type is clear

#### Scenario: VACUUM on system table
- GIVEN query "VACUUM duckdb_tables"
- WHEN executing
- THEN error indicates cannot vacuum system tables
- OR operation completes with no effect

#### Scenario: ANALYZE on system table
- GIVEN query "ANALYZE duckdb_tables"
- WHEN executing
- THEN error indicates cannot analyze system tables
- OR operation completes with no effect

#### Scenario: EXPLAIN on non-SELECT statement
- GIVEN query "EXPLAIN INSERT INTO users VALUES (1, 'test')"
- WHEN executing
- THEN explains the underlying SELECT if applicable
- OR returns appropriate error for non-explainable statements

### Requirement: Statement Type Names

The system SHALL return correct statement type names.

#### Scenario: STATEMENT_TYPE_PRAGMA returns "PRAGMA"
- GIVEN STATEMENT_TYPE_PRAGMA constant
- WHEN calling dukdb.StmtTypeName()
- THEN returns "PRAGMA"

#### Scenario: STATEMENT_TYPE_EXPLAIN returns "EXPLAIN"
- GIVEN STATEMENT_TYPE_EXPLAIN constant
- WHEN calling dukdb.StmtTypeName()
- THEN returns "EXPLAIN"

#### Scenario: STATEMENT_TYPE_VACUUM returns "VACUUM"
- GIVEN STATEMENT_TYPE_VACUUM constant
- WHEN calling dukdb.StmtTypeName()
- THEN returns "VACUUM"

#### Scenario: Statement type integers match duckdb-go
- GIVEN duckdb-go v1.4.3 statement type values
- THEN dukdb-go uses same integer values for compatibility
- AND STATEMENT_TYPE_PRAGMA = 17
- AND STATEMENT_TYPE_VACUUM = 18
