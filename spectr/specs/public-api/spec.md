# Public Api Specification

## Requirements

### Requirement: ConnId() Connection Identification API

The system SHALL provide a public API to retrieve unique identifiers for database connections.

**Rationale**: Users need to track per-connection state, implement connection pooling strategies, correlate queries with connections for monitoring, and debug multi-connection scenarios. This API exists in duckdb-go v1.4.3 and must be implemented for drop-in compatibility.

#### Scenario: Basic connection ID retrieval
- GIVEN open database connection
- WHEN calling ConnId(conn)
- THEN returns uint64 ID > 0
- AND ID is unique to this connection

#### Scenario: ID stability across multiple calls
- GIVEN open database connection
- WHEN calling ConnId(conn) 100 times
- THEN all 100 calls return identical ID value
- AND ID never changes for same connection

#### Scenario: ID uniqueness across connections
- GIVEN 100 different database connections created
- WHEN calling ConnId() on each connection
- THEN all 100 IDs are unique (no duplicates)
- AND IDs are monotonically increasing (sequential: 1, 2, 3, ...)

#### Scenario: Thread-safe ID generation
- GIVEN 100 connections created concurrently from 10 goroutines
- WHEN calling ConnId() on each connection
- THEN all 100 IDs are unique
- AND no race conditions occur (verified with -race flag)

#### Scenario: Nil connection error handling
- GIVEN nil *sql.Conn
- WHEN calling ConnId(nil)
- THEN returns error with message containing "nil"
- AND returns 0 for ID value (invalid ID sentinel)

#### Scenario: Closed connection error handling
- GIVEN database connection that has been closed
- WHEN calling ConnId(conn)
- THEN returns error with message containing "closed"
- AND returns 0 for ID value

#### Scenario: Wrong driver error handling
- GIVEN *sql.Conn for different database driver (e.g., sqlite3, postgres)
- WHEN calling ConnId(conn)
- THEN returns error with message containing "not a dukdb connection"
- AND returns 0 for ID value

#### Scenario: Performance target for ID lookup
- GIVEN open database connection
- WHEN calling ConnId(conn) in tight loop
- THEN completes in <100ns per call (negligible overhead)
- AND no memory allocations per call (after warmup)

#### Scenario: Connection reuse after close
- GIVEN connection ID 5
- WHEN connection is closed
- AND new connection is created
- THEN new connection has ID 6 (not 5 reused)
- AND old ID is never recycled

#### Scenario: ID space exhaustion (theoretical)
- GIVEN uint64 ID space (18 quintillion IDs)
- WHEN calculating connections needed to exhaust space
- THEN would require 584,542 years at 1M connections/second
- AND wraparound is not a practical concern (no handling needed)

#### Scenario: Cross-connection ID comparison
- GIVEN two connections conn1 and conn2
- WHEN calling ConnId(conn1) and ConnId(conn2)
- THEN conn1 ID ≠ conn2 ID
- AND comparison is safe for equality checks

### Requirement: API Signature Compatibility

The ConnId() function SHALL match the signature and behavior of duckdb-go v1.4.3 for drop-in replacement compatibility.

#### Scenario: Function signature matches reference
- GIVEN reference duckdb-go v1.4.3 API
- THEN dukdb-go ConnId() has signature: func ConnId(c *sql.Conn) (uint64, error)
- AND parameter types match exactly
- AND return types match exactly

#### Scenario: Behavior matches reference for success case
- GIVEN reference duckdb-go v1.4.3 implementation
- AND dukdb-go implementation
- WHEN calling ConnId() on valid connections in both
- THEN both return non-zero uint64 IDs
- AND both return nil error

#### Scenario: Behavior matches reference for nil connection
- GIVEN reference duckdb-go v1.4.3 implementation
- WHEN calling ConnId(nil) in both implementations
- THEN both return error
- AND both return 0 for ID
- AND error messages are comparable

#### Scenario: Behavior matches reference for closed connection
- GIVEN reference duckdb-go v1.4.3 implementation
- WHEN calling ConnId() on closed connection in both
- THEN both return error
- AND both return 0 for ID
- AND error messages are comparable

### Requirement: Thread Safety and Concurrency

The system SHALL ensure thread-safe ID generation and access from concurrent goroutines.

#### Scenario: Concurrent connection creation
- GIVEN 1000 connections created from 10 concurrent goroutines
- WHEN each goroutine creates 100 connections and calls ConnId()
- THEN all 1000 IDs are unique
- AND no race conditions detected (go test -race passes)

#### Scenario: Concurrent ID access on same connection
- GIVEN single database connection
- WHEN 100 goroutines call ConnId() concurrently on same connection
- THEN all 100 calls return identical ID
- AND no race conditions detected

#### Scenario: No mutex contention on ID access
- GIVEN Connection.ID() method implementation
- WHEN ID is accessed (read-only operation)
- THEN no mutex lock is required (ID is immutable after construction)
- AND performance is optimal (<10ns for field access)

### Requirement: Error Handling and Validation

The system SHALL validate connection state and return descriptive errors for invalid inputs.

#### Scenario: Descriptive error message for nil connection
- GIVEN nil *sql.Conn
- WHEN calling ConnId(nil)
- THEN error message is "connection is nil"
- AND error is actionable (user knows what to fix)

#### Scenario: Descriptive error message for closed connection
- GIVEN closed database connection
- WHEN calling ConnId(conn)
- THEN error message contains "connection is closed"
- AND error is actionable

#### Scenario: Descriptive error message for wrong driver
- GIVEN *sql.Conn from sqlite3 driver
- WHEN calling ConnId(conn)
- THEN error message contains "not a dukdb connection"
- AND error message includes actual type (e.g., "*sqlite3.SQLiteConn")

#### Scenario: Zero ID sentinel for all error cases
- GIVEN any error condition (nil, closed, wrong driver)
- WHEN ConnId() returns error
- THEN ID value is 0 (sentinel for invalid)
- AND user can check `if id == 0` to detect error without inspecting error value

### Requirement: Integration with database/sql Package

The system SHALL integrate with Go's standard database/sql package using documented APIs.

#### Scenario: Use database/sql.Conn.Raw() for driver access
- GIVEN *sql.Conn wrapper
- WHEN ConnId() implementation accesses underlying driver connection
- THEN uses c.Raw(func(driverConn any) error) standard method
- AND does not use reflection or unsafe operations
- AND follows database/sql best practices

#### Scenario: Type assertion to driver.Conn implementation
- GIVEN underlying driver connection accessed via Raw()
- WHEN ConnId() type asserts to *Conn
- THEN assertion is safe (checked with ok := assertion)
- AND error returned if assertion fails
- AND error message includes actual type for debugging

#### Scenario: Compatibility with sql.DB.Conn() method
- GIVEN database opened with sql.Open("dukdb", "")
- WHEN obtaining connection with db.Conn(context.Background())
- AND calling ConnId() on returned *sql.Conn
- THEN works correctly (no special handling needed)
- AND follows standard database/sql patterns

### Requirement: Documentation and Usability

The system SHALL provide clear documentation and examples for ConnId() API usage.

#### Scenario: Godoc example for basic usage
- GIVEN ConnId() godoc comments
- WHEN user views documentation
- THEN includes runnable example showing:
  - Opening database
  - Getting connection
  - Calling ConnId()
  - Printing ID
  - Error handling

#### Scenario: Documentation of thread-safety guarantees
- GIVEN ConnId() API documentation
- WHEN user reads godoc
- THEN clearly states "Thread-safe: Can be called concurrently on same or different connections"
- AND documents that ID is immutable after connection creation

#### Scenario: Documentation of error conditions
- GIVEN ConnId() godoc comments
- WHEN user reads "Returns error if:" section
- THEN lists all error cases: nil connection, closed connection, wrong driver
- AND users know what to expect

#### Scenario: Usage example for connection tracking
- GIVEN API documentation or examples
- WHEN user wants to implement connection pooling
- THEN example shows:
  ```go
  connMap := make(map[uint64]*sql.Conn)
  conn, _ := db.Conn(ctx)
  id, _ := dukdb.ConnId(conn)
  connMap[id] = conn  // Track connection by ID
  ```

#### Scenario: Usage example for query tracing
- GIVEN API documentation
- WHEN user wants to correlate queries with connections
- THEN example shows:
  ```go
  id, _ := dukdb.ConnId(conn)
  log.Printf("Executing query on connection %d", id)
  rows, _ := conn.QueryContext(ctx, "SELECT ...")
  ```

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
