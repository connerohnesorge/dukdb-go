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

