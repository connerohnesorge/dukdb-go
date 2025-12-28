## ADDED Requirements

### Requirement: Process Lifecycle Management

The backend SHALL manage DuckDB CLI process lifecycle including spawning, health monitoring, crash detection, and termination.

#### Scenario: Process spawning with correct flags
- GIVEN a configured DuckDB binary path
- WHEN initializing a new ProcessBackend and opening a connection
- THEN a DuckDB CLI process is spawned with flags `-json -noheader -nullvalue NULL <dbpath>`

#### Scenario: Graceful shutdown sequence
- GIVEN a running ProcessBackend with active process
- WHEN Close() is called
- THEN the backend sends `.quit\n` to stdin
- AND waits up to 5 seconds for clean exit
- AND sends SIGTERM if still running, waits 2 seconds
- AND sends SIGKILL if still running
- AND closes all file descriptors

#### Scenario: Process crash detection via exit monitoring
- GIVEN a ProcessBackend with running process
- WHEN the process is killed externally (kill -9)
- THEN the backend detects crash when cmd.Wait() returns (no specific timing guarantee)
- AND sets internal crashed atomic.Bool to true
- AND subsequent IsAlive() calls return false
- AND pending queries receive ErrorTypeConnection error

#### Scenario: Automatic restart after crash
- GIVEN a ProcessBackend whose process has crashed
- WHEN a new query is attempted
- THEN the backend spawns a new process
- AND the query executes on the new process
- AND returns results normally

### Requirement: Binary Location Resolution

The backend SHALL resolve the DuckDB CLI binary location from configuration, environment variable, or PATH in that order.

#### Scenario: Explicit path configuration
- GIVEN ProcessBackendConfig with BinaryPath = "/usr/local/bin/duckdb"
- WHEN initializing the backend
- THEN the binary at /usr/local/bin/duckdb is used

#### Scenario: Environment variable resolution
- GIVEN ProcessBackendConfig with empty BinaryPath
- AND environment variable DUCKDB_PATH = "/opt/duckdb/duckdb"
- WHEN initializing the backend
- THEN the binary at /opt/duckdb/duckdb is used

#### Scenario: PATH search
- GIVEN ProcessBackendConfig with empty BinaryPath
- AND no DUCKDB_PATH environment variable
- AND "duckdb" executable exists in PATH
- WHEN initializing the backend
- THEN the duckdb from PATH is used

#### Scenario: Binary not found error
- GIVEN no DuckDB binary available in any location
- WHEN initializing the backend
- THEN error of type ErrorTypeConnection is returned
- AND error message is "DuckDB CLI binary not found. Install from https://duckdb.org/docs/installation or set DUCKDB_PATH environment variable."

### Requirement: Version Compatibility

The backend SHALL require DuckDB CLI version 0.9.0 or later and verify on startup.

#### Scenario: Version check passes
- GIVEN DuckDB CLI version 0.9.0 or later
- WHEN initializing the backend
- THEN initialization succeeds

#### Scenario: Version check fails
- GIVEN DuckDB CLI version earlier than 0.9.0
- WHEN initializing the backend
- THEN error of type ErrorTypeConnection is returned
- AND error message contains "requires DuckDB 0.9.0 or later"
- AND error message contains the detected version

### Requirement: SQL Command Execution

The backend SHALL execute SQL commands via stdin and capture results from stdout using JSON format with UUID markers.

#### Scenario: Simple SELECT query
- GIVEN a running ProcessBackend
- WHEN executing "SELECT 1 AS value, 'hello' AS greeting"
- THEN result contains one row: `{"value": 1, "greeting": "hello"}`
- AND column names are ["value", "greeting"]

#### Scenario: Multi-row query
- GIVEN a running ProcessBackend
- WHEN executing "SELECT * FROM range(3) AS t(n)"
- THEN result contains three rows with n = 0, 1, 2

#### Scenario: Empty result set
- GIVEN a running ProcessBackend
- WHEN executing "SELECT 1 WHERE false"
- THEN result is empty slice `[]`
- AND no error is returned

#### Scenario: Multiple statements in one query
- GIVEN a running ProcessBackend
- WHEN executing "CREATE TABLE t(x INT); INSERT INTO t VALUES (1); SELECT * FROM t"
- THEN all statements execute in order
- AND final SELECT returns [{x: 1}]

#### Scenario: Query with syntax error
- GIVEN a running ProcessBackend
- WHEN executing "SELEC invalid syntax"
- THEN error of type ErrorTypeParser is returned
- AND error message contains DuckDB's error text

#### Scenario: Query with runtime error
- GIVEN a running ProcessBackend
- WHEN executing "SELECT 1/0"
- THEN error of type ErrorTypeDivideByZero is returned

### Requirement: Result Parsing

The backend SHALL parse DuckDB CLI JSON output into Go types correctly.

#### Scenario: NULL value parsing
- GIVEN a query returning NULL values
- WHEN parsing results
- THEN NULL is represented as nil in Go map

#### Scenario: Integer parsing
- GIVEN a query returning INTEGER column
- WHEN parsing results
- THEN values are Go float64 (JSON number default) or int64 after type assertion

#### Scenario: String parsing
- GIVEN a query returning VARCHAR column
- WHEN parsing results
- THEN values are Go string

#### Scenario: Boolean parsing
- GIVEN a query returning BOOLEAN column
- WHEN parsing results
- THEN values are Go bool

#### Scenario: Nested STRUCT parsing
- GIVEN a query returning STRUCT column like `{'a': 1, 'b': 'hello'}`
- WHEN parsing results
- THEN value is Go `map[string]any` with correct nested values

#### Scenario: LIST parsing
- GIVEN a query returning LIST column like `[1, 2, 3]`
- WHEN parsing results
- THEN value is Go `[]any` with correct elements

#### Scenario: Large result set
- GIVEN a query returning 100,000 rows
- WHEN parsing results
- THEN all rows are returned without memory error
- AND parsing completes within reasonable time (< 10 seconds)

### Requirement: Concurrent Query Support

The backend SHALL support concurrent query execution with correct result isolation.

#### Scenario: Concurrent SELECT queries
- GIVEN a running ProcessBackend
- WHEN 10 goroutines each execute "SELECT <unique_id>" simultaneously
- THEN each goroutine receives its correct unique_id
- AND no results are mixed between goroutines

#### Scenario: Concurrent queries with mutex
- GIVEN a running ProcessBackend
- WHEN 100 concurrent queries are submitted
- THEN stdin/stdout access is serialized via mutex
- AND all queries complete successfully

#### Scenario: Query timeout via context
- GIVEN a running ProcessBackend with QueryTimeout = 1 second
- WHEN executing a query that takes 5 seconds
- THEN error context.DeadlineExceeded is returned after ~1 second
- AND the underlying query continues in background (cannot cancel CLI)

#### Scenario: Context cancellation
- GIVEN a running ProcessBackend
- WHEN executing a query and context is cancelled before completion
- THEN error context.Canceled is returned immediately

### Requirement: Backend Interface Compliance

The ProcessBackend SHALL implement the Backend interface and ProcessConn SHALL implement BackendConn interface from project-foundation.

#### Scenario: Backend interface satisfaction
- GIVEN the ProcessBackend type
- WHEN checking interface compliance with `var _ Backend = (*ProcessBackend)(nil)`
- THEN compilation succeeds

#### Scenario: BackendConn interface satisfaction
- GIVEN the ProcessConn type
- WHEN checking interface compliance with `var _ BackendConn = (*ProcessConn)(nil)`
- THEN compilation succeeds

#### Scenario: Open returns usable connection
- GIVEN a ProcessBackend with valid DuckDB binary
- WHEN calling Open(":memory:", nil)
- THEN a BackendConn is returned
- AND calling conn.Ping(ctx) returns nil

#### Scenario: Close releases resources
- GIVEN a ProcessBackend with open connections
- WHEN calling Close()
- THEN all processes are terminated
- AND no goroutines are leaked (verified with runtime.NumGoroutine delta)

### Requirement: Configuration

The backend SHALL support configuration via ProcessBackendConfig struct with sensible defaults.

#### Scenario: Default configuration values
- GIVEN a zero-value ProcessBackendConfig
- WHEN using for backend initialization
- THEN QueryTimeout defaults to 30 seconds
- AND StartupTimeout defaults to 10 seconds
- AND MaxRetries defaults to 3
- AND RetryBackoff defaults to 100 milliseconds

#### Scenario: Custom timeout configuration
- GIVEN ProcessBackendConfig with QueryTimeout = 5 * time.Second
- WHEN a query takes 10 seconds
- THEN query times out at approximately 5 seconds

### Requirement: Error Handling

The backend SHALL return structured errors matching duckdb-go error types, mapping CLI stderr patterns to error types.

#### Scenario: Connection error type
- GIVEN a binary not found condition
- WHEN error is returned
- THEN error type is ErrorTypeConnection

#### Scenario: Parser error type
- GIVEN a SQL syntax error (CLI stderr contains "Parser Error")
- WHEN error is returned
- THEN error type is ErrorTypeParser

#### Scenario: Binder error type
- GIVEN a query with unknown column (CLI stderr contains "Binder Error")
- WHEN error is returned
- THEN error type is ErrorTypeBinder

#### Scenario: Catalog error type
- GIVEN a query referencing non-existent table (CLI stderr contains "Catalog Error")
- WHEN error is returned
- THEN error type is ErrorTypeCatalog

#### Scenario: Divide by zero error type
- GIVEN a query with division by zero (CLI stderr contains "division by zero")
- WHEN error is returned
- THEN error type is ErrorTypeDivideByZero

#### Scenario: Constraint error type
- GIVEN a query violating constraint (CLI stderr contains "constraint")
- WHEN error is returned
- THEN error type is ErrorTypeConstraint

#### Scenario: Unknown error type fallback
- GIVEN a DuckDB error not matching any known pattern
- WHEN error is returned
- THEN error type is ErrorTypeUnknown
- AND original CLI stderr is preserved in Msg field

#### Scenario: Error message preservation
- GIVEN an error from DuckDB CLI
- WHEN creating Error struct
- THEN the original DuckDB error message is preserved in Msg field

### Requirement: Edge Cases

The backend SHALL handle edge cases gracefully.

#### Scenario: Execute after Close
- GIVEN a ProcessBackend that has been closed
- WHEN attempting to execute a query
- THEN error of type ErrorTypeConnection is returned
- AND error message indicates backend is closed

#### Scenario: Double Close
- GIVEN a ProcessBackend
- WHEN Close() is called twice
- THEN second call returns nil (idempotent)
- AND no panic or error occurs

#### Scenario: Empty database path for in-memory
- GIVEN Open called with path = "" or path = ":memory:"
- WHEN initializing connection
- THEN an in-memory database is created
- AND queries execute correctly

#### Scenario: Special characters in query
- GIVEN a query containing Unicode, quotes, newlines
- WHEN executing the query
- THEN characters are handled correctly
- AND results are accurate
