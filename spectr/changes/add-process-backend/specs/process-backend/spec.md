## ADDED Requirements

### Requirement: Process Lifecycle Management

The backend SHALL manage DuckDB CLI process lifecycle including spawning, monitoring, and termination.

#### Scenario: Process spawning
- GIVEN a configured DuckDB binary path
- WHEN initializing a new ProcessBackend
- THEN a DuckDB CLI process is spawned with appropriate flags

#### Scenario: Graceful shutdown
- GIVEN a running ProcessBackend
- WHEN Close is called
- THEN the CLI process receives proper termination signals and exits cleanly

#### Scenario: Process crash recovery
- GIVEN a ProcessBackend with a crashed CLI process
- WHEN a new query is attempted
- THEN the backend automatically restarts the process and retries the query

### Requirement: Binary Location Resolution

The backend SHALL resolve the DuckDB CLI binary location from configuration, PATH, or embedded location.

#### Scenario: Explicit path configuration
- GIVEN a backend configuration with explicit binary path
- WHEN initializing the backend
- THEN the specified binary is used

#### Scenario: PATH resolution
- GIVEN no explicit binary path configured
- WHEN initializing the backend
- THEN the backend searches PATH for "duckdb" executable

#### Scenario: Binary not found
- GIVEN no DuckDB binary available
- WHEN initializing the backend
- THEN a descriptive error is returned explaining how to install DuckDB

### Requirement: SQL Command Execution

The backend SHALL execute SQL commands via stdin and capture results from stdout.

#### Scenario: Simple query execution
- GIVEN a running ProcessBackend
- WHEN executing "SELECT 1 as value"
- THEN the result contains one row with value 1

#### Scenario: Multi-statement execution
- GIVEN a running ProcessBackend
- WHEN executing multiple SQL statements separated by semicolons
- THEN all statements are executed in order

#### Scenario: Query with error
- GIVEN a running ProcessBackend
- WHEN executing invalid SQL
- THEN an appropriate error is returned with DuckDB's error message

### Requirement: Result Parsing

The backend SHALL parse DuckDB CLI JSON output into structured Go types.

#### Scenario: JSON result parsing
- GIVEN a query that returns results
- WHEN the CLI outputs JSON
- THEN the backend parses it into appropriate Go types

#### Scenario: Null value handling
- GIVEN a query returning NULL values
- WHEN parsing results
- THEN NULL is correctly represented as nil in Go

#### Scenario: Nested type handling
- GIVEN a query returning STRUCT or LIST types
- WHEN parsing results
- THEN nested structures are correctly parsed

### Requirement: Concurrent Query Support

The backend SHALL support concurrent query execution with proper isolation.

#### Scenario: Concurrent queries
- GIVEN a running ProcessBackend
- WHEN multiple goroutines execute queries simultaneously
- THEN each query receives its correct results without mixing

#### Scenario: Query timeout
- GIVEN a running ProcessBackend
- WHEN a query exceeds the configured timeout
- THEN the query is cancelled and a timeout error is returned

### Requirement: Backend Interface Compliance

The ProcessBackend SHALL implement the Backend interface defined in project-foundation.

#### Scenario: Interface satisfaction
- GIVEN the ProcessBackend type
- WHEN checking interface compliance
- THEN ProcessBackend satisfies the Backend interface
