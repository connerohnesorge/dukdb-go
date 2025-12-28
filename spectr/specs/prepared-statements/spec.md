# Prepared Statements Specification

## Requirements

### Requirement: Statement Preparation

The package SHALL prepare SQL statements for repeated execution.

#### Scenario: Prepare valid statement
- GIVEN valid SQL query "SELECT $1 + $2"
- WHEN calling Prepare(query)
- THEN *PreparedStmt is returned without error

#### Scenario: Prepare with zero parameters
- GIVEN SQL query "SELECT 1"
- WHEN calling Prepare(query)
- THEN NumInput() returns 0

#### Scenario: NumInput for positional parameters
- GIVEN prepared statement "SELECT $1, $2, $3"
- WHEN calling NumInput()
- THEN returns 3

#### Scenario: NumInput for non-sequential positional
- GIVEN prepared statement "SELECT $1, $3" (gap in sequence)
- WHEN calling NumInput()
- THEN returns 3 (max placeholder number)

#### Scenario: NumInput for named parameters
- GIVEN prepared statement "SELECT @foo, @bar"
- WHEN calling NumInput()
- THEN returns 2

#### Scenario: NumInput for repeated named parameter
- GIVEN prepared statement "SELECT @foo, @foo" (same name twice)
- WHEN calling NumInput()
- THEN returns 1 (unique names only)

### Requirement: Statement Execution

The package SHALL execute prepared statements with provided arguments.

#### Scenario: Execute with correct parameters
- GIVEN prepared statement "INSERT INTO t VALUES ($1, $2)"
- WHEN calling ExecContext with []driver.NamedValue of length 2
- THEN execution succeeds

#### Scenario: Execute with too few parameters
- GIVEN prepared statement "SELECT $1, $2"
- WHEN calling ExecContext with 1 argument
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 2 parameters, got 1"

#### Scenario: Execute with too many parameters
- GIVEN prepared statement "SELECT $1"
- WHEN calling ExecContext with 2 arguments
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 1 parameters, got 2"

#### Scenario: Multiple executions same statement
- GIVEN prepared statement "SELECT $1 * 2"
- WHEN executing 100 times with values 1 through 100
- THEN each execution returns correct result (2, 4, 6, ... 200)

#### Scenario: Query returns Rows
- GIVEN prepared statement "SELECT * FROM t WHERE id = $1"
- WHEN calling QueryContext with argument 42
- THEN driver.Rows is returned for iteration

#### Scenario: Concurrent execution
- GIVEN prepared statement
- WHEN 10 goroutines execute simultaneously
- THEN all executions complete without race conditions

### Requirement: Statement Lifecycle

The package SHALL manage statement lifecycle correctly.

#### Scenario: Close statement
- GIVEN an open prepared statement
- WHEN Close() is called
- THEN returns nil

#### Scenario: Execute after close
- GIVEN a closed prepared statement
- WHEN ExecContext is called
- THEN error of type ErrorTypeClosed is returned
- AND error message is "statement is closed"

#### Scenario: Query after close
- GIVEN a closed prepared statement
- WHEN QueryContext is called
- THEN error of type ErrorTypeClosed is returned

#### Scenario: Double close is idempotent
- GIVEN a prepared statement
- WHEN Close() is called twice
- THEN both calls return nil

#### Scenario: NumInput after close
- GIVEN a closed prepared statement
- WHEN NumInput() is called
- THEN returns original parameter count (still accessible)

### Requirement: Connection Association

The package SHALL associate statements with their connection.

#### Scenario: Statement uses connection timeout
- GIVEN connection with 5s timeout
- AND prepared statement on that connection
- WHEN executing query that takes 10s
- THEN timeout error occurs at ~5s

#### Scenario: Execute on closed connection
- GIVEN prepared statement on a connection
- WHEN connection is closed
- AND statement execution is attempted
- THEN error of type ErrorTypeConnection is returned
- AND error message indicates connection is closed

