## ADDED Requirements

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
