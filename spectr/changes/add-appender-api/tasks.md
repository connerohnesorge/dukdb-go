## 1. Appender Core

- [ ] 1.1 Implement Appender struct
  - Fields: conn *Conn, catalog string, schema string, table string
  - Fields: columns []string, colTypes []Type, buffer [][]any
  - Fields: threshold int (default 1024), closed bool, mu sync.Mutex
  - **Acceptance:** Struct compiles with all required fields

- [ ] 1.2 Implement NewAppenderFromConn(conn *Conn, schema, table string) (*Appender, error)
  - Use empty string for catalog (defaults to current database)
  - If schema is empty, use "main"
  - Query table metadata using duckdb_columns():
    ```sql
    SELECT column_name, data_type
    FROM duckdb_columns()
    WHERE database_name = 'memory' AND schema_name = '<schema>' AND table_name = '<table>'
    ORDER BY column_index
    ```
  - Populate columns and colTypes from query results
  - Return `&Error{Type: ErrorTypeCatalog, Msg: "table '<schema>.<table>' not found"}` if 0 rows
  - **Acceptance:** Returns Appender with correct column info or ErrorTypeCatalog

- [ ] 1.3 Implement NewAppender(conn *Conn, catalog, schema, table string) (*Appender, error)
  - Support full catalog.schema.table path
  - Empty catalog uses current database (detected from connection)
  - Empty schema uses "main"
  - Table name must be non-empty (return ErrorTypeInvalid if empty)
  - **Acceptance:** Works with fully-qualified table names

- [ ] 1.4 Implement NewAppenderWithThreshold(conn *Conn, schema, table string, threshold int) (*Appender, error)
  - Validate threshold >= 1, return `&Error{Type: ErrorTypeInvalid, Msg: "threshold must be >= 1"}` if < 1
  - Otherwise same as NewAppenderFromConn
  - **Acceptance:** Threshold=0 returns error, threshold=1 works

## 2. Row Appending

- [ ] 2.1 Implement AppendRow(values ...any) error
  - Lock mutex
  - Check if closed → return `&Error{Type: ErrorTypeClosed, Msg: "appender is closed"}`
  - Validate len(values) == len(columns) → return `&Error{Type: ErrorTypeInvalid, Msg: "expected N columns, got M"}` if mismatch
  - If len(buffer) >= threshold, call flushLocked() and propagate any error
  - Append values to buffer (copy slice to avoid aliasing)
  - Unlock mutex
  - **Acceptance:** Row added to buffer, auto-flush works, errors propagated

- [ ] 2.2 Implement type validation (deferred to Flush)
  - Type checking happens when INSERT is executed by DuckDB
  - FormatValue (from add-query-execution) converts Go types to SQL literals
  - Invalid types (e.g., Inf/NaN for floats) cause FormatValue error at Flush time
  - **Acceptance:** Invalid types cause error at Flush, not AppendRow

- [ ] 2.3 Implement NULL handling
  - nil value → stored as nil in buffer
  - FormatValue converts nil → "NULL" string
  - DuckDB validates NOT NULL constraints at INSERT time
  - NOT NULL violation returns ErrorTypeConstraint
  - **Acceptance:** NULL values append correctly, NOT NULL violations detected at Flush

## 3. Flush and Close

- [ ] 3.1 Implement Flush() error
  - Lock mutex
  - If closed → return `&Error{Type: ErrorTypeClosed, Msg: "appender is closed"}`
  - If buffer empty → return nil (no-op)
  - Build INSERT statement:
    ```sql
    INSERT INTO <qualified_name> (<columns>) VALUES (<row1>), (<row2>), ...
    ```
  - Format each value using FormatValue from add-query-execution
  - Execute via conn.ExecContext(context.Background(), insert)
  - On success: clear buffer (`buffer = buffer[:0]`), return nil
  - On error: preserve buffer for retry, return error
  - Unlock mutex
  - **Acceptance:** Data persisted, buffer cleared on success, preserved on error

- [ ] 3.2 Implement Close() error
  - Lock mutex
  - If already closed → return `&Error{Type: ErrorTypeClosed, Msg: "appender already closed"}`
  - Call flushLocked() for remaining rows
  - Set closed = true (regardless of flush result)
  - Unlock mutex
  - If flush failed → return flush error
  - Otherwise return nil
  - **Acceptance:** Remaining data flushed, appender marked closed

- [ ] 3.3 Implement threshold configuration
  - Default threshold: 1024 rows
  - Use NewAppenderWithThreshold for custom threshold
  - Threshold must be >= 1
  - **Acceptance:** Custom thresholds work, threshold < 1 rejected

## 4. Error Handling

- [ ] 4.1 Implement table not found error
  - NewAppender for non-existent table → ErrorTypeCatalog
  - Error message: "table '<schema>.<table>' not found"
  - **Acceptance:** Clear error for missing table

- [ ] 4.2 Implement column count mismatch error
  - AppendRow with wrong value count → ErrorTypeInvalid
  - Error message: "expected N columns, got M" where N and M are actual counts
  - **Acceptance:** Clear error for wrong column count

- [ ] 4.3 Implement append after close error
  - AppendRow on closed appender → ErrorTypeClosed
  - Error message: "appender is closed"
  - **Acceptance:** Clear error for closed appender

- [ ] 4.4 Implement flush error preservation
  - If Flush fails (e.g., constraint violation)
  - Buffer is preserved for potential retry
  - Buffer accessible via internal state (no public accessor needed)
  - Next successful Flush clears buffer
  - **Acceptance:** Buffer preserved after failed flush, cleared after success

- [ ] 4.5 Implement NOT NULL violation handling
  - nil value for NOT NULL column
  - Flush executes INSERT, DuckDB returns error
  - Error type: ErrorTypeConstraint
  - **Acceptance:** NOT NULL violations detected at Flush time

## 5. Testing

- [ ] 5.1 Basic append and flush tests
  - Create table with 2 columns (id INT, name VARCHAR)
  - Append 10 rows, flush, verify with SELECT COUNT(*)
  - Verify SELECT * returns correct data
  - **Acceptance:** Data correctly inserted, verified by query

- [ ] 5.2 Auto-flush threshold tests
  - Create appender with threshold=5
  - Append 7 rows
  - After 5th row: auto-flush triggered, verify 5 rows in table
  - After 7th row: 2 rows in buffer
  - Close: remaining 2 rows flushed, total 7 rows
  - **Acceptance:** Auto-flush at correct threshold, correct row counts

- [ ] 5.3 Type conversion tests
  - Test all primitive types: bool, int, float, string
  - Test custom types: UUID, Interval, Decimal
  - Test NULL values with nil
  - Test complex types: []any, map[string]any
  - **Acceptance:** All types converted correctly by FormatValue

- [ ] 5.4 Error handling tests
  - TestTableNotFound: verify ErrorTypeCatalog
  - TestColumnCountMismatch: verify ErrorTypeInvalid with "expected N, got M"
  - TestAppendAfterClose: verify ErrorTypeClosed
  - TestNotNullViolation: verify ErrorTypeConstraint at Flush
  - TestThresholdZero: verify ErrorTypeInvalid
  - **Acceptance:** All error cases return correct types and messages

- [ ] 5.5 Concurrent access tests
  - Create appender with threshold=100
  - 10 goroutines each append 50 rows
  - Verify total 500 rows in table after Close
  - go test -race passes
  - **Acceptance:** Thread-safe operation, no race conditions
