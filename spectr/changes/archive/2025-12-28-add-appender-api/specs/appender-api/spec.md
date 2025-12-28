## ADDED Requirements

### Requirement: Appender Creation

The package SHALL create appenders for bulk data loading.

#### Scenario: Create appender for existing table
- GIVEN existing table "users" with columns (id INT, name VARCHAR)
- WHEN calling NewAppenderFromConn(conn, "", "users")
- THEN Appender is returned without error
- AND Appender knows column names ["id", "name"]

#### Scenario: Create appender for non-existent table
- GIVEN no table "missing"
- WHEN calling NewAppenderFromConn(conn, "", "missing")
- THEN error of type ErrorTypeCatalog is returned
- AND error message is "table 'main.missing' not found"

#### Scenario: Create appender with schema
- GIVEN table "myschema.mytable"
- WHEN calling NewAppenderFromConn(conn, "myschema", "mytable")
- THEN Appender is returned for that table

#### Scenario: Create appender with full path
- GIVEN table in catalog.schema.table format
- WHEN calling NewAppender(conn, "mydb", "myschema", "mytable")
- THEN Appender is returned for that table

### Requirement: Row Appending

The package SHALL append rows to the buffer with type conversion.

#### Scenario: Append valid row
- GIVEN appender for table (id INT, name VARCHAR)
- WHEN calling AppendRow(1, "Alice")
- THEN row is added to buffer
- AND no error is returned

#### Scenario: Append with wrong column count - too few
- GIVEN appender for table with 3 columns
- WHEN calling AppendRow with 2 values
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 3 columns, got 2"

#### Scenario: Append with wrong column count - too many
- GIVEN appender for table with 2 columns
- WHEN calling AppendRow with 3 values
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 2 columns, got 3"

#### Scenario: Append NULL value
- GIVEN appender for nullable column
- WHEN calling AppendRow with nil
- THEN NULL is appended to buffer

#### Scenario: Append after close
- GIVEN closed appender
- WHEN calling AppendRow
- THEN error of type ErrorTypeClosed is returned
- AND error message is "appender is closed"

### Requirement: Auto-Flush

The package SHALL auto-flush when buffer reaches threshold.

#### Scenario: Auto-flush at default threshold
- GIVEN appender with default 1024-row threshold
- WHEN 1024 rows are appended
- THEN Flush is called automatically
- AND buffer is cleared

#### Scenario: Auto-flush at custom threshold
- GIVEN appender with threshold set to 100
- WHEN 100 rows are appended
- THEN auto-flush occurs before adding 101st row

#### Scenario: Partial buffer after auto-flush
- GIVEN appender with threshold 100
- WHEN 150 rows are appended
- THEN first 100 are flushed
- AND 50 remain in buffer

#### Scenario: Threshold of 1 (flush every row)
- GIVEN appender with threshold set to 1
- WHEN 5 rows are appended
- THEN each row triggers auto-flush immediately
- AND 5 separate INSERT statements are executed

#### Scenario: Threshold of 0 (invalid)
- GIVEN NewAppenderWithThreshold called with threshold=0
- WHEN constructor runs
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "threshold must be >= 1"

### Requirement: Flush Operation

The package SHALL flush buffered rows to the database.

#### Scenario: Flush persists data
- GIVEN appender with 10 buffered rows
- WHEN Flush() is called
- THEN rows are inserted into table
- AND buffer is cleared

#### Scenario: Flush with empty buffer
- GIVEN appender with no buffered rows
- WHEN Flush() is called
- THEN no error occurs
- AND no INSERT is executed

#### Scenario: Flush error preserves buffer
- GIVEN appender with invalid data (e.g., constraint violation)
- WHEN Flush() fails
- THEN error of type ErrorTypeConstraint is returned
- AND buffer is preserved for retry
- AND subsequent successful Flush clears buffer

#### Scenario: NOT NULL constraint violation at Flush
- GIVEN table with NOT NULL column
- WHEN appending row with nil for that column and calling Flush
- THEN error of type ErrorTypeConstraint is returned
- AND error message contains DuckDB's constraint error text

#### Scenario: Buffer atomicity on failure
- GIVEN appender with 10 buffered rows
- WHEN Flush fails due to constraint violation on row 5
- THEN all 10 rows remain in buffer (no partial insert)
- AND DuckDB INSERT is atomic (all-or-nothing)

#### Scenario: Flush after close
- GIVEN closed appender
- WHEN Flush() is called
- THEN error of type ErrorTypeClosed is returned

### Requirement: Close Operation

The package SHALL close appender and flush remaining data.

#### Scenario: Close flushes remaining
- GIVEN appender with 50 buffered rows
- WHEN Close() is called
- THEN remaining 50 rows are flushed
- AND appender is marked closed

#### Scenario: Double close
- GIVEN closed appender
- WHEN Close() is called again
- THEN error of type ErrorTypeClosed is returned
- AND error message is "appender already closed"

#### Scenario: Close with flush error
- GIVEN appender with invalid data
- WHEN Close() is called
- AND Flush fails
- THEN appender is still marked closed
- AND flush error is returned

### Requirement: Type Support

The package SHALL support all DuckDB types in appender.

#### Scenario: Append primitive types
- GIVEN columns of types BOOLEAN, INTEGER, DOUBLE, VARCHAR
- WHEN appending true, 42, 3.14, "hello"
- THEN all types are handled correctly

#### Scenario: Append temporal types
- GIVEN columns of types DATE, TIME, TIMESTAMP
- WHEN appending time.Time values
- THEN temporal types are converted correctly

#### Scenario: Append UUID type
- GIVEN column of type UUID
- WHEN appending UUID value
- THEN UUID is converted to string format

#### Scenario: Append Decimal type
- GIVEN column of type DECIMAL(10,2)
- WHEN appending Decimal{Scale: 2, Value: big.NewInt(12345)}
- THEN value "123.45" is inserted

#### Scenario: Append Interval type
- GIVEN column of type INTERVAL
- WHEN appending Interval{Months: 1, Days: 2, Micros: 0}
- THEN INTERVAL literal is inserted

#### Scenario: Append BLOB type
- GIVEN column of type BLOB
- WHEN appending []byte{0x48, 0x49}
- THEN hex literal X'4849' is inserted

### Requirement: Thread Safety

The package SHALL be safe for concurrent use via internal mutex serialization.

#### Scenario: Concurrent appends
- GIVEN single appender
- WHEN 10 goroutines each append 10 rows simultaneously
- THEN no data races occur (go test -race passes)
- AND all 100 rows are correctly buffered or flushed

#### Scenario: Concurrent append and flush
- GIVEN appender with active appends
- WHEN Flush is called from another goroutine
- THEN operations are serialized via mutex
- AND no interleaving of append/flush occurs

#### Scenario: Serialization guarantee
- GIVEN appender mutex locked by AppendRow
- WHEN another goroutine calls Flush
- THEN Flush blocks until AppendRow completes
- AND Flush sees consistent buffer state
