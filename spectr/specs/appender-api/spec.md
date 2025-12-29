# Appender Api Specification

## Requirements

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

The package SHALL append rows using DataChunk buffering for high performance.

#### Scenario: Append valid row with DataChunk
- GIVEN appender for table (id INT, name VARCHAR)
- WHEN calling AppendRow(1, "Alice")
- THEN row is added to internal DataChunk buffer via SetValue()
- AND currentChunk.SetValue(0, currentSize, 1) is called for id
- AND currentChunk.SetValue(1, currentSize, "Alice") is called for name
- AND currentSize is incremented
- AND no error is returned

#### Scenario: Append with wrong column count - too few
- GIVEN appender for table with 3 columns
- WHEN calling AppendRow with 2 values
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 3 columns, got 2"
- AND DataChunk is not modified

#### Scenario: Append with wrong column count - too many
- GIVEN appender for table with 2 columns
- WHEN calling AppendRow with 3 values
- THEN error of type ErrorTypeInvalid is returned
- AND error message is "expected 2 columns, got 3"
- AND DataChunk is not modified

#### Scenario: Append NULL value
- GIVEN appender for nullable column
- WHEN calling AppendRow with nil
- THEN NULL is set in DataChunk validity bitmap
- AND chunk.SetValue handles nil correctly

#### Scenario: Append after close
- GIVEN closed appender
- WHEN calling AppendRow
- THEN error of type ErrorTypeClosed is returned
- AND error message is "appender is closed"

### Requirement: Auto-Flush

The package SHALL auto-flush when DataChunk reaches VectorSize capacity (2048 rows) or user-configured threshold.

#### Scenario: Auto-flush at VectorSize
- GIVEN appender with currentSize = 2047
- WHEN AppendRow is called for 2048th row
- THEN DataChunk is flushed via Conn.appendChunkToTable()
- AND new DataChunk is allocated
- AND currentSize is reset to 0
- AND 2048th row is added to new chunk

#### Scenario: Manual flush respects threshold
- GIVEN appender with threshold = 1024
- WHEN 1024 rows are appended
- THEN user can call Flush() manually
- AND partial chunk (1024 rows) is flushed
- AND new chunk starts at row 0

#### Scenario: Auto-flush at min(threshold, VectorSize)
- GIVEN appender with threshold = 3000 (> VectorSize)
- WHEN 2048 rows are appended
- THEN auto-flush occurs at 2048 (VectorSize takes precedence)

#### Scenario: Manual flush with partial chunk
- GIVEN appender with currentSize = 100
- WHEN Flush() is called manually
- THEN chunk.SetSize(100) is called
- AND Conn.appendChunkToTable() receives 100-row chunk
- AND new chunk is allocated

### Requirement: Flush Operation

The package SHALL flush buffered rows via direct storage writes.

#### Scenario: Flush persists DataChunk
- GIVEN appender with 2048 buffered rows in DataChunk
- WHEN Flush() is called
- THEN chunk.SetSize(2048) is called
- AND conn.appendChunkToTable(catalog, schema, table, chunk) is called
- AND storage layer writes chunk to row groups
- AND currentChunk is reset to new DataChunk
- AND currentSize is set to 0

#### Scenario: Flush with empty buffer
- GIVEN appender with currentSize = 0
- WHEN Flush() is called
- THEN no storage write occurs
- AND no error occurs

#### Scenario: Flush error preserves buffer
- GIVEN appender with invalid data causing storage error
- WHEN Flush() fails
- THEN error is returned
- AND currentChunk is preserved (not reset)
- AND currentSize unchanged
- AND subsequent successful Flush clears buffer

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

### Requirement: Performance Characteristics

The package SHALL demonstrate measurable performance improvement over SQL-based appending.

#### Scenario: Benchmark INSERT vs DataChunk approach
- GIVEN table (id INT, value VARCHAR)
- WHEN running benchmark with 1,000,000 rows
- THEN DataChunk approach throughput is measured
- AND SQL INSERT approach throughput is measured
- AND improvement factor is calculated and documented
- AND zero time.Sleep calls in benchmark

#### Scenario: DataChunk reuse efficiency
- GIVEN appender flushing 10 times
- WHEN measuring allocations with testing.B.ReportAllocs()
- THEN DataChunk is reused (not reallocated each flush)
- AND allocation count is minimal (<5 per 10 flushes)

#### Scenario: Type conversion overhead
- GIVEN columns of various types (primitives, strings, complex)
- WHEN appending 100,000 mixed-type rows
- THEN chunk.SetValue() completes efficiently
- AND no expensive reflection for primitive types
- AND performance is comparable to duckdb-go CGO version (within 2-5x)

### Requirement: Storage Layer Integration

The package SHALL integrate with storage layer for direct chunk writes.

#### Scenario: Connection provides storage access
- GIVEN appender with full 2048-row chunk
- WHEN auto-flush triggers
- THEN conn.appendChunkToTable(catalog, schema, table, chunk) is called
- AND connection accesses storage layer internally
- AND NOT via Backend interface method

#### Scenario: Chunk written to row group
- GIVEN empty table
- WHEN appending 2048-row chunk
- THEN storage layer creates or reuses row group
- AND chunk data appended to row group columns
- AND row count updated

#### Scenario: Storage validation failure
- GIVEN chunk with schema mismatch (wrong column types)
- WHEN conn.appendChunkToTable() validates
- THEN error is returned
- AND no data is written to storage
- AND appender buffer is preserved

### Requirement: Transaction Integration

The package SHALL support transactional appends with LocalStorage pattern.

#### Scenario: Appended chunks in transaction-local storage
- GIVEN appender within active transaction (txn ID = 123)
- WHEN chunks are flushed
- THEN storage uses LocalStorage for uncommitted data
- AND chunks are visible within transaction
- AND chunks are invisible outside transaction until commit

#### Scenario: Transaction commit merges to global storage
- GIVEN appender flushed 2 chunks within transaction
- WHEN transaction commits
- THEN LocalStorage row groups merge to table's global storage
- AND chunks become visible to other connections

#### Scenario: Transaction rollback discards local storage (deterministic)
- GIVEN appender that flushed 2 chunks within transaction
- WHEN transaction is rolled back
- THEN LocalStorage row groups are discarded (simplified cleanup, no optimistic writer)
- AND subsequent queries return zero rows
- AND test uses quartz.Mock for deterministic timing

#### Scenario: Simplified LocalStorage (pure Go)
- GIVEN pure Go implementation without CGO
- WHEN implementing LocalStorage pattern
- THEN use simplified approach (row groups only, no optimistic writes)
- AND defer index management to separate implementation
- AND use bulk merge commit path (DuckDB's optimized case)

### Requirement: Query Appender Support

The package SHALL preserve Query Appender functionality using existing SQL-based approach.

#### Scenario: Query appender unchanged
- GIVEN NewQueryAppender with MERGE query
- WHEN AppendRow() is called
- THEN rows are buffered in [][]any (existing behavior)
- AND flushQueryAppender() uses SQL INSERT to temp table (existing implementation)
- AND DataChunk optimization is NOT applied to query appenders

#### Scenario: Query appender executes with temp table
- GIVEN query appender with buffered data
- WHEN Flush() or Close() is called
- THEN buffered rows inserted to temp table via SQL INSERT statement
- AND query is executed referencing temp table
- AND temp table is dropped in Close() (not after each flush)

#### Scenario: Only table appenders use DataChunk
- GIVEN table appender (not query appender)
- WHEN AppendRow() is called
- THEN rows are buffered in DataChunk (NEW behavior)
- AND flush uses conn.appendChunkToTable() (NEW behavior)

### Requirement: Deterministic Testing Compliance

The package SHALL support deterministic testing per deterministic-testing spec.

#### Scenario: Concurrent appends with quartz traps
- GIVEN two appenders to same table
- WHEN both flush simultaneously using quartz.Mock
- THEN trap := mClock.Trap().Now("Appender", "flush") coordinates
- AND defer trap.Close() prevents trap leaks
- AND call1 := trap.MustWait(ctx) captures first flush
- AND call2 := trap.MustWait(ctx) captures second flush
- AND call1.MustRelease(ctx) allows first flush to proceed
- AND call2.MustRelease(ctx) allows second flush to proceed deterministically
- AND zero race conditions occur

#### Scenario: Existing FlushWithContext preserved
- GIVEN appender with AppenderContext containing quartz.Clock
- WHEN FlushWithContext(ctx) is called
- THEN deadline checking uses ctx.Clock.Until()
- AND deterministic timeout behavior is maintained
- AND existing tests continue to pass

#### Scenario: Zero time.Sleep in tests
- GIVEN all appender tests
- WHEN searching for time.Sleep
- THEN zero occurrences found
- AND all timing uses quartz.Clock

#### Scenario: All timing operations tagged
- GIVEN appender flush operations
- WHEN using quartz clock
- THEN startTime := mClock.Now("Appender", "flush", "start") tags operation start
- AND duration := mClock.Since(startTime, "Appender", "flush", "end") measures duration
- AND tags enable precise trap filtering
