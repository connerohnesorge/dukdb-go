# Change: Improve Appender Performance with DataChunk-Based Flushing

## Why

The current dukdb-go Appender implementation (`appender.go` lines 24-43) uses **row-based buffering** with SQL INSERT statement generation:
- Buffers rows as `buffer [][]any` (line 31)
- Generates multi-row INSERT SQL via `buildInsert()` (lines 637-671)
- Executes through standard query path via `ExecContext()` (line 541)
- Threshold-based auto-flush (default 1024 rows, line 15)

The reference duckdb-go implementation (`duckdb-go/appender.go`) uses **columnar DataChunk buffering**:
- Buffers rows in `chunk DataChunk` (line 21)
- Fills chunk via `chunk.SetValue(i, rowCount, val)` (lines 327-336)
- Auto-flushes at chunk capacity (2048 rows, checked via `GetDataChunkCapacity()` at line 320)
- Flushes via CGO call `mapping.AppendDataChunk(appender, chunk.chunk)` (line 350)
- Bypasses SQL parser/planner entirely

This change replaces dukdb-go's INSERT-based approach with DataChunk-based buffering to improve performance while maintaining pure Go implementation (no CGO).

## What Changes

### Core Modifications

1. **Appender Struct** (`appender.go` lines 24-43)
   - REMOVE: `buffer [][]any` (line 31)
   - ADD: `currentChunk DataChunk` for columnar buffering
   - ADD: `currentSize int` to track rows in current chunk
   - KEEP: `mu sync.Mutex` for thread safety (line 34)
   - KEEP: Query appender fields (lines 36-42)

2. **AppendRow Method** (`appender.go` lines 472-509)
   - REPLACE: `a.buffer = append(a.buffer, args)` logic
   - WITH: `a.currentChunk.SetValue(colIdx, a.currentSize, val)` calls
   - Auto-flush when `a.currentSize == VectorSize` (2048 rows)

3. **Flush Method** (`appender.go` lines 533-599)
   - REMOVE: `buildInsert()` SQL generation (lines 637-671)
   - REMOVE: `ExecContext()` execution (line 541)
   - ADD: Direct storage layer access for chunk writes
   - KEEP: Separate logic for table vs query appenders

### Implementation Approach

**Pure Go Storage Access** (No Backend Interface Change):

The duckdb-go reference uses CGO to call C++ DuckDB directly:
```go
// duckdb-go/appender.go:350
mapping.AppendDataChunk(a.appender, a.chunk.chunk)
```

Our pure Go approach accesses storage directly:
```go
// appender.go (revised)
func (a *Appender) flush() error {
    if a.currentSize == 0 {
        return nil
    }

    a.currentChunk.SetSize(a.currentSize)

    // Direct storage layer access (no Backend interface)
    if err := a.conn.appendChunkToTable(
        a.catalog,
        a.schema,
        a.table,
        a.currentChunk,
    ); err != nil {
        return err
    }

    // Reset for reuse
    var err error
    a.currentChunk, err = NewDataChunk(a.columnTypes)
    if err != nil {
        return err
    }
    a.currentSize = 0
    return nil
}
```

**New Connection Method**:
```go
// conn.go
func (c *Conn) appendChunkToTable(catalog, schema, table string, chunk DataChunk) error {
    // Get table from catalog
    tbl := c.backend.GetTable(catalog, schema, table)

    // Write chunk to storage
    // Implementation depends on storage layer design
    return c.backend.AppendToStorage(tbl, chunk)
}
```

## Impact

- **Affected specs**: appender-api (MODIFIED), deterministic-testing (MODIFIED)
- **Affected code**:
  - `appender.go` (refactor ~300 lines for DataChunk buffering)
  - `conn.go` (add appendChunkToTable helper ~20 lines)
  - Storage layer integration (depends on internal architecture)
- **Dependencies**:
  - `data_chunk.go` ✅ (complete, matches duckdb-go API)
  - `vector.go` ✅ (complete, all type setters working)
  - Storage layer ⚠️ (needs chunk write capability)
- **Performance target**: To be measured via benchmarks (remove unverified "10-100x" claim)
- **API compatibility**: Maintained (signatures unchanged)

## Breaking Changes

None. Public API remains identical:
- `NewAppender(conn *Conn, catalog, schema, table string) (*Appender, error)` - unchanged
- `NewAppenderFromConn(conn *Conn, schema, table string) (*Appender, error)` - unchanged
- `NewAppenderWithThreshold(...)` - signature unchanged, threshold still applies
- `NewQueryAppender(...)` - unchanged, keeps SQL-based approach (no DataChunk optimization)
- `AppendRow(...driver.Value) error` - unchanged
- `Flush()`, `Close()` - unchanged
- `FlushWithContext(AppenderContext)` - unchanged (deterministic testing support maintained)

Internal implementation changes only.

## Notes on Reference Implementation

**duckdb-go Architecture** (for reference):
- Uses CGO to access DuckDB C++ appender API
- DataChunk wrapper around C memory: `mapping.DataChunk`
- Flush calls: `mapping.AppendDataChunk(appender, chunk)` → C++ storage
- Chunk capacity checked via `GetDataChunkCapacity()` (returns 2048)

**Unsupported Types** (per duckdb-go/type.go:52-58):
- TYPE_INVALID, TYPE_UHUGEINT, TYPE_BIT, TYPE_ANY, TYPE_BIGNUM
- dukdb-go should match this behavior

**VectorSize Constant**:
- duckdb-go uses `GetDataChunkCapacity()` which returns the chunk's internal capacity (2048)
- dukdb-go will use `chunk.Capacity()` method or hardcode `const VectorSize = 2048` (pure Go)

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`:

**Existing Support** (appender.go lines 725-870):
- `AppenderContext` with quartz.Clock integration ✅
- `FlushWithContext(ctx AppenderContext)` for deterministic flush timing ✅
- Deadline checking with mock clocks ✅

**Additions Needed**:
- Tag DataChunk operations: `mClock.Now("Appender", "flush", "start")`
- Trap-based concurrent append tests
- Transaction rollback tests with deterministic timing

See delta spec for detailed test scenarios.
