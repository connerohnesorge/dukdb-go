# Change: Add Table-Valued User-Defined Functions (Table UDFs)

## Why

Users need the ability to create custom table functions that return multiple rows, enabling:
- Dynamic data sources (read from external APIs, files, custom formats)
- Parameterized virtual tables
- Custom data generation
- Integration with external data systems

The duckdb-go reference supports four variants:
1. **RowTableFunction** - Sequential row-based execution
2. **ParallelRowTableFunction** - Parallel row-based with thread-local state
3. **ChunkTableFunction** - Sequential vectorized (DataChunk)
4. **ParallelChunkTableFunction** - Parallel vectorized with thread-local state

## What Changes

### Core Types

1. **ColumnInfo**: Column metadata (name and TypeInfo)
2. **CardinalityInfo**: Row count estimate with exactness flag
3. **TableFunctionConfig**: Arguments and named arguments
4. **ParallelTableSourceInfo**: Thread configuration (MaxThreads)

### Table Source Interfaces

```go
// Row-based table source
type RowTableSource interface {
    ColumnInfos() []ColumnInfo
    Cardinality() *CardinalityInfo  // Returns nil if cardinality unknown
    Init()
    FillRow(Row) (bool, error) // false = no more rows
}

// Parallel row-based with thread-local state
type ParallelRowTableSource interface {
    ColumnInfos() []ColumnInfo
    Cardinality() *CardinalityInfo
    Init() ParallelTableSourceInfo
    NewLocalState() any
    FillRow(localState any, row Row) (bool, error)  // localState is FIRST parameter
    MaxThreads() int
}

// Chunk-based (vectorized)
type ChunkTableSource interface {
    ColumnInfos() []ColumnInfo
    Cardinality() *CardinalityInfo
    Init()
    FillChunk(chunk DataChunk) error  // DataChunk passed by value (not pointer)
}

// Parallel chunk-based with thread-local state
type ParallelChunkTableSource interface {
    ColumnInfos() []ColumnInfo
    Cardinality() *CardinalityInfo
    Init() ParallelTableSourceInfo
    NewLocalState() any
    FillChunk(localState any, chunk DataChunk) error  // localState is FIRST parameter
    MaxThreads() int
}
```

### Table Function Types

```go
type RowTableFunction struct {
    Config TableFunctionConfig
    BindArguments func(named map[string]any, args ...any) (RowTableSource, error)
    BindArgumentsContext func(ctx context.Context, named map[string]any, args ...any) (RowTableSource, error)
}

// Similar for ParallelRowTableFunction, ChunkTableFunction, ParallelChunkTableFunction
```

### Public API

```go
func RegisterTableUDF[T TableFunction](c *sql.Conn, name string, f T) error
```

## Impact

- **Affected specs**: Depends on data-chunk-api, **deterministic-testing**
- **Affected code**: New file `table_udf.go`, `table_source.go`
- **Dependencies**: data-chunk-api must be implemented first; quartz.Clock for streaming timeouts
- **Consumers**: Users creating custom data sources

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`, Table UDF execution must support clock injection:

```go
// TableFunctionContext provides deterministic timing
type TableFunctionContext struct {
    ctx   context.Context
    clock quartz.Clock  // For timeout checking in streaming
}

// Parallel execution uses clock for worker coordination
func (p *ParallelExecutor) runWorkers(clock quartz.Clock) {
    ticker := clock.TickerFunc(ctx, 100*time.Millisecond, func() error {
        return p.checkProgress()
    })
    defer ticker.Stop()
}

// Tests use mock clock for deterministic parallel behavior
func TestParallelTableUDF(t *testing.T) {
    mClock := quartz.NewMock(t)

    // Advance worker ticks deterministically
    for i := 0; i < 10; i++ {
        mClock.Advance(100*time.Millisecond).MustWait()
    }
}
```

**Zero Flaky Tests Policy**: No `time.Sleep` or `runtime.Gosched` for synchronization. Use `quartz.Mock` traps for async coordination.

## Breaking Changes

None. This adds new functionality without modifying existing APIs.

## Implementation Approach

Unlike CGO which uses C callbacks for streaming data, the pure Go implementation will:
1. Register table functions in a connection-scoped registry
2. Create custom logical operators for table UDF calls
3. Execute the table source directly in the executor
4. Support parallelism through goroutine pools with worker coordination
