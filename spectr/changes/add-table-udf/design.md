## Context

Table-Valued UDFs allow users to create functions that return multiple rows, acting as virtual tables in SQL queries. DuckDB supports both row-based and chunk-based (vectorized) table functions, with optional parallelism.

**Stakeholders**: Users integrating external data sources, creating custom generators

**Constraints**:
- No CGO allowed
- Must support both row and chunk-based execution
- Must support parallel execution with thread-local state
- Column projection for efficiency

## Goals / Non-Goals

### Goals
- API-compatible table UDF registration matching duckdb-go
- Support all four function variants (Row, ParallelRow, Chunk, ParallelChunk)
- Column projection optimization
- Cardinality estimation for query optimizer
- Named and positional argument support

### Non-Goals
- Inter-thread communication between parallel workers
- Dynamic schema changes during execution
- Pushdown predicates (future optimization)

## Decisions

### Decision 1: Unified TableFunction Generic Type

**What**: Use Go generics to provide type-safe registration

**Why**:
- Single registration function works for all variants
- Compile-time type checking
- Matches duckdb-go API

**Implementation**:
```go
type TableFunction interface {
    RowTableFunction | ParallelRowTableFunction |
    ChunkTableFunction | ParallelChunkTableFunction
}

func RegisterTableUDF[T TableFunction](c *sql.Conn, name string, f T) error {
    // Type switch to handle each variant
}
```

### Decision 2: Parallel Execution Model

**What**: Use goroutine pool with work-stealing for parallel table functions

**Why**:
- Matches DuckDB's parallel execution model
- Efficient CPU utilization
- Thread-local state isolation

**Implementation**:
```go
type parallelTableExecutor struct {
    source   ParallelChunkTableSource
    workers  int
    results  chan *DataChunk
    errors   chan error
}

func (e *parallelTableExecutor) execute(ctx context.Context) error {
    var wg sync.WaitGroup
    for i := 0; i < e.workers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            localState := e.source.NewLocalState()
            chunk := newDataChunk(e.source.ColumnInfos())
            for {
                if err := e.source.FillChunk(chunk, localState); err != nil {
                    e.errors <- err
                    return
                }
                if chunk.GetSize() == 0 {
                    return // No more data
                }
                e.results <- chunk.copy()
                chunk.reset()
            }
        }(i)
    }
    wg.Wait()
    close(e.results)
    return nil
}
```

### Decision 3: Column Projection

**What**: Pass projection information to FillRow/FillChunk

**Why**:
- Avoid computing unused columns
- Significant performance improvement for wide tables

**Implementation**:
```go
type DataChunk struct {
    projection []int // -1 = not projected
}

// In FillRow, check IsProjected before setting values
func (source *mySource) FillRow(row Row) (bool, error) {
    if row.IsProjected(0) {
        row.SetRowValue(0, expensiveComputation())
    }
    return true, nil
}
```

### Decision 4: Bind Arguments with Context

**What**: Support both context-aware and context-free binding

**Why**:
- Context allows for timeout and cancellation
- Backward compatibility with simple bind functions

**Implementation**:
```go
type tableFunction[T tableSource] struct {
    Config               TableFunctionConfig
    BindArguments        func(named map[string]any, args ...any) (T, error)
    BindArgumentsContext func(ctx context.Context, named map[string]any, args ...any) (T, error)
}

// Prefer context version if available
func (f *tableFunction[T]) bind(ctx context.Context, named map[string]any, args ...any) (T, error) {
    if f.BindArgumentsContext != nil {
        return f.BindArgumentsContext(ctx, named, args...)
    }
    return f.BindArguments(named, args...)
}
```

## Risks / Trade-offs

### Risk 1: Goroutine Overhead for Small Tables
**Risk**: Parallel execution overhead exceeds benefit for small result sets
**Mitigation**: Use cardinality estimate to decide parallelism; fall back to sequential for small tables

### Risk 2: Memory Pressure from Parallel Chunks
**Risk**: Multiple workers producing chunks faster than consumer
**Mitigation**: Bounded channel for results; backpressure mechanism

### Risk 3: State Isolation Bugs
**Risk**: Users accidentally sharing state between workers
**Mitigation**: Clear documentation; NewLocalState called per worker; examples

## Migration Plan

New capability with no migration required.

**Rollout steps**:
1. Implement table source interfaces
2. Implement sequential row-based execution
3. Add chunk-based execution
4. Add parallel execution support
5. Integrate with query planner for FROM clause
6. Comprehensive testing with parallel workloads

## Open Questions

1. **Pushdown support**: Should we add predicate pushdown interface?
   - Deferred: Add FilterPushdown interface in future enhancement

2. **Progress reporting**: Should long-running table UDFs report progress?
   - Deferred: Not in initial implementation
