## Context

Apache Arrow is a cross-language columnar memory format. Integration with Arrow enables zero-copy data exchange with analytics tools.

**Stakeholders**: Data scientists, analytics pipeline developers

**Constraints**:
- Heavy dependency (apache/arrow-go is large)
- Must be opt-in via build tag
- Type mapping must be complete and correct
- Must support streaming for large results

## Goals / Non-Goals

### Goals
- Arrow record reader for query results
- Complete type mapping DuckDB <-> Arrow
- Streaming support for large datasets
- API compatibility with duckdb-go

### Non-Goals
- Arrow writes (INSERT from Arrow)
- Arrow flight RPC
- Arrow compute functions

## Decisions

### Decision 1: Build Tag Isolation

**What**: Isolate Arrow code behind build tag

**Why**: Large dependency, not everyone needs it

**Implementation**:
```go
//go:build duckdb_arrow

package duckdb

import "github.com/apache/arrow-go/v18/arrow"
```

### Decision 2: DataChunk to Arrow Conversion

**What**: Convert DataChunks to Arrow RecordBatches

**Why**: Efficient batch conversion leveraging columnar layout

**Implementation**:
```go
func dataChunkToRecordBatch(chunk *DataChunk, schema *arrow.Schema) arrow.Record {
    builders := make([]array.Builder, len(schema.Fields()))
    for i, field := range schema.Fields() {
        builders[i] = array.NewBuilder(memory.DefaultAllocator, field.Type)
    }
    for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
        for colIdx := range builders {
            val, _ := chunk.GetValue(colIdx, rowIdx)
            appendToBuilder(builders[colIdx], val)
        }
    }
    arrays := make([]arrow.Array, len(builders))
    for i, b := range builders {
        arrays[i] = b.NewArray()
    }
    return array.NewRecord(schema, arrays, int64(chunk.GetSize()))
}
```

### Decision 3: Pull-Based Streaming Record Reader

**What**: Implement array.RecordReader with pull-based streaming

**Why**: Support large result sets without loading all into memory

**Implementation**:
```go
type recordReader struct {
    schema   *arrow.Schema
    res      mapping.Result     // DuckDB result handle
    current  arrow.Record       // Current record batch
    refCount int64              // Reference counting
    mu       sync.Mutex
}

func (r *recordReader) Schema() *arrow.Schema { return r.schema }

func (r *recordReader) Next() bool {
    r.mu.Lock()
    defer r.mu.Unlock()

    // Pull next chunk from DuckDB (not channels)
    chunk := mapping.FetchChunk(r.res)
    if chunk.Ptr == nil {
        return false  // No more data
    }

    // Convert chunk to Arrow record batch
    rec := dataChunkToRecordBatch(chunk, r.schema)
    r.current = rec
    return true
}

func (r *recordReader) Record() arrow.Record { return r.current }

// Reference counting for Arrow memory management
func (r *recordReader) Retain()  { atomic.AddInt64(&r.refCount, 1) }
func (r *recordReader) Release() { /* decrement and cleanup */ }
```

**Note**: Uses pull-based fetching via `mapping.FetchChunk()`, NOT Go channels. This matches the synchronous DuckDB chunk streaming model.

### Decision 4: Clock Injection for Temporal Conversions

**What**: Use injected quartz.Clock for temporal type conversions

**Why**:
- Per deterministic-testing spec, all time-dependent code must use injected clock
- Timezone-aware conversions may depend on current time offset
- Enables deterministic testing of temporal Arrow columns

**Implementation**:
```go
// ArrowContext provides clock for temporal conversions
type ArrowContext struct {
    clock quartz.Clock
}

func (a *Arrow) WithClock(clock quartz.Clock) *Arrow {
    return &Arrow{conn: a.conn, clock: clock}
}

func convertTimestampWithClock(
    builder *array.TimestampBuilder,
    chunk *DataChunk,
    colIdx int,
    clock quartz.Clock,
) {
    for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
        val, _ := chunk.GetValue(colIdx, rowIdx)
        if ts, ok := val.(time.Time); ok {
            // Use clock for any timezone-aware operations
            builder.Append(arrow.Timestamp(ts.UnixNano()))
        }
    }
}
```

## Risks / Trade-offs

### Risk 1: Dependency Size
**Risk**: Arrow library significantly increases binary size
**Mitigation**: Build tag makes it opt-in

### Risk 2: Type Mapping Errors
**Risk**: Incorrect type conversion produces wrong data
**Mitigation**: Comprehensive type mapping tests; reference duckdb-go mappings

## Migration Plan

New capability with no migration required.
