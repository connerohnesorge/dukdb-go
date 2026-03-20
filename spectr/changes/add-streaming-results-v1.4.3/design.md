## Implementation Details

### Current Result Delivery Path

The current flow materializes all rows before returning:

```
SQL query
  -> parser.Parse()
  -> binder.Bind()
  -> planner.Plan()
  -> executor.Execute()          // returns *ExecutionResult
     -> collectResults()         // loops over all DataChunks, converts to []map[string]any
  -> EngineConn.Query()          // returns ([]map[string]any, []string, error)
  -> Conn.QueryContext()         // wraps in `rows{data: data}`, returns driver.Rows
  -> database/sql iterates rows  // calls rows.Next() which indexes into data slice
```

Key bottleneck: `collectResults()` in `operator.go` (line ~497) reads every
chunk from the source function and converts all rows to `map[string]any`
before returning. The `EngineConn.Query()` method in `conn.go` (line ~993)
receives the fully materialized `result.Rows` and passes it through.

### Streaming Iterator Interface

Note: `StreamingResult` lives in the root `dukdb` package (`streaming.go`).
Because the root package does not currently import `internal/storage`, the
chunk iterator uses `any` for the opaque chunk reference. Internally the
closure captures the typed `*storage.DataChunk` and performs columnar reads,
but the public struct avoids a direct dependency on the internal package.

```go
// StreamingResult delivers query results one DataChunk at a time.
// The consumer calls NextChunk() to pull the next batch.
// Close() MUST be called to release executor resources.
type StreamingResult struct {
    columns   []string
    colTypes  []Type
    scanNext  func(dest []driver.Value) error // row-at-a-time producer
    closed    bool
    mu        sync.Mutex
    ctx       context.Context
    cancel    context.CancelFunc
}
```

### Chunked Result Delivery

Instead of materializing all rows, the executor builds the operator pipeline
and returns a `StreamingResult` whose `scanNext` closure reads one row at a
time from the pipeline's root operator:

```go
// In executor: new method alongside Execute()
// Returns a *dukdb.StreamingResult (root package type).
func (e *Executor) ExecuteStreaming(
    ctx context.Context,
    plan planner.PhysicalPlan,
    args []driver.NamedValue,
) (*dukdb.StreamingResult, error) {
    // Build the operator pipeline but do NOT drain it.
    // Construct a scanNext closure that:
    //   1. Calls operator.Next() to get the next DataChunk
    //   2. Reads one row at a time from the chunk
    //   3. Converts columnar values to driver.Value
    // Return a StreamingResult wrapping that closure.
}
```

### Integration with database/sql Rows Interface

The `rows` struct in `conn.go` (line ~363) gains a streaming mode:

```go
type rows struct {
    // Existing materialized path (kept for backward compatibility)
    columns []string
    data    []map[string]any
    pos     int

    // Streaming path (used when streamResult is non-nil)
    streamResult *StreamingResult
}

func (r *rows) Next(dest []driver.Value) error {
    if r.streamResult != nil {
        return r.streamResult.ScanNext(dest) // delegates to the scanNext closure
    }
    // ... existing materialized path (indexes into r.data) ...
}
```

### Backpressure Model

Backpressure is implicit: the executor pipeline is pull-based. The
`PhysicalOperator.Next()` method on the root operator is only called when
`rows.Next()` requests the next row. Since `database/sql` calls `Next()` one
row at a time, the executor naturally pauses between chunks. No explicit
channel or semaphore is needed.

The only consideration is that some operators (Sort, HashAggregate, Window)
are blocking -- they must consume all input before producing output. These
operators already materialize internally; streaming benefits the non-blocking
operators (Scan, Filter, Project, Limit).

### Context Cancellation

The streaming result holds a derived context. When the parent context is
cancelled:

1. `StreamingResult.ScanNext()` checks `ctx.Err()` before calling the
   `scanNext` closure
2. Long-running scans in the executor check `ctx.Err()` between chunks
3. `StreamingResult.Close()` calls `cancel()` to propagate cancellation
   downstream

### BackendConn Interface Extension

```go
// BackendConn gains an optional streaming query interface.
// Implementations that do not support streaming can return
// (nil, ErrStreamingNotSupported) and the driver falls back
// to the materialized Query() path.
type BackendConnStreaming interface {
    QueryStreaming(
        ctx context.Context,
        query string,
        args []driver.NamedValue,
    ) (*StreamingResult, error)
}
```

The `Conn.QueryContext()` method checks if the backend implements
`BackendConnStreaming` and prefers the streaming path when available.

### Data Structures

- `StreamingResult` (root `dukdb` package) -- holds the `scanNext` closure,
  column metadata, and closed state. Lives for the duration of a
  `driver.Rows` lifetime. Does not directly reference `internal/storage`
  types; the closure captures `*storage.DataChunk` and row cursor state
  internally.
- `storage.DataChunk` -- already exists (2048-row columnar batch). Reused
  as-is for streaming; no copies needed. Accessed only inside the closure
  constructed by `ExecuteStreaming()`.

### File and Directory Structure

New files:
- `streaming.go` (root `dukdb` package) -- `StreamingResult` type with
  `ScanNext(dest []driver.Value) error` and `Close() error` methods. No
  dependency on `internal/storage`; the chunk iteration logic lives inside
  the closure provided by the executor.
- `internal/executor/streaming.go` -- `ExecuteStreaming()` method on
  `Executor`. Builds the operator pipeline and constructs the `scanNext`
  closure that captures `*storage.DataChunk` state internally. Returns
  `*dukdb.StreamingResult`.

Modified files:
- `backend.go` -- `BackendConnStreaming` interface (returns `*StreamingResult`)
- `conn.go` -- `rows` struct (line ~363) gains streaming mode, `QueryContext`
  (line ~170) updated to check for `BackendConnStreaming`
- `internal/engine/conn.go` -- `EngineConn` (line ~87) implements
  `BackendConnStreaming` via new `QueryStreaming()` method
- `internal/executor/operator.go` -- refactor `collectResults` (line ~497)
  to extract the chunk-producing source closure so it can be shared between
  materialized and streaming paths

## Context

Large analytical queries can return millions of rows. The current approach
loads everything into `[]map[string]any` which is both slow (allocation per
cell) and memory-intensive (~200 bytes per cell in a map). Streaming avoids
this by passing columnar DataChunks directly to the driver.

## Goals / Non-Goals

Goals:
- Reduce peak memory for large result sets from O(rows) to O(chunk_size)
- Reduce latency-to-first-row for streaming queries
- Maintain full backward compatibility with existing materialized path
- Support context cancellation for streaming queries

Non-Goals:
- Parallel query execution (out of scope)
- Push-based streaming / async notification model
- Changing the DataChunk size (remains 2048)
- Streaming for DML statements (INSERT/UPDATE/DELETE stay materialized)

## Decisions

- **Pull-based model**: Consumer pulls chunks via `Next()` rather than
  producer pushing via channels. Rationale: simpler, no goroutine leaks,
  natural backpressure.
- **Optional interface**: `BackendConnStreaming` is a separate interface so
  existing backends compile without changes.
- **Dual-mode rows struct**: The `rows` type supports both materialized and
  streaming modes to allow incremental adoption.
- **No intermediate map conversion**: In streaming mode, values go directly
  from DataChunk columns to `driver.Value` without the `map[string]any`
  intermediate representation.

## Risks / Trade-offs

- **Blocking operators**: Sort, Aggregate, and Window functions must still
  materialize all input. Streaming only benefits the final delivery to the
  driver, not the operator pipeline itself. Mitigation: document which query
  patterns benefit from streaming.
- **Connection locking**: The `EngineConn.mu` mutex is held during `Query()`.
  For streaming, the lock must be released before returning the iterator, or
  the connection will be blocked until all rows are consumed. Mitigation: use
  a per-query lock or release the connection mutex after pipeline setup.
- **Error handling**: Errors during streaming (mid-iteration) are harder to
  handle than errors from a fully materialized result. Mitigation: errors
  are returned from `Next()` and the `rows` struct transitions to an error
  state.

## Open Questions

- Should `QueryStreaming` be the default for all SELECT queries, or opt-in
  via a connection setting?
- Should the public `Rows` type (rows.go) also support streaming, or only
  the internal `rows` struct in conn.go?
- What is the right behavior when `rows.Close()` is called before all chunks
  are consumed -- should it drain remaining chunks or just cancel?
