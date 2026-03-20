# Change: Add Streaming/Chunked Result Delivery

## Why

Currently, all query results are fully materialized in memory before being
returned to the caller. The `ExecutionResult.Rows` field is a
`[]map[string]any` that holds every row, and the `BackendConn.Query()` method
returns this entire slice at once. For large result sets (millions of rows,
wide tables, or OLAP aggregations), this causes excessive memory consumption
and high latency-to-first-row. A streaming/chunked delivery model allows the
database/sql driver to consume rows incrementally, keeping only one DataChunk
(up to 2048 rows) in memory at a time.

## What Changes

- Add a `StreamingResult` type that wraps a `PhysicalOperator` (or equivalent
  chunk-producing iterator) and delivers `DataChunk` batches on demand
- Add a streaming-aware `BackendConn.QueryStreaming()` method (or extend
  `Query()` with a streaming code path) that returns a `StreamingResult`
  instead of fully materializing rows
- Modify the `rows` struct in `conn.go` (the `driver.Rows` implementation)
  to pull chunks lazily from the `StreamingResult` rather than reading from a
  pre-populated `[]map[string]any`
- Add backpressure: the executor pauses chunk production when the consumer
  has not yet read the current chunk
- Ensure context cancellation propagates through the streaming pipeline so
  that long-running queries can be interrupted mid-stream

## Impact

- Affected specs: `result-handling`, `query-execution`, `database-driver`
- Affected code:
  - `internal/executor/operator.go` -- `ExecutionResult` type and `collectResults`
  - `internal/engine/conn.go` -- `EngineConn.Query()` method
  - `conn.go` -- `rows` struct and `QueryContext`
  - `backend.go` -- `BackendConn` interface (new streaming method)
  - `rows.go` -- public `Rows` type may gain streaming support
  - `internal/storage/chunk.go` -- DataChunk lifecycle management
