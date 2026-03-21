## 1. Core Streaming Infrastructure

- [x] 1.1 Create `StreamingResult` type in `streaming.go` (root `dukdb` package) with `scanNext` closure, column metadata, Close(), and context cancellation support. The type must NOT import `internal/storage`; chunk state is captured inside the closure.
- [x] 1.2 Implement `StreamingResult.ScanNext(dest []driver.Value)` as a public method that delegates to the internal `scanNext` closure, reading one row at a time from the current DataChunk and advancing to the next chunk when exhausted
- [x] 1.3 The direct DataChunk-to-driver.Value conversion (bypassing map[string]any) lives inside the `scanNext` closure constructed by `ExecuteStreaming()` in `internal/executor/streaming.go`

## 2. Executor Streaming Support

- [x] 2.1 Add `ExecuteStreaming()` method to `internal/executor/Executor` that returns a `*dukdb.StreamingResult` wrapping the physical operator pipeline via a `scanNext` closure
- [x] 2.2 Refactor `collectResults()` in `operator.go` to extract the chunk-producing closure so it can be shared between materialized and streaming paths
- [x] 2.3 Ensure context cancellation checks in the streaming chunk iterator between chunk fetches

## 3. Backend Interface Extension

- [x] 3.1 Define `BackendConnStreaming` interface in `backend.go` with `QueryStreaming()` method
- [x] 3.2 Implement `BackendConnStreaming` on `EngineConn` in `internal/engine/conn.go` -- parse, bind, plan, then return StreamingResult instead of materializing
- [x] 3.3 Handle connection mutex correctly for streaming: release lock after pipeline setup so the connection is not blocked until all rows are consumed

## 4. Driver Integration

- [x] 4.1 Extend `rows` struct in `conn.go` with optional `streamResult` field for streaming mode
- [x] 4.2 Update `rows.Next()` to dispatch to streaming path when `streamResult` is non-nil
- [x] 4.3 Update `rows.Close()` to call `StreamingResult.Close()` in streaming mode
- [x] 4.4 Update `Conn.QueryContext()` to check for `BackendConnStreaming` and prefer streaming path for SELECT queries

## 5. Testing

- [x] 5.1 Unit tests for `StreamingResult` -- chunk iteration, row cursor advancement, Close idempotency, context cancellation
- [x] 5.2 Integration test: large result set (10k+ rows) verifying streaming delivers correct data
- [x] 5.3 Integration test: streaming with blocking operators (ORDER BY, GROUP BY) produces correct results
- [x] 5.4 Integration test: context cancellation mid-stream stops execution
- [x] 5.5 Integration test: fallback to materialized path when backend does not implement streaming
- [x] 5.6 Benchmark: compare memory usage of streaming vs materialized for large result sets
