# Streaming Results Specification

## Requirements

### Requirement: Streaming Result Iterator

The system SHALL provide a `StreamingResult` type that delivers query results
one DataChunk at a time without materializing the entire result set in memory.

#### Scenario: Large SELECT streams chunks on demand

- WHEN a SELECT query produces 100,000 rows
- THEN the StreamingResult delivers rows in chunks of up to 2048
- AND peak memory usage stays proportional to a single chunk, not the full result set

#### Scenario: StreamingResult reports column metadata

- WHEN a StreamingResult is created for a query with columns (id INTEGER, name VARCHAR)
- THEN Columns() returns ["id", "name"]
- AND column types are available for type-aware scanning

#### Scenario: StreamingResult respects context cancellation

- WHEN a streaming query is in progress
- AND the context is cancelled
- THEN the next call to the chunk iterator returns a context cancellation error
- AND resources held by the streaming pipeline are released

#### Scenario: StreamingResult Close releases resources

- WHEN Close() is called on a StreamingResult before all chunks are consumed
- THEN the executor pipeline is terminated
- AND no further chunks are produced
- AND Close() is idempotent (calling it again returns nil)

### Requirement: Chunked Driver Rows

The `driver.Rows` implementation SHALL support a streaming mode that pulls
DataChunks lazily from a StreamingResult instead of reading from a
pre-populated slice.

#### Scenario: Streaming rows.Next delivers one row at a time

- WHEN database/sql calls Next() on streaming rows
- THEN the implementation reads from the current DataChunk
- AND advances to the next chunk automatically when the current chunk is exhausted

#### Scenario: Streaming rows coexist with materialized rows

- WHEN the backend does not support streaming
- THEN the driver falls back to the existing materialized path
- AND behavior is identical to the current implementation

#### Scenario: Streaming rows convert DataChunk values to driver.Value

- WHEN Next() reads a row from a DataChunk in streaming mode
- THEN each column value is converted directly from the DataChunk columnar format to driver.Value
- AND the intermediate map[string]any representation is not used

### Requirement: Backend Streaming Interface

The system SHALL define a `BackendConnStreaming` interface that backends MAY
implement to provide streaming query results.

#### Scenario: Backend implements streaming

- WHEN a backend implements BackendConnStreaming
- THEN QueryContext uses the streaming path for SELECT queries
- AND results are delivered incrementally

#### Scenario: Backend does not implement streaming

- WHEN a backend does not implement BackendConnStreaming
- THEN QueryContext falls back to the existing Query() method
- AND all rows are materialized before iteration begins

### Requirement: Streaming Executor Pipeline

The executor SHALL support producing results as a lazy chunk iterator without
fully materializing the result set.

#### Scenario: ExecuteStreaming returns chunk iterator

- WHEN ExecuteStreaming is called with a physical plan
- THEN it returns a StreamingResult with a closure that produces DataChunks on demand
- AND the executor pipeline is not drained upfront

#### Scenario: Blocking operators materialize internally

- WHEN a query contains ORDER BY, GROUP BY, or window functions
- THEN the blocking operator materializes its input internally
- AND the final result delivery to the driver is still streamed chunk by chunk

#### Scenario: Non-blocking operators stream through

- WHEN a query contains only Scan, Filter, Project, and Limit operators
- THEN no full materialization occurs at any stage
- AND each Next() call on the StreamingResult triggers exactly one chunk through the pipeline

### Requirement: Implicit Backpressure

The streaming pipeline SHALL use pull-based execution so that the executor
only produces chunks when the consumer requests them.

#### Scenario: Consumer controls execution pace

- WHEN the consumer pauses between Next() calls
- THEN the executor does not produce additional chunks during the pause
- AND no buffering goroutines or channels are used

#### Scenario: Slow consumer does not cause unbounded buffering

- WHEN the consumer reads rows slowly (e.g., processing each row for 100ms)
- THEN memory usage remains bounded to the current chunk plus any blocking operator state
- AND no additional chunks accumulate in memory

