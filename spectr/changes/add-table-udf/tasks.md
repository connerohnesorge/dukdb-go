## 1. Core Types

- [ ] 1.1 Create `table_udf.go` with ColumnInfo and CardinalityInfo structs
- [ ] 1.2 Define TableFunctionConfig with Arguments and NamedArguments
- [ ] 1.3 Define ParallelTableSourceInfo with MaxThreads

## 2. Table Source Interfaces

- [ ] 2.1 Define RowTableSource interface
- [ ] 2.2 Define ParallelRowTableSource interface extending RowTableSource
- [ ] 2.3 Define ChunkTableSource interface
- [ ] 2.4 Define ParallelChunkTableSource interface extending ChunkTableSource

## 3. Table Function Types

- [ ] 3.1 Define tableFunction[T] generic struct
- [ ] 3.2 Define RowTableFunction, ParallelRowTableFunction type aliases
- [ ] 3.3 Define ChunkTableFunction, ParallelChunkTableFunction type aliases
- [ ] 3.4 Implement wrapper functions to promote Row to ParallelRow

## 4. Registration

- [ ] 4.1 Implement RegisterTableUDF with generic type constraint
- [ ] 4.2 Create table function registry per connection
- [ ] 4.3 Handle named argument validation
- [ ] 4.4 Write tests for registration

## 5. Binding Integration

- [ ] 5.1 Hook table UDF resolution in FROM clause parsing
- [ ] 5.2 Create BoundTableFunction expression type
- [ ] 5.3 Implement argument binding with type checking
- [ ] 5.4 Support both BindArguments and BindArgumentsContext

## 6. Sequential Row Execution

- [ ] 6.1 Implement row-based table scan operator
- [ ] 6.2 Call Init() before first FillRow
- [ ] 6.3 Iterate FillRow until false returned
- [ ] 6.4 Write tests for row-based execution

## 7. Sequential Chunk Execution

- [ ] 7.1 Implement chunk-based table scan operator
- [ ] 7.2 Create DataChunk from ColumnInfos
- [ ] 7.3 Call FillChunk until size is 0
- [ ] 7.4 Write tests for chunk-based execution

## 8. Column Projection

- [ ] 8.1 Pass projection to DataChunk initialization
- [ ] 8.2 Implement IsProjected in Row
- [ ] 8.3 Test projection optimization
- [ ] 8.4 Document projection usage

## 9. Parallel Row Execution

- [ ] 9.1 Implement parallel executor with goroutine pool
- [ ] 9.2 Create thread-local state via NewLocalState()
- [ ] 9.3 Coordinate result collection
- [ ] 9.4 Handle errors from workers
- [ ] 9.5 Write tests for parallel row execution

## 10. Parallel Chunk Execution

- [ ] 10.1 Implement parallel chunk executor
- [ ] 10.2 Create DataChunk per worker
- [ ] 10.3 Merge results from parallel workers
- [ ] 10.4 Write tests for parallel chunk execution

## 11. Cardinality and Optimization

- [ ] 11.1 Expose Cardinality() to query optimizer
- [ ] 11.2 Use cardinality for parallelism decisions
- [ ] 11.3 Document optimizer integration

## 12. Deterministic Testing Integration

- [ ] 12.1 Add quartz.Clock field to TableFunctionContext struct
- [ ] 12.2 Add quartz.Clock field to parallelTableExecutor
- [ ] 12.3 Implement WithClock() method for clock injection
- [ ] 12.4 Use clock.TickerFunc() for worker progress monitoring
- [ ] 12.5 Use clock.Until() for all timeout checking
- [ ] 12.6 Write deterministic tests for parallel execution using quartz.Mock
- [ ] 12.7 Write deterministic tests for streaming timeouts
- [ ] 12.8 Verify zero time.Sleep calls in test files
- [ ] 12.9 Verify zero polling loops or runtime.Gosched for synchronization

## 13. Validation

- [ ] 13.1 Run `go test -race` for thread safety
- [ ] 13.2 Run `golangci-lint`
- [ ] 13.3 Create benchmarks for different variants
- [ ] 13.4 Verify API matches duckdb-go
- [ ] 13.5 Verify compliance with deterministic-testing spec
