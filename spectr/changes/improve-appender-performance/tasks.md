## 1. Backend Interface Extension
- [ ] 1.1 Add AppendDataChunk() method to Backend interface in backend.go
- [ ] 1.2 Add GetTableSchema() helper to Backend for retrieving column TypeInfo

## 2. Engine Implementation
- [ ] 2.1 Implement Engine.AppendDataChunk() method in internal/engine/engine.go
- [ ] 2.2 Add validateChunkSchema() helper to verify chunk matches table schema
- [ ] 2.3 Integrate with transaction manager (getCurrentTransaction helper)

## 3. Storage Layer Implementation
- [ ] 3.1 Define RowGroup struct in internal/storage/rowgroup.go
- [ ] 3.2 Implement Storage.AppendChunk() in internal/storage/table.go
- [ ] 3.3 Implement getOrCreateRowGroup() helper (60 chunks = 122,880 rows per group)
- [ ] 3.4 Implement Column.AppendVector() for copying vector data
- [ ] 3.5 Implement RowGroup.UpdateStats() for min/max/null counts

## 4. Appender Refactoring
- [ ] 4.1 Add currentChunk and currentSize fields to Appender struct
- [ ] 4.2 Modify NewAppender() to initialize DataChunk instead of buffer [][]any
- [ ] 4.3 Refactor AppendRow() to use chunk.SetValue() instead of buffer append
- [ ] 4.4 Refactor flush() to call backend.AppendDataChunk() instead of executing SQL
- [ ] 4.5 Implement chunk reset/reuse after flush to avoid reallocations
- [ ] 4.6 Remove SQL generation code (buildInsertSQL and related helpers)

## 5. Type Handling Tests
- [ ] 5.1 Test primitive types (BOOLEAN through DOUBLE) with DataChunk appending
- [ ] 5.2 Test string types (VARCHAR, BLOB) with varying lengths
- [ ] 5.3 Test temporal types (DATE, TIME, TIMESTAMP variants) with deterministic clock
- [ ] 5.4 Test numeric types (DECIMAL, HUGEINT, UUID)
- [ ] 5.5 Test complex types (LIST, STRUCT, MAP, UNION, ARRAY) with nesting

## 6. Performance Tests
- [ ] 6.1 Write deterministic performance test measuring rows/sec with quartz clock
- [ ] 6.2 Benchmark 1M row append and verify >500k rows/sec throughput
- [ ] 6.3 Compare memory usage vs old SQL approach (must be <2x)

## 7. Concurrency Tests
- [ ] 7.1 Write concurrent append test using quartz traps for coordination
- [ ] 7.2 Test multiple connections appending to different tables simultaneously
- [ ] 7.3 Verify mutex serialization prevents race conditions (go test -race)

## 8. Transaction Integration Tests
- [ ] 8.1 Test transaction commit persists appended chunks
- [ ] 8.2 Test transaction rollback removes appended chunks (deterministic with quartz)
- [ ] 8.3 Test appended data visible within transaction but not outside

## 9. Error Handling Tests
- [ ] 9.1 Test type mismatch errors (wrong Go type for column)
- [ ] 9.2 Test NOT NULL constraint violation at flush time
- [ ] 9.3 Verify buffer preservation on flush failure (retry scenario)
- [ ] 9.4 Test append after close returns ErrorTypeClosed

## 10. API Compatibility Validation
- [ ] 10.1 Verify NewAppender signature unchanged (driver.Conn vs *Conn acceptable)
- [ ] 10.2 Verify threshold behavior unchanged (1024 rows still triggers flush)
- [ ] 10.3 Run all existing appender tests and ensure 100% pass

## 11. Integration & Documentation
- [ ] 11.1 Update appender_test.go with DataChunk-specific test cases
- [ ] 11.2 Add godoc comments explaining DataChunk buffering approach
- [ ] 11.3 Update CLAUDE.md with appender performance characteristics

## 12. Deterministic Testing Compliance
- [ ] 12.1 Verify zero time.Sleep calls in new tests (grep check)
- [ ] 12.2 Verify all timing uses quartz.Clock tags (Appender, flush)
- [ ] 12.3 Verify all concurrent tests use traps for coordination

## 13. Validation & Cleanup
- [ ] 13.1 Run golangci-lint and fix any issues
- [ ] 13.2 Run go test -race ./... and verify no race conditions
- [ ] 13.3 Validate with spectr validate improve-appender-performance
