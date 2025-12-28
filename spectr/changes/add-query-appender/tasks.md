## 1. TypeInfo Extensions

- [ ] 1.1 Add SQLType() method to TypeInfo
- [ ] 1.2 Implement SQL type generation for all 45 types
- [ ] 1.3 Handle nested types (LIST, STRUCT, MAP, ARRAY, UNION)
- [ ] 1.4 Add unit tests for SQLType() generation

## 2. Query Appender Core

- [ ] 2.1 Add query appender fields to Appender struct
- [ ] 2.2 Implement NewQueryAppender constructor
- [ ] 2.3 Add input validation (empty query, column mismatch)
- [ ] 2.4 Extract *Conn from driver.Conn interface

## 3. Temporary Table Management

- [ ] 3.1 Implement createTempTable() method
- [ ] 3.2 Implement truncateTempTable() method
- [ ] 3.3 Implement dropTempTable() method
- [ ] 3.4 Add cleanup on Close() for query appenders

## 4. Query Appender Flush

- [ ] 4.1 Detect appender type in Flush()
- [ ] 4.2 Implement flushQueryAppender() three-phase execution
- [ ] 4.3 Build INSERT statement for temp table
- [ ] 4.4 Execute user query after temp table population
- [ ] 4.5 Handle errors with proper cleanup

## 5. Testing

- [ ] 5.1 Test NewQueryAppender validation errors
- [ ] 5.2 Test INSERT query with batched data
- [ ] 5.3 Test MERGE INTO upsert pattern
- [ ] 5.4 Test UPDATE with batched criteria
- [ ] 5.5 Test DELETE with batched criteria
- [ ] 5.6 Test nested type columns in temp table
- [ ] 5.7 Test auto-flush threshold behavior
- [ ] 5.8 Test error cleanup scenarios

## 6. Deterministic Testing Integration

- [ ] 6.1 Add quartz.Clock field to appenderContext struct
- [ ] 6.2 Implement WithClock() method for clock injection
- [ ] 6.3 Use clock.Until() for deadline checking in FlushWithContext
- [ ] 6.4 Write deterministic tests for flush timeout using quartz.Mock
- [ ] 6.5 Verify zero time.Sleep calls in test files

## 7. Validation

- [ ] 7.1 Run `go test -race`
- [ ] 7.2 Run `golangci-lint`
- [ ] 7.3 Verify API matches duckdb-go NewQueryAppender
- [ ] 7.4 Verify compliance with deterministic-testing spec
