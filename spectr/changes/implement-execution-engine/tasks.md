# Implementation Tasks: SQL Execution Engine

**Note**: The pipeline (Parse→Bind→Plan→Execute) is ALREADY IMPLEMENTED in `internal/engine/conn.go`. These tasks focus on completing operator implementations, ResultSet, and testing.

---

## Phase A: DataChunk Integration (60-80 hours)

### Task 1: Define Operator Interface for DataChunks
- [ ] Update Operator interface in `internal/executor/operator.go`
- [ ] Change Next() to return *DataChunk (not [][]any)
- [ ] Add GetTypes() []TypeInfo method for column types
- [ ] **Validation**: Interface defined, compiles

### Task 2: Update Scan Operator for DataChunk Output
- [ ] Update PhysicalScan.Next() to produce DataChunk
- [ ] Read from storage RowGroups into DataChunk
- [ ] Set TypeInfo from table schema (P0-1a integration)
- [ ] **Validation**: Scan produces DataChunks

### Task 3: Update Filter Operator for DataChunk I/O
- [ ] Update PhysicalFilter.Next() to consume/produce DataChunk
- [ ] Evaluate predicates on DataChunk rows
- [ ] Use ValidityMask for NULL handling
- [ ] Produce output DataChunk with filtered rows
- [ ] **Validation**: Filter produces correct DataChunks

### Task 4: Update Project Operator for DataChunk I/O
- [ ] Update PhysicalProject.Next() to consume/produce DataChunk
- [ ] Select projected columns from input DataChunk
- [ ] Create output DataChunk with selected columns
- [ ] **Validation**: Project produces correct column subset

### Task 5: Add TypeInfo Integration
- [ ] Use P0-1a TypeInfo for column type metadata
- [ ] Store TypeInfo in table definitions
- [ ] Propagate TypeInfo through operators
- [ ] Validate types match during operations
- [ ] **Validation**: TypeInfo correctly propagated

### Task 6: Update Expression Evaluator for DataChunk
- [ ] Update `internal/executor/expr.go` Eval() to accept DataChunk
- [ ] Evaluate expressions on DataChunk row indices
- [ ] Return typed values using TypeInfo
- [ ] Handle NULL propagation via ValidityMask
- [ ] **Validation**: Expression evaluation works on DataChunks

### Task 7: Test Phase B Milestone
- [ ] Test: SELECT * FROM t returns DataChunk
- [ ] Test: SELECT a, b FROM t returns 2-column DataChunk
- [ ] Test: SELECT * FROM t WHERE x > 5 filters correctly
- [ ] Test: NULL values handled correctly
- [ ] **Validation**: Table queries return DataChunk results

---

## Phase B: Result Set (20-30 hours)

### Task 8: Implement ResultSet Struct
- [ ] Create `internal/executor/result_set.go`
- [ ] Define ResultSet struct wrapping []DataChunk
- [ ] Add currentChunk, currentRow indices
- [ ] **Validation**: ResultSet struct defined

### Task 9: Implement driver.Rows Interface
- [ ] Implement Columns() []string
- [ ] Implement Close() error
- [ ] Implement Next(dest []driver.Value) error
- [ ] **Validation**: Implements driver.Rows

### Task 10: Implement ColumnTypeDatabaseTypeName
- [ ] Implement ColumnTypeDatabaseTypeName(index int) string
- [ ] Return TypeInfo.SQLType() for column
- [ ] **Validation**: Returns correct type names (enables P0-3)

### Task 11: Implement ColumnTypeInfo for P0-3
- [ ] Add ColumnTypeInfo(index int) TypeInfo method
- [ ] Return full TypeInfo for column (InternalType, Details, SQLType)
- [ ] **Validation**: P0-3 can get column metadata

### Task 12: Add Row Iteration Logic
- [ ] Implement Next() to advance currentRow/currentChunk
- [ ] Handle chunk boundaries (transition between chunks)
- [ ] Return io.EOF when exhausted
- [ ] **Validation**: Row iteration works correctly

### Task 13: Implement Scan() Support
- [ ] Extract values from current row in current chunk
- [ ] Convert to driver.Value types
- [ ] Handle NULL values correctly
- [ ] **Validation**: database/sql Scan() works

### Task 14: Test Phase C Milestone
- [ ] Test: Rows.Columns() returns correct column names
- [ ] Test: Rows.Next() iterates all rows
- [ ] Test: Scan() copies row values
- [ ] Test: ColumnTypeDatabaseTypeName() returns types
- [ ] **Validation**: Result sets fully functional

---

## Phase C: Operator Completion (80-100 hours)

### Task 15: Implement Aggregate Operator
- [ ] Create PhysicalAggregate operator
- [ ] Implement hash table for GROUP BY
- [ ] Implement SUM, COUNT, AVG, MIN, MAX aggregates
- [ ] Produce output DataChunk with aggregated results
- [ ] **Validation**: SELECT SUM(x) FROM t works

### Task 16: Implement GROUP BY
- [ ] Add GROUP BY key extraction from DataChunk
- [ ] Build hash table with group keys
- [ ] Accumulate aggregates per group
- [ ] Produce one row per group
- [ ] **Validation**: SELECT category, COUNT(*) FROM t GROUP BY category works

### Task 17: Implement Hash Join Operator
- [ ] Create PhysicalHashJoin operator
- [ ] Implement build phase (build hash table from inner)
- [ ] Implement probe phase (probe with outer)
- [ ] Produce joined DataChunks
- [ ] **Validation**: SELECT * FROM a JOIN b ON a.id = b.id works

### Task 18: Implement ORDER BY
- [ ] Create PhysicalSort operator
- [ ] Collect all DataChunks into memory
- [ ] Sort rows by ORDER BY expressions
- [ ] Produce sorted DataChunks
- [ ] **Validation**: SELECT * FROM t ORDER BY x works

### Task 19: Implement LIMIT/OFFSET
- [ ] Add LIMIT count tracking to PhysicalLimit operator
- [ ] Add OFFSET skip logic
- [ ] Stop producing DataChunks after LIMIT reached
- [ ] **Validation**: SELECT * FROM t LIMIT 10 OFFSET 5 works

### Task 20: Implement DELETE Operator
- [ ] Update PhysicalDelete to mark rows as deleted
- [ ] Update storage layer to track deleted rows
- [ ] Return affected row count
- [ ] **Validation**: DELETE FROM t WHERE x = 1 works

### Task 21: Implement UPDATE Operator
- [ ] Update PhysicalUpdate to modify row values
- [ ] Handle SET clause evaluation
- [ ] Update storage layer
- [ ] Return affected row count
- [ ] **Validation**: UPDATE t SET x = 1 WHERE y = 2 works

### Task 22: Implement INSERT Operator
- [ ] Update PhysicalInsert to insert rows
- [ ] Create DataChunk from VALUES clause
- [ ] Add to storage layer
- [ ] Return affected row count
- [ ] **Validation**: INSERT INTO t VALUES (1, 2) works

### Task 23: Test Phase D Milestone - Complex Operators
- [ ] Test aggregate functions: SUM, AVG, COUNT, MIN, MAX (without GROUP BY)
- [ ] Test single-column GROUP BY with aggregates
- [ ] Test multi-column GROUP BY (2+ grouping columns)
- [ ] Test GROUP BY with HAVING clause
- [ ] Test INNER JOIN execution (2 tables, equi-join on single column)
- [ ] Test LEFT/RIGHT/FULL OUTER JOIN
- [ ] Test multi-way JOINs (3+ tables)
- [ ] Test JOIN combined with WHERE, ORDER BY, LIMIT
- [ ] Test ORDER BY with single column ASC/DESC
- [ ] Test ORDER BY with multiple columns
- [ ] Test LIMIT without OFFSET
- [ ] Test OFFSET without LIMIT
- [ ] Test LIMIT and OFFSET combined
- [ ] Test INSERT with multiple rows
- [ ] Test UPDATE with WHERE clause
- [ ] Test DELETE with WHERE clause
- [ ] **Validation**: All complex operators work correctly, results match expected

---

## Phase D: Testing & Integration (30-40 hours)

### Task 24: End-to-End SELECT Tests
- [ ] Test all SELECT scenarios from spec (lines 112-151)
- [ ] Literal SELECT, arithmetic expressions
- [ ] Table scans, filtered queries
- [ ] Aggregate queries, GROUP BY
- [ ] ORDER BY, LIMIT
- [ ] **Validation**: All SELECT spec scenarios pass

### Task 25: End-to-End DML Tests
- [ ] Test all INSERT scenarios from spec (lines 156-161)
- [ ] Test all UPDATE scenarios from spec (lines 162-167)
- [ ] Test all DELETE scenarios from spec (lines 168-173)
- [ ] **Validation**: All DML spec scenarios pass

### Task 26: Concurrent Access Tests
- [ ] Test 10 concurrent SELECTs on same table (read-read scenario)
- [ ] Test concurrent SELECT + INSERT on same table (read-write scenario)
- [ ] Test concurrent INSERTs to same table (write-write scenario)
- [ ] Test concurrent CREATE TABLE + SELECT (catalog modification during read)
- [ ] Test concurrent DROP TABLE + SELECT (catalog deletion during active query)
- [ ] Test concurrent transactions with isolation (one writes, one reads uncommitted)
- [ ] Stress test: 100 goroutines, mixed operations (SELECT/INSERT/UPDATE), 10 seconds
- [ ] Verify no panics, no deadlocks, consistent results across all concurrent operations
- [ ] Run all concurrent tests with `go test -race` flag
- [ ] **Validation**: No race warnings, no data corruption, all queries return correct results

### Task 27: Error Handling Tests with Specific SQL Scenarios
- [ ] ErrorTypeParser: Test "SELEC * FROM t" (typo), "SELECT FROM WHERE" (incomplete syntax)
- [ ] ErrorTypeCatalog: Test "SELECT * FROM nonexistent_table", "DROP TABLE no_such_table"
- [ ] ErrorTypeBinder: Test "SELECT nonexistent_column FROM t", "SELECT ambiguous_col FROM t1, t2"
- [ ] ErrorTypeMismatchType: Test "SELECT 'string' + 123", "SELECT * FROM t WHERE int_col = 'string'"
- [ ] ErrorTypeDivideByZero: Test "SELECT 1/0", "SELECT x/(y-y) FROM t"
- [ ] ErrorTypeConstraint: Test INSERT with duplicate primary key
- [ ] ErrorTypeNotImplemented: Test using unimplemented operator (if any)
- [ ] Verify error messages are clear and helpful
- [ ] **Validation**: All error types tested with specific SQL, correct error types returned

### Task 28: Performance Benchmark Suite
- [ ] Create internal/executor/executor_bench_test.go benchmark file
- [ ] Benchmark sequential scan: 100K rows, 1M rows, 10M rows (target: 1M rows <1 second)
- [ ] Benchmark filtered scan with varying selectivity: 1%, 10%, 50%, 90%
- [ ] Benchmark aggregation: SUM/COUNT/AVG over 1M rows (target: <500ms)
- [ ] Benchmark hash join: 100K × 100K rows (target: <2 seconds)
- [ ] Benchmark sorting: 1M rows with INT, VARCHAR, DECIMAL columns
- [ ] Add CI performance regression tracking: fail build if >20% slower than baseline
- [ ] Run benchmarks with `-benchmem` to track memory allocations
- [ ] **Validation**: All benchmarks meet performance targets, no regressions

### Task 29: Columnar Storage Verification
- [ ] Test: Data stored as contiguous []int64 (spec line 181)
- [ ] Test: NULL bitmap tracks NULL positions (spec line 186)
- [ ] Test: Rows organized into ~2048 chunks (spec line 191)
- [ ] **Validation**: Columnar storage spec scenarios pass

### Task 30: Backend Interface Compliance
- [ ] Test: Engine implements Backend interface (spec line 244)
- [ ] Test: EngineConn implements BackendConn interface (spec line 249)
- [ ] Test: Open() returns usable connection (spec line 254)
- [ ] Test: Close() releases resources (spec line 259)
- [ ] **Validation**: Backend interface spec scenarios pass

### Task 31: Integration with P0-3
- [ ] Verify ColumnTypeInfo() available for P0-3
- [ ] Test PreparedStmt can get column metadata
- [ ] **Validation**: P0-3 column metadata works

### Task 32: Final Validation
- [ ] All 292 lines of execution-engine spec scenarios pass
- [ ] All integration tests pass
- [ ] Performance benchmarks meet targets
- [ ] No race conditions
- [ ] Code coverage >80%
- [ ] **Validation**: Production-ready

---

## Success Criteria by Phase

### Phase A Success: DataChunk Integration
- [ ] All operators produce/consume DataChunks
- [ ] TypeInfo integrated for column types
- [ ] SELECT * FROM t returns DataChunk results
- [ ] NULL handling via ValidityMask

### Phase B Success: Result Set
- [ ] ResultSet implements driver.Rows
- [ ] Row iteration works
- [ ] Scan() copies values correctly
- [ ] Column metadata available for P0-3

### Phase C Success: Operator Completion
- [ ] Aggregates work (SUM, COUNT, AVG, MIN, MAX)
- [ ] GROUP BY works
- [ ] JOIN works
- [ ] ORDER BY, LIMIT work
- [ ] INSERT/UPDATE/DELETE work

### Phase D Success: Testing Complete
- [ ] All 292 spec lines pass
- [ ] Concurrent access verified
- [ ] Performance benchmarks met
- [ ] No race conditions
- [ ] Production-ready

---

## Estimated Time by Phase

| Phase | Tasks | Hours | Weeks (40h) |
|-------|-------|-------|-------------|
| A: DataChunk | 1-7 | 60-80 | 1.5-2 |
| B: Result Set | 8-14 | 20-30 | 0.5-0.75 |
| C: Operators | 15-23 | 80-100 | 2-2.5 |
| D: Testing | 24-32 | 30-40 | 0.75-1 |
| **Total** | **32 tasks** | **190-250** | **5-6.25** |

**Single Engineer**: 5-7 weeks (assuming full-time, no blockers)
**Two Engineers**: 4-6 weeks (parallel work on phases)

---

## Spec Coverage Matrix

This matrix maps the 11 base spec requirements to implementation and test tasks, ensuring complete coverage.

| Spec Requirement | Spec Lines | Implementation Tasks | Test Tasks | Status |
|-----------------|------------|---------------------|------------|--------|
| SQL Parser | 5-38 | Existing (conn.go) | Task 24, 27 | [ ] |
| Catalog Management | 39-63 | Existing (conn.go) | Task 25, 30 | [ ] |
| Query Binding | 64-82 | Existing (conn.go) | Task 25, 27 | [ ] |
| Query Planning | 83-107 | Existing (conn.go) | Task 24, 26 (JOIN tests) | [ ] |
| Query Execution | 108-151 | Tasks 1-7 | Task 24, 26, 27 | [ ] |
| DML Operations | 152-173 | Tasks 20-22 | Task 25, 28 | [ ] |
| Columnar Storage | 174-192 | Tasks 1-7 | Task 29 | [ ] |
| Type Support | 193-221 | Task 5 | Task 27, 30 | [ ] |
| Concurrent Access | 222-236 | N/A | Task 26 (expanded) | [ ] |
| Backend Interface | 237-262 | Existing (conn.go) | Task 30 | [ ] |
| Error Handling | 263-292 | Existing (conn.go) | Task 27 (expanded) | [ ] |

**Coverage Validation**: All 11 requirements have at least one implementation task and one test task mapped.
