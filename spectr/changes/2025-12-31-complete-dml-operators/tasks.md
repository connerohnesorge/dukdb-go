# Implementation Tasks: Complete DML Operators

## Phase 1: WHERE Clause Integration (Foundation)

### Parser & Binder
- [ ] Verify parser correctly captures WHERE clauses in UpdateStmt AST nodes
- [ ] Verify parser correctly captures WHERE clauses in DeleteStmt AST nodes
- [ ] Test: Parse "DELETE FROM t WHERE x > 5" and verify WHERE expression in AST
- [ ] Test: Parse "UPDATE t SET a = 1 WHERE b IN (1,2,3)" and verify complex WHERE
- [ ] Verify binder resolves column references in UPDATE WHERE clauses
- [ ] Verify binder resolves column references in DELETE WHERE clauses
- [ ] Test: Bind "DELETE FROM t WHERE nonexistent > 5" returns ErrorTypeBinder
- [ ] Test: Bind "UPDATE t SET x = y WHERE z > 10" type-checks expressions

### Executor - DELETE with WHERE
- [ ] Extend PhysicalDelete to include WhereClause field
- [ ] Implement WHERE clause evaluation in executeDelete() using evaluateExpr()
- [ ] Add row filtering logic before deletion (only delete matching rows)
- [ ] Return correct RowsAffected count (number of deleted rows)
- [ ] Test: DELETE FROM t WHERE id = 1 deletes only matching row
- [ ] Test: DELETE FROM t WHERE 1 = 0 deletes no rows (returns RowsAffected = 0)
- [ ] Test: DELETE FROM t WHERE x IS NULL handles NULL values correctly
- [ ] Test: DELETE FROM t WHERE a > 5 AND b < 10 evaluates complex predicates
- [ ] Test: DELETE with subquery WHERE id IN (SELECT ...) works correctly

### Executor - UPDATE with WHERE
- [ ] Extend PhysicalUpdate to include WhereClause field
- [ ] Implement WHERE clause evaluation in executeUpdate() using evaluateExpr()
- [ ] Add row filtering logic before update (only update matching rows)
- [ ] Return correct RowsAffected count (number of updated rows)
- [ ] Test: UPDATE t SET x = 1 WHERE id = 1 updates only matching row
- [ ] Test: UPDATE t SET x = x + 1 WHERE x > 5 evaluates expressions in SET clause
- [ ] Test: UPDATE t SET a = 1, b = 2 WHERE c = 3 handles multi-column updates
- [ ] Test: UPDATE with complex WHERE (OR, NOT, BETWEEN) works correctly

### Validation
- [ ] Run Phase D test suite - verify at least 2 previously skipped tests now pass
- [ ] Verify no regression in existing DELETE/UPDATE tests
- [ ] Add benchmark: DELETE 1000 rows with WHERE clause (<10ms target)
- [ ] Add benchmark: UPDATE 1000 rows with WHERE clause (<10ms target)

## Phase 2: Bulk INSERT Optimization (Performance)

### Storage Layer Enhancement
- [ ] Implement InsertChunk() method in Table (bulk insert from DataChunk)
- [ ] Test: InsertChunk with 2048-row DataChunk completes in <5ms
- [ ] Test: InsertChunk with multi-column DataChunk preserves all values
- [ ] Test: InsertChunk with NULL values correctly sets validity masks
- [ ] Verify InsertChunk maintains row IDs correctly
- [ ] Test: Verify memory usage bounded (<10MB for 2048-row chunk)

### Executor - Bulk INSERT Implementation
- [ ] Extend PhysicalInsert to batch VALUES into DataChunks
- [ ] Implement DataChunk construction from VALUES list
- [ ] Implement chunk flushing when reaching batch size (2048 rows)
- [ ] Call storage.InsertChunk() instead of row-by-row insertion
- [ ] Test: INSERT 100 rows creates single DataChunk
- [ ] Test: INSERT 5000 rows creates 3 DataChunks (2048 + 2048 + 904)
- [ ] Test: INSERT with expression VALUES (1+1, UPPER('x')) evaluates expressions
- [ ] Test: INSERT with NULL values handled correctly

### INSERT...SELECT Support
- [ ] Extend PhysicalInsert to support SELECT subplan
- [ ] Implement streaming execution: SELECT → batch → INSERT loop
- [ ] Flush DataChunks incrementally (don't buffer full SELECT result)
- [ ] Test: INSERT INTO t SELECT * FROM source copies all rows
- [ ] Test: INSERT INTO t SELECT * FROM large_table (1M rows) completes with bounded memory
- [ ] Test: INSERT INTO t SELECT x+1 FROM source evaluates expressions
- [ ] Test: INSERT...SELECT with WHERE clause filters correctly

### Performance Validation
- [ ] Benchmark: INSERT 100 rows (<1ms)
- [ ] Benchmark: INSERT 1,000 rows (<10ms)
- [ ] Benchmark: INSERT 10,000 rows (<100ms)
- [ ] Benchmark: INSERT 100,000 rows (<1 second)
- [ ] Benchmark: INSERT 1,000,000 rows (<30 seconds)
- [ ] Compare benchmarks against Appender API (should be within 20%)
- [ ] Verify memory usage: 1M row insert uses <100MB peak memory

## Phase 3: WAL Integration (ACID Compliance)

### WAL Entry Implementation
- [ ] Verify WALInsertEntry structure in entry_data.go handles DataChunk
- [ ] Verify WALUpdateEntry structure includes before/after values
- [ ] Verify WALDeleteEntry structure includes deleted row data
- [ ] Implement serialization for INSERT entry with DataChunk
- [ ] Implement serialization for UPDATE entry with row IDs + values
- [ ] Implement serialization for DELETE entry with row IDs + data
- [ ] Test: Serialize and deserialize INSERT entry preserves all data
- [ ] Test: Serialize and deserialize UPDATE entry preserves before/after
- [ ] Test: Serialize and deserialize DELETE entry preserves deleted data

### Executor WAL Integration
- [ ] Add WAL logging to executeInsert() after successful insertion
- [ ] Add WAL logging to executeUpdate() after successful update
- [ ] Add WAL logging to executeDelete() after successful deletion
- [ ] Use clock.Now() for timestamps (deterministic testing via quartz)
- [ ] Test: INSERT operation creates WAL entry
- [ ] Test: UPDATE operation creates WAL entry with before/after values
- [ ] Test: DELETE operation creates WAL entry with deleted data
- [ ] Test: Failed INSERT (e.g., constraint violation) does NOT create WAL entry

### WAL Recovery
- [ ] Extend WAL recovery to replay INSERT entries
- [ ] Extend WAL recovery to replay UPDATE entries
- [ ] Extend WAL recovery to replay DELETE entries
- [ ] Test: Crash after INSERT, recovery restores inserted rows
- [ ] Test: Crash after UPDATE, recovery restores updated values
- [ ] Test: Crash after DELETE, recovery removes deleted rows
- [ ] Test: Crash mid-transaction (before commit), recovery rolls back

### Deterministic Testing
- [ ] Add deterministic crash test: INSERT 1000 rows, crash at row 500
- [ ] Add deterministic crash test: UPDATE 1000 rows, crash mid-operation
- [ ] Add deterministic crash test: DELETE 1000 rows, crash mid-operation
- [ ] Test: Timeout during long-running DELETE (quartz.Mock clock advance)
- [ ] Test: Transaction rollback undoes partial INSERT

## Phase 4: Validation & Compatibility (Completeness)

### Phase D Test Suite
- [ ] Unskip TestPhaseD_DELETE_ComplexWhere - verify passes
- [ ] Unskip TestPhaseD_UPDATE_SubqueryWhere - verify passes
- [ ] Unskip TestPhaseD_INSERT_BulkValues - verify passes
- [ ] Unskip TestPhaseD_INSERT_SELECT - verify passes
- [ ] Unskip TestPhaseD_DML_Transaction - verify passes
- [ ] Add new test: DELETE with IN (SELECT ...) subquery
- [ ] Add new test: UPDATE with CASE expression in SET clause
- [ ] Add new test: INSERT with DEFAULT values

### Compatibility Testing
- [ ] Run compatibility test suite against reference duckdb-go
- [ ] Verify: INSERT results match reference implementation
- [ ] Verify: UPDATE results match reference implementation
- [ ] Verify: DELETE results match reference implementation
- [ ] Verify: RowsAffected counts match reference implementation
- [ ] Verify: Error types match reference implementation
- [ ] Compare: Performance within 2x of reference (acceptable for pure Go)

### Edge Case Testing
- [ ] Test: INSERT into empty table
- [ ] Test: INSERT with all NULL values
- [ ] Test: UPDATE with no matching rows (RowsAffected = 0)
- [ ] Test: DELETE with no matching rows (RowsAffected = 0)
- [ ] Test: UPDATE/DELETE on empty table
- [ ] Test: INSERT duplicate PRIMARY KEY (should error)
- [ ] Test: UPDATE violating NOT NULL constraint (should error)
- [ ] Test: Large batch (100K rows) completes successfully

### Documentation
- [ ] Add code comments to PhysicalDelete WHERE logic
- [ ] Add code comments to PhysicalUpdate WHERE logic
- [ ] Add code comments to PhysicalInsert DataChunk batching
- [ ] Add code comments to WAL entry structures
- [ ] Update CLAUDE.md with DML performance characteristics
- [ ] Add example code to demonstrate bulk INSERT usage

### Final Validation
- [ ] Run full test suite: `nix develop -c gotestsum --format short-verbose ./...`
- [ ] Verify all tests pass (no regressions)
- [ ] Run linter: `nix develop -c golangci-lint run`
- [ ] Run benchmarks: verify performance targets met
- [ ] Run coverage: verify >80% coverage on new code
- [ ] Verify memory profiling: no leaks, bounded memory usage

## Parallel Work Opportunities

**Can be done in parallel**:
- Phase 1 DELETE and UPDATE work (independent)
- Phase 2 InsertChunk and INSERT...SELECT (after bulk INSERT foundation)
- Phase 3 WAL entry structures and recovery logic (independent)

**Must be sequential**:
- Phase 1 → Phase 2 → Phase 3 (build foundation first)
- Within each phase: implementation → testing → validation

## Definition of Done

- [ ] All 5 skipped Phase D tests pass
- [ ] All benchmarks meet performance targets
- [ ] All compatibility tests pass
- [ ] Zero regressions in existing test suite
- [ ] Code coverage >80% on new code
- [ ] Documentation complete
- [ ] Code review approved
- [ ] CI pipeline green
