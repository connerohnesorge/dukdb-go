# Change: Complete DML Operator Implementation for Production Workloads

## Why

Currently, dukdb-go has partial implementations of INSERT, UPDATE, and DELETE operators with 5 Phase D tests remaining skipped. The executor has physical operators but lacks complete WHERE clause evaluation integration, bulk operation optimization, and WAL persistence for these DML operations. This prevents dukdb-go from being a drop-in replacement for duckdb-go v1.4.3 in production workloads that rely on bulk data manipulation and ETL use cases.

## What

Implement complete DML operator support through three integrated components:

1. **WHERE Clause Integration**: Extend physical operators to properly evaluate WHERE clauses for UPDATE and DELETE operations with full expression support (AND/OR/NOT, subqueries, IN clauses)
2. **Bulk Operation Optimization**: Implement batch processing for INSERT operations using DataChunk columnar storage to achieve 100K+ rows/sec throughput
3. **WAL Persistence Integration**: Ensure all DML operations are properly logged in Write-Ahead Log for ACID compliance and crash recovery

## Impact

### Users Affected
- **ETL Pipeline Developers**: Can now perform bulk inserts efficiently (target: 1M rows in <30 seconds using explicit transactions with group commit)
- **Application Developers**: Can use UPDATE/DELETE with complex WHERE clauses reliably, with proper NULL handling (three-valued logic)
- **Data Engineers**: Can migrate from duckdb-go cgo driver to pure Go implementation with acceptable performance (within 2x of reference for transactional workloads)

### Breaking Changes
None - this is additive functionality completing existing partial implementations.

### Performance Impact
- **INSERT (transaction with group commit)**: 50-100x improvement for bulk operations (from row-by-row to batched DataChunk insertion with single fsync at COMMIT)
- **INSERT (auto-commit)**: Limited by fsync() latency (~5-10ms per operation on typical SSD) - users should use explicit transactions for bulk operations
- **UPDATE/DELETE**: O(filtered rows) instead of O(total rows) with proper WHERE clause pushdown
- **Memory**: Bounded by DataChunk size (2048 rows max buffering), no unbounded growth
- **WAL Overhead**: Group commit strategy amortizes fsync cost across multiple operations in a transaction

### Dependencies
- Existing parser support for DML syntax (already present)
- Existing binder for column resolution (already present)
- Existing storage layer with row mutation support (needs enhancement: tombstone marking, RowID tracking)
- Existing WAL infrastructure (present at /internal/wal/ - needs DataChunk-based entries)
- Transaction support (/conn.go:96-129 - already implemented, needs integration with Executor.currentTx)
- Clock injection for deterministic testing (quartz.Clock interface)

## Alternatives Considered

### Alternative 1: Keep Row-by-Row Inserts
- **Pro**: Simpler implementation
- **Con**: Unacceptable performance for production ETL (100x slower than batched approach)
- **Rejected**: Performance is critical requirement for API parity

### Alternative 2: Implement Only INSERT Optimization
- **Pro**: Smaller scope, faster delivery
- **Con**: Leaves UPDATE/DELETE incomplete, blocks v1.4.3 compatibility
- **Rejected**: Need complete DML for API parity

### Alternative 3: Skip WAL Integration
- **Pro**: Faster initial implementation
- **Con**: No crash recovery, breaks ACID guarantees
- **Rejected**: ACID compliance is non-negotiable for database driver

## Success Criteria

- [ ] All 5 skipped Phase D tests pass without modification
- [ ] Bulk INSERT of 1M rows (in explicit transaction) completes in <30 seconds with group commit
- [ ] UPDATE/DELETE operations return correct rows affected count
- [ ] WAL correctly logs all DML operations with DataChunk payloads (verified by recovery test)
- [ ] WAL recovery is idempotent (multiple recovery passes produce same result)
- [ ] Transaction atomicity verified: uncommitted DML operations are rolled back after crash
- [ ] No regression in existing test suite (all tests pass)
- [ ] Memory usage stays bounded during bulk operations (<100MB for 1M row insert)
- [ ] Compatibility tests pass against reference duckdb-go v1.4.3:
  - [ ] Error types match (ErrorTypeCatalog, ErrorTypeBinder, ErrorTypeMismatchType, ErrorTypeInterrupt)
  - [ ] NULL handling matches (three-valued logic in WHERE clauses)
  - [ ] Performance within 2x of reference for transactional workloads

## Rollout Plan

### Phase 1: WHERE Clause Integration (Week 1)
- Implement WHERE clause evaluation in PhysicalDeleteOperator
- Implement WHERE clause evaluation in PhysicalUpdateOperator
- Add Phase D tests for complex WHERE clauses
- Target: 2 of 5 skipped tests passing

### Phase 2: Bulk INSERT Optimization (Week 1-2)
- Implement DataChunk batching in PhysicalInsertOperator
- Add benchmark tests for bulk inserts
- Target: INSERT performance 100K+ rows/sec

### Phase 3: WAL Integration (Week 2)
- Integrate DML operations with WAL writer
- Implement crash recovery tests
- Target: All DML operations persisted and recoverable

### Phase 4: Validation & Polish (Week 2)
- Enable all Phase D tests
- Run compatibility test suite
- Performance benchmarking against reference
- Documentation updates

## Out of Scope

- Index support for faster lookups (separate feature)
- Constraint enforcement beyond PRIMARY KEY (separate feature)
- MERGE statement support (not in v1.4.3)
- Parallel DML execution (future optimization)
- Query optimizer improvements (separate change)
