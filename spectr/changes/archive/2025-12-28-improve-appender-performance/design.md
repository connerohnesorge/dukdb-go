# Design: DataChunk-Based Appender Performance

## Context

The Appender provides bulk data loading functionality. Currently implemented using SQL INSERT statements with row-based buffering (`buffer [][]any`). The reference implementation (duckdb-go) uses DataChunk-based buffering with CGO access to C++ DuckDB storage. We need to achieve similar performance gains without CGO.

**Current stakeholders**:
- Users with bulk loading workloads (data pipelines, ETL)
- Internal: Execution engine, storage layer, transaction manager

**Constraints**:
- Must remain pure Go (no CGO)
- Must maintain API compatibility with duckdb-go
- Must integrate with existing deterministic testing framework (quartz)
- Must support transactions (rollback of appended data)
- Must support both table appenders AND query appenders

**Key Insight from Grading**:
- duckdb-go ALREADY uses DataChunk buffering (not row-based)
- Our goal is to replicate duckdb-go's approach in pure Go

## Goals / Non-Goals

**Goals**:
1. Replace INSERT-based flushing with DataChunk-based storage writes
2. Leverage existing DataChunk API for columnar operations
3. Maintain thread-safety (existing mutex serialization)
4. Support all types that duckdb-go supports (exclude: INVALID, UHUGEINT, BIT, ANY, BIGNUM)
5. Integrate with transaction/WAL system for durability
6. Preserve Query Appender functionality

**Non-Goals**:
1. Match exact CGO performance (acceptable to be slower)
2. Support parallel appending to same table from multiple connections
3. Optimize for small batch sizes (<100 rows) - focus on bulk operations
4. Add Backend interface methods (avoid architectural changes to Backend)

## Decisions

### Decision 1: DataChunk Buffer Replaces Row Buffer

**Options**:
A. Keep SQL approach, optimize string building
B. Use DataChunk for buffering, write directly to storage
C. Hybrid: DataChunk buffer but convert to SQL for execution

**Choice**: B - DataChunk buffering with direct storage writes

**Rationale**:
- Matches proven duckdb-go architecture
- Bypasses SQL parser/planner overhead
- Leverages existing DataChunk/Vector code
- A gives minimal improvement (~2x at best)
- C still pays SQL cost

**Trade-offs**:
- ✅ Eliminates SQL generation overhead
- ✅ Reuses existing DataChunk code
- ⚠️ Requires storage layer changes
- ⚠️ More complex than SQL approach

### Decision 2: Auto-Flush at VectorSize (2048 rows)

**Options**:
A. Keep user-configurable threshold (current: default 1024)
B. Auto-flush at VectorSize (2048), ignore user threshold
C. Hybrid: Flush at min(threshold, VectorSize)

**Choice**: C - Hybrid approach (flush at min(threshold, VectorSize))

**Rationale**:
- Preserves API compatibility (threshold still has meaning)
- User expectations unchanged
- VectorSize is internal optimization

**Implementation**:
```go
// User sets threshold=1024
a := NewAppenderWithThreshold(conn, "", "", "t", 1024)

// Internal behavior:
// - Manual Flush() after 1024 rows still works
// - Auto-flush at VectorSize (2048) if threshold not reached
// - Chunk may have <2048 rows on manual flush
```

### Decision 3: Storage Access Pattern

**Options**:
A. Add Backend.AppendDataChunk() method to interface
B. Add Conn.appendChunkToTable() helper (not in Backend interface)
C. Give Appender direct storage layer access

**Choice**: B - Connection helper method

**Rationale**:
- Backend interface is for query execution abstraction
- Appender is connection-specific operation
- Avoids polluting Backend interface with appender-specific methods
- Connection already has access to backend and catalog

**Implementation**:
```go
// conn.go
func (c *Conn) appendChunkToTable(catalog, schema, table string, chunk DataChunk) error {
    // This is internal, not part of Backend interface
    // Direct access to storage layer via connection's backend
}
```

**Why NOT Option A**:
- Backend interface would need to change across all implementations
- Appender is not a "backend" concept (it's a client-side buffer)
- Would create tight coupling

### Decision 4: Storage Layer Architecture (CORRECTED)

**DuckDB C++ Storage Hierarchy** (per grading agent #3):
```
Table
  → RowGroupCollection
    → RowGroup[] (122,880 rows each)
      → ColumnData[]
        → ColumnSegment[] (compressed storage units)
```

**NOT**:
```
Table → RowGroups[] → Columns[] → Chunks[]  ❌ WRONG
```

**Key Corrections**:
1. **DataChunk is ephemeral** - Used for transfer, not storage
2. **Segments are the storage unit** - Compressed column data
3. **RowVersionManager tracks transactions** - Not per-chunk transaction IDs

**Pure Go Storage Approach**:

Since we're pure Go, we implement a simplified version:

```go
// Simplified storage for pure Go
type Table struct {
    RowGroups []*RowGroup
}

type RowGroup struct {
    Columns      []*ColumnData
    RowCount     int
    VersionInfo  *RowVersionManager  // For MVCC
}

type ColumnData struct {
    Data         []any  // Simplified: actual DuckDB uses compressed segments
    ValidityMask []uint64
}
```

**Chunk Write Process**:
```go
func (c *Conn) appendChunkToTable(catalog, schema, table string, chunk DataChunk) error {
    // 1. Get table from catalog
    tbl := c.getTable(catalog, schema, table)

    // 2. Get or create current row group
    rg := tbl.getCurrentOrCreateRowGroup()

    // 3. Append chunk data to each column
    for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
        vector := chunk.GetVector(colIdx)
        rg.Columns[colIdx].AppendVector(vector, chunk.GetSize())
    }

    // 4. Update row count
    rg.RowCount += chunk.GetSize()

    // 5. Mark for WAL (transaction support)
    if c.inTransaction() {
        c.currentTransaction().MarkAppended(tbl, rg, chunk.GetSize())
    }

    return nil
}
```

### Decision 5: Transaction Integration (CORRECTED)

**DuckDB C++ Approach** (verified in source code):

DuckDB's `LocalStorage` (src/transaction/local_storage.hpp:28-62) is complex:
```cpp
class LocalTableStorage {
    unique_ptr<OptimisticWriteCollection> row_groups;  // Buffered writes
    TableIndexList append_indexes;                      // Index updates
    TableIndexList delete_indexes;                      // Deleted rows tracking
    vector<unique_ptr<OptimisticWriteCollection>> optimistic_collections;
    OptimisticDataWriter optimistic_writer;             // Writes data before commit
}
```

**Commit has TWO paths** (src/transaction/local_storage.cpp:571-589):
1. **Bulk merge**: If table empty OR bulk appending with no deletes → move storage directly
2. **Rollback + Append**: If mixed operations → rollback optimistic writes, append row-by-row

**Rollback cleans up** (src/transaction/local_storage.cpp:280-290):
1. Rollback optimistic writer
2. Commit-drop optimistic collections
3. Commit-drop main row groups collection

**Pure Go Simplified Approach**:

For our pure Go implementation, we simplify by:
- **Omitting optimistic writes** (data written only on commit, not during transaction)
- **Omitting index management** (defer to separate index implementation)
- **Using single commit path** (always bulk merge, no row-by-row fallback)

```go
type Transaction struct {
    ID             uint64
    LocalRowGroups map[*Table][]*RowGroup  // Transaction-local data (simplified)
}

func (txn *Transaction) Commit() error {
    // Simplified: Always bulk merge (DuckDB's optimized path)
    for table, rowGroups := range txn.LocalRowGroups {
        table.MergeRowGroups(rowGroups, txn.ID)
    }
    txn.LocalRowGroups = nil
    return nil
}

func (txn *Transaction) Rollback() error {
    // Discard all local row groups (simplified cleanup)
    txn.LocalRowGroups = nil
    return nil
}
```

**Simplifications Justified**:
- Optimistic writes are performance optimization (can add later)
- Index updates can be deferred to index implementation
- Row-by-row fallback is for mixed workloads (appender is append-only)
- Our approach matches DuckDB's bulk append path (the common case)

**Key Difference from Proposal v1**:
- NOT per-chunk transaction IDs
- Transaction-local row groups (LocalStorage pattern)
- Only visible within transaction until commit

### Decision 6: Query Appender Support

**Challenge**: Current implementation has separate Query Appender mode (field declarations at lines 36-42, constructor at lines 121-236, implementation at lines 556-599).

**Options**:
A. Convert query appender to use DataChunk approach
B. Keep SQL INSERT approach for query appender
C. Disable query appender functionality entirely

**Choice**: B - Keep SQL INSERT approach for query appender

**Rationale**:
- Query appenders are used for MERGE/INSERT...SELECT/UPDATE operations, not bulk append
- The temp table creation + SQL execution path is already relatively efficient
- The bottleneck for query appenders is the query execution, not the INSERT to temp table
- Focusing optimization on table appenders provides the most value
- Reference duckdb-go uses same approach: temp table + SQL execution

**Current Flow (PRESERVED)**:
1. `createTempTable()` - Creates temporary table via SQL
2. `flushQueryAppender()` - Inserts buffered rows to temp table via SQL INSERT (lines 572-584)
3. Query execution - Executes user's query referencing temp table (lines 586-594)
4. Cleanup - Temp table dropped in `Close()` (lines 712-714)

**Note**: Only **table appenders** (direct bulk loading) will be optimized with DataChunk approach. Query appenders remain SQL-based.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|----------|
| Complex type handling bugs | High | Comprehensive fuzzing, cross-validation with duckdb-go |
| Performance not meeting expectations | High | Benchmark early, profile hot paths |
| Storage layer complexity | Medium | Start simple (no compression), add later |
| Transaction semantics differ from DuckDB | High | Extensive transaction tests, study DuckDB behavior |
| Query appender breaks | Medium | Separate test suite for query appenders |

## Migration Plan

### Phase 1: Appender Refactoring (Week 1-2)
1. Add `currentChunk DataChunk` field to Appender struct
2. Refactor `AppendRow()` to use `chunk.SetValue()`
3. Keep SQL fallback behind feature flag
4. Add `conn.appendChunkToTable()` helper (stub implementation)

### Phase 2: Storage Layer Integration (Week 3-4)
1. Implement simplified RowGroup/ColumnData structures
2. Implement `appendChunkToTable()` with direct storage writes
3. Test with primitive types first
4. Add complex type support

### Phase 3: Transaction Support (Week 5-6)
1. Implement LocalStorage pattern (transaction-local row groups)
2. Add commit/rollback for appended data
3. Add deterministic transaction tests

### Phase 4: Query Appender Validation (Week 7)
1. Verify query appender functionality unchanged
2. Test MERGE/DELETE/UPDATE via query appenders (existing SQL-based implementation)
3. Confirm query appenders work with both table appender modes (SQL vs DataChunk)

### Phase 5: Testing & Optimization (Week 8-9)
1. Port all existing appender tests
2. Add benchmarks comparing INSERT vs DataChunk
3. Profile and optimize hot paths
4. Remove SQL fallback

### Rollback Plan
- Feature flag: `DUKDB_LEGACY_APPENDER=1` keeps SQL path
- If performance regression, revert to SQL
- All existing tests must pass with both paths

## Open Questions

1. **Row group size**: Use DuckDB's 122,880 or adjust for Go memory model?
   - **Answer**: Start with 122,880 (60 chunks), benchmark and adjust

2. **Compression**: Add column compression or start uncompressed?
   - **Answer**: Start uncompressed, add compression later as optimization

3. **Chunk pooling**: Reuse DataChunk instances or allocate new?
   - **Answer**: Simple pooling (single cached chunk per appender)

4. **VectorSize constant**: Hardcode 2048 or make configurable?
   - **Answer**: Hardcode `const VectorSize = 2048` (matches DuckDB)

5. **Storage persistence**: In-memory only or add WAL immediately?
   - **Answer**: In-memory first, add WAL in separate change

6. **Query appender temp tables**: Keep current SQL approach or optimize?
   - **Answer**: Keep SQL for query appender (only table appender gets DataChunk optimization)
