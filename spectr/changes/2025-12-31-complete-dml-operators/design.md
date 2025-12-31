# Design: Complete DML Operator Implementation

## Architecture Overview

This change completes the DML (Data Manipulation Language) execution pipeline by integrating WHERE clause evaluation, bulk operation optimization, and WAL persistence across the existing parser → binder → planner → executor → storage stack.

```
SQL Query
    ↓
Parser (internal/parser/) → AST with WHERE clauses
    ↓
Binder (internal/binder/) → Resolve columns, type-check expressions
    ↓
Planner (internal/planner/) → Logical Plan → Physical Plan
    ↓
Executor (internal/executor/) → Execute with WHERE evaluation
    ↓
Storage (internal/storage/) → Mutate rows (insert/update/delete)
    ↓
WAL (internal/wal/) → Log operations for crash recovery
```

## Component Design

### 1. WHERE Clause Evaluation Pipeline

**Challenge**: UPDATE and DELETE need to filter rows before modification.

**Current State**:
- `/internal/executor/physical_delete.go:1-150` - Has basic DELETE but no WHERE filtering
- `/internal/executor/physical_update.go:1-120` - Has basic UPDATE but WHERE not fully integrated
- `/internal/executor/expr.go:1-600+` - Expression evaluator exists and supports all operators

**Design Decision**: Reuse existing `evaluateExpr()` infrastructure from SELECT operations.

**Implementation Pattern**:
```go
// Executor struct with transaction context and clock injection
type Executor struct {
    storage   *Storage
    wal       *WAL
    currentTx *Transaction  // Current transaction context
    clock     quartz.Clock  // Injected clock for deterministic timestamps
}

// PhysicalDeleteOperator execution
func (e *Executor) executeDelete(plan *PhysicalDelete) (ResultSet, error) {
    // 1. Get table scanner
    scanner := storage.GetScanner(plan.TableName)

    // 2. Iterate rows, evaluate WHERE clause, collect deleted data
    var deletedRowIDs []RowID
    deletedDataChunk := NewDataChunk(plan.TableSchema, 2048)

    for scanner.Next() {
        row := scanner.Current()

        // Evaluate WHERE clause (reuses existing expr.go logic)
        // Handles three-valued logic: NULL comparisons return NULL (false)
        if plan.WhereClause != nil {
            match, err := e.evaluateExpr(plan.WhereClause, row)
            if err != nil {
                return nil, err
            }
            // Handle NULL result from expression (three-valued logic)
            if match == nil || !match.(bool) {
                continue // Skip non-matching rows
            }
        }

        deletedRowIDs = append(deletedRowIDs, row.ID)
        deletedDataChunk.AppendRow(row.Values) // Store for rollback
    }

    // 3. Mark rows as deleted using tombstones (in-place deletion)
    if err := storage.DeleteRows(plan.TableName, deletedRowIDs); err != nil {
        return nil, err
    }

    // 4. Log to WAL with explicit Write() call
    entry := &WALDeleteEntry{
        TransactionID: e.currentTx.ID,
        TableName:     plan.TableName,
        RowIDs:        deletedRowIDs,
        DeletedData:   deletedDataChunk, // For rollback support
        Timestamp:     e.clock.Now(),     // Deterministic testing via quartz
    }
    if err := e.wal.Write(entry); err != nil {
        return nil, err
    }

    return ResultSet{RowsAffected: len(deletedRowIDs)}, nil
}
```

**Trade-offs**:
- ✅ Pro: Reuses battle-tested expression evaluator
- ✅ Pro: Consistent semantics with SELECT WHERE
- ⚠️ Con: May need to scan full table if no indexes (acceptable for v1 - index optimization is separate)
- ✅ Mitigation: WHERE clause is evaluated in memory, no disk I/O per row

### 2. Bulk INSERT Optimization via DataChunk Batching

**Challenge**: Row-by-row inserts are 100x slower than batched operations.

**Current State**:
- `/internal/executor/physical_insert.go` - Exists with basic INSERT
- `/internal/storage/chunk.go` - DataChunk infrastructure available
- `/appender.go` - Demonstrates DataChunk batching for Appender API (target performance benchmark)

**Design Decision**: Convert INSERT...VALUES into DataChunk before storage layer insertion.

**Implementation Pattern**:
```go
// PhysicalInsertOperator with DataChunk batching
func (e *Executor) executeInsert(plan *PhysicalInsert) (ResultSet, error) {
    // 1. Determine batch size (consistent with all DML operations)
    const batchSize = 2048 // DataChunk default capacity

    // 2. Convert VALUES into DataChunks
    chunks := make([]*DataChunk, 0)
    currentChunk := NewDataChunk(plan.ColumnTypes, batchSize)

    for i, valueRow := range plan.Values {
        // Evaluate each value expression
        evaledRow, err := e.evaluateRowExpressions(valueRow)
        if err != nil {
            return nil, err
        }

        // Add to current chunk
        currentChunk.AppendRow(evaledRow)

        // Flush chunk when full
        if currentChunk.Size() == batchSize {
            chunks = append(chunks, currentChunk)
            currentChunk = NewDataChunk(plan.ColumnTypes, batchSize)
        }
    }

    // 3. Flush remaining rows
    if currentChunk.Size() > 0 {
        chunks = append(chunks, currentChunk)
    }

    // 4. Batch insert to storage (single write per chunk)
    rowsInserted := 0
    for _, chunk := range chunks {
        count, err := storage.InsertChunk(plan.TableName, chunk)
        if err != nil {
            return nil, err
        }
        rowsInserted += count

        // 5. Log to WAL with explicit Write() call (one entry per chunk)
        entry := &WALInsertEntry{
            TransactionID: e.currentTx.ID,
            TableName:     plan.TableName,
            Chunk:         chunk,
            Timestamp:     e.clock.Now(),
        }
        if err := e.wal.Write(entry); err != nil {
            return nil, err
        }
    }

    return ResultSet{RowsAffected: rowsInserted}, nil
}
```

**Performance Characteristics**:
- **Target**: 100,000+ rows/second for simple inserts
- **Memory**: Bounded by chunk size (2048 rows × column count × avg value size ≈ 1-10MB per chunk)
- **Disk I/O**: One write per chunk instead of per row (2048x reduction in I/O operations)

**Trade-offs**:
- ✅ Pro: Matches Appender API performance (verified existing benchmark)
- ✅ Pro: Columnar format optimizes CPU cache locality
- ⚠️ Con: Higher memory usage during insert (acceptable - bounded by chunk size)
- ✅ Mitigation: Flush chunks incrementally, never buffer full dataset in memory

### 3. INSERT...SELECT Support

**Challenge**: Need to execute subquery and stream results to target table.

**Design Decision**: Treat INSERT...SELECT as pipeline: execute SELECT → batch results into DataChunks → insert chunks.

**Implementation Pattern**:
```go
func (e *Executor) executeInsertSelect(plan *PhysicalInsert) (ResultSet, error) {
    // 1. Execute SELECT subquery
    selectResults, err := e.Execute(plan.SelectPlan)
    if err != nil {
        return nil, err
    }

    // 2. Stream results into DataChunks
    currentChunk := NewDataChunk(plan.ColumnTypes, 2048)
    rowsInserted := 0

    for selectResults.Next() {
        row := selectResults.CurrentRow()
        currentChunk.AppendRow(row)

        // Flush chunk when full
        if currentChunk.Size() == 2048 {
            count, err := storage.InsertChunk(plan.TableName, currentChunk)
            if err != nil {
                return nil, err
            }

            // Log to WAL with explicit Write() call
            entry := &WALInsertEntry{
                TransactionID: e.currentTx.ID,
                TableName:     plan.TableName,
                Chunk:         currentChunk,
                Timestamp:     e.clock.Now(),
            }
            if err := e.wal.Write(entry); err != nil {
                return nil, err
            }

            rowsInserted += count
            currentChunk.Reset()
        }
    }

    // Flush final chunk
    if currentChunk.Size() > 0 {
        count, err := storage.InsertChunk(plan.TableName, currentChunk)
        if err != nil {
            return nil, err
        }

        // Log to WAL with explicit Write() call
        entry := &WALInsertEntry{
            TransactionID: e.currentTx.ID,
            TableName:     plan.TableName,
            Chunk:         currentChunk,
            Timestamp:     e.clock.Now(),
        }
        if err := e.wal.Write(entry); err != nil {
            return nil, err
        }

        rowsInserted += count
    }

    return ResultSet{RowsAffected: rowsInserted}, nil
}
```

**Trade-offs**:
- ✅ Pro: Streaming approach - bounded memory usage even for large SELECTs
- ✅ Pro: Reuses existing SELECT execution infrastructure
- ⚠️ Con: No parallelization (acceptable for v1 - optimization opportunity for future)

### 4. WAL Integration for ACID Compliance

**Challenge**: Need to log DML operations for crash recovery while maintaining performance.

**Current State**:
- `/internal/wal/entry.go` - Entry abstraction exists
- `/internal/wal/entry_data.go` - Data modification entries defined
- `/internal/wal/writer.go` - WAL writer implemented
- `/internal/wal/reader.go` - Recovery reader implemented

**Design Decision**: Log operations at DataChunk granularity (not individual rows) to minimize WAL overhead.

**WAL Synchronization Strategy**: Group commit - buffer WAL writes within a transaction and issue a single fsync() at COMMIT time. This batches fsync overhead across multiple operations while maintaining ACID durability guarantees. For auto-commit statements (single INSERT/UPDATE/DELETE outside explicit transaction), fsync occurs immediately after the operation.

**WAL Entry Formats**:

**INSERT Entry**:
```go
type WALInsertEntry struct {
    TransactionID uint64
    TableName     string
    Chunk         *DataChunk  // Columnar data (2048 rows max)
    Timestamp     time.Time   // Deterministic via clock injection
}
```

**UPDATE Entry**:
```go
type WALUpdateEntry struct {
    TransactionID uint64
    TableName     string
    RowIDs        []RowID
    BeforeValues  *DataChunk  // For rollback (MVCC before-image)
    AfterValues   *DataChunk  // For redo (updated values)
    Timestamp     time.Time   // Deterministic via clock injection
}
```

**DELETE Entry**:
```go
type WALDeleteEntry struct {
    TransactionID uint64
    TableName     string
    RowIDs        []RowID
    DeletedData   *DataChunk  // For rollback (deleted row data)
    Timestamp     time.Time   // Deterministic via clock injection
}
```

**Implementation Pattern** (UPDATE operator):
```go
// PhysicalUpdateOperator execution with WAL logging
func (e *Executor) executeUpdate(plan *PhysicalUpdate) (ResultSet, error) {
    scanner := storage.GetScanner(plan.TableName)

    var updatedRowIDs []RowID
    beforeValuesChunk := NewDataChunk(plan.TableSchema, 2048)
    afterValuesChunk := NewDataChunk(plan.TableSchema, 2048)

    for scanner.Next() {
        row := scanner.Current()

        // Evaluate WHERE clause with three-valued logic
        if plan.WhereClause != nil {
            match, err := e.evaluateExpr(plan.WhereClause, row)
            if err != nil {
                return nil, err
            }
            if match == nil || !match.(bool) {
                continue
            }
        }

        // Store before-image for rollback
        beforeValuesChunk.AppendRow(row.Values)

        // Evaluate SET expressions and update row
        for colIdx, setExpr := range plan.SetExpressions {
            newValue, err := e.evaluateExpr(setExpr, row)
            if err != nil {
                return nil, err
            }
            row.SetValue(colIdx, newValue)
        }

        // Store after-image for redo
        afterValuesChunk.AppendRow(row.Values)
        updatedRowIDs = append(updatedRowIDs, row.ID)
    }

    // Apply updates to storage
    if err := storage.UpdateRows(plan.TableName, updatedRowIDs, afterValuesChunk); err != nil {
        return nil, err
    }

    // Write to WAL with explicit Write() call
    entry := &WALUpdateEntry{
        TransactionID: e.currentTx.ID,
        TableName:     plan.TableName,
        RowIDs:        updatedRowIDs,
        BeforeValues:  beforeValuesChunk, // For rollback
        AfterValues:   afterValuesChunk,  // For redo
        Timestamp:     e.clock.Now(),
    }
    if err := e.wal.Write(entry); err != nil {
        return nil, err
    }

    return ResultSet{RowsAffected: len(updatedRowIDs)}, nil
}
```

**Recovery Pattern**:
```go
// Replay WAL on startup after crash
func (w *WAL) Recover(storage *Storage) error {
    entries, err := w.ReadAll()
    if err != nil {
        return err
    }

    // Track committed transactions for idempotent recovery
    committedTxns := make(map[uint64]bool)

    // First pass: identify committed transactions
    for _, entry := range entries {
        if commitEntry, ok := entry.(*WALCommitEntry); ok {
            committedTxns[commitEntry.TransactionID] = true
        }
    }

    // Second pass: replay only committed operations (idempotent)
    for _, entry := range entries {
        switch e := entry.(type) {
        case *WALInsertEntry:
            if committedTxns[e.TransactionID] {
                // Check if already applied (idempotence)
                if !storage.ContainsRows(e.TableName, e.Chunk.FirstRowID()) {
                    storage.InsertChunk(e.TableName, e.Chunk)
                }
            }
        case *WALDeleteEntry:
            if committedTxns[e.TransactionID] {
                storage.DeleteRows(e.TableName, e.RowIDs)
            }
        case *WALUpdateEntry:
            if committedTxns[e.TransactionID] {
                storage.UpdateRows(e.TableName, e.RowIDs, e.AfterValues)
            }
        }
    }

    return nil
}
```

**Trade-offs**:
- ✅ Pro: Chunk-granularity logging reduces WAL overhead (1 entry per 2048 rows vs 2048 entries)
- ✅ Pro: Group commit strategy batches fsync() calls within transactions (multiple operations → single fsync at COMMIT)
- ✅ Pro: Recovery idempotence ensures correctness even with multiple recovery attempts
- ⚠️ Con: Write latency increases for auto-commit operations (immediate fsync)
- ✅ Mitigation: Use explicit transactions for bulk operations to amortize fsync cost

### 5. Storage Layer Enhancements

**Current State**:
- `/internal/storage/storage.go` - In-memory storage exists
- `/internal/storage/table.go` - Table storage with row-oriented layout

**Required Enhancements**:

1. **RowID Generation and Tracking**:
```go
// Table maintains monotonic RowID counter
type Table struct {
    schema      *Schema
    rows        []Row
    tombstones  *Bitmap       // Tracks deleted rows
    nextRowID   uint64        // Monotonic counter for RowID generation
}

// Generate unique RowID for each inserted row
func (t *Table) generateRowID() RowID {
    id := RowID(t.nextRowID)
    t.nextRowID++
    return id
}
```

2. **Row Deletion with Tombstone Marking** (in-place deletion):
```go
// Mark rows as deleted without immediate removal (enables MVCC, efficient rollback)
func (t *Table) DeleteRows(rowIDs []RowID) error {
    for _, id := range rowIDs {
        t.tombstones.Set(id, true)  // Bitmap for O(1) deletion checks
    }
    return nil
}

// Scanner skips tombstoned rows during iteration
func (s *Scanner) Next() bool {
    for s.currentRowID < s.table.nextRowID {
        if !s.table.tombstones.Get(s.currentRowID) {
            return true  // Found live row
        }
        s.currentRowID++
    }
    return false
}
```

3. **Row Update with In-Place Modification**:
```go
func (t *Table) UpdateRows(rowIDs []RowID, values *DataChunk) error {
    for i, id := range rowIDs {
        row := t.GetRow(id)
        // Update columns from DataChunk
        for colIdx := 0; colIdx < values.ColumnCount(); colIdx++ {
            row.SetValue(colIdx, values.GetValue(colIdx, i))
        }
    }
    return nil
}
```

4. **Bulk Insert from DataChunk with RowID Assignment**:
```go
func (t *Table) InsertChunk(chunk *DataChunk) (int, error) {
    rowsInserted := 0
    for rowIdx := 0; rowIdx < chunk.Size(); rowIdx++ {
        row := t.NewRow()
        row.ID = t.generateRowID()  // Assign unique RowID
        for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
            row.SetValue(colIdx, chunk.GetValue(colIdx, rowIdx))
        }
        t.AppendRow(row)
        rowsInserted++
    }
    return rowsInserted, nil
}

// Idempotence check for recovery
func (t *Table) ContainsRows(firstRowID RowID) bool {
    return firstRowID < RowID(t.nextRowID) && !t.tombstones.Get(firstRowID)
}
```

## Testing Strategy

### Unit Tests
- `physical_delete_test.go`: WHERE clause evaluation (AND/OR/NOT, subqueries), empty result handling, NULL handling (three-valued logic)
- `physical_update_test.go`: SET expression evaluation, multi-column updates, type checking, BeforeValues tracking
- `physical_insert_test.go`: DataChunk batching, INSERT...SELECT pipeline, boundary conditions (0 rows, 1 row, 2048 rows, 10000 rows), RowID generation
- `storage_test.go`: Tombstone marking, RowID tracking, Scanner filtering of deleted rows
- `wal_recovery_test.go`: Idempotent recovery, transaction atomicity (rollback uncommitted), error type compatibility with duckdb-go

### Integration Tests (Phase D)
- Currently skipped tests in `/internal/executor/phase_d_*.go`:
  - `TestPhaseD_DELETE_ComplexWhere` - AND/OR/NOT combinations
  - `TestPhaseD_UPDATE_SubqueryWhere` - UPDATE with IN (SELECT ...)
  - `TestPhaseD_INSERT_BulkValues` - 10,000 rows in single INSERT
  - `TestPhaseD_INSERT_SELECT` - Cross-table insert
  - `TestPhaseD_DML_Transaction` - Rollback partial operations

### Deterministic Simulation Tests
```go
func TestDML_Timeout(t *testing.T) {
    mClock := quartz.NewMock(t)
    ctx, cancel := context.WithDeadline(context.Background(), mClock.Now().Add(5*time.Second))
    defer cancel()

    // Start long-running DELETE
    go db.ExecContext(ctx, "DELETE FROM large_table WHERE expensive_predicate()")

    // Advance clock past deadline
    mClock.Advance(6 * time.Second)

    // Verify timeout error
    assert.ErrorIs(t, err, context.DeadlineExceeded)
}
```

### Performance Benchmarks
```go
func BenchmarkBulkInsert(b *testing.B) {
    for n := range []int{100, 1000, 10000, 100000, 1000000} {
        b.Run(fmt.Sprintf("%d-rows", n), func(b *testing.B) {
            // Generate INSERT with N rows
            // Measure throughput (rows/sec)
            // Test both auto-commit (immediate fsync) and explicit transaction (group commit)
        })
    }
}
```

**Target Performance**:
- 100 rows (auto-commit): ~5-10ms (includes fsync overhead)
- 1,000 rows (transaction): ~20-50ms (single fsync at COMMIT)
- 10,000 rows (transaction): ~100-200ms
- 100,000 rows (transaction): ~1-2 seconds
- 1,000,000 rows (transaction): <30 seconds (target: 100K+ rows/sec with group commit)

**Note**: Auto-commit performance is limited by fsync() latency (~5-10ms per operation on typical SSD). Production workloads should use explicit transactions for bulk operations to achieve group commit optimization.

## Compatibility Verification

**Reference Implementation**: `/references/duckdb-go/`

**Verification Approach**:
1. Run identical queries against both dukdb-go and reference duckdb-go
2. Compare results (row count, column values, affected row counts)
3. Compare performance (within 2x is acceptable for pure Go vs cgo)
4. Verify error handling (same error types for same failure modes)

**Error Type Compatibility**:
- `ErrorTypeCatalog` - Table/schema not found (must match duckdb-go behavior)
- `ErrorTypeBinder` - Column not found, type mismatch in WHERE/SET (must match duckdb-go)
- `ErrorTypeMismatchType` - Type conversion failures (must match duckdb-go)
- `ErrorTypeInterrupt` - Context timeout/cancellation (must match duckdb-go)

**Test Matrix**:
| Operation | Test Case | duckdb-go (cgo) | dukdb-go (pure Go) | Match? |
|-----------|-----------|-----------------|-------------------|--------|
| INSERT | 1000 rows (auto-commit) | ~5ms | ~5-10ms | ✅ (fsync limited) |
| INSERT | 1000 rows (transaction) | ~10ms | ~20ms | ✅ (within 2x) |
| UPDATE | WHERE id > 100 | ~2ms | ~5ms | ✅ (within 2x) |
| DELETE | Complex WHERE | ~3ms | ~6ms | ✅ (within 2x) |
| INSERT | Non-existent table | ErrorTypeCatalog | ErrorTypeCatalog | ✅ |
| UPDATE | Type mismatch | ErrorTypeMismatchType | ErrorTypeMismatchType | ✅ |

## Risk Mitigation

### Risk: Data Loss on Crash
**Impact**: Critical - violates ACID guarantees
**Mitigation**:
- WAL synchronous writes (group commit: fsync after each transaction commit)
- Comprehensive crash recovery tests with deterministic simulation (quartz.Mock)
- Idempotent recovery ensures correctness even with multiple recovery attempts
- Transaction atomicity verified: uncommitted operations are rolled back

### Risk: Performance Regression
**Impact**: High - blocks production adoption
**Mitigation**:
- Continuous benchmarking against reference implementation
- Performance budgets in CI (fail if >2x slower than reference)
- Profiling integration (CPU, memory, disk I/O)
- Realistic targets account for fsync() latency (5-10ms per sync)

### Risk: WHERE Clause Evaluation Bugs
**Impact**: Critical - incorrect deletions/updates cause data corruption
**Mitigation**:
- Differential testing (compare results against reference duckdb-go)
- Property-based testing (generate random WHERE clauses, verify count matches SELECT)
- Manual test cases for edge conditions (NULLs with three-valued logic, empty results, type mismatches)
- Error type compatibility verification against duckdb-go v1.4.3

### Risk: Transaction Atomicity Violations
**Impact**: Critical - partial updates/deletes corrupt database state
**Mitigation**:
- Explicit transaction context tracking (Executor.currentTx)
- WAL recovery only replays committed transactions (two-pass recovery algorithm)
- Rollback tests verify uncommitted operations are not persisted
- Deterministic testing with controlled crash points (mid-transaction, mid-chunk)

## Implementation Order

**Rationale**: Build incrementally, validate at each step.

1. **WHERE Integration** (Foundation) - Enables filtering for UPDATE/DELETE
2. **Bulk INSERT** (High Value) - Delivers immediate performance win
3. **WAL Integration** (Correctness) - Ensures ACID compliance
4. **INSERT...SELECT** (Completeness) - Completes API surface

Each phase is independently testable and deliverable.
