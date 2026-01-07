# Design: Snapshot Isolation and MVCC

## Context

The dukdb-go project currently has **no transaction isolation**. All transactions see the same mutable data, leading to dirty reads, lost updates, and inconsistent query results. This design adds Multi-Version Concurrency Control (MVCC) with snapshot isolation semantics.

**Stakeholders**:
- Users running concurrent transactions expecting isolation
- Analytical workloads requiring consistent point-in-time reads
- Applications requiring transactional integrity guarantees

**Constraints**:
- Must remain pure Go (no CGO)
- Must integrate with existing WAL for durability
- Must integrate with quartz for deterministic testing
- Memory overhead must be bounded (garbage collection required)
- Must not break existing non-MVCC code paths

## Goals / Non-Goals

**Goals**:
1. Snapshot isolation: Transactions see consistent data from start time
2. Write-write conflict detection at commit time
3. Version chain storage with efficient traversal
4. Garbage collection of old versions
5. Deterministic testing via quartz clock injection
6. Integration with existing storage and WAL layers

**Non-Goals**:
1. Serializable isolation (SSI) - higher isolation level, more complex
2. Optimistic concurrency without version chains - different approach
3. Distributed transactions - out of scope
4. Lock-based isolation - MVCC avoids locks for readers

## Decisions

### Decision 1: Version Chain Structure

**Options**:
A. Linked list of versions per row - Simple, O(n) traversal
B. Array of versions per row - Fast access, expensive insert
C. B-tree of versions per row - Balanced, complex
D. Append-only log with index - Write-optimized, read overhead

**Choice**: A - Linked list of versions (newest first)

**Rationale**:
- Most transactions read recent versions (short chains in practice)
- Simple implementation with clear semantics
- Memory efficient for small version counts
- Matches DuckDB's approach

**Data Structure**:
```go
type VersionedRow struct {
    // Row data
    Data      []any      // Column values
    RowID     uint64     // Stable row identifier

    // Version metadata
    TxnID     uint64     // Transaction that created this version
    CommitTS  uint64     // Commit timestamp (0 = uncommitted)
    DeletedBy uint64     // Transaction that deleted (0 = not deleted)
    DeleteTS  uint64     // Delete timestamp (0 = not deleted)

    // Chain pointer
    PrevPtr   *VersionedRow  // Previous version (nil = oldest)
}

type VersionChain struct {
    Head    *VersionedRow  // Newest version
    RowID   uint64         // Row identifier
    mu      sync.RWMutex   // Per-chain lock
}
```

**Memory Layout** (per version):
- Data: variable (reference to slice)
- RowID: 8 bytes
- TxnID: 8 bytes
- CommitTS: 8 bytes
- DeletedBy: 8 bytes
- DeleteTS: 8 bytes
- PrevPtr: 8 bytes
- Total overhead: ~48 bytes per version

### Decision 2: Transaction ID Assignment

**Options**:
A. Monotonic counter - Simple, requires synchronization
B. Timestamp-based IDs - Natural ordering, clock dependency
C. Hybrid (counter + timestamp) - Complex but flexible
D. UUID-based - No coordination, no ordering

**Choice**: B - Timestamp-based with monotonic guarantee

**Rationale**:
- Natural ordering for visibility checks
- Integrates with quartz for deterministic testing
- Single point of timestamp assignment (MVCCManager)
- Matches DuckDB's approach

**Implementation**:
```go
type MVCCManager struct {
    // Timestamp counters
    lastTS    uint64       // Last assigned timestamp
    clock     quartz.Clock // Injected clock for testing

    // Active transaction tracking
    activeTxns map[uint64]*MVCCTransaction
    mu         sync.RWMutex
}

func (m *MVCCManager) NextTimestamp() uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()

    // Get current time in nanoseconds
    now := uint64(m.clock.Now().UnixNano())

    // Ensure monotonicity
    if now <= m.lastTS {
        now = m.lastTS + 1
    }
    m.lastTS = now

    return now
}

func (m *MVCCManager) BeginTransaction() *MVCCTransaction {
    m.mu.Lock()
    defer m.mu.Unlock()

    startTS := m.NextTimestamp()

    // Capture snapshot of active transactions
    activeSnapshot := make([]uint64, 0, len(m.activeTxns))
    for txnID := range m.activeTxns {
        activeSnapshot = append(activeSnapshot, txnID)
    }

    txn := &MVCCTransaction{
        Transaction: &Transaction{id: startTS, active: true},
        StartTS:     startTS,
        ActiveAtStart: activeSnapshot,
        ReadSet:     make(map[string][]uint64),
        WriteSet:    make(map[string][]uint64),
    }

    m.activeTxns[startTS] = txn
    return txn
}
```

**Trap Points for Testing**:
```go
_ = m.clock.Now()  // Tag: "MVCC", "timestamp", "assign"
_ = m.clock.Now()  // Tag: "MVCC", "txn", "begin"
_ = m.clock.Now()  // Tag: "MVCC", "txn", "commit"
```

### Decision 3: Visibility Rules

**Options**:
A. Simple timestamp comparison - Fast, may see uncommitted
B. Active transaction list check - Correct, requires snapshot
C. Commit dependency tracking - Complex, serializable
D. Hybrid: timestamp + active check - Correct and efficient

**Choice**: D - Hybrid approach

**Rationale**:
- Timestamp comparison for quick rejection
- Active transaction snapshot for correctness
- No need for serializable dependency tracking
- Standard snapshot isolation semantics

**Visibility Algorithm**:
```go
type VisibilityChecker struct {
    txnStartTS     uint64     // Transaction start timestamp
    activeAtStart  []uint64   // Snapshot of active transactions at start
}

func (vc *VisibilityChecker) IsVisible(version *VersionedRow) bool {
    // Rule 1: Our own uncommitted writes are visible
    if version.TxnID == vc.txnStartTS && version.CommitTS == 0 {
        return true
    }

    // Rule 2: Uncommitted versions from other transactions are NOT visible
    if version.CommitTS == 0 {
        return false
    }

    // Rule 3: Versions committed after we started are NOT visible
    if version.CommitTS > vc.txnStartTS {
        return false
    }

    // Rule 4: Versions from transactions active when we started are NOT visible
    // (they may have committed during our execution)
    for _, activeTxn := range vc.activeAtStart {
        if version.TxnID == activeTxn {
            return false
        }
    }

    // Rule 5: Deleted versions - check if delete is visible
    if version.DeletedBy != 0 {
        // If deleted by us, not visible (we deleted it)
        if version.DeletedBy == vc.txnStartTS {
            return false
        }
        // If delete committed before we started, not visible
        if version.DeleteTS != 0 && version.DeleteTS <= vc.txnStartTS {
            return false
        }
    }

    return true
}

// FindVisibleVersion traverses chain to find visible version
func (vc *VisibilityChecker) FindVisibleVersion(chain *VersionChain) *VersionedRow {
    chain.mu.RLock()
    defer chain.mu.RUnlock()

    current := chain.Head
    for current != nil {
        if vc.IsVisible(current) {
            return current
        }
        current = current.PrevPtr
    }
    return nil  // No visible version (row doesn't exist for this txn)
}
```

### Decision 4: Write Conflict Detection

**Options**:
A. First-writer-wins - Reject later writes immediately
B. First-committer-wins - Check at commit time only
C. Optimistic with validation - Write freely, validate at commit
D. Pessimistic locking - Lock before write

**Choice**: B - First-committer-wins with commit-time validation

**Rationale**:
- Allows maximum concurrency during execution
- Conflicts detected at commit when outcome is certain
- Standard approach for MVCC systems
- No locks required for writers during execution

**Implementation**:
```go
type ConflictDetector struct {
    // Track writes by committed transactions
    committedWrites map[string]map[uint64]uint64  // table -> rowID -> commitTS
    mu              sync.RWMutex
}

func (cd *ConflictDetector) ValidateWriteSet(txn *MVCCTransaction) error {
    cd.mu.RLock()
    defer cd.mu.RUnlock()

    for table, rowIDs := range txn.WriteSet {
        tableWrites, ok := cd.committedWrites[table]
        if !ok {
            continue
        }

        for _, rowID := range rowIDs {
            commitTS, ok := tableWrites[rowID]
            if !ok {
                continue
            }

            // Conflict: another transaction committed a write to this row
            // after our transaction started
            if commitTS > txn.StartTS {
                return &WriteConflictError{
                    Table:       table,
                    RowID:       rowID,
                    OurTxnID:    txn.ID(),
                    ConflictTS:  commitTS,
                }
            }
        }
    }

    return nil
}

func (cd *ConflictDetector) RecordCommit(txn *MVCCTransaction) {
    cd.mu.Lock()
    defer cd.mu.Unlock()

    for table, rowIDs := range txn.WriteSet {
        if _, ok := cd.committedWrites[table]; !ok {
            cd.committedWrites[table] = make(map[uint64]uint64)
        }
        for _, rowID := range rowIDs {
            cd.committedWrites[table][rowID] = txn.CommitTS
        }
    }
}

// WriteConflictError provides details about the conflict
type WriteConflictError struct {
    Table      string
    RowID      uint64
    OurTxnID   uint64
    ConflictTS uint64
}

func (e *WriteConflictError) Error() string {
    return fmt.Sprintf(
        "write conflict on table %s row %d: "+
        "transaction %d conflicts with commit at %d",
        e.Table, e.RowID, e.OurTxnID, e.ConflictTS,
    )
}
```

### Decision 5: Garbage Collection (Vacuum)

**Options**:
A. Background thread - Continuous cleanup, CPU overhead
B. Triggered on commit - Piggyback on commits, latency
C. Periodic batch - Scheduled cleanup, memory spikes
D. Hybrid: low-watermark tracking + lazy cleanup

**Choice**: D - Hybrid with low-watermark tracking

**Rationale**:
- Track minimum active transaction timestamp (low-watermark)
- Versions older than low-watermark are safe to remove
- Lazy cleanup during table scans reduces overhead
- Background vacuum for proactive cleanup when idle

**Implementation**:
```go
type Vacuum struct {
    mvcc         *MVCCManager
    clock        quartz.Clock
    versionsGCd  uint64  // Statistics counter
    bytesFreed   uint64  // Statistics counter
}

func (v *Vacuum) GetLowWatermark() uint64 {
    v.mvcc.mu.RLock()
    defer v.mvcc.mu.RUnlock()

    if len(v.mvcc.activeTxns) == 0 {
        return v.mvcc.lastTS  // No active transactions, all versions reclaimable
    }

    minTS := uint64(^uint64(0))  // Max uint64
    for _, txn := range v.mvcc.activeTxns {
        if txn.StartTS < minTS {
            minTS = txn.StartTS
        }
    }
    return minTS
}

func (v *Vacuum) CanRemoveVersion(version *VersionedRow, lowWatermark uint64) bool {
    // Version must be committed
    if version.CommitTS == 0 {
        return false
    }

    // Version must be superseded by a newer committed version
    // AND committed before the low-watermark
    return version.CommitTS < lowWatermark
}

func (v *Vacuum) CleanVersionChain(chain *VersionChain) int {
    chain.mu.Lock()
    defer chain.mu.Unlock()

    lowWatermark := v.GetLowWatermark()
    _ = v.clock.Now()  // Tag: "vacuum", "chain", "start"

    removed := 0
    current := chain.Head

    // Find the cutoff point
    for current != nil && current.PrevPtr != nil {
        prev := current.PrevPtr

        if v.CanRemoveVersion(prev, lowWatermark) {
            // Truncate the chain here
            current.PrevPtr = nil
            removed++

            // Count remaining removed versions
            for prev.PrevPtr != nil {
                prev = prev.PrevPtr
                removed++
            }
            break
        }
        current = current.PrevPtr
    }

    if removed > 0 {
        _ = v.clock.Now()  // Tag: "vacuum", "chain", "cleaned"
    }

    v.versionsGCd += uint64(removed)
    return removed
}

// RunBackgroundVacuum runs periodic garbage collection
func (v *Vacuum) RunBackgroundVacuum(ctx context.Context, tables []*Table) {
    ticker := v.clock.NewTicker(time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            _ = v.clock.Now()  // Tag: "vacuum", "background", "start"

            for _, table := range tables {
                v.VacuumTable(table)
            }

            _ = v.clock.Now()  // Tag: "vacuum", "background", "complete"
        }
    }
}

func (v *Vacuum) VacuumTable(table *Table) {
    table.versionsMu.RLock()
    chains := make([]*VersionChain, 0, len(table.versions))
    for _, chain := range table.versions {
        chains = append(chains, chain)
    }
    table.versionsMu.RUnlock()

    for _, chain := range chains {
        v.CleanVersionChain(chain)
    }
}
```

### Decision 6: Integration with Storage Layer

**Options**:
A. Replace Table completely - Clean slate, high risk
B. Add versioned methods alongside existing - Gradual migration
C. Wrapper layer over existing storage - Abstraction overhead
D. Interface-based with version store - Flexible but complex

**Choice**: B - Add versioned methods alongside existing

**Rationale**:
- Minimal risk to existing functionality
- Gradual migration path
- Clear separation of MVCC vs non-MVCC paths
- Easy to test incrementally

**Storage Interface Changes**:
```go
// Table additions for MVCC support
type Table struct {
    // ... existing fields ...

    // MVCC storage
    versions   map[uint64]*VersionChain  // rowID -> version chain
    versionsMu sync.RWMutex

    // MVCC enabled flag
    mvccEnabled bool
}

// InsertVersioned creates a new versioned row
func (t *Table) InsertVersioned(txn *MVCCTransaction, values []any) (uint64, error) {
    rowID := t.nextRowID.Add(1)

    version := &VersionedRow{
        Data:     values,
        RowID:    rowID,
        TxnID:    txn.ID(),
        CommitTS: 0,  // Uncommitted
    }

    chain := &VersionChain{
        Head:  version,
        RowID: rowID,
    }

    t.versionsMu.Lock()
    t.versions[rowID] = chain
    t.versionsMu.Unlock()

    // Track in write set for conflict detection
    txn.RecordWrite(t.name, rowID)

    return rowID, nil
}

// ReadVersioned returns visible version for transaction
func (t *Table) ReadVersioned(txn *MVCCTransaction, rowID uint64) ([]any, error) {
    t.versionsMu.RLock()
    chain, ok := t.versions[rowID]
    t.versionsMu.RUnlock()

    if !ok {
        return nil, ErrRowNotFound
    }

    checker := txn.VisibilityChecker()
    visible := checker.FindVisibleVersion(chain)

    if visible == nil {
        return nil, ErrRowNotVisible
    }

    // Track in read set for read-only transaction optimization
    txn.RecordRead(t.name, rowID)

    return visible.Data, nil
}

// UpdateVersioned creates new version in chain
func (t *Table) UpdateVersioned(txn *MVCCTransaction, rowID uint64, values []any) error {
    t.versionsMu.Lock()
    defer t.versionsMu.Unlock()

    chain, ok := t.versions[rowID]
    if !ok {
        return ErrRowNotFound
    }

    chain.mu.Lock()
    defer chain.mu.Unlock()

    // Create new version pointing to current head
    newVersion := &VersionedRow{
        Data:     values,
        RowID:    rowID,
        TxnID:    txn.ID(),
        CommitTS: 0,
        PrevPtr:  chain.Head,
    }

    chain.Head = newVersion

    txn.RecordWrite(t.name, rowID)
    return nil
}

// DeleteVersioned marks row as deleted
func (t *Table) DeleteVersioned(txn *MVCCTransaction, rowID uint64) error {
    t.versionsMu.RLock()
    chain, ok := t.versions[rowID]
    t.versionsMu.RUnlock()

    if !ok {
        return ErrRowNotFound
    }

    chain.mu.Lock()
    defer chain.mu.Unlock()

    // Mark head version as deleted by this transaction
    chain.Head.DeletedBy = txn.ID()

    txn.RecordWrite(t.name, rowID)
    return nil
}

// CommitVersions marks all versions from a transaction as committed
func (t *Table) CommitVersions(txn *MVCCTransaction, commitTS uint64) {
    t.versionsMu.RLock()
    defer t.versionsMu.RUnlock()

    for _, rowID := range txn.WriteSet[t.name] {
        chain, ok := t.versions[rowID]
        if !ok {
            continue
        }

        chain.mu.Lock()
        // Find and commit versions from this transaction
        current := chain.Head
        for current != nil {
            if current.TxnID == txn.ID() && current.CommitTS == 0 {
                current.CommitTS = commitTS
            }
            if current.DeletedBy == txn.ID() && current.DeleteTS == 0 {
                current.DeleteTS = commitTS
            }
            current = current.PrevPtr
        }
        chain.mu.Unlock()
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Memory growth from versions | High | Low-watermark vacuum, version count limits |
| Long-running transaction blocking GC | Medium | Transaction timeout, watermark monitoring |
| Version chain traversal overhead | Medium | Keep chains short via aggressive GC |
| Conflict detection false positives | Low | Row-level granularity, not page-level |
| Complex debugging with versions | Medium | Version history inspection tooling |

## Migration Plan

### Phase 1: MVCC Infrastructure (Days 1-3)
1. Create `internal/storage/version.go` - Version chain structure
2. Create `internal/engine/mvcc.go` - MVCC manager
3. Unit tests for version creation and linking

### Phase 2: Visibility Implementation (Days 4-6)
1. Create `internal/storage/visibility.go` - Visibility checker
2. Implement visibility rules
3. Test snapshot isolation semantics

### Phase 3: Conflict Detection (Days 7-9)
1. Create `internal/engine/conflict.go` - Conflict detector
2. Implement write-set tracking
3. Test write-write conflict scenarios

### Phase 4: Garbage Collection (Days 10-12)
1. Create `internal/storage/vacuum.go` - Vacuum implementation
2. Implement low-watermark tracking
3. Test version cleanup

### Phase 5: Storage Integration (Days 13-16)
1. Modify `internal/storage/table.go` - Add versioned methods
2. Integrate with executor
3. End-to-end tests

### Phase 6: Engine Integration (Days 17-20)
1. Modify `internal/engine/engine.go` - MVCC transaction support
2. PRAGMA for MVCC configuration
3. Performance benchmarks

### Rollback Plan
- MVCC disabled by default via `mvccEnabled` flag
- Existing non-versioned paths unchanged
- Feature toggle per-database or per-transaction

## Open Questions

1. **Version chain length limit?**
   - Answer: Soft limit of 100 versions per row, force vacuum above threshold
   - Prevents pathological cases with long-running transactions

2. **Read-only transaction optimization?**
   - Answer: Read-only transactions don't need write-set tracking
   - Can be detected at first statement and optimized

3. **Serializable isolation as future work?**
   - Answer: Yes, can add SSI predicate locks on top of MVCC
   - Current design doesn't preclude future enhancement

4. **Integration with WAL?**
   - Answer: WAL entries include version metadata (TxnID, CommitTS)
   - Recovery reconstructs version chains from WAL
