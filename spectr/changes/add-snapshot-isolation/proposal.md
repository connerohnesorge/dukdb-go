# Change: Add Snapshot Isolation and MVCC

## Why

The current dukdb-go implementation has **no true transaction isolation**:

**Current State Analysis**:
- `internal/engine/engine.go` TransactionManager tracks IDs but provides no isolation:
  ```go
  func (tm *TransactionManager) Commit(txn *Transaction) error {
      delete(tm.active, txn.id)  // Just removes from map - no visibility control!
      txn.active = false
      return nil
  }
  ```
- All connections see the same data regardless of transaction state
- No protection against dirty reads, non-repeatable reads, or phantom reads
- Write-write conflicts silently corrupt data (last writer wins)
- No snapshot read capability for consistent analytical queries

**duckdb-go Reference Behavior**:
- DuckDB implements full MVCC with snapshot isolation
- Transactions see a consistent snapshot from their start time
- Write-write conflicts are detected and rejected
- Old versions are garbage collected via vacuum

**Required Capabilities**:
1. Snapshot reads: Transactions see consistent data from transaction start
2. Version chains: Multiple versions of rows coexist
3. Conflict detection: Write-write conflicts detected at commit time
4. Garbage collection: Old versions cleaned up automatically

## What Changes

### 1. Version Chain Structure (internal/storage/version.go - NEW)

```go
// VersionedRow represents a row with version information
type VersionedRow struct {
    Data       []any     // Row values
    TxnID      uint64    // Transaction that created this version
    CommitTS   uint64    // Commit timestamp (0 if uncommitted)
    DeletedBy  uint64    // Transaction that deleted (0 if not deleted)
    PrevPtr    *VersionedRow // Previous version pointer
}

// VersionChain manages multiple versions of a row
type VersionChain struct {
    Head    *VersionedRow
    RowID   uint64
    mu      sync.RWMutex
}
```

### 2. Transaction Timestamp Assignment (internal/engine/mvcc.go - NEW)

```go
// MVCCManager manages multi-version concurrency control
type MVCCManager struct {
    nextTxnID   uint64    // Monotonically increasing
    nextCommit  uint64    // Commit timestamp counter
    activeTxns  map[uint64]*MVCCTransaction
    mu          sync.RWMutex
    clock       quartz.Clock
}

// MVCCTransaction extends Transaction with MVCC semantics
type MVCCTransaction struct {
    *Transaction
    StartTS     uint64    // Transaction start timestamp
    CommitTS    uint64    // Assigned at commit (0 until committed)
    ReadSet     map[string][]uint64  // table -> rowIDs read
    WriteSet    map[string][]uint64  // table -> rowIDs written
}
```

### 3. Visibility Rules (internal/storage/visibility.go - NEW)

```go
// VisibilityChecker determines which versions are visible to a transaction
type VisibilityChecker struct {
    txnStartTS  uint64
    activeTxns  []uint64  // Snapshot of active transactions at start
}

// IsVisible returns true if the version is visible to this transaction
func (vc *VisibilityChecker) IsVisible(version *VersionedRow) bool {
    // Rule 1: Version must be committed before this transaction started
    if version.CommitTS == 0 || version.CommitTS > vc.txnStartTS {
        return false
    }
    // Rule 2: Version must not be deleted, or deleted after this txn started
    if version.DeletedBy != 0 && version.DeletedBy <= vc.txnStartTS {
        return false
    }
    return true
}
```

### 4. Write Conflict Detection (internal/engine/conflict.go - NEW)

```go
// ConflictDetector checks for write-write conflicts at commit time
type ConflictDetector struct {
    activeWrites map[string]map[uint64]uint64  // table -> rowID -> txnID
    mu           sync.RWMutex
}

// CheckConflict returns an error if another transaction modified the row
func (cd *ConflictDetector) CheckConflict(
    txn *MVCCTransaction,
    table string,
    rowID uint64,
) error {
    cd.mu.RLock()
    defer cd.mu.RUnlock()

    if rowWrites, ok := cd.activeWrites[table]; ok {
        if writerID, ok := rowWrites[rowID]; ok {
            if writerID != txn.ID() {
                return ErrWriteConflict
            }
        }
    }
    return nil
}
```

### 5. Garbage Collection (internal/storage/vacuum.go - NEW)

```go
// Vacuum removes old versions that are no longer visible
type Vacuum struct {
    minActiveTS  uint64    // Minimum start timestamp of active transactions
    versionsGCd  uint64    // Counter for statistics
    clock        quartz.Clock
}

// CollectGarbage removes old versions from the version chain
func (v *Vacuum) CollectGarbage(chain *VersionChain) {
    chain.mu.Lock()
    defer chain.mu.Unlock()

    // Keep versions that any active transaction might need
    current := chain.Head
    for current != nil && current.PrevPtr != nil {
        if v.canRemove(current.PrevPtr) {
            current.PrevPtr = nil  // Truncate chain
            v.versionsGCd++
            break
        }
        current = current.PrevPtr
    }
}

func (v *Vacuum) canRemove(version *VersionedRow) bool {
    // Can remove if committed before all active transactions
    return version.CommitTS != 0 && version.CommitTS < v.minActiveTS
}
```

### 6. Storage Layer Integration (internal/storage/table.go - MODIFIED)

```go
// Table now uses versioned storage
type Table struct {
    // ... existing fields ...
    versions    map[uint64]*VersionChain  // rowID -> version chain
    versionsMu  sync.RWMutex
}

// InsertVersioned creates a new versioned row
func (t *Table) InsertVersioned(txn *MVCCTransaction, values []any) (uint64, error)

// ReadVersioned returns the visible version for a transaction
func (t *Table) ReadVersioned(txn *MVCCTransaction, rowID uint64) ([]any, error)

// UpdateVersioned creates a new version, keeping old version in chain
func (t *Table) UpdateVersioned(txn *MVCCTransaction, rowID uint64, values []any) error

// DeleteVersioned marks a version as deleted
func (t *Table) DeleteVersioned(txn *MVCCTransaction, rowID uint64) error
```

## Impact

- **Affected specs**: execution-engine (MODIFIED), NEW snapshot-isolation spec
- **Affected code**:
  - NEW: `internal/storage/version.go` (~200 lines)
  - NEW: `internal/storage/visibility.go` (~100 lines)
  - NEW: `internal/storage/vacuum.go` (~150 lines)
  - NEW: `internal/engine/mvcc.go` (~300 lines)
  - NEW: `internal/engine/conflict.go` (~150 lines)
  - MODIFIED: `internal/storage/table.go` (~200 lines)
  - MODIFIED: `internal/engine/engine.go` (~100 lines)
  - NEW: Tests (~600 lines)

- **Dependencies**:
  - `internal/storage/` - Version chain storage
  - `internal/engine/` - Transaction management
  - `quartz.Clock` - Deterministic timestamp assignment

- **Performance considerations**:
  - Memory overhead: ~40 bytes per version (pointer + timestamps)
  - Read overhead: Version chain traversal (typically 1-3 versions)
  - Write overhead: Version creation + conflict check
  - GC overhead: Background vacuum process

## Breaking Changes

None. All changes are additive:
- Existing single-version reads continue to work
- MVCC can be enabled per-transaction or globally via config
- Default behavior unchanged until explicitly enabled
