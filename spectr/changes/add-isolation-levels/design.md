# Design: Transaction Isolation Levels

## Context

This change adds configurable transaction isolation levels to dukdb-go, implementing the four standard SQL isolation levels. The implementation follows MVCC (Multi-Version Concurrency Control) principles to provide snapshot-based isolation with minimal locking, matching DuckDB's approach.

## Goals / Non-Goals

### Goals
- Implement all four SQL standard isolation levels
- Provide configurable isolation at transaction start
- Minimize lock contention through MVCC snapshots
- Support isolation level configuration per connection
- Detect and abort conflicting transactions at SERIALIZABLE level

### Non-Goals
- Predicate locking (use simpler conflict detection)
- Per-table isolation configuration
- Custom isolation levels beyond SQL standard
- Distributed transaction isolation (single-node only)

## Decisions

### Decision 1: Isolation Level Configuration

**Decision**: Support isolation level specification in BEGIN statement and as a connection-level default.

**Rationale**: Standard SQL allows isolation level in BEGIN, and DuckDB supports both transaction-level and session-level defaults. This provides maximum flexibility.

**Implementation**:
```go
// internal/parser/ast.go
type IsolationLevel int

const (
    IsolationLevelDefault IsolationLevel = iota
    IsolationLevelReadUncommitted
    IsolationLevelReadCommitted
    IsolationLevelRepeatableRead
    IsolationLevelSerializable
)

type BeginStmt struct {
    ReadOnly       bool
    IsolationLevel IsolationLevel
}

// internal/engine/connection.go
type ConnectionConfig struct {
    DefaultIsolation IsolationLevel
    // ...
}

// SQL syntax support:
// BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
// SET default_transaction_isolation = 'SERIALIZABLE';
// SHOW transaction_isolation;
```

**Alternatives Considered**:
- Per-statement isolation: Too complex, not standard SQL
- Only connection-level: Less flexible for mixed workloads

### Decision 2: Read Behavior per Level

**Decision**: Implement visibility rules using transaction timestamps and commit status.

**Rationale**: MVCC-style visibility avoids read locks entirely. Each isolation level defines which data versions are visible:

| Level | Visible Data |
|-------|-------------|
| READ UNCOMMITTED | All versions (including uncommitted) |
| READ COMMITTED | Committed versions as of statement start |
| REPEATABLE READ | Committed versions as of transaction start |
| SERIALIZABLE | Committed versions as of transaction start + conflict detection |

**Implementation**:
```go
// internal/storage/mvcc.go
type VersionVisibility struct {
    CreatedTxn   uint64
    DeletedTxn   uint64
    IsCommitted  bool
}

type VisibilityChecker interface {
    IsVisible(ver VersionVisibility, readingTxn *Transaction) bool
}

// Read Uncommitted: see all rows
type ReadUncommittedVisibility struct{}

func (v *ReadUncommittedVisibility) IsVisible(ver VersionVisibility, txn *Transaction) bool {
    // Visible if not deleted, or deleted by uncommitted txn
    return ver.DeletedTxn == 0 || !isCommitted(ver.DeletedTxn)
}

// Read Committed: see committed as of statement start
type ReadCommittedVisibility struct{}

func (v *ReadCommittedVisibility) IsVisible(ver VersionVisibility, txn *Transaction) bool {
    stmtTime := txn.CurrentStatementTime()
    return ver.IsCommitted && ver.CreatedTxn <= stmtTime &&
           (ver.DeletedTxn == 0 || ver.DeletedTxn > stmtTime)
}

// Repeatable Read: see committed as of txn start
type RepeatableReadVisibility struct{}

func (v *RepeatableReadVisibility) IsVisible(ver VersionVisibility, txn *Transaction) bool {
    txnStart := txn.StartTime()
    return ver.IsCommitted && ver.CreatedTxn <= txnStart &&
           (ver.DeletedTxn == 0 || ver.DeletedTxn > txnStart)
}

// Serializable: same as Repeatable Read, plus conflict detection at commit
type SerializableVisibility struct {
    RepeatableReadVisibility
}
```

### Decision 3: Write Conflict Detection

**Decision**: Use first-committer-wins strategy with write-write conflict detection for SERIALIZABLE.

**Rationale**: SERIALIZABLE requires preventing write skew and other anomalies. We detect conflicts at commit time by checking if any read rows were modified by concurrent transactions.

**Implementation**:
```go
// internal/storage/conflict_detector.go
type ConflictDetector struct {
    mu          sync.RWMutex
    writeSet    map[uint64]map[string]RowKeys  // txn -> table -> keys
    readSet     map[uint64]map[string]RowKeys  // txn -> table -> keys
}

type RowKeys struct {
    PKValues [][]any  // Primary key values for each row
}

func (cd *ConflictDetector) RegisterRead(txn *Transaction, table string, pk []any) {
    // Track reads for SERIALIZABLE conflict detection
    cd.mu.Lock()
    defer cd.mu.Unlock()
    if cd.readSet[txn.ID] == nil {
        cd.readSet[txn.ID] = make(map[string]RowKeys)
    }
    rk := cd.readSet[txn.ID][table]
    rk.PKValues = append(rk.PKValues, pk)
    cd.readSet[txn.ID][table] = rk
}

func (cd *ConflictDetector) RegisterWrite(txn *Transaction, table string, pk []any) {
    // Track writes for conflict detection
    cd.mu.Lock()
    defer cd.mu.Unlock()
    if cd.writeSet[txn.ID] == nil {
        cd.writeSet[txn.ID] = make(map[string]RowKeys)
    }
    wk := cd.writeSet[txn.ID][table]
    wk.PKValues = append(wk.PKValues, pk)
    cd.writeSet[txn.ID][table] = wk
}

func (cd *ConflictDetector) CheckConflicts(txn *Transaction) error {
    // At commit time, check if any reads overlap with other txn writes
    cd.mu.RLock()
    defer cd.mu.RUnlock()

    for table, reads := range cd.readSet[txn.ID] {
        for otherTxnID, writes := range cd.writeSet {
            if otherTxnID == txn.ID || !isCommittedAfter(otherTxnID, txn.StartTime()) {
                continue
            }
            if overlap(reads.PKValues, writes[table].PKValues) {
                return ErrSerializationFailure
            }
        }
    }
    return nil
}
```

### Decision 4: Lock Management

**Decision**: Use optimistic locking with conflict detection rather than pessimistic locks.

**Rationale**: MVCC allows readers to never block writers and vice versa. Only at SERIALIZABLE level do we need conflict detection, which happens at commit time rather than during execution.

**Implementation**:
```go
// internal/storage/lock_manager.go
type LockManager struct {
    mu         sync.RWMutex
    rowLocks   map[string]map[string]*RowLock  // table -> pk -> lock
}

type RowLock struct {
    HolderTxn uint64
    Mode      LockMode
    Waiters   []chan struct{}
}

type LockMode int

const (
    LockModeNone LockMode = iota
    LockModeShared
    LockModeExclusive
)

// For write operations (INSERT, UPDATE, DELETE)
func (lm *LockManager) AcquireExclusive(txn *Transaction, table string, pk []any) error {
    pkKey := serializePK(pk)
    lm.mu.Lock()

    if lm.rowLocks[table] == nil {
        lm.rowLocks[table] = make(map[string]*RowLock)
    }

    existing := lm.rowLocks[table][pkKey]
    if existing != nil && existing.HolderTxn != txn.ID {
        // Another transaction holds the lock
        waiter := make(chan struct{})
        existing.Waiters = append(existing.Waiters, waiter)
        lm.mu.Unlock()

        // Wait with timeout
        select {
        case <-waiter:
            return lm.AcquireExclusive(txn, table, pk)  // Retry
        case <-time.After(txn.LockTimeout):
            return ErrLockTimeout
        }
    }

    lm.rowLocks[table][pkKey] = &RowLock{
        HolderTxn: txn.ID,
        Mode:      LockModeExclusive,
    }
    lm.mu.Unlock()
    return nil
}

func (lm *LockManager) Release(txn *Transaction) {
    lm.mu.Lock()
    defer lm.mu.Unlock()

    for table, locks := range lm.rowLocks {
        for pk, lock := range locks {
            if lock.HolderTxn == txn.ID {
                // Wake up waiters
                for _, waiter := range lock.Waiters {
                    close(waiter)
                }
                delete(lm.rowLocks[table], pk)
            }
        }
    }
}
```

### Decision 5: Default Isolation Level

**Decision**: Default to SERIALIZABLE, matching DuckDB's behavior.

**Rationale**: SERIALIZABLE is the safest default and matches DuckDB. Applications requiring lower isolation can explicitly request it.

**Implementation**:
```go
// internal/engine/config.go
const DefaultIsolationLevel = IsolationLevelSerializable

// Can be overridden per-connection
func (c *Connection) SetDefaultIsolation(level IsolationLevel) {
    c.config.DefaultIsolation = level
}

// Or per-transaction
func (c *Connection) BeginWithIsolation(level IsolationLevel) (*Transaction, error) {
    return c.txnManager.Begin(level)
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| SERIALIZABLE conflict overhead | Medium | Only track reads/writes at SERIALIZABLE level |
| Lock timeout causing aborts | Medium | Configurable timeout, retry logic in application |
| Memory for read/write sets | Low | Clean up sets after transaction commit/abort |
| Complexity of visibility rules | Medium | Thorough testing per isolation level |

## Migration Plan

1. No migration needed - existing behavior preserved for transactions without explicit isolation
2. Default remains SERIALIZABLE (matches DuckDB)
3. Applications can opt into lower isolation levels explicitly

## Open Questions

- Should we support `SET TRANSACTION` in addition to `BEGIN TRANSACTION ISOLATION LEVEL`?
  - **Resolution**: Support both for PostgreSQL compatibility
- Lock timeout default value?
  - **Resolution**: 30 seconds (configurable via PRAGMA)
- Should isolation level be visible in transaction metadata?
  - **Resolution**: Yes, via `current_setting('transaction_isolation')`
