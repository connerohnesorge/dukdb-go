# Design: Savepoint Support

## Context

Savepoints provide partial transaction rollback capability, allowing applications to create named checkpoints within a transaction. This is essential for complex transaction workflows where partial failures should not abort the entire transaction.

**Stakeholders**:
- Application developers needing fine-grained transaction control
- ORM frameworks that rely on savepoint semantics
- Users migrating from PostgreSQL/MySQL with savepoint usage

**Constraints**:
- Must remain pure Go (no CGO)
- Must integrate with existing Transaction and WAL systems
- Must maintain ACID properties for committed transactions
- Savepoint state is transaction-scoped (lost on rollback/commit)

## Goals / Non-Goals

**Goals**:
1. SAVEPOINT command creates named checkpoint in transaction
2. ROLLBACK TO SAVEPOINT reverts to checkpoint without ending transaction
3. RELEASE SAVEPOINT removes checkpoint (changes become permanent in transaction)
4. Nested savepoints work correctly (inner released with outer)
5. WAL correctly logs savepoint operations for crash recovery
6. Integration with deterministic testing via quartz clock

**Non-Goals**:
1. Autonomous transactions (transactions within transactions)
2. Distributed savepoints across multiple databases
3. Named transaction support
4. Savepoint persistence across sessions

## Decisions

### Decision 1: Savepoint Stack Structure

**Options**:
A. Map-based storage with name lookup - O(1) lookup, no ordering
B. Stack with name-indexed map - O(1) lookup, preserves order, more memory
C. Linked list with name lookup - O(n) lookup, preserves order
D. Slice-based stack - O(n) lookup, simple implementation

**Choice**: B - Stack with name-indexed map

**Rationale**:
- Savepoints are naturally nested (LIFO order)
- Need O(1) lookup by name for ROLLBACK TO and RELEASE
- Must track which savepoints are "inside" others for proper release
- Typical transaction has few savepoints (<10), memory is acceptable

**Implementation**:
```go
type Savepoint struct {
    Name      string
    UndoIndex int       // Index into transaction's undo log
    CreatedAt time.Time // For deterministic testing
}

type SavepointStack struct {
    stack    []*Savepoint          // Ordered list (newest at end)
    byName   map[string]*Savepoint // O(1) lookup
}

func (s *SavepointStack) Push(sp *Savepoint) error {
    if _, exists := s.byName[sp.Name]; exists {
        // Replace existing savepoint (PostgreSQL behavior)
        s.Remove(sp.Name)
    }
    s.stack = append(s.stack, sp)
    s.byName[sp.Name] = sp
    return nil
}

func (s *SavepointStack) Release(name string) error {
    sp, ok := s.byName[name]
    if !ok {
        return fmt.Errorf("savepoint %q does not exist", name)
    }

    // Remove this savepoint and all nested (newer) savepoints
    idx := s.indexOf(sp)
    for i := len(s.stack) - 1; i >= idx; i-- {
        delete(s.byName, s.stack[i].Name)
    }
    s.stack = s.stack[:idx]
    return nil
}
```

**Trade-offs**:
- + Fast lookup and ordered iteration
- + Clear semantics for nested releases
- - Slightly more memory than pure map or slice
- - Need to maintain consistency between stack and map

### Decision 2: WAL Integration for Savepoints

**Options**:
A. No WAL entries for savepoints - Simplest, no recovery support
B. WAL entries for savepoint creation only - Partial recovery
C. Full WAL entries for all savepoint operations - Complete recovery
D. Checkpoint-based savepoint markers - Ties to checkpoint system

**Choice**: C - Full WAL entries for all savepoint operations

**Rationale**:
- During recovery, must know transaction boundaries correctly
- ROLLBACK TO SAVEPOINT changes which operations are "active"
- RELEASE SAVEPOINT affects recovery of nested savepoints
- Without logging, crash during ROLLBACK TO could leave inconsistent state

**WAL Entry Types**:
```go
const (
    WAL_SAVEPOINT         WALEntryType = 92  // Create savepoint
    WAL_RELEASE_SAVEPOINT WALEntryType = 93  // Release savepoint
    WAL_ROLLBACK_SAVEPOINT WALEntryType = 94 // Rollback to savepoint
)

type SavepointEntry struct {
    TransactionID uint64
    Name          string
    UndoIndex     int       // For ROLLBACK_SAVEPOINT
    Timestamp     time.Time
}
```

**Recovery Algorithm**:
1. During replay, track savepoints per transaction
2. When WAL_ROLLBACK_SAVEPOINT encountered, mark operations after UndoIndex as "rolled back"
3. When WAL_RELEASE_SAVEPOINT encountered, remove savepoint from tracking
4. Only apply operations not marked as "rolled back"

**Trade-offs**:
- + Complete crash recovery support
- + Consistent with existing WAL design
- - More WAL entries for savepoint-heavy workloads
- - Slightly more complex recovery logic

### Decision 3: Nested Savepoint Handling

**Options**:
A. Flat namespace - All savepoints at same level, release one doesn't affect others
B. Implicit nesting - Later savepoints are "inside" earlier ones
C. Explicit nesting with parent reference - Tree structure
D. PostgreSQL-style implicit nesting - Release removes savepoint and all later ones

**Choice**: D - PostgreSQL-style implicit nesting

**Rationale**:
- Matches PostgreSQL and DuckDB behavior
- Simple mental model: savepoints form a stack
- RELEASE sp1 removes sp1 and any savepoints created after sp1
- ROLLBACK TO sp1 also removes later savepoints (they're invalid after rollback)

**Example**:
```sql
BEGIN;
INSERT INTO t VALUES (1);  -- undo log index 0
SAVEPOINT sp1;              -- savepoint at index 1
INSERT INTO t VALUES (2);  -- undo log index 1
SAVEPOINT sp2;              -- savepoint at index 2
INSERT INTO t VALUES (3);  -- undo log index 2
ROLLBACK TO SAVEPOINT sp1; -- undo operations 1,2; remove sp2
-- sp2 no longer exists
INSERT INTO t VALUES (4);  -- undo log index 1 (after rollback)
COMMIT;
-- Table contains: 1, 4
```

**Trade-offs**:
- + Matches expected behavior from other databases
- + Simple implementation (stack-based)
- - Users must understand implicit nesting
- - Cannot have independent savepoints at same "level"

### Decision 4: Memory Management for Savepoint State

**Options**:
A. Copy undo log at each savepoint - Complete isolation, high memory
B. Index-based reference - Low memory, undo log must be preserved
C. Copy-on-write with snapshots - Balanced, complex
D. Hybrid: index for recent, snapshot for old - Optimize common case

**Choice**: B - Index-based reference

**Rationale**:
- Undo log is already maintained by transaction for rollback
- Savepoint only needs to know "rollback to this index"
- No data duplication, minimal memory overhead
- Simple implementation

**Implementation**:
```go
type Transaction struct {
    id         uint64
    active     bool
    operations []UndoOperation  // Undo log
    savepoints *SavepointStack  // Savepoint tracking
}

func (t *Transaction) CreateSavepoint(name string, clock quartz.Clock) error {
    sp := &Savepoint{
        Name:      name,
        UndoIndex: len(t.operations), // Current position in undo log
        CreatedAt: clock.Now(),
    }
    return t.savepoints.Push(sp)
}

func (t *Transaction) RollbackToSavepoint(name string) error {
    sp, ok := t.savepoints.Get(name)
    if !ok {
        return fmt.Errorf("savepoint %q does not exist", name)
    }

    // Undo operations from end to savepoint index
    for i := len(t.operations) - 1; i >= sp.UndoIndex; i-- {
        if err := t.operations[i].Undo(); err != nil {
            return err
        }
    }

    // Truncate undo log
    t.operations = t.operations[:sp.UndoIndex]

    // Remove this savepoint and all later ones
    return t.savepoints.Release(name)
}
```

**Trade-offs**:
- + Minimal memory overhead
- + Simple implementation
- + Undo log already exists for full rollback
- - Undo log grows until commit (cannot free early)
- - Very long transactions with many savepoints accumulate memory

### Decision 5: Integration with Transaction System

**Options**:
A. Extend existing Transaction struct - Minimal changes
B. Create SavepointTransaction wrapper - Separation of concerns
C. Create new TransactionWithSavepoints type - Type safety
D. Make savepoints optional via interface - Flexibility

**Choice**: A - Extend existing Transaction struct

**Rationale**:
- Savepoints are intrinsically part of transaction semantics
- All transactions should support savepoints (even if not used)
- Minimal code changes, no new types to manage
- Consistent with how PostgreSQL/DuckDB treat savepoints

**Extended Transaction**:
```go
type Transaction struct {
    id         uint64
    active     bool
    operations []UndoOperation
    savepoints *SavepointStack  // NEW: savepoint tracking
    clock      quartz.Clock     // For deterministic timestamps
}

// TransactionManager changes
func (tm *TransactionManager) Savepoint(txn *Transaction, name string) error {
    if !txn.active {
        return errors.New("transaction not active")
    }

    // Log to WAL
    if tm.walWriter != nil {
        entry := &SavepointEntry{
            TransactionID: txn.id,
            Name:          name,
            Timestamp:     txn.clock.Now(),
        }
        if err := tm.walWriter.WriteEntry(entry); err != nil {
            return err
        }
    }

    return txn.CreateSavepoint(name, txn.clock)
}

func (tm *TransactionManager) RollbackToSavepoint(txn *Transaction, name string) error {
    if !txn.active {
        return errors.New("transaction not active")
    }

    sp, ok := txn.savepoints.Get(name)
    if !ok {
        return fmt.Errorf("savepoint %q does not exist", name)
    }

    // Log to WAL before rollback
    if tm.walWriter != nil {
        entry := &RollbackSavepointEntry{
            TransactionID: txn.id,
            Name:          name,
            UndoIndex:     sp.UndoIndex,
            Timestamp:     txn.clock.Now(),
        }
        if err := tm.walWriter.WriteEntry(entry); err != nil {
            return err
        }
    }

    return txn.RollbackToSavepoint(name)
}

func (tm *TransactionManager) ReleaseSavepoint(txn *Transaction, name string) error {
    if !txn.active {
        return errors.New("transaction not active")
    }

    // Log to WAL
    if tm.walWriter != nil {
        entry := &ReleaseSavepointEntry{
            TransactionID: txn.id,
            Name:          name,
            Timestamp:     txn.clock.Now(),
        }
        if err := tm.walWriter.WriteEntry(entry); err != nil {
            return err
        }
    }

    return txn.savepoints.Release(name)
}
```

**Trade-offs**:
- + Minimal API changes
- + Consistent transaction interface
- + Easy integration with existing code
- - Transaction struct grows (negligible)
- - All transactions allocate savepoint stack (lazy init mitigates)

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Undo log memory growth | Medium | Document best practices; encourage smaller transactions |
| Complex recovery logic | Medium | Extensive tests for crash scenarios |
| Savepoint name collisions | Low | Replace existing savepoint (PostgreSQL behavior) |
| Performance overhead | Low | Lazy initialization; index-based references |
| WAL bloat with many savepoints | Low | Savepoint entries are small (~50 bytes) |

## Migration Plan

### Phase 1: Infrastructure (1-2 days)
1. Add Savepoint struct and SavepointStack to internal/engine/
2. Add savepoint fields to Transaction struct
3. Add basic unit tests

### Phase 2: Transaction Manager Integration (1 day)
1. Add Savepoint/RollbackToSavepoint/ReleaseSavepoint to TransactionManager
2. Wire to Transaction methods
3. Add integration tests

### Phase 3: WAL Integration (1-2 days)
1. Add WAL entry types for savepoint operations
2. Implement serialization/deserialization
3. Update recovery to handle savepoint entries
4. Add recovery tests

### Phase 4: SQL Support (2-3 days)
1. Add AST nodes for savepoint statements
2. Add parser support
3. Add binder/planner/executor support
4. Add end-to-end SQL tests

### Phase 5: Testing and Polish (1 day)
1. Deterministic tests with quartz
2. Edge case tests (nested savepoints, duplicate names)
3. Performance benchmarks
4. Documentation

### Rollback Plan
- All savepoint code is additive
- Transaction struct changes are backward compatible
- WAL entries are new types (no conflict with existing)
- Can disable via feature flag if needed

## Open Questions

1. **Savepoint name limits?**
   - Answer: No limit on name length, but recommend <255 chars
   - Match PostgreSQL behavior

2. **Duplicate savepoint names?**
   - Answer: Replace existing savepoint (PostgreSQL behavior)
   - Alternative: Error on duplicate (stricter)

3. **Maximum savepoint depth?**
   - Answer: No artificial limit, bounded by memory
   - Practical limit ~1000 savepoints per transaction

4. **SAVEPOINT outside transaction?**
   - Answer: Error - "SAVEPOINT can only be used in transaction block"
   - Matches PostgreSQL behavior

5. **ROLLBACK TO non-existent savepoint?**
   - Answer: Error - "savepoint X does not exist"
   - Does not affect transaction state
