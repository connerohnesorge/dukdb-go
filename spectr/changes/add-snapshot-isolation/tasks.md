# Tasks: Snapshot Isolation and MVCC

## Phase 1: MVCC Infrastructure

- [ ] 1.1 Create `internal/storage/version.go` with VersionedRow and VersionChain types
- [ ] 1.2 Implement version chain creation and linking methods
- [ ] 1.3 Add sync.RWMutex for per-chain concurrency control
- [ ] 1.4 Write unit tests for VersionedRow serialization
- [ ] 1.5 Write unit tests for VersionChain operations (add, traverse)

## Phase 2: Version Chain Implementation

- [ ] 2.1 Create `internal/engine/mvcc.go` with MVCCManager
- [ ] 2.2 Implement timestamp-based transaction ID assignment
- [ ] 2.3 Implement active transaction tracking with snapshot capture
- [ ] 2.4 Create MVCCTransaction extending Transaction
- [ ] 2.5 Add read-set and write-set tracking to MVCCTransaction
- [ ] 2.6 Integrate quartz.Clock for deterministic timestamp assignment
- [ ] 2.7 Write unit tests for MVCCManager operations

## Phase 3: Snapshot Read

- [ ] 3.1 Create `internal/storage/visibility.go` with VisibilityChecker
- [ ] 3.2 Implement Rule 1: Own uncommitted writes visible
- [ ] 3.3 Implement Rule 2: Other uncommitted writes invisible
- [ ] 3.4 Implement Rule 3: Versions committed after start invisible
- [ ] 3.5 Implement Rule 4: Active-at-start transaction versions invisible
- [ ] 3.6 Implement Rule 5: Deleted version visibility
- [ ] 3.7 Implement FindVisibleVersion chain traversal
- [ ] 3.8 Write unit tests for each visibility rule
- [ ] 3.9 Write integration test for snapshot isolation semantics

## Phase 4: Conflict Detection

- [ ] 4.1 Create `internal/engine/conflict.go` with ConflictDetector
- [ ] 4.2 Implement write tracking for committed transactions
- [ ] 4.3 Implement ValidateWriteSet for commit-time conflict check
- [ ] 4.4 Create WriteConflictError with conflict details
- [ ] 4.5 Implement RecordCommit to track successful commits
- [ ] 4.6 Write unit tests for conflict detection
- [ ] 4.7 Write integration test for write-write conflict scenario

## Phase 5: Garbage Collection

- [ ] 5.1 Create `internal/storage/vacuum.go` with Vacuum type
- [ ] 5.2 Implement GetLowWatermark from active transactions
- [ ] 5.3 Implement CanRemoveVersion visibility check
- [ ] 5.4 Implement CleanVersionChain to truncate old versions
- [ ] 5.5 Implement VacuumTable to process all chains
- [ ] 5.6 Add background vacuum goroutine with quartz ticker
- [ ] 5.7 Add vacuum statistics counters
- [ ] 5.8 Write unit tests for version removal eligibility
- [ ] 5.9 Write integration test for garbage collection

## Phase 6: Integration

- [ ] 6.1 Modify `internal/storage/table.go` to add versions map
- [ ] 6.2 Implement InsertVersioned method
- [ ] 6.3 Implement ReadVersioned method
- [ ] 6.4 Implement UpdateVersioned method
- [ ] 6.5 Implement DeleteVersioned method
- [ ] 6.6 Implement CommitVersions method
- [ ] 6.7 Modify `internal/engine/engine.go` to use MVCCManager
- [ ] 6.8 Add PRAGMA for MVCC enable/disable
- [ ] 6.9 Write end-to-end snapshot isolation test
- [ ] 6.10 Write end-to-end conflict detection test
- [ ] 6.11 Write end-to-end garbage collection test
- [ ] 6.12 Add performance benchmark comparing MVCC vs non-MVCC
- [ ] 6.13 Update documentation with MVCC usage guide
