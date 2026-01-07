# Tasks: Snapshot Isolation and MVCC

## Phase 1: MVCC Infrastructure

- [x] 1.1 Create `internal/storage/version.go` with VersionedRow and VersionChain types
- [x] 1.2 Implement version chain creation and linking methods
- [x] 1.3 Add sync.RWMutex for per-chain concurrency control
- [x] 1.4 Write unit tests for VersionedRow serialization
- [x] 1.5 Write unit tests for VersionChain operations (add, traverse)

## Phase 2: Version Chain Implementation

- [x] 2.1 Create `internal/engine/mvcc.go` with MVCCManager
- [x] 2.2 Implement timestamp-based transaction ID assignment
- [x] 2.3 Implement active transaction tracking with snapshot capture
- [x] 2.4 Create MVCCTransaction extending Transaction
- [x] 2.5 Add read-set and write-set tracking to MVCCTransaction
- [x] 2.6 Integrate quartz.Clock for deterministic timestamp assignment
- [x] 2.7 Write unit tests for MVCCManager operations

## Phase 3: Snapshot Read

- [x] 3.1 Create `internal/storage/visibility.go` with VisibilityChecker
- [x] 3.2 Implement Rule 1: Own uncommitted writes visible
- [x] 3.3 Implement Rule 2: Other uncommitted writes invisible
- [x] 3.4 Implement Rule 3: Versions committed after start invisible
- [x] 3.5 Implement Rule 4: Active-at-start transaction versions invisible
- [x] 3.6 Implement Rule 5: Deleted version visibility
- [x] 3.7 Implement FindVisibleVersion chain traversal
- [x] 3.8 Write unit tests for each visibility rule
- [x] 3.9 Write integration test for snapshot isolation semantics

## Phase 4: Conflict Detection

- [x] 4.1 Create `internal/engine/conflict.go` with ConflictDetector
- [x] 4.2 Implement write tracking for committed transactions
- [x] 4.3 Implement ValidateWriteSet for commit-time conflict check
- [x] 4.4 Create WriteConflictError with conflict details
- [x] 4.5 Implement RecordCommit to track successful commits
- [x] 4.6 Write unit tests for conflict detection
- [x] 4.7 Write integration test for write-write conflict scenario

## Phase 5: Garbage Collection

- [x] 5.1 Create `internal/storage/vacuum.go` with Vacuum type
- [x] 5.2 Implement GetLowWatermark from active transactions
- [x] 5.3 Implement CanRemoveVersion visibility check
- [x] 5.4 Implement CleanVersionChain to truncate old versions
- [x] 5.5 Implement VacuumTable to process all chains
- [x] 5.6 Add background vacuum goroutine with quartz ticker
- [x] 5.7 Add vacuum statistics counters
- [x] 5.8 Write unit tests for version removal eligibility
- [x] 5.9 Write integration test for garbage collection

## Phase 6: Integration

- [x] 6.1 Modify `internal/storage/table.go` to add versions map
- [x] 6.2 Implement InsertVersioned method
- [x] 6.3 Implement ReadVersioned method
- [x] 6.4 Implement UpdateVersioned method
- [x] 6.5 Implement DeleteVersioned method
- [x] 6.6 Implement CommitVersions method
- [x] 6.7 Modify `internal/engine/engine.go` to use MVCCManager
- [x] 6.8 Add PRAGMA for MVCC enable/disable
- [x] 6.9 Write end-to-end snapshot isolation test
- [x] 6.10 Write end-to-end conflict detection test
- [x] 6.11 Write end-to-end garbage collection test
- [x] 6.12 Add performance benchmark comparing MVCC vs non-MVCC
- [x] 6.13 Update documentation with MVCC usage guide
