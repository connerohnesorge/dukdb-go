# Tasks: Transaction Isolation Levels

## Phase 1: Isolation Level Infrastructure

- [ ] 1.1 Add isolation level types to AST
  - [ ] 1.1.1 Define `IsolationLevel` enum in `internal/parser/ast.go`
  - [ ] 1.1.2 Add `IsolationLevel` field to `BeginStmt` struct
  - [ ] 1.1.3 Add constants for all four isolation levels
  - [ ] 1.1.4 Add `String()` method for IsolationLevel

- [ ] 1.2 Update parser for isolation level syntax
  - [ ] 1.2.1 Parse `ISOLATION LEVEL` clause in BEGIN statement
  - [ ] 1.2.2 Support `READ UNCOMMITTED` keyword parsing
  - [ ] 1.2.3 Support `READ COMMITTED` keyword parsing
  - [ ] 1.2.4 Support `REPEATABLE READ` keyword parsing
  - [ ] 1.2.5 Support `SERIALIZABLE` keyword parsing
  - [ ] 1.2.6 Add parser tests for all isolation level syntax variants

- [ ] 1.3 Add SET/SHOW for isolation configuration
  - [ ] 1.3.1 Parse `SET default_transaction_isolation = level`
  - [ ] 1.3.2 Parse `SHOW transaction_isolation`
  - [ ] 1.3.3 Store default isolation in connection config
  - [ ] 1.3.4 Add tests for SET/SHOW isolation level

- [ ] 1.4 Update transaction manager
  - [ ] 1.4.1 Add `IsolationLevel` field to `Transaction` struct
  - [ ] 1.4.2 Add `BeginWithIsolation(level)` method to txn manager
  - [ ] 1.4.3 Add `GetIsolationLevel()` method to Transaction
  - [ ] 1.4.4 Initialize transaction with configured isolation level
  - [ ] 1.4.5 Add tests for transaction creation with isolation levels

## Phase 2: READ UNCOMMITTED

- [ ] 2.1 Create visibility checker interface
  - [ ] 2.1.1 Define `VisibilityChecker` interface in `internal/storage/mvcc.go`
  - [ ] 2.1.2 Define `VersionInfo` struct with creation/deletion txn info
  - [ ] 2.1.3 Add `IsVisible(version, transaction) bool` method signature

- [ ] 2.2 Implement READ UNCOMMITTED visibility
  - [ ] 2.2.1 Create `ReadUncommittedVisibility` struct
  - [ ] 2.2.2 Implement visibility: all non-deleted rows are visible
  - [ ] 2.2.3 Handle uncommitted deletes (row still visible)
  - [ ] 2.2.4 Add unit tests for READ UNCOMMITTED visibility

- [ ] 2.3 Integrate visibility into table scan
  - [ ] 2.3.1 Add visibility checker to scan operators
  - [ ] 2.3.2 Apply visibility filter during row iteration
  - [ ] 2.3.3 Add integration tests for READ UNCOMMITTED scans

## Phase 3: READ COMMITTED

- [ ] 3.1 Add statement-level timestamps
  - [ ] 3.1.1 Add `CurrentStatementTime()` to Transaction
  - [ ] 3.1.2 Update statement time at start of each statement
  - [ ] 3.1.3 Track statement boundaries in executor

- [ ] 3.2 Implement READ COMMITTED visibility
  - [ ] 3.2.1 Create `ReadCommittedVisibility` struct
  - [ ] 3.2.2 Implement visibility: committed as of statement start
  - [ ] 3.2.3 Handle in-progress transactions (not visible)
  - [ ] 3.2.4 Handle rows deleted after statement start (still visible)
  - [ ] 3.2.5 Add unit tests for READ COMMITTED visibility

- [ ] 3.3 Add READ COMMITTED integration tests
  - [ ] 3.3.1 Test dirty read prevention
  - [ ] 3.3.2 Test non-repeatable reads allowed
  - [ ] 3.3.3 Test phantom reads allowed
  - [ ] 3.3.4 Test concurrent transaction behavior

## Phase 4: REPEATABLE READ

- [ ] 4.1 Add transaction-level snapshots
  - [ ] 4.1.1 Create `Snapshot` struct in `internal/storage/snapshot.go`
  - [ ] 4.1.2 Add `TakeSnapshot()` method to txn manager
  - [ ] 4.1.3 Store snapshot timestamp and active txn list
  - [ ] 4.1.4 Associate snapshot with transaction at BEGIN

- [ ] 4.2 Implement REPEATABLE READ visibility
  - [ ] 4.2.1 Create `RepeatableReadVisibility` struct
  - [ ] 4.2.2 Implement visibility: committed as of txn start
  - [ ] 4.2.3 Use transaction snapshot for all statements
  - [ ] 4.2.4 Add unit tests for REPEATABLE READ visibility

- [ ] 4.3 Add REPEATABLE READ integration tests
  - [ ] 4.3.1 Test dirty read prevention
  - [ ] 4.3.2 Test non-repeatable read prevention
  - [ ] 4.3.3 Test phantom reads allowed
  - [ ] 4.3.4 Test snapshot consistency across statements

## Phase 5: SERIALIZABLE

- [ ] 5.1 Create conflict detector
  - [ ] 5.1.1 Define `ConflictDetector` struct in `internal/storage/conflict_detector.go`
  - [ ] 5.1.2 Add read set tracking per transaction
  - [ ] 5.1.3 Add write set tracking per transaction
  - [ ] 5.1.4 Implement `RegisterRead()` method
  - [ ] 5.1.5 Implement `RegisterWrite()` method

- [ ] 5.2 Implement conflict detection at commit
  - [ ] 5.2.1 Add `CheckConflicts()` method
  - [ ] 5.2.2 Detect read-write conflicts with concurrent transactions
  - [ ] 5.2.3 Detect write-write conflicts
  - [ ] 5.2.4 Return `ErrSerializationFailure` on conflict
  - [ ] 5.2.5 Clean up read/write sets after commit/abort

- [ ] 5.3 Create lock manager
  - [ ] 5.3.1 Define `LockManager` struct in `internal/storage/lock_manager.go`
  - [ ] 5.3.2 Implement row-level exclusive locks for writes
  - [ ] 5.3.3 Add lock timeout handling
  - [ ] 5.3.4 Implement `Release()` for transaction cleanup
  - [ ] 5.3.5 Add deadlock detection (optional)

- [ ] 5.4 Implement SERIALIZABLE visibility
  - [ ] 5.4.1 Create `SerializableVisibility` struct (extends RepeatableRead)
  - [ ] 5.4.2 Track reads during scan operations
  - [ ] 5.4.3 Track writes during DML operations
  - [ ] 5.4.4 Invoke conflict check at transaction commit

- [ ] 5.5 Add SERIALIZABLE integration tests
  - [ ] 5.5.1 Test dirty read prevention
  - [ ] 5.5.2 Test non-repeatable read prevention
  - [ ] 5.5.3 Test phantom read prevention
  - [ ] 5.5.4 Test write-write conflict detection
  - [ ] 5.5.5 Test read-write conflict detection
  - [ ] 5.5.6 Test serialization failure error handling

## Phase 6: Integration

- [ ] 6.1 Update executor for isolation levels
  - [ ] 6.1.1 Get visibility checker based on transaction isolation level
  - [ ] 6.1.2 Apply visibility during all read operations
  - [ ] 6.1.3 Track read/write sets for SERIALIZABLE transactions
  - [ ] 6.1.4 Acquire locks before writes

- [ ] 6.2 Update transaction commit/rollback
  - [ ] 6.2.1 Check conflicts before commit for SERIALIZABLE
  - [ ] 6.2.2 Release all locks on commit/rollback
  - [ ] 6.2.3 Clean up read/write tracking data
  - [ ] 6.2.4 Handle serialization failure with proper error type

- [ ] 6.3 Add comprehensive integration tests
  - [ ] 6.3.1 Test all isolation levels with concurrent transactions
  - [ ] 6.3.2 Test isolation level switching between transactions
  - [ ] 6.3.3 Test default isolation level configuration
  - [ ] 6.3.4 Test error messages for isolation violations
  - [ ] 6.3.5 Test performance impact of each isolation level

- [ ] 6.4 Update documentation
  - [ ] 6.4.1 Document isolation level syntax
  - [ ] 6.4.2 Document behavior of each isolation level
  - [ ] 6.4.3 Document conflict detection for SERIALIZABLE
  - [ ] 6.4.4 Add examples for common use cases
