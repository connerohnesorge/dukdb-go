# Tasks: Savepoint Support

## Phase 1: Savepoint Infrastructure

- [ ] 1.1 Create savepoint data structures
  - [ ] 1.1.1 Create `internal/engine/savepoint.go` with `Savepoint` struct
  - [ ] 1.1.2 Add `Name`, `UndoIndex`, `CreatedAt` fields to Savepoint
  - [ ] 1.1.3 Create `SavepointStack` struct with slice and map
  - [ ] 1.1.4 Implement `Push()` method with duplicate handling
  - [ ] 1.1.5 Implement `Get()` method for name lookup
  - [ ] 1.1.6 Implement `Release()` method with nested removal
  - [ ] 1.1.7 Implement `RollbackTo()` method
  - [ ] 1.1.8 Add unit tests for SavepointStack

- [ ] 1.2 Extend Transaction struct
  - [ ] 1.2.1 Add `savepoints *SavepointStack` field to Transaction
  - [ ] 1.2.2 Add `clock quartz.Clock` field to Transaction
  - [ ] 1.2.3 Initialize SavepointStack in Transaction creation
  - [ ] 1.2.4 Add `CreateSavepoint()` method to Transaction
  - [ ] 1.2.5 Add `RollbackToSavepoint()` method to Transaction
  - [ ] 1.2.6 Add `ReleaseSavepoint()` method to Transaction
  - [ ] 1.2.7 Add unit tests for Transaction savepoint methods

## Phase 2: SAVEPOINT Command

- [ ] 2.1 Add parser support for SAVEPOINT
  - [ ] 2.1.1 Add `SavepointStmt` to `internal/parser/ast.go`
  - [ ] 2.1.2 Add `VisitSavepointStmt` to visitor interface
  - [ ] 2.1.3 Create `parseSavepoint()` in `internal/parser/parser_txn.go`
  - [ ] 2.1.4 Add SAVEPOINT dispatch in parser
  - [ ] 2.1.5 Add parser tests for SAVEPOINT

- [ ] 2.2 Add binder support for SAVEPOINT
  - [ ] 2.2.1 Add `BoundSavepointStmt` to expressions
  - [ ] 2.2.2 Implement `bindSavepoint()` function
  - [ ] 2.2.3 Add case for SavepointStmt in Bind()

- [ ] 2.3 Add planner support for SAVEPOINT
  - [ ] 2.3.1 Add `LogicalSavepoint` node
  - [ ] 2.3.2 Add `PhysicalSavepoint` operator
  - [ ] 2.3.3 Add plan creation for savepoint statements

- [ ] 2.4 Add executor support for SAVEPOINT
  - [ ] 2.4.1 Create `internal/executor/physical_savepoint.go`
  - [ ] 2.4.2 Implement `executeSavepoint()` function
  - [ ] 2.4.3 Add case for PhysicalSavepoint in Execute()
  - [ ] 2.4.4 Add executor tests for SAVEPOINT

## Phase 3: ROLLBACK TO SAVEPOINT

- [ ] 3.1 Add parser support for ROLLBACK TO SAVEPOINT
  - [ ] 3.1.1 Add `RollbackToSavepointStmt` to ast.go
  - [ ] 3.1.2 Add visitor method
  - [ ] 3.1.3 Implement `parseRollbackToSavepoint()` function
  - [ ] 3.1.4 Handle "ROLLBACK TO [SAVEPOINT] name" syntax
  - [ ] 3.1.5 Add parser tests

- [ ] 3.2 Add binder support for ROLLBACK TO SAVEPOINT
  - [ ] 3.2.1 Add `BoundRollbackToSavepointStmt`
  - [ ] 3.2.2 Implement `bindRollbackToSavepoint()` function

- [ ] 3.3 Add planner support
  - [ ] 3.3.1 Add `LogicalRollbackToSavepoint` node
  - [ ] 3.3.2 Add `PhysicalRollbackToSavepoint` operator

- [ ] 3.4 Add executor support
  - [ ] 3.4.1 Implement `executeRollbackToSavepoint()` function
  - [ ] 3.4.2 Wire to TransactionManager.RollbackToSavepoint()
  - [ ] 3.4.3 Add executor tests

## Phase 4: RELEASE SAVEPOINT

- [ ] 4.1 Add parser support for RELEASE SAVEPOINT
  - [ ] 4.1.1 Add `ReleaseSavepointStmt` to ast.go
  - [ ] 4.1.2 Add visitor method
  - [ ] 4.1.3 Implement `parseReleaseSavepoint()` function
  - [ ] 4.1.4 Handle "RELEASE [SAVEPOINT] name" syntax
  - [ ] 4.1.5 Add parser tests

- [ ] 4.2 Add binder support for RELEASE SAVEPOINT
  - [ ] 4.2.1 Add `BoundReleaseSavepointStmt`
  - [ ] 4.2.2 Implement `bindReleaseSavepoint()` function

- [ ] 4.3 Add planner support
  - [ ] 4.3.1 Add `LogicalReleaseSavepoint` node
  - [ ] 4.3.2 Add `PhysicalReleaseSavepoint` operator

- [ ] 4.4 Add executor support
  - [ ] 4.4.1 Implement `executeReleaseSavepoint()` function
  - [ ] 4.4.2 Wire to TransactionManager.ReleaseSavepoint()
  - [ ] 4.4.3 Add executor tests

## Phase 5: Integration

- [ ] 5.1 WAL integration for savepoints
  - [ ] 5.1.1 Add `WAL_SAVEPOINT` entry type (92)
  - [ ] 5.1.2 Add `WAL_RELEASE_SAVEPOINT` entry type (93)
  - [ ] 5.1.3 Add `WAL_ROLLBACK_SAVEPOINT` entry type (94)
  - [ ] 5.1.4 Implement `SavepointEntry` serialization
  - [ ] 5.1.5 Implement `ReleaseSavepointEntry` serialization
  - [ ] 5.1.6 Implement `RollbackSavepointEntry` serialization
  - [ ] 5.1.7 Update recovery to handle savepoint entries
  - [ ] 5.1.8 Add WAL tests for savepoint entries

- [ ] 5.2 TransactionManager integration
  - [ ] 5.2.1 Add `Savepoint()` method to TransactionManager
  - [ ] 5.2.2 Add `RollbackToSavepoint()` method to TransactionManager
  - [ ] 5.2.3 Add `ReleaseSavepoint()` method to TransactionManager
  - [ ] 5.2.4 Wire WAL logging in each method
  - [ ] 5.2.5 Add integration tests

- [ ] 5.3 End-to-end tests
  - [ ] 5.3.1 Test simple savepoint workflow
  - [ ] 5.3.2 Test nested savepoints
  - [ ] 5.3.3 Test duplicate savepoint names
  - [ ] 5.3.4 Test SAVEPOINT outside transaction (error)
  - [ ] 5.3.5 Test ROLLBACK TO non-existent savepoint (error)
  - [ ] 5.3.6 Test crash recovery with savepoints
  - [ ] 5.3.7 Test deterministic timestamps with quartz

- [ ] 5.4 Documentation
  - [ ] 5.4.1 Document SAVEPOINT syntax and usage
  - [ ] 5.4.2 Document ROLLBACK TO SAVEPOINT syntax
  - [ ] 5.4.3 Document RELEASE SAVEPOINT syntax
  - [ ] 5.4.4 Document nested savepoint behavior
  - [ ] 5.4.5 Add examples to documentation
