# Savepoints Delta Spec

## ADDED Requirements

### Requirement: Create Savepoint

The system SHALL support creating named savepoints within a transaction using the SAVEPOINT command.

#### Scenario: Create savepoint in active transaction
- GIVEN an active transaction
- WHEN executing "SAVEPOINT sp1"
- THEN a savepoint named "sp1" is created
- AND the savepoint records the current undo log position
- AND subsequent operations can be rolled back to this point

#### Scenario: Create savepoint outside transaction
- GIVEN no active transaction (auto-commit mode)
- WHEN executing "SAVEPOINT sp1"
- THEN an error is returned: "SAVEPOINT can only be used in transaction block"
- AND no savepoint is created

#### Scenario: Create savepoint with duplicate name
- GIVEN an active transaction with savepoint "sp1"
- WHEN executing "SAVEPOINT sp1" again
- THEN the existing savepoint "sp1" is replaced
- AND the new savepoint records the current undo log position
- AND no error is returned (PostgreSQL behavior)

#### Scenario: Create multiple savepoints
- GIVEN an active transaction
- WHEN executing "SAVEPOINT sp1" then "INSERT" then "SAVEPOINT sp2"
- THEN two savepoints exist: sp1 at position 0, sp2 at position 1
- AND both can be individually rolled back to

### Requirement: Rollback to Savepoint

The system SHALL support rolling back to a named savepoint using ROLLBACK TO SAVEPOINT.

#### Scenario: Rollback to existing savepoint
- GIVEN an active transaction with savepoint "sp1" after initial INSERT
- AND additional operations after savepoint
- WHEN executing "ROLLBACK TO SAVEPOINT sp1"
- THEN operations after the savepoint are undone
- AND the transaction remains active
- AND the savepoint "sp1" is released (removed)

#### Scenario: Rollback to savepoint syntax variants
- GIVEN an active transaction with savepoint "sp1"
- WHEN executing "ROLLBACK TO sp1" (without SAVEPOINT keyword)
- THEN the rollback succeeds
- AND operations after sp1 are undone

#### Scenario: Rollback to non-existent savepoint
- GIVEN an active transaction without savepoint "sp1"
- WHEN executing "ROLLBACK TO SAVEPOINT sp1"
- THEN an error is returned: "savepoint \"sp1\" does not exist"
- AND the transaction state is unchanged

#### Scenario: Rollback removes nested savepoints
- GIVEN an active transaction with savepoints sp1, sp2, sp3 (in order)
- WHEN executing "ROLLBACK TO SAVEPOINT sp1"
- THEN operations after sp1 are undone
- AND savepoints sp1, sp2, sp3 are all released
- AND subsequent "ROLLBACK TO SAVEPOINT sp2" returns error

#### Scenario: Rollback to savepoint multiple times
- GIVEN an active transaction with savepoint "sp1"
- WHEN executing "ROLLBACK TO SAVEPOINT sp1"
- AND executing "SAVEPOINT sp1" again
- AND executing more operations
- AND executing "ROLLBACK TO SAVEPOINT sp1" again
- THEN each rollback succeeds
- AND each rollback undoes operations since the savepoint

### Requirement: Release Savepoint

The system SHALL support releasing (removing) a savepoint using RELEASE SAVEPOINT.

#### Scenario: Release existing savepoint
- GIVEN an active transaction with savepoint "sp1"
- WHEN executing "RELEASE SAVEPOINT sp1"
- THEN the savepoint "sp1" is removed
- AND operations before the savepoint remain in the transaction
- AND the transaction remains active

#### Scenario: Release savepoint syntax variant
- GIVEN an active transaction with savepoint "sp1"
- WHEN executing "RELEASE sp1" (without SAVEPOINT keyword)
- THEN the release succeeds
- AND the savepoint is removed

#### Scenario: Release non-existent savepoint
- GIVEN an active transaction without savepoint "sp1"
- WHEN executing "RELEASE SAVEPOINT sp1"
- THEN an error is returned: "savepoint \"sp1\" does not exist"
- AND the transaction state is unchanged

#### Scenario: Release removes nested savepoints
- GIVEN an active transaction with savepoints sp1, sp2, sp3 (in order)
- WHEN executing "RELEASE SAVEPOINT sp1"
- THEN savepoints sp1, sp2, sp3 are all removed
- AND subsequent "RELEASE SAVEPOINT sp2" returns error

### Requirement: Nested Savepoints

The system SHALL support nested savepoints with implicit parent-child relationships.

#### Scenario: Nested savepoint creation
- GIVEN an active transaction
- WHEN executing:
  ```sql
  INSERT INTO t VALUES (1);
  SAVEPOINT outer;
  INSERT INTO t VALUES (2);
  SAVEPOINT inner;
  INSERT INTO t VALUES (3);
  ```
- THEN savepoint "outer" includes operation at index 0
- AND savepoint "inner" includes operations at indices 0, 1

#### Scenario: Nested savepoint rollback
- GIVEN an active transaction with nested savepoints outer and inner
- AND values 1, 2, 3 inserted before outer, between, and after inner
- WHEN executing "ROLLBACK TO SAVEPOINT outer"
- THEN values 2 and 3 are undone
- AND value 1 remains
- AND both outer and inner savepoints are released

#### Scenario: Inner savepoint release
- GIVEN an active transaction with nested savepoints outer and inner
- WHEN executing "RELEASE SAVEPOINT inner"
- THEN only inner savepoint is released
- AND outer savepoint remains valid
- AND "ROLLBACK TO SAVEPOINT outer" still works

#### Scenario: Transaction commit with savepoints
- GIVEN an active transaction with savepoints
- WHEN executing COMMIT
- THEN all changes are committed
- AND all savepoints are implicitly released
- AND the transaction ends

#### Scenario: Transaction rollback with savepoints
- GIVEN an active transaction with savepoints and changes
- WHEN executing ROLLBACK (full transaction rollback)
- THEN all changes are undone
- AND all savepoints are implicitly released
- AND the transaction ends

### Requirement: Error Handling

The system SHALL provide clear error messages for savepoint operations.

#### Scenario: Invalid savepoint name
- GIVEN an active transaction
- WHEN executing "SAVEPOINT 123abc" (name starting with number)
- THEN parser error is returned for invalid identifier
- AND no savepoint is created

#### Scenario: Empty savepoint name
- GIVEN an active transaction
- WHEN executing "SAVEPOINT" (no name provided)
- THEN parser error is returned for missing name
- AND no savepoint is created

#### Scenario: Savepoint in committed transaction
- GIVEN a committed transaction
- WHEN attempting savepoint operation
- THEN an error is returned: "transaction not active"
- AND no operation is performed

#### Scenario: Savepoint in rolled-back transaction
- GIVEN a rolled-back transaction
- WHEN attempting savepoint operation
- THEN an error is returned: "transaction not active"
- AND no operation is performed

### Requirement: WAL Logging for Savepoints

The system SHALL log savepoint operations to the Write-Ahead Log for crash recovery.

#### Scenario: SAVEPOINT logged to WAL
- GIVEN a persistent database with active transaction
- WHEN executing "SAVEPOINT sp1"
- THEN WAL entry with type WAL_SAVEPOINT (92) is written
- AND entry includes transaction ID, savepoint name, and timestamp

#### Scenario: ROLLBACK TO SAVEPOINT logged to WAL
- GIVEN a persistent database with active transaction and savepoint
- WHEN executing "ROLLBACK TO SAVEPOINT sp1"
- THEN WAL entry with type WAL_ROLLBACK_SAVEPOINT (94) is written
- AND entry includes transaction ID, savepoint name, and undo index

#### Scenario: RELEASE SAVEPOINT logged to WAL
- GIVEN a persistent database with active transaction and savepoint
- WHEN executing "RELEASE SAVEPOINT sp1"
- THEN WAL entry with type WAL_RELEASE_SAVEPOINT (93) is written
- AND entry includes transaction ID and savepoint name

#### Scenario: Recovery with savepoint rollback
- GIVEN WAL with: BEGIN, INSERT A, SAVEPOINT sp1, INSERT B, ROLLBACK TO sp1, INSERT C, COMMIT
- WHEN performing crash recovery
- THEN only INSERT A and INSERT C are replayed
- AND INSERT B is not replayed (rolled back before commit)

#### Scenario: Recovery with uncommitted savepoint
- GIVEN WAL with: BEGIN, INSERT A, SAVEPOINT sp1, INSERT B (no COMMIT)
- WHEN performing crash recovery
- THEN neither INSERT A nor INSERT B is replayed
- AND savepoint state is not recovered (transaction uncommitted)

#### Scenario: In-memory database skips WAL
- GIVEN an in-memory database (":memory:")
- WHEN executing savepoint operations
- THEN no WAL entries are written
- AND savepoints function correctly in memory

### Requirement: Deterministic Testing

The system SHALL support deterministic testing of savepoint operations via clock injection.

#### Scenario: Deterministic savepoint timestamps
- GIVEN a mock clock set to specific time
- WHEN creating a savepoint
- THEN savepoint CreatedAt equals clock.Now()
- AND timestamp is reproducible across test runs

#### Scenario: Deterministic WAL timestamps
- GIVEN a mock clock and persistent database
- WHEN executing savepoint operations
- THEN WAL entry timestamps equal clock.Now()
- AND recovery produces identical results
