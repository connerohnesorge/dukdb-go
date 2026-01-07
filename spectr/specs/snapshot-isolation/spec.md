# Snapshot Isolation Specification

## Requirements

### Requirement: Version Chain Storage

The system SHALL store multiple versions of each row in a linked list structure, with the newest version at the head. Each version SHALL contain row data, transaction ID, commit timestamp, and a pointer to the previous version.

#### Scenario: Create new version on insert

- WHEN a transaction inserts a new row
- THEN a new version chain is created with one uncommitted version
- AND the version's TxnID equals the inserting transaction's ID
- AND the version's CommitTS is 0 (uncommitted)

#### Scenario: Create new version on update

- WHEN a transaction updates an existing row
- THEN a new version is created at the head of the chain
- AND the new version's PrevPtr points to the previous head
- AND the previous version remains unchanged

#### Scenario: Mark version as deleted

- WHEN a transaction deletes a row
- THEN the head version's DeletedBy is set to the transaction ID
- AND the version data is NOT removed from the chain

### Requirement: Transaction Timestamp Assignment

The system SHALL assign monotonically increasing timestamps to transactions. Start timestamp is assigned at transaction begin, commit timestamp at commit time. Timestamps SHALL be injectable via quartz.Clock for deterministic testing.

#### Scenario: Transaction start timestamp

- WHEN a new transaction begins
- THEN it receives a StartTS greater than all previously assigned timestamps
- AND the StartTS is captured from the injected clock

#### Scenario: Transaction commit timestamp

- WHEN a transaction commits successfully
- THEN it receives a CommitTS greater than its StartTS
- AND the CommitTS is greater than all other active transaction timestamps

#### Scenario: Deterministic timestamp testing

- WHEN using quartz mock clock in tests
- THEN transaction timestamps are deterministic
- AND trap points can intercept timestamp assignment

### Requirement: Snapshot Isolation Visibility

The system SHALL provide snapshot isolation where each transaction sees a consistent snapshot of data from its start time. A version is visible if it was committed before the transaction started and not deleted before the transaction started.

#### Scenario: Own uncommitted writes visible

- WHEN transaction T1 inserts a row
- AND T1 reads the same row before committing
- THEN T1 sees its own uncommitted insert

#### Scenario: Other uncommitted writes invisible

- WHEN transaction T1 inserts a row without committing
- AND transaction T2 starts and reads the same table
- THEN T2 does NOT see T1's uncommitted row

#### Scenario: Committed writes after start invisible

- WHEN transaction T1 starts at time 100
- AND transaction T2 commits a row insert at time 150
- AND T1 reads the table after T2 commits
- THEN T1 does NOT see T2's row (committed after T1 started)

#### Scenario: Committed writes before start visible

- WHEN transaction T1 commits a row insert at time 100
- AND transaction T2 starts at time 150
- AND T2 reads the table
- THEN T2 sees T1's committed row

#### Scenario: Deleted row visibility

- WHEN transaction T1 deletes a row at time 100
- AND T1 commits at time 110
- AND transaction T2 started at time 90 reads the row
- THEN T2 sees the row (delete not visible to T2)
- AND transaction T3 started at time 120 reads the row
- THEN T3 does NOT see the row (delete visible to T3)

### Requirement: Write-Write Conflict Detection

The system SHALL detect write-write conflicts at commit time. If another transaction has committed a write to the same row after the current transaction started, the commit SHALL fail with a WriteConflictError.

#### Scenario: No conflict with disjoint writes

- WHEN transaction T1 updates row A
- AND transaction T2 updates row B
- AND both transactions commit
- THEN both commits succeed

#### Scenario: Conflict detected at commit

- WHEN transaction T1 starts at time 100
- AND transaction T2 starts at time 110
- AND T2 updates row A and commits at time 120
- AND T1 updates row A and attempts to commit at time 130
- THEN T1's commit fails with WriteConflictError
- AND the error contains table name, row ID, and conflict timestamp

#### Scenario: First committer wins

- WHEN transaction T1 and T2 both update row A
- AND T1 commits first
- THEN T1's commit succeeds
- AND T2's subsequent commit attempt fails

#### Scenario: Read-only transaction no conflict

- WHEN transaction T1 only reads rows
- AND other transactions modify those rows and commit
- THEN T1's commit always succeeds

### Requirement: Version Garbage Collection

The system SHALL garbage collect old versions that are no longer visible to any active transaction. A version can be removed when its commit timestamp is older than the minimum start timestamp of all active transactions.

#### Scenario: Calculate low watermark

- WHEN transactions T1 (startTS=100), T2 (startTS=150), T3 (startTS=200) are active
- THEN the low watermark is 100
- AND versions committed before 100 are eligible for removal

#### Scenario: Remove old versions

- WHEN a version chain has versions with CommitTS [50, 100, 150, 200]
- AND the low watermark is 120
- THEN versions with CommitTS 50 and 100 can be removed
- AND the chain is truncated at the version with CommitTS 150

#### Scenario: No removal with long-running transaction

- WHEN transaction T1 with startTS=50 is still active
- AND other transactions have committed many versions
- THEN NO versions can be removed (low watermark is 50)

#### Scenario: Background vacuum runs periodically

- WHEN the database is idle
- THEN background vacuum runs on a configurable interval
- AND version chains are cleaned according to current low watermark

#### Scenario: Vacuum statistics tracking

- WHEN versions are garbage collected
- THEN the system tracks total versions removed
- AND the count is available via system table or PRAGMA

### Requirement: MVCC Integration with Storage

The system SHALL integrate MVCC with the existing storage layer via versioned methods (InsertVersioned, ReadVersioned, UpdateVersioned, DeleteVersioned). Non-MVCC paths SHALL remain available for backward compatibility.

#### Scenario: Insert with MVCC enabled

- WHEN MVCC is enabled for a table
- AND a transaction inserts a row
- THEN the row is stored as a VersionedRow in a new VersionChain
- AND the row ID is tracked in the transaction's write set

#### Scenario: Read with MVCC enabled

- WHEN MVCC is enabled for a table
- AND a transaction reads a row
- THEN the visibility checker finds the appropriate version
- AND the row ID is tracked in the transaction's read set

#### Scenario: Commit publishes versions

- WHEN a transaction with MVCC commits
- THEN all uncommitted versions created by the transaction receive CommitTS
- AND all deletes by the transaction receive DeleteTS
- AND the transaction is removed from active transaction set

#### Scenario: Rollback discards versions

- WHEN a transaction with MVCC rolls back
- THEN all uncommitted versions created by the transaction are marked invalid
- AND the version chains remain intact for concurrent readers
- AND garbage collection will clean them up

