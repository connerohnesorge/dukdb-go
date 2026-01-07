# Isolation Levels Specification

## Requirements

### Requirement: Isolation Level Configuration

The system SHALL support configuring transaction isolation levels at transaction start and connection level.

#### Scenario: BEGIN with ISOLATION LEVEL clause
- GIVEN a database connection
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
- THEN a new transaction starts with READ COMMITTED isolation
- AND all statements in the transaction use READ COMMITTED visibility rules

#### Scenario: BEGIN with READ UNCOMMITTED
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
- THEN transaction uses READ UNCOMMITTED isolation
- AND dirty reads are allowed

#### Scenario: BEGIN with REPEATABLE READ
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
- THEN transaction uses REPEATABLE READ isolation
- AND a snapshot is taken at transaction start

#### Scenario: BEGIN with SERIALIZABLE
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
- THEN transaction uses SERIALIZABLE isolation
- AND conflict detection is enabled

#### Scenario: Default isolation level without explicit specification
- GIVEN no isolation level specified in BEGIN
- WHEN executing "BEGIN" or "BEGIN TRANSACTION"
- THEN transaction uses the connection's default isolation level
- AND default is SERIALIZABLE unless configured otherwise

#### Scenario: SET default_transaction_isolation
- WHEN executing "SET default_transaction_isolation = 'READ COMMITTED'"
- THEN subsequent BEGIN statements without explicit isolation use READ COMMITTED

#### Scenario: SHOW transaction_isolation
- GIVEN a transaction with READ COMMITTED isolation
- WHEN executing "SHOW transaction_isolation"
- THEN result contains value "READ COMMITTED"

### Requirement: READ UNCOMMITTED Isolation

The system SHALL implement READ UNCOMMITTED isolation allowing dirty reads.

#### Scenario: Dirty read allowed in READ UNCOMMITTED
- GIVEN Transaction T1 with READ UNCOMMITTED isolation
- AND Transaction T2 inserts a row but does not commit
- WHEN T1 queries the table
- THEN T1 sees the uncommitted row from T2

#### Scenario: Uncommitted delete visible in READ UNCOMMITTED
- GIVEN Transaction T1 with READ UNCOMMITTED isolation
- AND Transaction T2 deletes a row but does not commit
- WHEN T1 queries the table
- THEN T1 does not see the deleted row (even though uncommitted)

#### Scenario: READ UNCOMMITTED sees own changes
- GIVEN Transaction T1 with READ UNCOMMITTED isolation
- WHEN T1 inserts a row
- AND T1 queries the table
- THEN T1 sees its own inserted row

#### Scenario: READ UNCOMMITTED aborted transaction
- GIVEN Transaction T1 with READ UNCOMMITTED isolation
- AND Transaction T2 inserts a row and aborts
- WHEN T1 queries the table after T2 aborts
- THEN T1 does not see the aborted row

### Requirement: READ COMMITTED Isolation

The system SHALL implement READ COMMITTED isolation preventing dirty reads.

#### Scenario: Dirty read prevented in READ COMMITTED
- GIVEN Transaction T1 with READ COMMITTED isolation
- AND Transaction T2 inserts a row but does not commit
- WHEN T1 queries the table
- THEN T1 does not see the uncommitted row from T2

#### Scenario: Committed data visible in READ COMMITTED
- GIVEN Transaction T1 with READ COMMITTED isolation
- AND Transaction T2 inserts a row and commits
- WHEN T1 queries the table after T2 commits
- THEN T1 sees the committed row from T2

#### Scenario: Non-repeatable read allowed in READ COMMITTED
- GIVEN Transaction T1 with READ COMMITTED isolation
- AND T1 reads row R with value V1
- WHEN Transaction T2 updates R to V2 and commits
- AND T1 reads R again
- THEN T1 sees value V2 (non-repeatable read occurs)

#### Scenario: Statement-level snapshot in READ COMMITTED
- GIVEN Transaction T1 with READ COMMITTED isolation
- AND T1 starts executing a SELECT statement
- WHEN Transaction T2 commits a new row during T1's SELECT
- THEN T1's SELECT may not see the new row (depends on timing)
- AND subsequent SELECT in T1 sees the new row

#### Scenario: Phantom read allowed in READ COMMITTED
- GIVEN Transaction T1 with READ COMMITTED isolation
- AND T1 executes SELECT * FROM t WHERE x > 5 returning 3 rows
- WHEN Transaction T2 inserts a row with x = 10 and commits
- AND T1 re-executes SELECT * FROM t WHERE x > 5
- THEN T1 sees 4 rows (phantom read occurs)

### Requirement: REPEATABLE READ Isolation

The system SHALL implement REPEATABLE READ isolation preventing dirty reads and non-repeatable reads.

#### Scenario: Dirty read prevented in REPEATABLE READ
- GIVEN Transaction T1 with REPEATABLE READ isolation
- AND Transaction T2 inserts a row but does not commit
- WHEN T1 queries the table
- THEN T1 does not see the uncommitted row from T2

#### Scenario: Non-repeatable read prevented in REPEATABLE READ
- GIVEN Transaction T1 with REPEATABLE READ isolation
- AND T1 reads row R with value V1
- WHEN Transaction T2 updates R to V2 and commits
- AND T1 reads R again
- THEN T1 still sees value V1 (snapshot isolation)

#### Scenario: Transaction snapshot taken at BEGIN
- GIVEN Transaction T1 with REPEATABLE READ isolation starts
- WHEN Transaction T2 commits new data after T1 starts
- AND T1 queries the table
- THEN T1 does not see T2's committed data

#### Scenario: Phantom read allowed in REPEATABLE READ
- GIVEN Transaction T1 with REPEATABLE READ isolation
- AND T1 executes SELECT * FROM t WHERE x > 5 returning 3 rows
- WHEN Transaction T2 inserts a row with x = 10 and commits
- AND T1 re-executes SELECT * FROM t WHERE x > 5
- THEN T1 may see 3 or 4 rows depending on implementation
- Note: Standard REPEATABLE READ allows phantoms; SNAPSHOT isolation prevents them

#### Scenario: Own changes visible in REPEATABLE READ
- GIVEN Transaction T1 with REPEATABLE READ isolation
- WHEN T1 inserts a row
- AND T1 queries the table
- THEN T1 sees its own inserted row

### Requirement: SERIALIZABLE Isolation

The system SHALL implement SERIALIZABLE isolation preventing all anomalies including phantoms.

#### Scenario: Dirty read prevented in SERIALIZABLE
- GIVEN Transaction T1 with SERIALIZABLE isolation
- AND Transaction T2 inserts a row but does not commit
- WHEN T1 queries the table
- THEN T1 does not see the uncommitted row from T2

#### Scenario: Non-repeatable read prevented in SERIALIZABLE
- GIVEN Transaction T1 with SERIALIZABLE isolation
- AND T1 reads row R with value V1
- WHEN Transaction T2 updates R to V2 and commits
- AND T1 reads R again
- THEN T1 still sees value V1

#### Scenario: Phantom read prevented in SERIALIZABLE
- GIVEN Transaction T1 with SERIALIZABLE isolation
- AND T1 executes SELECT * FROM t WHERE x > 5 returning 3 rows
- WHEN Transaction T2 inserts a row with x = 10 and commits
- AND T1 re-executes SELECT * FROM t WHERE x > 5
- THEN T1 still sees 3 rows (phantom prevented)

#### Scenario: Write-write conflict detection
- GIVEN Transaction T1 with SERIALIZABLE isolation
- AND Transaction T2 with SERIALIZABLE isolation
- AND both transactions update the same row
- WHEN T1 commits first
- AND T2 attempts to commit
- THEN T2 receives serialization failure error

#### Scenario: Read-write conflict detection
- GIVEN Transaction T1 with SERIALIZABLE isolation reads row R
- AND Transaction T2 with SERIALIZABLE isolation updates row R and commits
- WHEN T1 attempts to commit
- THEN T1 receives serialization failure error
- AND T1 must retry the transaction

#### Scenario: Serialization failure error type
- WHEN a serialization conflict is detected
- THEN error code indicates serialization failure
- AND error message indicates conflicting transaction

#### Scenario: Non-conflicting transactions commit successfully
- GIVEN Transaction T1 with SERIALIZABLE isolation reads table A
- AND Transaction T2 with SERIALIZABLE isolation reads table B
- WHEN both transactions commit
- THEN both commits succeed

### Requirement: Write Conflict Handling

The system SHALL detect and handle write conflicts appropriately.

#### Scenario: Exclusive lock acquisition for writes
- GIVEN Transaction T1 performing UPDATE on row R
- WHEN Transaction T2 attempts UPDATE on same row R
- THEN T2 blocks until T1 commits or rolls back

#### Scenario: Lock timeout
- GIVEN Transaction T1 holds lock on row R
- AND T1 does not commit within lock timeout period
- WHEN Transaction T2 waits for lock on row R
- AND timeout expires
- THEN T2 receives lock timeout error

#### Scenario: Lock release on commit
- GIVEN Transaction T1 holds locks on multiple rows
- WHEN T1 commits
- THEN all locks held by T1 are released
- AND waiting transactions can proceed

#### Scenario: Lock release on rollback
- GIVEN Transaction T1 holds locks on multiple rows
- WHEN T1 rolls back
- THEN all locks held by T1 are released
- AND waiting transactions can proceed

### Requirement: Isolation Level Default Configuration

The system SHALL support configuring the default isolation level.

#### Scenario: SERIALIZABLE as default
- GIVEN no configuration changes
- WHEN executing "BEGIN"
- THEN transaction uses SERIALIZABLE isolation

#### Scenario: Connection-level default override
- GIVEN "SET default_transaction_isolation = 'READ COMMITTED'"
- WHEN executing "BEGIN"
- THEN transaction uses READ COMMITTED isolation

#### Scenario: Transaction-level override of connection default
- GIVEN "SET default_transaction_isolation = 'READ COMMITTED'"
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE"
- THEN transaction uses SERIALIZABLE isolation (explicit overrides default)

#### Scenario: PRAGMA for default isolation
- WHEN executing "PRAGMA default_transaction_isolation"
- THEN result contains current default isolation level

#### Scenario: Invalid isolation level error
- WHEN executing "BEGIN TRANSACTION ISOLATION LEVEL INVALID"
- THEN parser error is returned
- AND error message indicates invalid isolation level

