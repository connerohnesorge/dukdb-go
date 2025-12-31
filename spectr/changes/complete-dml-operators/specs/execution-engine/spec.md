# Execution Engine Specification - Delta

## MODIFIED Requirements

### Requirement: DML Operations

The engine SHALL support INSERT, UPDATE, and DELETE with full WHERE clause evaluation, bulk operation optimization, and WAL persistence.

**Changes from base spec**:
- ADDED: WHERE clause evaluation for UPDATE and DELETE
- ADDED: Bulk INSERT optimization via DataChunk batching
- ADDED: INSERT...SELECT support
- ADDED: WAL logging for all DML operations
- ADDED: RowsAffected count for all DML operations

#### Scenario: DELETE with simple WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE id = 2"
- THEN only row with id=2 is deleted
- AND rows with id=1 and id=3 remain
- AND RowsAffected returns 1
- AND WAL contains DELETE entry for row 2

#### Scenario: DELETE with complex WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')]
- WHEN executing "DELETE FROM t WHERE id > 1 AND id < 4"
- THEN rows with id=2 and id=3 are deleted
- AND rows with id=1 and id=4 remain
- AND RowsAffected returns 2
- AND WAL contains DELETE entry for rows 2 and 3

#### Scenario: DELETE with no matching rows
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "DELETE FROM t WHERE id = 999"
- THEN no rows are deleted
- AND RowsAffected returns 0
- AND WAL contains no DELETE entry (optimization: WAL write skipped when len(deletedRowIDs) = 0)

#### Scenario: DELETE with NULL handling (IS NULL)
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL"
- THEN only row with id=2 is deleted
- AND RowsAffected returns 1

#### Scenario: DELETE with NULL in comparison (three-valued logic)
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name = 'a'"
- THEN only row with id=1 is deleted
- AND row with id=2 (NULL) is NOT deleted (NULL = 'a' evaluates to NULL, not true)
- AND RowsAffected returns 1

#### Scenario: DELETE with complex WHERE and NULLs
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL OR name = 'c'"
- THEN rows with id=2 and id=3 are deleted
- AND row with id=1 remains
- AND RowsAffected returns 2

#### Scenario: DELETE with subquery
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND table "ids_to_delete" with values [1, 3]
- WHEN executing "DELETE FROM t WHERE id IN (SELECT id FROM ids_to_delete)"
- THEN rows with id=1 and id=3 are deleted
- AND RowsAffected returns 2

#### Scenario: UPDATE with WHERE clause
- GIVEN table "t" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- WHEN executing "UPDATE t SET name = 'updated' WHERE id = 2"
- THEN row 2 is updated to (2, 'updated')
- AND rows 1 and 3 are unchanged
- AND RowsAffected returns 1
- AND WAL contains UPDATE entry with before=(2, 'b'), after=(2, 'updated')

#### Scenario: UPDATE with expression in SET clause
- GIVEN table "t" with column "count" INT
- AND rows [(1, 10), (2, 20), (3, 30)]
- WHEN executing "UPDATE t SET count = count + 5 WHERE id > 1"
- THEN row 2 becomes (2, 25)
- AND row 3 becomes (3, 35)
- AND row 1 is unchanged at (1, 10)
- AND RowsAffected returns 2

#### Scenario: UPDATE with multi-column SET
- GIVEN table "t" with columns (id, name, count)
- AND row (1, 'old', 10)
- WHEN executing "UPDATE t SET name = 'new', count = 20 WHERE id = 1"
- THEN row becomes (1, 'new', 20)
- AND RowsAffected returns 1
- AND WAL entry includes both column updates

#### Scenario: UPDATE with no matching rows
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "UPDATE t SET name = 'x' WHERE id = 999"
- THEN no rows are updated
- AND RowsAffected returns 0
- AND WAL contains no UPDATE entry (optimization: WAL write skipped when len(updatedRowIDs) = 0)

#### Scenario: INSERT with 0 rows (empty VALUES)
- GIVEN empty table "t" with columns (id INT, name VARCHAR)
- WHEN executing "INSERT INTO t VALUES" (empty VALUES clause)
- THEN RowsAffected returns 0
- AND WAL contains no INSERT entry (optimization: WAL write skipped when chunk.Size() = 0)
- AND no storage operations are performed

#### Scenario: Bulk INSERT with VALUES
- GIVEN empty table "t" with columns (id INT, name VARCHAR)
- WHEN executing "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
- THEN all 3 rows are inserted as single batch
- AND RowsAffected returns 3
- AND DataChunk batching is used (not row-by-row)
- AND WAL contains single INSERT entry with 3 rows

#### Scenario: Bulk INSERT with 10,000 rows
- GIVEN empty table "t"
- WHEN executing INSERT with 10,000 rows in VALUES clause
- THEN all rows are inserted
- AND operation completes in <100ms
- AND memory usage stays <20MB during insertion
- AND DataChunks are flushed incrementally (not all buffered)
- AND WAL contains multiple entries (one per chunk of 2048 rows)

#### Scenario: INSERT...SELECT basic
- GIVEN table "source" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND empty table "target" with same schema
- WHEN executing "INSERT INTO target SELECT * FROM source"
- THEN all 3 rows are copied to target
- AND RowsAffected returns 3
- AND source table is unchanged

#### Scenario: INSERT...SELECT with large result set
- GIVEN table "source" with 1,000,000 rows
- AND empty table "target" with same schema
- WHEN executing "INSERT INTO target SELECT * FROM source"
- THEN all rows are copied
- AND memory usage stays <100MB (streaming via DataChunk batching)
- AND operation completes in <30 seconds

#### Scenario: INSERT...SELECT with WHERE clause
- GIVEN table "source" with rows [(1, 'a'), (2, 'b'), (3, 'c')]
- AND empty table "target"
- WHEN executing "INSERT INTO target SELECT * FROM source WHERE id > 1"
- THEN only rows 2 and 3 are copied
- AND RowsAffected returns 2

#### Scenario: INSERT with expression evaluation
- GIVEN empty table "t" with columns (id INT, upper_name VARCHAR)
- WHEN executing "INSERT INTO t VALUES (1, UPPER('hello')), (2, UPPER('world'))"
- THEN rows are inserted with values [(1, 'HELLO'), (2, 'WORLD')]
- AND expressions are evaluated during insertion

#### Scenario: DML operation timeout (deterministic)
- GIVEN table "t" with 1,000,000 rows
- AND context with deadline 5 seconds from now
- AND deterministic clock (quartz.Mock)
- WHEN executing "DELETE FROM t WHERE expensive_function(x) = true"
- AND clock advances 6 seconds
- THEN operation is cancelled
- AND ErrorTypeInterrupt or context.DeadlineExceeded is returned
- AND partial deletions are rolled back (ACID atomicity)

#### Scenario: Transaction rollback of DML
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN BEGIN TRANSACTION
- AND executing "INSERT INTO t VALUES (3, 'c')"
- AND executing "DELETE FROM t WHERE id = 1"
- AND executing ROLLBACK
- THEN table contains original rows [(1, 'a'), (2, 'b')]
- AND row 3 was not inserted
- AND row 1 was not deleted
- AND WAL contains rollback entry

#### Scenario: Transaction atomicity - partial operations rolled back
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- AND transaction with 5 INSERT operations
- WHEN 3 INSERTs succeed
- AND 4th INSERT fails (e.g., constraint violation)
- AND transaction is rolled back
- THEN table contains only original rows [(1, 'a'), (2, 'b')]
- AND first 3 INSERTs are undone (atomicity)
- AND WAL recovery does not replay uncommitted operations

#### Scenario: Executor initialization with clock injection
- GIVEN storage layer and WAL writer
- WHEN creating new Executor via `NewExecutor(storage, wal, clock)`
- THEN Executor is initialized with:
  - storage reference
  - wal reference
  - clock (quartz.Clock) for deterministic timestamps
  - currentTx set to nil (no active transaction)
- AND clock is injected for testability (quartz.Mock in tests, quartz.NewReal() in production)
- AND currentTx is set by BeginTransaction() when transaction starts

#### Scenario: RowID tracking across operations
- GIVEN empty table "t"
- WHEN executing "INSERT INTO t VALUES (1, 'a'), (2, 'b')"
- THEN rows are assigned RowIDs 0 and 1 (monotonic)
- WHEN executing "DELETE FROM t WHERE id = 1"
- THEN row with RowID 0 is tombstoned
- WHEN executing "INSERT INTO t VALUES (3, 'c')"
- THEN new row is assigned RowID 2 (RowID counter continues)
- AND tombstoned RowID 0 is NOT reused

#### Scenario: WAL recovery after crash during INSERT
- GIVEN database with table "t" containing [(1, 'a')]
- WHEN executing "INSERT INTO t VALUES (2, 'b'), (3, 'c')"
- AND INSERT is logged to WAL
- AND database crashes before checkpoint
- AND database restarts
- THEN WAL recovery replays INSERT
- AND table contains [(1, 'a'), (2, 'b'), (3, 'c')]

#### Scenario: Performance - 100K row INSERT
- GIVEN empty table "t"
- WHEN executing INSERT with 100,000 rows
- THEN operation completes in <1 second
- AND throughput >100,000 rows/second
- AND memory usage <50MB peak

#### Scenario: Performance - UPDATE with selective WHERE
- GIVEN table "t" with 100,000 rows
- WHEN executing "UPDATE t SET x = 1 WHERE id < 1000"
- THEN only 1,000 rows are updated (not full table scan with updates)
- AND operation completes in <50ms
- AND RowsAffected returns 1000

#### Scenario: Performance - DELETE with selective WHERE
- GIVEN table "t" with 100,000 rows
- WHEN executing "DELETE FROM t WHERE id < 1000"
- THEN only 1,000 rows are deleted
- AND operation completes in <50ms
- AND RowsAffected returns 1000

## ADDED Requirements

### Requirement: DataChunk Batching for DML

The engine SHALL batch DML operations using DataChunk columnar format for optimal performance.

#### Scenario: INSERT VALUES batched into DataChunks
- GIVEN INSERT statement with 5000 rows in VALUES clause
- WHEN executing the INSERT
- THEN rows are batched into DataChunks of 2048 rows each
- AND 3 chunks are created: [2048 rows, 2048 rows, 904 rows]
- AND each chunk is inserted to storage as atomic unit
- AND WAL logs one entry per chunk (3 total)

#### Scenario: DataChunk memory usage bounded
- GIVEN INSERT statement with 1,000,000 rows
- WHEN executing the INSERT
- THEN memory usage never exceeds 100MB
- AND chunks are flushed incrementally (not all buffered)
- AND garbage collector can reclaim flushed chunks

#### Scenario: UPDATE batching with WHERE filter
- GIVEN table with 10,000 rows
- WHEN executing "UPDATE t SET x = 1 WHERE id % 2 = 0"
- THEN matching rows (5,000) are collected into DataChunk
- AND update is applied as batch to storage
- AND WAL logs single UPDATE entry with 5,000 rows (or multiple chunk-sized entries)

### Requirement: Expression Evaluation in DML Context

The engine SHALL evaluate expressions in INSERT VALUES, UPDATE SET, and WHERE clauses.

#### Scenario: Expression in INSERT VALUES
- GIVEN empty table "t" with columns (id INT, computed INT)
- WHEN executing "INSERT INTO t VALUES (1, 10 + 20), (2, 5 * 6)"
- THEN expressions are evaluated to (1, 30), (2, 30)

#### Scenario: Expression in UPDATE SET clause
- GIVEN table "t" with row (1, 10)
- WHEN executing "UPDATE t SET count = count * 2 + 5"
- THEN count is updated to 25 (10 * 2 + 5)

#### Scenario: Expression in WHERE clause
- GIVEN table "t" with rows [(1, 'HELLO'), (2, 'WORLD')]
- WHEN executing "DELETE FROM t WHERE LOWER(name) = 'hello'"
- THEN row 1 is deleted (expression evaluated per row)

### Requirement: NULL Handling in DML Operations

The engine SHALL correctly handle NULL values in all DML operations per SQL three-valued logic.

#### Scenario: INSERT with NULL values
- GIVEN table "t" with nullable column "name"
- WHEN executing "INSERT INTO t VALUES (1, NULL)"
- THEN row is inserted with NULL in name column
- AND validity mask correctly marks NULL

#### Scenario: UPDATE to NULL
- GIVEN table "t" with row (1, 'value')
- WHEN executing "UPDATE t SET name = NULL WHERE id = 1"
- THEN name is updated to NULL
- AND validity mask is updated

#### Scenario: WHERE clause with NULL
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name = 'a'"
- THEN only row 1 is deleted (row 2 with NULL does not match)

#### Scenario: WHERE IS NULL
- GIVEN table "t" with rows [(1, 'a'), (2, NULL), (3, 'c')]
- WHEN executing "DELETE FROM t WHERE name IS NULL"
- THEN only row 2 is deleted

### Requirement: Error Handling in DML Operations

The engine SHALL return appropriate errors for invalid DML operations.

#### Scenario: INSERT into non-existent table
- GIVEN no table "nonexistent"
- WHEN executing "INSERT INTO nonexistent VALUES (1)"
- THEN ErrorTypeCatalog is returned
- AND no WAL entry is created

#### Scenario: UPDATE with non-existent column
- GIVEN table "t" with column "x"
- WHEN executing "UPDATE t SET nonexistent = 1"
- THEN ErrorTypeBinder is returned

#### Scenario: DELETE with invalid WHERE expression
- GIVEN table "t" with INT column "x"
- WHEN executing "DELETE FROM t WHERE x = 'string'"
- THEN ErrorTypeMismatchType is returned

#### Scenario: INSERT with column count mismatch
- GIVEN table "t" with columns (id, name)
- WHEN executing "INSERT INTO t VALUES (1)"
- THEN error is returned (column count mismatch)
- AND no rows are inserted

#### Scenario: UPDATE with type mismatch
- GIVEN table "t" with INT column "x"
- WHEN executing "UPDATE t SET x = 'string'"
- THEN ErrorTypeMismatchType is returned
- AND no rows are updated

#### Scenario: Error type compatibility - catalog errors
- GIVEN no table "nonexistent"
- WHEN executing "INSERT INTO nonexistent VALUES (1)"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)
- WHEN executing "UPDATE nonexistent SET x = 1"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)
- WHEN executing "DELETE FROM nonexistent"
- THEN ErrorTypeCatalog is returned (matching duckdb-go behavior)

#### Scenario: Error type compatibility - binder errors
- GIVEN table "t" with columns (id, name)
- WHEN executing "UPDATE t SET nonexistent_column = 1"
- THEN ErrorTypeBinder is returned (matching duckdb-go behavior)
- WHEN executing "DELETE FROM t WHERE nonexistent_column = 1"
- THEN ErrorTypeBinder is returned (matching duckdb-go behavior)

#### Scenario: Error type compatibility - interrupt errors
- GIVEN long-running DML operation
- WHEN context is cancelled
- THEN ErrorTypeInterrupt is returned (matching duckdb-go behavior)
- AND operation is aborted
- AND partial changes are rolled back
