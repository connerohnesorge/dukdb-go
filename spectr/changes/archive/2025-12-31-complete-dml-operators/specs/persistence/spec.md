# Persistence Specification - Delta

## MODIFIED Requirements

### Requirement: WAL Entry Types

The system SHALL log all DML operations (INSERT, UPDATE, DELETE) to the Write-Ahead Log for crash recovery and ACID durability.

**Changes from base spec**:
- ENHANCED: WAL entries for INSERT operations include DataChunk payload (not just single tuples)
- ADDED: WAL entries for UPDATE operations with before/after values
- ADDED: WAL entries for DELETE operations with deleted row data
- ADDED: Chunk-granularity logging (one entry per DataChunk, not per row)
- ADDED: Deterministic timestamps via clock injection (quartz)

#### Scenario: INSERT operation logged to WAL
- GIVEN empty table "t"
- WHEN executing "INSERT INTO t VALUES (1, 'a'), (2, 'b')"
- THEN WAL contains INSERT entry
- AND entry includes transaction ID
- AND entry includes table name "t"
- AND entry includes DataChunk with 2 rows
- AND entry includes timestamp from clock.Now()

#### Scenario: UPDATE operation logged to WAL with BeforeValues
- GIVEN table "t" with row (1, 'old')
- WHEN executing "UPDATE t SET name = 'new' WHERE id = 1"
- THEN WAL contains UPDATE entry
- AND entry includes row ID 1
- AND entry includes BeforeValues DataChunk: [(1, 'old')] for rollback
- AND entry includes AfterValues DataChunk: [(1, 'new')] for redo
- AND entry includes timestamp from clock.Now()
- AND entry enables both rollback and redo operations

#### Scenario: DELETE operation logged to WAL with DeletedData
- GIVEN table "t" with rows [(1, 'a'), (2, 'b')]
- WHEN executing "DELETE FROM t WHERE id = 1"
- THEN WAL contains DELETE entry
- AND entry includes row ID 1
- AND entry includes DeletedData DataChunk: [(1, 'a')] for rollback
- AND entry includes timestamp from clock.Now()
- AND entry enables rollback (undo deletion)

#### Scenario: Bulk INSERT logged as chunks
- GIVEN empty table "t"
- WHEN executing INSERT with 5000 rows
- THEN WAL contains 3 entries (not 5000 entries)
- AND entries correspond to DataChunks: [2048, 2048, 904]
- AND total log size is O(data size), not O(row count)

#### Scenario: WAL recovery replays INSERT
- GIVEN database crashed after INSERT but before checkpoint
- AND WAL contains INSERT entry with 100 rows
- WHEN database restarts and runs recovery
- THEN recovery reads INSERT entry
- AND 100 rows are inserted to table
- AND final state matches pre-crash state

#### Scenario: WAL recovery replays UPDATE
- GIVEN database crashed after UPDATE but before checkpoint
- AND WAL contains UPDATE entry for 50 rows
- WHEN recovery runs
- THEN 50 rows are updated to after-values
- AND table state reflects committed updates

#### Scenario: WAL recovery replays DELETE
- GIVEN database crashed after DELETE but before checkpoint
- AND WAL contains DELETE entry for 30 rows
- WHEN recovery runs
- THEN 30 rows are deleted from table
- AND table state reflects committed deletions

#### Scenario: WAL recovery skips uncommitted transaction
- GIVEN transaction started with INSERT
- AND WAL contains INSERT entry
- AND transaction not committed (no COMMIT entry)
- AND database crashed
- WHEN recovery runs
- THEN INSERT is not replayed (rollback uncommitted work)
- AND table remains in pre-transaction state

#### Scenario: WAL recovery handles mixed operations
- GIVEN WAL with sequence: [INSERT 10 rows, DELETE 5 rows, UPDATE 3 rows, COMMIT]
- WHEN recovery runs
- THEN operations are replayed in order
- AND final state: 5 rows inserted (10 - 5), 3 rows updated

#### Scenario: Deterministic WAL timestamps
- GIVEN quartz.Mock clock set to specific time
- WHEN executing "INSERT INTO t VALUES (1, 'a')"
- THEN WAL entry timestamp equals clock.Now()
- AND timestamp is reproducible in tests (not time.Now())

#### Scenario: WAL entry serialization - INSERT
- GIVEN INSERT entry with DataChunk containing all column types (INT, VARCHAR, NULL, etc.)
- WHEN serializing entry to disk
- AND deserializing entry from disk
- THEN deserialized entry equals original entry
- AND DataChunk data is preserved exactly
- AND Timestamp field is preserved
- AND serialization order: TransactionID → TableName → Chunk → Timestamp

#### Scenario: WAL entry serialization - UPDATE with BeforeValues
- GIVEN UPDATE entry with BeforeValues and AfterValues DataChunks
- WHEN serializing entry to disk
- AND deserializing entry from disk
- THEN both BeforeValues and AfterValues are preserved exactly (CRITICAL requirement)
- AND RowIDs array is preserved
- AND Timestamp field is preserved
- AND serialization order: TransactionID → TableName → RowIDs → BeforeValues → AfterValues → Timestamp

#### Scenario: WAL entry serialization - DELETE with DeletedData
- GIVEN DELETE entry with DeletedData DataChunk
- WHEN serializing entry to disk
- AND deserializing entry from disk
- THEN DeletedData DataChunk is preserved exactly
- AND RowIDs array is preserved
- AND Timestamp field is preserved
- AND serialization order: TransactionID → TableName → RowIDs → DeletedData → Timestamp

#### Scenario: WAL entry size efficiency
- GIVEN INSERT of 2048 rows (full DataChunk)
- WHEN measuring WAL entry size
- THEN entry size is O(column count × row count × avg value size)
- AND header overhead is <1KB
- AND compression (if enabled) reduces size further

## ADDED Requirements

### Requirement: WAL Entry Types for DML

The system SHALL define distinct WAL entry types for INSERT, UPDATE, and DELETE operations with appropriate payloads.

#### Scenario: WALInsertEntry structure
- GIVEN INSERT operation
- WHEN creating WAL entry
- THEN entry includes fields:
  - TransactionID (uint64)
  - TableName (string)
  - Chunk (*DataChunk) - columnar row data
  - Timestamp (time.Time)
- AND entry is serializable to byte stream

#### Scenario: WALUpdateEntry structure with BeforeValues
- GIVEN UPDATE operation
- WHEN creating WAL entry
- THEN entry includes fields:
  - TransactionID (uint64)
  - TableName (string)
  - RowIDs ([]RowID) - affected rows
  - BeforeValues (*DataChunk) - BEFORE-image for rollback (CRITICAL for MVCC)
  - AfterValues (*DataChunk) - AFTER-image for redo
  - Timestamp (time.Time) - deterministic via clock injection
- AND BeforeValues are captured BEFORE applying updates
- AND AfterValues are captured AFTER evaluating SET expressions

#### Scenario: WALDeleteEntry structure with DeletedData
- GIVEN DELETE operation
- WHEN creating WAL entry
- THEN entry includes fields:
  - TransactionID (uint64)
  - TableName (string)
  - RowIDs ([]RowID)
  - DeletedData (*DataChunk) - deleted row data for rollback (CRITICAL for undo)
  - Timestamp (time.Time) - deterministic via clock injection
- AND DeletedData is captured BEFORE rows are tombstoned
- AND DeletedData enables undo of deletion (restore deleted rows)

### Requirement: Synchronous WAL Writes with Group Commit

The system SHALL write WAL entries with group commit optimization: buffer entries within a transaction and issue a single fsync() at COMMIT time.

#### Scenario: INSERT commits after WAL flush (auto-commit)
- GIVEN INSERT operation outside explicit transaction (auto-commit)
- WHEN writing to WAL
- THEN WAL writer calls fsync() before returning
- AND INSERT result is not returned to client until fsync completes
- AND crash before fsync => INSERT is lost (acceptable, not committed)
- AND crash after fsync => INSERT is durable (recovered on restart)

#### Scenario: Transaction with multiple operations uses group commit
- GIVEN explicit transaction with 5 INSERT operations
- WHEN executing INSERTs
- THEN WAL.Write() is called for each INSERT
- AND WAL.inTransaction == true (transaction mode)
- AND entries are appended to bufferedEntries slice (no fsync)
- AND WAL.Write() returns immediately without calling writer.Sync()
- WHEN executing COMMIT
- THEN WAL.Commit() iterates bufferedEntries
- AND all entries are written via writeEntry()
- AND single fsync() is issued: `w.writer.Sync()` (group commit optimization)
- AND bufferedEntries is cleared: `w.bufferedEntries = nil`
- AND commit latency is O(1) fsync, not O(N) fsyncs
- AND throughput increases ~10-100x vs auto-commit for bulk operations

#### Scenario: WAL write failure propagates error
- GIVEN disk full or I/O error
- WHEN attempting to write WAL entry
- THEN error is returned to executor
- AND INSERT/UPDATE/DELETE is aborted
- AND no partial state is left in database

### Requirement: WAL Truncation After Checkpoint

The system SHALL truncate WAL after successful checkpoint to prevent unbounded growth.

#### Scenario: Checkpoint writes all dirty data
- GIVEN 1000 INSERT operations logged in WAL
- WHEN checkpoint is triggered
- THEN all data is written to main database file
- AND checkpoint marker is written to WAL
- AND old WAL entries before checkpoint can be truncated

#### Scenario: WAL truncation after checkpoint
- GIVEN checkpoint completed successfully
- WHEN truncating WAL
- THEN entries before checkpoint marker are removed
- AND WAL size is reduced
- AND only uncommitted transactions remain in WAL
- NOTE: Automatic checkpoint triggering based on WAL size/time is out of scope for this change - checkpoint must be triggered manually via API call

### Requirement: Crash Recovery Correctness

The system SHALL recover to a consistent state after crash, with all committed transactions durable and uncommitted transactions rolled back.

#### Scenario: Recovery with committed transaction
- GIVEN transaction: BEGIN, INSERT 100 rows, COMMIT
- AND all entries logged to WAL
- AND crash before checkpoint
- WHEN recovery runs
- THEN 100 rows are restored
- AND transaction is marked as committed

#### Scenario: Recovery with uncommitted transaction
- GIVEN transaction: BEGIN, INSERT 100 rows (no COMMIT)
- AND crash
- WHEN recovery runs
- THEN 100 rows are NOT inserted
- AND transaction is rolled back

#### Scenario: Recovery with partial transaction
- GIVEN transaction: BEGIN, INSERT 50 rows, DELETE 20 rows, <crash before COMMIT>
- WHEN recovery runs
- THEN both INSERT and DELETE are rolled back
- AND database state equals pre-transaction state

#### Scenario: Recovery idempotence with RowID checking
- GIVEN WAL with committed INSERT of 100 rows (RowIDs 0-99)
- WHEN recovery runs first time
- THEN 100 rows are inserted
- WHEN recovery runs second time (e.g., crash during recovery)
- THEN storage.ContainsRows() checks detect already-inserted rows
- AND 100 rows exist exactly once (not duplicated)
- AND recovery is idempotent

#### Scenario: Recovery idempotence mechanism
- GIVEN WAL recovery algorithm with two-pass structure
- WHEN replaying INSERT entry
- THEN algorithm checks: `if !storage.ContainsRows(e.TableName, e.Chunk.FirstRowID())`
- AND if firstRowID already exists, skip insertion (already applied)
- AND if not exists, apply insertion via `storage.InsertChunk(e.TableName, e.Chunk)`
- WHEN replaying UPDATE entry
- THEN algorithm iterates RowIDs: `for i, rowID := range e.RowIDs`
- AND for each row, checks: `if currentRow.Matches(e.BeforeValues.GetRow(i))`
- AND if BeforeValues match, apply update: `storage.UpdateRow(e.TableName, rowID, e.AfterValues.GetRow(i))`
- AND if BeforeValues don't match, skip update (already applied or modified by later transaction)
- WHEN replaying DELETE entry
- THEN algorithm iterates RowIDs: `for _, rowID := range e.RowIDs`
- AND for each row, checks: `if !storage.IsTombstoned(e.TableName, rowID)`
- AND if row is not tombstoned, apply deletion: `storage.DeleteRow(e.TableName, rowID)`
- AND if row is already tombstoned, skip deletion (already applied)
- AND all three operations use symmetrical two-pass structure: (1) identify committed transactions, (2) replay with idempotence checks

#### Scenario: Zero-row operations skip WAL write
- GIVEN table "t" with rows
- WHEN executing "DELETE FROM t WHERE id = 999" (no matching rows)
- THEN RowsAffected returns 0
- AND no WAL DELETE entry is created (optimization: WAL write skipped when len(deletedRowIDs) = 0)
- WHEN executing "UPDATE t SET x = 1 WHERE id = 999" (no matching rows)
- THEN RowsAffected returns 0
- AND no WAL UPDATE entry is created (optimization: WAL write skipped when len(updatedRowIDs) = 0)
- WHEN executing "INSERT INTO t VALUES" (0 rows)
- THEN RowsAffected returns 0
- AND no WAL INSERT entry is created (optimization: WAL write skipped when chunk.Size() = 0)

### Requirement: Performance - WAL Write Latency

The system SHALL minimize WAL write latency through batching and efficient I/O.

#### Scenario: Batch WAL writes within transaction (group commit)
- GIVEN transaction with 10 INSERT statements
- WHEN executing transaction
- THEN WAL entries are buffered in memory
- AND single fsync is issued at COMMIT (not 10 fsyncs)
- AND commit latency is ~5-10ms (single fsync overhead)
- AND throughput is 10x better than auto-commit (10 fsyncs = ~50-100ms)

#### Scenario: WAL write does not block readers
- GIVEN transaction writing to WAL
- WHEN concurrent SELECT queries execute
- THEN SELECT queries are not blocked by WAL write
- AND reads see pre-transaction state (isolation)

## ADDED Requirements

### Requirement: Deterministic Crash Testing

The system SHALL support deterministic crash testing using clock injection and controlled failures.

#### Scenario: Deterministic crash simulation
- GIVEN quartz.Mock clock
- AND INSERT operation in progress
- WHEN clock reaches specific time (e.g., 5 seconds)
- AND crash is simulated (panic or controlled shutdown)
- THEN recovery behavior is reproducible
- AND test can verify exact recovery outcome

#### Scenario: Crash mid-INSERT
- GIVEN INSERT of 10,000 rows (5 DataChunks)
- WHEN crash occurs after writing 3 chunks to WAL
- THEN recovery replays 3 chunks (6144 rows)
- AND remaining 3856 rows are lost (transaction not committed)

#### Scenario: Crash during WAL write
- GIVEN WAL entry being written to disk
- WHEN crash occurs mid-write (torn write)
- THEN recovery detects incomplete entry (checksum mismatch)
- AND incomplete entry is ignored
- AND recovery proceeds from last valid entry

### Requirement: WAL Performance Optimization

The system SHALL optimize WAL write performance through efficient I/O and optional compression.

#### Scenario: WAL entries written efficiently
- GIVEN INSERT with 2048 rows
- WHEN writing to WAL
- THEN DataChunk is written as contiguous block
- AND I/O operations are minimized
- AND write latency is <10ms for typical chunk

#### Scenario: WAL supports future compression
- GIVEN WAL entry structure
- WHEN designing entry format
- THEN format allows for future compression implementation
- AND compressed/uncompressed entries can coexist
- AND compression flag is part of entry header
