# Persistence Specification

## Requirements

### Requirement: Write-Ahead Log Entry Format

The Write-Ahead Log SHALL use DuckDB's WAL format for compatibility.

**Previous**: Custom binary format with CRC64 checksums
**Updated**: DuckDB WAL format with version 3 entries

#### Scenario: DuckDB WAL entry structure
- GIVEN a WAL entry for INSERT operation
- WHEN serializing the entry
- THEN entry header includes: type, flags, length, sequence number
- AND entry payload follows DuckDB serialization format
- AND checksum is calculated over the entry

#### Scenario: WAL recovery with DuckDB format
- GIVEN a WAL file created by DuckDB
- WHEN performing recovery
- THEN entries are parsed using DuckDB WAL format
- AND committed transactions are replayed correctly

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

### Requirement: WAL Recovery

The WAL SHALL implement three-phase recovery.

#### Scenario: Phase 1 scan pass
- GIVEN a WAL file with 100 entries and 1 checkpoint
- WHEN performing scan pass
- THEN checkpoint position is identified
- AND checkpoint iteration is extracted

#### Scenario: Clean shutdown recovery
- GIVEN a WAL file from clean shutdown (checkpoint at end)
- WHEN opening database
- THEN recovery loads checkpoint data
- AND database is operational

#### Scenario: Crash before checkpoint
- GIVEN a WAL file with entries but no checkpoint
- WHEN opening database
- THEN all valid entries are replayed
- AND committed transactions are restored

#### Scenario: Crash during checkpoint
- GIVEN main WAL and checkpoint WAL files
- WHEN opening database
- THEN WALs are merged via recovery file
- AND merged WAL is replayed

#### Scenario: Torn write recovery
- GIVEN a WAL file with checksum failure at position N
- WHEN performing recovery
- THEN entries before N are replayed
- AND entries at/after N are discarded

### Requirement: Checkpoint System

The persistence layer SHALL implement checkpointing.

#### Scenario: Manual checkpoint
- GIVEN a database with uncommitted data in WAL
- WHEN executing PRAGMA checkpoint
- THEN all data is written to checkpoint
- AND WAL is truncated

#### Scenario: Auto-checkpoint threshold
- GIVEN checkpoint_threshold set to 1GB
- WHEN WAL exceeds 1GB
- THEN automatic checkpoint is triggered

#### Scenario: Close with checkpoint
- GIVEN a database connection
- WHEN calling Close()
- THEN full checkpoint is performed
- AND WAL is deleted (clean shutdown)

#### Scenario: Checkpoint iteration tracking
- GIVEN a database with checkpoint iteration N
- WHEN checkpoint completes
- THEN iteration becomes N+1
- AND WAL header is updated

### Requirement: Persistent Storage

Persistent storage SHALL use DuckDB file format.

**Previous**: Simple block-based storage
**Updated**: DuckDB header with `DUCK` magic and dual 4KB blocks

#### Scenario: Persistent file structure
- GIVEN a persistent database file
- WHEN writing data to disk
- THEN file structure follows DuckDB format:
  - Block A (4096 bytes)
  - Block B (4096 bytes)
  - Metadata (property-based)
  - Data storage (row groups)

#### Scenario: In-memory mode unchanged
- GIVEN path `:memory:`
- WHEN opening database
- THEN no file is created
- AND in-memory behavior is unchanged

### Requirement: Transaction Durability

Committed transactions SHALL be durable.

#### Scenario: Commit syncs to disk
- GIVEN a transaction with INSERT statements
- WHEN calling Commit()
- THEN WAL entries are written
- AND fsync is called

#### Scenario: Crash after commit recoverable
- GIVEN committed transaction with data
- WHEN simulating crash (process kill)
- AND reopening database
- THEN committed data is present

#### Scenario: Uncommitted data lost on crash
- GIVEN uncommitted transaction with data
- WHEN simulating crash
- AND reopening database
- THEN uncommitted data is absent

### Requirement: PRAGMA Configuration

WAL behavior SHALL be configurable via PRAGMA.

#### Scenario: Set checkpoint threshold
- GIVEN database connection
- WHEN executing "PRAGMA checkpoint_threshold = '500MB'"
- THEN auto-checkpoint triggers at 500MB WAL size

#### Scenario: Get checkpoint threshold
- GIVEN database with custom threshold
- WHEN executing "PRAGMA checkpoint_threshold"
- THEN current threshold value is returned

#### Scenario: Alias wal_autocheckpoint
- GIVEN database connection
- WHEN executing "PRAGMA wal_autocheckpoint = '2GB'"
- THEN checkpoint_threshold is set to 2GB

#### Scenario: Force checkpoint
- GIVEN database with pending WAL data
- WHEN executing "CHECKPOINT"
- THEN checkpoint is performed immediately

### Requirement: Database Path Handling

Database paths SHALL determine storage mode.

#### Scenario: File path creates persistent database
- GIVEN path "/data/mydb.duckdb"
- WHEN opening database
- THEN PersistentStorage is used
- AND WAL is created

#### Scenario: Memory path creates volatile database
- GIVEN path ":memory:" or ""
- WHEN opening database
- THEN in-memory Storage is used
- AND no files are created

#### Scenario: DSN parameters preserved
- GIVEN path "file.db?threads=4&access_mode=read_write"
- WHEN opening database
- THEN file.db is used for persistence
- AND configuration parameters are applied

### Requirement: Engine Initialization

Engine SHALL support both storage modes.

#### Scenario: Engine with file path
- GIVEN Engine created with path option
- WHEN path is file path
- THEN PersistentStorage is initialized
- AND clock is propagated

#### Scenario: Engine with memory path
- GIVEN Engine created with ":memory:" path
- WHEN initialized
- THEN in-memory Storage is used
- AND no persistence overhead

### Requirement: Transaction Commit Semantics

Transaction commit SHALL ensure durability for persistent databases.

#### Scenario: Commit writes to WAL
- GIVEN persistent database with transaction
- WHEN committing transaction
- THEN all operations are written to WAL
- AND WAL is synced

#### Scenario: Commit triggers auto-checkpoint check
- GIVEN persistent database with large WAL
- WHEN committing transaction
- THEN auto-checkpoint is considered
- AND checkpoint runs if threshold exceeded

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

### Requirement: DuckDB File Format Support

The system SHALL use the official DuckDB file format for persistent storage, enabling compatibility with the DuckDB ecosystem.

**Changes from base spec**:
- ADDED: DuckDB magic number (`DUCK`) support
- ADDED: Dual 4KB rotating header blocks for crash safety
- ADDED: Format version 64+ support
- ADDED: Binary property-based catalog serialization
- ADDED: Support for advanced compression algorithms

#### Scenario: Open DuckDB file
- GIVEN a file created by official DuckDB with `DUCK` magic number
- WHEN opening the file with dukdb-go
- THEN the header is parsed successfully
- AND catalog is deserialized using DuckDB binary format
- AND data is readable using DuckDB row group format
- AND the database is fully functional

#### Scenario: Create new DuckDB file
- GIVEN no existing database file
- WHEN opening a new database file
- THEN a file with `DUCK` magic number is created
- AND dual 4KB header blocks are initialized
- AND format version 64 is written
- AND all subsequent writes use DuckDB format

#### Scenario: Read dual header blocks
- GIVEN a DuckDB file with two header blocks
- WHEN reading the file header
- THEN both Block A and Block B are read
- AND the newer header (based on salt) is selected
- AND file corruption is detected if headers are inconsistent

#### Scenario: Write header atomically
- GIVEN changes to header metadata
- WHEN writing the updated header
- THEN Block A is written to a temp file
- AND Block B is written to a temp file with flipped salt
- AND Block A is atomically renamed to the target location
- AND Block B is atomically renamed as backup
- AND crash during write does not corrupt existing header

### Requirement: DuckDB Magic Number

The system SHALL use the `DUCK` magic number (0x4455434B) for file identification.

#### Scenario: Magic number detection
- GIVEN a file with bytes `DUCK` at offset 0
- WHEN opening the file
- THEN the file is recognized as a DuckDB-compatible file
- AND format version is read from header

#### Scenario: Invalid magic number
- GIVEN a file without `DUCK` magic number
- WHEN opening the file
- THEN an error indicating unsupported format is returned

### Requirement: Dual Header Block System

The system SHALL implement dual 4KB rotating header blocks for crash-safe updates.

#### Scenario: Header structure validation
- GIVEN a DuckDB file header
- WHEN validating the header structure
- THEN header size is exactly 4096 bytes
- AND checksum covers the header data
- AND salt values are consistent between blocks

#### Scenario: Crash recovery with header blocks
- GIVEN a database file with Block A at offset 0 and Block B at offset 4096
- AND Block A has salt 100, Block B has salt 99
- WHEN reading headers
- THEN Block A is selected as the newer header
- AND database opens successfully

#### Scenario: Header write failure recovery
- GIVEN a database file
- AND a header write is in progress
- WHEN the write fails before completion
- THEN the original header remains intact
- AND temporary header files are cleaned up

### Requirement: DuckDB Format Version Support

The system SHALL support reading and writing DuckDB format version 64 and above.

#### Scenario: Version negotiation
- GIVEN a DuckDB file with version 64
- WHEN reading the file
- THEN version 64 is accepted and processed correctly

#### Scenario: Future version handling
- GIVEN a DuckDB file with a version greater than current support
- WHEN reading the file
- THEN a warning is logged about potential compatibility issues
- AND reading proceeds if the version is backward compatible

### Requirement: Binary Property-Based Catalog Serialization

The system SHALL use DuckDB's binary property-based format for catalog serialization.

#### Scenario: Catalog serialization
- GIVEN a catalog with schemas, tables, and columns
- WHEN serializing to disk
- THEN properties are written using property IDs (not JSON)
- AND each property has a varint ID followed by type-specific encoding

#### Scenario: Catalog deserialization
- GIVEN serialized catalog data in DuckDB binary format
- WHEN deserializing the catalog
- THEN properties are read by their IDs
- AND all type information (including nested types) is reconstructed

#### Scenario: Union type support
- GIVEN a table with a UNION type column
- WHEN serializing the catalog
- THEN UnionType is serialized with member count and member types
- AND deserialization reconstructs the UnionType correctly

#### Scenario: New type support
- GIVEN a table with TIME_TZ or TIMESTAMP_TZ columns
- WHEN serializing the catalog
- THEN these types are correctly serialized
- AND deserialization reconstructs the types correctly

### Requirement: Checkpoint Threshold Storage

The system SHALL store the checkpoint_threshold setting in the duckdb.settings table for persistence across database restarts.

#### Scenario: Threshold stored in settings table

- GIVEN a database connection
- WHEN executing `PRAGMA checkpoint_threshold = '256MB'`
- THEN the value '256MB' SHALL be stored in `duckdb.settings` table
- AND the setting persists after database close and reopen

#### Scenario: Threshold retrieved from settings

- GIVEN a database with checkpoint_threshold stored in settings
- WHEN the database is opened
- THEN the threshold value SHALL be read from `duckdb.settings`
- AND passed to the CheckpointManager initialization

#### Scenario: Default threshold when not set

- GIVEN a new database or one without checkpoint_threshold in settings
- WHEN no threshold is configured
- THEN a default value of '256MB' SHALL be used
- AND the default can be changed at runtime

### Requirement: Checkpoint Threshold Parser

The system SHALL parse checkpoint_threshold values with standard DuckDB size suffixes.

#### Scenario: Parse byte suffix

- GIVEN threshold value '1024b'
- WHEN parsing the value
- THEN the result SHALL be 1024 bytes

#### Scenario: Parse kilobyte suffix

- GIVEN threshold value '512KB'
- WHEN parsing the value
- THEN the result SHALL be 512 * 1024 = 524288 bytes

#### Scenario: Parse megabyte suffix

- GIVEN threshold value '256MB'
- WHEN parsing the value
- THEN the result SHALL be 256 * 1024 * 1024 = 268435456 bytes

#### Scenario: Parse gigabyte suffix

- GIVEN threshold value '1GB'
- WHEN parsing the value
- THEN the result SHALL be 1 * 1024 * 1024 * 1024 = 1073741824 bytes

#### Scenario: Parse plain number

- GIVEN threshold value '1000000'
- WHEN parsing the value
- THEN the result SHALL be 1000000 bytes (no suffix = bytes)

#### Scenario: Invalid threshold format

- GIVEN threshold value 'invalid'
- WHEN parsing the value
- THEN an error SHALL be returned
- AND the previous threshold value SHALL be preserved

### Requirement: CheckpointManager Threshold Integration

The CheckpointManager SHALL accept and use a configurable threshold for automatic checkpoint triggering.

#### Scenario: CheckpointManager accepts configurable threshold

- GIVEN a CheckpointManager constructor
- WHEN called with thresholdBytes = 268435456 (256MB)
- THEN the manager SHALL use this threshold for auto-checkpoint decisions
- AND not a hardcoded value

#### Scenario: Checkpoint triggered at threshold

- GIVEN checkpoint_threshold set to '1GB'
- AND WAL size grows to exceed 1GB
- WHEN the WAL size is checked after a commit
- THEN an automatic checkpoint SHALL be triggered
- AND WAL SHALL be truncated after successful checkpoint

#### Scenario: Threshold update at runtime

- GIVEN a running database with threshold = '256MB'
- WHEN executing `PRAGMA checkpoint_threshold = '1GB'`
- THEN subsequent checkpoint decisions SHALL use 1GB
- AND the new value SHALL be persisted to settings table

### Requirement: PRAGMA Execution Integration

The PRAGMA checkpoint_threshold execution handler SHALL connect to the configuration system.

#### Scenario: Execute SET checkpoint_threshold

- GIVEN a PRAGMA checkpoint_threshold = '512MB' statement
- WHEN executed by the executor
- THEN the value SHALL be parsed and validated
- AND stored in duckdb.settings
- AND the CheckpointManager SHALL be notified of the new threshold

#### Scenario: Execute GET checkpoint_threshold

- GIVEN a PRAGMA checkpoint_threshold statement (without =)
- WHEN executed by the executor
- THEN the current threshold value SHALL be retrieved from settings
- AND returned as the result
