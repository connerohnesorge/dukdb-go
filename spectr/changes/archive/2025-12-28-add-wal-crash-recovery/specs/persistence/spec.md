# Persistence Delta Spec

## ADDED Requirements

### Requirement: Write-Ahead Log Entry Format

The WAL SHALL use a checksummed binary format for entries.

#### Scenario: Write entry with checksum
- GIVEN a WAL writer with CRC64 checksum enabled
- WHEN writing an InsertTupleEntry
- THEN entry is written with 17-byte header (size + checksum + type)
- AND CRC64 checksum covers type byte and data

#### Scenario: Detect corrupted entry
- GIVEN a WAL file with corrupted data bytes
- WHEN reading the entry
- THEN checksum mismatch error is returned
- AND reader stops at corruption point

#### Scenario: Handle truncated entry (torn write)
- GIVEN a WAL file truncated mid-entry
- WHEN reading entries
- THEN IO error is returned for incomplete entry
- AND entries before truncation are valid

### Requirement: WAL Entry Types

The WAL SHALL support catalog and data entry types.

#### Scenario: Write catalog entries
- GIVEN a WAL writer
- WHEN writing CREATE_TABLE entry
- THEN entry with type=1 is written
- AND table definition is serialized

#### Scenario: Write data entries
- GIVEN a WAL writer with table context set
- WHEN writing INSERT_TUPLE entry
- THEN entry with type=26 is written
- AND row data is serialized

#### Scenario: Write control entries
- GIVEN a WAL writer
- WHEN writing CHECKPOINT entry
- THEN entry with type=99 is written
- AND checkpoint iteration is serialized

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

File paths SHALL persist data across restarts.

#### Scenario: Create persistent database
- GIVEN path "/tmp/test.db"
- WHEN opening database
- THEN WAL file "/tmp/test.db.wal" is created
- AND subsequent writes go to WAL

#### Scenario: Data survives restart
- GIVEN a persistent database with inserted rows
- WHEN closing and reopening database
- THEN all committed rows are present

#### Scenario: In-memory mode unaffected
- GIVEN path ":memory:"
- WHEN opening database
- THEN no WAL file is created
- AND data is volatile

#### Scenario: Transaction rollback not persisted
- GIVEN a persistent database with active transaction
- WHEN calling Rollback()
- THEN no WAL entries are written
- AND data is not persisted

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
