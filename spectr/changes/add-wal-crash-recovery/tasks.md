# Tasks: Write-Ahead Log and Crash Recovery System

## 1. WAL Entry Types and Serialization

- [ ] 1.1 Create `internal/wal/entry.go` with WALEntryType constants
  - Match DuckDB entry type values (1-100)
  - Define WALEntry interface: `Type()`, `Serialize()`, `Deserialize()`
  - Add WALEntryHeader struct (size, checksum, type)

- [ ] 1.2 Implement catalog entry types
  - CreateTableEntry: table name, schema, columns
  - DropTableEntry: table name
  - CreateSchemaEntry: schema name
  - DropSchemaEntry: schema name
  - CreateViewEntry: view name, SQL definition
  - DropViewEntry: view name
  - AlterInfoEntry: table name, alteration type, details

- [ ] 1.3 Implement data entry types
  - UseTableEntry: table ID (context for following entries)
  - InsertTupleEntry: row data serialized
  - DeleteTupleEntry: row ID
  - UpdateTupleEntry: row ID, new values
  - RowGroupEntry: bulk columnar data

- [ ] 1.4 Implement control entry types
  - VersionEntry: WAL version, magic, iteration
  - CheckpointEntry: checkpoint iteration, timestamp
  - FlushEntry: explicit sync marker

- [ ] 1.5 Add serialization tests
  - Round-trip test for each entry type
  - Test with NULL values in data
  - Test with maximum size data
  - Test empty entries

## 2. WAL Writer Implementation

- [ ] 2.1 Create `internal/wal/writer.go` with WALWriter struct
  - File handle, buffered writer
  - CRC64 checksum calculator
  - Bytes written counter
  - Mutex for thread safety
  - Quartz clock injection

- [ ] 2.2 Implement `NewWALWriter(path, clock)` constructor
  - Open or create file with append mode
  - Initialize CRC64 table (ISO polynomial)
  - Write version header if new file

- [ ] 2.3 Implement `WriteEntry(entry)` method
  - Serialize entry to buffer
  - Calculate CRC64 checksum
  - Write header (size, checksum, type)
  - Write entry data
  - Update bytes written counter

- [ ] 2.4 Implement `Sync()` method
  - Flush buffer to file
  - Call fsync on file descriptor
  - Use clock for timing tags

- [ ] 2.5 Implement `Close()` method
  - Flush pending data
  - Sync to disk
  - Close file handle

- [ ] 2.6 Add writer tests
  - Test entry writing and reading
  - Test checksum calculation
  - Test concurrent writes (mutex)
  - Test sync durability

## 3. WAL Reader and Validation

- [ ] 3.1 Create `internal/wal/reader.go` with WALReader struct
  - File handle, buffered reader
  - Current position tracker
  - CRC64 checksum validator
  - Quartz clock injection

- [ ] 3.2 Implement `NewWALReader(path, clock)` constructor
  - Open file for reading
  - Read and validate version header
  - Extract checkpoint iteration

- [ ] 3.3 Implement `ReadEntry()` method
  - Read header (size, checksum, type)
  - Read entry data
  - Validate checksum
  - Return error on mismatch (torn write)
  - Deserialize based on type

- [ ] 3.4 Implement `ReadEntryAt(position)` method
  - Seek to position
  - Read single entry
  - Return entry and next position

- [ ] 3.5 Add reader tests
  - Test reading valid entries
  - Test checksum validation failure
  - Test truncated entry detection
  - Test empty WAL file

## 4. Recovery System

- [ ] 4.1 Implement Phase 1: Scan Pass
  - Create `scanForCheckpoints()` method
  - Iterate through all entries (deserialize only)
  - Record last checkpoint position and iteration
  - Stop at first checksum failure (torn write)
  - Return (checkpointPos, checkpointID, error)

- [ ] 4.2 Implement Phase 2: Checkpoint Reconciliation
  - Create `reconcileCheckpoint()` method
  - Check if checkpoint WAL exists (*.wal.ckpt)
  - If clean checkpoint: truncate main WAL at checkpoint
  - If incomplete: merge WALs via recovery file

- [ ] 4.3 Implement WAL merge for incomplete checkpoint
  - Create recovery WAL file (*.wal.recovery)
  - Copy main WAL up to checkpoint position
  - Append full checkpoint WAL
  - Atomic rename to main WAL
  - Delete checkpoint WAL

- [ ] 4.4 Implement Phase 3: Replay Pass
  - Create `Replay(storage)` method
  - Reset to start of WAL
  - For each entry, call appropriate replay function
  - Handle catalog entries: create/drop tables/schemas
  - Handle data entries: insert/delete/update rows

- [ ] 4.5 Implement entry replay functions
  - `replayCreateTable(storage, entry)` - create table in storage
  - `replayInsertTuple(storage, entry)` - insert row
  - `replayDeleteTuple(storage, entry)` - delete row
  - `replayRowGroup(storage, entry)` - bulk insert

- [ ] 4.6 Add recovery tests
  - Test clean shutdown recovery
  - Test crash before checkpoint
  - Test crash during checkpoint
  - Test torn write recovery
  - Test corrupted WAL handling

## 5. Checkpoint Manager

- [ ] 5.1 Create `internal/wal/checkpoint.go` with CheckpointManager
  - Reference to storage and WAL writer
  - Checkpoint threshold (default 1GB)
  - Current checkpoint iteration
  - Quartz clock injection
  - Mutex for single checkpoint

- [ ] 5.2 Implement `NewCheckpointManager()` constructor
  - Set default threshold
  - Initialize iteration counter from WAL header

- [ ] 5.3 Implement `Checkpoint(opts)` method
  - Create checkpoint WAL file
  - Write all catalog entries (schemas, tables, views)
  - Write all table data as row groups
  - Write checkpoint marker to main WAL
  - Sync both files
  - Atomic rename checkpoint to main WAL
  - Increment iteration counter

- [ ] 5.4 Implement `MaybeAutoCheckpoint()` method
  - Check WAL bytes written against threshold
  - If exceeded, trigger concurrent checkpoint
  - Return nil if no checkpoint needed

- [ ] 5.5 Implement checkpoint types
  - FULL_CHECKPOINT: Complete data write, vacuum
  - CONCURRENT_CHECKPOINT: Incremental, no vacuum
  - Called from different contexts (auto vs manual)

- [ ] 5.6 Add checkpoint tests
  - Test checkpoint creates valid WAL
  - Test auto-checkpoint triggers correctly
  - Test checkpoint during concurrent writes
  - Test crash during checkpoint recovery

## 6. Persistent Storage Layer

- [ ] 6.1 Create `internal/storage/persistent.go` with PersistentStorage
  - Embed existing Storage for in-memory operations
  - WAL writer reference
  - Checkpoint manager reference
  - Database path
  - Quartz clock injection

- [ ] 6.2 Implement `NewPersistentStorage(path, clock)` constructor
  - Check for existing WAL file
  - If exists: perform recovery
  - Open WAL for writing
  - Create checkpoint manager

- [ ] 6.3 Implement transaction commit with WAL
  - Override `CommitTransaction()` to write to WAL
  - Write all pending operations as entries
  - Sync WAL to disk
  - Check auto-checkpoint

- [ ] 6.4 Implement close with checkpoint
  - Perform full checkpoint (per duckdb-go README requirement)
  - Close WAL file
  - Clean up resources

- [ ] 6.5 Add persistent storage tests
  - Test create → close → reopen cycle
  - Test data persists across restarts
  - Test transaction rollback (no WAL entries)
  - Test concurrent access

## 7. Engine Integration

- [ ] 7.1 Modify `internal/engine/engine.go` NewEngine
  - Check path for `:memory:` vs file path
  - Create appropriate storage type
  - Pass clock through to storage

- [ ] 7.2 Add Storage interface abstraction
  - Extract interface from current Storage
  - Implement interface in PersistentStorage
  - Engine uses interface type

- [ ] 7.3 Implement PRAGMA handlers in `internal/engine/pragma.go`
  - `checkpoint_threshold`: get/set threshold
  - `wal_autocheckpoint`: alias for threshold
  - `checkpoint`: force checkpoint now
  - `wal_checkpoint`: alias for checkpoint

- [ ] 7.4 Add integration tests
  - Test file-based database creation
  - Test PRAGMA commands
  - Test mixed in-memory and persistent

## 8. Deterministic Testing

- [ ] 8.1 Add clock integration throughout WAL
  - WALWriter uses clock for timestamps
  - CheckpointManager uses clock for timing
  - Tag all clock calls for trapping

- [ ] 8.2 Create deterministic recovery tests
  - Mock clock for consistent timestamps
  - Trap checkpoint operations
  - Verify recovery at specific time points

- [ ] 8.3 Create concurrent operation tests
  - Multiple writers with deterministic ordering
  - Trap-based synchronization
  - Verify no race conditions

- [ ] 8.4 Add crash simulation tests
  - Simulate crash at various points
  - Verify recovery correctness
  - Test with mock clock advancement

## 9. Documentation and Polish

- [ ] 9.1 Add godoc comments
  - All public functions in wal package
  - PersistentStorage methods
  - PRAGMA handlers

- [ ] 9.2 Add usage examples
  - File-based database example
  - Checkpoint configuration example
  - Recovery scenario example

- [ ] 9.3 Update FEATURE_PARITY_ANALYSIS.md
  - Mark WAL/crash recovery as complete
  - Update percentage

- [ ] 9.4 Add performance benchmarks
  - WAL write throughput
  - Checkpoint time for N rows
  - Recovery time for N entries

## Validation

- [ ] Run `go test -v -race ./internal/wal/...`
- [ ] Run `go test -v -race -count=100 ./internal/wal/...` for flaky detection
- [ ] Run crash simulation tests
- [ ] Verify `spectr validate add-wal-crash-recovery`
- [ ] Memory profile for leaks during recovery
- [ ] Verify fsync calls with strace
