# Design: Write-Ahead Log and Crash Recovery System

## Context

The dukdb-go project currently has **zero persistence**. All data is stored in volatile memory and lost on process termination. This design adds a Write-Ahead Log (WAL) system with crash recovery, checkpointing, and durability guarantees matching DuckDB's behavior.

**Stakeholders**:
- Users requiring data persistence beyond process lifetime
- Applications needing crash recovery guarantees
- Users migrating from duckdb-go expecting file-based storage

**Constraints**:
- Must remain pure Go (no CGO)
- Must maintain API compatibility with duckdb-go
- Must integrate with quartz for deterministic testing
- WAL format does NOT need to be binary-compatible with DuckDB (our own format)
- Must handle concurrent access safely

## Goals / Non-Goals

**Goals**:
1. File paths in DSN actually persist data
2. `Close()` performs implicit checkpoint (per duckdb-go README)
3. Crash recovery restores all committed transactions
4. PRAGMA statements for WAL configuration
5. Deterministic testing via quartz clock injection
6. Auto-checkpoint based on WAL size threshold

**Non-Goals**:
1. Binary compatibility with DuckDB WAL format (different internal representations)
2. Encryption support (v3 WAL) - can add later
3. Concurrent checkpoints - single checkpoint at a time
4. Streaming replication - out of scope
5. Point-in-time recovery - out of scope

## Decisions

### Decision 1: WAL Entry Format

**Options**:
A. Binary format with fixed header - Simple, fast, fixed overhead
B. Protocol buffers - Self-describing, versioned, but adds dependency
C. JSON lines - Human readable, slow, large
D. Custom binary with CRC64 - Matches DuckDB approach

**Choice**: D - Custom binary format with CRC64 checksums

**Rationale**:
- Matches DuckDB's approach for consistency
- CRC64 provides corruption detection for torn writes
- Fixed header enables efficient seeking during recovery
- No external dependencies (pure Go crc64 package)

**Entry Format**:
```
+----------------+----------------+----------------+----------------+
|   Size (8B)    | Checksum (8B)  |   Type (1B)    |    Data (N)    |
+----------------+----------------+----------------+----------------+
        └─────────── Header (17 bytes) ───────────┘
```

**Checksum Coverage**:
- Checksum covers: Size (8B) + Type (1B) + Data (N bytes)
- Calculated BEFORE writing, stored in header
- CRC64-ISO polynomial (0xD800906D62A802FF, little-endian output)
- Size is checksummed to detect header corruption

**Torn Write Detection**:
- If Size can be read but full entry cannot → torn write (acceptable, stop recovery)
- If checksum mismatch with complete entry → corruption (log error, stop recovery)
- If header incomplete → torn write (acceptable, stop recovery)

**Trade-offs**:
- + Fast serialization/deserialization
- + Efficient corruption detection
- + Simple seeking for recovery
- - Not human readable
- - Custom format needs documentation

### Decision 2: WAL Version Strategy

**Options**:
A. Single version - Simpler, no upgrade path
B. Multiple versions like DuckDB (v1=none, v2=checksum, v3=encrypted)
C. Version in header with upgrade path

**Choice**: C - Version in header, start with v2 (checksummed)

**Rationale**:
- Enables future encryption support
- Can read older WAL files during upgrades
- Matches DuckDB versioning for familiarity

**Header Format**:
```go
type WALHeader struct {
    Magic      [4]byte  // "DWGO" (DuckDB-GO)
    Version    uint8    // 1=legacy, 2=checksummed
    Iteration  uint64   // Checkpoint iteration counter
    DBPath     [256]byte // Database path for verification
}
```

### Decision 2.5: WAL File Header (First Entry)

**Every WAL file MUST start with a WAL_VERSION entry** that validates the file:

```go
type WALFileHeader struct {
    Magic       [4]byte   // "DWGO" - DuckDB-GO WAL identifier
    Version     uint8     // 2 = checksummed (current)
    Iteration   uint64    // Checkpoint iteration counter
    DBPathHash  uint64    // CRC64 of database path (prevents wrong-file recovery)
    CreatedAt   int64     // Unix timestamp of creation
}

// Validation on open:
func validateWALHeader(header WALFileHeader, dbPath string) error {
    if string(header.Magic[:]) != "DWGO" {
        return errors.New("not a valid WAL file")
    }
    if header.Version > 2 {
        return fmt.Errorf("WAL version %d not supported", header.Version)
    }
    expectedHash := crc64.Checksum([]byte(dbPath), crc64.MakeTable(crc64.ISO))
    if header.DBPathHash != expectedHash {
        return errors.New("WAL file belongs to different database")
    }
    return nil
}
```

**Checkpoint WAL MUST also have version entry**:
- When merging WALs, skip the version entry from checkpoint WAL
- Version must match main WAL version (error if mismatch)

### Decision 3: Entry Type Taxonomy

**Options**:
A. Single entry type with operation enum - Simple but less efficient
B. Separate entry types per operation - Matches DuckDB, efficient parsing
C. Grouped by category (catalog vs data) - Middle ground

**Choice**: B - Separate entry types matching DuckDB taxonomy

**Entry Types** (matching DuckDB values):
```go
// Catalog operations (1-24)
const (
    WAL_CREATE_TABLE       WALEntryType = 1
    WAL_DROP_TABLE         WALEntryType = 2
    WAL_CREATE_SCHEMA      WALEntryType = 3
    WAL_DROP_SCHEMA        WALEntryType = 4
    WAL_CREATE_VIEW        WALEntryType = 5
    WAL_DROP_VIEW          WALEntryType = 6
    WAL_CREATE_SEQUENCE    WALEntryType = 7
    WAL_DROP_SEQUENCE      WALEntryType = 8
    WAL_SEQUENCE_VALUE     WALEntryType = 9
    WAL_CREATE_MACRO       WALEntryType = 10
    WAL_DROP_MACRO         WALEntryType = 11
    WAL_CREATE_TABLE_MACRO WALEntryType = 12
    WAL_DROP_TABLE_MACRO   WALEntryType = 13
    WAL_CREATE_TYPE        WALEntryType = 14
    WAL_DROP_TYPE          WALEntryType = 15
    WAL_ALTER_INFO         WALEntryType = 20
    WAL_CREATE_INDEX       WALEntryType = 23
    WAL_DROP_INDEX         WALEntryType = 24
)

// Data operations (25-99)
const (
    WAL_USE_TABLE     WALEntryType = 25  // Set current table context
    WAL_INSERT_TUPLE  WALEntryType = 26
    WAL_DELETE_TUPLE  WALEntryType = 27
    WAL_UPDATE_TUPLE  WALEntryType = 28
    WAL_ROW_GROUP     WALEntryType = 29  // Bulk data
)

// Transaction boundary markers
const (
    WAL_TXN_BEGIN  WALEntryType = 90  // Transaction start marker
    WAL_TXN_COMMIT WALEntryType = 91  // Transaction commit marker
)

// Control entries (98-100)
const (
    WAL_VERSION    WALEntryType = 98   // Version header (first entry in file)
    WAL_CHECKPOINT WALEntryType = 99   // Checkpoint marker
    WAL_FLUSH      WALEntryType = 100  // Explicit sync marker
)
```

**Transaction Boundary Markers**:
- `WAL_TXN_BEGIN`: Written at transaction start with transaction ID
- `WAL_TXN_COMMIT`: Written at transaction commit with transaction ID
- During recovery, only replay entries between matching BEGIN/COMMIT pairs
- Entries without COMMIT are discarded (partial transaction rollback)

**Rationale**:
- Efficient type dispatch during replay
- Clear separation of concerns
- Matches DuckDB for familiarity

### Decision 4: Data Serialization Strategy

**Options**:
A. Row-by-row serialization - Simple, slow for large inserts
B. Columnar serialization - Matches internal format, complex
C. Row groups with columnar data - Bulk efficiency, moderate complexity
D. Hybrid: rows for small, row groups for bulk - Adaptive

**Choice**: D - Hybrid approach

**Rationale**:
- Small transactions (1-100 rows): Individual WAL_INSERT_TUPLE entries
- Bulk inserts (Appender): WAL_ROW_GROUP with columnar data
- Matches how data flows through the system

**Row Entry Format**:
```go
type InsertTupleEntry struct {
    TableID  uint64
    RowData  []byte  // Serialized row values
}
```

**Row Group Entry Format**:
```go
type RowGroupEntry struct {
    TableID    uint64
    NumRows    uint32
    NumColumns uint16
    // Column data follows (type, validity bitmap, values)
}
```

### Decision 5: Checkpoint Strategy

**Options**:
A. Copy-on-write database file - Complex, storage overhead
B. Two-file swap (current + new) - Atomic but doubles storage
C. Incremental WAL truncation with full checkpoint - DuckDB approach
D. Snapshot + WAL - Simple but recovery takes longer

**Choice**: C - Incremental WAL with full checkpoint (matches DuckDB)

**Checkpoint Process** (order is critical for crash safety):
1. Write checkpoint INTENT marker to main WAL (iteration N+1, MetaBlockPointer)
2. Sync main WAL (records intent before any work)
3. Create checkpoint WAL file (`*.wal.ckpt`)
4. Write WAL_VERSION entry to checkpoint WAL (must match main WAL version)
5. Write all current catalog state to checkpoint WAL
6. Write all table data to checkpoint WAL
7. Sync checkpoint WAL
8. Sync directory containing WAL files (ensures rename is durable)
9. Atomic rename checkpoint WAL to main WAL
10. Sync directory again (ensures rename completed)
11. Increment iteration counter in database file header
12. Sync database file

**Directory Fsync Requirement**:
- After os.Rename(), must call fsync on the parent directory
- Without this, rename may not be durable on power loss
- Implementation: `dir, _ := os.Open(filepath.Dir(walPath)); dir.Sync(); dir.Close()`

**Checkpoint Files**:
```
mydb.duckdb      - Database file (catalog metadata)
mydb.duckdb.wal  - Main WAL file
mydb.duckdb.wal.ckpt     - Checkpoint in progress
mydb.duckdb.wal.recovery - Recovery merge file
```

**Trade-offs**:
- + Matches DuckDB behavior exactly
- + Handles crashes during checkpoint
- + WAL size bounded by checkpoint frequency
- - Checkpoint can be slow for large databases
- - Multiple file coordination complexity

### Decision 6: Recovery Algorithm

**Three-Phase Recovery** (per DuckDB):

**Phase 1: Scan Pass (Deserialize Only)**
```go
func (r *WALReader) scanPass() (checkpointPos int64, checkpointID uint64, err error) {
    for {
        entry, pos, err := r.readEntryAt(r.position)
        if err == io.EOF {
            return checkpointPos, checkpointID, nil
        }
        if err != nil {
            // Checksum failure = torn write, stop here
            return checkpointPos, checkpointID, nil
        }

        if entry.Type() == WAL_CHECKPOINT {
            checkpointPos = pos
            checkpointID = entry.(*CheckpointEntry).Iteration
        }

        r.position += entry.Size()
    }
}
```

**Phase 2: Checkpoint Reconciliation**
```go
func (r *WALReader) reconcileCheckpoint(pos int64, id uint64) error {
    ckptPath := r.path + ".ckpt"

    if !fileExists(ckptPath) {
        // Clean checkpoint, WAL is authoritative from checkpoint onwards
        r.truncateTo(pos)
        return nil
    }

    // Incomplete checkpoint - merge WAL and checkpoint WAL
    recoveryPath := r.path + ".recovery"
    return r.mergeWALs(r.path, ckptPath, recoveryPath, pos)
}
```

**Phase 3: Replay Pass**
```go
func (r *WALReader) replayPass(storage *Storage) error {
    r.position = 0  // Reset to start

    for {
        entry, err := r.readEntry()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }

        if err := r.replayEntry(entry, storage); err != nil {
            return err
        }
    }
}
```

### Decision 7: Concurrency Model

**Options**:
A. Single writer, multiple readers - Simple, limited concurrency
B. MVCC with WAL - Complex, full concurrency
C. Lock-based with WAL - Moderate complexity, safe
D. Append-only with transaction grouping - Good throughput

**Choice**: A - Single writer model initially

**Rationale**:
- Matches current dukdb-go concurrency model (single writer)
- WAL writes are serialized via mutex
- Readers can read from in-memory state
- Can enhance to MVCC later if needed

**Implementation**:
```go
type WALWriter struct {
    mu sync.Mutex  // Protects all writes
    // ...
}

func (w *WALWriter) WriteEntry(entry WALEntry) error {
    w.mu.Lock()
    defer w.mu.Unlock()
    // ...
}
```

### Decision 8: Transaction Durability

**Options**:
A. Sync on every commit - Durable but slow
B. Batch sync (group commit) - Fast but complex
C. Configurable sync policy - Flexible
D. Async sync with flush control - Performance focused

**Choice**: C - Configurable sync policy

**Sync Policies**:
```go
type SyncPolicy int

const (
    SYNC_IMMEDIATE SyncPolicy = iota  // fsync on every commit (default)
    SYNC_BATCH                         // fsync every N commits or T time
    SYNC_NONE                          // No fsync (testing only)
)
```

**Default**: SYNC_IMMEDIATE for safety, configurable via PRAGMA

### Decision 9: Auto-Checkpoint Trigger

**Options**:
A. Time-based - Checkpoint every N seconds
B. Size-based - Checkpoint when WAL exceeds N bytes (DuckDB default)
C. Transaction-based - Checkpoint every N transactions
D. Combined - Multiple triggers

**Choice**: B - Size-based (matches DuckDB)

**Implementation**:
```go
func (cm *CheckpointManager) MaybeAutoCheckpoint() error {
    if cm.wal.BytesWritten() >= cm.threshold {
        return cm.Checkpoint(CheckpointOptions{
            Type:      CONCURRENT_CHECKPOINT,
            Action:    IF_REQUIRED,
            WALAction: DONT_DELETE,
        })
    }
    return nil
}
```

**Default Threshold**: 1GB (matches DuckDB wal_autocheckpoint)

### Decision 10: Clock Integration for Deterministic Testing

**All time-sensitive operations use injected clock with explicit trap tags**:

```go
type WALWriter struct {
    clock quartz.Clock
    // ...
}

func (w *WALWriter) WriteEntry(entry WALEntry) error {
    _ = w.clock.Now()  // Tag: "WAL", "write", entry.Type().String()
    // ... write logic ...
}

func (w *WALWriter) Sync() error {
    _ = w.clock.Now()  // Tag: "WAL", "sync", "start"
    // ... sync logic ...
    _ = w.clock.Now()  // Tag: "WAL", "sync", "complete"
}

type CheckpointManager struct {
    clock quartz.Clock
    // ...
}

func (cm *CheckpointManager) Checkpoint(opts CheckpointOptions) error {
    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "intent"
    // ... write intent marker ...
    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "catalog"
    // ... write catalog ...
    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "data"
    // ... write data ...
    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "rename"
    // ... atomic rename ...
    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "complete"
}

func (cm *CheckpointManager) MaybeAutoCheckpoint() error {
    _ = cm.clock.Now()  // Tag: "checkpoint", "auto", "check"
    if cm.wal.BytesWritten() >= cm.threshold {
        _ = cm.clock.Now()  // Tag: "checkpoint", "auto", "trigger"
        return cm.Checkpoint(...)
    }
    return nil
}

type WALReader struct {
    clock quartz.Clock
    // ...
}

func (r *WALReader) Recover(storage *Storage) error {
    _ = r.clock.Now()  // Tag: "recovery", "phase1", "start"
    // ... scan pass ...
    _ = r.clock.Now()  // Tag: "recovery", "phase2", "start"
    // ... checkpoint reconciliation ...
    _ = r.clock.Now()  // Tag: "recovery", "phase3", "start"
    // ... replay pass ...
    _ = r.clock.Now()  // Tag: "recovery", "complete"
}
```

**FileSystem Interface for Crash Simulation**:
```go
// Allows tests to inject failures at specific points
type FileSystem interface {
    Open(path string) (*os.File, error)
    Create(path string) (*os.File, error)
    Rename(old, new string) error
    Remove(path string) error
    Sync(f *os.File) error
    SyncDir(path string) error
}

// Production implementation uses os package
type OSFileSystem struct{}

// Mock implementation for deterministic crash simulation
type MockFileSystem struct {
    failOnRename bool
    failOnSync   bool
    // ...
}
```

**Test Pattern with Context**:
```go
func TestRecoveryDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    trap := mClock.Trap().Now("checkpoint", "phase")
    defer trap.Close()

    storage := NewPersistentStorage("/tmp/db", mClock)

    // ... operations ...

    // Verify checkpoint phases occur in order
    for _, expectedPhase := range []string{"intent", "catalog", "data", "rename", "complete"} {
        call := trap.Wait(ctx)
        assert.Equal(t, expectedPhase, call.Tags[2])
        call.Release()
    }
}

func TestCrashDuringCheckpoint(t *testing.T) {
    mClock := quartz.NewMock(t)
    mockFS := &MockFileSystem{failOnRename: true}
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    storage := NewPersistentStorage("/tmp/db", mClock, WithFileSystem(mockFS))

    // Trigger checkpoint
    err := storage.Checkpoint()
    assert.Error(t, err)  // Should fail on rename

    // Verify recovery handles partial checkpoint
    storage2 := NewPersistentStorage("/tmp/db", mClock)
    // ... verify data integrity ...
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| WAL corruption | High | CRC64 checksums, torn write detection |
| Checkpoint during crash | High | Three-file protocol with atomic renames |
| Large WAL recovery time | Medium | Auto-checkpoint with configurable threshold |
| Concurrent access bugs | Medium | Single-writer model, mutex protection |
| Storage space growth | Low | Auto-checkpoint truncates WAL |
| Performance overhead | Medium | Configurable sync policy, batch modes |

## Migration Plan

### Phase 1: WAL Infrastructure (Days 1-3)
1. Create `internal/wal/` package structure
2. Implement WALEntry interface and types
3. Implement WALWriter with checksum
4. Unit tests for serialization

### Phase 2: Basic Recovery (Days 4-6)
1. Implement WALReader with checksum verification
2. Implement single-phase replay
3. Add recovery on storage open
4. Test basic crash/recovery cycle

### Phase 3: Checkpoint System (Days 7-10)
1. Implement CheckpointManager
2. Add checkpoint WAL file handling
3. Implement three-phase recovery
4. Add auto-checkpoint trigger

### Phase 4: Integration (Days 11-14)
1. Integrate with Engine
2. Add PRAGMA support
3. Modify Connector to use PersistentStorage
4. End-to-end tests

### Phase 5: Testing & Polish (Days 15-18)
1. Deterministic tests with quartz
2. Crash simulation tests
3. Concurrent access tests
4. Performance benchmarks

### Rollback Plan
- Feature flag: `DUKDB_PERSISTENCE=0` disables persistence
- In-memory mode (`:memory:`) always available
- All changes behind PersistentStorage abstraction

## Open Questions

1. **WAL file locking?**
   - Answer: Use `flock()` via `syscall` package for exclusive access
   - Prevents multiple processes from corrupting WAL

2. **Maximum WAL entry size?**
   - Answer: Limit to 16MB per entry, split larger row groups
   - Prevents memory exhaustion during recovery

3. **Concurrent read during checkpoint?**
   - Answer: Readers use in-memory snapshot, checkpoint writes to files
   - No blocking of reads during checkpoint

4. **Database file format?**
   - Answer: Minimal - just catalog metadata (schemas, table definitions)
   - Actual data rebuilt from WAL on open

5. **Partial WAL recovery?**
   - Answer: Stop at first checksum failure (torn write)
   - Transactions after corruption are lost
   - Log warning about potential data loss
