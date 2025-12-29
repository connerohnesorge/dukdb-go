# Change: Add Write-Ahead Log and Crash Recovery System

## Why

The current dukdb-go implementation is **purely in-memory** with no persistence layer:

**Current State Analysis** (from exploration):
- `internal/storage/storage.go` holds tables in `map[string]*Table` - volatile memory only
- `internal/engine/engine.go` TransactionManager just tracks IDs with no real semantics:
  ```go
  func (tm *TransactionManager) Commit(txn *Transaction) error {
      delete(tm.active, txn.id)  // Just removes from map - nothing persisted!
      txn.active = false
      return nil
  }
  ```
- File paths in DSN are accepted but completely ignored
- All data is lost on process termination
- No isolation between concurrent transactions

**duckdb-go Reference Behavior** (from exploration):
- Relies on DuckDB C library for all WAL management
- Critical pattern from README: "Call `Close()` on the database... DuckDB synchronizes all changes from the WAL to its persistent storage"
- No explicit checkpoint API exposed - just implicit checkpoint on Close()
- PRAGMA statements for configuration (`wal_autocheckpoint`, `checkpoint_threshold`)

**DuckDB C++ WAL Architecture** (from exploration):
- Checksummed WAL entries (v1: no checksum, v2: CRC64, v3: encrypted)
- Entry types: catalog operations (CREATE_TABLE, etc.) and data operations (INSERT, DELETE, UPDATE)
- Checkpoint files: `*.wal`, `*.wal.ckpt`, `*.wal.recovery`
- Two-phase recovery: deserialize pass → checkpoint reconciliation → actual replay
- Auto-checkpoint based on WAL size threshold (default 1GB)
- Checkpoint iteration counters match WAL to database state

**API Compatibility Requirements**:
1. File paths must actually persist data
2. `Close()` must synchronize WAL to storage
3. Transaction `Commit()` must guarantee durability
4. Crash recovery must restore committed transactions
5. PRAGMA statements for WAL configuration

## What Changes

### 1. WAL Entry Format and Types (internal/wal/entry.go - NEW)

```go
// WAL entry types matching DuckDB semantics
type WALEntryType uint8

const (
    // Catalog operations (1-24)
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

    // Data operations (25-89)
    WAL_USE_TABLE     WALEntryType = 25
    WAL_INSERT_TUPLE  WALEntryType = 26
    WAL_DELETE_TUPLE  WALEntryType = 27
    WAL_UPDATE_TUPLE  WALEntryType = 28
    WAL_ROW_GROUP     WALEntryType = 29

    // Transaction boundaries (90-97)
    WAL_TXN_BEGIN  WALEntryType = 90  // Transaction start with txn ID
    WAL_TXN_COMMIT WALEntryType = 91  // Transaction commit with txn ID

    // Control entries (98-100)
    WAL_VERSION    WALEntryType = 98  // File header entry
    WAL_CHECKPOINT WALEntryType = 99  // Checkpoint marker
    WAL_FLUSH      WALEntryType = 100 // Explicit sync marker
)

// WAL file header (first entry in every WAL file)
type WALFileHeader struct {
    Magic       [4]byte   // "DWGO" - DuckDB-GO WAL identifier
    Version     uint8     // 2 = checksummed (current)
    Iteration   uint64    // Checkpoint iteration counter
    DBPathHash  uint64    // CRC64 of database path
    CreatedAt   int64     // Unix timestamp
}

// WAL entry header format
type WALEntryHeader struct {
    Size     uint64       // Entry size in bytes
    Checksum uint64       // CRC64 checksum covering Size + Type + Data
    Type     WALEntryType
}

// Entry serialization interface
type WALEntry interface {
    Type() WALEntryType
    Serialize(w io.Writer) error
    Deserialize(r io.Reader) error
}
```

### 2. WAL Writer (internal/wal/writer.go - NEW)

```go
type WALWriter struct {
    file         *os.File
    buffer       *bufio.Writer
    checksum     hash.Hash64
    version      uint8  // 1=no checksum, 2=checksum, 3=encrypted
    iteration    uint64 // Checkpoint iteration counter
    bytesWritten uint64
    clock        quartz.Clock
    fs           FileSystem  // Abstraction for crash simulation
    mu           sync.Mutex
}

// FileSystem interface for crash simulation in tests
type FileSystem interface {
    Open(path string) (*os.File, error)
    Create(path string) (*os.File, error)
    Rename(old, new string) error
    Remove(path string) error
    Sync(f *os.File) error
    SyncDir(path string) error
}

// Create new WAL writer with file header
func NewWALWriter(path string, dbPath string, clock quartz.Clock, opts ...WALOption) (*WALWriter, error) {
    cfg := defaultWALConfig()
    for _, opt := range opts {
        opt(cfg)
    }

    file, err := cfg.fs.Open(path)
    if err != nil {
        file, err = cfg.fs.Create(path)
        if err != nil {
            return nil, err
        }
    }

    w := &WALWriter{
        file:     file,
        buffer:   bufio.NewWriter(file),
        checksum: crc64.New(crc64.MakeTable(crc64.ISO)),
        version:  2,
        clock:    clock,
        fs:       cfg.fs,
    }

    // Write WAL file header as first entry
    _ = clock.Now()  // Tag: "WAL", "header", "write"
    header := WALFileHeader{
        Magic:      [4]byte{'D', 'W', 'G', 'O'},
        Version:    2,
        Iteration:  0,
        DBPathHash: crc64.Checksum([]byte(dbPath), crc64.MakeTable(crc64.ISO)),
        CreatedAt:  clock.Now().Unix(),
    }
    if err := w.writeFileHeader(header); err != nil {
        return nil, err
    }

    return w, nil
}

// Write entry with checksum (covers Size + Type + Data)
func (w *WALWriter) WriteEntry(entry WALEntry) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    _ = w.clock.Now()  // Tag: "WAL", "write", entry.Type().String()

    // Serialize to buffer first for checksum
    var buf bytes.Buffer
    if err := entry.Serialize(&buf); err != nil {
        return err
    }
    data := buf.Bytes()
    size := uint64(len(data))

    // Calculate checksum over Size + Type + Data
    w.checksum.Reset()
    binary.Write(w.checksum, binary.LittleEndian, size)
    w.checksum.Write([]byte{byte(entry.Type())})
    w.checksum.Write(data)
    checksum := w.checksum.Sum64()

    // Write header
    header := WALEntryHeader{
        Size:     size,
        Checksum: checksum,
        Type:     entry.Type(),
    }
    if err := binary.Write(w.buffer, binary.LittleEndian, header); err != nil {
        return err
    }

    // Write data
    if _, err := w.buffer.Write(data); err != nil {
        return err
    }

    w.bytesWritten += size + 17 // header size
    return nil
}

// Sync to disk (fsync) with clock instrumentation
func (w *WALWriter) Sync() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    _ = w.clock.Now()  // Tag: "WAL", "sync", "start"

    if err := w.buffer.Flush(); err != nil {
        return err
    }
    if err := w.fs.Sync(w.file); err != nil {
        return err
    }

    _ = w.clock.Now()  // Tag: "WAL", "sync", "complete"
    return nil
}
```

### 3. WAL Reader and Recovery (internal/wal/reader.go - NEW)

```go
type WALReader struct {
    file     *os.File
    reader   *bufio.Reader
    checksum hash.Hash64
    version  uint8
    clock    quartz.Clock
    fs       FileSystem
    position int64
}

// Three-phase recovery per DuckDB architecture
func (r *WALReader) Recover(storage *storage.Storage) error {
    _ = r.clock.Now()  // Tag: "recovery", "phase1", "start"

    // Phase 1: Deserialize pass - find checkpoints, validate checksums
    checkpointPos, checkpointID, err := r.scanForCheckpoints()
    if err != nil {
        return err
    }

    _ = r.clock.Now()  // Tag: "recovery", "phase2", "start"

    // Phase 2: Checkpoint reconciliation
    if checkpointID > 0 {
        if err := r.reconcileCheckpoint(checkpointPos, checkpointID); err != nil {
            return err
        }
    }

    _ = r.clock.Now()  // Tag: "recovery", "phase3", "start"

    // Phase 3: Replay committed transactions only
    if err := r.replayCommittedTransactions(storage); err != nil {
        return err
    }

    _ = r.clock.Now()  // Tag: "recovery", "complete"
    return nil
}

func (r *WALReader) ReadEntry() (WALEntry, error) {
    var header WALEntryHeader
    if err := binary.Read(r.reader, binary.LittleEndian, &header); err != nil {
        return nil, err
    }

    // Read data
    data := make([]byte, header.Size)
    if _, err := io.ReadFull(r.reader, data); err != nil {
        // Partial read = torn write, acceptable
        return nil, err
    }

    // Verify checksum (covers Size + Type + Data)
    r.checksum.Reset()
    binary.Write(r.checksum, binary.LittleEndian, header.Size)
    r.checksum.Write([]byte{byte(header.Type)})
    r.checksum.Write(data)
    if r.checksum.Sum64() != header.Checksum {
        return nil, fmt.Errorf("WAL checksum mismatch at position %d", r.position)
    }

    // Deserialize entry based on type
    return r.deserializeEntry(header.Type, data)
}
```

### 4. Checkpoint Manager (internal/wal/checkpoint.go - NEW)

```go
type CheckpointManager struct {
    storage     *storage.Storage
    wal         *WALWriter
    walPath     string
    dbPath      string
    threshold   uint64  // Auto-checkpoint threshold (default 1GB)
    iteration   uint64  // Current checkpoint iteration
    clock       quartz.Clock
    fs          FileSystem
    mu          sync.Mutex
}

type CheckpointOptions struct {
    Type      CheckpointType   // FULL, CONCURRENT, VACUUM_ONLY
    Action    CheckpointAction // IF_REQUIRED, ALWAYS
    WALAction WALAction        // DELETE_WAL, DONT_DELETE
}

func (cm *CheckpointManager) Checkpoint(opts CheckpointOptions) error {
    cm.mu.Lock()
    defer cm.mu.Unlock()

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "intent"

    // 1. Write checkpoint INTENT marker to main WAL FIRST (records intent before any work)
    cm.wal.WriteEntry(&CheckpointEntry{
        Iteration: cm.iteration + 1,  // Next iteration
        Timestamp: cm.clock.Now(),
    })

    // 2. Sync main WAL (records intent durably)
    if err := cm.wal.Sync(); err != nil {
        return err
    }

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "create"

    // 3. Create checkpoint WAL file
    ckptPath := cm.walPath + ".ckpt"
    ckptWriter, err := NewWALWriter(ckptPath, cm.dbPath, cm.clock, WithFileSystem(cm.fs))
    if err != nil {
        return err
    }

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "catalog"

    // 4. Write all catalog entries
    for _, schema := range cm.storage.Schemas() {
        if err := cm.writeSchemaCheckpoint(ckptWriter, schema); err != nil {
            return err
        }
    }

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "data"

    // 5. Write all table data
    for _, table := range cm.storage.Tables() {
        if err := cm.writeTableCheckpoint(ckptWriter, table); err != nil {
            return err
        }
    }

    // 6. Sync checkpoint WAL
    if err := ckptWriter.Sync(); err != nil {
        return err
    }

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "rename"

    // 7. Sync directory before rename (ensures file is durable)
    if err := cm.fs.SyncDir(filepath.Dir(cm.walPath)); err != nil {
        return err
    }

    // 8. Move checkpoint WAL to main WAL (atomic rename)
    if opts.WALAction == DELETE_WAL {
        cm.fs.Remove(cm.walPath)
    }
    if err := cm.fs.Rename(ckptPath, cm.walPath); err != nil {
        return err
    }

    // 9. Sync directory AGAIN (ensures rename is durable)
    if err := cm.fs.SyncDir(filepath.Dir(cm.walPath)); err != nil {
        return err
    }

    _ = cm.clock.Now()  // Tag: "checkpoint", "phase", "complete"

    // 10. Increment iteration counter
    cm.iteration++
    return nil
}

// Auto-checkpoint based on WAL size
func (cm *CheckpointManager) MaybeAutoCheckpoint() error {
    _ = cm.clock.Now()  // Tag: "checkpoint", "auto", "check"

    if cm.wal.BytesWritten() >= cm.threshold {
        _ = cm.clock.Now()  // Tag: "checkpoint", "auto", "trigger"
        return cm.Checkpoint(CheckpointOptions{
            Type:      CONCURRENT_CHECKPOINT,
            Action:    IF_REQUIRED,
            WALAction: DONT_DELETE,
        })
    }
    return nil
}
```

### 5. Storage Integration (internal/storage/persistent.go - NEW)

```go
type PersistentStorage struct {
    *Storage                  // Embed in-memory storage
    wal        *wal.WALWriter
    checkpoint *wal.CheckpointManager
    path       string
    clock      quartz.Clock
}

func NewPersistentStorage(path string, clock quartz.Clock) (*PersistentStorage, error) {
    // Try to recover from existing WAL
    storage := NewStorage()
    walPath := path + ".wal"

    if _, err := os.Stat(walPath); err == nil {
        // WAL exists - recover
        reader, err := wal.NewWALReader(walPath, clock)
        if err != nil {
            return nil, fmt.Errorf("opening WAL: %w", err)
        }
        if err := reader.Recover(storage); err != nil {
            return nil, fmt.Errorf("WAL recovery: %w", err)
        }
    }

    // Open WAL for writing
    writer, err := wal.NewWALWriter(walPath, clock)
    if err != nil {
        return nil, err
    }

    return &PersistentStorage{
        Storage:    storage,
        wal:        writer,
        checkpoint: wal.NewCheckpointManager(storage, writer, clock),
        path:       path,
        clock:      clock,
    }, nil
}

// Transaction commit with WAL durability
func (ps *PersistentStorage) CommitTransaction(txn *Transaction) error {
    // Write all pending operations to WAL
    for _, op := range txn.operations {
        if err := ps.wal.WriteEntry(op.ToWALEntry()); err != nil {
            return err
        }
    }

    // Sync WAL to disk
    if err := ps.wal.Sync(); err != nil {
        return err
    }

    // Check auto-checkpoint
    return ps.checkpoint.MaybeAutoCheckpoint()
}

// Close with implicit checkpoint
func (ps *PersistentStorage) Close() error {
    // Checkpoint to ensure durability (per duckdb-go README requirement)
    if err := ps.checkpoint.Checkpoint(CheckpointOptions{
        Type:      FULL_CHECKPOINT,
        Action:    ALWAYS,
        WALAction: DELETE_WAL, // Clean shutdown
    }); err != nil {
        return err
    }
    return ps.wal.Close()
}
```

### 6. Engine Integration (internal/engine/engine.go - MODIFIED)

```go
type Engine struct {
    storage  storage.Storage  // Interface - can be memory or persistent
    clock    quartz.Clock
    // ...existing fields
}

func NewEngine(opts ...Option) *Engine {
    cfg := defaultConfig()
    for _, opt := range opts {
        opt(cfg)
    }

    var store storage.Storage
    if cfg.Path == "" || cfg.Path == ":memory:" {
        store = storage.NewStorage() // In-memory
    } else {
        var err error
        store, err = storage.NewPersistentStorage(cfg.Path, cfg.Clock)
        if err != nil {
            return nil, err
        }
    }

    return &Engine{
        storage: store,
        clock:   cfg.Clock,
    }
}
```

### 7. PRAGMA Support (internal/engine/pragma.go - NEW)

```go
var walPragmas = map[string]PragmaHandler{
    "checkpoint_threshold": {
        Get: func(e *Engine) (any, error) {
            return e.storage.(Persistent).CheckpointThreshold(), nil
        },
        Set: func(e *Engine, val any) error {
            size, err := parseSize(val.(string)) // "1GB", "500MB", etc.
            if err != nil {
                return err
            }
            return e.storage.(Persistent).SetCheckpointThreshold(size)
        },
    },
    "wal_autocheckpoint": {
        // Alias for checkpoint_threshold
        Get: walPragmas["checkpoint_threshold"].Get,
        Set: walPragmas["checkpoint_threshold"].Set,
    },
}

func (e *Engine) ExecutePragma(name string, value any) (any, error) {
    handler, ok := walPragmas[name]
    if !ok {
        return nil, fmt.Errorf("unknown pragma: %s", name)
    }
    if value == nil {
        return handler.Get(e)
    }
    return nil, handler.Set(e, value)
}
```

### 8. Deterministic Testing Support

All WAL operations use quartz.Clock injection:
- Checkpoint timestamps use mock clock
- WAL entry timestamps use mock clock
- Auto-checkpoint timing is deterministic

```go
func TestWALRecoveryDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

    // Phase 1: Write data
    storage, _ := NewPersistentStorage("/tmp/test.db", mClock)
    storage.Insert(...)
    storage.CommitTransaction(txn)

    // Simulate crash (don't call Close)
    storage.wal.Sync()
    storage.wal.file.Close() // Crash simulation

    // Phase 2: Recovery
    mClock.Advance(time.Hour) // Deterministic time advancement
    recovered, _ := NewPersistentStorage("/tmp/test.db", mClock)

    // Verify data recovered
    rows, _ := recovered.Query("SELECT * FROM test")
    assert.Equal(t, expectedRows, rows)
}

func TestAutoCheckpointDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    trap := mClock.Trap().Now("WAL", "checkpoint")
    defer trap.Close()

    storage, _ := NewPersistentStorage("/tmp/test.db", mClock)
    storage.SetCheckpointThreshold(1024) // 1KB for testing

    // Write enough data to trigger auto-checkpoint
    go func() {
        for i := 0; i < 1000; i++ {
            storage.Insert(...)
        }
    }()

    // Wait for checkpoint trap
    call := trap.Wait(ctx)
    call.Release() // Allow checkpoint to proceed

    // Verify checkpoint occurred at deterministic time
    assert.True(t, storage.checkpoint.lastCheckpoint.Equal(mClock.Now()))
}
```

## Impact

- **Affected specs**: execution-engine (MODIFIED), NEW persistence spec
- **Affected code**:
  - NEW: `internal/wal/` package (~1500 lines)
    - `entry.go` - Entry types and serialization
    - `writer.go` - WAL writing
    - `reader.go` - WAL reading and recovery
    - `checkpoint.go` - Checkpoint manager
  - NEW: `internal/storage/persistent.go` (~400 lines)
  - MODIFIED: `internal/engine/engine.go` (~50 lines)
  - NEW: `internal/engine/pragma.go` (~100 lines)
  - NEW: `wal_test.go`, `checkpoint_test.go` (~800 lines)

- **Dependencies**:
  - `internal/storage/` - Must implement Storage interface
  - `internal/catalog/` - Schema serialization
  - `quartz.Clock` - Deterministic timing

- **Performance considerations**:
  - WAL write path adds ~1-5ms per transaction (sync overhead)
  - Auto-checkpoint configurable (default 1GB threshold)
  - Recovery time proportional to WAL size

## Breaking Changes

None. All changes are additive:
- In-memory mode (`:memory:`) unchanged
- File paths now actually persist data (previously ignored)
- `Close()` now performs checkpoint (was no-op for persistence)

## Reference Implementation Analysis

**DuckDB C++ WAL Key Files**:
- `duckdb/src/storage/write_ahead_log.cpp` - WAL entry writing
- `duckdb/src/storage/wal_replay.cpp` - Two-phase recovery
- `duckdb/src/storage/checkpoint_manager.cpp` - Checkpoint execution
- `duckdb/src/include/duckdb/common/enums/wal_type.hpp` - Entry types

**Key Implementation Details**:
1. CRC64 checksum (ISO polynomial) per entry
2. Checkpoint iteration counter matches WAL to database state
3. Three WAL files during recovery: main, checkpoint, recovery
4. Entry types separate catalog operations from data operations
5. Auto-checkpoint triggers at configurable WAL size threshold

## Validation Criteria

1. File paths actually persist data across process restarts
2. `Close()` performs implicit checkpoint (per duckdb-go README)
3. Crash recovery restores all committed transactions
4. Uncommitted transactions are rolled back on recovery
5. PRAGMA `checkpoint_threshold` configures auto-checkpoint
6. All tests pass with mock clock injection
7. No race conditions in concurrent WAL access
