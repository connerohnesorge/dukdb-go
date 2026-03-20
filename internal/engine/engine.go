// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/cache"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/extension"
	"github.com/dukdb/dukdb-go/internal/fts"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/persistence"
	pgcatalog "github.com/dukdb/dukdb-go/internal/postgres/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/storage/duckdb"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// Engine is the core execution engine implementing the Backend interface.
// It manages the catalog, storage, and transaction manager for the database.
type Engine struct {
	mu            sync.RWMutex
	catalog       *catalog.Catalog
	storage       *storage.Storage
	txnMgr        *TransactionManager
	optimizer     *optimizer.CostBasedOptimizer // Cost-based query optimizer
	walWriter     *wal.Writer                   // WAL writer for persistent databases
	checkpointMgr *wal.CheckpointManager        // Checkpoint manager for WAL-based checkpointing
	config        *dukdb.Config
	path          string
	persistent    bool // true if not :memory:
	closed        bool
	queryCache    *cache.QueryResultCache

	// Extension registry for INSTALL/LOAD support
	extensions *extension.Registry

	// SERIALIZABLE isolation level support
	conflictDetector *storage.ConflictDetector // Shared conflict detector for all connections
	lockManager      *storage.LockManager      // Shared lock manager for all connections

	// Database management
	dbManager *DatabaseManager // Registry of attached databases

	// Full-text search
	ftsRegistry *fts.Registry // Registry of FTS indexes
}

// NewEngine creates a new Engine instance.
func NewEngine() *Engine {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	dbMgr := NewDatabaseManager()
	// Register the default in-memory database
	_ = dbMgr.Attach("memory", ":memory:", false, cat, stor)
	return &Engine{
		catalog:          cat,
		storage:          stor,
		txnMgr:           NewTransactionManager(),
		optimizer:        optimizer.NewCostBasedOptimizer(cat),
		extensions:       extension.DefaultRegistry(),
		conflictDetector: storage.NewConflictDetector(),
		lockManager:      storage.NewLockManager(),
		queryCache:       cache.NewQueryResultCache(cache.DefaultMaxBytes, cache.DefaultTTL),
		dbManager:        dbMgr,
		ftsRegistry:      fts.NewRegistry(),
	}
}

// QueryCache returns the shared query result cache.
func (e *Engine) QueryCache() *cache.QueryResultCache {
	return e.queryCache
}

// Open opens a connection to the database at the given path.
// Implements the Backend interface.
func (e *Engine) Open(
	path string,
	config *dukdb.Config,
) (dukdb.BackendConn, error) {
	// For :memory: databases, create a new isolated Engine instance
	// to ensure complete isolation between connections
	if path == ":memory:" {
		isolatedEngine := NewEngine()
		isolatedEngine.path = path
		if config != nil {
			isolatedEngine.config = config
		} else {
			isolatedEngine.config = &dukdb.Config{
				AccessMode: "read_write",
			}
		}
		isolatedEngine.persistent = false
		if err := registerInformationSchema(isolatedEngine, path); err != nil {
			return nil, err
		}

		// Initialize glob settings from config
		maxFilesPerGlob := dukdb.DefaultMaxFilesPerGlob
		fileGlobTimeout := dukdb.DefaultFileGlobTimeout
		if isolatedEngine.config.MaxFilesPerGlob > 0 {
			maxFilesPerGlob = isolatedEngine.config.MaxFilesPerGlob
		}
		if isolatedEngine.config.FileGlobTimeout > 0 {
			fileGlobTimeout = isolatedEngine.config.FileGlobTimeout
		}

		conn := &EngineConn{
			id:              generateConnID(),
			engine:          isolatedEngine,
			txn:             isolatedEngine.txnMgr.Begin(),
			maxFilesPerGlob: maxFilesPerGlob,
			fileGlobTimeout: fileGlobTimeout,
			sqlPrepared:     make(map[string]*sqlPreparedStatement),
		}

		return conn, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, dukdb.ErrConnectionClosed
	}

	// Store configuration
	e.path = path
	if config != nil {
		e.config = config
	} else {
		e.config = &dukdb.Config{
			AccessMode: "read_write",
		}
	}

	// Handle persistence for non-memory databases
	if path != "" && path != ":memory:" {
		e.persistent = true

		// WAL path is database path + ".wal"
		walPath := path + ".wal"

		// First, load from the database file if it exists
		// This contains all checkpointed data
		if _, err := os.Stat(path); err == nil {
			// Auto-detect format
			format, detectErr := persistence.DetectFileFormat(path)
			if detectErr != nil {
				return nil, fmt.Errorf(
					"failed to detect file format: %w",
					detectErr,
				)
			}

			if format != persistence.FormatDuckDB {
				return nil, fmt.Errorf(
					"unknown or unsupported file format: file may be corrupted or not a valid DuckDB database file",
				)
			}

			// Load the file
			if err := e.loadFromFile(path); err != nil {
				return nil, fmt.Errorf(
					"failed to load database: %w",
					err,
				)
			}
		}

		// Then replay any existing WAL file to recover uncommitted state
		// (changes that happened after the last checkpoint)
		if _, err := os.Stat(walPath); err == nil {
			// WAL file exists - replay it
			if err := e.replayWAL(walPath); err != nil {
				return nil, fmt.Errorf(
					"failed to replay WAL: %w",
					err,
				)
			}
		}

		// Create/open WAL writer for new operations
		walWriter, err := wal.NewWriter(walPath, quartz.NewReal())
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create WAL writer: %w",
				err,
			)
		}
		e.walWriter = walWriter

		// Initialize CheckpointManager with configured threshold
		// Task 4.1: Read checkpoint_threshold from settings during database open
		thresholdStr := "256MB" // Task 4.3: Default value
		if e.config != nil && e.config.CheckpointThreshold != "" {
			thresholdStr = e.config.CheckpointThreshold
		}

		// Task 4.1: Parse the threshold value
		thresholdBytes, err := dukdb.ParseThreshold(thresholdStr)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to parse checkpoint_threshold: %w",
				err,
			)
		}

		// Task 4.2: Pass threshold to CheckpointManager constructor
		e.checkpointMgr = wal.NewCheckpointManager(
			e.walWriter,
			e.catalog,
			e.storage,
			quartz.NewReal(),
			uint64(thresholdBytes),
		)
	}
	if err := registerInformationSchema(e, path); err != nil {
		return nil, err
	}

	// Initialize glob settings from config
	maxFilesPerGlob := dukdb.DefaultMaxFilesPerGlob
	fileGlobTimeout := dukdb.DefaultFileGlobTimeout
	if e.config != nil {
		if e.config.MaxFilesPerGlob > 0 {
			maxFilesPerGlob = e.config.MaxFilesPerGlob
		}
		if e.config.FileGlobTimeout > 0 {
			fileGlobTimeout = e.config.FileGlobTimeout
		}
	}

	// Create a new connection
	conn := &EngineConn{
		id:              generateConnID(),
		engine:          e,
		txn:             e.txnMgr.Begin(),
		maxFilesPerGlob: maxFilesPerGlob,
		fileGlobTimeout: fileGlobTimeout,
		sqlPrepared:     make(map[string]*sqlPreparedStatement),
	}

	return conn, nil
}

func registerInformationSchema(engine *Engine, path string) error {
	if engine == nil || engine.catalog == nil {
		return nil
	}

	dbName := "dukdb"
	if path == ":memory:" {
		dbName = "memory"
	} else if path != "" {
		dbName = filepath.Base(path)
		if dbName == "" {
			dbName = path
		}
	}

	return pgcatalog.RegisterInformationSchemaVirtualTables(
		engine.catalog,
		engine.catalog,
		dbName,
	)
}

// Close closes the engine and releases all resources.
// Implements the Backend interface.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	// Sync and close WAL writer first
	if e.walWriter != nil {
		if err := e.walWriter.Sync(); err != nil {
			return fmt.Errorf(
				"failed to sync WAL: %w",
				err,
			)
		}
		if err := e.walWriter.Close(); err != nil {
			return fmt.Errorf(
				"failed to close WAL: %w",
				err,
			)
		}
		e.walWriter = nil
	}

	// Save to file if persistent
	if e.persistent && e.path != "" &&
		e.path != ":memory:" {
		if err := e.saveToFile(e.path); err != nil {
			return fmt.Errorf(
				"failed to save database: %w",
				err,
			)
		}

		// After successful checkpoint, remove the WAL file
		// since all changes are now persisted to the data file
		walPath := e.path + ".wal"
		if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
			// Log but don't fail - WAL removal is not critical
			// The next open will replay it which is harmless but inefficient
		}
	}

	e.closed = true

	// Close the storage
	if e.storage != nil {
		if err := e.storage.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Catalog returns the catalog for metadata operations.
func (e *Engine) Catalog() *catalog.Catalog {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.catalog
}

// Storage returns the storage manager.
func (e *Engine) Storage() *storage.Storage {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.storage
}

// TransactionManager returns the transaction manager.
func (e *Engine) TransactionManager() *TransactionManager {
	return e.txnMgr
}

// Optimizer returns the cost-based query optimizer.
// Returns nil if optimization is disabled.
func (e *Engine) Optimizer() *optimizer.CostBasedOptimizer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.optimizer
}

// Extensions returns the extension registry.
func (e *Engine) Extensions() *extension.Registry {
	return e.extensions
}

// ConflictDetector returns the shared conflict detector for SERIALIZABLE isolation.
func (e *Engine) ConflictDetector() *storage.ConflictDetector {
	return e.conflictDetector
}

// LockManager returns the shared lock manager for SERIALIZABLE isolation.
func (e *Engine) LockManager() *storage.LockManager {
	return e.lockManager
}

// DatabaseManager returns the database manager for attached databases.
func (e *Engine) DatabaseManager() *DatabaseManager {
	return e.dbManager
}

// FTSRegistry returns the full-text search index registry.
func (e *Engine) FTSRegistry() *fts.Registry {
	return e.ftsRegistry
}

// TransactionManager manages transactions for the engine.
type TransactionManager struct {
	mu        sync.Mutex
	nextTxnID uint64
	active    map[uint64]*Transaction
	// committedSince tracks transaction IDs that have committed since each active transaction started.
	// Key is the observing transaction ID, value is slice of transaction IDs that committed after it started.
	// Used for SERIALIZABLE conflict detection at commit time.
	committedSince map[uint64][]uint64
}

// NewTransactionManager creates a new TransactionManager.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		nextTxnID:      1,
		active:         make(map[uint64]*Transaction),
		committedSince: make(map[uint64][]uint64),
	}
}

// Begin starts a new transaction with the default isolation level (SERIALIZABLE).
func (tm *TransactionManager) Begin() *Transaction {
	return tm.BeginWithIsolation(parser.IsolationLevelSerializable)
}

// BeginWithIsolation starts a new transaction with the specified isolation level.
// The isolation level determines what data the transaction can see when other
// transactions are running concurrently.
//
// Supported isolation levels:
//   - IsolationLevelSerializable: Strictest level, prevents all anomalies
//   - IsolationLevelRepeatableRead: Prevents dirty and non-repeatable reads
//   - IsolationLevelReadCommitted: Prevents dirty reads only
//   - IsolationLevelReadUncommitted: Allows dirty reads
//
// For REPEATABLE READ and SERIALIZABLE isolation levels, a snapshot is taken
// at transaction start. This snapshot captures which transactions were active
// at that moment, enabling consistent reads throughout the transaction.
func (tm *TransactionManager) BeginWithIsolation(level parser.IsolationLevel) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()

	txn := &Transaction{
		id:             tm.nextTxnID,
		active:         true,
		isolationLevel: level,
		startTime:      now,
		savepoints:     NewSavepointStack(),
	}

	// For REPEATABLE READ and SERIALIZABLE, take a snapshot at transaction start.
	// This snapshot captures the current set of active transactions, which is
	// used for visibility checks to ensure consistent reads.
	if level == parser.IsolationLevelRepeatableRead || level == parser.IsolationLevelSerializable {
		txn.snapshot = tm.takeSnapshotLocked(now)
	}

	// For SERIALIZABLE, initialize tracking for concurrent committed transactions
	if level == parser.IsolationLevelSerializable {
		tm.committedSince[txn.id] = []uint64{}
	}

	tm.nextTxnID++
	tm.active[txn.id] = txn

	return txn
}

// TakeSnapshot captures the current database state for snapshot-based isolation.
// It returns a Snapshot containing the current timestamp and the list of
// currently active (uncommitted) transaction IDs.
//
// This method is thread-safe and acquires the necessary locks.
func (tm *TransactionManager) TakeSnapshot() *storage.Snapshot {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.takeSnapshotLocked(time.Now())
}

// takeSnapshotLocked creates a snapshot while holding the mutex.
// The caller must hold tm.mu.
func (tm *TransactionManager) takeSnapshotLocked(timestamp time.Time) *storage.Snapshot {
	// Collect all currently active transaction IDs
	activeIDs := make([]uint64, 0, len(tm.active))
	for id := range tm.active {
		activeIDs = append(activeIDs, id)
	}
	return storage.NewSnapshot(timestamp, activeIDs)
}

// GetActiveTransactionIDs returns a slice of all currently active transaction IDs.
// This is useful for debugging and testing.
func (tm *TransactionManager) GetActiveTransactionIDs() []uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	ids := make([]uint64, 0, len(tm.active))
	for id := range tm.active {
		ids = append(ids, id)
	}
	return ids
}

// Commit commits a transaction.
func (tm *TransactionManager) Commit(
	txn *Transaction,
) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !txn.active {
		return dukdb.ErrTransactionAlreadyEnded
	}

	// For all other active SERIALIZABLE transactions, record that this transaction committed.
	// This is used for conflict detection at their commit time.
	for otherID, txnRef := range tm.active {
		if otherID != txn.id && txnRef.isolationLevel == parser.IsolationLevelSerializable {
			tm.committedSince[otherID] = append(tm.committedSince[otherID], txn.id)
		}
	}

	delete(tm.active, txn.id)
	txn.active = false

	// Clean up committed tracking for this transaction
	delete(tm.committedSince, txn.id)

	// Clear savepoints on commit
	if txn.savepoints != nil {
		txn.savepoints.Clear()
	}

	return nil
}

// Rollback rolls back a transaction.
func (tm *TransactionManager) Rollback(
	txn *Transaction,
) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !txn.active {
		return dukdb.ErrTransactionAlreadyEnded
	}

	delete(tm.active, txn.id)
	txn.active = false

	// Clean up committed tracking for this transaction
	delete(tm.committedSince, txn.id)

	// Clear savepoints on rollback
	if txn.savepoints != nil {
		txn.savepoints.Clear()
	}

	return nil
}

// GetConcurrentCommitted returns the list of transaction IDs that have committed
// since the given transaction started. This is used for SERIALIZABLE conflict detection.
func (tm *TransactionManager) GetConcurrentCommitted(txnID uint64) []uint64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if concurrent, ok := tm.committedSince[txnID]; ok {
		// Return a copy to prevent external modification
		result := make([]uint64, len(concurrent))
		copy(result, concurrent)
		return result
	}
	return nil
}

// UndoOpType represents the type of DML operation that can be undone.
type UndoOpType int

const (
	// UndoInsert - to undo: delete these rows
	UndoInsert UndoOpType = iota
	// UndoDelete - to undo: undelete (clear tombstones)
	UndoDelete
	// UndoUpdate - to undo: restore before-image
	UndoUpdate
)

// UndoOperation represents a single DML operation that can be undone.
type UndoOperation struct {
	TableName   string
	OpType      UndoOpType
	RowIDs      []uint64
	BeforeImage map[int][]any // Column index -> values (for UPDATE)
}

// Transaction represents a database transaction.
type Transaction struct {
	id     uint64
	active bool

	// Isolation level for this transaction
	isolationLevel parser.IsolationLevel

	// startTime is when this transaction began.
	// Used for REPEATABLE READ and SERIALIZABLE snapshot-based visibility.
	startTime time.Time

	// Statement time tracking for READ COMMITTED isolation.
	// This is the timestamp when the current statement started.
	// It gets updated at the beginning of each statement execution.
	statementTime time.Time

	// snapshot is the point-in-time view for REPEATABLE READ and SERIALIZABLE.
	// It captures which transactions were active at transaction start.
	// This is nil for READ UNCOMMITTED and READ COMMITTED isolation levels.
	snapshot *storage.Snapshot

	// DDL rollback support: snapshot state at transaction start
	catalogSnapshot *catalog.Catalog
	storageSnapshot *storage.Storage

	// DML rollback support: undo log for INSERT/UPDATE/DELETE operations
	undoLog []UndoOperation
	mu      sync.Mutex // protects undoLog, savepoints, and statementTime

	// Savepoint support: stack of named savepoints within the transaction
	savepoints *SavepointStack
}

// RecordUndo adds an undo operation to the transaction's undo log.
// Operations are added in order and will be applied in reverse on rollback.
func (t *Transaction) RecordUndo(op UndoOperation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.undoLog = append(t.undoLog, op)
}

// GetUndoLog returns the undo log for rollback processing.
func (t *Transaction) GetUndoLog() []UndoOperation {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.undoLog
}

// ClearUndoLog clears the undo log and savepoints (called on commit).
func (t *Transaction) ClearUndoLog() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.undoLog = nil
	if t.savepoints != nil {
		t.savepoints.Clear()
	}
}

// ID returns the transaction ID.
func (t *Transaction) ID() uint64 {
	return t.id
}

// IsActive returns whether the transaction is still active.
func (t *Transaction) IsActive() bool {
	return t.active
}

// GetIsolationLevel returns the isolation level of this transaction.
// The isolation level determines what data the transaction can see when
// other transactions are running concurrently.
func (t *Transaction) GetIsolationLevel() parser.IsolationLevel {
	return t.isolationLevel
}

// SetIsolationLevel sets the isolation level for this transaction.
// This should only be called before the transaction performs any reads.
// Calling this on an active transaction with ongoing reads may lead to
// inconsistent behavior.
func (t *Transaction) SetIsolationLevel(level parser.IsolationLevel) {
	t.isolationLevel = level
}

// BeginStatement marks the start of a new statement within this transaction.
// This updates the statement timestamp which is used by READ COMMITTED
// isolation level to determine visibility. Each statement in READ COMMITTED
// sees a snapshot of data as of its start time.
//
// This method should be called at the beginning of each statement execution,
// before any reads are performed.
func (t *Transaction) BeginStatement() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statementTime = time.Now()
}

// GetStatementTime returns the timestamp when the current statement started.
// This is used by READ COMMITTED isolation to determine visibility of rows.
// If no statement has started yet, returns the zero time.
func (t *Transaction) GetStatementTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.statementTime
}

// GetStartTime returns the timestamp when this transaction started.
// This is used for REPEATABLE READ and SERIALIZABLE isolation levels
// to determine the snapshot point for visibility checks.
func (t *Transaction) GetStartTime() time.Time {
	return t.startTime
}

// GetSnapshot returns the snapshot taken at transaction start.
// This is only non-nil for REPEATABLE READ and SERIALIZABLE isolation levels.
// The snapshot contains the timestamp and list of active transaction IDs
// at the moment the transaction began.
func (t *Transaction) GetSnapshot() *storage.Snapshot {
	return t.snapshot
}

// HasSnapshot returns true if this transaction has a snapshot.
// Snapshots are created for REPEATABLE READ and SERIALIZABLE isolation levels.
func (t *Transaction) HasSnapshot() bool {
	return t.snapshot != nil
}

// WasActiveAtSnapshot returns true if the given transaction ID was active
// (uncommitted) when this transaction's snapshot was taken.
// Returns false if this transaction has no snapshot.
func (t *Transaction) WasActiveAtSnapshot(txnID uint64) bool {
	if t.snapshot == nil {
		return false
	}
	return t.snapshot.WasActiveAtSnapshot(txnID)
}

// CreateSavepoint creates a new savepoint with the given name.
// The savepoint records the current position in the undo log so that
// RollbackToSavepoint can undo operations back to this point.
// If a savepoint with the same name already exists, it is replaced (PostgreSQL behavior).
func (t *Transaction) CreateSavepoint(name string, createdAt time.Time) error {
	if !t.active {
		return errors.New("cannot create savepoint: transaction is not active")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Initialize savepoints if nil (shouldn't happen if Begin was used)
	if t.savepoints == nil {
		t.savepoints = NewSavepointStack()
	}

	sp := &Savepoint{
		Name:      name,
		UndoIndex: len(t.undoLog),
		CreatedAt: createdAt,
	}

	return t.savepoints.Push(sp)
}

// RollbackToSavepoint rolls back the transaction to the specified savepoint.
// This undoes all operations performed after the savepoint was created.
// The savepoint and any nested savepoints are released after the rollback.
func (t *Transaction) RollbackToSavepoint(
	name string,
	undoFunc func(op UndoOperation) error,
) error {
	if !t.active {
		return errors.New("cannot rollback to savepoint: transaction is not active")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savepoints == nil {
		return fmt.Errorf("savepoint %q does not exist", name)
	}

	// Get the savepoint and release it along with any nested savepoints
	sp, err := t.savepoints.RollbackTo(name)
	if err != nil {
		return err
	}

	// Undo operations in REVERSE order (from end of undoLog back to savepoint's UndoIndex)
	if undoFunc != nil {
		for i := len(t.undoLog) - 1; i >= sp.UndoIndex; i-- {
			if err := undoFunc(t.undoLog[i]); err != nil {
				return fmt.Errorf("failed to undo operation %d: %w", i, err)
			}
		}
	}

	// Truncate the undo log to the savepoint's position
	t.undoLog = t.undoLog[:sp.UndoIndex]

	return nil
}

// ReleaseSavepoint releases the specified savepoint and any nested savepoints.
// The operations performed since the savepoint are kept and will be committed
// or rolled back with the transaction.
func (t *Transaction) ReleaseSavepoint(name string) error {
	if !t.active {
		return errors.New("cannot release savepoint: transaction is not active")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savepoints == nil {
		return fmt.Errorf("savepoint %q does not exist", name)
	}

	return t.savepoints.Release(name)
}

// GetSavepoint returns the savepoint with the given name, or nil if not found.
func (t *Transaction) GetSavepoint(name string) (*Savepoint, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savepoints == nil {
		return nil, false
	}

	return t.savepoints.Get(name)
}

// SavepointCount returns the number of active savepoints in the transaction.
func (t *Transaction) SavepointCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savepoints == nil {
		return 0
	}

	return t.savepoints.Len()
}

// TransactionContextAdapter implements storage.TransactionContext for visibility checks.
// It wraps a Transaction and TransactionManager to provide the context needed for MVCC.
type TransactionContextAdapter struct {
	txn    *Transaction
	txnMgr *TransactionManager
}

// NewTransactionContextAdapter creates a new TransactionContextAdapter.
func NewTransactionContextAdapter(
	txn *Transaction,
	txnMgr *TransactionManager,
) *TransactionContextAdapter {
	return &TransactionContextAdapter{
		txn:    txn,
		txnMgr: txnMgr,
	}
}

// GetTxnID returns the unique identifier for this transaction.
func (a *TransactionContextAdapter) GetTxnID() uint64 {
	return a.txn.ID()
}

// GetIsolationLevel returns the isolation level for this transaction.
func (a *TransactionContextAdapter) GetIsolationLevel() parser.IsolationLevel {
	return a.txn.GetIsolationLevel()
}

// GetStartTime returns the timestamp when this transaction started.
// This is used for REPEATABLE READ and SERIALIZABLE isolation levels
// to determine the snapshot point for visibility checks.
func (a *TransactionContextAdapter) GetStartTime() time.Time {
	return a.txn.GetStartTime()
}

// GetStatementTime returns the timestamp when the current statement started.
// This is used by READ COMMITTED isolation level to determine visibility.
// Each statement sees a snapshot of committed data as of this time.
func (a *TransactionContextAdapter) GetStatementTime() time.Time {
	return a.txn.GetStatementTime()
}

// IsCommitted checks if another transaction has committed.
func (a *TransactionContextAdapter) IsCommitted(txnID uint64) bool {
	a.txnMgr.mu.Lock()
	defer a.txnMgr.mu.Unlock()
	// If the transaction is not in the active map, it has either committed or been rolled back.
	// For now, we assume non-active transactions have committed (unless aborted).
	// A proper implementation would track committed vs rolled back transactions.
	_, isActive := a.txnMgr.active[txnID]
	return !isActive
}

// IsAborted checks if another transaction has been aborted.
// For now, this returns false since we don't track aborted transactions separately.
// A proper implementation would track rolled back transaction IDs.
func (a *TransactionContextAdapter) IsAborted(txnID uint64) bool {
	// TODO: Track aborted transactions for proper MVCC support
	return false
}

// GetSnapshot returns the snapshot taken at transaction start.
// This is only non-nil for REPEATABLE READ and SERIALIZABLE isolation levels.
func (a *TransactionContextAdapter) GetSnapshot() *storage.Snapshot {
	return a.txn.GetSnapshot()
}

// WasActiveAtSnapshot returns true if the given transaction ID was active
// (uncommitted) when this transaction's snapshot was taken.
// This is used for REPEATABLE READ and SERIALIZABLE visibility checks.
// Returns false if this transaction has no snapshot.
func (a *TransactionContextAdapter) WasActiveAtSnapshot(txnID uint64) bool {
	return a.txn.WasActiveAtSnapshot(txnID)
}

// loadFromFile loads the database from a file
func (e *Engine) loadFromFile(path string) error {
	fm, err := persistence.OpenFile(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = fm.Close()
	}()

	// Load combined catalog and storage data
	data, err := fm.ReadCatalog()
	if err != nil {
		return fmt.Errorf(
			"failed to read data: %w",
			err,
		)
	}

	// Import combined catalog and storage data
	if err := e.importDatabase(data); err != nil {
		return fmt.Errorf(
			"failed to import database: %w",
			err,
		)
	}

	return nil
}

// saveToFile saves the database to a file
func (e *Engine) saveToFile(path string) error {
	// Atomic save: write to temp file, verify, then rename
	tmpPath := path + ".tmp"

	fm, err := persistence.CreateFile(tmpPath)
	if err != nil {
		return err
	}

	// Export combined catalog and storage data
	data, err := e.exportDatabase()
	if err != nil {
		_ = fm.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf(
			"failed to export database: %w",
			err,
		)
	}

	// Write combined data
	if err := fm.WriteCatalog(data); err != nil {
		_ = fm.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"failed to write data: %w",
			err,
		)
	}

	// Finalize with headers
	if err := fm.Finalize(); err != nil {
		_ = fm.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"failed to finalize file: %w",
			err,
		)
	}
	_ = fm.Close()

	// Verify checksum before rename
	if err := persistence.VerifyFile(tmpPath); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"save verification failed: %w",
			err,
		)
	}

	// Atomic rename (preserves original on failure)
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)

		return err
	}

	return nil
}

// WAL returns the WAL writer for this engine.
// Returns nil for in-memory databases.
func (e *Engine) WAL() *wal.Writer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.walWriter
}

// replayWAL replays the WAL file to recover database state.
func (e *Engine) replayWAL(walPath string) error {
	reader, err := wal.NewReader(walPath, quartz.NewReal())
	if err != nil {
		return fmt.Errorf(
			"failed to open WAL reader: %w",
			err,
		)
	}
	defer func() {
		_ = reader.Close()
	}()

	// Use the WAL reader's Recover method to replay entries
	if err := reader.Recover(e.catalog, e.storage); err != nil {
		return fmt.Errorf(
			"failed to recover from WAL: %w",
			err,
		)
	}

	return nil
}

// Verify that Engine implements Backend interface
var _ dukdb.Backend = (*Engine)(nil)

// init registers the Engine as the default backend.
func init() {
	dukdb.RegisterBackend(NewEngine())
}

// exportDatabase exports both catalog and storage data to a single byte slice.
// The format is:
//   - Catalog data (binary serialized)
//   - Storage marker (STRG)
//   - Number of tables (uint32)
//   - For each table:
//   - Table name length (uint32) + name bytes
//   - Column count (uint16)
//   - Column types (uint8 each)
//   - Row group count (uint32)
//   - For each row group:
//   - Serialized row group data
func (e *Engine) exportDatabase() ([]byte, error) {
	// Export catalog first
	catalogData, err := e.catalog.Export()
	if err != nil {
		return nil, fmt.Errorf("failed to export catalog: %w", err)
	}

	// Create combined data buffer
	buf := new(bytes.Buffer)

	// Write catalog data length and data
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(catalogData))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(catalogData); err != nil {
		return nil, err
	}

	// Write storage marker
	if _, err := buf.WriteString("STRG"); err != nil {
		return nil, err
	}

	// Get all tables from storage
	tables := e.storage.Tables()

	// Write table count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tables))); err != nil {
		return nil, err
	}

	// Write each table's data
	for name, table := range tables {
		// Write table name
		nameBytes := []byte(name)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, err
		}

		// Write column types
		colTypes := table.ColumnTypes()
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(colTypes))); err != nil {
			return nil, err
		}
		for _, ct := range colTypes {
			if err := buf.WriteByte(byte(ct)); err != nil {
				return nil, err
			}
		}

		// Get and write row groups
		rowGroups := table.RowGroups()
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(rowGroups))); err != nil {
			return nil, err
		}

		for i, rg := range rowGroups {
			rgData, err := table.ExportRowGroup(rg)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to export row group %d of table %s: %w",
					i,
					name,
					err,
				)
			}

			// Write row group data length and data
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(rgData))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(rgData); err != nil {
				return nil, err
			}
		}
	}

	// Write statistics marker to enable forward compatibility
	if _, err := buf.WriteString("STAT"); err != nil {
		return nil, err
	}

	// Serialize and persist statistics for all tables
	for name := range tables {
		// Get table definition from catalog to access statistics
		tableDef, ok := e.catalog.GetTableInSchema("main", name)
		if !ok {
			// Table exists in storage but not in catalog - skip statistics
			continue
		}

		stats := tableDef.GetStatistics()
		if stats == nil {
			// No statistics collected for this table yet - write empty marker
			if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
				return nil, err
			}
			continue
		}

		// Serialize table statistics in DuckDB binary format
		serializer := optimizer.NewStatsSerializer()
		statsData, err := serializer.SerializeTableStatistics(stats)
		if err != nil {
			// Log but don't fail - statistics not available for this table
			if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
				return nil, err
			}
			continue
		}

		// Write table name for statistics lookup
		nameBytes := []byte(name)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, err
		}

		// Write statistics data length and data
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(statsData))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(statsData); err != nil {
			return nil, err
		}
	}

	// Write terminator for statistics section (length 0 with empty name)
	if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// CatalogFormat represents the detected format of the database file.
type CatalogFormat int

const (
	// FormatUnknown indicates the format could not be determined.
	FormatUnknown CatalogFormat = iota
	// FormatCustom indicates the native dukdb-go format (4-byte length prefix).
	FormatCustom
	// FormatDuckDB indicates a real DuckDB database file.
	FormatDuckDB
)

// detectCatalogFormat detects whether the data is in DuckDB format or custom format.
// DuckDB format: Has "DUCK" signature at offset 8, or starts with field IDs (uint16 LE).
// Custom format: First 4 bytes are a small uint32 catalog length (<100MB).
func detectCatalogFormat(data []byte) CatalogFormat {
	if len(data) < 4 {
		return FormatUnknown
	}

	// Check for DuckDB "DUCK" signature at offset 8 (standard DuckDB file format)
	if len(data) >= 12 {
		if string(data[8:12]) == "DUCK" {
			return FormatDuckDB
		}
	}

	// Check first 4 bytes as uint32 (custom format catalog length) FIRST
	// This prevents misidentification of native format files which start with
	// PropertyIDType (1) and Version (1), appearing as uint16 value 257 (0x0101)
	// which falls in the DuckDB field ID range (99-300) when checked as uint16.
	first4 := binary.LittleEndian.Uint32(data[0:4])
	if first4 > 0 && first4 < 100*1024*1024 {
		return FormatCustom
	}

	// Check first 2 bytes as uint16 (DuckDB field ID for metadata block format)
	fieldID := binary.LittleEndian.Uint16(data[0:2])
	if (fieldID >= 99 && fieldID <= 300) || fieldID == 0xFFFF {
		return FormatDuckDB
	}

	return FormatUnknown
}

// importDatabase imports both catalog and storage data from a byte slice.
// It auto-detects the format (DuckDB vs custom) and imports accordingly.
func (e *Engine) importDatabase(data []byte) error {
	format := detectCatalogFormat(data)

	switch format {
	case FormatDuckDB:
		return e.importDuckDBFormat(data)
	case FormatCustom:
		return e.importCustomFormat(data)
	case FormatUnknown:
		return fmt.Errorf("unknown database format: cannot detect catalog format from first bytes")
	default:
		return fmt.Errorf("unknown database format: %v", format)
	}
}

// importCustomFormat imports data from the native dukdb-go format.
// Format: catalog length (4 bytes) + catalog data + "STRG" marker + table data + optional "STAT" section.
func (e *Engine) importCustomFormat(data []byte) error {
	r := bytes.NewReader(data)

	// Read catalog data length
	var catalogLen uint32
	if err := binary.Read(r, binary.LittleEndian, &catalogLen); err != nil {
		return fmt.Errorf("failed to read catalog length: %w", err)
	}

	// Read catalog data
	catalogData := make([]byte, catalogLen)
	if _, err := io.ReadFull(r, catalogData); err != nil {
		return fmt.Errorf("failed to read catalog data: %w", err)
	}

	// Import catalog
	if err := e.catalog.Import(catalogData); err != nil {
		return fmt.Errorf("failed to import catalog: %w", err)
	}

	// Read and verify storage marker
	marker := make([]byte, 4)
	if _, err := io.ReadFull(r, marker); err != nil {
		// No storage data (old format), just return
		return nil
	}
	if string(marker) != "STRG" {
		return fmt.Errorf("invalid storage marker: %s", string(marker))
	}

	// Read table count
	var tableCount uint32
	if err := binary.Read(r, binary.LittleEndian, &tableCount); err != nil {
		return fmt.Errorf("failed to read table count: %w", err)
	}

	// Read each table's data
	for i := uint32(0); i < tableCount; i++ {
		// Read table name
		var nameLen uint32
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return fmt.Errorf("failed to read table name length: %w", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return fmt.Errorf("failed to read table name: %w", err)
		}
		tableName := string(nameBytes)

		// Read column types
		var colCount uint16
		if err := binary.Read(r, binary.LittleEndian, &colCount); err != nil {
			return fmt.Errorf("failed to read column count: %w", err)
		}
		colTypes := make([]dukdb.Type, colCount)
		for j := uint16(0); j < colCount; j++ {
			typeByte, err := r.ReadByte()
			if err != nil {
				return fmt.Errorf("failed to read column type: %w", err)
			}
			colTypes[j] = dukdb.Type(typeByte)
		}

		// Create or get the storage table
		table, err := e.storage.CreateTable(tableName, colTypes)
		if err != nil {
			// Table might already exist from catalog import
			existingTable, ok := e.storage.GetTable(tableName)
			if !ok {
				return fmt.Errorf("failed to create or get table %s: %w", tableName, err)
			}
			table = existingTable
		}

		// Read row group count
		var rgCount uint32
		if err := binary.Read(r, binary.LittleEndian, &rgCount); err != nil {
			return fmt.Errorf("failed to read row group count: %w", err)
		}

		// Read and import each row group
		for j := uint32(0); j < rgCount; j++ {
			// Read row group data length
			var rgLen uint32
			if err := binary.Read(r, binary.LittleEndian, &rgLen); err != nil {
				return fmt.Errorf("failed to read row group length: %w", err)
			}

			// Read row group data
			rgData := make([]byte, rgLen)
			if _, err := io.ReadFull(r, rgData); err != nil {
				return fmt.Errorf("failed to read row group data: %w", err)
			}

			// Import the row group
			rg, err := table.ImportRowGroup(rgData)
			if err != nil {
				return fmt.Errorf(
					"failed to import row group %d of table %s: %w",
					j,
					tableName,
					err,
				)
			}

			// Add to table
			table.AddRowGroup(rg)
		}
	}

	// Try to load statistics section if it exists
	// Read and verify statistics marker (optional - may not exist in old databases)
	statMarker := make([]byte, 4)
	_, errReadMarker := io.ReadFull(r, statMarker)
	if errReadMarker == nil && string(statMarker) == "STAT" {
		// Statistics section exists, load statistics for each table
		for {
			// Read table name length
			var nameLen uint32
			if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
				// End of statistics section or read error
				break
			}

			if nameLen == 0 {
				// Terminator reached
				break
			}

			// Read table name
			nameBytes := make([]byte, nameLen)
			if _, err := io.ReadFull(r, nameBytes); err != nil {
				// Skip statistics loading on error - table data is valid
				break
			}
			tableName := string(nameBytes)

			// Read statistics data length
			var statsLen uint32
			if err := binary.Read(r, binary.LittleEndian, &statsLen); err != nil {
				break
			}

			if statsLen == 0 {
				// No statistics for this table
				continue
			}

			// Read statistics data
			statsData := make([]byte, statsLen)
			if _, err := io.ReadFull(r, statsData); err != nil {
				// Skip on read error
				continue
			}

			// Deserialize statistics using io.Reader wrapper for bytes
			statsReader := bytes.NewReader(statsData)
			deserializer := optimizer.NewStatsDeserializer(statsReader, int64(statsLen))
			stats, err := deserializer.DeserializeTableStatistics()
			if err != nil {
				// Skip on deserialization error - table data is valid
				continue
			}

			// Store statistics in catalog table definition
			tableDef, ok := e.catalog.GetTableInSchema("main", tableName)
			if ok && stats != nil {
				tableDef.Statistics = stats
			}
		}
	}

	return nil
}

// importDuckDBFormat imports data from a real DuckDB database file.
// This enables opening .duckdb files created by the official DuckDB CLI.
func (e *Engine) importDuckDBFormat(data []byte) error {
	tmpFile, err := os.CreateTemp("", "duckdb-import-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for DuckDB import: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write DuckDB data to temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	file, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to open temp file for reading: %w", err)
	}
	defer file.Close()

	bm := duckdb.NewBlockManager(file, duckdb.DefaultBlockSize, 128)

	dcat, err := duckdb.ReadCatalogFromMetadata(bm, 0)
	if err != nil {
		return fmt.Errorf("failed to read DuckDB catalog from metadata: %w", err)
	}

	cat, err := duckdb.ConvertCatalogFromDuckDB(dcat)
	if err != nil {
		return fmt.Errorf("failed to convert DuckDB catalog to native format: %w", err)
	}

	e.catalog = cat

	return nil
}
