// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/persistence"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// Engine is the core execution engine implementing the Backend interface.
// It manages the catalog, storage, and transaction manager for the database.
type Engine struct {
	mu         sync.RWMutex
	catalog    *catalog.Catalog
	storage    *storage.Storage
	txnMgr     *TransactionManager
	optimizer  *optimizer.CostBasedOptimizer // Cost-based query optimizer
	walWriter  *wal.Writer                   // WAL writer for persistent databases
	config     *dukdb.Config
	path       string
	persistent bool // true if not :memory:
	closed     bool
}

// NewEngine creates a new Engine instance.
func NewEngine() *Engine {
	cat := catalog.NewCatalog()
	return &Engine{
		catalog:   cat,
		storage:   storage.NewStorage(),
		txnMgr:    NewTransactionManager(),
		optimizer: optimizer.NewCostBasedOptimizer(cat),
	}
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

		conn := &EngineConn{
			id:     generateConnID(),
			engine: isolatedEngine,
			txn:    isolatedEngine.txnMgr.Begin(),
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
	}

	// Create a new connection
	conn := &EngineConn{
		id:     generateConnID(),
		engine: e,
		txn:    e.txnMgr.Begin(),
	}

	return conn, nil
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

// TransactionManager manages transactions for the engine.
type TransactionManager struct {
	mu        sync.Mutex
	nextTxnID uint64
	active    map[uint64]*Transaction
}

// NewTransactionManager creates a new TransactionManager.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		nextTxnID: 1,
		active:    make(map[uint64]*Transaction),
	}
}

// Begin starts a new transaction.
func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn := &Transaction{
		id:         tm.nextTxnID,
		active:     true,
		savepoints: NewSavepointStack(),
	}
	tm.nextTxnID++
	tm.active[txn.id] = txn

	return txn
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

	delete(tm.active, txn.id)
	txn.active = false

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

	// Clear savepoints on rollback
	if txn.savepoints != nil {
		txn.savepoints.Clear()
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

	// DDL rollback support: snapshot state at transaction start
	catalogSnapshot *catalog.Catalog
	storageSnapshot *storage.Storage

	// DML rollback support: undo log for INSERT/UPDATE/DELETE operations
	undoLog []UndoOperation
	mu      sync.Mutex // protects undoLog and savepoints

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
func (t *Transaction) RollbackToSavepoint(name string, undoFunc func(op UndoOperation) error) error {
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
//     - Table name length (uint32) + name bytes
//     - Column count (uint16)
//     - Column types (uint8 each)
//     - Row group count (uint32)
//     - For each row group:
//       - Serialized row group data
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
				return nil, fmt.Errorf("failed to export row group %d of table %s: %w", i, name, err)
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

	return buf.Bytes(), nil
}

// importDatabase imports both catalog and storage data from a byte slice.
func (e *Engine) importDatabase(data []byte) error {
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
				return fmt.Errorf("failed to import row group %d of table %s: %w", j, tableName, err)
			}

			// Add to table
			table.AddRowGroup(rg)
		}
	}

	return nil
}