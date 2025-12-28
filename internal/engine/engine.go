// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Engine is the core execution engine implementing the Backend interface.
// It manages the catalog, storage, and transaction manager for the database.
type Engine struct {
	mu      sync.RWMutex
	catalog *catalog.Catalog
	storage *storage.Storage
	txnMgr  *TransactionManager
	config  *dukdb.Config
	path    string
	closed  bool
}

// NewEngine creates a new Engine instance.
func NewEngine() *Engine {
	return &Engine{
		catalog: catalog.NewCatalog(),
		storage: storage.NewStorage(),
		txnMgr:  NewTransactionManager(),
	}
}

// Open opens a connection to the database at the given path.
// Implements the Backend interface.
func (e *Engine) Open(
	path string,
	config *dukdb.Config,
) (dukdb.BackendConn, error) {
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

	// Create a new connection
	conn := &EngineConn{
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
		id:     tm.nextTxnID,
		active: true,
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

	return nil
}

// Transaction represents a database transaction.
type Transaction struct {
	id     uint64
	active bool
}

// ID returns the transaction ID.
func (t *Transaction) ID() uint64 {
	return t.id
}

// IsActive returns whether the transaction is still active.
func (t *Transaction) IsActive() bool {
	return t.active
}

// Verify that Engine implements Backend interface
var _ dukdb.Backend = (*Engine)(nil)

// init registers the Engine as the default backend.
func init() {
	dukdb.RegisterBackend(NewEngine())
}
