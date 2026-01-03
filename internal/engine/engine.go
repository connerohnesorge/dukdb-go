// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"encoding/base64"
	"fmt"
	"os"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/persistence"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Engine is the core execution engine implementing the Backend interface.
// It manages the catalog, storage, and transaction manager for the database.
type Engine struct {
	mu         sync.RWMutex
	catalog    *catalog.Catalog
	storage    *storage.Storage
	txnMgr     *TransactionManager
	config     *dukdb.Config
	path       string
	persistent bool // true if not :memory:
	closed     bool
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
		// Check if file exists and load it
		if _, err := os.Stat(path); err == nil {
			// Detect file format if not explicitly specified
			detectedFormat := e.config.Format
			if detectedFormat == "" || detectedFormat == "duckdb" {
				// Auto-detect format
				format, detectErr := persistence.DetectFileFormat(path)
				if detectErr != nil {
					return nil, fmt.Errorf(
						"failed to detect file format: %w",
						detectErr,
					)
				}

				// Handle detected format
				switch format {
				case persistence.FormatDuckDB:
					// DuckDB format - proceed normally
					detectedFormat = "duckdb"
				case persistence.FormatLegacy:
					// Legacy format detected but not requested
					if e.config.Format == "duckdb" {
						return nil, fmt.Errorf(
							"file is in legacy format, but DuckDB format was requested. "+
							"Use ?format=legacy to open legacy files, or migrate the file first",
						)
					}
					detectedFormat = "legacy"
				case persistence.FormatUnknown:
					return nil, fmt.Errorf(
						"unknown file format: file may be corrupted or not a valid database file",
					)
				}
			}

			// Validate format compatibility
			if e.config.Format == "legacy" && detectedFormat == "duckdb" {
				return nil, fmt.Errorf(
					"file is in DuckDB format, but legacy format was requested",
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

	// Save to file if persistent
	if e.persistent && e.path != "" &&
		e.path != ":memory:" {
		if err := e.saveToFile(e.path); err != nil {
			return fmt.Errorf(
				"failed to save database: %w",
				err,
			)
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

// loadFromFile loads the database from a file
func (e *Engine) loadFromFile(path string) error {
	fm, err := persistence.OpenFile(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := fm.Close(); err != nil {
			// Log the error but don't propagate it from a defer
			// This ensures we don't shadow the original error
		}
	}()

	// Load catalog
	catalogData, err := fm.ReadCatalog()
	if err != nil {
		return fmt.Errorf(
			"failed to read catalog: %w",
			err,
		)
	}

	catalogJSON, err := persistence.UnmarshalCatalog(
		catalogData,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to unmarshal catalog: %w",
			err,
		)
	}

	if err := e.catalog.Import(catalogJSON); err != nil {
		return fmt.Errorf(
			"failed to import catalog: %w",
			err,
		)
	}

	// Load table data from catalog JSON
	if catalogJSON.TableData != nil {
		for tableName, rowGroupsEncoded := range catalogJSON.TableData {
			// Get table definition from catalog
			tableDef, ok := e.catalog.GetTable(tableName)
			if !ok {
				return fmt.Errorf(
					"table %s not found in catalog",
					tableName,
				)
			}

			// Create table in storage
			table, err := e.storage.CreateTable(
				tableName,
				tableDef.ColumnTypes(),
			)
			if err != nil {
				return fmt.Errorf(
					"failed to create table %s: %w",
					tableName,
					err,
				)
			}

			// Decode and import each row group
			for i, encoded := range rowGroupsEncoded {
				data, err := decodeRowGroup(encoded)
				if err != nil {
					return fmt.Errorf(
						"failed to decode row group %d for table %s: %w",
						i,
						tableName,
						err,
					)
				}

				rg, err := table.ImportRowGroup(data)
				if err != nil {
					return fmt.Errorf(
						"failed to import row group %d for table %s: %w",
						i,
						tableName,
						err,
					)
				}
				table.AddRowGroup(rg)
			}
		}
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

	// Export catalog
	catalogJSON := e.catalog.Export()

	// Add table data to catalog JSON
	if catalogJSON.TableData == nil {
		catalogJSON.TableData = make(map[string][]string)
	}

	// Serialize row groups for each table
	for tableName, table := range e.storage.Tables() {
		rowGroups := table.RowGroups()
		encodedGroups := make([]string, 0, len(rowGroups))

		for i, rg := range rowGroups {
			data, err := table.ExportRowGroup(rg)
			if err != nil {
				if err := fm.Close(); err != nil {
					// Log error but don't propagate from cleanup
				}
				_ = os.Remove(tmpPath)

				return fmt.Errorf(
					"failed to export row group %d for table %s: %w",
					i,
					tableName,
					err,
				)
			}

			// Encode to base64
			encoded := encodeRowGroup(data)
			encodedGroups = append(encodedGroups, encoded)
		}

		if len(encodedGroups) > 0 {
			catalogJSON.TableData[tableName] = encodedGroups
		}
	}

	// Marshal catalog with table data
	catalogData, err := persistence.MarshalCatalog(
		catalogJSON,
	)
	if err != nil {
		if err := fm.Close(); err != nil {
			// Log error but don't propagate from cleanup
		}
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"failed to marshal catalog: %w",
			err,
		)
	}

	// Write catalog
	if err := fm.WriteCatalog(catalogData); err != nil {
		if err := fm.Close(); err != nil {
			// Log error but don't propagate from cleanup
		}
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"failed to write catalog: %w",
			err,
		)
	}

	// Finalize with headers
	if err := fm.Finalize(); err != nil {
		if err := fm.Close(); err != nil {
			// Log error but don't propagate from cleanup
		}
		_ = os.Remove(tmpPath)

		return fmt.Errorf(
			"failed to finalize file: %w",
			err,
		)
	}
	if err := fm.Close(); err != nil {
		// Log error but don't propagate from cleanup
	}

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

// encodeRowGroup encodes row group data to base64
func encodeRowGroup(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// decodeRowGroup decodes row group data from base64
func decodeRowGroup(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

// Verify that Engine implements Backend interface
var _ dukdb.Backend = (*Engine)(nil)

// init registers the Engine as the default backend.
func init() {
	dukdb.RegisterBackend(NewEngine())
}
