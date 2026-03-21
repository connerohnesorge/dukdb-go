package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/cache"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/executor"
	"github.com/dukdb/dukdb-go/internal/extension"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// nextConnID is the global atomic counter for generating unique connection IDs.
//
// IDs start at 1 and increment monotonically. The counter is thread-safe
// and uses [sync/atomic.Uint64] operations to ensure uniqueness across
// concurrent connection creations. ID 0 is reserved as an invalid/error
// sentinel value.
//
// The counter is initialized to 1 in [init] to ensure the first call to
// [generateConnID] returns 1 (not 0).
var nextConnID atomic.Uint64

// init initializes the connection ID counter to start at 1.
//
// This ensures:
//   - The first connection gets ID 1 (not 0)
//   - ID 0 remains reserved as an invalid/error sentinel
//   - [generateConnID] can use Add(1)-1 pattern for atomic fetch-and-increment
func init() {
	nextConnID.Store(1)
}

// generateConnID generates a unique connection ID using atomic increment.
//
// Each call returns a new, never-before-used ID. The implementation uses
// [sync/atomic.Uint64.Add] which provides both atomicity and memory ordering
// guarantees for concurrent access.
//
// # ID Assignment
//
// IDs are assigned sequentially starting at 1:
//   - First connection gets ID 1
//   - Second connection gets ID 2
//   - And so on...
//
// ID 0 is reserved as an invalid/error sentinel and is never returned.
//
// # Thread Safety
//
// This function is safe to call concurrently from any number of goroutines.
// Each call is guaranteed to return a unique ID even under heavy contention.
// The atomic increment operation ensures no two connections ever receive
// the same ID.
//
// # Wraparound Behavior
//
// With a uint64 counter, wraparound would only occur after 2^64 connections
// (over 18 quintillion). At 1 million connections per second, this would
// take over 500,000 years. Wraparound is not a practical concern.
//
// # Process Lifetime
//
// IDs are unique within a single process lifetime. After process restart,
// IDs reset to 1. If global ID coordination is required, a higher-level
// mechanism must be used.
func generateConnID() uint64 {
	return nextConnID.Add(1) - 1
}

// EngineConn represents a connection to the engine.
// It implements the BackendConn interface.
type EngineConn struct {
	mu                    sync.Mutex
	id                    uint64 // Unique connection ID, assigned at creation
	engine                *Engine
	txn                   *Transaction
	closed                bool
	inTxn                 bool                  // Whether BEGIN was explicitly called (explicit transaction mode)
	defaultIsolationLevel parser.IsolationLevel // Default isolation level for new transactions
	currentIsolationLevel parser.IsolationLevel // Isolation level of current transaction (if any)

	// Glob settings - initialized from config, can be overridden via SET
	maxFilesPerGlob int // Max files a glob pattern can match (default: 10000)
	fileGlobTimeout int // Timeout for cloud storage glob operations in seconds (default: 60)

	// Settings map for storing session-level settings like checkpoint_threshold
	settings map[string]string

	// sqlPrepared stores SQL-level prepared statements (PREPARE/EXECUTE/DEALLOCATE)
	sqlPrepared map[string]*sqlPreparedStatement
}

// sqlPreparedStatement holds a named SQL-level prepared statement.
type sqlPreparedStatement struct {
	name       string
	query      string           // Original SQL for debugging
	stmt       parser.Statement // Parsed AST
	paramCount int              // Number of $N parameters expected
}

// extensionRegistryAdapter adapts extension.Registry to executor.ExtensionRegistryInterface.
type extensionRegistryAdapter struct {
	registry *extension.Registry
}

func (a *extensionRegistryAdapter) ListExtensions() []executor.ExtensionInfo {
	infos := a.registry.ListExtensions()
	result := make([]executor.ExtensionInfo, len(infos))
	for i, info := range infos {
		result[i] = executor.ExtensionInfo{
			Name:        info.Name,
			Description: info.Description,
			Version:     info.Version,
			Installed:   info.Installed,
			Loaded:      info.Loaded,
		}
	}
	return result
}

// undoRecorderAdapter adapts a Transaction to the executor.UndoRecorder interface.
// This allows the executor to record undo operations without knowing about the engine package.
type undoRecorderAdapter struct {
	txn *Transaction
}

// RecordUndo implements executor.UndoRecorder by converting and forwarding to Transaction.
func (a *undoRecorderAdapter) RecordUndo(op executor.UndoOperation) {
	// Convert executor.UndoOperation to engine.UndoOperation
	engineOp := UndoOperation{
		TableName:   op.TableName,
		OpType:      UndoOpType(op.OpType), // Same values, just different packages
		RowIDs:      op.RowIDs,
		BeforeImage: op.BeforeImage,
	}
	a.txn.RecordUndo(engineOp)
}

// Execute executes a query that doesn't return rows.
func (c *EngineConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, dukdb.ErrConnectionClosed
	}

	// Mark the start of a new statement for READ COMMITTED isolation.
	// This must happen before any reads so that visibility checks use
	// the correct statement timestamp.
	if c.txn != nil {
		c.txn.BeginStatement()
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return 0, err
	}

	// Handle transaction statements at connection level for DDL rollback support
	switch s := stmt.(type) {
	case *parser.BeginStmt:
		return c.handleBeginWithIsolation(s)
	case *parser.CommitStmt:
		return c.handleCommit()
	case *parser.RollbackStmt:
		return c.handleRollback()
	case *parser.SavepointStmt:
		return c.handleSavepoint(s)
	case *parser.RollbackToSavepointStmt:
		return c.handleRollbackToSavepoint(s)
	case *parser.ReleaseSavepointStmt:
		return c.handleReleaseSavepoint(s)
	case *parser.SetStmt:
		return c.handleSet(s)
	case *parser.ResetStmt:
		return c.handleReset(s)
	case *parser.PrepareStmt:
		return c.handlePrepare(s, query)
	case *parser.ExecuteStmt:
		return c.handleExecuteStmt(ctx, s, args)
	case *parser.DeallocateStmt:
		return c.handleDeallocate(s)
	case *parser.ExportDatabaseStmt:
		return c.handleExportDatabase(s)
	case *parser.ImportDatabaseStmt:
		return c.handleImportDatabase(ctx, s)
	case *parser.InstallStmt:
		return c.handleInstall(s)
	case *parser.LoadStmt:
		return c.handleLoad(s)
	case *parser.AttachStmt:
		return c.handleAttach(s)
	case *parser.DetachStmt:
		return c.handleDetach(s)
	case *parser.UseStmt:
		return c.handleUse(s)
	case *parser.CreateDatabaseStmt:
		return c.handleCreateDatabase(s)
	case *parser.DropDatabaseStmt:
		return c.handleDropDatabase(s)
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return 0, err
	}

	// Plan the statement
	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return 0, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	// Set connection for accessing session-level settings
	exec.SetConnection(c)
	// Set extension registry for duckdb_extensions()
	if c.engine.extensions != nil {
		exec.SetExtensionRegistry(&extensionRegistryAdapter{registry: c.engine.extensions})
	}
	// Set FTS registry for full-text search operations
	if c.engine.ftsRegistry != nil {
		exec.SetFTSRegistry(&ftsRegistryAdapter{registry: c.engine.ftsRegistry})
	}
	// Set shared query cache for invalidation
	exec.SetQueryCache(c.engine.QueryCache())
	// Set WAL writer for persistent databases
	if c.engine.WAL() != nil {
		exec.SetWAL(c.engine.WAL())
		if c.txn != nil {
			exec.SetTxnID(c.txn.ID())
		}
	}
	// Set up undo recorder for DML rollback support
	if c.txn != nil && c.inTxn {
		exec.SetUndoRecorder(&undoRecorderAdapter{txn: c.txn})
		exec.SetInTransaction(true)
	}
	// Configure MVCC visibility based on transaction's isolation level
	c.configureExecutorMVCC(exec)

	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected, nil
}

// handleBeginWithIsolation handles the BEGIN statement with optional isolation level,
// capturing catalog and storage snapshots for DDL transaction rollback support
// and enabling DML undo logging.
func (c *EngineConn) handleBeginWithIsolation(stmt *parser.BeginStmt) (int64, error) {
	// Mark that we're in an explicit transaction
	c.inTxn = true

	// Determine the isolation level for this transaction
	// If the statement explicitly specifies an isolation level, use it; otherwise use the default
	var isolationLevel parser.IsolationLevel
	if stmt.HasExplicitIsolation {
		// Explicit isolation level specified in BEGIN statement
		isolationLevel = stmt.IsolationLevel
	} else {
		// Use the connection's default isolation level
		isolationLevel = c.defaultIsolationLevel
	}

	// Store the isolation level on both connection and transaction
	c.currentIsolationLevel = isolationLevel

	// Create a new transaction with the correct isolation level.
	// This ensures the snapshot is properly created (for REPEATABLE READ/SERIALIZABLE)
	// or not created (for READ UNCOMMITTED/READ COMMITTED) based on the isolation level.
	c.txn = c.engine.txnMgr.BeginWithIsolation(isolationLevel)

	// Capture catalog and storage snapshots for DDL rollback
	if c.txn != nil {
		c.txn.catalogSnapshot = c.engine.catalog.Clone()
		c.txn.storageSnapshot = c.engine.storage.Clone()
	}

	return 0, nil
}

// handleCommit handles the COMMIT statement, clearing any snapshots and undo log.
// For SERIALIZABLE isolation, it first checks for conflicts with concurrent transactions.
func (c *EngineConn) handleCommit() (int64, error) {
	if c.txn == nil {
		c.inTxn = false

		return 0, nil
	}

	txnID := c.txn.ID()
	isolationLevel := c.txn.GetIsolationLevel()

	// Use defer to ensure cleanup happens even on error
	defer func() {
		// Release all locks held by this transaction
		if c.engine.LockManager() != nil {
			c.engine.LockManager().Release(txnID)
		}

		// Clear read/write tracking data for this transaction
		if c.engine.ConflictDetector() != nil {
			c.engine.ConflictDetector().ClearTransaction(txnID)
		}
	}()

	// For SERIALIZABLE isolation, check for conflicts before committing
	if isolationLevel == parser.IsolationLevelSerializable {
		// Get list of transactions that committed since this transaction started
		concurrentCommitted := c.engine.txnMgr.GetConcurrentCommitted(txnID)

		// Check for conflicts with those concurrent committed transactions
		if len(concurrentCommitted) > 0 && c.engine.ConflictDetector() != nil {
			if err := c.engine.ConflictDetector().CheckConflicts(txnID, concurrentCommitted); err != nil {
				// Conflict detected - abort the transaction
				// The defer will still clean up locks and tracking data
				c.txn.catalogSnapshot = nil
				c.txn.storageSnapshot = nil
				c.txn.ClearUndoLog()
				c.inTxn = false

				return 0, err
			}
		}
	}

	// No conflicts - clear snapshots and undo log, changes are now permanent
	c.txn.catalogSnapshot = nil
	c.txn.storageSnapshot = nil
	c.txn.ClearUndoLog()

	// Exit explicit transaction mode
	c.inTxn = false

	return 0, nil
}

// handleRollback handles the ROLLBACK statement, restoring from snapshots
// for DDL transaction rollback and executing DML undo operations.
// Also releases all locks and cleans up conflict tracking data.
func (c *EngineConn) handleRollback() (int64, error) {
	if c.txn != nil {
		txnID := c.txn.ID()

		// Use defer to ensure lock and tracking cleanup happens
		defer func() {
			// Release all locks held by this transaction
			if c.engine.LockManager() != nil {
				c.engine.LockManager().Release(txnID)
			}

			// Clear read/write tracking data for this transaction
			if c.engine.ConflictDetector() != nil {
				c.engine.ConflictDetector().ClearTransaction(txnID)
			}
		}()

		// First, execute DML undo operations in reverse order (LIFO)
		undoLog := c.txn.GetUndoLog()
		for i := len(undoLog) - 1; i >= 0; i-- {
			op := undoLog[i]
			table, ok := c.engine.storage.GetTable(op.TableName)
			if !ok {
				// Table might have been dropped, skip this undo operation
				continue
			}

			switch op.OpType {
			case UndoInsert:
				// Undo INSERT: Delete the inserted rows by setting tombstones
				table.HardDeleteRows(op.RowIDs)

			case UndoDelete:
				// Undo DELETE: Clear tombstones (un-delete)
				rowIDs := make([]storage.RowID, len(op.RowIDs))
				for j, id := range op.RowIDs {
					rowIDs[j] = storage.RowID(id)
				}
				table.ClearTombstones(rowIDs)

			case UndoUpdate:
				// Undo UPDATE: Restore before-image values
				_ = table.RestoreRows(op.RowIDs, op.BeforeImage)
			}
		}

		// Clear the undo log
		c.txn.ClearUndoLog()

		// Then, restore DDL snapshots if available
		if c.txn.catalogSnapshot != nil {
			c.engine.catalog.RestoreFrom(c.txn.catalogSnapshot)
			c.txn.catalogSnapshot = nil
		}
		if c.txn.storageSnapshot != nil {
			c.engine.storage.RestoreFrom(c.txn.storageSnapshot)
			c.txn.storageSnapshot = nil
		}
	}

	// Exit explicit transaction mode
	c.inTxn = false

	return 0, nil
}

// handleSavepoint creates a savepoint within the current transaction.
func (c *EngineConn) handleSavepoint(stmt *parser.SavepointStmt) (int64, error) {
	// Must be in a transaction
	if !c.inTxn {
		return 0, errors.New("SAVEPOINT can only be used in transaction block")
	}

	now := time.Now()

	// Get current undo log position before creating savepoint
	undoIndex := len(c.txn.GetUndoLog())

	// Write WAL entry for persistent databases (before creating savepoint)
	if walWriter := c.engine.WAL(); walWriter != nil {
		entry := wal.NewSavepointEntry(c.txn.ID(), stmt.Name, undoIndex, now)
		if err := walWriter.WriteEntry(entry); err != nil {
			return 0, err
		}
	}

	// Create the savepoint using the Transaction method
	if err := c.txn.CreateSavepoint(stmt.Name, now); err != nil {
		return 0, err
	}
	return 0, nil
}

// handleRollbackToSavepoint rolls back to a savepoint.
func (c *EngineConn) handleRollbackToSavepoint(
	stmt *parser.RollbackToSavepointStmt,
) (int64, error) {
	// Must be in a transaction
	if !c.inTxn {
		return 0, errors.New("ROLLBACK TO SAVEPOINT can only be used in transaction block")
	}

	// Get the savepoint's undo index before rolling back
	sp, exists := c.txn.GetSavepoint(stmt.Name)
	if !exists {
		return 0, errors.New("savepoint \"" + stmt.Name + "\" does not exist")
	}
	undoIndex := sp.UndoIndex

	// Write WAL entry for persistent databases (before rollback)
	if walWriter := c.engine.WAL(); walWriter != nil {
		entry := wal.NewRollbackSavepointEntry(c.txn.ID(), stmt.Name, undoIndex, time.Now())
		if err := walWriter.WriteEntry(entry); err != nil {
			return 0, err
		}
	}

	// Roll back using the Transaction method
	// The undo function handles each operation type using the same logic as handleRollback
	err := c.txn.RollbackToSavepoint(stmt.Name, func(op UndoOperation) error {
		table, ok := c.engine.storage.GetTable(op.TableName)
		if !ok {
			// Table might have been dropped, skip this undo operation
			return nil
		}

		switch op.OpType {
		case UndoInsert:
			// Undo INSERT: Delete the inserted rows by setting tombstones
			table.HardDeleteRows(op.RowIDs)

		case UndoDelete:
			// Undo DELETE: Clear tombstones (un-delete)
			rowIDs := make([]storage.RowID, len(op.RowIDs))
			for j, id := range op.RowIDs {
				rowIDs[j] = storage.RowID(id)
			}
			table.ClearTombstones(rowIDs)

		case UndoUpdate:
			// Undo UPDATE: Restore before-image values
			_ = table.RestoreRows(op.RowIDs, op.BeforeImage)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// handleReleaseSavepoint releases a savepoint.
func (c *EngineConn) handleReleaseSavepoint(stmt *parser.ReleaseSavepointStmt) (int64, error) {
	// Must be in a transaction
	if !c.inTxn {
		return 0, errors.New("RELEASE SAVEPOINT can only be used in transaction block")
	}

	// Write WAL entry for persistent databases (before release)
	if walWriter := c.engine.WAL(); walWriter != nil {
		entry := wal.NewReleaseSavepointEntry(c.txn.ID(), stmt.Name, time.Now())
		if err := walWriter.WriteEntry(entry); err != nil {
			return 0, err
		}
	}

	if err := c.txn.ReleaseSavepoint(stmt.Name); err != nil {
		return 0, err
	}
	return 0, nil
}

// handleSet handles the SET statement for session configuration.
// Supports:
//   - SET default_transaction_isolation = 'level'
//   - SET transaction_isolation = 'level' (synonym)
func (c *EngineConn) handleSet(stmt *parser.SetStmt) (int64, error) {
	switch stmt.Variable {
	case "default_transaction_isolation", "transaction_isolation":
		// Parse the isolation level from the value
		level, err := parseIsolationLevelString(stmt.Value)
		if err != nil {
			return 0, err
		}
		c.defaultIsolationLevel = level
		return 0, nil
	case "max_files_per_glob":
		// Parse the max files per glob setting
		maxFiles, err := parsePositiveInt(stmt.Value, "max_files_per_glob", 1, 1000000)
		if err != nil {
			return 0, err
		}
		c.maxFilesPerGlob = maxFiles
		return 0, nil
	case "file_glob_timeout":
		// Parse the file glob timeout setting
		timeout, err := parsePositiveInt(stmt.Value, "file_glob_timeout", 1, 600)
		if err != nil {
			return 0, err
		}
		c.fileGlobTimeout = timeout
		return 0, nil
	default:
		// Unknown variable - for now we silently accept it
		// In a full implementation, we might store these in a config map
		return 0, nil
	}
}

// handleReset handles the RESET statement by resetting variables to defaults.
func (c *EngineConn) handleReset(stmt *parser.ResetStmt) (int64, error) {
	if stmt.All {
		// Reset all settings to defaults
		c.defaultIsolationLevel = parser.IsolationLevelSerializable
		c.maxFilesPerGlob = dukdb.DefaultMaxFilesPerGlob
		c.fileGlobTimeout = dukdb.DefaultFileGlobTimeout
		return 0, nil
	}

	switch stmt.Variable {
	case "default_transaction_isolation", "transaction_isolation":
		c.defaultIsolationLevel = parser.IsolationLevelSerializable
	case "max_files_per_glob":
		c.maxFilesPerGlob = dukdb.DefaultMaxFilesPerGlob
	case "file_glob_timeout":
		c.fileGlobTimeout = dukdb.DefaultFileGlobTimeout
	default:
		// Silently accept unknown variables (matching SET behavior)
		return 0, nil
	}
	return 0, nil
}

// parseIsolationLevelString converts a string to an IsolationLevel.
// Accepts case-insensitive values like:
//   - 'READ UNCOMMITTED' or 'read uncommitted'
//   - 'READ COMMITTED' or 'read committed'
//   - 'REPEATABLE READ' or 'repeatable read'
//   - 'SERIALIZABLE' or 'serializable'
func parseIsolationLevelString(s string) (parser.IsolationLevel, error) {
	// Normalize to uppercase for comparison
	upper := ""
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c = c - 32 // Convert to uppercase
		}
		upper += string(c)
	}

	switch upper {
	case "READ UNCOMMITTED":
		return parser.IsolationLevelReadUncommitted, nil
	case "READ COMMITTED":
		return parser.IsolationLevelReadCommitted, nil
	case "REPEATABLE READ":
		return parser.IsolationLevelRepeatableRead, nil
	case "SERIALIZABLE":
		return parser.IsolationLevelSerializable, nil
	default:
		return parser.IsolationLevelSerializable, errors.New("invalid isolation level: " + s)
	}
}

// parsePositiveInt parses a positive integer from a string.
// Returns an error if the value is not a valid positive integer or outside the specified range.
func parsePositiveInt(s string, name string, minVal, maxVal int) (int, error) {
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %s (must be a positive integer)", name, s)
	}
	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("%s must be between %d and %d, got %d", name, minVal, maxVal, val)
	}
	return val, nil
}

// handleShow handles the SHOW statement for session configuration.
// Returns a single row with the value of the requested variable.
func (c *EngineConn) handleShow(stmt *parser.ShowStmt) ([]map[string]any, []string, error) {
	// Handle SHOW TABLES
	if stmt.Variable == "__tables" {
		return c.handleShowTables()
	}

	// Handle SHOW ALL TABLES
	if stmt.Variable == "__all_tables" {
		return c.handleShowAllTables()
	}

	// Handle SHOW COLUMNS FROM table
	if stmt.Variable == "__columns" {
		return c.describeTable(stmt.TableName, "", describeColumns())
	}

	var value string

	switch stmt.Variable {
	case "transaction_isolation":
		// Return the current transaction's isolation level if in a transaction,
		// otherwise return the default isolation level
		if c.inTxn {
			value = c.currentIsolationLevel.String()
		} else {
			value = c.defaultIsolationLevel.String()
		}
	case "default_transaction_isolation":
		// Return the default isolation level
		value = c.defaultIsolationLevel.String()
	case "max_files_per_glob":
		// Return the max files per glob setting
		value = strconv.Itoa(c.maxFilesPerGlob)
	case "file_glob_timeout":
		// Return the file glob timeout setting
		value = strconv.Itoa(c.fileGlobTimeout)
	default:
		// Unknown variable - return empty
		value = ""
	}

	// Return a single row with the variable name as column name
	rows := []map[string]any{
		{stmt.Variable: value},
	}
	columns := []string{stmt.Variable}

	return rows, columns, nil
}

// describeColumns returns the standard column names for DESCRIBE output.
func describeColumns() []string {
	return []string{"column_name", "column_type", "null", "key", "default", "extra"}
}

// handleDescribe handles the DESCRIBE statement.
func (c *EngineConn) handleDescribe(stmt *parser.DescribeStmt) ([]map[string]any, []string, error) {
	columns := describeColumns()

	if stmt.Query != nil {
		return c.describeQuery(stmt.Query, columns)
	}

	return c.describeTable(stmt.TableName, stmt.Schema, columns)
}

// describeTable returns column metadata for a table.
func (c *EngineConn) describeTable(tableName, schemaName string, columns []string) ([]map[string]any, []string, error) {
	if schemaName == "" {
		schemaName = "main"
	}

	tableDef, ok := c.engine.catalog.GetTableInSchema(schemaName, tableName)
	if !ok || tableDef == nil {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Table with name %s does not exist!", tableName),
		}
	}

	rows := make([]map[string]any, 0, len(tableDef.Columns))
	for i, col := range tableDef.Columns {
		isPK := "NO"
		for _, pkIdx := range tableDef.PrimaryKey {
			if pkIdx == i {
				isPK = "YES"
				break
			}
		}

		nullStr := "YES"
		if !col.Nullable {
			nullStr = "NO"
		}

		var defaultVal any
		if col.HasDefault {
			defaultVal = fmt.Sprintf("%v", col.DefaultValue)
		}

		rows = append(rows, map[string]any{
			"column_name": col.Name,
			"column_type": col.Type.String(),
			"null":        nullStr,
			"key":         isPK,
			"default":     defaultVal,
			"extra":       "",
		})
	}

	return rows, columns, nil
}

// describeQuery returns column metadata for a query by binding it and extracting
// output column names and types without executing the query.
func (c *EngineConn) describeQuery(query parser.Statement, columns []string) ([]map[string]any, []string, error) {
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(query)
	if err != nil {
		return nil, nil, err
	}

	boundSelect, ok := boundStmt.(*binder.BoundSelectStmt)
	if !ok {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "DESCRIBE query must be a SELECT statement",
		}
	}

	rows := make([]map[string]any, 0, len(boundSelect.Columns))
	for _, col := range boundSelect.Columns {
		colName := col.Alias
		if colName == "" {
			colName = fmt.Sprintf("col%d", len(rows))
		}

		colType := "VARCHAR"
		if col.Expr != nil {
			rt := col.Expr.ResultType()
			if rt != dukdb.TYPE_INVALID {
				colType = rt.String()
			}
		}

		rows = append(rows, map[string]any{
			"column_name": colName,
			"column_type": colType,
			"null":        "YES",
			"key":         "NO",
			"default":     nil,
			"extra":       "",
		})
	}

	return rows, columns, nil
}

// handleSummarize handles the SUMMARIZE statement by computing per-column statistics.
func (c *EngineConn) handleSummarize(stmt *parser.SummarizeStmt) ([]map[string]any, []string, error) {
	if stmt.Query != nil {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "SUMMARIZE SELECT is not yet supported; use SUMMARIZE table_name",
		}
	}

	schemaName := stmt.Schema
	if schemaName == "" {
		schemaName = "main"
	}

	tableDef, ok := c.engine.catalog.GetTableInSchema(schemaName, stmt.TableName)
	if !ok || tableDef == nil {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Table with name %s does not exist!", stmt.TableName),
		}
	}

	// Build and execute SELECT * FROM table
	var querySQL string
	if stmt.Schema != "" {
		querySQL = fmt.Sprintf("SELECT * FROM %s.%s", stmt.Schema, stmt.TableName)
	} else {
		querySQL = fmt.Sprintf("SELECT * FROM %s", stmt.TableName)
	}

	innerStmt, err := parser.Parse(querySQL)
	if err != nil {
		return nil, nil, err
	}

	dataRows, dataCols, err := c.queryInnerStmt(context.Background(), innerStmt, nil)
	if err != nil {
		return nil, nil, err
	}

	// Build per-column statistics
	columns := []string{"column_name", "column_type", "min", "max", "unique_count", "null_count", "avg", "std", "count"}
	totalCount := len(dataRows)

	resultRows := make([]map[string]any, 0, len(dataCols))
	for colIdx, colName := range dataCols {
		colType := "VARCHAR"
		if colIdx < len(tableDef.Columns) {
			colType = tableDef.Columns[colIdx].Type.String()
		}

		var (
			minVal      any
			maxVal      any
			nullCount   int
			uniqueSet   = make(map[any]struct{})
			sum         float64
			sumSq       float64
			numericVals int
			isNumeric   bool
		)

		// Check if column type is numeric
		if colIdx < len(tableDef.Columns) {
			ct := tableDef.Columns[colIdx].Type
			switch ct {
			case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT,
				dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT,
				dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE, dukdb.TYPE_HUGEINT, dukdb.TYPE_DECIMAL:
				isNumeric = true
			}
		}

		for _, row := range dataRows {
			val := row[colName]
			if val == nil {
				nullCount++
				continue
			}

			uniqueSet[val] = struct{}{}

			// Track min/max using string comparison for non-numeric, numeric for numeric
			valStr := fmt.Sprintf("%v", val)
			if minVal == nil {
				minVal = valStr
				maxVal = valStr
			} else {
				minStr := fmt.Sprintf("%v", minVal)
				maxStr := fmt.Sprintf("%v", maxVal)
				if valStr < minStr {
					minVal = valStr
				}
				if valStr > maxStr {
					maxVal = valStr
				}
			}

			if isNumeric {
				fVal := toFloat64(val)
				if fVal != nil {
					sum += *fVal
					sumSq += (*fVal) * (*fVal)
					numericVals++
				}
			}
		}

		row := map[string]any{
			"column_name":  colName,
			"column_type":  colType,
			"min":          minVal,
			"max":          maxVal,
			"unique_count": int64(len(uniqueSet)),
			"null_count":   int64(nullCount),
			"avg":          nil,
			"std":          nil,
			"count":        int64(totalCount),
		}

		if isNumeric && numericVals > 0 {
			avg := sum / float64(numericVals)
			row["avg"] = avg
			if numericVals > 1 {
				variance := (sumSq / float64(numericVals)) - (avg * avg)
				if variance < 0 {
					variance = 0
				}
				row["std"] = math.Sqrt(variance)
			} else {
				row["std"] = 0.0
			}
		}

		resultRows = append(resultRows, row)
	}

	return resultRows, columns, nil
}

// toFloat64 converts a value to float64 for statistical computation.
func toFloat64(val any) *float64 {
	var f float64
	switch v := val.(type) {
	case int:
		f = float64(v)
	case int8:
		f = float64(v)
	case int16:
		f = float64(v)
	case int32:
		f = float64(v)
	case int64:
		f = float64(v)
	case uint8:
		f = float64(v)
	case uint16:
		f = float64(v)
	case uint32:
		f = float64(v)
	case uint64:
		f = float64(v)
	case float32:
		f = float64(v)
	case float64:
		f = v
	default:
		return nil
	}
	return &f
}

// handleCall handles the CALL statement by converting it to a SELECT * FROM function(args).
func (c *EngineConn) handleCall(stmt *parser.CallStmt) ([]map[string]any, []string, error) {
	// Serialize arguments to SQL
	argParts := make([]string, 0, len(stmt.Args))
	for _, arg := range stmt.Args {
		argParts = append(argParts, exprToSQL(arg))
	}

	argsStr := strings.Join(argParts, ", ")
	querySQL := fmt.Sprintf("SELECT * FROM %s(%s)", stmt.FunctionName, argsStr)

	innerStmt, err := parser.Parse(querySQL)
	if err != nil {
		return nil, nil, err
	}

	return c.queryInnerStmt(context.Background(), innerStmt, nil)
}

// exprToSQL converts a parser Expr to its SQL string representation.
// Handles common expression types used in CALL arguments.
func exprToSQL(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.Literal:
		if e.Value == nil {
			return "NULL"
		}
		switch e.Type {
		case dukdb.TYPE_VARCHAR:
			return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", e.Value), "'", "''"))
		default:
			return fmt.Sprintf("%v", e.Value)
		}
	case *parser.ColumnRef:
		if e.Table != "" {
			return fmt.Sprintf("%s.%s", e.Table, e.Column)
		}
		return e.Column
	case *parser.FunctionCall:
		args := make([]string, 0, len(e.Args))
		for _, arg := range e.Args {
			args = append(args, exprToSQL(arg))
		}
		if e.Star {
			return fmt.Sprintf("%s(*)", e.Name)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	case *parser.UnaryExpr:
		switch e.Op {
		case parser.OpNeg:
			return fmt.Sprintf("-%s", exprToSQL(e.Expr))
		case parser.OpNot:
			return fmt.Sprintf("NOT %s", exprToSQL(e.Expr))
		default:
			return exprToSQL(e.Expr)
		}
	case *parser.BinaryExpr:
		left := exprToSQL(e.Left)
		right := exprToSQL(e.Right)
		op := "+"
		switch e.Op {
		case parser.OpAdd:
			op = "+"
		case parser.OpSub:
			op = "-"
		case parser.OpMul:
			op = "*"
		case parser.OpDiv:
			op = "/"
		case parser.OpMod:
			op = "%"
		case parser.OpEq:
			op = "="
		case parser.OpNe:
			op = "!="
		case parser.OpLt:
			op = "<"
		case parser.OpLe:
			op = "<="
		case parser.OpGt:
			op = ">"
		case parser.OpGe:
			op = ">="
		case parser.OpAnd:
			op = "AND"
		case parser.OpOr:
			op = "OR"
		case parser.OpConcat:
			op = "||"
		}
		return fmt.Sprintf("(%s %s %s)", left, op, right)
	case *parser.CastExpr:
		return fmt.Sprintf("CAST(%s AS %s)", exprToSQL(e.Expr), e.TargetType.String())
	default:
		return fmt.Sprintf("%v", expr)
	}
}

// handleShowTables returns all tables in the default schema.
func (c *EngineConn) handleShowTables() ([]map[string]any, []string, error) {
	tables := c.engine.catalog.ListTables()
	columns := []string{"name"}

	rows := make([]map[string]any, 0, len(tables))
	for _, t := range tables {
		rows = append(rows, map[string]any{
			"name": t.Name,
		})
	}

	return rows, columns, nil
}

// handleShowAllTables returns all tables across all schemas.
func (c *EngineConn) handleShowAllTables() ([]map[string]any, []string, error) {
	schemas := c.engine.catalog.ListSchemas()
	columns := []string{"database", "schema", "name", "column_names", "column_types", "temporary"}

	var rows []map[string]any
	for _, s := range schemas {
		tables := c.engine.catalog.ListTablesInSchema(s.Name())
		for _, t := range tables {
			colNames := make([]string, len(t.Columns))
			colTypes := make([]string, len(t.Columns))
			for i, col := range t.Columns {
				colNames[i] = col.Name
				colTypes[i] = col.Type.String()
			}
			rows = append(rows, map[string]any{
				"database":     "memory",
				"schema":       s.Name(),
				"name":         t.Name,
				"column_names": colNames,
				"column_types": colTypes,
				"temporary":    false,
			})
		}
	}

	return rows, columns, nil
}

// handlePrepare stores a named prepared statement on the connection.
func (c *EngineConn) handlePrepare(stmt *parser.PrepareStmt, query string) (int64, error) {
	name := strings.ToLower(stmt.Name)
	if _, exists := c.sqlPrepared[name]; exists {
		return 0, fmt.Errorf("prepared statement %q already exists", stmt.Name)
	}

	// Count parameters in the inner statement
	paramCount := parser.CountParameters(stmt.Inner)

	c.sqlPrepared[name] = &sqlPreparedStatement{
		name:       name,
		query:      query,
		stmt:       stmt.Inner,
		paramCount: paramCount,
	}
	return 0, nil
}

// handleExecuteStmt executes a named prepared statement (for Exec path).
func (c *EngineConn) handleExecuteStmt(ctx context.Context, stmt *parser.ExecuteStmt, args []driver.NamedValue) (int64, error) {
	name := strings.ToLower(stmt.Name)
	prep, ok := c.sqlPrepared[name]
	if !ok {
		return 0, fmt.Errorf("prepared statement %q does not exist", stmt.Name)
	}

	// Build parameter values by evaluating the EXECUTE parameter expressions
	execArgs, err := c.buildExecArgs(stmt.Params, prep.paramCount)
	if err != nil {
		return 0, err
	}

	// Re-bind, re-plan, and execute the inner statement with the parameter values
	return c.executeInnerStmt(ctx, prep.stmt, execArgs)
}

// handleExecuteStmtQuery executes a named prepared statement (for Query path).
func (c *EngineConn) handleExecuteStmtQuery(ctx context.Context, stmt *parser.ExecuteStmt, args []driver.NamedValue) ([]map[string]any, []string, error) {
	name := strings.ToLower(stmt.Name)
	prep, ok := c.sqlPrepared[name]
	if !ok {
		return nil, nil, fmt.Errorf("prepared statement %q does not exist", stmt.Name)
	}

	execArgs, err := c.buildExecArgs(stmt.Params, prep.paramCount)
	if err != nil {
		return nil, nil, err
	}

	return c.queryInnerStmt(ctx, prep.stmt, execArgs)
}

// handleDeallocate removes a named prepared statement or all prepared statements.
func (c *EngineConn) handleDeallocate(stmt *parser.DeallocateStmt) (int64, error) {
	if stmt.All {
		c.sqlPrepared = make(map[string]*sqlPreparedStatement)
		return 0, nil
	}

	name := strings.ToLower(stmt.Name)
	if _, ok := c.sqlPrepared[name]; !ok {
		return 0, fmt.Errorf("prepared statement %q does not exist", stmt.Name)
	}
	delete(c.sqlPrepared, name)
	return 0, nil
}

// handleInstall handles the INSTALL extension_name statement.
// INSTALL is a no-op for compiled-in extensions.
func (c *EngineConn) handleInstall(stmt *parser.InstallStmt) (int64, error) {
	if c.engine.extensions != nil {
		return 0, c.engine.extensions.Install(stmt.Name)
	}
	return 0, nil
}

// handleLoad handles the LOAD extension_name statement.
// LOAD activates a registered extension.
func (c *EngineConn) handleLoad(stmt *parser.LoadStmt) (int64, error) {
	if c.engine.extensions != nil {
		return 0, c.engine.extensions.Load(stmt.Name)
	}
	return 0, nil
}

// handleAttach handles the ATTACH statement by registering a new database.
// For now, all attached databases are created as in-memory databases.
// File-based attachment is future work.
func (c *EngineConn) handleAttach(stmt *parser.AttachStmt) (int64, error) {
	alias := stmt.Alias
	if alias == "" {
		// Derive alias from path
		alias = stmt.Path
		// Strip directory and extension
		if idx := strings.LastIndexByte(alias, '/'); idx >= 0 {
			alias = alias[idx+1:]
		}
		if idx := strings.LastIndexByte(alias, '\\'); idx >= 0 {
			alias = alias[idx+1:]
		}
		if idx := strings.LastIndexByte(alias, '.'); idx >= 0 {
			alias = alias[:idx]
		}
		if alias == "" || alias == ":memory:" {
			alias = "db"
		}
	}
	// Create a new in-memory catalog and storage for the attached database
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	return 0, c.engine.dbManager.Attach(alias, stmt.Path, stmt.ReadOnly, cat, stor)
}

// handleDetach handles the DETACH statement by removing an attached database.
func (c *EngineConn) handleDetach(stmt *parser.DetachStmt) (int64, error) {
	return 0, c.engine.dbManager.Detach(stmt.Name, stmt.IfExists)
}

// handleUse handles the USE statement by setting the default database.
func (c *EngineConn) handleUse(stmt *parser.UseStmt) (int64, error) {
	return 0, c.engine.dbManager.Use(stmt.Database)
}

// handleCreateDatabase handles the CREATE DATABASE statement.
func (c *EngineConn) handleCreateDatabase(stmt *parser.CreateDatabaseStmt) (int64, error) {
	return 0, c.engine.dbManager.CreateDatabase(stmt.Name, stmt.IfNotExists)
}

// handleDropDatabase handles the DROP DATABASE statement.
func (c *EngineConn) handleDropDatabase(stmt *parser.DropDatabaseStmt) (int64, error) {
	return 0, c.engine.dbManager.DropDatabase(stmt.Name, stmt.IfExists)
}

// buildExecArgs evaluates EXECUTE parameter expressions into driver.NamedValue args.
func (c *EngineConn) buildExecArgs(params []parser.Expr, expectedCount int) ([]driver.NamedValue, error) {
	if len(params) != expectedCount {
		return nil, fmt.Errorf("expected %d parameters, got %d", expectedCount, len(params))
	}

	execArgs := make([]driver.NamedValue, len(params))
	for i, paramExpr := range params {
		val, err := c.evaluateSimpleExpr(paramExpr)
		if err != nil {
			return nil, fmt.Errorf("error evaluating parameter %d: %v", i+1, err)
		}
		execArgs[i] = driver.NamedValue{Ordinal: i + 1, Value: val}
	}
	return execArgs, nil
}

// executeInnerStmt binds, plans, and executes a statement (Exec path).
func (c *EngineConn) executeInnerStmt(ctx context.Context, stmt parser.Statement, args []driver.NamedValue) (int64, error) {
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return 0, err
	}

	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return 0, err
	}

	exec := executor.NewExecutor(c.engine.catalog, c.engine.storage)
	exec.SetConnection(c)
	if c.engine.extensions != nil {
		exec.SetExtensionRegistry(&extensionRegistryAdapter{registry: c.engine.extensions})
	}
	// Set FTS registry for full-text search operations
	if c.engine.ftsRegistry != nil {
		exec.SetFTSRegistry(&ftsRegistryAdapter{registry: c.engine.ftsRegistry})
	}
	exec.SetQueryCache(c.engine.QueryCache())
	if c.engine.WAL() != nil {
		exec.SetWAL(c.engine.WAL())
		if c.txn != nil {
			exec.SetTxnID(c.txn.ID())
		}
	}
	if c.txn != nil && c.inTxn {
		exec.SetUndoRecorder(&undoRecorderAdapter{txn: c.txn})
		exec.SetInTransaction(true)
	}
	c.configureExecutorMVCC(exec)

	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected, nil
}

// queryInnerStmt binds, plans, and executes a statement (Query path).
func (c *EngineConn) queryInnerStmt(ctx context.Context, stmt parser.Statement, args []driver.NamedValue) ([]map[string]any, []string, error) {
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, nil, err
	}

	p := planner.NewPlanner(c.engine.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	exec := executor.NewExecutor(c.engine.catalog, c.engine.storage)
	exec.SetConnection(c)
	if c.engine.extensions != nil {
		exec.SetExtensionRegistry(&extensionRegistryAdapter{registry: c.engine.extensions})
	}
	// Set FTS registry for full-text search operations
	if c.engine.ftsRegistry != nil {
		exec.SetFTSRegistry(&ftsRegistryAdapter{registry: c.engine.ftsRegistry})
	}
	exec.SetQueryCache(c.engine.QueryCache())
	if c.engine.WAL() != nil {
		exec.SetWAL(c.engine.WAL())
		if c.txn != nil {
			exec.SetTxnID(c.txn.ID())
		}
	}
	if c.txn != nil && c.inTxn {
		exec.SetUndoRecorder(&undoRecorderAdapter{txn: c.txn})
		exec.SetInTransaction(true)
	}
	c.configureExecutorMVCC(exec)

	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return nil, nil, err
	}
	return result.Rows, result.Columns, nil
}

// evaluateSimpleExpr evaluates a simple expression (literal, negative literal) for EXECUTE parameters.
func (c *EngineConn) evaluateSimpleExpr(expr parser.Expr) (any, error) {
	switch e := expr.(type) {
	case *parser.Literal:
		return e.Value, nil
	case *parser.UnaryExpr:
		if e.Op == parser.OpNeg {
			val, err := c.evaluateSimpleExpr(e.Expr)
			if err != nil {
				return nil, err
			}
			switch v := val.(type) {
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			default:
				return nil, fmt.Errorf("cannot negate value of type %T", val)
			}
		}
		return nil, fmt.Errorf("unsupported unary operator in EXECUTE parameter")
	default:
		return nil, fmt.Errorf("unsupported expression type in EXECUTE parameter: %T", expr)
	}
}

// configureExecutorMVCC configures the executor with MVCC visibility settings
// based on the current transaction's isolation level.
//
// This method sets up:
//   - VisibilityChecker: Determines which row versions are visible during reads
//   - TransactionContext: Provides transaction state for visibility checks
//   - ConflictDetector: Tracks read/write sets for SERIALIZABLE isolation
//   - LockManager: Manages row locks for SERIALIZABLE write operations
//
// The visibility checker and transaction context are only set when there is
// an active transaction. For auto-commit (single statement) mode, MVCC is not
// enabled since there are no concurrent visibility concerns.
func (c *EngineConn) configureExecutorMVCC(exec *executor.Executor) {
	// Only configure MVCC if we have an active transaction
	if c.txn == nil {
		return
	}

	// Get the appropriate visibility checker for the transaction's isolation level
	isolationLevel := c.txn.GetIsolationLevel()
	visibility := storage.GetVisibilityChecker(isolationLevel)
	exec.SetVisibility(visibility)

	// Create a transaction context adapter for visibility checks
	txnCtx := NewTransactionContextAdapter(c.txn, c.engine.txnMgr)
	exec.SetTransactionContext(txnCtx)

	// For SERIALIZABLE isolation, set up conflict detection and locking
	// These are only needed for the strictest isolation level
	if isolationLevel == parser.IsolationLevelSerializable {
		// Use the engine's shared conflict detector and lock manager
		// This allows conflict detection across all connections
		exec.SetConflictDetector(c.engine.ConflictDetector())
		exec.SetLockManager(c.engine.LockManager())
	}
}

// MaxFilesPerGlob returns the maximum number of files that a glob pattern can match.
func (c *EngineConn) MaxFilesPerGlob() int {
	return c.maxFilesPerGlob
}

// FileGlobTimeout returns the timeout for cloud storage glob operations in seconds.
func (c *EngineConn) FileGlobTimeout() int {
	return c.fileGlobTimeout
}

// SetSetting stores a session-level setting value.
// Note: This method is called from the executor package while the connection
// mutex (c.mu) is already held, and during initialization before concurrent
// access, so it must not acquire the mutex.
func (c *EngineConn) SetSetting(key string, value string) {
	if c.settings == nil {
		c.settings = make(map[string]string)
	}
	c.settings[key] = value
}

// GetSetting retrieves a session-level setting value, returning empty string if not set.
// Note: This method is called from the executor and metadata packages while the
// connection mutex (c.mu) is already held, so it must not acquire it again.
func (c *EngineConn) GetSetting(key string) string {
	if c.settings == nil {
		return ""
	}
	return c.settings[key]
}

// Query executes a query that returns rows.
func (c *EngineConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Mark the start of a new statement for READ COMMITTED isolation.
	// This must happen before any reads so that visibility checks use
	// the correct statement timestamp.
	if c.txn != nil {
		c.txn.BeginStatement()
	}

	queryCache := c.engine.QueryCache()
	if queryCache != nil {
		c.cacheConfigChanged(queryCache)
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	// Handle SHOW statements at connection level
	if showStmt, ok := stmt.(*parser.ShowStmt); ok {
		return c.handleShow(showStmt)
	}

	// Handle DESCRIBE statements at connection level
	if describeStmt, ok := stmt.(*parser.DescribeStmt); ok {
		return c.handleDescribe(describeStmt)
	}

	// Handle SUMMARIZE statements at connection level
	if summarizeStmt, ok := stmt.(*parser.SummarizeStmt); ok {
		return c.handleSummarize(summarizeStmt)
	}

	// Handle CALL statements at connection level
	if callStmt, ok := stmt.(*parser.CallStmt); ok {
		return c.handleCall(callStmt)
	}

	// Handle extension and database management statements
	switch s := stmt.(type) {
	case *parser.InstallStmt:
		if _, err := c.handleInstall(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.LoadStmt:
		if _, err := c.handleLoad(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.AttachStmt:
		if _, err := c.handleAttach(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.DetachStmt:
		if _, err := c.handleDetach(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.UseStmt:
		if _, err := c.handleUse(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.CreateDatabaseStmt:
		if _, err := c.handleCreateDatabase(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.DropDatabaseStmt:
		if _, err := c.handleDropDatabase(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	default:
		// not a special statement, continue
	}

	// Handle SQL-level prepared statement operations
	switch s := stmt.(type) {
	case *parser.PrepareStmt:
		if _, err := c.handlePrepare(s, query); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	case *parser.ExecuteStmt:
		return c.handleExecuteStmtQuery(ctx, s, args)
	case *parser.DeallocateStmt:
		if _, err := c.handleDeallocate(s); err != nil {
			return nil, nil, err
		}
		return []map[string]any{}, []string{}, nil
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, nil, err
	}

	cacheKey := ""
	cacheable := false
	if queryCache != nil && c.cacheEnabled() && !c.inTxn {
		if selectStmt, isSelect := boundStmt.(*binder.BoundSelectStmt); isSelect {
			if isCacheableSelect(selectStmt) {
				key, keyErr := cacheKeyForQuery(query, args, c.cacheParameterMode())
				if keyErr == nil {
					cacheKey = key
					cacheable = true
					if cached, ok := queryCache.Get(cacheKey); ok {
						return cached.Rows, cached.Columns, nil
					}
				}
			}
		}
	}

	// Plan the statement with optional optimization hints
	p := planner.NewPlanner(c.engine.catalog)

	// For SELECT statements, run the cost-based optimizer if enabled
	if _, isSelect := boundStmt.(*binder.BoundSelectStmt); isSelect {
		hints := c.getOptimizationHints(boundStmt)
		if hints != nil {
			p.SetHints(hints)
		}
	}

	// For EXPLAIN statements, run the cost-based optimizer on the child query
	// so that EXPLAIN output shows IndexScan when an index would be used.
	if explainStmt, isExplain := boundStmt.(*binder.BoundExplainStmt); isExplain {
		if _, isSelectChild := explainStmt.Query.(*binder.BoundSelectStmt); isSelectChild {
			hints := c.getOptimizationHints(explainStmt.Query)
			if hints != nil {
				p.SetHints(hints)
			}
		}
	}

	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	// Set connection for accessing session-level settings
	exec.SetConnection(c)
	// Set extension registry for duckdb_extensions()
	if c.engine.extensions != nil {
		exec.SetExtensionRegistry(&extensionRegistryAdapter{registry: c.engine.extensions})
	}
	// Set FTS registry for full-text search operations
	if c.engine.ftsRegistry != nil {
		exec.SetFTSRegistry(&ftsRegistryAdapter{registry: c.engine.ftsRegistry})
	}
	// Set shared query cache for invalidation
	exec.SetQueryCache(queryCache)
	// Set WAL writer for persistent databases
	if c.engine.WAL() != nil {
		exec.SetWAL(c.engine.WAL())
		if c.txn != nil {
			exec.SetTxnID(c.txn.ID())
		}
	}
	// Set up undo recorder for DML rollback support
	if c.txn != nil && c.inTxn {
		exec.SetUndoRecorder(&undoRecorderAdapter{txn: c.txn})
		exec.SetInTransaction(true)
	}
	// Configure MVCC visibility based on transaction's isolation level
	c.configureExecutorMVCC(exec)

	result, err := exec.Execute(ctx, plan, args)
	if err != nil {
		return nil, nil, err
	}

	if cacheable && queryCache != nil {
		tables := collectPlanTables(plan)
		queryCache.Put(cacheKey, cache.QueryResult{
			Rows:         result.Rows,
			Columns:      result.Columns,
			RowsAffected: result.RowsAffected,
		}, 0, 0, tables)
	}

	return result.Rows, result.Columns, nil
}

// getOptimizationHints runs the cost-based optimizer on a bound statement
// and returns planner hints derived from the optimized plan.
// Returns nil if optimization is disabled or fails.
func (c *EngineConn) getOptimizationHints(
	boundStmt binder.BoundStatement,
) *planner.OptimizationHints {
	opt := c.engine.Optimizer()
	if opt == nil || !opt.IsEnabled() {
		return nil
	}

	// We need to convert the bound statement to a logical plan node that
	// implements optimizer.LogicalPlanNode. The optimizer works on logical plans,
	// not bound statements directly. For now, we create a wrapper.
	logicalPlan := createLogicalPlanAdapter(boundStmt)
	if logicalPlan == nil {
		return nil
	}

	// Run the optimizer
	optimizedPlan, err := opt.Optimize(logicalPlan)
	if err != nil {
		// Optimization failed, continue without hints
		return nil
	}

	// Convert optimizer hints to planner hints
	return convertOptimizerHints(optimizedPlan)
}

// convertOptimizerHints converts optimizer.OptimizedPlan hints to planner.OptimizationHints.
func convertOptimizerHints(optimizedPlan *optimizer.OptimizedPlan) *planner.OptimizationHints {
	if optimizedPlan == nil {
		return nil
	}

	hints := planner.NewOptimizationHints()

	// Convert join hints
	for key, hint := range optimizedPlan.JoinHints {
		hints.JoinHints[key] = planner.JoinHint{
			Method:    string(hint.Method),
			BuildSide: hint.BuildSide,
		}
	}

	// Convert access hints
	for key, hint := range optimizedPlan.AccessHints {
		// Convert LookupKeys from []optimizer.PredicateExpr to []any
		// If the PredicateExpr is a boundExprAdapter, extract the underlying BoundExpr
		// so the planner can recognize it as a binder.BoundExpr
		var lookupKeys []any
		for _, lk := range hint.LookupKeys {
			// Try to extract the underlying BoundExpr if this is a boundExprAdapter
			if adapter, ok := lk.(*boundExprAdapter); ok && adapter.expr != nil {
				lookupKeys = append(lookupKeys, adapter.expr)
			} else {
				lookupKeys = append(lookupKeys, lk)
			}
		}

		// Convert ResidualPredicates to []any for residual filter
		var residualFilter any
		if len(hint.ResidualPredicates) > 0 {
			residualAsAny := make([]any, len(hint.ResidualPredicates))
			for i, rp := range hint.ResidualPredicates {
				residualAsAny[i] = rp
			}
			residualFilter = residualAsAny
		}

		// Convert MatchedPredicates from []optimizer.PredicateExpr to []any
		var matchedPredicates []any
		for _, mp := range hint.MatchedPredicates {
			matchedPredicates = append(matchedPredicates, mp)
		}

		// Convert RangeBounds if this is a range scan
		var rangeBounds *planner.RangeScanBounds
		if hint.IsRangeScan && hint.RangeBounds != nil {
			rangeBounds = convertRangeBoundsToPlanner(hint.RangeBounds)
		}

		hints.AccessHints[key] = planner.AccessHint{
			Method:            string(hint.Method),
			IndexName:         hint.IndexName,
			LookupKeys:        lookupKeys,
			ResidualFilter:    residualFilter,
			MatchedPredicates: matchedPredicates,
			Selectivity:       hint.Selectivity,
			MatchedColumns:    hint.MatchedColumns,
			IsFullMatch:       hint.IsFullMatch,
			IsRangeScan:       hint.IsRangeScan,
			RangeBounds:       rangeBounds,
		}
	}

	return hints
}

// convertRangeBoundsToPlanner converts optimizer RangeScanBounds to planner RangeScanBounds.
// This bridges the gap between the optimizer representation and the planner representation.
// The bounds may contain boundExprAdapter wrappers that need to be preserved for proper
// expression evaluation in the executor.
func convertRangeBoundsToPlanner(optBounds *optimizer.RangeScanBounds) *planner.RangeScanBounds {
	if optBounds == nil {
		return nil
	}

	// Convert LowerBound - extract BoundExpr from adapter if needed
	var lowerBound any
	if optBounds.LowerBound != nil {
		if adapter, ok := optBounds.LowerBound.(*boundExprAdapter); ok && adapter.expr != nil {
			lowerBound = adapter.expr
		} else {
			lowerBound = optBounds.LowerBound
		}
	}

	// Convert UpperBound - extract BoundExpr from adapter if needed
	var upperBound any
	if optBounds.UpperBound != nil {
		if adapter, ok := optBounds.UpperBound.(*boundExprAdapter); ok && adapter.expr != nil {
			upperBound = adapter.expr
		} else {
			upperBound = optBounds.UpperBound
		}
	}

	return &planner.RangeScanBounds{
		LowerBound:       lowerBound,
		UpperBound:       upperBound,
		LowerInclusive:   optBounds.LowerInclusive,
		UpperInclusive:   optBounds.UpperInclusive,
		RangeColumnIndex: optBounds.RangeColumnIndex,
	}
}

// createLogicalPlanAdapter creates a logical plan adapter for the optimizer.
// This adapts a bound statement to the optimizer.LogicalPlanNode interface.
// For now, we only support SELECT statements.
func createLogicalPlanAdapter(boundStmt binder.BoundStatement) optimizer.LogicalPlanNode {
	selectStmt, ok := boundStmt.(*binder.BoundSelectStmt)
	if !ok {
		return nil
	}

	return &selectLogicalPlanAdapter{stmt: selectStmt}
}

// selectLogicalPlanAdapter adapts a BoundSelectStmt to optimizer.LogicalPlanNode.
type selectLogicalPlanAdapter struct {
	stmt *binder.BoundSelectStmt
}

func (a *selectLogicalPlanAdapter) PlanType() string {
	// If there's no FROM clause, this is a dummy scan (e.g., SELECT 1+1)
	if len(a.stmt.From) == 0 && len(a.stmt.Joins) == 0 {
		return "LogicalDummyScan"
	}
	return "LogicalProject" // Top level of a SELECT is typically a projection
}

func (a *selectLogicalPlanAdapter) PlanChildren() []optimizer.LogicalPlanNode {
	// Build children from the FROM clause
	var children []optimizer.LogicalPlanNode

	// Add scan nodes for each table in FROM
	// If there's a WHERE clause, wrap each scan in a filter node
	for _, tableRef := range a.stmt.From {
		scanNode := &tableRefLogicalPlanAdapter{ref: tableRef}
		if a.stmt.Where != nil {
			// Wrap the scan in a filter node for the optimizer to detect index opportunities
			children = append(children, &filterLogicalPlanAdapter{
				condition: a.stmt.Where,
				child:     scanNode,
			})
		} else {
			children = append(children, scanNode)
		}
	}

	// Add join nodes for explicit joins
	for _, join := range a.stmt.Joins {
		children = append(children, &joinLogicalPlanAdapter{join: join, stmt: a.stmt})
	}

	return children
}

func (a *selectLogicalPlanAdapter) PlanOutputColumns() []optimizer.OutputColumn {
	var cols []optimizer.OutputColumn
	for _, col := range a.stmt.Columns {
		outputCol := optimizer.OutputColumn{
			Column: col.Alias,
		}
		if col.Expr != nil {
			outputCol.Type = col.Expr.ResultType()
		}
		cols = append(cols, outputCol)
	}
	return cols
}

// tableRefLogicalPlanAdapter adapts a BoundTableRef to optimizer.LogicalPlanNode/ScanNode.
type tableRefLogicalPlanAdapter struct {
	ref *binder.BoundTableRef
}

func (a *tableRefLogicalPlanAdapter) PlanType() string {
	return "LogicalScan"
}

func (a *tableRefLogicalPlanAdapter) PlanChildren() []optimizer.LogicalPlanNode {
	return nil
}

func (a *tableRefLogicalPlanAdapter) PlanOutputColumns() []optimizer.OutputColumn {
	var cols []optimizer.OutputColumn
	if a.ref.TableDef != nil {
		for _, colDef := range a.ref.TableDef.Columns {
			cols = append(cols, optimizer.OutputColumn{
				Table:  a.ref.Alias,
				Column: colDef.Name,
				Type:   colDef.Type,
			})
		}
	}
	return cols
}

// Implement optimizer.ScanNode interface
func (a *tableRefLogicalPlanAdapter) Schema() string {
	return a.ref.Schema
}

func (a *tableRefLogicalPlanAdapter) TableName() string {
	return a.ref.TableName
}

func (a *tableRefLogicalPlanAdapter) Alias() string {
	return a.ref.Alias
}

func (a *tableRefLogicalPlanAdapter) IsTableFunction() bool {
	return a.ref.TableFunction != nil
}

func (a *tableRefLogicalPlanAdapter) IsVirtualTable() bool {
	return a.ref.VirtualTable != nil
}

// filterLogicalPlanAdapter adapts a WHERE clause to optimizer.FilterNode.
// This allows the optimizer to detect Filter -> Scan patterns and generate IndexScan hints.
type filterLogicalPlanAdapter struct {
	condition binder.BoundExpr
	child     optimizer.LogicalPlanNode
}

func (a *filterLogicalPlanAdapter) PlanType() string {
	return "LogicalFilter"
}

func (a *filterLogicalPlanAdapter) PlanChildren() []optimizer.LogicalPlanNode {
	return []optimizer.LogicalPlanNode{a.child}
}

func (a *filterLogicalPlanAdapter) PlanOutputColumns() []optimizer.OutputColumn {
	// Filter passes through all columns from child
	return a.child.PlanOutputColumns()
}

// FilterChild implements optimizer.FilterNode interface.
func (a *filterLogicalPlanAdapter) FilterChild() optimizer.LogicalPlanNode {
	return a.child
}

// FilterCondition implements optimizer.FilterNode interface.
func (a *filterLogicalPlanAdapter) FilterCondition() optimizer.ExprNode {
	return &boundExprAdapter{expr: a.condition}
}

// boundExprAdapter adapts binder.BoundExpr to optimizer.ExprNode interface.
type boundExprAdapter struct {
	expr binder.BoundExpr
}

func (a *boundExprAdapter) ExprType() string {
	if a.expr == nil {
		return "Unknown"
	}
	switch e := a.expr.(type) {
	case *binder.BoundBinaryExpr:
		return "BinaryExpr"
	case *binder.BoundUnaryExpr:
		return "UnaryExpr"
	case *binder.BoundColumnRef:
		return "ColumnRef"
	case *binder.BoundLiteral:
		return "Literal"
	case *binder.BoundFunctionCall:
		return "FunctionCall"
	default:
		_ = e
		return "Unknown"
	}
}

func (a *boundExprAdapter) ExprResultType() dukdb.Type {
	if a.expr == nil {
		return dukdb.TYPE_ANY
	}
	return a.expr.ResultType()
}

// Implement optimizer.BinaryExprNode for binary expressions
func (a *boundExprAdapter) Left() optimizer.ExprNode {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		return &boundExprAdapter{expr: binExpr.Left}
	}
	return nil
}

func (a *boundExprAdapter) Right() optimizer.ExprNode {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		return &boundExprAdapter{expr: binExpr.Right}
	}
	return nil
}

func (a *boundExprAdapter) Operator() optimizer.BinaryOp {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		// Map parser.BinaryOp to optimizer.BinaryOp
		// Both packages use the same constant ordering, so we can cast directly
		// but we explicitly map for safety
		switch binExpr.Op {
		case parser.OpEq:
			return optimizer.OpEq
		case parser.OpNe:
			return optimizer.OpNe
		case parser.OpLt:
			return optimizer.OpLt
		case parser.OpLe:
			return optimizer.OpLe
		case parser.OpGt:
			return optimizer.OpGt
		case parser.OpGe:
			return optimizer.OpGe
		case parser.OpAnd:
			return optimizer.OpAnd
		case parser.OpOr:
			return optimizer.OpOr
		case parser.OpLike:
			return optimizer.OpLike
		case parser.OpIn:
			return optimizer.OpIn
		default:
			// For unknown operators, return -1 as a sentinel value
			return optimizer.BinaryOp(-1)
		}
	}
	return optimizer.BinaryOp(-1)
}

// Implement optimizer.PredicateExpr interface for index matching
func (a *boundExprAdapter) PredicateType() string {
	if a.expr == nil {
		return "Unknown"
	}
	switch a.expr.(type) {
	case *binder.BoundBinaryExpr:
		return "BinaryExpr"
	case *binder.BoundColumnRef:
		return "ColumnRef"
	case *binder.BoundLiteral:
		return "Literal"
	case *binder.BoundBetweenExpr:
		return "BetweenExpr"
	case *binder.BoundInListExpr:
		return "InListExpr"
	default:
		return "Unknown"
	}
}

func (a *boundExprAdapter) GetColumn() string {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		// For equality predicates, extract column name from left side
		if colRef, ok := binExpr.Left.(*binder.BoundColumnRef); ok {
			return colRef.Column
		}
	}
	if colRef, ok := a.expr.(*binder.BoundColumnRef); ok {
		return colRef.Column
	}
	return ""
}

func (a *boundExprAdapter) GetTable() string {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		// For equality predicates, extract table from left side
		if colRef, ok := binExpr.Left.(*binder.BoundColumnRef); ok {
			return colRef.Table
		}
	}
	if colRef, ok := a.expr.(*binder.BoundColumnRef); ok {
		return colRef.Table
	}
	return ""
}

func (a *boundExprAdapter) GetOperator() optimizer.BinaryOp {
	return a.Operator()
}

func (a *boundExprAdapter) GetValue() any {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		// For equality predicates, extract literal value from right side
		if lit, ok := binExpr.Right.(*binder.BoundLiteral); ok {
			return lit.Value
		}
	}
	if lit, ok := a.expr.(*binder.BoundLiteral); ok {
		return lit.Value
	}
	return nil
}

// Implement optimizer.BinaryPredicateExpr interface for index matching
func (a *boundExprAdapter) PredicateLeft() optimizer.PredicateExpr {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		return &boundExprAdapter{expr: binExpr.Left}
	}
	return nil
}

func (a *boundExprAdapter) PredicateRight() optimizer.PredicateExpr {
	if binExpr, ok := a.expr.(*binder.BoundBinaryExpr); ok {
		return &boundExprAdapter{expr: binExpr.Right}
	}
	return nil
}

func (a *boundExprAdapter) PredicateOperator() optimizer.BinaryOp {
	return a.Operator()
}

// Implement optimizer.ColumnRefPredicateExpr interface for index matching
func (a *boundExprAdapter) PredicateTable() string {
	if colRef, ok := a.expr.(*binder.BoundColumnRef); ok {
		return colRef.Table
	}
	return ""
}

func (a *boundExprAdapter) PredicateColumn() string {
	if colRef, ok := a.expr.(*binder.BoundColumnRef); ok {
		return colRef.Column
	}
	return ""
}

// Implement optimizer.BetweenPredicateExpr interface for BETWEEN index matching
func (a *boundExprAdapter) PredicateBetweenExpr() optimizer.PredicateExpr {
	if betweenExpr, ok := a.expr.(*binder.BoundBetweenExpr); ok {
		return &boundExprAdapter{expr: betweenExpr.Expr}
	}
	return nil
}

func (a *boundExprAdapter) PredicateLowBound() optimizer.PredicateExpr {
	if betweenExpr, ok := a.expr.(*binder.BoundBetweenExpr); ok {
		return &boundExprAdapter{expr: betweenExpr.Low}
	}
	return nil
}

func (a *boundExprAdapter) PredicateHighBound() optimizer.PredicateExpr {
	if betweenExpr, ok := a.expr.(*binder.BoundBetweenExpr); ok {
		return &boundExprAdapter{expr: betweenExpr.High}
	}
	return nil
}

func (a *boundExprAdapter) PredicateIsNotBetween() bool {
	if betweenExpr, ok := a.expr.(*binder.BoundBetweenExpr); ok {
		return betweenExpr.Not
	}
	return false
}

// Implement optimizer.LiteralPredicateExpr interface for literal value extraction
func (a *boundExprAdapter) PredicateLiteralValue() any {
	if lit, ok := a.expr.(*binder.BoundLiteral); ok {
		return lit.Value
	}
	return nil
}

// joinLogicalPlanAdapter adapts a BoundJoin to optimizer.LogicalPlanNode.
type joinLogicalPlanAdapter struct {
	join *binder.BoundJoin
	stmt *binder.BoundSelectStmt
}

func (a *joinLogicalPlanAdapter) PlanType() string {
	return "LogicalJoin"
}

func (a *joinLogicalPlanAdapter) PlanChildren() []optimizer.LogicalPlanNode {
	// Left child is the table being joined
	return []optimizer.LogicalPlanNode{
		&tableRefLogicalPlanAdapter{ref: a.join.Table},
	}
}

func (a *joinLogicalPlanAdapter) PlanOutputColumns() []optimizer.OutputColumn {
	var cols []optimizer.OutputColumn
	if a.join.Table != nil && a.join.Table.TableDef != nil {
		for _, colDef := range a.join.Table.TableDef.Columns {
			cols = append(cols, optimizer.OutputColumn{
				Table:  a.join.Table.Alias,
				Column: colDef.Name,
				Type:   colDef.Type,
			})
		}
	}
	return cols
}

// Prepare prepares a statement for execution.
func (c *EngineConn) Prepare(
	ctx context.Context,
	query string,
) (dukdb.BackendStmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, dukdb.ErrConnectionClosed
	}

	// Parse the query to validate it
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	// Count and collect parameters
	numParams := parser.CountParameters(stmt)
	params := parser.CollectParameters(stmt)

	engineStmt := &EngineStmt{
		conn:       c,
		query:      query,
		stmt:       stmt,
		numParams:  numParams,
		params:     params,
		paramTypes: make(map[int]dukdb.Type),
	}

	// Bind the statement to get parameter types and column metadata
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, bindErr := b.Bind(stmt)
	if bindErr == nil {
		// Extract inferred parameter types from binder
		engineStmt.paramTypes = b.GetParamTypes()

		// For SELECT statements, also extract column metadata
		if boundSelect, ok := boundStmt.(*binder.BoundSelectStmt); ok {
			engineStmt.columns = make(
				[]columnInfo,
				0,
				len(boundSelect.Columns),
			)
			for _, col := range boundSelect.Columns {
				name := col.Alias
				if name == "" && col.Expr != nil {
					// Try to infer name from expression
					if colRef, ok := col.Expr.(*binder.BoundColumnRef); ok {
						name = colRef.Column
					}
				}
				var colType dukdb.Type
				if col.Expr != nil {
					colType = col.Expr.ResultType()
				}
				engineStmt.columns = append(
					engineStmt.columns,
					columnInfo{
						name:    name,
						colType: colType,
					},
				)
			}
		}
	}

	return engineStmt, nil
}

// Close closes the connection.
func (c *EngineConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Clean up prepared statements
	c.sqlPrepared = nil

	// Rollback any active transaction
	if c.txn != nil && c.txn.IsActive() {
		_ = c.engine.txnMgr.Rollback(c.txn)
	}

	return nil
}

// ID returns the unique connection ID for this engine connection.
//
// The ID is assigned during connection creation via [generateConnID] and
// remains stable throughout the connection's lifetime. IDs are:
//   - Unique: Each connection within a process gets a distinct ID
//   - Stable: The same connection always returns the same ID
//   - Never reused: IDs increment monotonically and are never recycled
//   - Sequential: IDs are assigned in creation order (1, 2, 3, ...)
//
// This method implements the [dukdb.BackendConnIdentifiable] interface,
// enabling the public [dukdb.ConnId] API to retrieve connection IDs.
//
// # Thread Safety
//
// This method is safe to call concurrently from multiple goroutines.
// The ID is immutable once assigned during connection creation.
//
// # ID Space
//
// Connection IDs are 64-bit unsigned integers starting at 1 (ID 0 is
// reserved as an invalid/error value). The uint64 space allows for
// over 18 quintillion unique IDs before wraparound, which is not a
// practical concern for any application.
func (c *EngineConn) ID() uint64 {
	return c.id
}

// IsClosed returns whether the connection has been closed.
// Thread-safe.
func (c *EngineConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Ping verifies that the connection is still alive.
func (c *EngineConn) Ping(
	ctx context.Context,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return dukdb.ErrConnectionClosed
	}

	return nil
}

// AppendDataChunk appends a DataChunk directly to a table, bypassing SQL parsing.
// This provides efficient bulk data loading for the Appender.
func (c *EngineConn) AppendDataChunk(
	ctx context.Context,
	schema, table string,
	chunk *dukdb.DataChunk,
) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, dukdb.ErrConnectionClosed
	}

	// Get the table from storage
	tableKey := schema + "." + table
	storageTable, ok := c.engine.storage.GetTable(
		tableKey,
	)
	if !ok {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "table not found: " + tableKey,
		}
	}

	// Convert dukdb.DataChunk to storage.DataChunk
	// Get the number of rows in the chunk
	rowCount := chunk.GetSize()
	if rowCount == 0 {
		return 0, nil
	}

	// Create a storage DataChunk with the same column types
	colTypes := storageTable.ColumnTypes()
	storageChunk := storage.NewDataChunkWithCapacity(
		colTypes,
		rowCount,
	)

	// Copy data from dukdb.DataChunk to storage.DataChunk
	colCount := chunk.GetColumnCount()
	for row := range rowCount {
		values := make([]any, colCount)
		for col := range colCount {
			val, err := chunk.GetValue(col, row)
			if err != nil {
				return 0, err
			}
			values[col] = val
		}
		storageChunk.AppendRow(values)
	}

	// Append the storage chunk to the table
	if err := storageTable.AppendChunk(storageChunk); err != nil {
		return 0, err
	}

	return int64(rowCount), nil
}

// GetTableTypeInfos returns the TypeInfo for all columns in a table.
// This is used by the Appender to create DataChunks with the correct types.
func (c *EngineConn) GetTableTypeInfos(
	schema, table string,
) ([]dukdb.TypeInfo, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Get the table definition from catalog
	tableDef, ok := c.engine.catalog.GetTableInSchema(
		schema,
		table,
	)
	if !ok {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "table not found: " + schema + "." + table,
		}
	}

	// Get TypeInfos and column names
	typeInfos := tableDef.ColumnTypeInfos()
	colNames := tableDef.ColumnNames()

	return typeInfos, colNames, nil
}

// EngineStmt represents a prepared statement.
type EngineStmt struct {
	mu        sync.Mutex
	conn      *EngineConn
	query     string
	stmt      parser.Statement
	numParams int
	closed    bool

	// Introspection metadata
	params     []parser.ParameterInfo
	paramTypes map[int]dukdb.Type // position -> inferred type
	columns    []columnInfo       // Populated after binding for SELECT statements
}

// columnInfo holds result column metadata.
type columnInfo struct {
	name    string
	colType dukdb.Type
}

// Execute executes the prepared statement.
func (s *EngineStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Execute(ctx, s.query, args)
}

// Query executes the prepared statement and returns rows.
func (s *EngineStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, &dukdb.Error{
			Type: dukdb.ErrorTypeConnection,
			Msg:  "statement closed",
		}
	}

	return s.conn.Query(ctx, s.query, args)
}

// Close closes the statement.
func (s *EngineStmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *EngineStmt) NumInput() int {
	return s.numParams
}

// StatementType returns the type of the prepared statement.
func (s *EngineStmt) StatementType() dukdb.StmtType {
	if s.closed || s.stmt == nil {
		return dukdb.STATEMENT_TYPE_INVALID
	}

	return s.stmt.Type()
}

// ParamName returns the name of the parameter at the given index (1-based).
// Returns empty string for positional parameters.
func (s *EngineStmt) ParamName(index int) string {
	if index < 1 || index > len(s.params) {
		return ""
	}

	return s.params[index-1].Name
}

// ParamType returns the inferred type of the parameter at the given index (1-based).
// Returns TYPE_ANY if the type could not be inferred from context.
func (s *EngineStmt) ParamType(
	index int,
) dukdb.Type {
	if index < 1 || index > s.numParams {
		return dukdb.TYPE_INVALID
	}
	if typ, ok := s.paramTypes[index]; ok {
		return typ
	}

	return dukdb.TYPE_ANY
}

// ColumnCount returns the number of result columns.
// Returns 0 for non-SELECT statements.
func (s *EngineStmt) ColumnCount() int {
	return len(s.columns)
}

// ColumnName returns the name of the result column at the given index (0-based).
func (s *EngineStmt) ColumnName(
	index int,
) string {
	if index < 0 || index >= len(s.columns) {
		return ""
	}

	return s.columns[index].name
}

// ColumnType returns the type of the result column at the given index (0-based).
func (s *EngineStmt) ColumnType(
	index int,
) dukdb.Type {
	if index < 0 || index >= len(s.columns) {
		return dukdb.TYPE_INVALID
	}

	return s.columns[index].colType
}

// ColumnTypeInfo returns extended type info for the result column at the given index (0-based).
func (s *EngineStmt) ColumnTypeInfo(
	index int,
) dukdb.TypeInfo {
	if index < 0 || index >= len(s.columns) {
		return nil
	}
	colType := s.columns[index].colType
	// For primitive types, create TypeInfo using NewTypeInfo
	// Complex types would need additional metadata from the binder
	info, err := dukdb.NewTypeInfo(colType)
	if err != nil {
		// For complex types where NewTypeInfo fails, return a basic wrapper
		// This is a limitation - full complex type info requires binder enhancement
		return &basicTypeInfo{typ: colType}
	}

	return info
}

// basicTypeInfo is a simple TypeInfo wrapper for types that don't have
// specialized constructors available.
type basicTypeInfo struct {
	typ dukdb.Type
}

func (b *basicTypeInfo) InternalType() dukdb.Type {
	return b.typ
}

func (b *basicTypeInfo) Details() dukdb.TypeDetails {
	return nil
}

func (b *basicTypeInfo) SQLType() string {
	return b.typ.String()
}

// Properties returns metadata about the prepared statement.
func (s *EngineStmt) Properties() dukdb.StmtProperties {
	stmtType := s.StatementType()

	return dukdb.StmtProperties{
		Type:        stmtType,
		ReturnType:  stmtType.ReturnType(),
		IsReadOnly:  s.isReadOnly(),
		IsStreaming: stmtType.ReturnType() == dukdb.RETURN_QUERY_RESULT,
		ColumnCount: s.ColumnCount(),
		ParamCount:  s.NumInput(),
	}
}

// isReadOnly returns true if the statement doesn't modify any data.
func (s *EngineStmt) isReadOnly() bool {
	switch s.StatementType() {
	case dukdb.STATEMENT_TYPE_SELECT,
		dukdb.STATEMENT_TYPE_EXPLAIN,
		dukdb.STATEMENT_TYPE_PRAGMA,
		dukdb.STATEMENT_TYPE_PREPARE,
		dukdb.STATEMENT_TYPE_RELATION,
		dukdb.STATEMENT_TYPE_LOGICAL_PLAN:
		return true
	default:
		return false
	}
}

// GetCatalog returns the catalog for virtual table registration.
// Implements the BackendConnCatalog interface.
func (c *EngineConn) GetCatalog() any {
	return c.engine.Catalog()
}

// ExtractTableNames parses a SQL query and returns all referenced table names.
// Implements the BackendConnTableExtractor interface.
//
// This method uses the internal parser to extract table references from the query
// without executing it. It supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE,
// and DROP TABLE statements.
//
// Parameters:
//   - query: The SQL query string to parse
//   - qualified: If true, returns qualified names (schema.table); if false, returns unqualified names
//
// Returns:
//   - A sorted, deduplicated slice of table names
//   - An empty slice (not nil) for queries with no table references
//   - An error if the query cannot be parsed
func (c *EngineConn) ExtractTableNames(query string, qualified bool) ([]string, error) {
	// Note: This operation doesn't require connection state or locking
	// since it only parses the query without accessing the database

	// Handle empty query - return empty slice, not nil
	trimmedQuery := query
	for trimmedQuery != "" && (trimmedQuery[0] == ' ' || trimmedQuery[0] == '\t' || trimmedQuery[0] == '\n' || trimmedQuery[0] == '\r') {
		trimmedQuery = trimmedQuery[1:]
	}
	if trimmedQuery == "" {
		return []string{}, nil
	}

	// Parse the query using the internal parser
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	// Create a table extractor with the specified qualified mode
	extractor := parser.NewTableExtractor(qualified)

	// Use the visitor pattern to extract table names
	// Each statement type has an Accept method that calls the appropriate visitor method
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		s.Accept(extractor)
	case *parser.InsertStmt:
		s.Accept(extractor)
	case *parser.UpdateStmt:
		s.Accept(extractor)
	case *parser.DeleteStmt:
		s.Accept(extractor)
	case *parser.CreateTableStmt:
		s.Accept(extractor)
	case *parser.DropTableStmt:
		s.Accept(extractor)
	case *parser.BeginStmt:
		s.Accept(extractor)
	case *parser.CommitStmt:
		s.Accept(extractor)
	case *parser.RollbackStmt:
		s.Accept(extractor)
	default:
		// Unknown statement type - no tables to extract
		// Return empty slice, not nil
		return []string{}, nil
	}

	// Get the sorted, deduplicated table names
	tables := extractor.GetTables()

	// Ensure we never return nil - return empty slice instead
	if tables == nil {
		return []string{}, nil
	}

	return tables, nil
}

// QueryStreaming executes a query and returns a StreamingResult for streaming consumption.
// It follows the same parse/bind/plan flow as Query() but calls executor.ExecuteStreaming()
// instead of executor.Execute().
//
// Note on mutex handling (task 3.3): In the current materialized-then-wrapped implementation,
// the full result set is computed before returning, so the mutex is held for the duration of
// execution (same as Query). When true lazy evaluation is implemented, the mutex will need to
// be released after pipeline setup so the connection is not blocked while rows are consumed.
func (c *EngineConn) QueryStreaming(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (*dukdb.StreamingResult, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, dukdb.ErrConnectionClosed
	}

	// Mark the start of a new statement for READ COMMITTED isolation.
	if c.txn != nil {
		c.txn.BeginStatement()
	}

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	// Handle SHOW statements at connection level
	if showStmt, ok := stmt.(*parser.ShowStmt); ok {
		rows, cols, err := c.handleShow(showStmt)
		if err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, rows, cols)
	}

	// Handle DESCRIBE statements at connection level
	if describeStmt, ok := stmt.(*parser.DescribeStmt); ok {
		rows, cols, err := c.handleDescribe(describeStmt)
		if err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, rows, cols)
	}

	// Handle SUMMARIZE statements at connection level
	if summarizeStmt, ok := stmt.(*parser.SummarizeStmt); ok {
		rows, cols, err := c.handleSummarize(summarizeStmt)
		if err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, rows, cols)
	}

	// Handle CALL statements at connection level
	if callStmt, ok := stmt.(*parser.CallStmt); ok {
		rows, cols, err := c.handleCall(callStmt)
		if err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, rows, cols)
	}

	// Handle extension and database management statements
	switch s := stmt.(type) {
	case *parser.InstallStmt:
		if _, err := c.handleInstall(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.LoadStmt:
		if _, err := c.handleLoad(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.AttachStmt:
		if _, err := c.handleAttach(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.DetachStmt:
		if _, err := c.handleDetach(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.UseStmt:
		if _, err := c.handleUse(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.CreateDatabaseStmt:
		if _, err := c.handleCreateDatabase(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.DropDatabaseStmt:
		if _, err := c.handleDropDatabase(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	default:
		// not a special statement, continue
	}

	// Handle SQL-level prepared statement operations
	switch s := stmt.(type) {
	case *parser.PrepareStmt:
		if _, err := c.handlePrepare(s, query); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	case *parser.ExecuteStmt:
		rows, cols, err := c.handleExecuteStmtQuery(ctx, s, args)
		if err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, rows, cols)
	case *parser.DeallocateStmt:
		if _, err := c.handleDeallocate(s); err != nil {
			return nil, nil, err
		}
		return c.wrapRowsAsStreaming(ctx, []map[string]any{}, []string{})
	}

	// Bind the statement
	b := binder.NewBinder(c.engine.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, nil, err
	}

	// Plan the statement with optional optimization hints
	p := planner.NewPlanner(c.engine.catalog)

	// For SELECT statements, run the cost-based optimizer if enabled
	if _, isSelect := boundStmt.(*binder.BoundSelectStmt); isSelect {
		hints := c.getOptimizationHints(boundStmt)
		if hints != nil {
			p.SetHints(hints)
		}
	}

	// For EXPLAIN statements, run the cost-based optimizer on the child query
	if explainStmt, isExplain := boundStmt.(*binder.BoundExplainStmt); isExplain {
		if _, isSelectChild := explainStmt.Query.(*binder.BoundSelectStmt); isSelectChild {
			hints := c.getOptimizationHints(explainStmt.Query)
			if hints != nil {
				p.SetHints(hints)
			}
		}
	}

	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	// Execute the plan via streaming
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
	exec.SetConnection(c)
	if c.engine.extensions != nil {
		exec.SetExtensionRegistry(&extensionRegistryAdapter{registry: c.engine.extensions})
	}
	if c.engine.ftsRegistry != nil {
		exec.SetFTSRegistry(&ftsRegistryAdapter{registry: c.engine.ftsRegistry})
	}
	exec.SetQueryCache(c.engine.QueryCache())
	if c.engine.WAL() != nil {
		exec.SetWAL(c.engine.WAL())
		if c.txn != nil {
			exec.SetTxnID(c.txn.ID())
		}
	}
	if c.txn != nil && c.inTxn {
		exec.SetUndoRecorder(&undoRecorderAdapter{txn: c.txn})
		exec.SetInTransaction(true)
	}
	c.configureExecutorMVCC(exec)

	sr, err := exec.ExecuteStreaming(ctx, plan, args)
	if err != nil {
		return nil, nil, err
	}

	return sr, sr.Columns(), nil
}

// wrapRowsAsStreaming wraps materialized rows in a StreamingResult.
// This is used for special statement types (SHOW, INSTALL, etc.) that
// return materialized results but need to be wrapped in the streaming interface.
func (c *EngineConn) wrapRowsAsStreaming(
	ctx context.Context,
	data []map[string]any,
	columns []string,
) (*dukdb.StreamingResult, []string, error) {
	_, cancel := context.WithCancel(ctx)
	pos := 0
	scanNext := func(dest []driver.Value) error {
		if pos >= len(data) {
			return io.EOF
		}
		row := data[pos]
		for i, col := range columns {
			dest[i] = row[col]
		}
		pos++
		return nil
	}

	return dukdb.NewStreamingResult(columns, scanNext, cancel), columns, nil
}

// Verify interface implementations
var (
	_ dukdb.BackendConn = (*EngineConn)(
		nil,
	)
	_ dukdb.BackendConnCatalog = (*EngineConn)(
		nil,
	)
	_ dukdb.BackendConnStreaming = (*EngineConn)(
		nil,
	)
	_ dukdb.BackendConnTableExtractor = (*EngineConn)(
		nil,
	)
	_ dukdb.BackendStmt = (*EngineStmt)(
		nil,
	)
	_ dukdb.BackendStmtIntrospector = (*EngineStmt)(
		nil,
	)
	_ dukdb.BackendStmtProperties = (*EngineStmt)(
		nil,
	)
)
