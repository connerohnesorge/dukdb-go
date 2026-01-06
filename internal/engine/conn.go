package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/executor"
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
	mu                        sync.Mutex
	id                        uint64 // Unique connection ID, assigned at creation
	engine                    *Engine
	txn                       *Transaction
	closed                    bool
	inTxn                     bool                 // Whether BEGIN was explicitly called (explicit transaction mode)
	defaultIsolationLevel     parser.IsolationLevel // Default isolation level for new transactions
	currentIsolationLevel     parser.IsolationLevel // Isolation level of current transaction (if any)
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
func (c *EngineConn) handleRollbackToSavepoint(stmt *parser.RollbackToSavepointStmt) (int64, error) {
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
	default:
		// Unknown variable - for now we silently accept it
		// In a full implementation, we might store these in a config map
		return 0, nil
	}
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

// handleShow handles the SHOW statement for session configuration.
// Returns a single row with the value of the requested variable.
func (c *EngineConn) handleShow(stmt *parser.ShowStmt) ([]map[string]any, []string, error) {
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

	// Parse the query
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	// Handle SHOW statements at connection level
	if showStmt, ok := stmt.(*parser.ShowStmt); ok {
		return c.handleShow(showStmt)
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

	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, nil, err
	}

	// Execute the plan
	exec := executor.NewExecutor(
		c.engine.catalog,
		c.engine.storage,
	)
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

	return result.Rows, result.Columns, nil
}

// getOptimizationHints runs the cost-based optimizer on a bound statement
// and returns planner hints derived from the optimized plan.
// Returns nil if optimization is disabled or fails.
func (c *EngineConn) getOptimizationHints(boundStmt binder.BoundStatement) *planner.OptimizationHints {
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
		hints.AccessHints[key] = planner.AccessHint{
			Method:    string(hint.Method),
			IndexName: hint.IndexName,
		}
	}

	return hints
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
	for _, tableRef := range a.stmt.From {
		children = append(children, &tableRefLogicalPlanAdapter{ref: tableRef})
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

// Verify interface implementations
var (
	_ dukdb.BackendConn = (*EngineConn)(
		nil,
	)
	_ dukdb.BackendConnCatalog = (*EngineConn)(
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
