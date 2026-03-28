// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/cache"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/storage/index"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// ConnectionInterface is the interface for accessing connection-level settings.
type ConnectionInterface interface {
	GetSetting(key string) string
	SetSetting(key string, value string)
}

// ExtensionInfo holds metadata about a registered extension.
type ExtensionInfo struct {
	Name        string
	Description string
	Version     string
	Installed   bool
	Loaded      bool
}

// ExtensionRegistryInterface provides access to the extension registry.
type ExtensionRegistryInterface interface {
	ListExtensions() []ExtensionInfo
}

// FTSSearchResult represents a single full-text search result.
type FTSSearchResult struct {
	DocID int64
	Score float64
}

// FTSIndex represents a full-text search index that can be searched.
type FTSIndex interface {
	Search(query string) []FTSSearchResult
	AddDocument(docID int64, text string)
	RemoveDocument(docID int64)
	TableName() string
	ColumnName() string
	DocCount() int
}

// FTSRegistryInterface provides access to the FTS index registry.
type FTSRegistryInterface interface {
	CreateIndex(tableName, columnName string) FTSIndex
	GetIndex(tableName string) (FTSIndex, bool)
	DropIndex(tableName string) bool
	HasIndex(tableName string) bool
}

// ExecutionContext holds context for query execution.
type ExecutionContext struct {
	Context          context.Context
	Args             []driver.NamedValue
	CorrelatedValues map[string]any      // Values from outer scope for LATERAL/correlated subqueries
	// SubqueryCache caches results for non-correlated scalar subqueries keyed by
	// the *BoundSelectStmt pointer. This relies on pointer identity: the same AST
	// node pointer must be reused across evaluations (not cloned) for cache hits.
	SubqueryCache map[*binder.BoundSelectStmt]any
	conn             ConnectionInterface // Connection for accessing session-level settings
}

// ExecutionResult holds the result of query execution.
type ExecutionResult struct {
	Rows         []map[string]any
	Columns      []string
	RowsAffected int64
}

// GlobalOperatorState holds global state shared across operator instances.
type GlobalOperatorState interface{}

// LocalOperatorState holds local state for a single operator instance.
type LocalOperatorState interface{}

// PhysicalOperator is the interface for physical operators that produce DataChunks.
// This interface is used by the execution engine to iterate through query results.
type PhysicalOperator interface {
	// Next returns the next DataChunk of results, or nil if no more data.
	Next() (*storage.DataChunk, error)

	// GetTypes returns the TypeInfo for each column produced by this operator.
	GetTypes() []dukdb.TypeInfo
}

// isAggregateFunc checks if a function name is an aggregate function.
// This is used by executeProject to detect pre-computed aggregate values.
// This list should match the functions implemented in expr.go computeAggregate.
func isAggregateFunc(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MEAN", "MIN", "MAX",
		// Statistical aggregates
		"MEDIAN", "MODE", "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC",
		"ENTROPY", "SKEWNESS", "KURTOSIS",
		"VAR_POP", "VAR_SAMP", "VARIANCE", "STDDEV_POP", "STDDEV_SAMP", "STDDEV",
		// Approximate aggregates
		"APPROX_COUNT_DISTINCT", "APPROX_MEDIAN", "APPROX_QUANTILE",
		// String/list aggregates
		"STRING_AGG", "GROUP_CONCAT", "LISTAGG", "LIST", "ARRAY_AGG", "LIST_DISTINCT",
		// JSON aggregates
		"JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT",
		// Boolean aggregates
		"BOOL_AND", "BOOL_OR", "EVERY",
		// Bitwise aggregates
		"BIT_AND", "BIT_OR", "BIT_XOR",
		// Time series aggregates
		"COUNT_IF", "SUM_IF", "AVG_IF", "MIN_IF", "MAX_IF",
		"FIRST", "LAST", "ANY_VALUE", "ARBITRARY",
		"ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY",
		"HISTOGRAM",
		// Regression aggregates
		"REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2",
		"REGR_COUNT", "REGR_AVGX", "REGR_AVGY", "REGR_SXX", "REGR_SYY", "REGR_SXY",
		"CORR", "COVAR_POP", "COVAR_SAMP",
		// Multiplicative, deviation, and precision aggregates
		"PRODUCT", "MAD", "FAVG", "FSUM", "BITSTRING_AGG",
		// Geometric and weighted aggregates
		"GEOMETRIC_MEAN", "GEOMEAN", "WEIGHTED_AVG":
		return true
	}
	return false
}

// Source produces data chunks.
type Source interface {
	GetData(
		ctx *ExecutionContext,
		chunk *storage.DataChunk,
		state LocalOperatorState,
	) (bool, error)
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
}

// Operator transforms data chunks.
type Operator interface {
	Execute(
		ctx *ExecutionContext,
		input *storage.DataChunk,
		output *storage.DataChunk,
		state LocalOperatorState,
	) error
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
}

// Sink consumes data chunks.
type Sink interface {
	Sink(
		ctx *ExecutionContext,
		chunk *storage.DataChunk,
		state LocalOperatorState,
	) error
	Combine(
		ctx *ExecutionContext,
		gstate GlobalOperatorState,
		lstate LocalOperatorState,
	) error
	Finalize(
		ctx *ExecutionContext,
		gstate GlobalOperatorState,
	) error
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
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

// UndoRecorder is an interface for recording undo operations for transaction rollback.
type UndoRecorder interface {
	RecordUndo(op UndoOperation)
}

// DatabaseManager is an interface for managing attached databases.
// This allows the executor to work with database management without importing the engine package directly.
type DatabaseManager interface {
	Attach(name, path string, readOnly bool, cat *catalog.Catalog, stor *storage.Storage) error
	Detach(name string, ifExists bool) error
	Use(database string) error
	CreateDatabase(name string, ifNotExists bool) error
	DropDatabase(name string, ifExists bool) error
	// GetAttached returns the catalog and storage for an attached database by name.
	// Returns false if no database with that name is attached.
	GetAttached(name string) (*catalog.Catalog, *storage.Storage, bool)
}

// SecretManager is an interface for managing secrets.
// This allows the executor to work with secrets without importing the secret package directly.
type SecretManager interface {
	Create(
		ctx context.Context,
		name string,
		secretType string,
		provider string,
		scope string,
		persistent bool,
		options map[string]string,
	) error
	Delete(ctx context.Context, name string) error
	Update(ctx context.Context, name string, options map[string]string) error
	Get(ctx context.Context, name string) (interface{}, error)
	Exists(ctx context.Context, name string) bool
}

// Executor executes physical plans.
type Executor struct {
	catalog       *catalog.Catalog
	storage       *storage.Storage
	planner       *planner.Planner
	wal           *wal.Writer   // WAL writer for logging DML operations (optional, may be nil)
	txnID         uint64        // Current transaction ID for WAL entries
	undoRecorder  UndoRecorder  // Undo recorder for transaction rollback (optional, may be nil)
	inTxn         bool          // Whether we're in an explicit transaction (BEGIN was called)
	secretManager SecretManager  // Secret manager for CREATE/DROP/ALTER SECRET (optional, may be nil)
	dbManager     DatabaseManager // Database manager for ATTACH/DETACH/USE/CREATE/DROP DATABASE (optional, may be nil)

	// MVCC isolation level support
	visibility       storage.VisibilityChecker  // Visibility checker based on isolation level (optional, may be nil)
	txnCtx           storage.TransactionContext // Transaction context for visibility checks (optional, may be nil)
	conflictDetector *storage.ConflictDetector  // Conflict detector for SERIALIZABLE (optional, may be nil)
	lockManager      *storage.LockManager       // Lock manager for SERIALIZABLE write locks (optional, may be nil)

	// Connection for accessing session-level settings
	conn ConnectionInterface // Connection interface for settings (optional, may be nil)

	queryCache *cache.QueryResultCache

	// Extension registry for duckdb_extensions() table function
	extRegistry ExtensionRegistryInterface // Extension registry (optional, may be nil)

	// Full-text search registry for FTS index operations
	ftsRegistry FTSRegistryInterface // FTS registry (optional, may be nil)

	// sqlExecFunc is a callback for executing SQL statements from within the executor.
	// Used by EXPORT/IMPORT DATABASE to execute sub-statements (COPY TO/FROM, DDL).
	// If nil, EXPORT/IMPORT DATABASE operations will return an error.
	sqlExecFunc func(ctx context.Context, sql string) error
}

// NewExecutor creates a new Executor.
func NewExecutor(
	cat *catalog.Catalog,
	stor *storage.Storage,
) *Executor {
	return &Executor{
		catalog: cat,
		storage: stor,
		planner: planner.NewPlanner(cat),
	}
}

// SetWAL sets the WAL writer for logging DML operations.
// If set to nil, WAL logging is disabled.
func (e *Executor) SetWAL(w *wal.Writer) {
	e.wal = w
}

// SetTxnID sets the current transaction ID for WAL entries.
func (e *Executor) SetTxnID(txnID uint64) {
	e.txnID = txnID
}

// SetUndoRecorder sets the undo recorder for transaction rollback.
// If set to nil, undo recording is disabled (auto-commit mode).
func (e *Executor) SetUndoRecorder(recorder UndoRecorder) {
	e.undoRecorder = recorder
}

// SetInTransaction sets whether we're in an explicit transaction.
func (e *Executor) SetInTransaction(inTxn bool) {
	e.inTxn = inTxn
}

// SetSecretManager sets the secret manager for handling CREATE/DROP/ALTER SECRET operations.
// If set to nil, secret operations will return an error.
func (e *Executor) SetSecretManager(mgr SecretManager) {
	e.secretManager = mgr
}

// SetDatabaseManager sets the database manager for handling ATTACH/DETACH/USE/CREATE/DROP DATABASE operations.
// If set to nil, database management operations will return an error.
func (e *Executor) SetDatabaseManager(mgr DatabaseManager) {
	e.dbManager = mgr
}

// SetVisibility sets the visibility checker for MVCC isolation.
// This determines which row versions are visible during read operations.
// If set to nil, no visibility filtering is applied (all rows are visible).
func (e *Executor) SetVisibility(visibility storage.VisibilityChecker) {
	e.visibility = visibility
}

// SetTransactionContext sets the transaction context for visibility checks.
// This provides the transaction's state (ID, timestamps, snapshot) needed for MVCC.
// If set to nil, visibility checking is disabled.
func (e *Executor) SetTransactionContext(txnCtx storage.TransactionContext) {
	e.txnCtx = txnCtx
}

// SetConflictDetector sets the conflict detector for SERIALIZABLE isolation.
// This tracks read/write sets for detecting serialization conflicts at commit time.
// If set to nil, conflict detection is disabled.
func (e *Executor) SetConflictDetector(cd *storage.ConflictDetector) {
	e.conflictDetector = cd
}

// SetLockManager sets the lock manager for SERIALIZABLE isolation.
// This manages row-level locks for write operations to prevent concurrent modifications.
// If set to nil, locking is disabled.
func (e *Executor) SetLockManager(lm *storage.LockManager) {
	e.lockManager = lm
}

// SetConnection sets the connection for accessing session-level settings.
// If set to nil, settings will not be accessible.
func (e *Executor) SetConnection(conn ConnectionInterface) {
	e.conn = conn
}

// SetQueryCache sets the shared query result cache.
func (e *Executor) SetQueryCache(queryCache *cache.QueryResultCache) {
	e.queryCache = queryCache
}

// SetExtensionRegistry sets the extension registry for duckdb_extensions().
func (e *Executor) SetExtensionRegistry(registry ExtensionRegistryInterface) {
	e.extRegistry = registry
}

// SetFTSRegistry sets the full-text search registry for FTS operations.
func (e *Executor) SetFTSRegistry(registry FTSRegistryInterface) {
	e.ftsRegistry = registry
}

// SetSQLExecFunc sets the callback for executing SQL statements from within the executor.
// This is used by EXPORT/IMPORT DATABASE to execute sub-statements (COPY TO/FROM, DDL).
func (e *Executor) SetSQLExecFunc(fn func(ctx context.Context, sql string) error) {
	e.sqlExecFunc = fn
}

func (e *Executor) invalidateQueryCache(tables ...string) {
	if e.queryCache == nil || len(tables) == 0 {
		return
	}
	e.queryCache.InvalidateTables(tables)
}

// recordUndo records an undo operation if we're in a transaction.
func (e *Executor) recordUndo(op UndoOperation) {
	if e.undoRecorder != nil && e.inTxn {
		e.undoRecorder.RecordUndo(op)
	}
}

// Execute executes a physical plan and returns the result.
func (e *Executor) Execute(
	ctx context.Context,
	plan planner.PhysicalPlan,
	args []driver.NamedValue,
) (*ExecutionResult, error) {
	execCtx := &ExecutionContext{
		Context:       ctx,
		Args:          args,
		SubqueryCache: make(map[*binder.BoundSelectStmt]any),
		conn:          e.conn,
	}
	return e.executeWithContext(execCtx, plan)
}

// executeWithContext executes a physical plan with a given execution context.
// This allows passing correlated values for LATERAL joins.
func (e *Executor) executeWithContext(
	execCtx *ExecutionContext,
	plan planner.PhysicalPlan,
) (*ExecutionResult, error) {
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		return e.executeScan(execCtx, p)
	case *planner.PhysicalIndexScan:
		return e.executeIndexScan(execCtx, p)
	case *planner.PhysicalVirtualTableScan:
		return e.executeVirtualTableScan(execCtx, p)
	case *planner.PhysicalTableFunctionScan:
		return e.executeTableFunctionScan(execCtx, p)
	case *planner.PhysicalFilter:
		return e.executeFilter(execCtx, p)
	case *planner.PhysicalProject:
		return e.executeProject(execCtx, p)
	case *planner.PhysicalHashJoin:
		return e.executeHashJoin(execCtx, p)
	case *planner.PhysicalNestedLoopJoin:
		return e.executeNestedLoopJoin(execCtx, p)
	case *planner.PhysicalPositionalJoin:
		return e.executePositionalJoin(execCtx, p)
	case *planner.PhysicalAsOfJoin:
		return e.executeAsOfJoin(execCtx, p)
	case *planner.PhysicalHashAggregate:
		return e.executeHashAggregate(execCtx, p)
	case *planner.PhysicalSort:
		return e.executeSort(execCtx, p)
	case *planner.PhysicalLimit:
		return e.executeLimit(execCtx, p)
	case *planner.PhysicalDistinct:
		return e.executeDistinct(execCtx, p)
	case *planner.PhysicalDistinctOn:
		return e.executeDistinctOn(execCtx, p)
	case *planner.PhysicalInsert:
		return e.executeInsert(execCtx, p)
	case *planner.PhysicalUpdate:
		return e.executeUpdate(execCtx, p)
	case *planner.PhysicalDelete:
		return e.executeDelete(execCtx, p)
	case *planner.PhysicalCreateTable:
		return e.executeCreateTableWithCTAS(execCtx, p)
	case *planner.PhysicalDropTable:
		return e.executeDropTable(execCtx, p)
	case *planner.PhysicalTruncate:
		return e.executeTruncate(execCtx, p)
	case *planner.PhysicalDummyScan:
		return e.executeDummyScan(execCtx, p)
	case *planner.PhysicalValues:
		return e.executeValues(execCtx, p)
	case *planner.PhysicalBegin:
		return e.executeBegin(execCtx, p)
	case *planner.PhysicalCommit:
		return e.executeCommit(execCtx, p)
	case *planner.PhysicalRollback:
		return e.executeRollback(execCtx, p)
	case *planner.PhysicalWindow:
		return e.executeWindow(execCtx, p)
	case *planner.PhysicalCopyFrom:
		return e.executeCopyFrom(execCtx, p)
	case *planner.PhysicalCopyTo:
		return e.executeCopyTo(execCtx, p)
	// DDL operations
	case *planner.PhysicalCreateView:
		return e.executeCreateView(execCtx, p)
	case *planner.PhysicalDropView:
		return e.executeDropView(execCtx, p)
	case *planner.PhysicalCreateIndex:
		return e.executeCreateIndex(execCtx, p)
	case *planner.PhysicalDropIndex:
		return e.executeDropIndex(execCtx, p)
	case *planner.PhysicalCreateSequence:
		return e.executeCreateSequence(execCtx, p)
	case *planner.PhysicalDropSequence:
		return e.executeDropSequence(execCtx, p)
	case *planner.PhysicalCreateSchema:
		return e.executeCreateSchema(execCtx, p)
	case *planner.PhysicalDropSchema:
		return e.executeDropSchema(execCtx, p)
	case *planner.PhysicalAlterTable:
		return e.executeAlterTable(execCtx, p)
	case *planner.PhysicalComment:
		return e.executeComment(execCtx, p)
	case *planner.PhysicalMerge:
		return e.executeMerge(execCtx, p)
	case *planner.PhysicalLateralJoin:
		return e.executeLateralJoin(execCtx, p)
	case *planner.PhysicalSample:
		return e.executeSample(execCtx, p)
	// CTE operations
	case *planner.PhysicalRecursiveCTE:
		return e.executeRecursiveCTE(execCtx, p)
	case *planner.PhysicalCTEScan:
		return e.executeCTEScan(execCtx, p)
	// PIVOT/UNPIVOT operations
	case *planner.PhysicalPivot:
		return e.executePivotPlan(execCtx, p)
	case *planner.PhysicalUnpivot:
		return e.executeUnpivotPlan(execCtx, p)
	// Type DDL operations
	case *planner.PhysicalCreateType:
		return e.executeCreateType(execCtx, p)
	case *planner.PhysicalDropType:
		return e.executeDropType(execCtx, p)
	// Macro DDL operations
	case *planner.PhysicalCreateMacro:
		return e.executeCreateMacro(execCtx, p)
	case *planner.PhysicalDropMacro:
		return e.executeDropMacro(execCtx, p)
	// Secret DDL operations
	case *planner.PhysicalCreateSecret:
		return e.executeCreateSecret(execCtx, p)
	case *planner.PhysicalDropSecret:
		return e.executeDropSecret(execCtx, p)
	case *planner.PhysicalAlterSecret:
		return e.executeAlterSecret(execCtx, p)
	// Database maintenance operations
	case *planner.PhysicalPragma:
		return e.executePragma(execCtx, p)
	case *planner.PhysicalExplain:
		return e.executeExplain(execCtx, p)
	case *planner.PhysicalVacuum:
		return e.executeVacuum(execCtx, p)
	case *planner.PhysicalAnalyze:
		return e.executeAnalyze(execCtx, p)
	case *planner.PhysicalCheckpoint:
		return e.executeCheckpoint(execCtx, p)
	case *planner.PhysicalIcebergScan:
		return e.executePhysicalIcebergScan(execCtx, p)
	case *planner.PhysicalSetOp:
		return e.executeSetOp(execCtx, p)
	case *planner.PhysicalExportDatabase:
		return e.executeExportDatabase(execCtx, p)
	case *planner.PhysicalImportDatabase:
		return e.executeImportDatabase(execCtx, p)
	case *planner.PhysicalSummarize:
		return e.executeSummarize(execCtx, p)
	// Database management operations
	case *planner.PhysicalAttach:
		return e.executeAttach(execCtx, p)
	case *planner.PhysicalDetach:
		return e.executeDetach(execCtx, p)
	case *planner.PhysicalUse:
		return e.executeUse(execCtx, p)
	case *planner.PhysicalCreateDatabase:
		return e.executeCreateDatabase(execCtx, p)
	case *planner.PhysicalDropDatabase:
		return e.executeDropDatabase(execCtx, p)
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "unsupported physical plan type",
		}
	}
}

// collectResults collects all chunks from a source into a result set.
func (e *Executor) collectResults(
	ctx *ExecutionContext,
	source func(chunk *storage.DataChunk) (bool, error),
	outputCols []planner.ColumnBinding,
) (*ExecutionResult, error) {
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(outputCols)),
	}

	for i, col := range outputCols {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Create initial types slice based on output columns
	types := make([]dukdb.Type, len(outputCols))
	for i, col := range outputCols {
		types[i] = col.Type
	}

	chunk := storage.NewDataChunk(types)

	for {
		chunk.Reset()
		hasMore, err := source(chunk)
		if err != nil {
			return nil, err
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j, col := range outputCols {
				val := chunk.GetValue(i, j)
				// Store with unqualified name for backwards compatibility
				row[result.Columns[j]] = val
				// Also store with qualified name (table.column) if table is specified
				// This allows joins to avoid column name conflicts
				if col.Table != "" {
					row[col.Table+"."+col.Column] = val
				}
			}
			result.Rows = append(result.Rows, row)
		}

		if !hasMore {
			break
		}
	}

	return result, nil
}

func (e *Executor) executeScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalScan,
) (*ExecutionResult, error) {
	// Resolve to attached database storage if schema matches an attached DB
	_, scanStor, _ := e.resolveSchemaTarget(plan.Schema)
	table, ok := scanStor.GetTable(
		plan.TableName,
	)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Use visibility-aware scanning if visibility checker and transaction context are set
	var scanner *storage.TableScanner
	if e.visibility != nil && e.txnCtx != nil {
		scanner = table.ScanWithVisibility(e.visibility, e.txnCtx)
	} else {
		scanner = table.Scan()
	}
	outputCols := plan.OutputColumns()

	return e.collectResults(
		ctx,
		func(output *storage.DataChunk) (bool, error) {
			chunk := scanner.Next()
			if chunk == nil {
				return false, nil
			}

			// For SERIALIZABLE isolation, register reads for conflict detection
			// Each row returned by the scanner should be tracked in the read set
			if e.conflictDetector != nil && e.txnCtx != nil {
				for i := 0; i < chunk.Count(); i++ {
					// Get RowID from scanner for conflict tracking
					rowID := scanner.GetRowID(i)
					if rowID != nil {
						e.conflictDetector.RegisterRead(
							e.txnCtx.GetTxnID(),
							plan.TableName,
							fmt.Sprintf("%d", *rowID),
						)
					}
				}
			}

			// Copy data to output
			for i := 0; i < chunk.Count(); i++ {
				values := make(
					[]any,
					len(outputCols),
				)
				for j, col := range outputCols {
					values[j] = chunk.GetValue(
						i,
						col.ColumnIdx,
					)
				}
				output.AppendRow(values)
			}

			return true, nil
		},
		outputCols,
	)
}

// executeIndexScan executes a PhysicalIndexScan plan node.
// It uses the index to look up matching row IDs and fetches the corresponding rows.
// For range scans, it uses an ART index; for point lookups, it uses a HashIndex.
func (e *Executor) executeIndexScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalIndexScan,
) (*ExecutionResult, error) {
	// Verify the table exists
	_, ok := e.storage.GetTable(plan.TableName)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Get the HashIndex from storage (used for point lookups)
	// This can fail if the index was dropped between planning and execution (TOCTOU race)
	hashIndex := e.storage.GetIndex(plan.Schema, plan.IndexName)
	if hashIndex == nil && !plan.IsRangeScan {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"index %q not found in schema %q for table %q during execution; "+
					"the index may have been dropped after query planning",
				plan.IndexName, plan.Schema, plan.TableName,
			),
		}
	}

	// For range scans, we need an ART index
	// The ART index is typically created alongside the HashIndex when the index is built
	// For now, we create an ART index from the HashIndex data if needed
	var artIndex *index.ART
	if plan.IsRangeScan {
		// Get or create ART index for range scans
		artIndex = e.getOrCreateARTIndex(plan, hashIndex)
		if artIndex == nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg: fmt.Sprintf(
					"ART index required for range scan on index %q but could not be created",
					plan.IndexName,
				),
			}
		}
	}

	// Create the index scan operator with full configuration
	indexScanOp, err := NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:        plan.TableName,
		Schema:           plan.Schema,
		TableDef:         plan.TableDef,
		IndexName:        plan.IndexName,
		IndexDef:         plan.IndexDef,
		Index:            hashIndex,
		ARTIndex:         artIndex,
		LookupKeys:       plan.LookupKeys,
		IsRangeScan:      plan.IsRangeScan,
		LowerBound:       plan.LowerBound,
		UpperBound:       plan.UpperBound,
		LowerInclusive:   plan.LowerInclusive,
		UpperInclusive:   plan.UpperInclusive,
		RangeColumnIndex: plan.RangeColumnIndex,
		Projections:      plan.Projections,
		IsIndexOnly:      plan.IsIndexOnly,
		Storage:          e.storage,
		Executor:         e,
		Ctx:              ctx,
	})
	if err != nil {
		return nil, err
	}

	outputCols := plan.OutputColumns()

	// Collect results from the index scan
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(outputCols)),
	}

	// Build column names
	for i, col := range outputCols {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	// Fetch all chunks from the index scan
	for {
		chunk, err := indexScanOp.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j, col := range outputCols {
				val := chunk.GetValue(i, j)
				row[result.Columns[j]] = val
				// Also store with qualified name if table is specified
				if col.Table != "" {
					row[col.Table+"."+col.Column] = val
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Apply residual filter if present
	if plan.ResidualFilter != nil {
		filteredRows := make([]map[string]any, 0)
		for _, row := range result.Rows {
			passes, err := e.evaluateExprAsBool(ctx, plan.ResidualFilter, row)
			if err != nil {
				return nil, err
			}
			if passes {
				filteredRows = append(filteredRows, row)
			}
		}
		result.Rows = filteredRows
	}

	return result, nil
}

// getOrCreateARTIndex retrieves or creates an ART index for range scans.
// It creates an ART index from the table data using ALL indexed columns to form
// composite keys. This is necessary because HashIndex uses hashed keys and can't
// support range scans.
//
// For composite indexes (e.g., (category, price)), the key is the concatenation
// of all encoded column values, ensuring unique keys and proper ordering.
func (e *Executor) getOrCreateARTIndex(
	plan *planner.PhysicalIndexScan,
	_ *storage.HashIndex,
) *index.ART {
	// Validate input
	if plan.IndexDef == nil || len(plan.IndexDef.Columns) == 0 || plan.TableDef == nil {
		return nil
	}

	// Find the column indices for ALL index columns (for composite key)
	indexColIndices := make([]int, 0, len(plan.IndexDef.Columns))
	for _, idxColName := range plan.IndexDef.Columns {
		for i, col := range plan.TableDef.Columns {
			if strings.EqualFold(col.Name, idxColName) {
				indexColIndices = append(indexColIndices, i)
				break
			}
		}
	}

	if len(indexColIndices) != len(plan.IndexDef.Columns) {
		return nil // Not all index columns found in table
	}

	// Determine key type from the first column (used for ART encoding hints)
	// For composite indexes, the ART treats the key as a byte sequence
	keyType := dukdb.TYPE_BLOB
	if len(indexColIndices) == 1 {
		// Single column index - use the actual column type
		keyType = plan.TableDef.Columns[indexColIndices[0]].Type
		if keyType == dukdb.TYPE_ANY {
			keyType = dukdb.TYPE_BIGINT
		}
	}

	// Create a new ART index
	art := index.NewART(keyType)

	// Get the table to scan for building the ART
	table, ok := e.storage.GetTable(plan.TableName)
	if !ok {
		return art // Return empty ART if table not found
	}

	// Scan the table and build the ART index using the TableScanner
	// This correctly handles the rowIDMap which may have non-sequential RowIDs
	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Process each row in the chunk
		for i := 0; i < chunk.Count(); i++ {
			// Get the RowID for this row from the scanner
			rowIDPtr := scanner.GetRowID(i)
			if rowIDPtr == nil {
				continue
			}
			rowID := *rowIDPtr

			// Build the composite key from ALL index columns
			var compositeKey []byte
			allColumnsValid := true
			for _, colIdx := range indexColIndices {
				if colIdx >= chunk.ColumnCount() {
					allColumnsValid = false
					break
				}
				keyVal := chunk.GetValue(i, colIdx)
				if keyVal == nil {
					allColumnsValid = false
					break
				}
				encodedPart := encodeKeyValue(keyVal)
				if encodedPart == nil {
					allColumnsValid = false
					break
				}
				compositeKey = append(compositeKey, encodedPart...)
			}

			if allColumnsValid && len(compositeKey) > 0 {
				// Use InsertEncoded since encodeKeyValue already produces
				// properly encoded keys for lexicographic ordering
				_ = art.InsertEncoded(compositeKey, uint64(rowID))
			}
		}
	}

	return art
}

func (e *Executor) executeVirtualTableScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalVirtualTableScan,
) (*ExecutionResult, error) {
	vt := plan.VirtualTable
	if vt == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "virtual table not found",
		}
	}

	// Get the underlying virtual table and scan it
	it, err := vt.VirtualTable().Scan()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"failed to scan virtual table: %v",
				err,
			),
		}
	}
	defer func() {
		_ = it.Close()
	}()

	outputCols := plan.OutputColumns()
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(outputCols)),
	}

	// Build column names
	for i, col := range outputCols {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Iterate over the virtual table
	for it.Next() {
		values := it.Values()
		row := make(map[string]any)

		// Apply projections if specified
		if plan.Projections != nil {
			for i, idx := range plan.Projections {
				if idx < len(values) {
					val := values[idx]
					row[result.Columns[i]] = val
					// Also store with qualified name if table is specified
					if outputCols[i].Table != "" {
						row[outputCols[i].Table+"."+outputCols[i].Column] = val
					}
				}
			}
		} else {
			// No projections - use all columns
			for i := range outputCols {
				if i < len(values) {
					val := values[i]
					row[result.Columns[i]] = val
					// Also store with qualified name if table is specified
					if outputCols[i].Table != "" {
						row[outputCols[i].Table+"."+outputCols[i].Column] = val
					}
				}
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if err := it.Err(); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"error iterating virtual table: %v",
				err,
			),
		}
	}

	return result, nil
}

func (e *Executor) executeFilter(
	ctx *ExecutionContext,
	plan *planner.PhysicalFilter,
) (*ExecutionResult, error) {
	// First execute child, preserving correlated values
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Filter rows
	filteredRows := make([]map[string]any, 0)
	for _, row := range childResult.Rows {
		passes, err := e.evaluateExprAsBool(
			ctx,
			plan.Condition,
			row,
		)
		if err != nil {
			return nil, err
		}
		if passes {
			filteredRows = append(
				filteredRows,
				row,
			)
		}
	}

	return &ExecutionResult{
		Rows:    filteredRows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeProject(
	ctx *ExecutionContext,
	plan *planner.PhysicalProject,
) (*ExecutionResult, error) {
	// Execute child, preserving correlated values
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Build column names
	columns := make(
		[]string,
		len(plan.Expressions),
	)
	for i := range plan.Expressions {
		if i < len(plan.Aliases) &&
			plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Project rows
	projectedRows := make(
		[]map[string]any,
		len(childResult.Rows),
	)
	for i, row := range childResult.Rows {
		projectedRow := make(map[string]any)
		for j, expr := range plan.Expressions {
			// Check if this is an aggregate function call
			// If the alias matches a key in the row, use the pre-computed value
			// This handles the case where aggregates have already been computed
			// by the aggregate operator and we're just projecting the results
			if fn, ok := expr.(*binder.BoundFunctionCall); ok {
				if isAggregateFunc(fn.Name) {
					alias := columns[j]
					if val, exists := row[alias]; exists {
						projectedRow[alias] = val
						continue
					}
				}
			}

			// Check if this is a GROUPING() function call
			// GROUPING() values are pre-computed by the grouping sets aggregate operator
			if _, ok := expr.(*binder.BoundGroupingCall); ok {
				alias := columns[j]
				if val, exists := row[alias]; exists {
					projectedRow[alias] = val
					continue
				}
			}

			val, err := e.evaluateExpr(
				ctx,
				expr,
				row,
			)
			if err != nil {
				return nil, err
			}
			projectedRow[columns[j]] = val
		}
		projectedRows[i] = projectedRow
	}

	return &ExecutionResult{
		Rows:    projectedRows,
		Columns: columns,
	}, nil
}

func (e *Executor) executeHashJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalHashJoin,
) (*ExecutionResult, error) {
	// Execute left and right children, preserving correlated values
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.executeWithContext(ctx, plan.Right)
	if err != nil {
		return nil, err
	}

	// Build hash table from right side (build side)
	// For now, use simple nested loop join
	return e.performJoin(
		ctx,
		leftResult,
		rightResult,
		plan.JoinType,
		plan.Condition,
	)
}

func (e *Executor) executeNestedLoopJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalNestedLoopJoin,
) (*ExecutionResult, error) {
	// Execute left and right children, preserving correlated values
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.executeWithContext(ctx, plan.Right)
	if err != nil {
		return nil, err
	}

	return e.performJoin(
		ctx,
		leftResult,
		rightResult,
		plan.JoinType,
		plan.Condition,
	)
}

func (e *Executor) executeLateralJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalLateralJoin,
) (*ExecutionResult, error) {
	// Execute left side first
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	// Get output columns for result building
	leftCols := plan.Left.OutputColumns()
	rightCols := plan.Right.OutputColumns()

	// Build column names for result
	columns := make([]string, 0, len(leftCols)+len(rightCols))
	for _, col := range leftCols {
		if col.Column != "" {
			columns = append(columns, col.Column)
		} else {
			columns = append(columns, "col")
		}
	}
	for _, col := range rightCols {
		if col.Column != "" {
			columns = append(columns, col.Column)
		} else {
			columns = append(columns, "col")
		}
	}

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	// For each row in the left result, execute the right side
	// LATERAL joins re-evaluate the right side for each left row
	for _, leftRow := range leftResult.Rows {
		// Execute the right plan for this left row
		// The right side can reference columns from the left row through correlation
		// Create a new context with the left row's values as correlated values
		lateralCtx := &ExecutionContext{
			Context:          ctx.Context,
			Args:             ctx.Args,
			CorrelatedValues: leftRow, // Pass left row values for correlated column resolution
		}
		rightResult, err := e.executeWithContext(lateralCtx, plan.Right)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, rightRow := range rightResult.Rows {
			// Combine left and right rows
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}

			// Check condition if present
			if plan.Condition != nil {
				passes, err := e.evaluateExprAsBool(ctx, plan.Condition, combinedRow)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue
				}
			}

			matched = true
			result.Rows = append(result.Rows, combinedRow)
		}

		// Handle LEFT LATERAL: emit left row with NULLs for right if no matches
		if !matched && plan.JoinType == planner.JoinTypeLeft {
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			// Add NULL values for right columns
			for _, col := range rightCols {
				if col.Column != "" {
					combinedRow[col.Column] = nil
				}
				if col.Table != "" && col.Column != "" {
					combinedRow[col.Table+"."+col.Column] = nil
				}
			}
			result.Rows = append(result.Rows, combinedRow)
		}
	}

	return result, nil
}

func (e *Executor) performJoin(
	ctx *ExecutionContext,
	left, right *ExecutionResult,
	joinType planner.JoinType,
	condition interface{},
) (*ExecutionResult, error) {
	// Combine column names
	columns := make(
		[]string,
		0,
		len(left.Columns)+len(right.Columns),
	)
	columns = append(columns, left.Columns...)
	columns = append(columns, right.Columns...)

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	// Track which right rows have been matched (for RIGHT/FULL OUTER joins)
	matchedRight := make(map[int]bool)

	// Nested loop join
	for _, leftRow := range left.Rows {
		matched := false
		for ri, rightRow := range right.Rows {
			// Combine rows
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}

			// Check condition
			if condition != nil {
				passes, err := e.evaluateExprAsBool(
					ctx,
					condition,
					combinedRow,
				)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue
				}
			}

			matched = true
			matchedRight[ri] = true
			// For SEMI join, only output left columns (no right columns)
			if joinType == planner.JoinTypeSemi {
				result.Rows = append(result.Rows, leftRow)
				break // Only need one match for semi-join
			} else {
				result.Rows = append(
					result.Rows,
					combinedRow,
				)
			}
		}

		// Handle left/full outer join - add left row with NULLs for right
		if !matched &&
			(joinType == planner.JoinTypeLeft || joinType == planner.JoinTypeFull) {
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for _, col := range right.Columns {
				combinedRow[col] = nil
			}
			result.Rows = append(
				result.Rows,
				combinedRow,
			)
		}
	}

	// Handle RIGHT/FULL OUTER join - emit unmatched right rows with NULLs for left
	if joinType == planner.JoinTypeRight || joinType == planner.JoinTypeFull {
		for ri, rightRow := range right.Rows {
			if !matchedRight[ri] {
				combinedRow := make(map[string]any)
				for _, col := range left.Columns {
					combinedRow[col] = nil
				}
				for k, v := range rightRow {
					combinedRow[k] = v
				}
				result.Rows = append(result.Rows, combinedRow)
			}
		}
	}

	return result, nil
}

func (e *Executor) executePositionalJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalPositionalJoin,
) (*ExecutionResult, error) {
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.executeWithContext(ctx, plan.Right)
	if err != nil {
		return nil, err
	}

	// Combine column names
	columns := make(
		[]string,
		0,
		len(leftResult.Columns)+len(rightResult.Columns),
	)
	columns = append(columns, leftResult.Columns...)
	columns = append(columns, rightResult.Columns...)

	// Determine max row count
	maxRows := len(leftResult.Rows)
	if len(rightResult.Rows) > maxRows {
		maxRows = len(rightResult.Rows)
	}

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, maxRows),
	}

	for i := 0; i < maxRows; i++ {
		row := make(map[string]any)
		if i < len(leftResult.Rows) {
			for k, v := range leftResult.Rows[i] {
				row[k] = v
			}
		} else {
			for _, col := range leftResult.Columns {
				row[col] = nil
			}
		}
		if i < len(rightResult.Rows) {
			for k, v := range rightResult.Rows[i] {
				row[k] = v
			}
		} else {
			for _, col := range rightResult.Columns {
				row[col] = nil
			}
		}
		result.Rows[i] = row
	}

	return result, nil
}

func (e *Executor) executeAsOfJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalAsOfJoin,
) (*ExecutionResult, error) {
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.executeWithContext(ctx, plan.Right)
	if err != nil {
		return nil, err
	}

	// Combine column names
	columns := make(
		[]string,
		0,
		len(leftResult.Columns)+len(rightResult.Columns),
	)
	columns = append(columns, leftResult.Columns...)
	columns = append(columns, rightResult.Columns...)

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	// For each left row, find the best matching right row.
	// The ASOF join condition typically has equality parts (e.g., ticker = ticker)
	// AND an inequality part (e.g., t.ts >= q.ts).
	// We evaluate the condition for all right rows and pick the one that satisfies
	// the condition AND has the closest value (the last right row that satisfies
	// the condition when right is sorted ascending on the inequality column).
	for _, leftRow := range leftResult.Rows {
		var bestMatch map[string]any

		for _, rightRow := range rightResult.Rows {
			// Combine rows for condition evaluation
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}

			// Check the full condition
			if plan.Condition != nil {
				passes, err := e.evaluateExprAsBool(
					ctx,
					plan.Condition,
					combinedRow,
				)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue
				}
			}

			// Keep the latest matching row (ASOF semantics: closest preceding match)
			bestMatch = combinedRow
		}

		if bestMatch != nil {
			result.Rows = append(result.Rows, bestMatch)
		} else if plan.JoinType == planner.JoinTypeAsOfLeft {
			// LEFT ASOF: emit left row with NULL right side
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for _, col := range rightResult.Columns {
				combinedRow[col] = nil
			}
			result.Rows = append(result.Rows, combinedRow)
		}
	}

	return result, nil
}

func (e *Executor) executeHashAggregate(
	ctx *ExecutionContext,
	plan *planner.PhysicalHashAggregate,
) (*ExecutionResult, error) {
	// Execute child
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Check if we have grouping sets (GROUPING SETS/ROLLUP/CUBE)
	if len(plan.GroupingSets) > 0 {
		return e.executeHashAggregateWithGroupingSets(ctx, plan, childResult)
	}

	// Regular GROUP BY execution
	// Group rows by group-by expressions
	type groupKey string
	groups := make(map[groupKey][]map[string]any)
	groupOrder := make([]groupKey, 0)

	for _, row := range childResult.Rows {
		// Compute group key
		keyParts := make([]any, len(plan.GroupBy))
		for i, expr := range plan.GroupBy {
			val, err := e.evaluateExpr(
				ctx,
				expr,
				row,
			)
			if err != nil {
				return nil, err
			}
			keyParts[i] = val
		}
		key := groupKey(formatGroupKey(keyParts))

		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], row)
	}

	// If no groups and no rows, create a single empty group for aggregate functions
	if len(groups) == 0 &&
		len(plan.GroupBy) == 0 {
		groupOrder = append(groupOrder, "")
		groups[""] = []map[string]any{}
	}

	// Build result columns
	numGroupBy := len(plan.GroupBy)
	numAgg := len(plan.Aggregates)
	columns := make([]string, numGroupBy+numAgg)

	for i := range plan.GroupBy {
		if i < len(plan.Aliases) &&
			plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = "col" + string(rune('0'+i))
		}
	}
	for i := range plan.Aggregates {
		if numGroupBy+i < len(plan.Aliases) &&
			plan.Aliases[numGroupBy+i] != "" {
			columns[numGroupBy+i] = plan.Aliases[numGroupBy+i]
		} else {
			columns[numGroupBy+i] = "col" + string(rune('0'+numGroupBy+i))
		}
	}

	// Compute aggregates for each group
	result := &ExecutionResult{
		Columns: columns,
		Rows: make(
			[]map[string]any,
			len(groupOrder),
		),
	}

	for i, key := range groupOrder {
		groupRows := groups[key]
		row := make(map[string]any)

		// Add group-by values
		if len(groupRows) > 0 {
			for j, expr := range plan.GroupBy {
				val, _ := e.evaluateExpr(
					ctx,
					expr,
					groupRows[0],
				)
				row[columns[j]] = val
			}
		}

		// Compute aggregates
		for j, expr := range plan.Aggregates {
			val, err := e.computeAggregate(
				ctx,
				expr,
				groupRows,
			)
			if err != nil {
				return nil, err
			}
			row[columns[numGroupBy+j]] = val
		}

		result.Rows[i] = row
	}

	return result, nil
}

// executeHashAggregateWithGroupingSets handles GROUPING SETS/ROLLUP/CUBE execution.
// For each grouping set, we:
// 1. Aggregate the data using only columns in that grouping set
// 2. Set columns NOT in the grouping set to NULL
// 3. Compute GROUPING() bitmasks for each row
// 4. UNION ALL the results from all grouping sets
func (e *Executor) executeHashAggregateWithGroupingSets(
	ctx *ExecutionContext,
	plan *planner.PhysicalHashAggregate,
	childResult *ExecutionResult,
) (*ExecutionResult, error) {
	// Build result columns (same structure as regular aggregate)
	numGroupBy := len(plan.GroupBy)
	numAgg := len(plan.Aggregates)
	numGroupingCalls := len(plan.GroupingCalls)
	columns := make([]string, numGroupBy+numAgg+numGroupingCalls)

	for i := range plan.GroupBy {
		if i < len(plan.Aliases) && plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = "col" + string(rune('0'+i))
		}
	}
	for i := range plan.Aggregates {
		idx := numGroupBy + i
		if idx < len(plan.Aliases) && plan.Aliases[idx] != "" {
			columns[idx] = plan.Aliases[idx]
		} else {
			columns[idx] = "col" + string(rune('0'+idx))
		}
	}
	// Add GROUPING() columns
	for i := range plan.GroupingCalls {
		idx := numGroupBy + numAgg + i
		if idx < len(plan.Aliases) && plan.Aliases[idx] != "" {
			columns[idx] = plan.Aliases[idx]
		} else {
			columns[idx] = "GROUPING"
		}
	}

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	// Process each grouping set
	for _, groupingSet := range plan.GroupingSets {
		// Build a set of column keys that are in this grouping set
		inGroupingSet := make(map[string]bool)
		for _, expr := range groupingSet {
			key := getGroupByExprKey(expr)
			inGroupingSet[key] = true
		}

		// Group rows by only the columns in this grouping set
		type groupKey string
		groups := make(map[groupKey][]map[string]any)
		groupOrder := make([]groupKey, 0)

		for _, row := range childResult.Rows {
			// Compute group key using only columns in this grouping set
			keyParts := make([]any, len(groupingSet))
			for i, expr := range groupingSet {
				val, err := e.evaluateExpr(ctx, expr, row)
				if err != nil {
					return nil, err
				}
				keyParts[i] = val
			}
			key := groupKey(formatGroupKey(keyParts))

			if _, exists := groups[key]; !exists {
				groupOrder = append(groupOrder, key)
			}
			groups[key] = append(groups[key], row)
		}

		// If no groups and empty grouping set (grand total), create empty group
		if len(groups) == 0 && len(groupingSet) == 0 {
			groupOrder = append(groupOrder, "")
			groups[""] = childResult.Rows
		}

		// Compute aggregates for each group in this grouping set
		for _, key := range groupOrder {
			groupRows := groups[key]
			row := make(map[string]any)

			// Add group-by values
			// For columns NOT in this grouping set, set to NULL
			if len(groupRows) > 0 {
				for j, expr := range plan.GroupBy {
					exprKey := getGroupByExprKey(expr)
					if inGroupingSet[exprKey] {
						// Column is in this grouping set - use its value
						val, _ := e.evaluateExpr(ctx, expr, groupRows[0])
						row[columns[j]] = val
					} else {
						// Column is NOT in this grouping set - set to NULL
						row[columns[j]] = nil
					}
				}
			} else {
				// Empty group (grand total case)
				for j := range plan.GroupBy {
					row[columns[j]] = nil
				}
			}

			// Compute aggregates
			for j, expr := range plan.Aggregates {
				val, err := e.computeAggregate(ctx, expr, groupRows)
				if err != nil {
					return nil, err
				}
				row[columns[numGroupBy+j]] = val
			}

			// Compute GROUPING() function values
			for j, gc := range plan.GroupingCalls {
				bitmask := e.computeGroupingBitmask(gc, plan.GroupBy, inGroupingSet)
				row[columns[numGroupBy+numAgg+j]] = bitmask
			}

			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// getGroupByExprKey returns a unique key for a GROUP BY expression.
func getGroupByExprKey(expr binder.BoundExpr) string {
	switch e := expr.(type) {
	case *binder.BoundColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Column
		}
		return e.Column
	default:
		return ""
	}
}

// computeGroupingBitmask computes the GROUPING() bitmask for a row.
// For each argument in GROUPING(col1, col2, ...):
// - If the column is in the current grouping set (not aggregated), bit = 0
// - If the column is NOT in the current grouping set (aggregated/NULL), bit = 1
// Bits are ordered with the first argument in the most significant position.
func (e *Executor) computeGroupingBitmask(
	gc *binder.BoundGroupingCall,
	allGroupBy []binder.BoundExpr,
	inGroupingSet map[string]bool,
) int64 {
	var bitmask int64 = 0
	for i, arg := range gc.Args {
		// Check if this column is NOT in the current grouping set
		var key string
		if arg.Table != "" {
			key = arg.Table + "." + arg.Column
		} else {
			key = arg.Column
		}

		if !inGroupingSet[key] {
			// Column is aggregated (NULL) in this grouping set - bit = 1
			bitmask |= 1 << (len(gc.Args) - 1 - i)
		}
		// else: Column is in grouping set (grouped) - bit = 0
	}
	return bitmask
}

func (e *Executor) executeSort(
	ctx *ExecutionContext,
	plan *planner.PhysicalSort,
) (*ExecutionResult, error) {
	// Execute child, preserving correlated values
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Sort rows using insertion sort for simplicity
	rows := childResult.Rows
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			cmp, err := e.compareRows(
				ctx,
				rows[j-1],
				rows[j],
				plan.OrderBy,
			)
			if err != nil {
				return nil, err
			}
			if cmp <= 0 {
				break
			}
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeLimit(
	ctx *ExecutionContext,
	plan *planner.PhysicalLimit,
) (*ExecutionResult, error) {
	// Execute child, preserving correlated values
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	rows := childResult.Rows

	// Determine offset value (static or dynamic)
	offset := plan.Offset
	if plan.OffsetExpr != nil {
		// Evaluate dynamic offset expression using correlated values
		val, err := e.evaluateExpr(ctx, plan.OffsetExpr, ctx.CorrelatedValues)
		if err != nil {
			return nil, err
		}
		if val != nil {
			offset = toInt64Value(val)
		}
	}

	// Apply offset
	if offset > 0 {
		if int(offset) >= len(rows) {
			rows = []map[string]any{}
		} else {
			rows = rows[offset:]
		}
	}

	// Determine limit value (static or dynamic)
	limit := plan.Limit
	if plan.LimitExpr != nil {
		// Evaluate dynamic limit expression using correlated values
		val, err := e.evaluateExpr(ctx, plan.LimitExpr, ctx.CorrelatedValues)
		if err != nil {
			return nil, err
		}
		if val != nil {
			limit = toInt64Value(val)
		}
	}

	// Apply limit
	if limit >= 0 && int(limit) < len(rows) {
		if plan.WithTies && len(plan.OrderBy) > 0 {
			// WITH TIES: include additional rows that tie with the last row at the limit boundary
			lastIdx := int(limit) - 1
			lastRow := rows[lastIdx]
			// Evaluate ORDER BY values for the last row within the limit
			lastOrderVals := make([]any, len(plan.OrderBy))
			for i, ob := range plan.OrderBy {
				val, err := e.evaluateExpr(ctx, ob.Expr, lastRow)
				if err != nil {
					return nil, err
				}
				lastOrderVals[i] = val
			}
			// Extend past the limit while ORDER BY values match (ties)
			endIdx := int(limit)
			for endIdx < len(rows) {
				row := rows[endIdx]
				tied := true
				for i, ob := range plan.OrderBy {
					val, err := e.evaluateExpr(ctx, ob.Expr, row)
					if err != nil {
						return nil, err
					}
					if !valuesEqual(lastOrderVals[i], val) {
						tied = false
						break
					}
				}
				if !tied {
					break
				}
				endIdx++
			}
			rows = rows[:endIdx]
		} else {
			rows = rows[:limit]
		}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeDistinct(
	ctx *ExecutionContext,
	plan *planner.PhysicalDistinct,
) (*ExecutionResult, error) {
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Use a map to track seen rows
	seen := make(map[string]bool)
	distinctRows := make([]map[string]any, 0)

	for _, row := range childResult.Rows {
		// Use the actual keys from the row for uniqueness check
		key := formatRowMap(row)
		if !seen[key] {
			seen[key] = true
			distinctRows = append(
				distinctRows,
				row,
			)
		}
	}

	return &ExecutionResult{
		Rows:    distinctRows,
		Columns: childResult.Columns,
	}, nil
}

// executeDistinctOn implements DISTINCT ON semantics.
// DISTINCT ON (col1, col2) keeps only the first row for each unique combination
// of the specified columns. The ORDER BY clause determines which row is "first".
func (e *Executor) executeDistinctOn(
	ctx *ExecutionContext,
	plan *planner.PhysicalDistinctOn,
) (*ExecutionResult, error) {
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// If there are no DISTINCT ON expressions, return all rows
	if len(plan.DistinctOn) == 0 {
		return childResult, nil
	}

	// Sort by ORDER BY first (if not already sorted upstream)
	// The ORDER BY determines which row is "first" for each DISTINCT ON group
	rows := childResult.Rows
	if len(plan.OrderBy) > 0 {
		// Sort rows using the ORDER BY expressions
		for i := 1; i < len(rows); i++ {
			for j := i; j > 0; j-- {
				cmp, err := e.compareRows(ctx, rows[j-1], rows[j], plan.OrderBy)
				if err != nil {
					return nil, err
				}
				if cmp <= 0 {
					break
				}
				rows[j-1], rows[j] = rows[j], rows[j-1]
			}
		}
	}

	// Use a map to track which DISTINCT ON groups we've seen
	seen := make(map[string]bool)
	distinctRows := make([]map[string]any, 0)

	for _, row := range rows {
		// Build key from DISTINCT ON expressions
		keyParts := make([]any, len(plan.DistinctOn))
		for i, expr := range plan.DistinctOn {
			val, err := e.evaluateExpr(ctx, expr, row)
			if err != nil {
				return nil, err
			}
			keyParts[i] = val
		}
		key := formatGroupKey(keyParts)

		// Keep only the first row for each DISTINCT ON group
		if !seen[key] {
			seen[key] = true
			distinctRows = append(distinctRows, row)
		}
	}

	return &ExecutionResult{
		Rows:    distinctRows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeInsert(
	ctx *ExecutionContext,
	plan *planner.PhysicalInsert,
) (*ExecutionResult, error) {
	// Resolve catalog and storage (may redirect to an attached database)
	_, insertStor, _ := e.resolveSchemaTarget(plan.Schema)

	// Get or create storage table
	table, ok := insertStor.GetTable(plan.Table)
	if !ok {
		// Create table in storage
		var err error
		table, err = insertStor.CreateTable(
			plan.Table,
			plan.TableDef.ColumnTypes(),
		)
		if err != nil {
			return nil, err
		}
	}

	rowsAffected := int64(0) // counts actually inserted rows
	updatedCount := int64(0) // counts rows updated via ON CONFLICT DO UPDATE
	var pkIndices []int
	var pkColumns []*catalog.ColumnDef
	if plan.TableDef != nil && plan.TableDef.HasPrimaryKey() {
		pkIndices = plan.TableDef.PrimaryKey
		pkColumns = plan.TableDef.Columns
	}

	// For ON CONFLICT, we need to map PK keys to RowIDs so we can find
	// existing rows to update. For plain INSERT, we only need existence check.
	hasOnConflict := plan.OnConflict != nil
	var pkKeys map[string]struct{}
	var pkToRowID map[string]storage.RowID
	if len(pkIndices) > 0 {
		if hasOnConflict {
			pkToRowID = loadPrimaryKeyToRowID(table, pkIndices)
			// Also build pkKeys from pkToRowID for consistency
			pkKeys = make(map[string]struct{}, len(pkToRowID))
			for k := range pkToRowID {
				pkKeys[k] = struct{}{}
			}
		} else {
			pkKeys = loadPrimaryKeyKeys(table, pkIndices)
		}
	}

	// Use the ON CONFLICT's conflict column indices if specified, otherwise fall back to PK
	conflictIndices := pkIndices
	if hasOnConflict && len(plan.OnConflict.ConflictColumnIndices) > 0 {
		conflictIndices = plan.OnConflict.ConflictColumnIndices
	}

	// checkPrimaryKey checks for PK conflicts. Returns:
	// - (false, nil) if no conflict (row should be inserted normally)
	// - (true, nil) if conflict handled by ON CONFLICT (row should be skipped)
	// - (false, err) on error
	checkPrimaryKey := func(values []any) (bool, error) {
		if len(pkIndices) == 0 {
			return false, nil
		}
		pkValues, hasNull := extractPrimaryKeyValues(values, pkIndices)
		detail := formatPrimaryKeyDetail(
			pkValues,
			pkIndices,
			pkColumns,
		)
		if hasNull {
			return false, constraintErrorf(
				"NULL constraint violation on primary key (%s)",
				detail,
			)
		}
		key := primaryKeyKey(pkValues)
		if _, exists := pkKeys[key]; exists {
			// Conflict detected
			if !hasOnConflict {
				return false, constraintErrorf(
					"duplicate key \"%s\" violates primary key constraint",
					detail,
				)
			}

			// Handle ON CONFLICT
			switch plan.OnConflict.Action {
			case parser.OnConflictDoNothing:
				// Skip this row
				return true, nil
			case parser.OnConflictDoUpdate:
				// Find the existing row and update it
				existingRowID, rowIDExists := pkToRowID[key]
				if !rowIDExists {
					return false, constraintErrorf(
						"duplicate key \"%s\" violates primary key constraint (row not found for update)",
						detail,
					)
				}

				existingRow := table.GetRow(existingRowID)
				if existingRow == nil {
					return false, constraintErrorf(
						"duplicate key \"%s\" violates primary key constraint (row deleted)",
						detail,
					)
				}

				// Build a row map with both existing values and EXCLUDED values for expression evaluation
				rowMap := make(map[string]any)
				for i, col := range plan.TableDef.Columns {
					rowMap[col.Name] = existingRow[i]
					// Add EXCLUDED pseudo-table values (the values being inserted)
					if i < len(values) {
						rowMap["EXCLUDED."+col.Name] = values[i]
					}
				}

				// Check optional WHERE clause for DO UPDATE
				if plan.OnConflict.UpdateWhere != nil {
					whereVal, err := e.evaluateExpr(ctx, plan.OnConflict.UpdateWhere, rowMap)
					if err != nil {
						return false, err
					}
					// If WHERE evaluates to false/nil, skip the update (like DO NOTHING for this row)
					if whereVal == nil || whereVal == false {
						return true, nil
					}
					if boolVal, ok := whereVal.(bool); ok && !boolVal {
						return true, nil
					}
				}

				// Apply SET clause expressions
				columnValues := make(map[int]any)
				for _, sc := range plan.OnConflict.UpdateSet {
					val, err := e.evaluateExpr(ctx, sc.Value, rowMap)
					if err != nil {
						return false, err
					}
					columnValues[sc.ColumnIdx] = val
				}

				// Update the existing row in storage
				if err := table.UpdateRows([]storage.RowID{existingRowID}, columnValues); err != nil {
					return false, fmt.Errorf("failed to update row on conflict: %w", err)
				}

				// Count as an affected row (updated, not inserted)
				updatedCount++
				return true, nil // skip inserting (we updated instead)
			}
		}
		pkKeys[key] = struct{}{}
		return false, nil
	}
	_ = conflictIndices // used only for semantic validation above

	// Build UNIQUE constraint checkers from table constraints
	type uniqueChecker struct {
		name    string
		indices []int
		keys    map[string]struct{}
	}
	var uniqueCheckers []uniqueChecker
	if plan.TableDef != nil {
		for _, c := range plan.TableDef.Constraints {
			uc, ok := c.(*catalog.UniqueConstraintDef)
			if !ok {
				continue
			}
			indices := make([]int, len(uc.Columns))
			for i, colName := range uc.Columns {
				for j, col := range plan.TableDef.Columns {
					if strings.EqualFold(col.Name, colName) {
						indices[i] = j
						break
					}
				}
			}
			// Load existing keys from table
			existingKeys := loadPrimaryKeyKeys(table, indices)
			name := uc.Name
			if name == "" {
				name = strings.Join(uc.Columns, ", ")
			}
			uniqueCheckers = append(uniqueCheckers, uniqueChecker{
				name:    name,
				indices: indices,
				keys:    existingKeys,
			})
		}
	}

	// checkUniqueConstraints checks all UNIQUE constraints for a row
	checkUniqueConstraints := func(values []any) error {
		for i := range uniqueCheckers {
			ck := &uniqueCheckers[i]
			keyVals := make([]any, len(ck.indices))
			hasNull := false
			for j, idx := range ck.indices {
				if idx < len(values) {
					keyVals[j] = values[idx]
				}
				if keyVals[j] == nil {
					hasNull = true
				}
			}
			// NULL values do not violate UNIQUE per SQL standard
			if hasNull {
				continue
			}
			key := primaryKeyKey(keyVals)
			if _, exists := ck.keys[key]; exists {
				return constraintErrorf(
					"duplicate key violates unique constraint on (%s)",
					ck.name,
				)
			}
			ck.keys[key] = struct{}{}
		}
		return nil
	}

	// Build CHECK constraint evaluators from table constraints
	type checkEvaluator struct {
		name string
		expr string
	}
	var checkEvaluators []checkEvaluator
	if plan.TableDef != nil {
		for _, c := range plan.TableDef.Constraints {
			cc, ok := c.(*catalog.CheckConstraintDef)
			if !ok || cc.Expression == "" {
				continue
			}
			name := cc.Name
			if name == "" {
				name = cc.Expression
			}
			checkEvaluators = append(checkEvaluators, checkEvaluator{
				name: name,
				expr: cc.Expression,
			})
		}
	}

	// checkCheckConstraints evaluates all CHECK constraints for a row
	checkCheckConstraints := func(values []any) error {
		if len(checkEvaluators) == 0 {
			return nil
		}
		// Build row map for expression evaluation
		rowMap := make(map[string]any)
		if plan.TableDef != nil {
			for i, col := range plan.TableDef.Columns {
				if i < len(values) {
					rowMap[col.Name] = values[i]
					// Also add lowercase version for case-insensitive matching
					rowMap[strings.ToLower(col.Name)] = values[i]
				}
			}
		}
		for _, ce := range checkEvaluators {
			// Parse the CHECK expression by wrapping in SELECT
			wrapSQL := "SELECT " + ce.expr
			stmt, err := parser.Parse(wrapSQL)
			if err != nil {
				continue // Skip unparseable expressions
			}
			selectStmt, ok := stmt.(*parser.SelectStmt)
			if !ok || len(selectStmt.Columns) == 0 {
				continue
			}
			// Evaluate the expression using the row values
			result, err := e.evaluateExpr(ctx, selectStmt.Columns[0].Expr, rowMap)
			if err != nil {
				continue // Skip expressions that fail to evaluate
			}
			// NULL result passes CHECK per SQL standard
			if result == nil {
				continue
			}
			// Check if result is false
			switch v := result.(type) {
			case bool:
				if !v {
					return constraintErrorf(
						"CHECK constraint violation: %s",
						ce.name,
					)
				}
			}
		}
		return nil
	}

	// Get column types for DataChunk creation
	columnTypes := table.ColumnTypes()

	// Batch size is StandardVectorSize (2048) for DuckDB compatibility
	batchSize := storage.StandardVectorSize

	// Collect all inserted values for WAL logging and RETURNING clause
	var allInsertedValues [][]any

	// Track RowIDs for undo support - capture the starting RowID before insert
	startRowID := table.NextRowID()

	// Helper function to flush a DataChunk to the table
	flushChunk := func(chunk *storage.DataChunk) (int, error) {
		if chunk.Count() == 0 {
			return 0, nil
		}
		count, err := table.InsertChunk(chunk)
		if err != nil {
			return 0, err
		}
		return count, nil
	}

	if plan.Source != nil {
		// INSERT ... SELECT with DataChunk batching
		sourceResult, err := e.Execute(
			ctx.Context,
			plan.Source,
			ctx.Args,
		)
		if err != nil {
			return nil, err
		}

		// Create initial chunk for batching
		currentChunk := storage.NewDataChunkWithCapacity(columnTypes, batchSize)

		for _, row := range sourceResult.Rows {
			values := make(
				[]any,
				len(plan.TableDef.Columns),
			)
			specifiedCols := make(map[int]bool, len(plan.Columns))
			for i, col := range sourceResult.Columns {
				values[plan.Columns[i]] = row[col]
				specifiedCols[plan.Columns[i]] = true
			}

			// Fill in default values for unspecified columns
			for i, col := range plan.TableDef.Columns {
				if !specifiedCols[i] && col.HasDefault {
					val, err := e.resolveDefaultValue(ctx, col)
					if err != nil {
						return nil, fmt.Errorf("evaluating default for column %q: %w", col.Name, err)
					}
					values[i] = val
				}
			}

			// Evaluate generated column expressions
			if err := e.evaluateGeneratedColumns(ctx, plan.TableDef, values); err != nil {
				return nil, err
			}

			// Check NOT NULL constraints
			for i, col := range plan.TableDef.Columns {
				if !col.Nullable && values[i] == nil {
					isPK := false
					for _, pkIdx := range pkIndices {
						if pkIdx == i {
							isPK = true
							break
						}
					}
					if !isPK {
						return nil, constraintErrorf(
							"NOT NULL constraint failed: column %q does not allow NULL values",
							col.Name,
						)
					}
				}
			}
			conflictHandled, err := checkPrimaryKey(values)
			if err != nil {
				return nil, err
			}
			if conflictHandled {
				continue // Row was handled by ON CONFLICT (skipped or updated)
			}

			// Check UNIQUE constraints
			if err := checkUniqueConstraints(values); err != nil {
				return nil, err
			}

			// Check CHECK constraints
			if err := checkCheckConstraints(values); err != nil {
				return nil, err
			}

			// Check foreign key constraints
			if plan.TableDef != nil {
				colNames := make([]string, len(plan.TableDef.Columns))
				for i, col := range plan.TableDef.Columns {
					colNames[i] = col.Name
				}
				if err := e.checkForeignKeys(plan.TableDef, values, colNames); err != nil {
					return nil, err
				}
			}

			// Collect values for WAL logging and RETURNING
			valuesCopy := make([]any, len(values))
			copy(valuesCopy, values)
			allInsertedValues = append(allInsertedValues, valuesCopy)

			// Append to current chunk
			currentChunk.AppendRow(values)

			// Flush when chunk is full (reached batch size)
			if currentChunk.Count() >= batchSize {
				count, err := flushChunk(currentChunk)
				if err != nil {
					return nil, err
				}
				rowsAffected += int64(count)
				// Create new chunk for next batch
				currentChunk = storage.NewDataChunkWithCapacity(columnTypes, batchSize)
			}
		}

		// Flush remaining rows in the final chunk
		if currentChunk.Count() > 0 {
			count, err := flushChunk(currentChunk)
			if err != nil {
				return nil, err
			}
			rowsAffected += int64(count)
		}
	} else {
		// INSERT ... VALUES with DataChunk batching
		// Create initial chunk for batching
		currentChunk := storage.NewDataChunkWithCapacity(columnTypes, batchSize)

		for _, valueRow := range plan.Values {
			if len(valueRow) != len(plan.Columns) {
				return nil, fmt.Errorf(
					"column count mismatch: expected %d values, got %d",
					len(plan.Columns),
					len(valueRow),
				)
			}

			// Evaluate each expression in the row
			values := make([]any, len(plan.TableDef.Columns))
			specifiedCols := make(map[int]bool, len(plan.Columns))
			for i, expr := range valueRow {
				val, err := e.evaluateExpr(ctx, expr, nil)
				if err != nil {
					return nil, err
				}
				values[plan.Columns[i]] = val
				specifiedCols[plan.Columns[i]] = true
			}

			// Fill in default values for unspecified columns
			for i, col := range plan.TableDef.Columns {
				if !specifiedCols[i] && col.HasDefault {
					val, err := e.resolveDefaultValue(ctx, col)
					if err != nil {
						return nil, fmt.Errorf("evaluating default for column %q: %w", col.Name, err)
					}
					values[i] = val
				}
			}

			// Evaluate generated column expressions
			if err := e.evaluateGeneratedColumns(ctx, plan.TableDef, values); err != nil {
				return nil, err
			}

			// Check NOT NULL constraints
			for i, col := range plan.TableDef.Columns {
				if !col.Nullable && values[i] == nil {
					// Check if this is a PK column (PK null check is done separately)
					isPK := false
					for _, pkIdx := range pkIndices {
						if pkIdx == i {
							isPK = true
							break
						}
					}
					if !isPK {
						return nil, constraintErrorf(
							"NOT NULL constraint failed: column %q does not allow NULL values",
							col.Name,
						)
					}
				}
			}
			conflictHandled, err := checkPrimaryKey(values)
			if err != nil {
				return nil, err
			}
			if conflictHandled {
				continue // Row was handled by ON CONFLICT (skipped or updated)
			}

			// Check UNIQUE constraints
			if err := checkUniqueConstraints(values); err != nil {
				return nil, err
			}

			// Check CHECK constraints
			if err := checkCheckConstraints(values); err != nil {
				return nil, err
			}

			// Check foreign key constraints
			if plan.TableDef != nil {
				colNames := make([]string, len(plan.TableDef.Columns))
				for i, col := range plan.TableDef.Columns {
					colNames[i] = col.Name
				}
				if err := e.checkForeignKeys(plan.TableDef, values, colNames); err != nil {
					return nil, err
				}
			}

			// Collect values for WAL logging and RETURNING
			valuesCopy := make([]any, len(values))
			copy(valuesCopy, values)
			allInsertedValues = append(allInsertedValues, valuesCopy)

			// Append to current chunk
			currentChunk.AppendRow(values)

			// Flush when chunk is full (reached batch size of 2048 rows)
			if currentChunk.Count() >= batchSize {
				count, err := flushChunk(currentChunk)
				if err != nil {
					return nil, err
				}
				rowsAffected += int64(count)
				// Create new chunk for next batch
				currentChunk = storage.NewDataChunkWithCapacity(columnTypes, batchSize)
			}
		}

		// Flush remaining rows in the final chunk
		if currentChunk.Count() > 0 {
			count, err := flushChunk(currentChunk)
			if err != nil {
				return nil, err
			}
			rowsAffected += int64(count)
		}
	}

	// Record undo operation for transaction rollback
	// Calculate the RowIDs that were inserted (from startRowID to current nextRowID)
	if rowsAffected > 0 {
		insertedRowIDs := make([]uint64, rowsAffected)
		for i := int64(0); i < rowsAffected; i++ {
			insertedRowIDs[i] = startRowID + uint64(i)
		}
		e.recordUndo(UndoOperation{
			TableName: plan.Table,
			OpType:    UndoInsert,
			RowIDs:    insertedRowIDs,
		})

		// Update all indexes on this table with the newly inserted rows
		if err := e.updateIndexesForInsert(plan.Table, plan.TableDef, allInsertedValues, startRowID); err != nil {
			return nil, fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Total affected rows = inserted rows + rows updated via ON CONFLICT DO UPDATE
	totalAffected := rowsAffected + updatedCount

	// WAL logging: Log INSERT entry AFTER successful insertion
	// This ensures atomicity - if the insert fails, no WAL entry is written
	if e.wal != nil && rowsAffected > 0 {
		schema := "main" // Default schema
		if plan.TableDef != nil && plan.TableDef.Schema != "" {
			schema = plan.TableDef.Schema
		}
		// Use unique WAL transaction ID for each auto-committed statement
		walTxnID := e.wal.NextTxnID()
		// Write TxnBegin entry
		beginEntry := wal.NewTxnBeginEntry(walTxnID, e.wal.Clock().Now())
		if err := e.wal.WriteEntry(beginEntry); err != nil {
			return nil, fmt.Errorf("WAL TxnBegin append failed: %w", err)
		}
		// Write INSERT entry
		entry := wal.NewInsertEntry(walTxnID, schema, plan.Table, allInsertedValues)
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
		// Write TxnCommit entry
		commitEntry := wal.NewTxnCommitEntry(walTxnID, e.wal.Clock().Now())
		if err := e.wal.WriteEntry(commitEntry); err != nil {
			return nil, fmt.Errorf("WAL TxnCommit append failed: %w", err)
		}
	}

	if totalAffected > 0 {
		e.invalidateQueryCache(plan.Table)
	}

	// Handle RETURNING clause
	if len(plan.Returning) > 0 {
		return e.evaluateReturning(ctx, plan.Returning, allInsertedValues, plan.TableDef)
	}

	return &ExecutionResult{
		RowsAffected: totalAffected,
	}, nil
}

// loadPrimaryKeyToRowID builds a map from primary key string to RowID.
// This is used for ON CONFLICT DO UPDATE to find existing rows.
func loadPrimaryKeyToRowID(
	table *storage.Table,
	indices []int,
) map[string]storage.RowID {
	result := make(map[string]storage.RowID)
	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}
		for row := 0; row < chunk.Count(); row++ {
			pkValues := make([]any, len(indices))
			for i, idx := range indices {
				pkValues[i] = chunk.GetValue(row, idx)
			}
			key := primaryKeyKey(pkValues)
			if rowID := scanner.GetRowID(row); rowID != nil {
				result[key] = *rowID
			}
		}
	}
	return result
}

// updateIndexesForInsert updates all indexes on a table with newly inserted rows.
// tableName: the name of the table
// tableDef: the table definition with column info
// values: the inserted row values (each []any corresponds to one row)
// startRowID: the RowID of the first inserted row
func (e *Executor) updateIndexesForInsert(
	tableName string,
	tableDef *catalog.TableDef,
	values [][]any,
	startRowID uint64,
) error {
	// Get the schema (default to "main")
	schema := "main"
	if tableDef != nil && tableDef.Schema != "" {
		schema = tableDef.Schema
	}

	// Get all indexes for this table
	indexes := e.storage.GetIndexesForTable(schema, tableName)
	if len(indexes) == 0 {
		return nil
	}

	// Build column name to index mapping for efficient lookup
	colNameToIdx := make(map[string]int)
	if tableDef != nil {
		for i, col := range tableDef.Columns {
			colNameToIdx[strings.ToLower(col.Name)] = i
		}
	}

	// For each index, update with all inserted rows
	for _, idx := range indexes {
		// Find column indices for this index
		colIndices := make([]int, len(idx.Columns))
		for i, colName := range idx.Columns {
			colIdx, ok := colNameToIdx[strings.ToLower(colName)]
			if !ok {
				// Column not found - skip this index
				continue
			}
			colIndices[i] = colIdx
		}

		// Insert each row into the index
		for rowOffset, rowValues := range values {
			// Build the key from indexed columns
			key := make([]any, len(colIndices))
			for i, colIdx := range colIndices {
				if colIdx < len(rowValues) {
					key[i] = rowValues[colIdx]
				}
			}

			// Calculate RowID for this row
			rowID := storage.RowID(startRowID + uint64(rowOffset))

			// Insert into index
			if err := idx.Insert(key, rowID); err != nil {
				return fmt.Errorf("failed to insert into index %s: %w", idx.Name, err)
			}
		}
	}

	return nil
}

// resolveSchemaTarget resolves a schema name to its catalog and storage.
// When the schema name matches an attached database name, the attached
// database's catalog and storage are returned with the schema rewritten to
// "main".  Otherwise the executor's own catalog and storage are returned
// unchanged.
func (e *Executor) resolveSchemaTarget(schema string) (cat *catalog.Catalog, stor *storage.Storage, resolvedSchema string) {
	if e.dbManager != nil && schema != "" && schema != "main" && schema != "temp" {
		if attachedCat, attachedStor, ok := e.dbManager.GetAttached(schema); ok {
			return attachedCat, attachedStor, "main"
		}
	}
	return e.catalog, e.storage, schema
}

func (e *Executor) executeCreateTable(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateTable,
) (*ExecutionResult, error) {
	// OR REPLACE and IF NOT EXISTS are mutually exclusive
	if plan.OrReplace && plan.IfNotExists {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "cannot use both OR REPLACE and IF NOT EXISTS",
		}
	}

	// TEMPORARY: override schema to temp
	schema := plan.Schema
	if plan.Temporary {
		schema = "temp"
		// Auto-create temp schema if needed
		if _, ok := e.catalog.GetSchema(schema); !ok {
			if _, err := e.catalog.CreateSchema(schema); err != nil {
				return nil, err
			}
		}
	}

	// Resolve schema to attached database catalog/storage if needed
	cat, stor, schema := e.resolveSchemaTarget(schema)

	// OR REPLACE: drop existing table first
	if plan.OrReplace {
		_, exists := cat.GetTableInSchema(schema, plan.Table)
		if exists {
			// Drop from storage first
			if err := stor.DropTable(plan.Table); err != nil &&
				err != dukdb.ErrTableNotFound {
				return nil, err
			}
			// Drop from catalog
			if err := cat.DropTableInSchema(schema, plan.Table); err != nil {
				return nil, err
			}
		}
	}

	// Check if table already exists
	_, exists := cat.GetTableInSchema(
		schema,
		plan.Table,
	)
	if exists {
		if plan.IfNotExists {
			return &ExecutionResult{
				RowsAffected: 0,
			}, nil
		}

		return nil, dukdb.ErrTableAlreadyExists
	}

	// Create table definition
	tableDef := catalog.NewTableDef(
		plan.Table,
		plan.Columns,
	)
	if len(plan.PrimaryKey) > 0 {
		if err := tableDef.SetPrimaryKey(plan.PrimaryKey); err != nil {
			return nil, err
		}
	}

	// Store constraints on the table definition
	if len(plan.Constraints) > 0 {
		tableDef.Constraints = plan.Constraints
	}

	// Validate foreign key constraints
	for _, c := range plan.Constraints {
		fk, ok := c.(*catalog.ForeignKeyConstraintDef)
		if !ok {
			continue
		}
		// Validate parent table exists
		parentDef, exists := cat.GetTableInSchema(schema, fk.RefTable)
		if !exists {
			// Try default schema
			parentDef, exists = cat.GetTable(fk.RefTable)
			if !exists {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeConstraint,
					Msg:  fmt.Sprintf("foreign key constraint references non-existent table %q", fk.RefTable),
				}
			}
		}
		// Validate referenced columns exist in parent
		for _, refCol := range fk.RefColumns {
			found := false
			for _, col := range parentDef.Columns {
				if strings.EqualFold(col.Name, refCol) {
					found = true
					break
				}
			}
			if !found {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeConstraint,
					Msg:  fmt.Sprintf("foreign key references non-existent column %q in table %q", refCol, fk.RefTable),
				}
			}
		}
		// Validate referenced columns form PK or UNIQUE constraint
		if !isKeyOrUnique(parentDef, fk.RefColumns) {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeConstraint,
				Msg:  fmt.Sprintf("foreign key references columns that are not a PRIMARY KEY or UNIQUE constraint on table %q", fk.RefTable),
			}
		}
	}

	// Add to catalog
	if err := cat.CreateTableInSchema(schema, tableDef); err != nil {
		return nil, err
	}

	// Create in storage
	types := make([]dukdb.Type, len(plan.Columns))
	for i, col := range plan.Columns {
		types[i] = col.Type
	}
	if _, err := stor.CreateTable(plan.Table, types); err != nil {
		// Rollback catalog change
		_ = cat.DropTableInSchema(
			schema,
			plan.Table,
		)

		return nil, err
	}

	// Write WAL entry for CREATE TABLE (only for the main database WAL)
	if e.wal != nil && cat == e.catalog {
		columns := make([]wal.ColumnDef, len(plan.Columns))
		for i, col := range plan.Columns {
			columns[i] = wal.ColumnDef{
				Name:            col.Name,
				Type:            col.Type,
				Nullable:        col.Nullable,
				HasDefault:      col.HasDefault,
				DefaultExprText: col.DefaultExprText,
			}
		}
		entry := &wal.CreateTableEntry{
			Schema:  schema,
			Name:    plan.Table,
			Columns: columns,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			// Rollback catalog and storage changes
			_ = cat.DropTableInSchema(schema, plan.Table)
			_ = stor.DropTable(plan.Table)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	e.invalidateQueryCache(plan.Table)

	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeDropTable(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropTable,
) (*ExecutionResult, error) {
	// Check if table exists
	_, exists := e.catalog.GetTableInSchema(
		plan.Schema,
		plan.Table,
	)
	if !exists {
		if plan.IfExists {
			return &ExecutionResult{
				RowsAffected: 0,
			}, nil
		}

		return nil, dukdb.ErrTableNotFound
	}

	// Check if any views depend on this table
	dependentViews := e.catalog.GetViewsDependingOnTable(plan.Schema, plan.Table)
	if len(dependentViews) > 0 {
		// Build list of view names for error message
		viewNames := make([]string, len(dependentViews))
		for i, v := range dependentViews {
			viewNames[i] = v.Name
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg: fmt.Sprintf(
				"cannot drop table %s: referenced by view(s) %s",
				plan.Table,
				strings.Join(viewNames, ", "),
			),
		}
	}

	// Drop from storage
	if err := e.storage.DropTable(plan.Table); err != nil &&
		err != dukdb.ErrTableNotFound {
		return nil, err
	}

	// Drop from catalog
	if err := e.catalog.DropTableInSchema(plan.Schema, plan.Table); err != nil {
		return nil, err
	}

	// Write WAL entry for DROP TABLE
	if e.wal != nil {
		entry := &wal.DropTableEntry{
			Schema: plan.Schema,
			Name:   plan.Table,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	e.invalidateQueryCache(plan.Table)

	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeDummyScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalDummyScan,
) (*ExecutionResult, error) {
	// Return a single empty row for queries without FROM
	return &ExecutionResult{
		Rows:    []map[string]any{{}},
		Columns: []string{},
	}, nil
}

func (e *Executor) executeValues(
	ctx *ExecutionContext,
	plan *planner.PhysicalValues,
) (*ExecutionResult, error) {
	// Build column names from the plan
	columns := make([]string, len(plan.Columns))
	for i, col := range plan.Columns {
		columns[i] = col.Column
	}

	// Evaluate each row's expressions
	var rows []map[string]any
	for _, boundRow := range plan.Rows {
		row := make(map[string]any)
		for colIdx, expr := range boundRow {
			val, err := e.evaluateExpr(ctx, expr, nil)
			if err != nil {
				return nil, fmt.Errorf("error evaluating VALUES expression: %w", err)
			}
			row[columns[colIdx]] = val
		}
		rows = append(rows, row)
	}

	return &ExecutionResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func (e *Executor) executeBegin(
	ctx *ExecutionContext,
	plan *planner.PhysicalBegin,
) (*ExecutionResult, error) {
	// BEGIN is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeCommit(
	ctx *ExecutionContext,
	plan *planner.PhysicalCommit,
) (*ExecutionResult, error) {
	// COMMIT is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeRollback(
	ctx *ExecutionContext,
	plan *planner.PhysicalRollback,
) (*ExecutionResult, error) {
	// ROLLBACK is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeWindow executes a window function operator.
// Window functions are blocking operators that materialize all input,
// partition/sort, evaluate window functions, and emit results.
func (e *Executor) executeWindow(
	ctx *ExecutionContext,
	plan *planner.PhysicalWindow,
) (*ExecutionResult, error) {
	// First execute child
	childResult, err := e.executeWithContext(ctx, plan.Child)
	if err != nil {
		return nil, err
	}

	// Get child columns from the child plan
	childColumns := plan.Child.OutputColumns()

	// Create the window executor
	// We need to create a PhysicalOperator for the child that yields our rows
	childOp := &resultSetOperator{
		rows:    childResult.Rows,
		columns: childColumns,
		index:   0,
	}

	windowExec, err := NewPhysicalWindowExecutor(
		plan,
		childOp,
		childColumns,
		e,
		ctx,
	)
	if err != nil {
		return nil, err
	}

	// Collect results
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, 0),
	}

	// Build column names: child columns + window result columns
	for _, col := range childColumns {
		if col.Column != "" {
			result.Columns = append(result.Columns, col.Column)
		} else {
			result.Columns = append(result.Columns, "col")
		}
	}
	for _, windowExpr := range plan.WindowExprs {
		result.Columns = append(result.Columns, windowExpr.FunctionName)
	}

	// Collect all chunks from the window executor
	for {
		chunk, err := windowExec.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j := 0; j < len(result.Columns); j++ {
				row[result.Columns[j]] = chunk.GetValue(i, j)
			}
			// Also store window results under their aliases for QUALIFY clause support.
			// The QUALIFY clause may reference window functions by their alias
			// (e.g., "QUALIFY rn <= 2" where rn is an alias for ROW_NUMBER()).
			numChildCols := len(childColumns)
			for j, windowExpr := range plan.WindowExprs {
				if windowExpr.Alias != "" && windowExpr.Alias != windowExpr.FunctionName {
					// Store the window result under the alias as well
					row[windowExpr.Alias] = chunk.GetValue(i, numChildCols+j)
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// resultSetOperator wraps an ExecutionResult to provide PhysicalOperator interface.
// This allows us to feed the results of child execution into the window executor.
type resultSetOperator struct {
	rows    []map[string]any
	columns []planner.ColumnBinding
	index   int
}

func (r *resultSetOperator) Next() (*storage.DataChunk, error) {
	if r.index >= len(r.rows) {
		return nil, nil
	}

	// Create types for the chunk
	types := make([]dukdb.Type, len(r.columns))
	for i, col := range r.columns {
		types[i] = col.Type
	}

	// Create chunk with capacity for remaining rows
	remaining := len(r.rows) - r.index
	chunkSize := storage.StandardVectorSize
	if remaining < chunkSize {
		chunkSize = remaining
	}

	chunk := storage.NewDataChunkWithCapacity(types, chunkSize)

	// Add rows to chunk
	for i := 0; i < chunkSize && r.index < len(r.rows); i++ {
		row := r.rows[r.index]
		values := make([]any, len(r.columns))
		for j, col := range r.columns {
			// Try column name first
			if val, ok := row[col.Column]; ok {
				values[j] = val
			} else if val, ok := row[col.Table+"."+col.Column]; ok {
				values[j] = val
			} else {
				values[j] = nil
			}
		}
		chunk.AppendRow(values)
		r.index++
	}

	return chunk, nil
}

func (r *resultSetOperator) GetTypes() []dukdb.TypeInfo {
	types := make([]dukdb.TypeInfo, len(r.columns))
	for i, col := range r.columns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}
	return types
}

// Helper functions

func formatGroupKey(values []any) string {
	if len(values) == 0 {
		return ""
	}
	result := ""
	for i, v := range values {
		if i > 0 {
			result += "|"
		}
		result += formatValue(v)
	}

	return result
}

func formatRowMap(row map[string]any) string {
	// Sort keys for consistent ordering
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort for consistency
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	result := ""
	for i, k := range keys {
		if i > 0 {
			result += "|"
		}
		result += k + ":" + formatValue(row[k])
	}

	return result
}

func formatValue(v any) string {
	if v == nil {
		return "<null>"
	}

	return fmt.Sprintf("%v", v)
}

func constraintErrorf(
	format string,
	args ...any,
) error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeConstraint,
		Msg: fmt.Sprintf(
			"Constraint Error: "+format,
			args...,
		),
	}
}

func loadPrimaryKeyKeys(
	table *storage.Table,
	indices []int,
) map[string]struct{} {
	keys := make(map[string]struct{})
	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}
		for row := 0; row < chunk.Count(); row++ {
			pkValues := make([]any, len(indices))
			for i, idx := range indices {
				pkValues[i] = chunk.GetValue(row, idx)
			}
			keys[primaryKeyKey(pkValues)] = struct{}{}
		}
	}

	return keys
}

func extractPrimaryKeyValues(
	values []any,
	indices []int,
) ([]any, bool) {
	pkValues := make([]any, len(indices))
	hasNull := false
	for i, idx := range indices {
		if idx < 0 || idx >= len(values) {
			pkValues[i] = nil
			hasNull = true

			continue
		}
		val := values[idx]
		if val == nil {
			hasNull = true
		}
		pkValues[i] = val
	}

	return pkValues, hasNull
}

func primaryKeyKey(
	values []any,
) string {
	return fmt.Sprintf("%#v", values)
}

func formatPrimaryKeyDetail(
	values []any,
	indices []int,
	columns []*catalog.ColumnDef,
) string {
	parts := make([]string, len(indices))
	for i, idx := range indices {
		name := fmt.Sprintf("col%d", idx)
		if idx >= 0 && idx < len(columns) {
			if columns[idx].Name != "" {
				name = columns[idx].Name
			}
		}
		var val any
		if i < len(values) {
			val = values[i]
		}
		parts[i] = fmt.Sprintf(
			"%s: %s",
			name,
			formatValue(val),
		)
	}

	return strings.Join(parts, ", ")
}

// evaluateReturning evaluates the RETURNING clause expressions for affected rows.
// It takes the RETURNING column expressions, the affected row values, and the table definition.
// Returns an ExecutionResult with the projected columns from the RETURNING clause.
func (e *Executor) evaluateReturning(
	ctx *ExecutionContext,
	returning []*binder.BoundSelectColumn,
	affectedRows [][]any,
	tableDef *catalog.TableDef,
) (*ExecutionResult, error) {
	if len(returning) == 0 || len(affectedRows) == 0 {
		return &ExecutionResult{
			RowsAffected: int64(len(affectedRows)),
		}, nil
	}

	// Build column names for result
	columns := make([]string, len(returning))
	for i, col := range returning {
		if col.Alias != "" {
			columns[i] = col.Alias
		} else if colRef, ok := col.Expr.(*binder.BoundColumnRef); ok {
			columns[i] = colRef.Column
		} else {
			columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	// Build rows for result
	resultRows := make([]map[string]any, 0, len(affectedRows))

	for _, rowValues := range affectedRows {
		// Convert row values to a map for expression evaluation
		rowMap := make(map[string]any)
		if tableDef != nil {
			for j, col := range tableDef.Columns {
				if j < len(rowValues) {
					rowMap[col.Name] = rowValues[j]
					// Also add with table name prefix for qualified references
					rowMap[tableDef.Name+"."+col.Name] = rowValues[j]
				}
			}
		}

		// Evaluate each RETURNING expression
		resultRow := make(map[string]any)
		for i, col := range returning {
			val, err := e.evaluateExpr(ctx, col.Expr, rowMap)
			if err != nil {
				return nil, err
			}
			resultRow[columns[i]] = val
		}
		resultRows = append(resultRows, resultRow)
	}

	return &ExecutionResult{
		Rows:         resultRows,
		Columns:      columns,
		RowsAffected: int64(len(resultRows)),
	}, nil
}

// executeSetOp executes a set operation (UNION, INTERSECT, EXCEPT).
func (e *Executor) executeSetOp(
	ctx *ExecutionContext,
	plan *planner.PhysicalSetOp,
) (*ExecutionResult, error) {
	// Execute left side
	leftResult, err := e.executeWithContext(ctx, plan.Left)
	if err != nil {
		return nil, err
	}

	// Execute right side
	rightResult, err := e.executeWithContext(ctx, plan.Right)
	if err != nil {
		return nil, err
	}

	// Get output columns from left side (both sides have the same structure)
	columns := leftResult.Columns

	switch plan.OpType {
	case planner.SetOpUnionAll:
		// UNION ALL: simply concatenate all rows from both sides
		// This preserves all duplicates
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0, len(leftResult.Rows)+len(rightResult.Rows)),
		}
		result.Rows = append(result.Rows, leftResult.Rows...)
		result.Rows = append(result.Rows, rightResult.Rows...)
		return result, nil

	case planner.SetOpUnion:
		// UNION: concatenate and remove duplicates
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0),
		}
		seen := make(map[string]struct{})

		// Add rows from left side
		for _, row := range leftResult.Rows {
			key := formatRowForSetOp(row, columns)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				result.Rows = append(result.Rows, row)
			}
		}

		// Add rows from right side (only if not already seen)
		for _, row := range rightResult.Rows {
			key := formatRowForSetOp(row, columns)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				result.Rows = append(result.Rows, row)
			}
		}
		return result, nil

	case planner.SetOpIntersect:
		// INTERSECT: rows that appear in both (no duplicates)
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0),
		}

		// Build a set of rows from the right side
		rightSet := make(map[string]struct{})
		for _, row := range rightResult.Rows {
			key := formatRowForSetOp(row, columns)
			rightSet[key] = struct{}{}
		}

		// Keep left rows that exist in right (no duplicates)
		seen := make(map[string]struct{})
		for _, row := range leftResult.Rows {
			key := formatRowForSetOp(row, columns)
			if _, existsInRight := rightSet[key]; existsInRight {
				if _, alreadySeen := seen[key]; !alreadySeen {
					seen[key] = struct{}{}
					result.Rows = append(result.Rows, row)
				}
			}
		}
		return result, nil

	case planner.SetOpIntersectAll:
		// INTERSECT ALL: rows that appear in both (with duplicate counting)
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0),
		}

		// Count occurrences in right side
		rightCounts := make(map[string]int)
		for _, row := range rightResult.Rows {
			key := formatRowForSetOp(row, columns)
			rightCounts[key]++
		}

		// For each left row, include it if it has remaining count in right
		for _, row := range leftResult.Rows {
			key := formatRowForSetOp(row, columns)
			if rightCounts[key] > 0 {
				rightCounts[key]--
				result.Rows = append(result.Rows, row)
			}
		}
		return result, nil

	case planner.SetOpExcept:
		// EXCEPT: rows in left but not in right (no duplicates)
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0),
		}

		// Build a set of rows from the right side
		rightSet := make(map[string]struct{})
		for _, row := range rightResult.Rows {
			key := formatRowForSetOp(row, columns)
			rightSet[key] = struct{}{}
		}

		// Keep left rows that don't exist in right (no duplicates)
		seen := make(map[string]struct{})
		for _, row := range leftResult.Rows {
			key := formatRowForSetOp(row, columns)
			if _, existsInRight := rightSet[key]; !existsInRight {
				if _, alreadySeen := seen[key]; !alreadySeen {
					seen[key] = struct{}{}
					result.Rows = append(result.Rows, row)
				}
			}
		}
		return result, nil

	case planner.SetOpExceptAll:
		// EXCEPT ALL: rows in left but not in right (with duplicate counting)
		result := &ExecutionResult{
			Columns: columns,
			Rows:    make([]map[string]any, 0),
		}

		// Count occurrences in right side
		rightCounts := make(map[string]int)
		for _, row := range rightResult.Rows {
			key := formatRowForSetOp(row, columns)
			rightCounts[key]++
		}

		// For each left row, include it if right doesn't have remaining count
		for _, row := range leftResult.Rows {
			key := formatRowForSetOp(row, columns)
			if rightCounts[key] > 0 {
				// Right side still has this row, don't include and decrement
				rightCounts[key]--
			} else {
				// Right side doesn't have this row (or ran out), include it
				result.Rows = append(result.Rows, row)
			}
		}
		return result, nil

	case planner.SetOpUnionAllByName:
		return e.executeUnionByName(leftResult, rightResult, false)

	case planner.SetOpUnionByName:
		return e.executeUnionByName(leftResult, rightResult, true)

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "unsupported set operation type",
		}
	}
}

// executeUnionByName executes UNION [ALL] BY NAME with column name matching.
// Output columns are left columns in order, then right-only columns in order.
// Missing columns are filled with NULL.
func (e *Executor) executeUnionByName(
	leftResult, rightResult *ExecutionResult,
	distinct bool,
) (*ExecutionResult, error) {
	leftCols := leftResult.Columns
	rightCols := rightResult.Columns

	// Build column name -> index maps (case-insensitive)
	leftColMap := make(map[string]int, len(leftCols))
	for i, col := range leftCols {
		leftColMap[strings.ToLower(col)] = i
	}

	rightColMap := make(map[string]int, len(rightCols))
	for i, col := range rightCols {
		rightColMap[strings.ToLower(col)] = i
	}

	// Output columns: left columns in order, then right-only columns in order
	outputCols := make([]string, 0, len(leftCols)+len(rightCols))
	outputCols = append(outputCols, leftCols...)
	for _, col := range rightCols {
		if _, exists := leftColMap[strings.ToLower(col)]; !exists {
			outputCols = append(outputCols, col)
		}
	}

	// Build mappings: for each output column, index in left/right result (-1 = NULL)
	leftMapping := make([]int, len(outputCols))
	rightMapping := make([]int, len(outputCols))
	for i, col := range outputCols {
		colLower := strings.ToLower(col)
		if idx, ok := leftColMap[colLower]; ok {
			leftMapping[i] = idx
		} else {
			leftMapping[i] = -1
		}
		if idx, ok := rightColMap[colLower]; ok {
			rightMapping[i] = idx
		} else {
			rightMapping[i] = -1
		}
	}

	// Remap rows from both sides
	rows := make([]map[string]any, 0, len(leftResult.Rows)+len(rightResult.Rows))

	for _, row := range leftResult.Rows {
		newRow := make(map[string]any, len(outputCols))
		for i, col := range outputCols {
			if leftMapping[i] >= 0 {
				newRow[col] = row[leftCols[leftMapping[i]]]
			} else {
				newRow[col] = nil
			}
		}
		rows = append(rows, newRow)
	}

	for _, row := range rightResult.Rows {
		newRow := make(map[string]any, len(outputCols))
		for i, col := range outputCols {
			if rightMapping[i] >= 0 {
				newRow[col] = row[rightCols[rightMapping[i]]]
			} else {
				newRow[col] = nil
			}
		}
		rows = append(rows, newRow)
	}

	// Apply distinct if needed (UNION BY NAME vs UNION ALL BY NAME)
	if distinct {
		seen := make(map[string]struct{})
		distinctRows := make([]map[string]any, 0)
		for _, row := range rows {
			key := formatRowForSetOp(row, outputCols)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				distinctRows = append(distinctRows, row)
			}
		}
		rows = distinctRows
	}

	return &ExecutionResult{
		Columns: outputCols,
		Rows:    rows,
	}, nil
}

// formatRowForSetOp creates a string key for a row that can be used for set comparison.
// It uses only the columns specified in the column list to ensure consistent comparison.
func formatRowForSetOp(row map[string]any, columns []string) string {
	result := ""
	for i, col := range columns {
		if i > 0 {
			result += "|"
		}
		result += formatValue(row[col])
	}
	return result
}

// isKeyOrUnique checks whether the given columns match the primary key or a UNIQUE constraint on the table.
func isKeyOrUnique(tableDef *catalog.TableDef, columns []string) bool {
	// Check if columns match the primary key
	if len(tableDef.PrimaryKey) > 0 && len(columns) == len(tableDef.PrimaryKey) {
		match := true
		for i, pkIdx := range tableDef.PrimaryKey {
			if pkIdx < len(tableDef.Columns) {
				if !strings.EqualFold(tableDef.Columns[pkIdx].Name, columns[i]) {
					match = false
					break
				}
			}
		}
		if match {
			return true
		}
	}
	// Check if columns match a UNIQUE constraint
	for _, c := range tableDef.Constraints {
		uc, ok := c.(*catalog.UniqueConstraintDef)
		if !ok {
			continue
		}
		if len(uc.Columns) != len(columns) {
			continue
		}
		match := true
		for i := range columns {
			if !strings.EqualFold(uc.Columns[i], columns[i]) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
