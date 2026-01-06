package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalIndexScanOperator performs table access via index lookup.
// It uses a HashIndex to find RowIDs matching the lookup keys,
// then fetches the corresponding rows from the table.
type PhysicalIndexScanOperator struct {
	// Table metadata
	tableName string
	schema    string
	tableDef  *catalog.TableDef

	// Index metadata
	indexName string
	indexDef  *catalog.IndexDef
	index     *storage.HashIndex // The actual index for lookups

	// Lookup configuration
	lookupKeys []binder.BoundExpr // Key expressions to evaluate for lookup
	columns    []planner.ColumnBinding

	// Projections specifies which columns to return (nil = all columns)
	projections []int

	// IsIndexOnly indicates if we can satisfy the query from index alone
	// (covering index - index contains all needed columns)
	// Note: Current HashIndex only stores RowIDs, so true index-only scan
	// is not yet possible. This field is for future use.
	isIndexOnly bool

	// Execution state
	storage  *storage.Storage
	executor *Executor
	ctx      *ExecutionContext
	table    *storage.Table
	types    []dukdb.TypeInfo

	// State for iteration
	rowIDs       []storage.RowID // RowIDs from index lookup
	currentIndex int             // Current position in rowIDs
	exhausted    bool            // True when all rows have been returned
}

// NewPhysicalIndexScanOperator creates a new PhysicalIndexScanOperator.
//
// Parameters:
//   - tableName: Name of the table to scan
//   - schema: Schema containing the table
//   - tableDef: Table definition from catalog
//   - indexName: Name of the index to use
//   - indexDef: Index definition from catalog
//   - index: The actual HashIndex instance for lookups
//   - lookupKeys: Expressions that evaluate to the lookup key values
//   - projections: Column indices to project (nil for all columns)
//   - isIndexOnly: Whether this can be an index-only scan (future use)
//   - stor: Storage layer for table access
//   - executor: Executor for expression evaluation
//   - ctx: Execution context
func NewPhysicalIndexScanOperator(
	tableName string,
	schema string,
	tableDef *catalog.TableDef,
	indexName string,
	indexDef *catalog.IndexDef,
	index *storage.HashIndex,
	lookupKeys []binder.BoundExpr,
	projections []int,
	isIndexOnly bool,
	stor *storage.Storage,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalIndexScanOperator, error) {
	// Get the table from storage
	table, ok := stor.GetTable(tableName)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Build column bindings for output
	var columns []planner.ColumnBinding
	if projections != nil {
		columns = make([]planner.ColumnBinding, len(projections))
		for i, idx := range projections {
			if idx >= 0 && idx < len(tableDef.Columns) {
				col := tableDef.Columns[idx]
				columns[i] = planner.ColumnBinding{
					Table:     tableName,
					Column:    col.Name,
					Type:      col.Type,
					ColumnIdx: idx,
				}
			}
		}
	} else {
		columns = make([]planner.ColumnBinding, len(tableDef.Columns))
		for i, col := range tableDef.Columns {
			columns[i] = planner.ColumnBinding{
				Table:     tableName,
				Column:    col.Name,
				Type:      col.Type,
				ColumnIdx: i,
			}
		}
	}

	// Build TypeInfo for output columns
	types := make([]dukdb.TypeInfo, len(columns))
	for i, col := range columns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}

	return &PhysicalIndexScanOperator{
		tableName:    tableName,
		schema:       schema,
		tableDef:     tableDef,
		indexName:    indexName,
		indexDef:     indexDef,
		index:        index,
		lookupKeys:   lookupKeys,
		columns:      columns,
		projections:  projections,
		isIndexOnly:  isIndexOnly,
		storage:      stor,
		executor:     executor,
		ctx:          ctx,
		table:        table,
		types:        types,
		rowIDs:       nil,
		currentIndex: 0,
		exhausted:    false,
	}, nil
}

// Next returns the next DataChunk of results from the index scan.
// On first call, it evaluates lookup keys and performs the index lookup.
// Subsequent calls return batches of rows until all matching rows are returned.
// Returns nil when there are no more results.
//
// Index-Only Scan Execution Path:
// When isIndexOnly is true, this indicates a covering index scenario where all
// required columns could theoretically be retrieved from the index without
// accessing the main table heap. However, the current HashIndex implementation
// only stores RowIDs, not actual column values.
//
// Current behavior with isIndexOnly=true:
// - The RowID filtering benefit is still realized (reduced heap access)
// - Future optimization: Extend HashIndex to store column values for true
//   index-only scans that avoid heap access entirely
//
// The isIndexOnly flag is set by the optimizer when it detects a covering index
// using the IsCoveringIndex() function from the optimizer package.
func (op *PhysicalIndexScanOperator) Next() (*storage.DataChunk, error) {
	// If we've already exhausted all rows, return nil
	if op.exhausted {
		return nil, nil
	}

	// On first call, perform the index lookup
	if op.rowIDs == nil {
		rowIDs, err := op.performIndexLookup()
		if err != nil {
			return nil, err
		}
		op.rowIDs = rowIDs
		op.currentIndex = 0

		// If no matches, we're done
		if len(op.rowIDs) == 0 {
			op.exhausted = true
			return nil, nil
		}
	}

	// Index-Only Scan Path (Future Optimization):
	// When isIndexOnly is true and the index stores column values (future work),
	// we would return data directly from the index here:
	//
	// if op.isIndexOnly && op.index.HasStoredValues() {
	//     return op.fetchFromIndexOnly()
	// }
	//
	// For now, we fall through to fetch from the table even when isIndexOnly
	// is true, because HashIndex only stores RowIDs. The benefit of having
	// the flag is that we can still leverage the RowID list for efficient
	// filtering, and the cost model can make better decisions about index usage.

	// Determine column types for the chunk
	var chunkTypes []dukdb.Type
	if op.projections != nil {
		chunkTypes = make([]dukdb.Type, len(op.projections))
		for i, idx := range op.projections {
			chunkTypes[i] = op.table.ColumnTypes()[idx]
		}
	} else {
		chunkTypes = op.table.ColumnTypes()
	}

	// Create output chunk
	chunk := storage.NewDataChunkWithCapacity(chunkTypes, storage.StandardVectorSize)

	// Fetch rows by RowID until chunk is full or we run out of rows
	rowsAdded := 0
	for op.currentIndex < len(op.rowIDs) && rowsAdded < storage.StandardVectorSize {
		rowID := op.rowIDs[op.currentIndex]
		op.currentIndex++

		// Fetch the row from the table
		rowValues := op.table.GetRow(rowID)
		if rowValues == nil {
			// Row was deleted or doesn't exist - skip it
			continue
		}

		// Apply projection if specified
		var outputValues []any
		if op.projections != nil {
			outputValues = make([]any, len(op.projections))
			for i, idx := range op.projections {
				if idx < len(rowValues) {
					outputValues[i] = rowValues[idx]
				}
			}
		} else {
			outputValues = rowValues
		}

		chunk.AppendRow(outputValues)
		rowsAdded++
	}

	// Check if we've exhausted all rows
	if op.currentIndex >= len(op.rowIDs) {
		op.exhausted = true
	}

	// If no rows were added, return nil (all remaining rows were deleted)
	if rowsAdded == 0 {
		op.exhausted = true
		return nil, nil
	}

	return chunk, nil
}

// performIndexLookup evaluates the lookup key expressions and performs the index lookup.
// Returns the RowIDs matching the lookup key.
func (op *PhysicalIndexScanOperator) performIndexLookup() ([]storage.RowID, error) {
	// Check if we have an index
	if op.index == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "index scan requires an index but none was provided",
		}
	}

	// Evaluate lookup key expressions to get the key values
	keyValues := make([]any, len(op.lookupKeys))
	for i, keyExpr := range op.lookupKeys {
		// Evaluate the expression - use nil row since lookup keys should be constants
		// or parameters, not column references
		val, err := op.executor.evaluateExpr(op.ctx, keyExpr, nil)
		if err != nil {
			return nil, err
		}
		keyValues[i] = val
	}

	// Perform the index lookup
	rowIDs := op.index.Lookup(keyValues)

	return rowIDs, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalIndexScanOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// OutputColumns returns the column bindings for the output of this operator.
func (op *PhysicalIndexScanOperator) OutputColumns() []planner.ColumnBinding {
	return op.columns
}

// Reset resets the operator state for re-execution.
func (op *PhysicalIndexScanOperator) Reset() {
	op.rowIDs = nil
	op.currentIndex = 0
	op.exhausted = false
}

// IsIndexOnlyScan returns true if this is an index-only scan (covering index).
// When true, all required columns are present in the index columns.
//
// Note: Currently, even when this returns true, we still access the heap table
// because HashIndex only stores RowIDs, not column values. This method is useful
// for cost estimation and future optimization when indexes store column values.
func (op *PhysicalIndexScanOperator) IsIndexOnlyScan() bool {
	return op.isIndexOnly
}

// GetIndexName returns the name of the index being used for this scan.
func (op *PhysicalIndexScanOperator) GetIndexName() string {
	return op.indexName
}

// GetMatchedRowCount returns the number of rows matched by the index lookup.
// Returns -1 if the lookup hasn't been performed yet.
func (op *PhysicalIndexScanOperator) GetMatchedRowCount() int {
	if op.rowIDs == nil {
		return -1
	}
	return len(op.rowIDs)
}
