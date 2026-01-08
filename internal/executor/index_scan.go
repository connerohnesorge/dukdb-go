package executor

import (
	"math"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/storage/index"
)

// PhysicalIndexScanOperator performs table access via index lookup.
// It uses a HashIndex to find RowIDs matching the lookup keys,
// then fetches the corresponding rows from the table.
// For range scans, it uses an ART index with RangeScan().
type PhysicalIndexScanOperator struct {
	// Table metadata
	tableName string
	schema    string
	tableDef  *catalog.TableDef

	// Index metadata
	indexName string
	indexDef  *catalog.IndexDef
	index     *storage.HashIndex // The actual index for point lookups
	artIndex  *index.ART         // The ART index for range scans (optional)

	// Lookup configuration for point lookups
	lookupKeys []binder.BoundExpr // Key expressions to evaluate for lookup
	columns    []planner.ColumnBinding

	// Range scan configuration (for <, >, <=, >=, BETWEEN predicates)
	isRangeScan      bool             // True if using range scan instead of point lookup
	lowerBound       binder.BoundExpr // Lower bound expression (nil = unbounded)
	upperBound       binder.BoundExpr // Upper bound expression (nil = unbounded)
	lowerInclusive   bool             // True if lower bound is inclusive (>=)
	upperInclusive   bool             // True if upper bound is inclusive (<=)
	rangeColumnIndex int              // Index of range column in composite index

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

// IndexScanConfig holds the configuration for creating a PhysicalIndexScanOperator.
// This struct groups all the parameters needed to construct the operator.
type IndexScanConfig struct {
	// Table metadata
	TableName string
	Schema    string
	TableDef  *catalog.TableDef

	// Index metadata
	IndexName string
	IndexDef  *catalog.IndexDef
	Index     *storage.HashIndex // For point lookups
	ARTIndex  *index.ART         // For range scans (optional)

	// Point lookup configuration
	LookupKeys []binder.BoundExpr

	// Range scan configuration
	IsRangeScan      bool
	LowerBound       binder.BoundExpr
	UpperBound       binder.BoundExpr
	LowerInclusive   bool
	UpperInclusive   bool
	RangeColumnIndex int

	// Projections (nil = all columns)
	Projections []int
	IsIndexOnly bool

	// Execution environment
	Storage  *storage.Storage
	Executor *Executor
	Ctx      *ExecutionContext
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
	hashIndex *storage.HashIndex,
	lookupKeys []binder.BoundExpr,
	projections []int,
	isIndexOnly bool,
	stor *storage.Storage,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalIndexScanOperator, error) {
	return NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:   tableName,
		Schema:      schema,
		TableDef:    tableDef,
		IndexName:   indexName,
		IndexDef:    indexDef,
		Index:       hashIndex,
		LookupKeys:  lookupKeys,
		Projections: projections,
		IsIndexOnly: isIndexOnly,
		Storage:     stor,
		Executor:    executor,
		Ctx:         ctx,
	})
}

// NewPhysicalIndexScanOperatorWithConfig creates a new PhysicalIndexScanOperator with full configuration.
// This constructor supports both point lookups and range scans.
func NewPhysicalIndexScanOperatorWithConfig(cfg IndexScanConfig) (*PhysicalIndexScanOperator, error) {
	tableName := cfg.TableName
	tableDef := cfg.TableDef
	projections := cfg.Projections
	stor := cfg.Storage
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
		tableName:        tableName,
		schema:           cfg.Schema,
		tableDef:         tableDef,
		indexName:        cfg.IndexName,
		indexDef:         cfg.IndexDef,
		index:            cfg.Index,
		artIndex:         cfg.ARTIndex,
		lookupKeys:       cfg.LookupKeys,
		columns:          columns,
		isRangeScan:      cfg.IsRangeScan,
		lowerBound:       cfg.LowerBound,
		upperBound:       cfg.UpperBound,
		lowerInclusive:   cfg.LowerInclusive,
		upperInclusive:   cfg.UpperInclusive,
		rangeColumnIndex: cfg.RangeColumnIndex,
		projections:      projections,
		isIndexOnly:      cfg.IsIndexOnly,
		storage:          stor,
		executor:         cfg.Executor,
		ctx:              cfg.Ctx,
		table:            table,
		types:            types,
		rowIDs:           nil,
		currentIndex:     0,
		exhausted:        false,
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
// For point lookups, it uses the HashIndex.Lookup() method.
// For range scans, it uses the ART.RangeScan() method.
// Returns the RowIDs matching the lookup criteria.
func (op *PhysicalIndexScanOperator) performIndexLookup() ([]storage.RowID, error) {
	// Handle range scans with ART index
	if op.isRangeScan {
		return op.performRangeScan()
	}

	// Point lookup path - use HashIndex
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

// performRangeScan performs a range scan using the ART index.
// It evaluates the lower and upper bound expressions, encodes them as keys,
// and uses the ART iterator to collect all matching RowIDs.
func (op *PhysicalIndexScanOperator) performRangeScan() ([]storage.RowID, error) {
	if op.artIndex == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "range scan requires an ART index but none was provided",
		}
	}

	// Evaluate and encode lower bound
	var lowerKey []byte
	if op.lowerBound != nil {
		val, err := op.executor.evaluateExpr(op.ctx, op.lowerBound, nil)
		if err != nil {
			return nil, err
		}
		lowerKey = encodeKeyValue(val)
	}

	// Evaluate and encode upper bound
	var upperKey []byte
	if op.upperBound != nil {
		val, err := op.executor.evaluateExpr(op.ctx, op.upperBound, nil)
		if err != nil {
			return nil, err
		}
		upperKey = encodeKeyValue(val)
	}

	// Configure range scan options
	opts := index.RangeScanOptions{
		LowerInclusive: op.lowerInclusive,
		UpperInclusive: op.upperInclusive,
	}

	// Perform the range scan
	it := op.artIndex.RangeScan(lowerKey, upperKey, opts)
	defer it.Close()

	// Collect all matching RowIDs
	var rowIDs []storage.RowID
	for {
		_, value, ok := it.Next()
		if !ok {
			break
		}
		// The ART stores RowIDs as uint64 values
		if rowID, ok := value.(uint64); ok {
			rowIDs = append(rowIDs, storage.RowID(rowID))
		} else if rowID, ok := value.(storage.RowID); ok {
			rowIDs = append(rowIDs, rowID)
		}
	}

	return rowIDs, nil
}

// encodeKeyValue encodes a value for use as an ART key.
// This function converts Go values to byte arrays suitable for ART comparisons.
// The encoding ensures proper lexicographic ordering for range scans.
func encodeKeyValue(val any) []byte {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case int:
		return encodeInt64(int64(v))
	case int8:
		return encodeInt64(int64(v))
	case int16:
		return encodeInt64(int64(v))
	case int32:
		return encodeInt64(int64(v))
	case int64:
		return encodeInt64(v)
	case uint:
		return encodeUint64(uint64(v))
	case uint8:
		return encodeUint64(uint64(v))
	case uint16:
		return encodeUint64(uint64(v))
	case uint32:
		return encodeUint64(uint64(v))
	case uint64:
		return encodeUint64(v)
	case float32:
		return encodeFloat64(float64(v))
	case float64:
		return encodeFloat64(v)
	case string:
		// Strings are already in proper lexicographic order
		return []byte(v)
	case []byte:
		return v
	default:
		// For unsupported types, return nil (will match nothing)
		return nil
	}
}

// encodeInt64 encodes an int64 as 8 bytes in big-endian format.
// It flips the sign bit to ensure proper lexicographic ordering
// (negative numbers sort before positive numbers).
func encodeInt64(v int64) []byte {
	// Flip the sign bit so that negative numbers sort before positive
	u := uint64(v) ^ (1 << 63)
	return encodeUint64(u)
}

// encodeUint64 encodes a uint64 as 8 bytes in big-endian format.
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
	return buf
}

// encodeFloat64 encodes a float64 as 8 bytes for lexicographic ordering.
// This encoding ensures that -3.0 < -2.0 < -1.0 < 0.0 < 1.0 < 2.0 < 3.0
// when comparing the byte arrays lexicographically.
func encodeFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	// If the sign bit is set (negative), flip all bits
	// If the sign bit is not set (positive), flip only the sign bit
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	return encodeUint64(bits)
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

// IsRangeScan returns true if this operator performs a range scan.
// Range scans use the ART index with lower/upper bounds.
func (op *PhysicalIndexScanOperator) IsRangeScan() bool {
	return op.isRangeScan
}

// GetARTIndex returns the ART index used for range scans.
// Returns nil if this operator uses point lookups only.
func (op *PhysicalIndexScanOperator) GetARTIndex() *index.ART {
	return op.artIndex
}

// SetARTIndex sets the ART index for range scans.
// This allows the executor to provide the ART index at execution time.
func (op *PhysicalIndexScanOperator) SetARTIndex(art *index.ART) {
	op.artIndex = art
}
