package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalScanOperator implements the PhysicalOperator interface for table scans.
// It reads data from the storage layer and produces DataChunks.
type PhysicalScanOperator struct {
	plan    *planner.PhysicalScan
	storage *storage.Storage
	scanner *storage.TableScanner
	types   []dukdb.TypeInfo
}

// NewPhysicalScanOperator creates a new PhysicalScanOperator.
func NewPhysicalScanOperator(
	plan *planner.PhysicalScan,
	stor *storage.Storage,
) (*PhysicalScanOperator, error) {
	// Get the table from storage
	table, ok := stor.GetTable(plan.TableName)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Create a scanner for the table
	scanner := table.Scan()

	// Get TypeInfo for output columns
	outputCols := plan.OutputColumns()
	types := make(
		[]dukdb.TypeInfo,
		len(outputCols),
	)
	for i, col := range outputCols {
		if col.ColumnIdx >= 0 &&
			col.ColumnIdx < len(
				plan.TableDef.Columns,
			) {
			colDef := plan.TableDef.Columns[col.ColumnIdx]
			types[i] = colDef.GetTypeInfo()
		} else {
			// Fallback: create basic TypeInfo from Type
			info, err := dukdb.NewTypeInfo(col.Type)
			if err != nil {
				// Use a basic wrapper if TypeInfo creation fails
				types[i] = &basicTypeInfo{typ: col.Type}
			} else {
				types[i] = info
			}
		}
	}

	return &PhysicalScanOperator{
		plan:    plan,
		storage: stor,
		scanner: scanner,
		types:   types,
	}, nil
}

// Next returns the next DataChunk of results, or nil if no more data.
func (op *PhysicalScanOperator) Next() (*storage.DataChunk, error) {
	// Get the next chunk from the scanner
	chunk := op.scanner.Next()
	if chunk == nil {
		// No more data
		return nil, nil
	}

	// If there are projections, we need to select only specific columns
	if op.plan.Projections != nil {
		// Create a new chunk with only the projected columns
		projectedTypes := make(
			[]dukdb.Type,
			len(op.plan.Projections),
		)
		for i, colIdx := range op.plan.Projections {
			projectedTypes[i] = chunk.Types()[colIdx]
		}

		projectedChunk := storage.NewDataChunkWithCapacity(
			projectedTypes,
			chunk.Count(),
		)

		// Copy projected columns
		for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
			values := make(
				[]any,
				len(op.plan.Projections),
			)
			for i, colIdx := range op.plan.Projections {
				values[i] = chunk.GetValue(
					rowIdx,
					colIdx,
				)
			}
			projectedChunk.AppendRow(values)
		}

		return projectedChunk, nil
	}

	// Return the chunk as-is (all columns)
	return chunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalScanOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
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
