package executor

import (
	"sort"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalSortOperator implements the PhysicalOperator interface for sorting.
// It is a blocking operator that consumes all input from the child, sorts it,
// and then produces sorted output chunks.
type PhysicalSortOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	orderBy      []*binder.BoundOrderBy
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo
	finished     bool
	sorted       []*storage.DataChunk
	chunkIdx     int
}

// NewPhysicalSortOperator creates a new PhysicalSortOperator.
func NewPhysicalSortOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	orderBy []*binder.BoundOrderBy,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalSortOperator, error) {
	// Get types from child
	types := child.GetTypes()

	return &PhysicalSortOperator{
		child:        child,
		childColumns: childColumns,
		orderBy:      orderBy,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		finished:     false,
		sorted:       nil,
		chunkIdx:     0,
	}, nil
}

// Next returns the next sorted DataChunk, or nil if no more data.
// On the first call, it consumes all input, sorts it, and prepares output chunks.
func (op *PhysicalSortOperator) Next() (*storage.DataChunk, error) {
	// If not yet sorted, collect and sort all input
	if !op.finished {
		if err := op.collectAndSort(); err != nil {
			return nil, err
		}
		op.finished = true
	}

	// Return the next sorted chunk
	if op.chunkIdx >= len(op.sorted) {
		return nil, nil
	}

	chunk := op.sorted[op.chunkIdx]
	op.chunkIdx++

	return chunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalSortOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// collectAndSort collects all input chunks, sorts all rows, and creates output chunks.
func (op *PhysicalSortOperator) collectAndSort() error {
	// Collect all rows from child operator
	var allRows []rowWithData
	var columnTypes []dukdb.Type

	for {
		inputChunk, err := op.child.Next()
		if err != nil {
			return err
		}
		if inputChunk == nil {
			// No more input
			break
		}

		// Record column types from first chunk
		if columnTypes == nil {
			columnTypes = inputChunk.Types()
		}

		// Extract rows from chunk
		for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
			rowData := make(
				[]any,
				inputChunk.ColumnCount(),
			)
			for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
				rowData[colIdx] = inputChunk.GetValue(
					rowIdx,
					colIdx,
				)
			}
			allRows = append(
				allRows,
				rowWithData{data: rowData},
			)
		}
	}

	// If no rows, return empty
	if len(allRows) == 0 {
		op.sorted = []*storage.DataChunk{}

		return nil
	}

	// Sort the rows using Go's sort.Slice
	sort.Slice(allRows, func(i, j int) bool {
		cmp, err := op.compareRowData(
			allRows[i].data,
			allRows[j].data,
		)
		if err != nil {
			// In case of error, maintain original order
			return false
		}

		return cmp < 0
	})

	// Re-chunk sorted data into output DataChunks
	// Use standard chunk size (e.g., 2048 rows per chunk)
	const chunkSize = 2048
	numChunks := (len(allRows) + chunkSize - 1) / chunkSize
	op.sorted = make(
		[]*storage.DataChunk,
		0,
		numChunks,
	)

	for i := 0; i < len(allRows); i += chunkSize {
		end := i + chunkSize
		if end > len(allRows) {
			end = len(allRows)
		}

		chunk := storage.NewDataChunkWithCapacity(
			columnTypes,
			end-i,
		)
		for j := i; j < end; j++ {
			chunk.AppendRow(allRows[j].data)
		}
		op.sorted = append(op.sorted, chunk)
	}

	return nil
}

// compareRowData compares two rows using ORDER BY expressions.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func (op *PhysicalSortOperator) compareRowData(
	a, b []any,
) (int, error) {
	// Build row maps for expression evaluation
	rowA := op.buildRowMap(a)
	rowB := op.buildRowMap(b)

	for _, order := range op.orderBy {
		valA, err := op.executor.evaluateExpr(
			op.ctx,
			order.Expr,
			rowA,
		)
		if err != nil {
			return 0, err
		}
		valB, err := op.executor.evaluateExpr(
			op.ctx,
			order.Expr,
			rowB,
		)
		if err != nil {
			return 0, err
		}

		cmp, isNull := compareOrderByValues(valA, valB, order.NullsFirst, order.Desc, order.Collation)
		if cmp != 0 {
			if order.Desc && !isNull {
				return -cmp, nil
			}

			return cmp, nil
		}
	}

	return 0, nil
}

// buildRowMap builds a map from column indices for expression evaluation.
func (op *PhysicalSortOperator) buildRowMap(
	data []any,
) map[string]any {
	rowMap := make(map[string]any)

	if len(op.childColumns) > 0 {
		// Use column bindings if available
		for colIdx := 0; colIdx < len(data) && colIdx < len(op.childColumns); colIdx++ {
			value := data[colIdx]
			col := op.childColumns[colIdx]

			// Add column by simple name
			if col.Column != "" {
				rowMap[col.Column] = value
			}

			// Add column by table-qualified name
			if col.Table != "" &&
				col.Column != "" {
				qualifiedName := col.Table + "." + col.Column
				rowMap[qualifiedName] = value
			}
		}
	} else {
		// Fallback: use column indices as string keys
		for colIdx := range len(data) {
			value := data[colIdx]
			rowMap[string(rune('0'+colIdx))] = value
		}
	}

	return rowMap
}

// rowWithData holds a row's data for sorting.
type rowWithData struct {
	data []any
}
