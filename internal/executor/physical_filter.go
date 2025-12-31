package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalFilterOperator implements the PhysicalOperator interface for filtering rows.
// It evaluates a predicate expression on each row from a child operator and produces
// DataChunks containing only the rows that satisfy the predicate.
type PhysicalFilterOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	predicate    binder.BoundExpr
	executor     *Executor
	ctx          *ExecutionContext
}

// NewPhysicalFilterOperator creates a new PhysicalFilterOperator.
func NewPhysicalFilterOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	predicate binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) *PhysicalFilterOperator {
	return &PhysicalFilterOperator{
		child:        child,
		childColumns: childColumns,
		predicate:    predicate,
		executor:     executor,
		ctx:          ctx,
	}
}

// Next returns the next DataChunk of filtered results, or nil if no more data.
// It reads chunks from the child operator, evaluates the predicate on each row,
// and produces output chunks with only the rows that pass the filter.
func (op *PhysicalFilterOperator) Next() (*storage.DataChunk, error) {
	for {
		// Get next chunk from child
		inputChunk, err := op.child.Next()
		if err != nil {
			return nil, err
		}
		if inputChunk == nil {
			// No more data
			return nil, nil
		}

		// Create output chunk with same types as input
		outputChunk := storage.NewDataChunkWithCapacity(
			inputChunk.Types(),
			inputChunk.Capacity(),
		)

		// Evaluate predicate on each row and collect matching rows
		for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
			// Create a row map for expression evaluation
			// This is row-at-a-time for simplicity (vectorization can come later)
			rowMap := make(map[string]any)

			if len(op.childColumns) > 0 {
				// Use column bindings if available
				for colIdx := 0; colIdx < inputChunk.ColumnCount() && colIdx < len(op.childColumns); colIdx++ {
					value := inputChunk.GetValue(rowIdx, colIdx)
					col := op.childColumns[colIdx]

					// Add column by simple name
					if col.Column != "" {
						rowMap[col.Column] = value
					}

					// Add column by table-qualified name
					if col.Table != "" && col.Column != "" {
						qualifiedName := col.Table + "." + col.Column
						rowMap[qualifiedName] = value
					}
				}
			} else {
				// Fallback: use column indices as string keys
				for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
					value := inputChunk.GetValue(rowIdx, colIdx)
					rowMap[string(rune('0'+colIdx))] = value
				}
			}

			// Evaluate the predicate
			result, err := op.executor.evaluateExpr(op.ctx, op.predicate, rowMap)
			if err != nil {
				return nil, err
			}

			// NULL predicate result means filter out the row (SQL semantics)
			if result == nil {
				continue
			}

			// Convert result to boolean
			passes := toBool(result)
			if passes {
				// Row passes filter - copy to output chunk
				values := make([]any, inputChunk.ColumnCount())
				for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
					values[colIdx] = inputChunk.GetValue(rowIdx, colIdx)
				}
				outputChunk.AppendRow(values)
			}
		}

		// If we got any matching rows, return the chunk
		if outputChunk.Count() > 0 {
			return outputChunk, nil
		}

		// No rows passed the filter in this chunk, try the next chunk
	}
}

// GetTypes returns the TypeInfo for each column produced by this operator.
// Filter doesn't change the schema, so it returns the same types as the child.
func (op *PhysicalFilterOperator) GetTypes() []dukdb.TypeInfo {
	return op.child.GetTypes()
}
