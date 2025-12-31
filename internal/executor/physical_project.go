package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalProjectOperator implements the PhysicalOperator interface for projection.
// It evaluates projection expressions on each row from a child operator and produces
// DataChunks containing only the projected columns.
type PhysicalProjectOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	expressions  []binder.BoundExpr
	types        []dukdb.TypeInfo
	executor     *Executor
	ctx          *ExecutionContext
}

// NewPhysicalProjectOperator creates a new PhysicalProjectOperator.
func NewPhysicalProjectOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	expressions []binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalProjectOperator, error) {
	// Get TypeInfo for each projection expression
	types := make([]dukdb.TypeInfo, len(expressions))
	for i, expr := range expressions {
		resultType := expr.ResultType()
		info, err := dukdb.NewTypeInfo(resultType)
		if err != nil {
			// Fallback: use basic wrapper if TypeInfo creation fails
			types[i] = &basicTypeInfo{typ: resultType}
		} else {
			types[i] = info
		}
	}

	return &PhysicalProjectOperator{
		child:        child,
		childColumns: childColumns,
		expressions:  expressions,
		types:        types,
		executor:     executor,
		ctx:          ctx,
	}, nil
}

// Next returns the next DataChunk of projected results, or nil if no more data.
// It reads chunks from the child operator, evaluates the projection expressions on each row,
// and produces output chunks with the projected columns.
func (op *PhysicalProjectOperator) Next() (*storage.DataChunk, error) {
	// Get next chunk from child
	inputChunk, err := op.child.Next()
	if err != nil {
		return nil, err
	}
	if inputChunk == nil {
		// No more data
		return nil, nil
	}

	// Create output chunk with projection expression types
	outputTypes := make([]dukdb.Type, len(op.expressions))
	for i, expr := range op.expressions {
		outputTypes[i] = expr.ResultType()
	}

	outputChunk := storage.NewDataChunkWithCapacity(
		outputTypes,
		inputChunk.Capacity(),
	)

	// Evaluate projection expressions for each row
	for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
		// Create a row map for expression evaluation
		// Build the row context with column names from the child operator
		rowMap := op.buildRowMap(inputChunk, rowIdx)

		// Evaluate each projection expression
		projectedValues := make([]any, len(op.expressions))
		for colIdx, expr := range op.expressions {
			val, err := op.executor.evaluateExpr(op.ctx, expr, rowMap)
			if err != nil {
				return nil, err
			}
			projectedValues[colIdx] = val
		}

		// Append the projected row to the output chunk
		outputChunk.AppendRow(projectedValues)
	}

	return outputChunk, nil
}

// buildRowMap creates a map of column names to values for expression evaluation.
// This maps both simple column names and table-qualified names from the child operator.
func (op *PhysicalProjectOperator) buildRowMap(chunk *storage.DataChunk, rowIdx int) map[string]any {
	rowMap := make(map[string]any)

	// Map columns using the child's column bindings
	for colIdx := 0; colIdx < chunk.ColumnCount() && colIdx < len(op.childColumns); colIdx++ {
		value := chunk.GetValue(rowIdx, colIdx)
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

	return rowMap
}

// GetTypes returns the TypeInfo for each column produced by this operator.
// Project changes the schema based on the projection expressions.
func (op *PhysicalProjectOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}
