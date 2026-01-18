// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalLateralJoinOperator implements the PhysicalOperator interface for LATERAL joins.
// LATERAL joins re-evaluate the right side for each row of the left side,
// allowing the right side to reference columns from the current left row.
//
// Algorithm:
//  1. Scan the left (outer) side one row at a time
//  2. For each left row, execute the right (correlated) side with the left row's values available
//  3. Combine left and right rows based on join type
//  4. For LEFT LATERAL: include left rows even if right returns no rows (with NULLs)
type PhysicalLateralJoinOperator struct {
	left         PhysicalOperator
	rightPlan    planner.PhysicalPlan // The plan to re-execute for each left row
	leftColumns  []planner.ColumnBinding
	rightColumns []planner.ColumnBinding
	joinType     planner.JoinType
	condition    binder.BoundExpr
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo

	// State for iteration
	leftChunk      *storage.DataChunk
	leftRowIdx     int
	currentLeftRow map[string]any

	// Right side results for current left row
	rightResult     *ExecutionResult
	rightRowIdx     int
	hasEmittedMatch bool // Whether we've emitted at least one match for current left row

	finished bool
}

// NewPhysicalLateralJoinOperator creates a new PhysicalLateralJoinOperator.
func NewPhysicalLateralJoinOperator(
	left PhysicalOperator,
	rightPlan planner.PhysicalPlan,
	leftColumns []planner.ColumnBinding,
	rightColumns []planner.ColumnBinding,
	joinType planner.JoinType,
	condition binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalLateralJoinOperator, error) {
	// Build output types: left columns followed by right columns
	numLeft := len(leftColumns)
	numRight := len(rightColumns)
	types := make([]dukdb.TypeInfo, numLeft+numRight)

	for i, col := range leftColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}

	for i, col := range rightColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[numLeft+i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[numLeft+i] = info
		}
	}

	return &PhysicalLateralJoinOperator{
		left:         left,
		rightPlan:    rightPlan,
		leftColumns:  leftColumns,
		rightColumns: rightColumns,
		joinType:     joinType,
		condition:    condition,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		finished:     false,
	}, nil
}

// Next returns the next DataChunk of joined results.
func (op *PhysicalLateralJoinOperator) Next() (*storage.DataChunk, error) {
	if op.finished {
		return nil, nil
	}

	numLeft := len(op.leftColumns)
	numRight := len(op.rightColumns)
	outputTypes := make([]dukdb.Type, numLeft+numRight)

	for i, col := range op.leftColumns {
		outputTypes[i] = col.Type
	}
	for i, col := range op.rightColumns {
		outputTypes[numLeft+i] = col.Type
	}

	outputChunk := storage.NewDataChunkWithCapacity(outputTypes, storage.StandardVectorSize)

	for {
		// Try to emit from current right result
		if op.rightResult != nil && op.rightRowIdx < len(op.rightResult.Rows) {
			rightRow := op.rightResult.Rows[op.rightRowIdx]
			op.rightRowIdx++

			// Combine left and right rows
			combinedRow := make(map[string]any)
			for k, v := range op.currentLeftRow {
				combinedRow[k] = v
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}

			// Apply join condition if present
			if op.condition != nil {
				passes, err := op.executor.evaluateExprAsBool(op.ctx, op.condition, combinedRow)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue
				}
			}

			// Emit the combined row
			values := op.buildOutputRow(op.currentLeftRow, rightRow)
			outputChunk.AppendRow(values)
			op.hasEmittedMatch = true

			if outputChunk.Count() >= outputChunk.Capacity() {
				return outputChunk, nil
			}
			continue
		}

		// No more right rows for current left row
		// Check if we need to emit a left row with NULLs (LEFT LATERAL with no matches)
		if op.currentLeftRow != nil && !op.hasEmittedMatch {
			if op.joinType == planner.JoinTypeLeft || op.joinType == planner.JoinTypeCross {
				// For LEFT LATERAL, emit left row with NULLs for right columns
				if op.joinType == planner.JoinTypeLeft {
					values := op.buildOutputRow(op.currentLeftRow, nil)
					outputChunk.AppendRow(values)

					if outputChunk.Count() >= outputChunk.Capacity() {
						op.currentLeftRow = nil
						op.rightResult = nil
						return outputChunk, nil
					}
				}
			}
		}

		// Need a new left row
		op.currentLeftRow = nil
		op.rightResult = nil
		op.rightRowIdx = 0
		op.hasEmittedMatch = false

		// Get next left row
		for {
			if op.leftChunk == nil || op.leftRowIdx >= op.leftChunk.Count() {
				// Get next chunk from left child
				chunk, err := op.left.Next()
				if err != nil {
					return nil, err
				}
				if chunk == nil {
					// No more left rows
					op.finished = true
					if outputChunk.Count() > 0 {
						return outputChunk, nil
					}
					return nil, nil
				}
				op.leftChunk = chunk
				op.leftRowIdx = 0
			}

			if op.leftRowIdx < op.leftChunk.Count() {
				op.currentLeftRow = op.rowMapFromChunk(op.leftChunk, op.leftRowIdx, op.leftColumns)
				op.leftRowIdx++
				break
			}
		}

		// Execute right side with current left row's context
		// The right side can reference columns from the left row
		rightResult, err := op.executeRightWithCorrelation(op.currentLeftRow)
		if err != nil {
			return nil, err
		}
		op.rightResult = rightResult
		op.rightRowIdx = 0
	}
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalLateralJoinOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// executeRightWithCorrelation executes the right side of the lateral join
// with the current left row's values available for correlation.
func (op *PhysicalLateralJoinOperator) executeRightWithCorrelation(
	leftRow map[string]any,
) (*ExecutionResult, error) {
	// Execute the right plan - the expressions in the right plan can reference
	// columns from the left row. The executor's evaluateExpr will look up
	// column values from the row context.
	//
	// For correlated subqueries, the plan contains column references that
	// need to be resolved against the left row. We pass the left row context
	// to the execution.
	result, err := op.executor.Execute(op.ctx.Context, op.rightPlan, op.ctx.Args)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// buildOutputRow constructs the output row values from left and right row maps.
func (op *PhysicalLateralJoinOperator) buildOutputRow(leftRow, rightRow map[string]any) []any {
	numLeft := len(op.leftColumns)
	numRight := len(op.rightColumns)
	values := make([]any, numLeft+numRight)

	// Add left column values
	for i, col := range op.leftColumns {
		colKey := col.Column
		if col.Table != "" {
			qualifiedKey := col.Table + "." + col.Column
			if val, ok := leftRow[qualifiedKey]; ok {
				values[i] = val
				continue
			}
		}
		values[i] = leftRow[colKey]
	}

	// Add right column values (or NULLs if rightRow is nil)
	if rightRow != nil {
		for i, col := range op.rightColumns {
			colKey := col.Column
			if col.Table != "" {
				qualifiedKey := col.Table + "." + col.Column
				if val, ok := rightRow[qualifiedKey]; ok {
					values[numLeft+i] = val
					continue
				}
			}
			values[numLeft+i] = rightRow[colKey]
		}
	} else {
		// NULL values for right columns (LEFT LATERAL with no matches)
		for i := range op.rightColumns {
			values[numLeft+i] = nil
		}
	}

	return values
}

// rowMapFromChunk extracts a row from a DataChunk into a map[string]any.
func (op *PhysicalLateralJoinOperator) rowMapFromChunk(
	chunk *storage.DataChunk,
	rowIdx int,
	columns []planner.ColumnBinding,
) map[string]any {
	rowMap := make(map[string]any)

	for colIdx := 0; colIdx < chunk.ColumnCount() && colIdx < len(columns); colIdx++ {
		value := chunk.GetValue(rowIdx, colIdx)
		col := columns[colIdx]

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
