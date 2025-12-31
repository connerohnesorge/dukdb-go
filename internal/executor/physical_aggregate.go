package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalAggregateOperator implements the PhysicalOperator interface for aggregation.
// It consumes all input from the child operator, groups by the GROUP BY expressions,
// computes aggregate functions, and produces one output chunk with one row per group.
type PhysicalAggregateOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	groupBy      []binder.BoundExpr
	aggregates   []binder.BoundExpr
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo
	finished     bool
	resultChunk  *storage.DataChunk
}

// NewPhysicalAggregateOperator creates a new PhysicalAggregateOperator.
func NewPhysicalAggregateOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	groupBy []binder.BoundExpr,
	aggregates []binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalAggregateOperator, error) {
	// Build output types: GROUP BY columns followed by aggregate columns
	numGroupBy := len(groupBy)
	numAgg := len(aggregates)
	types := make([]dukdb.TypeInfo, numGroupBy+numAgg)

	// Group by column types
	for i, expr := range groupBy {
		typ := expr.ResultType()
		info, err := dukdb.NewTypeInfo(typ)
		if err != nil {
			types[i] = &basicTypeInfo{typ: typ}
		} else {
			types[i] = info
		}
	}

	// Aggregate column types
	for i, expr := range aggregates {
		typ := expr.ResultType()
		info, err := dukdb.NewTypeInfo(typ)
		if err != nil {
			types[numGroupBy+i] = &basicTypeInfo{typ: typ}
		} else {
			types[numGroupBy+i] = info
		}
	}

	return &PhysicalAggregateOperator{
		child:        child,
		childColumns: childColumns,
		groupBy:      groupBy,
		aggregates:   aggregates,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		finished:     false,
	}, nil
}

// Next returns the aggregated results as a single DataChunk, or nil if already called.
// The operator consumes all input from the child, builds hash groups, and returns
// one chunk with all group results.
func (op *PhysicalAggregateOperator) Next() (*storage.DataChunk, error) {
	// Aggregation is a blocking operator - we only produce output once
	if op.finished {
		return nil, nil
	}
	op.finished = true

	// Hash table: groupKey -> list of rows belonging to that group
	type groupKey string
	groups := make(map[groupKey][]map[string]any)
	groupOrder := make([]groupKey, 0) // Preserve insertion order

	// Consume all input from child operator
	for {
		inputChunk, err := op.child.Next()
		if err != nil {
			return nil, err
		}
		if inputChunk == nil {
			// No more input
			break
		}

		// Process each row in the chunk
		for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
			// Build row map for expression evaluation
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

			// Compute group key from GROUP BY expressions
			var key groupKey
			if len(op.groupBy) > 0 {
				keyParts := make([]any, len(op.groupBy))
				for i, expr := range op.groupBy {
					val, err := op.executor.evaluateExpr(op.ctx, expr, rowMap)
					if err != nil {
						return nil, err
					}
					keyParts[i] = val
				}
				key = groupKey(formatGroupKey(keyParts))
			} else {
				// No GROUP BY - single group for all rows
				key = ""
			}

			// Add row to group
			if _, exists := groups[key]; !exists {
				groupOrder = append(groupOrder, key)
				groups[key] = make([]map[string]any, 0)
			}
			groups[key] = append(groups[key], rowMap)
		}
	}

	// If no groups and no GROUP BY, create a single empty group for aggregates
	// (e.g., SELECT COUNT(*) FROM empty_table should return 0, not no rows)
	if len(groups) == 0 && len(op.groupBy) == 0 {
		groupOrder = append(groupOrder, "")
		groups[""] = []map[string]any{}
	}

	// Build output chunk
	numGroupBy := len(op.groupBy)
	numAgg := len(op.aggregates)
	outputTypes := make([]dukdb.Type, numGroupBy+numAgg)

	for i, expr := range op.groupBy {
		outputTypes[i] = expr.ResultType()
	}
	for i, expr := range op.aggregates {
		outputTypes[numGroupBy+i] = expr.ResultType()
	}

	outputChunk := storage.NewDataChunkWithCapacity(outputTypes, len(groupOrder))

	// Compute aggregates for each group and add to output
	for _, key := range groupOrder {
		groupRows := groups[key]
		rowValues := make([]any, numGroupBy+numAgg)

		// Add GROUP BY column values
		if len(groupRows) > 0 && len(op.groupBy) > 0 {
			for i, expr := range op.groupBy {
				val, _ := op.executor.evaluateExpr(op.ctx, expr, groupRows[0])
				rowValues[i] = val
			}
		} else if len(op.groupBy) > 0 {
			// Empty group - use NULLs for group by columns
			for i := range op.groupBy {
				rowValues[i] = nil
			}
		}

		// Compute aggregate values
		for i, expr := range op.aggregates {
			val, err := op.computeAggregate(expr, groupRows)
			if err != nil {
				return nil, err
			}
			rowValues[numGroupBy+i] = val
		}

		outputChunk.AppendRow(rowValues)
	}

	op.resultChunk = outputChunk
	return outputChunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalAggregateOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// computeAggregate computes an aggregate function over a set of rows.
func (op *PhysicalAggregateOperator) computeAggregate(
	expr binder.BoundExpr,
	rows []map[string]any,
) (any, error) {
	fn, ok := expr.(*binder.BoundFunctionCall)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "expected aggregate function",
		}
	}

	switch fn.Name {
	case "COUNT":
		// COUNT(*) - count all rows
		if fn.Star {
			return int64(len(rows)), nil
		}
		// COUNT(expr) - count non-NULL values
		count := int64(0)
		seen := make(map[string]bool)
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
				if err != nil {
					return nil, err
				}
				if val != nil {
					if fn.Distinct {
						key := formatValue(val)
						if !seen[key] {
							seen[key] = true
							count++
						}
					} else {
						count++
					}
				}
			}
		}
		return count, nil

	case "SUM":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		hasValue := false
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				hasValue = true
			}
		}
		if !hasValue {
			return nil, nil
		}
		// Return as int64 if all inputs were integers
		return sum, nil

	case "AVG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		count := 0
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case "MIN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var minVal any
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if minVal == nil || compareValues(val, minVal) < 0 {
					minVal = val
				}
			}
		}
		return minVal, nil

	case "MAX":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var maxVal any
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if maxVal == nil || compareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
		}
		return maxVal, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("unknown aggregate function: %s", fn.Name),
		}
	}
}
