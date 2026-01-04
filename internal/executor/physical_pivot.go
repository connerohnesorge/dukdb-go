package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executePivotPlan executes a PIVOT physical plan.
// PIVOT transforms rows into columns using conditional aggregation.
//
// Example:
//
//	PIVOT source_table
//	FOR category IN ('A', 'B', 'C')
//	USING SUM(amount)
//	GROUP BY year
//
// Becomes:
//
//	SELECT year,
//	       SUM(CASE WHEN category = 'A' THEN amount END) AS A,
//	       SUM(CASE WHEN category = 'B' THEN amount END) AS B,
//	       SUM(CASE WHEN category = 'C' THEN amount END) AS C
//	FROM source_table
//	GROUP BY year
func (e *Executor) executePivotPlan(
	ctx *ExecutionContext,
	p *planner.PhysicalPivot,
) (*ExecutionResult, error) {
	// Execute the source plan
	sourceResult, err := e.Execute(ctx.Context, p.Source, ctx.Args)
	if err != nil {
		return nil, err
	}

	// Build output column metadata
	outCols := p.OutputColumns()
	columns := make([]string, len(outCols))
	for i, col := range outCols {
		columns[i] = col.Column
	}

	// If no aggregates specified, return empty result
	if len(p.Aggregates) == 0 {
		return &ExecutionResult{
			Columns: columns,
			Rows:    nil,
		}, nil
	}

	// Group the source rows by GROUP BY keys
	groups := make(map[string][]map[string]any)
	groupKeys := make(map[string][]any)

	for _, row := range sourceResult.Rows {
		// Compute group key
		keyParts := make([]string, len(p.GroupBy))
		keyValues := make([]any, len(p.GroupBy))
		for i, expr := range p.GroupBy {
			val, err := e.evaluateExpr(ctx, expr, row)
			if err != nil {
				return nil, err
			}
			keyParts[i] = formatValue(val)
			keyValues[i] = val
		}
		key := strings.Join(keyParts, "\x00")

		groups[key] = append(groups[key], row)
		if _, exists := groupKeys[key]; !exists {
			groupKeys[key] = keyValues
		}
	}

	// Build result rows
	var resultRows []map[string]any

	for key, groupRows := range groups {
		outRow := make(map[string]any)

		// Add group by column values
		keyValues := groupKeys[key]
		for i, expr := range p.GroupBy {
			colName := ""
			if colRef, ok := expr.(*binder.BoundColumnRef); ok {
				colName = colRef.Column
			} else {
				colName = fmt.Sprintf("col%d", i)
			}
			outRow[colName] = keyValues[i]
		}

		// For each aggregate and pivot value, compute the conditional aggregate
		for _, agg := range p.Aggregates {
			for _, pivotVal := range p.InValues {
				colName := formatPivotColumnName(agg.Function, agg.Alias, pivotVal)

				// Filter rows where the FOR column matches this pivot value
				var filteredRows []map[string]any
				for _, row := range groupRows {
					// Get the value of the FOR column from this row
					if p.ForColumn != nil {
						forVal, err := e.evaluateExpr(ctx, p.ForColumn, row)
						if err != nil {
							return nil, err
						}
						// Only include rows where FOR column value matches the pivot value
						if valuesEqual(forVal, pivotVal) {
							filteredRows = append(filteredRows, row)
						}
					} else {
						// If no FOR column, include all rows (fallback behavior)
						filteredRows = append(filteredRows, row)
					}
				}

				// Compute the aggregate over filtered rows
				aggResult, err := e.computePivotAggregate(ctx, agg, filteredRows, pivotVal)
				if err != nil {
					return nil, err
				}
				outRow[colName] = aggResult
			}
		}

		resultRows = append(resultRows, outRow)
	}

	return &ExecutionResult{
		Columns: columns,
		Rows:    resultRows,
	}, nil
}

// valuesEqual compares two values for equality, handling type coercion.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle string comparison (most common for PIVOT)
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return aStr == bStr
	}

	// Handle numeric comparisons with type coercion
	aNum, aIsNum := toNumericValue(a)
	bNum, bIsNum := toNumericValue(b)
	if aIsNum && bIsNum {
		return aNum == bNum
	}

	// Direct comparison
	return a == b
}

// toNumericValue converts a value to float64 if possible.
func toNumericValue(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// computePivotAggregate computes an aggregate function over rows matching a pivot value.
func (e *Executor) computePivotAggregate(
	ctx *ExecutionContext,
	agg *binder.BoundPivotAggregate,
	rows []map[string]any,
	_ any, // pivotVal - unused in this simplified implementation
) (any, error) {
	switch strings.ToUpper(agg.Function) {
	case "COUNT":
		count := int64(0)
		for range rows {
			count++
		}
		return count, nil

	case "SUM":
		var sum float64
		hasValue := false
		for _, row := range rows {
			val, err := e.evaluateExpr(ctx, agg.Expr, row)
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
		return sum, nil

	case "AVG":
		var sum float64
		count := 0
		for _, row := range rows {
			val, err := e.evaluateExpr(ctx, agg.Expr, row)
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
		var minVal any
		for _, row := range rows {
			val, err := e.evaluateExpr(ctx, agg.Expr, row)
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
		var maxVal any
		for _, row := range rows {
			val, err := e.evaluateExpr(ctx, agg.Expr, row)
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
			Msg:  fmt.Sprintf("unsupported aggregate function in PIVOT: %s", agg.Function),
		}
	}
}

// formatPivotColumnName creates a column name for a pivot result column.
func formatPivotColumnName(_, alias string, pivotValue any) string {
	valStr := formatValue(pivotValue)
	if alias != "" {
		return valStr + "_" + alias
	}
	return valStr
}

// executeUnpivotPlan executes an UNPIVOT physical plan.
// UNPIVOT transforms columns into rows (the inverse of PIVOT).
//
// Example:
//
//	UNPIVOT source_table
//	ON column1, column2, column3
//	INTO NAME category VALUE amount
//
// Becomes:
//
//	SELECT other_cols, 'column1' AS category, column1 AS amount FROM source_table
//	UNION ALL
//	SELECT other_cols, 'column2' AS category, column2 AS amount FROM source_table
//	UNION ALL
//	SELECT other_cols, 'column3' AS category, column3 AS amount FROM source_table
func (e *Executor) executeUnpivotPlan(
	ctx *ExecutionContext,
	u *planner.PhysicalUnpivot,
) (*ExecutionResult, error) {
	// Execute the source plan
	sourceResult, err := e.Execute(ctx.Context, u.Source, ctx.Args)
	if err != nil {
		return nil, err
	}

	// Build output column metadata
	outCols := u.OutputColumns()
	columns := make([]string, len(outCols))
	for i, col := range outCols {
		columns[i] = col.Column
	}

	// Determine which source columns are NOT being unpivoted
	nonUnpivotCols := make([]string, 0)
	for _, srcCol := range sourceResult.Columns {
		isUnpivot := false
		for _, unpivotCol := range u.UnpivotColumns {
			if srcCol == unpivotCol {
				isUnpivot = true
				break
			}
		}
		if !isUnpivot {
			nonUnpivotCols = append(nonUnpivotCols, srcCol)
		}
	}

	// Build result rows
	var resultRows []map[string]any

	// For each source row, create one output row per unpivot column
	for _, srcRow := range sourceResult.Rows {
		for _, unpivotColName := range u.UnpivotColumns {
			outRow := make(map[string]any)

			// Copy non-unpivoted columns
			for _, colName := range nonUnpivotCols {
				outRow[colName] = srcRow[colName]
			}

			// Add name column (the unpivoted column name)
			outRow[u.NameColumn] = unpivotColName

			// Add value column (the value from the unpivoted column)
			outRow[u.ValueColumn] = srcRow[unpivotColName]

			resultRows = append(resultRows, outRow)
		}
	}

	return &ExecutionResult{
		Columns: columns,
		Rows:    resultRows,
	}, nil
}
