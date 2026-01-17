// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeUnnest executes an UNNEST table function.
// UNNEST expands arrays/lists into rows, producing one row per element.
//
// Syntax examples:
//   - SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)
//   - SELECT * FROM UNNEST([1, 2, 3]) AS t(x)
//   - SELECT t.id, u.val FROM test_table t, UNNEST(t.arr_col) AS u(val)
func (e *Executor) executeUnnest(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Get the array expression from the options
	// The binder stores the bound expression in options["array_expr"]
	arrayExprVal, ok := plan.Options["array_expr"]
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "UNNEST: missing array expression",
		}
	}

	// Get the output column name - defaults to "unnest" if not specified
	outputColName := "unnest"
	if colNameVal, ok := plan.Options["output_column"]; ok {
		if colName, ok := colNameVal.(string); ok && colName != "" {
			outputColName = colName
		}
	}

	// Evaluate the array expression
	var arrayValues []any
	switch expr := arrayExprVal.(type) {
	case binder.BoundExpr:
		// Evaluate the bound expression
		// For LATERAL joins, the correlated values should be in ctx.CorrelatedValues
		val, err := e.evaluateExpr(ctx, expr, nil)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("UNNEST: failed to evaluate array expression: %v", err),
			}
		}
		arrayValues, err = extractArrayValues(val)
		if err != nil {
			return nil, err
		}

	case []any:
		// Already an evaluated array
		arrayValues = expr

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("UNNEST: unexpected array expression type: %T", arrayExprVal),
		}
	}

	// Handle NULL or empty array
	if arrayValues == nil || len(arrayValues) == 0 {
		// Empty array produces no rows
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{outputColName},
		}, nil
	}

	// Create result rows - one row per array element
	rows := make([]map[string]any, 0, len(arrayValues))
	for _, val := range arrayValues {
		row := map[string]any{
			outputColName: val,
		}
		rows = append(rows, row)
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: []string{outputColName},
	}, nil
}

// executeUnnestWithRow executes UNNEST with a correlated row context.
// This is used for LATERAL joins where the array comes from an outer table.
func (e *Executor) executeUnnestWithRow(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
	correlatedRow map[string]any,
) (*ExecutionResult, error) {
	// Get the array expression from the options
	arrayExprVal, ok := plan.Options["array_expr"]
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "UNNEST: missing array expression",
		}
	}

	// Get the output column name
	outputColName := "unnest"
	if colNameVal, ok := plan.Options["output_column"]; ok {
		if colName, ok := colNameVal.(string); ok && colName != "" {
			outputColName = colName
		}
	}

	// Evaluate the array expression with the correlated row context
	var arrayValues []any
	switch expr := arrayExprVal.(type) {
	case binder.BoundExpr:
		// Create execution context with correlated values
		evalCtx := &ExecutionContext{
			Context:          ctx.Context,
			Args:             ctx.Args,
			CorrelatedValues: correlatedRow,
		}
		val, err := e.evaluateExpr(evalCtx, expr, correlatedRow)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("UNNEST: failed to evaluate array expression: %v", err),
			}
		}
		arrayValues, err = extractArrayValues(val)
		if err != nil {
			return nil, err
		}

	case []any:
		arrayValues = expr

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("UNNEST: unexpected array expression type: %T", arrayExprVal),
		}
	}

	// Handle NULL or empty array
	if arrayValues == nil || len(arrayValues) == 0 {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{outputColName},
		}, nil
	}

	// Create result rows
	rows := make([]map[string]any, 0, len(arrayValues))
	for _, val := range arrayValues {
		row := map[string]any{
			outputColName: val,
		}
		rows = append(rows, row)
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: []string{outputColName},
	}, nil
}

// extractArrayValues extracts an array of values from various representations.
func extractArrayValues(val any) ([]any, error) {
	if val == nil {
		return nil, nil
	}

	switch v := val.(type) {
	case []any:
		return v, nil

	case []int:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []int32:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []int64:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []float32:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []float64:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []string:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	case []bool:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return result, nil

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("UNNEST: value is not an array type: %T", val),
		}
	}
}

// isUnnestFunction returns true if the function name is "unnest".
func isUnnestFunction(name string) bool {
	return strings.ToLower(name) == "unnest"
}
