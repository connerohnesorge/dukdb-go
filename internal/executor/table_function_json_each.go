// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"encoding/json"
	"fmt"
	"strconv"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeJSONEach executes the json_each table function.
// json_each(json_string) expands a JSON object or array into rows with key and value columns.
// For objects: key is the property name, value is the property value (as JSON string).
// For arrays: key is the index (as string), value is the element (as JSON string).
func (e *Executor) executeJSONEach(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Get JSON argument from plan options
	var jsonStr string

	if jsonExpr, ok := plan.Options["json_expr"]; ok {
		switch expr := jsonExpr.(type) {
		case binder.BoundExpr:
			val, err := e.evaluateExpr(ctx, expr, nil)
			if err != nil {
				return nil, fmt.Errorf("json_each: failed to evaluate expression: %w", err)
			}
			if val == nil {
				return &ExecutionResult{
					Rows:    []map[string]any{},
					Columns: []string{"key", "value"},
				}, nil
			}
			jsonStr = toString(val)
		case string:
			jsonStr = expr
		default:
			jsonStr = toString(jsonExpr)
		}
	} else {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "json_each requires 1 argument",
		}
	}

	// Parse JSON
	var doc any
	if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("json_each: invalid JSON: %v", err),
		}
	}

	var rows []map[string]any
	columns := []string{"key", "value"}

	switch d := doc.(type) {
	case map[string]any:
		rows = make([]map[string]any, 0, len(d))
		for k, v := range d {
			valJSON, _ := json.Marshal(v)
			rows = append(rows, map[string]any{
				"key":   k,
				"value": string(valJSON),
			})
		}
	case []any:
		rows = make([]map[string]any, 0, len(d))
		for i, v := range d {
			valJSON, _ := json.Marshal(v)
			rows = append(rows, map[string]any{
				"key":   strconv.Itoa(i),
				"value": string(valJSON),
			})
		}
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "json_each: argument must be a JSON object or array",
		}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: columns,
	}, nil
}
