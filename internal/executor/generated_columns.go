package executor

import (
	"fmt"
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// evaluateGeneratedColumns evaluates all generated column expressions for a row.
// It builds a row map from the current values and evaluates each generated column's
// stored SQL expression using the existing expression evaluation infrastructure.
func (e *Executor) evaluateGeneratedColumns(
	ctx *ExecutionContext,
	tableDef *catalog.TableDef,
	values []any,
) error {
	if tableDef == nil {
		return nil
	}

	// Quick check: does this table have any generated columns?
	hasGenerated := false
	for _, col := range tableDef.Columns {
		if col.IsGenerated {
			hasGenerated = true
			break
		}
	}
	if !hasGenerated {
		return nil
	}

	// Build row map from current values for expression evaluation
	rowMap := make(map[string]any, len(tableDef.Columns))
	for i, col := range tableDef.Columns {
		if i < len(values) {
			rowMap[col.Name] = values[i]
			// Also add lowercase version for case-insensitive matching
			rowMap[strings.ToLower(col.Name)] = values[i]
		}
	}

	// Evaluate each generated column
	for i, col := range tableDef.Columns {
		if !col.IsGenerated || col.GeneratedExpr == "" {
			continue
		}

		// Parse the generated expression by wrapping in SELECT
		wrapSQL := "SELECT " + col.GeneratedExpr
		stmt, err := parser.Parse(wrapSQL)
		if err != nil {
			return fmt.Errorf(
				"failed to parse generated column expression for %q: %w",
				col.Name, err,
			)
		}
		selectStmt, ok := stmt.(*parser.SelectStmt)
		if !ok || len(selectStmt.Columns) == 0 {
			return fmt.Errorf(
				"invalid generated column expression for %q",
				col.Name,
			)
		}

		// Evaluate the expression using the row values
		result, err := e.evaluateExpr(ctx, selectStmt.Columns[0].Expr, rowMap)
		if err != nil {
			return fmt.Errorf(
				"failed to evaluate generated column %q: %w",
				col.Name, err,
			)
		}

		// Cast the result to the column type
		if result != nil {
			castedValue, castErr := castValue(result, col.Type)
			if castErr != nil {
				return fmt.Errorf(
					"failed to cast generated column %q value: %w",
					col.Name, castErr,
				)
			}
			result = castedValue
		}

		// Set the computed value
		if i < len(values) {
			values[i] = result
		}

		// Update the row map for subsequent generated columns that might reference this one
		rowMap[col.Name] = result
		rowMap[strings.ToLower(col.Name)] = result
	}

	return nil
}

// recomputeGeneratedColumnsForUpdate re-evaluates all generated columns after
// base columns have been updated. It uses the rowMap (which already has updated
// base column values) and adds computed values to columnValues map.
func (e *Executor) recomputeGeneratedColumnsForUpdate(
	ctx *ExecutionContext,
	tableDef *catalog.TableDef,
	rowMap map[string]any,
	columnValues map[int]any,
) error {
	if tableDef == nil {
		return nil
	}

	for i, col := range tableDef.Columns {
		if !col.IsGenerated || col.GeneratedExpr == "" {
			continue
		}

		// Parse the generated expression by wrapping in SELECT
		wrapSQL := "SELECT " + col.GeneratedExpr
		stmt, err := parser.Parse(wrapSQL)
		if err != nil {
			return fmt.Errorf(
				"failed to parse generated column expression for %q: %w",
				col.Name, err,
			)
		}
		selectStmt, ok := stmt.(*parser.SelectStmt)
		if !ok || len(selectStmt.Columns) == 0 {
			return fmt.Errorf(
				"invalid generated column expression for %q",
				col.Name,
			)
		}

		// Evaluate the expression using the updated row values
		result, err := e.evaluateExpr(ctx, selectStmt.Columns[0].Expr, rowMap)
		if err != nil {
			return fmt.Errorf(
				"failed to evaluate generated column %q: %w",
				col.Name, err,
			)
		}

		// Cast the result to the column type
		if result != nil {
			castedValue, castErr := castValue(result, col.Type)
			if castErr != nil {
				return fmt.Errorf(
					"failed to cast generated column %q value: %w",
					col.Name, castErr,
				)
			}
			result = castedValue
		}

		// Add computed value to the update map
		columnValues[i] = result

		// Update the row map for subsequent generated columns
		rowMap[col.Name] = result
		rowMap[strings.ToLower(col.Name)] = result
	}

	return nil
}
