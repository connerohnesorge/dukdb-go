package executor

import (
	"fmt"
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// parseAndEvalGeneratedExpr parses colExpr as an SQL expression and evaluates it
// against rowMap, then casts the result to the column's declared type.
func (e *Executor) parseAndEvalGeneratedExpr(
	ctx *ExecutionContext,
	col *catalog.ColumnDef,
	rowMap map[string]any,
) (any, error) {
	wrapSQL := "SELECT " + col.GeneratedExpr
	stmt, err := parser.Parse(wrapSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated column expression for %q: %w", col.Name, err)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || len(selectStmt.Columns) == 0 {
		return nil, fmt.Errorf("invalid generated column expression for %q", col.Name)
	}
	result, err := e.evaluateExpr(ctx, selectStmt.Columns[0].Expr, rowMap)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate generated column %q: %w", col.Name, err)
	}
	if result != nil {
		result, err = castValue(result, col.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to cast generated column %q value: %w", col.Name, err)
		}
	}
	return result, nil
}

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
			rowMap[strings.ToLower(col.Name)] = values[i]
		}
	}

	for i, col := range tableDef.Columns {
		if !col.IsGenerated || col.GeneratedExpr == "" {
			continue
		}
		result, err := e.parseAndEvalGeneratedExpr(ctx, col, rowMap)
		if err != nil {
			return err
		}
		if i < len(values) {
			values[i] = result
		}
		rowMap[col.Name] = result
		rowMap[strings.ToLower(col.Name)] = result
	}

	return nil
}

// evaluateDefaultExpr evaluates a non-literal default expression (e.g., NEXTVAL('seq'))
// by parsing the expression text and evaluating it.
func (e *Executor) evaluateDefaultExpr(
	ctx *ExecutionContext,
	exprText string,
) (any, error) {
	// Wrap in SELECT to parse as an expression
	wrapSQL := "SELECT " + exprText
	stmt, err := parser.Parse(wrapSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse default expression %q: %w", exprText, err)
	}
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || len(selectStmt.Columns) == 0 {
		return nil, fmt.Errorf("invalid default expression %q", exprText)
	}

	// Evaluate the expression (no row context needed for sequence calls etc.)
	result, err := e.evaluateExpr(ctx, selectStmt.Columns[0].Expr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate default expression %q: %w", exprText, err)
	}
	return result, nil
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
		result, err := e.parseAndEvalGeneratedExpr(ctx, col, rowMap)
		if err != nil {
			return err
		}
		columnValues[i] = result
		rowMap[col.Name] = result
		rowMap[strings.ToLower(col.Name)] = result
	}

	return nil
}
