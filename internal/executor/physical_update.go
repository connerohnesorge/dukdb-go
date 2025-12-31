package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeUpdate executes an UPDATE operation.
// UPDATE works by:
// 1. Executing the source plan (scan + optional filter) to find rows to update
// 2. For each matching row, evaluating SET expressions and updating column values
// 3. Returning the count of updated rows
func (e *Executor) executeUpdate(
	ctx *ExecutionContext,
	plan *planner.PhysicalUpdate,
) (*ExecutionResult, error) {
	// Get the table from storage
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	var rowsAffected int64 = 0

	// For this implementation, we'll:
	// 1. Scan all rows
	// 2. Apply the filter (if any) to find rows to update
	// 3. Evaluate SET expressions for matching rows
	// 4. Recreate the table with updated rows

	// Get all rows from the table
	scanner := table.Scan()
	allRows := make([][]any, 0)

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			row := make(
				[]any,
				chunk.ColumnCount(),
			)
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			allRows = append(allRows, row)
		}
	}

	// Process each row
	updatedRows := make([][]any, 0, len(allRows))
	updatedCount := int64(0)

	for _, row := range allRows {
		// Convert row array to map for evaluation
		rowMap := make(map[string]any)
		for i, col := range plan.TableDef.Columns {
			if i < len(row) {
				rowMap[col.Name] = row[i]
			}
		}

		// Check if this row should be updated
		var shouldUpdate bool
		if plan.Source != nil {
			// Check if row matches the filter
			if filter, ok := plan.Source.(*planner.PhysicalFilter); ok {
				passes, err := e.evaluateExprAsBool(
					ctx,
					filter.Condition,
					rowMap,
				)
				if err != nil {
					return nil, err
				}
				shouldUpdate = passes
			} else {
				// No filter means update all rows
				shouldUpdate = true
			}
		} else {
			// No source means update all rows
			shouldUpdate = true
		}

		if shouldUpdate {
			// Apply SET clauses
			updatedRow := make([]any, len(row))
			copy(updatedRow, row)

			for _, setClause := range plan.Set {
				// Evaluate the new value
				newValue, err := e.evaluateExpr(
					ctx,
					setClause.Value,
					rowMap,
				)
				if err != nil {
					return nil, err
				}

				// Update the column
				if setClause.ColumnIdx >= 0 &&
					setClause.ColumnIdx < len(
						updatedRow,
					) {
					updatedRow[setClause.ColumnIdx] = newValue
					// Also update rowMap for subsequent SET clauses that might reference this column
					if setClause.ColumnIdx < len(
						plan.TableDef.Columns,
					) {
						rowMap[plan.TableDef.Columns[setClause.ColumnIdx].Name] = newValue
					}
				}
			}

			updatedRows = append(
				updatedRows,
				updatedRow,
			)
			updatedCount++
		} else {
			// Keep the original row
			updatedRows = append(updatedRows, row)
		}
	}

	// Recreate the table with updated rows
	if err := e.storage.DropTable(plan.Table); err != nil {
		return nil, err
	}

	newTable, err := e.storage.CreateTable(
		plan.Table,
		table.ColumnTypes(),
	)
	if err != nil {
		return nil, err
	}

	// Append all rows
	for _, row := range updatedRows {
		if err := newTable.AppendRow(row); err != nil {
			return nil, err
		}
	}

	rowsAffected = updatedCount

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}
