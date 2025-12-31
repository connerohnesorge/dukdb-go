package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeUpdate executes an UPDATE operation.
// UPDATE works by:
// 1. Scanning all rows from the table
// 2. For each row, checking if it matches the filter condition (from Source plan if present)
// 3. For matching rows, evaluating SET expressions and updating column values
// 4. Recreating the table with all rows (both updated and unchanged)
// 5. Returning the count of updated rows
func (e *Executor) executeUpdate(
	ctx *ExecutionContext,
	plan *planner.PhysicalUpdate,
) (*ExecutionResult, error) {
	// Get the table from storage
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

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

	// Process each row to determine which ones should be updated
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

		// Check if this row should be updated based on Source filter
		var shouldUpdate bool
		if plan.Source != nil {
			// Extract the filter condition from the source plan
			if filter, ok := plan.Source.(*planner.PhysicalFilter); ok {
				// Evaluate the filter condition against this row
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
				// If source is not a filter, update all rows
				shouldUpdate = true
			}
		} else {
			// No source plan means update all rows (UPDATE without WHERE)
			shouldUpdate = true
		}

		if shouldUpdate {
			// Apply SET clauses to create updated row
			updatedRow := make([]any, len(row))
			copy(updatedRow, row)

			for _, setClause := range plan.Set {
				// Evaluate the new value expression
				newValue, err := e.evaluateExpr(
					ctx,
					setClause.Value,
					rowMap,
				)
				if err != nil {
					return nil, err
				}

				// Update the column in the row
				if setClause.ColumnIdx >= 0 &&
					setClause.ColumnIdx < len(
						updatedRow,
					) {
					// Convert the new value to the correct type for this column
					if setClause.ColumnIdx < len(
						plan.TableDef.Columns,
					) {
						colType := plan.TableDef.Columns[setClause.ColumnIdx].Type
						castedValue, castErr := castValue(newValue, colType)
						if castErr != nil {
							return nil, castErr
						}
						newValue = castedValue
					}
					updatedRow[setClause.ColumnIdx] = newValue
					// Also update rowMap for subsequent SET clauses that might reference this updated column
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
			// Keep the original row unchanged
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

	// Append all rows (both updated and unchanged)
	for _, row := range updatedRows {
		if err := newTable.AppendRow(row); err != nil {
			return nil, err
		}
	}

	return &ExecutionResult{
		RowsAffected: updatedCount,
	}, nil
}
