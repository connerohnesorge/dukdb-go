package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeDelete executes a DELETE operation.
// DELETE works by:
// 1. Executing the source plan (scan + optional filter) to find rows to delete
// 2. For each matching row, removing it from storage
// 3. Returning the count of deleted rows
func (e *Executor) executeDelete(
	ctx *ExecutionContext,
	plan *planner.PhysicalDelete,
) (*ExecutionResult, error) {
	// Get the table from storage
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	var rowsAffected int64 = 0

	if plan.Source != nil {
		// DELETE with WHERE clause - execute source to find matching rows
		sourceResult, err := e.Execute(
			ctx.Context,
			plan.Source,
			ctx.Args,
		)
		if err != nil {
			return nil, err
		}

		// Count the rows that would be deleted
		rowsAffected = int64(
			len(sourceResult.Rows),
		)

		// For now, we implement DELETE by:
		// 1. Collecting all non-deleted rows
		// 2. Recreating the table with only those rows
		// This is a simple but inefficient approach that works for the initial implementation

		// Build a set of row identifiers to delete
		// Since we don't have row IDs, we'll use a different approach:
		// Scan the table and compare each row against the filter condition

		// For simplicity in this phase, we'll recreate the table without deleted rows
		// First, get all rows from the table
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

		// Now filter out rows that match the WHERE condition
		// We need to re-evaluate the filter for each row
		keptRows := make([][]any, 0)
		deletedCount := int64(0)

		for _, row := range allRows {
			// Convert row array to map for evaluation
			rowMap := make(map[string]any)
			for i, col := range plan.TableDef.Columns {
				if i < len(row) {
					rowMap[col.Name] = row[i]
				}
			}

			// Check if this row should be deleted
			shouldDelete := false
			if plan.Source != nil {
				// Re-execute the filter condition
				if filter, ok := plan.Source.(*planner.PhysicalFilter); ok {
					passes, err := e.evaluateExprAsBool(
						ctx,
						filter.Condition,
						rowMap,
					)
					if err != nil {
						return nil, err
					}
					shouldDelete = passes
				} else {
					// No filter means delete all
					shouldDelete = true
				}
			}

			if shouldDelete {
				deletedCount++
			} else {
				keptRows = append(keptRows, row)
			}
		}

		// Recreate the table with kept rows
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

		// Append kept rows
		for _, row := range keptRows {
			if err := newTable.AppendRow(row); err != nil {
				return nil, err
			}
		}

		rowsAffected = deletedCount
	} else {
		// DELETE without WHERE clause - delete all rows
		rowsAffected = table.RowCount()

		// Drop and recreate empty table
		if err := e.storage.DropTable(plan.Table); err != nil {
			return nil, err
		}

		if _, err := e.storage.CreateTable(plan.Table, table.ColumnTypes()); err != nil {
			return nil, err
		}
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}
