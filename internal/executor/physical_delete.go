package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// executeDelete executes a DELETE operation using the RowID/tombstone architecture.
// DELETE works by:
// 1. Scanning the table to find rows matching the WHERE clause
// 2. Collecting RowIDs of matching rows
// 3. Marking those RowIDs as deleted using tombstones (no physical deletion)
// 4. Returning the count of deleted rows
//
// This approach is efficient because:
// - No data is copied (unlike drop/recreate)
// - Deletion is in-place via tombstone marking
// - Supports future MVCC and rollback capabilities
func (e *Executor) executeDelete(
	ctx *ExecutionContext,
	plan *planner.PhysicalDelete,
) (*ExecutionResult, error) {
	// Get the table from storage
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Create a scanner with RowID tracking enabled
	scanner := table.Scan()

	// Collect RowIDs and data of rows to delete
	// We store both RowID and row data for WAL logging (rollback support)
	type deletedRow struct {
		rowID storage.RowID
		data  []any
	}
	var deletedRows []deletedRow

	// Iterate through all rows in the table
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Process each row in the chunk
		for i := 0; i < chunk.Count(); i++ {
			// Convert chunk row to map for WHERE clause evaluation
			// Also collect row data for WAL logging
			rowMap := make(map[string]any)
			rowData := make([]any, len(plan.TableDef.Columns))
			for j, col := range plan.TableDef.Columns {
				if j < chunk.ColumnCount() {
					value := chunk.GetValue(i, j)
					rowMap[col.Name] = value
					rowData[j] = value
				}
			}

			// Evaluate WHERE clause (three-valued logic: NULL comparisons return NULL/false)
			var shouldDelete bool
			if plan.Source != nil {
				// Check if source is a filter (WHERE clause)
				if filter, ok := plan.Source.(*planner.PhysicalFilter); ok {
					match, err := e.evaluateExprAsBool(
						ctx,
						filter.Condition,
						rowMap,
					)
					if err != nil {
						return nil, err
					}
					shouldDelete = match
				} else {
					// No filter means delete all rows
					shouldDelete = true
				}
			} else {
				// No source plan means DELETE all rows
				shouldDelete = true
			}

			if !shouldDelete {
				continue // Skip non-matching rows
			}

			// Get the RowID for this row from the scanner
			// The scanner tracks RowIDs for all rows in the last returned chunk
			rowID := scanner.GetRowID(i)
			if rowID != nil {
				deletedRows = append(deletedRows, deletedRow{
					rowID: *rowID,
					data:  rowData,
				})
			}
		}
	}

	// Extract RowIDs for deletion
	deletedRowIDs := make([]storage.RowID, len(deletedRows))
	for i, row := range deletedRows {
		deletedRowIDs[i] = row.rowID
	}

	// Mark rows as deleted using tombstones (in-place deletion)
	if len(deletedRowIDs) > 0 {
		if err := table.DeleteRows(deletedRowIDs); err != nil {
			return nil, err
		}

		// Record undo operation for transaction rollback
		// Convert storage.RowID to uint64 for undo operation
		undoRowIDs := make([]uint64, len(deletedRowIDs))
		for i, rid := range deletedRowIDs {
			undoRowIDs[i] = uint64(rid)
		}
		e.recordUndo(UndoOperation{
			TableName: plan.Table,
			OpType:    UndoDelete,
			RowIDs:    undoRowIDs,
		})
	}

	// Collect deleted data for RETURNING and WAL
	deletedData := make([][]any, len(deletedRows))
	for i, row := range deletedRows {
		deletedData[i] = row.data
	}

	// WAL logging: Log DELETE entry AFTER successful deletion
	// This ensures atomicity - if the delete fails, no WAL entry is written
	if e.wal != nil && len(deletedRows) > 0 {
		schema := "main" // Default schema
		if plan.TableDef != nil && plan.TableDef.Schema != "" {
			schema = plan.TableDef.Schema
		}

		// Collect row IDs for WAL
		rowIDs := make([]uint64, len(deletedRows))
		for i, row := range deletedRows {
			rowIDs[i] = uint64(row.rowID)
		}

		entry := wal.NewDeleteEntryWithData(
			e.txnID,
			schema,
			plan.Table,
			rowIDs,
			deletedData,
		)
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	// Handle RETURNING clause - use the before (deleted) values
	if len(plan.Returning) > 0 {
		return e.evaluateReturning(ctx, plan.Returning, deletedData, plan.TableDef)
	}

	return &ExecutionResult{
		RowsAffected: int64(len(deletedRows)),
	}, nil
}
