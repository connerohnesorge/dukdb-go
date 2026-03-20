package executor

import (
	"fmt"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// defaultLockTimeout is the default timeout for acquiring row locks
// in SERIALIZABLE isolation level.
const defaultLockTimeout = 5 * time.Second

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

	// Create a scanner with visibility support if MVCC is enabled
	// This ensures DELETE only sees rows visible to the current transaction
	var scanner *storage.TableScanner
	if e.visibility != nil && e.txnCtx != nil {
		scanner = table.ScanWithVisibility(e.visibility, e.txnCtx)
	} else {
		scanner = table.Scan()
	}

	// Collect RowIDs and data of rows to delete
	// We store both RowID and row data for WAL logging (rollback support)
	type deletedRow struct {
		rowID storage.RowID
		data  []any
	}
	var deletedRows []deletedRow

	// Handle IN subqueries and other complex sources that have been converted to joins
	// by executing the source plan first to get matching rows
	if plan.Source != nil {
		if _, isFilter := plan.Source.(*planner.PhysicalFilter); !isFilter {
			// Source is a join (e.g., for IN subquery) - execute it to get matching rows
			sourceResult, err := e.executeWithContext(ctx, plan.Source)
			if err != nil {
				return nil, err
			}

			// Build a map of matching row keys from the source result
			matchingKeys := make(map[string]bool)
			for _, row := range sourceResult.Rows {
				key := buildRowKey(row, plan.TableDef.Columns)
				matchingKeys[key] = true
			}

			// Now scan the table and delete matching rows
			for {
				chunk := scanner.Next()
				if chunk == nil {
					break
				}

				for i := 0; i < chunk.Count(); i++ {
					rowData := make([]any, len(plan.TableDef.Columns))
					rowMap := make(map[string]any)
					for j, col := range plan.TableDef.Columns {
						if j < chunk.ColumnCount() {
							value := chunk.GetValue(i, j)
							rowData[j] = value
							rowMap[col.Name] = value
						}
					}

					// Check if this row matches any key from the source result
					key := buildRowKey(rowMap, plan.TableDef.Columns)
					if matchingKeys[key] {
						rowID := scanner.GetRowID(i)
						if rowID != nil {
							deletedRows = append(deletedRows, deletedRow{
								rowID: *rowID,
								data:  rowData,
							})
						}
					}
				}
			}

			// Skip the normal scan loop since we've already processed all rows
			goto doneScanning
		}
	}

	// Iterate through all rows in the table (for simple filters or no filter)
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
				// Source must be a filter (we handled joins above)
				filter := plan.Source.(*planner.PhysicalFilter)
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

doneScanning:

	// Check FK constraints - ensure no child rows reference the rows being deleted
	if plan.TableDef != nil {
		for _, dr := range deletedRows {
			keyColumns := make([]string, len(plan.TableDef.Columns))
			for i, col := range plan.TableDef.Columns {
				keyColumns[i] = col.Name
			}
			if err := e.checkNoChildReferences(plan.Table, plan.TableDef, dr.data, keyColumns); err != nil {
				return nil, err
			}
		}
	}

	// Extract RowIDs for deletion
	deletedRowIDs := make([]storage.RowID, len(deletedRows))
	for i, row := range deletedRows {
		deletedRowIDs[i] = row.rowID
	}

	// For SERIALIZABLE isolation, acquire locks and register writes before modifying data
	// This ensures proper conflict detection at commit time
	if len(deletedRowIDs) > 0 {
		// Acquire row locks for SERIALIZABLE isolation
		if e.lockManager != nil && e.txnCtx != nil {
			for _, rid := range deletedRowIDs {
				err := e.lockManager.Lock(
					e.txnCtx.GetTxnID(),
					plan.Table,
					fmt.Sprintf("%d", rid),
					defaultLockTimeout,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to acquire lock for delete: %w", err)
				}
			}
		}

		// Register writes in conflict detector for SERIALIZABLE isolation
		if e.conflictDetector != nil && e.txnCtx != nil {
			for _, rid := range deletedRowIDs {
				e.conflictDetector.RegisterWrite(
					e.txnCtx.GetTxnID(),
					plan.Table,
					fmt.Sprintf("%d", rid),
				)
			}
		}
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

	if len(deletedRows) > 0 {
		e.invalidateQueryCache(plan.Table)
	}

	// Handle RETURNING clause - use the before (deleted) values
	if len(plan.Returning) > 0 {
		return e.evaluateReturning(ctx, plan.Returning, deletedData, plan.TableDef)
	}

	return &ExecutionResult{
		RowsAffected: int64(len(deletedRows)),
	}, nil
}

// buildRowKey creates a unique key for a row based on its column values
func buildRowKey(row map[string]any, columns []*catalog.ColumnDef) string {
	key := ""
	for _, col := range columns {
		if val, ok := row[col.Name]; ok {
			key += fmt.Sprintf("%v:", val)
		}
	}
	return key
}
