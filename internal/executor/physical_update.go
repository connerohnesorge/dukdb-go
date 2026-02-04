package executor

import (
	"fmt"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// updateLockTimeout is the default timeout for acquiring row locks in UPDATE.
const updateLockTimeout = 5 * time.Second

// executeUpdate executes an UPDATE operation using the RowID/tombstone architecture.
// UPDATE works by:
// 1. Scanning the table to find rows matching the WHERE clause
// 2. For each matching row, evaluating SET expressions using current row values
// 3. Collecting RowIDs and their new column values
// 4. Updating rows in-place using storage.UpdateRows() (no data copying)
// 5. Returning the count of updated rows
//
// This approach is efficient because:
// - No data is copied (unlike drop/recreate)
// - Updates are in-place via RowID lookup
// - Supports expressions in SET clause that reference current row values (e.g., SET x = x + 1)
// - Supports future MVCC and rollback capabilities
func (e *Executor) executeUpdate(
	ctx *ExecutionContext,
	plan *planner.PhysicalUpdate,
) (*ExecutionResult, error) {
	// Get the table from storage
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Create a scanner with visibility support if MVCC is enabled
	// This ensures UPDATE only sees rows visible to the current transaction
	var scanner *storage.TableScanner
	if e.visibility != nil && e.txnCtx != nil {
		scanner = table.ScanWithVisibility(e.visibility, e.txnCtx)
	} else {
		scanner = table.Scan()
	}

	// Collect updates: each entry maps a RowID to its new column values
	// We need per-row updates because SET expressions may reference current row values
	type rowUpdate struct {
		rowID        storage.RowID
		columnValues map[int]any // column index -> new value
		beforeValues []any       // values before update for WAL logging
	}
	var updates []rowUpdate

	// Iterate through all rows in the table
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Process each row in the chunk
		for i := 0; i < chunk.Count(); i++ {
			// Convert chunk row to map for WHERE clause and SET expression evaluation
			// Also collect before values for WAL logging
			rowMap := make(map[string]any)
			beforeValues := make([]any, len(plan.TableDef.Columns))
			for j, col := range plan.TableDef.Columns {
				if j < chunk.ColumnCount() {
					value := chunk.GetValue(i, j)
					rowMap[col.Name] = value
					beforeValues[j] = value
				}
			}

			// Evaluate WHERE clause (three-valued logic: NULL comparisons return NULL/false)
			var shouldUpdate bool
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
					shouldUpdate = match
				} else {
					// No filter means update all rows
					shouldUpdate = true
				}
			} else {
				// No source plan means UPDATE all rows
				shouldUpdate = true
			}

			if !shouldUpdate {
				continue // Skip non-matching rows
			}

			// Get the RowID for this row from the scanner
			rowID := scanner.GetRowID(i)
			if rowID == nil {
				// Skip rows without RowID (shouldn't happen in normal operation)
				continue
			}

			// Evaluate SET expressions for this specific row
			columnValues := make(map[int]any)
			for _, setClause := range plan.Set {
				// Evaluate the SET expression using current row values
				newValue, err := e.evaluateExpr(
					ctx,
					setClause.Value,
					rowMap,
				)
				if err != nil {
					return nil, err
				}

				// Cast the new value to the correct column type
				if setClause.ColumnIdx < len(plan.TableDef.Columns) {
					colType := plan.TableDef.Columns[setClause.ColumnIdx].Type
					castedValue, castErr := castValue(newValue, colType)
					if castErr != nil {
						return nil, castErr
					}
					newValue = castedValue
				}

				// Store the new value for this column
				columnValues[setClause.ColumnIdx] = newValue

				// Update rowMap for subsequent SET clauses that might reference this column
				// This handles cases like: UPDATE t SET x = 1, y = x WHERE ...
				if setClause.ColumnIdx < len(plan.TableDef.Columns) {
					rowMap[plan.TableDef.Columns[setClause.ColumnIdx].Name] = newValue
				}
			}

			// Record this row's update
			updates = append(updates, rowUpdate{
				rowID:        *rowID,
				columnValues: columnValues,
				beforeValues: beforeValues,
			})
		}
	}

	// For SERIALIZABLE isolation, acquire locks and register writes before modifying data
	// This ensures proper conflict detection at commit time
	if len(updates) > 0 {
		// Acquire row locks for SERIALIZABLE isolation
		if e.lockManager != nil && e.txnCtx != nil {
			for _, update := range updates {
				err := e.lockManager.Lock(
					e.txnCtx.GetTxnID(),
					plan.Table,
					fmt.Sprintf("%d", update.rowID),
					updateLockTimeout,
				)
				if err != nil {
					return nil, fmt.Errorf("failed to acquire lock for update: %w", err)
				}
			}
		}

		// Register writes in conflict detector for SERIALIZABLE isolation
		if e.conflictDetector != nil && e.txnCtx != nil {
			for _, update := range updates {
				e.conflictDetector.RegisterWrite(
					e.txnCtx.GetTxnID(),
					plan.Table,
					fmt.Sprintf("%d", update.rowID),
				)
			}
		}
	}

	// Apply all updates using in-place modification
	// Since UpdateRows() applies the same values to all rows, we need to call it per row
	// for expressions that produce row-specific values (like SET x = x + 1)
	for _, update := range updates {
		if err := table.UpdateRows([]storage.RowID{update.rowID}, update.columnValues); err != nil {
			return nil, err
		}
	}

	// Record undo operation for transaction rollback
	if len(updates) > 0 {
		// Build the before-image for all updated rows
		// Map column index -> slice of before values (one per row)
		beforeImage := make(map[int][]any)

		// First, collect all column indices that were updated
		columnIndices := make(map[int]bool)
		for _, update := range updates {
			for colIdx := range update.columnValues {
				columnIndices[colIdx] = true
			}
		}

		// Initialize slices for each column
		for colIdx := range columnIndices {
			beforeImage[colIdx] = make([]any, len(updates))
		}

		// Collect RowIDs and before values
		undoRowIDs := make([]uint64, len(updates))
		for i, update := range updates {
			undoRowIDs[i] = uint64(update.rowID)
			// For each updated column, store the before value
			for colIdx := range columnIndices {
				if colIdx < len(update.beforeValues) {
					beforeImage[colIdx][i] = update.beforeValues[colIdx]
				}
			}
		}

		e.recordUndo(UndoOperation{
			TableName:   plan.Table,
			OpType:      UndoUpdate,
			RowIDs:      undoRowIDs,
			BeforeImage: beforeImage,
		})
	}

	// Collect after values for RETURNING and WAL
	allAfterValues := make([][]any, len(updates))
	for i, update := range updates {
		// Construct after values by applying updates to before values
		after := make([]any, len(update.beforeValues))
		copy(after, update.beforeValues)
		for colIdx, newVal := range update.columnValues {
			if colIdx < len(after) {
				after[colIdx] = newVal
			}
		}
		allAfterValues[i] = after
	}

	// WAL logging: Log UPDATE entry AFTER successful update
	// This ensures atomicity - if the update fails, no WAL entry is written
	if e.wal != nil && len(updates) > 0 {
		schema := "main" // Default schema
		if plan.TableDef != nil && plan.TableDef.Schema != "" {
			schema = plan.TableDef.Schema
		}

		// Collect column indices that were updated
		var columnIdxs []int
		if len(updates) > 0 {
			for idx := range updates[0].columnValues {
				columnIdxs = append(columnIdxs, idx)
			}
		}

		// Collect row IDs, before values, and after values for WAL
		rowIDs := make([]uint64, len(updates))
		beforeValues := make([][]any, len(updates))

		for i, update := range updates {
			rowIDs[i] = uint64(update.rowID)
			beforeValues[i] = update.beforeValues
		}

		entry := wal.NewUpdateEntryWithBeforeValues(
			e.txnID,
			schema,
			plan.Table,
			rowIDs,
			columnIdxs,
			beforeValues,
			allAfterValues,
		)
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	if len(updates) > 0 {
		e.invalidateQueryCache(plan.Table)
	}

	// Handle RETURNING clause - use the after values
	if len(plan.Returning) > 0 {
		return e.evaluateReturning(ctx, plan.Returning, allAfterValues, plan.TableDef)
	}

	return &ExecutionResult{
		RowsAffected: int64(len(updates)),
	}, nil
}
