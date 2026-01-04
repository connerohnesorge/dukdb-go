package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// executeMerge executes a MERGE INTO operation.
// MERGE combines INSERT, UPDATE, and DELETE operations based on conditions.
//
// The MERGE operation works by:
// 1. Getting target table from storage
// 2. Executing source plan to get source rows
// 3. For each source row, checking ON condition against target rows
// 4. If matched: executing WHEN MATCHED actions (UPDATE/DELETE)
// 5. If not matched: executing WHEN NOT MATCHED actions (INSERT)
// 6. If not matched by source: executing WHEN NOT MATCHED BY SOURCE actions (UPDATE/DELETE)
// 7. Returning total affected rows
func (e *Executor) executeMerge(
	ctx *ExecutionContext,
	plan *planner.PhysicalMerge,
) (*ExecutionResult, error) {
	// Get the target table from storage
	table, ok := e.storage.GetTable(plan.TargetTable)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Execute source plan to get source rows
	var sourceRows []map[string]any
	if plan.SourcePlan != nil {
		sourceResult, err := e.Execute(ctx.Context, plan.SourcePlan, ctx.Args)
		if err != nil {
			return nil, fmt.Errorf("failed to execute source plan: %w", err)
		}
		sourceRows = sourceResult.Rows
	}

	// Collect all target rows with their RowIDs
	type targetRow struct {
		rowID  storage.RowID
		values map[string]any
	}
	var targetRows []targetRow

	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			rowMap := make(map[string]any)
			for j, col := range plan.TargetTableDef.Columns {
				if j < chunk.ColumnCount() {
					value := chunk.GetValue(i, j)
					// Store with both plain column name and aliased name
					rowMap[col.Name] = value
					if plan.TargetAlias != "" {
						rowMap[plan.TargetAlias+"."+col.Name] = value
					}
				}
			}

			rowID := scanner.GetRowID(i)
			if rowID != nil {
				targetRows = append(targetRows, targetRow{
					rowID:  *rowID,
					values: rowMap,
				})
			}
		}
	}

	// Track which target rows have been matched (to determine NOT MATCHED BY SOURCE)
	matchedTargetRows := make(map[storage.RowID]bool)

	// Track operations for counting affected rows
	var rowsAffected int64

	// Process each source row
	for _, sourceRow := range sourceRows {
		matched := false

		// Check against each target row
		for _, target := range targetRows {
			// Combine source and target rows for condition evaluation
			combinedRow := make(map[string]any)
			for k, v := range target.values {
				combinedRow[k] = v
			}
			for k, v := range sourceRow {
				combinedRow[k] = v
			}

			// Evaluate ON condition
			onMatch, err := e.evaluateExprAsBool(ctx, plan.OnCondition, combinedRow)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate ON condition: %w", err)
			}

			if onMatch {
				matched = true
				matchedTargetRows[target.rowID] = true

				// Execute WHEN MATCHED actions
				for _, action := range plan.WhenMatched {
					// Check additional condition if present
					if action.Cond != nil {
						condMatch, err := e.evaluateExprAsBool(ctx, action.Cond, combinedRow)
						if err != nil {
							return nil, fmt.Errorf("failed to evaluate WHEN MATCHED condition: %w", err)
						}
						if !condMatch {
							continue
						}
					}

					switch action.Type {
					case binder.BoundMergeActionUpdate:
						// Execute UPDATE
						columnValues := make(map[int]any)
						for _, setClause := range action.Update {
							newValue, err := e.evaluateExpr(ctx, setClause.Value, combinedRow)
							if err != nil {
								return nil, fmt.Errorf("failed to evaluate SET expression: %w", err)
							}

							// Cast the new value to the correct column type
							if setClause.ColumnIdx < len(plan.TargetTableDef.Columns) {
								colType := plan.TargetTableDef.Columns[setClause.ColumnIdx].Type
								castedValue, castErr := castValue(newValue, colType)
								if castErr != nil {
									return nil, castErr
								}
								newValue = castedValue
							}

							columnValues[setClause.ColumnIdx] = newValue
						}

						if err := table.UpdateRows([]storage.RowID{target.rowID}, columnValues); err != nil {
							return nil, fmt.Errorf("failed to update row: %w", err)
						}
						rowsAffected++

					case binder.BoundMergeActionDelete:
						// Execute DELETE
						if err := table.DeleteRows([]storage.RowID{target.rowID}); err != nil {
							return nil, fmt.Errorf("failed to delete row: %w", err)
						}
						rowsAffected++

					case binder.BoundMergeActionDoNothing:
						// DO NOTHING - just mark as matched but don't count as affected
						continue

					case binder.BoundMergeActionInsert:
						// INSERT is not valid in WHEN MATCHED clause, skip
						continue
					}

					// Only execute the first matching action
					break
				}

				// Only match with the first target row for this source row
				break
			}
		}

		// If not matched, execute WHEN NOT MATCHED actions (INSERT)
		if !matched {
			for _, action := range plan.WhenNotMatched {
				// Check additional condition if present
				if action.Cond != nil {
					condMatch, err := e.evaluateExprAsBool(ctx, action.Cond, sourceRow)
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate WHEN NOT MATCHED condition: %w", err)
					}
					if !condMatch {
						continue
					}
				}

				switch action.Type {
				case binder.BoundMergeActionInsert:
					// Execute INSERT
					values := make([]any, len(plan.TargetTableDef.Columns))

					// Initialize all values to nil
					for i := range values {
						values[i] = nil
					}

					// Evaluate and set INSERT values
					for i, colIdx := range action.InsertColumns {
						if i < len(action.InsertValues) {
							val, err := e.evaluateExpr(ctx, action.InsertValues[i], sourceRow)
							if err != nil {
								return nil, fmt.Errorf("failed to evaluate INSERT value: %w", err)
							}

							// Cast the value to the correct column type
							if colIdx < len(plan.TargetTableDef.Columns) {
								colType := plan.TargetTableDef.Columns[colIdx].Type
								castedValue, castErr := castValue(val, colType)
								if castErr != nil {
									return nil, castErr
								}
								val = castedValue
							}

							values[colIdx] = val
						}
					}

					// Create a DataChunk and insert
					columnTypes := table.ColumnTypes()
					chunk := storage.NewDataChunkWithCapacity(columnTypes, 1)
					chunk.AppendRow(values)

					if _, err := table.InsertChunk(chunk); err != nil {
						return nil, fmt.Errorf("failed to insert row: %w", err)
					}
					rowsAffected++

				case binder.BoundMergeActionDoNothing:
					// DO NOTHING - don't count as affected
					continue

				case binder.BoundMergeActionUpdate, binder.BoundMergeActionDelete:
					// UPDATE/DELETE are not valid in WHEN NOT MATCHED clause, skip
					continue
				}

				// Only execute the first matching action
				break
			}
		}
	}

	// Process WHEN NOT MATCHED BY SOURCE actions (target rows with no source match)
	if len(plan.WhenNotMatchedBySource) > 0 {
		for _, target := range targetRows {
			if matchedTargetRows[target.rowID] {
				continue // Skip rows that were matched
			}

			for _, action := range plan.WhenNotMatchedBySource {
				// Check additional condition if present
				if action.Cond != nil {
					condMatch, err := e.evaluateExprAsBool(ctx, action.Cond, target.values)
					if err != nil {
						return nil, fmt.Errorf("failed to evaluate WHEN NOT MATCHED BY SOURCE condition: %w", err)
					}
					if !condMatch {
						continue
					}
				}

				switch action.Type {
				case binder.BoundMergeActionUpdate:
					// Execute UPDATE
					columnValues := make(map[int]any)
					for _, setClause := range action.Update {
						newValue, err := e.evaluateExpr(ctx, setClause.Value, target.values)
						if err != nil {
							return nil, fmt.Errorf("failed to evaluate SET expression: %w", err)
						}

						// Cast the new value to the correct column type
						if setClause.ColumnIdx < len(plan.TargetTableDef.Columns) {
							colType := plan.TargetTableDef.Columns[setClause.ColumnIdx].Type
							castedValue, castErr := castValue(newValue, colType)
							if castErr != nil {
								return nil, castErr
							}
							newValue = castedValue
						}

						columnValues[setClause.ColumnIdx] = newValue
					}

					if err := table.UpdateRows([]storage.RowID{target.rowID}, columnValues); err != nil {
						return nil, fmt.Errorf("failed to update row: %w", err)
					}
					rowsAffected++

				case binder.BoundMergeActionDelete:
					// Execute DELETE
					if err := table.DeleteRows([]storage.RowID{target.rowID}); err != nil {
						return nil, fmt.Errorf("failed to delete row: %w", err)
					}
					rowsAffected++

				case binder.BoundMergeActionDoNothing:
					// DO NOTHING - don't count as affected
					continue

				case binder.BoundMergeActionInsert:
					// INSERT is not valid in WHEN NOT MATCHED BY SOURCE clause, skip
					continue
				}

				// Only execute the first matching action
				break
			}
		}
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}
