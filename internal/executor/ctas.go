package executor

import (
	"fmt"

	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

func (e *Executor) executeCreateTableWithCTAS(ctx *ExecutionContext, plan *planner.PhysicalCreateTable) (*ExecutionResult, error) {
	result, err := e.executeCreateTable(ctx, plan)
	if err != nil {
		return result, err
	}
	if plan.AsSelect == nil {
		return result, nil
	}
	schema := plan.Schema
	if plan.Temporary {
		schema = "temp"
	}
	_, stor, _ := e.resolveSchemaTarget(schema)
	return e.executeCTASInsert(ctx, plan, stor)
}

func (e *Executor) executeCTASInsert(ctx *ExecutionContext, plan *planner.PhysicalCreateTable, stor *storage.Storage) (*ExecutionResult, error) {
	selectPlan, err := e.planner.Plan(plan.AsSelect)
	if err != nil {
		return nil, fmt.Errorf("CTAS: failed to plan SELECT: %w", err)
	}
	selectResult, err := e.executeWithContext(ctx, selectPlan)
	if err != nil {
		return nil, fmt.Errorf("CTAS: failed to execute SELECT: %w", err)
	}
	table, ok := stor.GetTable(plan.Table)
	if !ok {
		return nil, fmt.Errorf("CTAS: table %q not found in storage", plan.Table)
	}
	rowsInserted := int64(0)
	for _, row := range selectResult.Rows {
		values := make([]any, len(plan.Columns))
		for i, col := range plan.Columns {
			values[i] = row[col.Name]
		}
		if err := table.AppendRow(values); err != nil {
			return nil, fmt.Errorf("CTAS: failed to insert row: %w", err)
		}
		rowsInserted++
	}
	return &ExecutionResult{RowsAffected: rowsInserted}, nil
}
