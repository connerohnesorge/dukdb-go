// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// CTEContext holds the context for CTE execution, including work table data.
// This is used to pass the current CTE results to recursive references.
type CTEContext struct {
	// WorkTable holds the current iteration's data for recursive CTEs
	WorkTable []*storage.DataChunk
	// CTEName is the name of the CTE
	CTEName string
}

// PhysicalRecursiveCTEOperator executes a recursive Common Table Expression.
// The algorithm is:
//  1. Execute the base (anchor) plan to produce initial rows (work table)
//  2. Execute the recursive plan using the work table as input
//  3. Append new results to the output and replace the work table
//  4. Repeat until no new rows are produced or max recursion is reached
type PhysicalRecursiveCTEOperator struct {
	plan         *planner.PhysicalRecursiveCTE
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo
	resultChunks []*storage.DataChunk
	resultIndex  int
	executed     bool
	limitErr     error
}

// NewPhysicalRecursiveCTEOperator creates a new PhysicalRecursiveCTEOperator.
func NewPhysicalRecursiveCTEOperator(
	plan *planner.PhysicalRecursiveCTE,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalRecursiveCTEOperator, error) {
	// Get output column types
	outputCols := plan.OutputColumns()
	types := make([]dukdb.TypeInfo, len(outputCols))
	for i, col := range outputCols {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}

	return &PhysicalRecursiveCTEOperator{
		plan:         plan,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		resultChunks: make([]*storage.DataChunk, 0),
		resultIndex:  0,
		executed:     false,
		limitErr:     nil,
	}, nil
}

// Next returns the next DataChunk of results, or nil if no more data.
func (op *PhysicalRecursiveCTEOperator) Next() (*storage.DataChunk, error) {
	// Execute the CTE on first call
	if !op.executed {
		if err := op.execute(); err != nil {
			return nil, err
		}
		op.executed = true
	}

	// Return results one chunk at a time
	if op.resultIndex >= len(op.resultChunks) {
		if op.limitErr != nil {
			err := op.limitErr
			op.limitErr = nil
			return nil, err
		}
		return nil, nil
	}

	chunk := op.resultChunks[op.resultIndex]
	op.resultIndex++
	return chunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalRecursiveCTEOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// execute runs the recursive CTE algorithm.
func (op *PhysicalRecursiveCTEOperator) execute() error {
	// Get column types for creating chunks
	outputCols := op.plan.OutputColumns()
	types := make([]dukdb.Type, len(outputCols))
	for i, col := range outputCols {
		types[i] = col.Type
	}

	// Step 1: Execute the base plan to produce initial rows (work table)
	baseResult, err := op.executor.Execute(
		op.ctx.Context,
		op.plan.BasePlan,
		op.ctx.Args,
	)
	if err != nil {
		return fmt.Errorf("recursive CTE base execution failed: %w", err)
	}

	// Convert base result to chunks
	workTable := op.resultToChunks(baseResult, types)

	// Add base results to output
	for _, chunk := range workTable {
		if chunk.Count() > 0 {
			op.resultChunks = append(op.resultChunks, chunk.Clone())
		}
	}

	// Step 2: Iteratively execute the recursive plan
	iteration := 0
	maxRecursion := op.plan.MaxRecursion
	if maxRecursion <= 0 {
		maxRecursion = 1000 // Default max recursion
	}

	for iteration < maxRecursion {
		// Check if work table is empty
		if len(workTable) == 0 || op.chunksEmpty(workTable) {
			break
		}

		// Execute the recursive plan with the work table as input
		newRows, err := op.executeRecursivePlan(workTable, types)
		if err != nil {
			return fmt.Errorf("recursive CTE iteration %d failed: %w", iteration, err)
		}

		// If no new rows, we're done
		if len(newRows) == 0 || op.chunksEmpty(newRows) {
			break
		}

		// Add new rows to output
		for _, chunk := range newRows {
			if chunk.Count() > 0 {
				op.resultChunks = append(op.resultChunks, chunk.Clone())
			}
		}

		// Replace work table with new rows for next iteration
		workTable = newRows
		iteration++
	}

	// Check if we hit max recursion limit
	if maxRecursion >= 0 && iteration >= maxRecursion {
		op.limitErr = &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("recursion limit exceeded: max %d iterations", maxRecursion),
		}
	}

	return nil
}

// executeRecursivePlan executes the recursive plan with the work table as input.
func (op *PhysicalRecursiveCTEOperator) executeRecursivePlan(
	workTable []*storage.DataChunk,
	types []dukdb.Type,
) ([]*storage.DataChunk, error) {
	// We need to execute the recursive plan, but replace any CTEScan
	// references with data from the work table.
	// This is done by using a special executor context.

	// First, convert work table chunks to ExecutionResult format
	// so the recursive plan's CTEScan can read from it
	workResult := op.chunksToResult(workTable)

	// Create a context with the work table
	cteCtx := &cteExecutionContext{
		executor:   op.executor,
		ctx:        op.ctx,
		cteName:    op.plan.CTEName,
		workResult: workResult,
	}

	// Execute the recursive plan with CTE context
	result, err := cteCtx.executeWithCTE(op.plan.RecursivePlan)
	if err != nil {
		return nil, err
	}

	// Convert result back to chunks
	return op.resultToChunks(result, types), nil
}

// cteExecutionContext provides a context for executing plans with CTE data.
type cteExecutionContext struct {
	executor   *Executor
	ctx        *ExecutionContext
	cteName    string
	workResult *ExecutionResult
}

// executeWithCTE executes a plan, replacing CTEScan nodes with work table data.
func (cte *cteExecutionContext) executeWithCTE(
	plan planner.PhysicalPlan,
) (*ExecutionResult, error) {
	switch p := plan.(type) {
	case *planner.PhysicalCTEScan:
		// If this is a scan of our CTE, return the work table data
		if p.CTEName == cte.cteName {
			// Need to remap column names to use the CTE scan's alias
			// The work result has column names like "id", "name", etc.
			// But the join condition expects "oc.id", "oc.name", etc. (using the alias)
			return cte.remapColumnsWithAlias(cte.workResult, p.Alias)
		}
		// Otherwise, execute normally
		return cte.executor.Execute(cte.ctx.Context, plan, cte.ctx.Args)

	case *planner.PhysicalFilter:
		// Execute child with CTE context, then apply filter
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		return cte.applyFilter(childResult, p)

	case *planner.PhysicalProject:
		// Execute child with CTE context, then apply projection
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		return cte.applyProject(childResult, p)

	case *planner.PhysicalHashJoin:
		// Execute both sides with CTE context
		leftResult, err := cte.executeWithCTE(p.Left)
		if err != nil {
			return nil, err
		}
		rightResult, err := cte.executeWithCTE(p.Right)
		if err != nil {
			return nil, err
		}
		return cte.executor.performJoin(cte.ctx, leftResult, rightResult, p.JoinType, p.Condition)

	case *planner.PhysicalNestedLoopJoin:
		// Execute both sides with CTE context
		leftResult, err := cte.executeWithCTE(p.Left)
		if err != nil {
			return nil, err
		}
		rightResult, err := cte.executeWithCTE(p.Right)
		if err != nil {
			return nil, err
		}
		return cte.executor.performJoin(cte.ctx, leftResult, rightResult, p.JoinType, p.Condition)

	case *planner.PhysicalSort:
		// Execute child with CTE context, then apply sort
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		// Create a temporary sort plan with the child result
		return cte.applySort(childResult, p)

	case *planner.PhysicalLimit:
		// Execute child with CTE context, then apply limit
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		return cte.applyLimit(childResult, p)

	case *planner.PhysicalDistinct:
		// Execute child with CTE context, then apply distinct
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		return cte.applyDistinct(childResult)

	case *planner.PhysicalHashAggregate:
		// Execute child with CTE context, then aggregate
		childResult, err := cte.executeWithCTE(p.Child)
		if err != nil {
			return nil, err
		}
		return cte.applyAggregate(childResult, p)

	default:
		// For other plans, execute normally
		return cte.executor.Execute(cte.ctx.Context, plan, cte.ctx.Args)
	}
}

// applyFilter applies a filter to an execution result.
func (cte *cteExecutionContext) applyFilter(
	result *ExecutionResult,
	plan *planner.PhysicalFilter,
) (*ExecutionResult, error) {
	filteredRows := make([]map[string]any, 0)
	for _, row := range result.Rows {
		passes, err := cte.executor.evaluateExprAsBool(cte.ctx, plan.Condition, row)
		if err != nil {
			return nil, err
		}
		if passes {
			filteredRows = append(filteredRows, row)
		}
	}
	return &ExecutionResult{
		Rows:    filteredRows,
		Columns: result.Columns,
	}, nil
}

// applyProject applies a projection to an execution result.
func (cte *cteExecutionContext) applyProject(
	result *ExecutionResult,
	plan *planner.PhysicalProject,
) (*ExecutionResult, error) {
	columns := make([]string, len(plan.Expressions))
	for i := range plan.Expressions {
		if i < len(plan.Aliases) && plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	projectedRows := make([]map[string]any, len(result.Rows))
	for i, row := range result.Rows {
		projectedRow := make(map[string]any)
		for j, expr := range plan.Expressions {
			val, err := cte.executor.evaluateExpr(cte.ctx, expr, row)
			if err != nil {
				return nil, err
			}
			projectedRow[columns[j]] = val
		}
		projectedRows[i] = projectedRow
	}

	return &ExecutionResult{
		Rows:    projectedRows,
		Columns: columns,
	}, nil
}

// applySort applies sorting to an execution result.
func (cte *cteExecutionContext) applySort(
	result *ExecutionResult,
	plan *planner.PhysicalSort,
) (*ExecutionResult, error) {
	rows := make([]map[string]any, len(result.Rows))
	copy(rows, result.Rows)

	// Use insertion sort for simplicity (matches executor.go)
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			cmp, err := cte.executor.compareRows(cte.ctx, rows[j-1], rows[j], plan.OrderBy)
			if err != nil {
				return nil, err
			}
			if cmp <= 0 {
				break
			}
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: result.Columns,
	}, nil
}

// applyLimit applies limit/offset to an execution result.
func (cte *cteExecutionContext) applyLimit(
	result *ExecutionResult,
	plan *planner.PhysicalLimit,
) (*ExecutionResult, error) {
	rows := result.Rows

	// Apply offset
	if plan.Offset > 0 {
		if int(plan.Offset) >= len(rows) {
			rows = []map[string]any{}
		} else {
			rows = rows[plan.Offset:]
		}
	}

	// Apply limit
	if plan.Limit >= 0 && int(plan.Limit) < len(rows) {
		rows = rows[:plan.Limit]
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: result.Columns,
	}, nil
}

// applyDistinct applies distinct to an execution result.
func (cte *cteExecutionContext) applyDistinct(
	result *ExecutionResult,
) (*ExecutionResult, error) {
	seen := make(map[string]bool)
	distinctRows := make([]map[string]any, 0)

	for _, row := range result.Rows {
		key := formatRowMap(row)
		if !seen[key] {
			seen[key] = true
			distinctRows = append(distinctRows, row)
		}
	}

	return &ExecutionResult{
		Rows:    distinctRows,
		Columns: result.Columns,
	}, nil
}

// applyAggregate applies aggregation to an execution result.
func (cte *cteExecutionContext) applyAggregate(
	result *ExecutionResult,
	plan *planner.PhysicalHashAggregate,
) (*ExecutionResult, error) {
	// Group rows by group-by expressions
	type groupKey string
	groups := make(map[groupKey][]map[string]any)
	groupOrder := make([]groupKey, 0)

	for _, row := range result.Rows {
		// Compute group key
		keyParts := make([]any, len(plan.GroupBy))
		for i, expr := range plan.GroupBy {
			val, err := cte.executor.evaluateExpr(cte.ctx, expr, row)
			if err != nil {
				return nil, err
			}
			keyParts[i] = val
		}
		key := groupKey(formatGroupKey(keyParts))

		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], row)
	}

	// If no groups and no rows, create single empty group
	if len(groups) == 0 && len(plan.GroupBy) == 0 {
		groupOrder = append(groupOrder, "")
		groups[""] = []map[string]any{}
	}

	// Build result columns
	numGroupBy := len(plan.GroupBy)
	numAgg := len(plan.Aggregates)
	columns := make([]string, numGroupBy+numAgg)

	for i := range plan.GroupBy {
		if i < len(plan.Aliases) && plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = fmt.Sprintf("col%d", i)
		}
	}
	for i := range plan.Aggregates {
		if numGroupBy+i < len(plan.Aliases) && plan.Aliases[numGroupBy+i] != "" {
			columns[numGroupBy+i] = plan.Aliases[numGroupBy+i]
		} else {
			columns[numGroupBy+i] = fmt.Sprintf("col%d", numGroupBy+i)
		}
	}

	// Compute aggregates for each group
	aggResult := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, len(groupOrder)),
	}

	for i, key := range groupOrder {
		groupRows := groups[key]
		row := make(map[string]any)

		// Add group-by values
		if len(groupRows) > 0 {
			for j, expr := range plan.GroupBy {
				val, _ := cte.executor.evaluateExpr(cte.ctx, expr, groupRows[0])
				row[columns[j]] = val
			}
		}

		// Compute aggregates
		for j, expr := range plan.Aggregates {
			val, err := cte.executor.computeAggregate(cte.ctx, expr, groupRows)
			if err != nil {
				return nil, err
			}
			row[columns[numGroupBy+j]] = val
		}

		aggResult.Rows[i] = row
	}

	return aggResult, nil
}

// remapColumnsWithAlias creates a new ExecutionResult with column names prefixed by the alias.
// This is needed because the work table has column names like "id", "name", etc.
// but the join condition expects "alias.id", "alias.name", etc.
func (cte *cteExecutionContext) remapColumnsWithAlias(
	result *ExecutionResult,
	alias string,
) (*ExecutionResult, error) {
	if alias == "" {
		return result, nil
	}

	// Create new column names with alias prefix
	newColumns := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		newColumns[i] = alias + "." + col
	}

	// Create new rows with aliased column names
	// Also keep the original column names for compatibility
	newRows := make([]map[string]any, len(result.Rows))
	for i, row := range result.Rows {
		newRow := make(map[string]any)
		for j, col := range result.Columns {
			val := row[col]
			// Add with aliased name (e.g., "oc.id")
			newRow[newColumns[j]] = val
			// Also keep the original name (e.g., "id") for expressions that use unqualified names
			newRow[col] = val
		}
		newRows[i] = newRow
	}

	return &ExecutionResult{
		Columns: newColumns,
		Rows:    newRows,
	}, nil
}

// resultToChunks converts an ExecutionResult to DataChunks.
func (op *PhysicalRecursiveCTEOperator) resultToChunks(
	result *ExecutionResult,
	types []dukdb.Type,
) []*storage.DataChunk {
	if len(result.Rows) == 0 {
		return nil
	}

	chunks := make([]*storage.DataChunk, 0)
	batchSize := storage.StandardVectorSize

	for i := 0; i < len(result.Rows); i += batchSize {
		end := i + batchSize
		if end > len(result.Rows) {
			end = len(result.Rows)
		}

		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			row := result.Rows[j]
			values := make([]any, len(result.Columns))
			for k, col := range result.Columns {
				values[k] = row[col]
			}
			chunk.AppendRow(values)
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

// chunksToResult converts DataChunks to an ExecutionResult.
func (op *PhysicalRecursiveCTEOperator) chunksToResult(
	chunks []*storage.DataChunk,
) *ExecutionResult {
	outputCols := op.plan.OutputColumns()
	columns := make([]string, len(outputCols))
	for i, col := range outputCols {
		if col.Column != "" {
			columns[i] = col.Column
		} else {
			columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	rows := make([]map[string]any, 0)
	for _, chunk := range chunks {
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j, col := range columns {
				row[col] = chunk.GetValue(i, j)
			}
			rows = append(rows, row)
		}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: columns,
	}
}

// chunksEmpty checks if all chunks are empty.
func (op *PhysicalRecursiveCTEOperator) chunksEmpty(chunks []*storage.DataChunk) bool {
	for _, chunk := range chunks {
		if chunk.Count() > 0 {
			return false
		}
	}
	return true
}

// PhysicalCTEScanOperator implements scanning of a CTE.
// This is used when the main query references a CTE (non-recursive case).
type PhysicalCTEScanOperator struct {
	plan     *planner.PhysicalCTEScan
	executor *Executor
	ctx      *ExecutionContext
	types    []dukdb.TypeInfo
	chunks   []*storage.DataChunk
	index    int
	executed bool
}

// NewPhysicalCTEScanOperator creates a new PhysicalCTEScanOperator.
func NewPhysicalCTEScanOperator(
	plan *planner.PhysicalCTEScan,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalCTEScanOperator, error) {
	// Get output column types
	outputCols := plan.OutputColumns()
	types := make([]dukdb.TypeInfo, len(outputCols))
	for i, col := range outputCols {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}

	return &PhysicalCTEScanOperator{
		plan:     plan,
		executor: executor,
		ctx:      ctx,
		types:    types,
		chunks:   make([]*storage.DataChunk, 0),
		index:    0,
		executed: false,
	}, nil
}

// Next returns the next DataChunk of results, or nil if no more data.
func (op *PhysicalCTEScanOperator) Next() (*storage.DataChunk, error) {
	// Execute the CTE plan on first call
	if !op.executed {
		if err := op.execute(); err != nil {
			return nil, err
		}
		op.executed = true
	}

	// Return results one chunk at a time
	if op.index >= len(op.chunks) {
		return nil, nil
	}

	chunk := op.chunks[op.index]
	op.index++
	return chunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalCTEScanOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// execute runs the CTE plan and stores results.
func (op *PhysicalCTEScanOperator) execute() error {
	// If this is a recursive self-reference, the work table should be provided
	// through the CTE context (handled by PhysicalRecursiveCTEOperator)
	if op.plan.IsRecursive && op.plan.CTEPlan == nil {
		// This should not happen - recursive self-references are handled
		// by the PhysicalRecursiveCTEOperator
		return &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "recursive CTE self-reference without work table",
		}
	}

	// For non-recursive CTEs, execute the CTE plan
	if op.plan.CTEPlan != nil {
		result, err := op.executor.Execute(
			op.ctx.Context,
			op.plan.CTEPlan,
			op.ctx.Args,
		)
		if err != nil {
			return err
		}

		// Convert result to chunks
		op.chunks = op.resultToChunks(result)
	}

	return nil
}

// resultToChunks converts an ExecutionResult to DataChunks.
func (op *PhysicalCTEScanOperator) resultToChunks(
	result *ExecutionResult,
) []*storage.DataChunk {
	if len(result.Rows) == 0 {
		return nil
	}

	outputCols := op.plan.OutputColumns()
	types := make([]dukdb.Type, len(outputCols))
	for i, col := range outputCols {
		types[i] = col.Type
	}

	chunks := make([]*storage.DataChunk, 0)
	batchSize := storage.StandardVectorSize

	for i := 0; i < len(result.Rows); i += batchSize {
		end := i + batchSize
		if end > len(result.Rows) {
			end = len(result.Rows)
		}

		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			row := result.Rows[j]
			values := make([]any, len(result.Columns))
			for k, col := range result.Columns {
				values[k] = row[col]
			}
			chunk.AppendRow(values)
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

// executeRecursiveCTE executes a recursive CTE physical plan.
func (e *Executor) executeRecursiveCTE(
	ctx *ExecutionContext,
	plan *planner.PhysicalRecursiveCTE,
) (*ExecutionResult, error) {
	op, err := NewPhysicalRecursiveCTEOperator(plan, e, ctx)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into result
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(plan.Columns)),
	}

	for i, col := range plan.Columns {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	for {
		chunk, err := op.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j, col := range result.Columns {
				row[col] = chunk.GetValue(i, j)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// executeCTEScan executes a CTE scan physical plan.
func (e *Executor) executeCTEScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalCTEScan,
) (*ExecutionResult, error) {
	op, err := NewPhysicalCTEScanOperator(plan, e, ctx)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into result
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(plan.Columns)),
	}

	for i, col := range plan.Columns {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	for {
		chunk, err := op.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j, col := range result.Columns {
				row[col] = chunk.GetValue(i, j)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}
