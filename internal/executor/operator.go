// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// ExecutionContext holds context for query execution.
type ExecutionContext struct {
	Context context.Context
	Args    []driver.NamedValue
}

// ExecutionResult holds the result of query execution.
type ExecutionResult struct {
	Rows         []map[string]any
	Columns      []string
	RowsAffected int64
}

// GlobalOperatorState holds global state shared across operator instances.
type GlobalOperatorState interface{}

// LocalOperatorState holds local state for a single operator instance.
type LocalOperatorState interface{}

// PhysicalOperator is the interface for physical operators that produce DataChunks.
// This interface is used by the execution engine to iterate through query results.
type PhysicalOperator interface {
	// Next returns the next DataChunk of results, or nil if no more data.
	Next() (*storage.DataChunk, error)

	// GetTypes returns the TypeInfo for each column produced by this operator.
	GetTypes() []dukdb.TypeInfo
}

// Source produces data chunks.
type Source interface {
	GetData(
		ctx *ExecutionContext,
		chunk *storage.DataChunk,
		state LocalOperatorState,
	) (bool, error)
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
}

// Operator transforms data chunks.
type Operator interface {
	Execute(
		ctx *ExecutionContext,
		input *storage.DataChunk,
		output *storage.DataChunk,
		state LocalOperatorState,
	) error
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
}

// Sink consumes data chunks.
type Sink interface {
	Sink(
		ctx *ExecutionContext,
		chunk *storage.DataChunk,
		state LocalOperatorState,
	) error
	Combine(
		ctx *ExecutionContext,
		gstate GlobalOperatorState,
		lstate LocalOperatorState,
	) error
	Finalize(
		ctx *ExecutionContext,
		gstate GlobalOperatorState,
	) error
	GetGlobalState() GlobalOperatorState
	GetLocalState(
		gstate GlobalOperatorState,
	) LocalOperatorState
}

// Executor executes physical plans.
type Executor struct {
	catalog *catalog.Catalog
	storage *storage.Storage
	planner *planner.Planner
	wal     *wal.Writer // WAL writer for logging DML operations (optional, may be nil)
	txnID   uint64      // Current transaction ID for WAL entries
}

// NewExecutor creates a new Executor.
func NewExecutor(
	cat *catalog.Catalog,
	stor *storage.Storage,
) *Executor {
	return &Executor{
		catalog: cat,
		storage: stor,
		planner: planner.NewPlanner(cat),
	}
}

// SetWAL sets the WAL writer for logging DML operations.
// If set to nil, WAL logging is disabled.
func (e *Executor) SetWAL(w *wal.Writer) {
	e.wal = w
}

// SetTxnID sets the current transaction ID for WAL entries.
func (e *Executor) SetTxnID(txnID uint64) {
	e.txnID = txnID
}

// Execute executes a physical plan and returns the result.
func (e *Executor) Execute(
	ctx context.Context,
	plan planner.PhysicalPlan,
	args []driver.NamedValue,
) (*ExecutionResult, error) {
	execCtx := &ExecutionContext{
		Context: ctx,
		Args:    args,
	}

	switch p := plan.(type) {
	case *planner.PhysicalScan:
		return e.executeScan(execCtx, p)
	case *planner.PhysicalVirtualTableScan:
		return e.executeVirtualTableScan(execCtx, p)
	case *planner.PhysicalFilter:
		return e.executeFilter(execCtx, p)
	case *planner.PhysicalProject:
		return e.executeProject(execCtx, p)
	case *planner.PhysicalHashJoin:
		return e.executeHashJoin(execCtx, p)
	case *planner.PhysicalNestedLoopJoin:
		return e.executeNestedLoopJoin(execCtx, p)
	case *planner.PhysicalHashAggregate:
		return e.executeHashAggregate(execCtx, p)
	case *planner.PhysicalSort:
		return e.executeSort(execCtx, p)
	case *planner.PhysicalLimit:
		return e.executeLimit(execCtx, p)
	case *planner.PhysicalDistinct:
		return e.executeDistinct(execCtx, p)
	case *planner.PhysicalInsert:
		return e.executeInsert(execCtx, p)
	case *planner.PhysicalUpdate:
		return e.executeUpdate(execCtx, p)
	case *planner.PhysicalDelete:
		return e.executeDelete(execCtx, p)
	case *planner.PhysicalCreateTable:
		return e.executeCreateTable(execCtx, p)
	case *planner.PhysicalDropTable:
		return e.executeDropTable(execCtx, p)
	case *planner.PhysicalDummyScan:
		return e.executeDummyScan(execCtx, p)
	case *planner.PhysicalBegin:
		return e.executeBegin(execCtx, p)
	case *planner.PhysicalCommit:
		return e.executeCommit(execCtx, p)
	case *planner.PhysicalRollback:
		return e.executeRollback(execCtx, p)
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "unsupported physical plan type",
		}
	}
}

// collectResults collects all chunks from a source into a result set.
func (e *Executor) collectResults(
	ctx *ExecutionContext,
	source func(chunk *storage.DataChunk) (bool, error),
	outputCols []planner.ColumnBinding,
) (*ExecutionResult, error) {
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(outputCols)),
	}

	for i, col := range outputCols {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Create initial types slice based on output columns
	types := make([]dukdb.Type, len(outputCols))
	for i, col := range outputCols {
		types[i] = col.Type
	}

	chunk := storage.NewDataChunk(types)

	for {
		chunk.Reset()
		hasMore, err := source(chunk)
		if err != nil {
			return nil, err
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j := range len(outputCols) {
				row[result.Columns[j]] = chunk.GetValue(
					i,
					j,
				)
			}
			result.Rows = append(result.Rows, row)
		}

		if !hasMore {
			break
		}
	}

	return result, nil
}

func (e *Executor) executeScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalScan,
) (*ExecutionResult, error) {
	table, ok := e.storage.GetTable(
		plan.TableName,
	)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	scanner := table.Scan()
	outputCols := plan.OutputColumns()

	return e.collectResults(
		ctx,
		func(output *storage.DataChunk) (bool, error) {
			chunk := scanner.Next()
			if chunk == nil {
				return false, nil
			}

			// Copy data to output
			for i := 0; i < chunk.Count(); i++ {
				values := make(
					[]any,
					len(outputCols),
				)
				for j, col := range outputCols {
					values[j] = chunk.GetValue(
						i,
						col.ColumnIdx,
					)
				}
				output.AppendRow(values)
			}

			return true, nil
		},
		outputCols,
	)
}

func (e *Executor) executeVirtualTableScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalVirtualTableScan,
) (*ExecutionResult, error) {
	vt := plan.VirtualTable
	if vt == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "virtual table not found",
		}
	}

	// Get the underlying virtual table and scan it
	it, err := vt.VirtualTable().Scan()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"failed to scan virtual table: %v",
				err,
			),
		}
	}
	defer func() {
		_ = it.Close()
	}()

	outputCols := plan.OutputColumns()
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: make([]string, len(outputCols)),
	}

	// Build column names
	for i, col := range outputCols {
		if col.Column != "" {
			result.Columns[i] = col.Column
		} else {
			result.Columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Iterate over the virtual table
	for it.Next() {
		values := it.Values()
		row := make(map[string]any)

		// Apply projections if specified
		if plan.Projections != nil {
			for i, idx := range plan.Projections {
				if idx < len(values) {
					row[result.Columns[i]] = values[idx]
				}
			}
		} else {
			// No projections - use all columns
			for i := range outputCols {
				if i < len(values) {
					row[result.Columns[i]] = values[i]
				}
			}
		}

		result.Rows = append(result.Rows, row)
	}

	if err := it.Err(); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"error iterating virtual table: %v",
				err,
			),
		}
	}

	return result, nil
}

func (e *Executor) executeFilter(
	ctx *ExecutionContext,
	plan *planner.PhysicalFilter,
) (*ExecutionResult, error) {
	// First execute child
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Filter rows
	filteredRows := make([]map[string]any, 0)
	for _, row := range childResult.Rows {
		passes, err := e.evaluateExprAsBool(
			ctx,
			plan.Condition,
			row,
		)
		if err != nil {
			return nil, err
		}
		if passes {
			filteredRows = append(
				filteredRows,
				row,
			)
		}
	}

	return &ExecutionResult{
		Rows:    filteredRows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeProject(
	ctx *ExecutionContext,
	plan *planner.PhysicalProject,
) (*ExecutionResult, error) {
	// Execute child
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Build column names
	columns := make(
		[]string,
		len(plan.Expressions),
	)
	for i := range plan.Expressions {
		if i < len(plan.Aliases) &&
			plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = "col" + string(rune('0'+i))
		}
	}

	// Project rows
	projectedRows := make(
		[]map[string]any,
		len(childResult.Rows),
	)
	for i, row := range childResult.Rows {
		projectedRow := make(map[string]any)
		for j, expr := range plan.Expressions {
			val, err := e.evaluateExpr(
				ctx,
				expr,
				row,
			)
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

func (e *Executor) executeHashJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalHashJoin,
) (*ExecutionResult, error) {
	// Execute left and right children
	leftResult, err := e.Execute(
		ctx.Context,
		plan.Left,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.Execute(
		ctx.Context,
		plan.Right,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Build hash table from right side (build side)
	// For now, use simple nested loop join
	return e.performJoin(
		ctx,
		leftResult,
		rightResult,
		plan.JoinType,
		plan.Condition,
	)
}

func (e *Executor) executeNestedLoopJoin(
	ctx *ExecutionContext,
	plan *planner.PhysicalNestedLoopJoin,
) (*ExecutionResult, error) {
	leftResult, err := e.Execute(
		ctx.Context,
		plan.Left,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	rightResult, err := e.Execute(
		ctx.Context,
		plan.Right,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	return e.performJoin(
		ctx,
		leftResult,
		rightResult,
		plan.JoinType,
		plan.Condition,
	)
}

func (e *Executor) performJoin(
	ctx *ExecutionContext,
	left, right *ExecutionResult,
	joinType planner.JoinType,
	condition interface{},
) (*ExecutionResult, error) {
	// Combine column names
	columns := make(
		[]string,
		0,
		len(left.Columns)+len(right.Columns),
	)
	columns = append(columns, left.Columns...)
	columns = append(columns, right.Columns...)

	result := &ExecutionResult{
		Columns: columns,
		Rows:    make([]map[string]any, 0),
	}

	// Nested loop join
	for _, leftRow := range left.Rows {
		matched := false
		for _, rightRow := range right.Rows {
			// Combine rows
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for k, v := range rightRow {
				combinedRow[k] = v
			}

			// Check condition
			if condition != nil {
				passes, err := e.evaluateExprAsBool(
					ctx,
					condition,
					combinedRow,
				)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue
				}
			}

			matched = true
			result.Rows = append(
				result.Rows,
				combinedRow,
			)
		}

		// Handle left/full outer join - add left row with NULLs for right
		if !matched &&
			(joinType == planner.JoinTypeLeft || joinType == planner.JoinTypeFull) {
			combinedRow := make(map[string]any)
			for k, v := range leftRow {
				combinedRow[k] = v
			}
			for _, col := range right.Columns {
				combinedRow[col] = nil
			}
			result.Rows = append(
				result.Rows,
				combinedRow,
			)
		}
	}

	// Handle right outer join - track which right rows were matched
	// For simplicity, we'll skip this for now

	return result, nil
}

func (e *Executor) executeHashAggregate(
	ctx *ExecutionContext,
	plan *planner.PhysicalHashAggregate,
) (*ExecutionResult, error) {
	// Execute child
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Group rows by group-by expressions
	type groupKey string
	groups := make(map[groupKey][]map[string]any)
	groupOrder := make([]groupKey, 0)

	for _, row := range childResult.Rows {
		// Compute group key
		keyParts := make([]any, len(plan.GroupBy))
		for i, expr := range plan.GroupBy {
			val, err := e.evaluateExpr(
				ctx,
				expr,
				row,
			)
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

	// If no groups and no rows, create a single empty group for aggregate functions
	if len(groups) == 0 &&
		len(plan.GroupBy) == 0 {
		groupOrder = append(groupOrder, "")
		groups[""] = []map[string]any{}
	}

	// Build result columns
	numGroupBy := len(plan.GroupBy)
	numAgg := len(plan.Aggregates)
	columns := make([]string, numGroupBy+numAgg)

	for i := range plan.GroupBy {
		if i < len(plan.Aliases) &&
			plan.Aliases[i] != "" {
			columns[i] = plan.Aliases[i]
		} else {
			columns[i] = "col" + string(rune('0'+i))
		}
	}
	for i := range plan.Aggregates {
		if numGroupBy+i < len(plan.Aliases) &&
			plan.Aliases[numGroupBy+i] != "" {
			columns[numGroupBy+i] = plan.Aliases[numGroupBy+i]
		} else {
			columns[numGroupBy+i] = "col" + string(rune('0'+numGroupBy+i))
		}
	}

	// Compute aggregates for each group
	result := &ExecutionResult{
		Columns: columns,
		Rows: make(
			[]map[string]any,
			len(groupOrder),
		),
	}

	for i, key := range groupOrder {
		groupRows := groups[key]
		row := make(map[string]any)

		// Add group-by values
		if len(groupRows) > 0 {
			for j, expr := range plan.GroupBy {
				val, _ := e.evaluateExpr(
					ctx,
					expr,
					groupRows[0],
				)
				row[columns[j]] = val
			}
		}

		// Compute aggregates
		for j, expr := range plan.Aggregates {
			val, err := e.computeAggregate(
				ctx,
				expr,
				groupRows,
			)
			if err != nil {
				return nil, err
			}
			row[columns[numGroupBy+j]] = val
		}

		result.Rows[i] = row
	}

	return result, nil
}

func (e *Executor) executeSort(
	ctx *ExecutionContext,
	plan *planner.PhysicalSort,
) (*ExecutionResult, error) {
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Sort rows using insertion sort for simplicity
	rows := childResult.Rows
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			cmp, err := e.compareRows(
				ctx,
				rows[j-1],
				rows[j],
				plan.OrderBy,
			)
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
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeLimit(
	ctx *ExecutionContext,
	plan *planner.PhysicalLimit,
) (*ExecutionResult, error) {
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	rows := childResult.Rows

	// Apply offset
	if plan.Offset > 0 {
		if int(plan.Offset) >= len(rows) {
			rows = []map[string]any{}
		} else {
			rows = rows[plan.Offset:]
		}
	}

	// Apply limit
	if plan.Limit >= 0 &&
		int(plan.Limit) < len(rows) {
		rows = rows[:plan.Limit]
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeDistinct(
	ctx *ExecutionContext,
	plan *planner.PhysicalDistinct,
) (*ExecutionResult, error) {
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Use a map to track seen rows
	seen := make(map[string]bool)
	distinctRows := make([]map[string]any, 0)

	for _, row := range childResult.Rows {
		// Use the actual keys from the row for uniqueness check
		key := formatRowMap(row)
		if !seen[key] {
			seen[key] = true
			distinctRows = append(
				distinctRows,
				row,
			)
		}
	}

	return &ExecutionResult{
		Rows:    distinctRows,
		Columns: childResult.Columns,
	}, nil
}

func (e *Executor) executeInsert(
	ctx *ExecutionContext,
	plan *planner.PhysicalInsert,
) (*ExecutionResult, error) {
	// Get or create storage table
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		// Create table in storage
		var err error
		table, err = e.storage.CreateTable(
			plan.Table,
			plan.TableDef.ColumnTypes(),
		)
		if err != nil {
			return nil, err
		}
	}

	rowsAffected := int64(0)
	var pkIndices []int
	var pkColumns []*catalog.ColumnDef
	if plan.TableDef != nil && plan.TableDef.HasPrimaryKey() {
		pkIndices = plan.TableDef.PrimaryKey
		pkColumns = plan.TableDef.Columns
	}
	var pkKeys map[string]struct{}
	if len(pkIndices) > 0 {
		pkKeys = loadPrimaryKeyKeys(table, pkIndices)
	}

	checkPrimaryKey := func(values []any) error {
		if len(pkIndices) == 0 {
			return nil
		}
		pkValues, hasNull := extractPrimaryKeyValues(values, pkIndices)
		detail := formatPrimaryKeyDetail(
			pkValues,
			pkIndices,
			pkColumns,
		)
		if hasNull {
			return constraintErrorf(
				"NULL constraint violation on primary key (%s)",
				detail,
			)
		}
		key := primaryKeyKey(pkValues)
		if _, exists := pkKeys[key]; exists {
			return constraintErrorf(
				"duplicate key \"%s\" violates primary key constraint",
				detail,
			)
		}
		pkKeys[key] = struct{}{}

		return nil
	}

	// Get column types for DataChunk creation
	columnTypes := table.ColumnTypes()

	// Batch size is StandardVectorSize (2048) for DuckDB compatibility
	batchSize := storage.StandardVectorSize

	// Collect all inserted values for WAL logging
	var allInsertedValues [][]any

	// Helper function to flush a DataChunk to the table
	flushChunk := func(chunk *storage.DataChunk) (int, error) {
		if chunk.Count() == 0 {
			return 0, nil
		}
		count, err := table.InsertChunk(chunk)
		if err != nil {
			return 0, err
		}
		return count, nil
	}

	if plan.Source != nil {
		// INSERT ... SELECT with DataChunk batching
		sourceResult, err := e.Execute(
			ctx.Context,
			plan.Source,
			ctx.Args,
		)
		if err != nil {
			return nil, err
		}

		// Create initial chunk for batching
		currentChunk := storage.NewDataChunkWithCapacity(columnTypes, batchSize)

		for _, row := range sourceResult.Rows {
			values := make(
				[]any,
				len(plan.TableDef.Columns),
			)
			for i, col := range sourceResult.Columns {
				values[plan.Columns[i]] = row[col]
			}
			if err := checkPrimaryKey(values); err != nil {
				return nil, err
			}

			// Collect values for WAL logging
			if e.wal != nil {
				valuesCopy := make([]any, len(values))
				copy(valuesCopy, values)
				allInsertedValues = append(allInsertedValues, valuesCopy)
			}

			// Append to current chunk
			currentChunk.AppendRow(values)

			// Flush when chunk is full (reached batch size)
			if currentChunk.Count() >= batchSize {
				count, err := flushChunk(currentChunk)
				if err != nil {
					return nil, err
				}
				rowsAffected += int64(count)
				// Create new chunk for next batch
				currentChunk = storage.NewDataChunkWithCapacity(columnTypes, batchSize)
			}
		}

		// Flush remaining rows in the final chunk
		if currentChunk.Count() > 0 {
			count, err := flushChunk(currentChunk)
			if err != nil {
				return nil, err
			}
			rowsAffected += int64(count)
		}
	} else {
		// INSERT ... VALUES with DataChunk batching
		// Create initial chunk for batching
		currentChunk := storage.NewDataChunkWithCapacity(columnTypes, batchSize)

		for _, valueRow := range plan.Values {
			if len(valueRow) != len(plan.Columns) {
				return nil, fmt.Errorf(
					"column count mismatch: expected %d values, got %d",
					len(plan.Columns),
					len(valueRow),
				)
			}

			// Evaluate each expression in the row
			values := make([]any, len(plan.TableDef.Columns))
			for i, expr := range valueRow {
				val, err := e.evaluateExpr(ctx, expr, nil)
				if err != nil {
					return nil, err
				}
				values[plan.Columns[i]] = val
			}
			if err := checkPrimaryKey(values); err != nil {
				return nil, err
			}

			// Collect values for WAL logging
			if e.wal != nil {
				valuesCopy := make([]any, len(values))
				copy(valuesCopy, values)
				allInsertedValues = append(allInsertedValues, valuesCopy)
			}

			// Append to current chunk
			currentChunk.AppendRow(values)

			// Flush when chunk is full (reached batch size of 2048 rows)
			if currentChunk.Count() >= batchSize {
				count, err := flushChunk(currentChunk)
				if err != nil {
					return nil, err
				}
				rowsAffected += int64(count)
				// Create new chunk for next batch
				currentChunk = storage.NewDataChunkWithCapacity(columnTypes, batchSize)
			}
		}

		// Flush remaining rows in the final chunk
		if currentChunk.Count() > 0 {
			count, err := flushChunk(currentChunk)
			if err != nil {
				return nil, err
			}
			rowsAffected += int64(count)
		}
	}

	// WAL logging: Log INSERT entry AFTER successful insertion
	// This ensures atomicity - if the insert fails, no WAL entry is written
	if e.wal != nil && rowsAffected > 0 {
		schema := "main" // Default schema
		if plan.TableDef != nil && plan.TableDef.Schema != "" {
			schema = plan.TableDef.Schema
		}
		entry := wal.NewInsertEntry(e.txnID, schema, plan.Table, allInsertedValues)
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (e *Executor) executeCreateTable(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateTable,
) (*ExecutionResult, error) {
	// Check if table already exists
	_, exists := e.catalog.GetTableInSchema(
		plan.Schema,
		plan.Table,
	)
	if exists {
		if plan.IfNotExists {
			return &ExecutionResult{
				RowsAffected: 0,
			}, nil
		}

		return nil, dukdb.ErrTableAlreadyExists
	}

	// Create table definition
	tableDef := catalog.NewTableDef(
		plan.Table,
		plan.Columns,
	)
	if len(plan.PrimaryKey) > 0 {
		if err := tableDef.SetPrimaryKey(plan.PrimaryKey); err != nil {
			return nil, err
		}
	}

	// Add to catalog
	if err := e.catalog.CreateTableInSchema(plan.Schema, tableDef); err != nil {
		return nil, err
	}

	// Create in storage
	types := make([]dukdb.Type, len(plan.Columns))
	for i, col := range plan.Columns {
		types[i] = col.Type
	}
	if _, err := e.storage.CreateTable(plan.Table, types); err != nil {
		// Rollback catalog change
		_ = e.catalog.DropTableInSchema(
			plan.Schema,
			plan.Table,
		)

		return nil, err
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeDropTable(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropTable,
) (*ExecutionResult, error) {
	// Check if table exists
	_, exists := e.catalog.GetTableInSchema(
		plan.Schema,
		plan.Table,
	)
	if !exists {
		if plan.IfExists {
			return &ExecutionResult{
				RowsAffected: 0,
			}, nil
		}

		return nil, dukdb.ErrTableNotFound
	}

	// Drop from storage
	if err := e.storage.DropTable(plan.Table); err != nil &&
		err != dukdb.ErrTableNotFound {
		return nil, err
	}

	// Drop from catalog
	if err := e.catalog.DropTableInSchema(plan.Schema, plan.Table); err != nil {
		return nil, err
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeDummyScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalDummyScan,
) (*ExecutionResult, error) {
	// Return a single empty row for queries without FROM
	return &ExecutionResult{
		Rows:    []map[string]any{{}},
		Columns: []string{},
	}, nil
}

func (e *Executor) executeBegin(
	ctx *ExecutionContext,
	plan *planner.PhysicalBegin,
) (*ExecutionResult, error) {
	// BEGIN is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeCommit(
	ctx *ExecutionContext,
	plan *planner.PhysicalCommit,
) (*ExecutionResult, error) {
	// COMMIT is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

func (e *Executor) executeRollback(
	ctx *ExecutionContext,
	plan *planner.PhysicalRollback,
) (*ExecutionResult, error) {
	// ROLLBACK is a no-op in the executor.
	// Transaction management is handled at the connection level.
	return &ExecutionResult{RowsAffected: 0}, nil
}

// Helper functions

func formatGroupKey(values []any) string {
	if len(values) == 0 {
		return ""
	}
	result := ""
	for i, v := range values {
		if i > 0 {
			result += "|"
		}
		result += formatValue(v)
	}

	return result
}

func formatRowKey(
	row map[string]any,
	columns []string,
) string {
	result := ""
	for i, col := range columns {
		if i > 0 {
			result += "|"
		}
		result += formatValue(row[col])
	}

	return result
}

func formatRowMap(row map[string]any) string {
	// Sort keys for consistent ordering
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort for consistency
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	result := ""
	for i, k := range keys {
		if i > 0 {
			result += "|"
		}
		result += k + ":" + formatValue(row[k])
	}

	return result
}

func formatValue(v any) string {
	if v == nil {
		return "<null>"
	}

	return fmt.Sprintf("%v", v)
}

func constraintErrorf(
	format string,
	args ...any,
) error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeConstraint,
		Msg: fmt.Sprintf(
			"Constraint Error: "+format,
			args...,
		),
	}
}

func loadPrimaryKeyKeys(
	table *storage.Table,
	indices []int,
) map[string]struct{} {
	keys := make(map[string]struct{})
	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}
		for row := 0; row < chunk.Count(); row++ {
			pkValues := make([]any, len(indices))
			for i, idx := range indices {
				pkValues[i] = chunk.GetValue(row, idx)
			}
			keys[primaryKeyKey(pkValues)] = struct{}{}
		}
	}

	return keys
}

func extractPrimaryKeyValues(
	values []any,
	indices []int,
) ([]any, bool) {
	pkValues := make([]any, len(indices))
	hasNull := false
	for i, idx := range indices {
		if idx < 0 || idx >= len(values) {
			pkValues[i] = nil
			hasNull = true

			continue
		}
		val := values[idx]
		if val == nil {
			hasNull = true
		}
		pkValues[i] = val
	}

	return pkValues, hasNull
}

func primaryKeyKey(
	values []any,
) string {
	return fmt.Sprintf("%#v", values)
}

func formatPrimaryKeyDetail(
	values []any,
	indices []int,
	columns []*catalog.ColumnDef,
) string {
	parts := make([]string, len(indices))
	for i, idx := range indices {
		name := fmt.Sprintf("col%d", idx)
		if idx >= 0 && idx < len(columns) {
			if columns[idx].Name != "" {
				name = columns[idx].Name
			}
		}
		var val any
		if i < len(values) {
			val = values[i]
		}
		parts[i] = fmt.Sprintf(
			"%s: %s",
			name,
			formatValue(val),
		)
	}

	return strings.Join(parts, ", ")
}
