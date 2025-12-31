package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalHashJoinOperator implements the PhysicalOperator interface for hash joins.
// It performs a hash join in two phases:
// 1. Build phase: consume all rows from the right child and build a hash table
// 2. Probe phase: stream through left child, probing the hash table for matches
type PhysicalHashJoinOperator struct {
	left         PhysicalOperator
	right        PhysicalOperator
	leftColumns  []planner.ColumnBinding
	rightColumns []planner.ColumnBinding
	joinType     planner.JoinType
	condition    binder.BoundExpr
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo

	// Hash table state
	hashTable      map[string][]map[string]any // key -> list of matching right rows
	hashTableBuilt bool

	// Probe state
	leftChunk        *storage.DataChunk
	leftRowIdx       int
	currentLeftRow   map[string]any
	currentMatches   []map[string]any
	currentMatchIdx  int
	finished         bool
}

// NewPhysicalHashJoinOperator creates a new PhysicalHashJoinOperator.
func NewPhysicalHashJoinOperator(
	left PhysicalOperator,
	right PhysicalOperator,
	leftColumns []planner.ColumnBinding,
	rightColumns []planner.ColumnBinding,
	joinType planner.JoinType,
	condition binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalHashJoinOperator, error) {
	// Build output types: left columns followed by right columns
	numLeft := len(leftColumns)
	numRight := len(rightColumns)
	types := make([]dukdb.TypeInfo, numLeft+numRight)

	for i, col := range leftColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[i] = info
		}
	}

	for i, col := range rightColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[numLeft+i] = &basicTypeInfo{typ: col.Type}
		} else {
			types[numLeft+i] = info
		}
	}

	return &PhysicalHashJoinOperator{
		left:            left,
		right:           right,
		leftColumns:     leftColumns,
		rightColumns:    rightColumns,
		joinType:        joinType,
		condition:       condition,
		executor:        executor,
		ctx:             ctx,
		types:           types,
		hashTable:       make(map[string][]map[string]any),
		hashTableBuilt:  false,
		currentMatches:  nil,
		currentMatchIdx: 0,
		finished:        false,
	}, nil
}

// Next returns the next DataChunk of joined results.
func (op *PhysicalHashJoinOperator) Next() (*storage.DataChunk, error) {
	if op.finished {
		return nil, nil
	}

	// Build phase: build hash table from right side if not already built
	if !op.hashTableBuilt {
		if err := op.buildHashTable(); err != nil {
			return nil, err
		}
		op.hashTableBuilt = true
	}

	// Probe phase: stream through left side and probe hash table
	numLeft := len(op.leftColumns)
	numRight := len(op.rightColumns)
	outputTypes := make([]dukdb.Type, numLeft+numRight)

	for i, col := range op.leftColumns {
		outputTypes[i] = col.Type
	}
	for i, col := range op.rightColumns {
		outputTypes[numLeft+i] = col.Type
	}

	outputChunk := storage.NewDataChunkWithCapacity(outputTypes, storage.StandardVectorSize)

	for {
		// Try to emit from current matches
		if op.currentMatches != nil && op.currentMatchIdx < len(op.currentMatches) {
			// Get current left row and right match
			rightRow := op.currentMatches[op.currentMatchIdx]
			op.currentMatchIdx++

			// Combine rows and append to output
			values := make([]any, numLeft+numRight)
			for i, col := range op.leftColumns {
				colKey := col.Column
				if col.Table != "" {
					qualifiedKey := col.Table + "." + col.Column
					if val, ok := op.currentLeftRow[qualifiedKey]; ok {
						values[i] = val
					} else {
						values[i] = op.currentLeftRow[colKey]
					}
				} else {
					values[i] = op.currentLeftRow[colKey]
				}
			}
			for i, col := range op.rightColumns {
				colKey := col.Column
				if col.Table != "" {
					qualifiedKey := col.Table + "." + col.Column
					if val, ok := rightRow[qualifiedKey]; ok {
						values[numLeft+i] = val
					} else {
						values[numLeft+i] = rightRow[colKey]
					}
				} else {
					values[numLeft+i] = rightRow[colKey]
				}
			}

			outputChunk.AppendRow(values)

			// If output chunk is full, return it
			if outputChunk.Count() >= outputChunk.Capacity() {
				return outputChunk, nil
			}
			continue
		}

		// Need new left row
		if op.leftChunk == nil || op.leftRowIdx >= op.leftChunk.Count() {
			// Get next chunk from left child
			chunk, err := op.left.Next()
			if err != nil {
				return nil, err
			}
			if chunk == nil {
				// No more left rows
				op.finished = true
				if outputChunk.Count() > 0 {
					return outputChunk, nil
				}
				return nil, nil
			}
			op.leftChunk = chunk
			op.leftRowIdx = 0
		}

		// Probe hash table with current left row
		if op.leftRowIdx < op.leftChunk.Count() {
			op.currentLeftRow = op.rowMapFromChunk(op.leftChunk, op.leftRowIdx, op.leftColumns)
			op.leftRowIdx++

			// Extract join key from left row
			joinKey, err := op.extractJoinKey(op.currentLeftRow)
			if err != nil {
				return nil, err
			}

			// Look up matches in hash table
			if joinKey != "" {
				op.currentMatches = op.hashTable[joinKey]
			} else {
				// NULL key doesn't match anything in equi-join
				op.currentMatches = nil
			}

			op.currentMatchIdx = 0

			// If no matches and it's an INNER join, skip this left row
			if len(op.currentMatches) == 0 && op.joinType == planner.JoinTypeInner {
				continue
			}

			// If no matches but it's a LEFT join, emit left row with NULLs for right
			if len(op.currentMatches) == 0 && op.joinType == planner.JoinTypeLeft {
				values := make([]any, numLeft+numRight)
				for i, col := range op.leftColumns {
					colKey := col.Column
					if col.Table != "" {
						qualifiedKey := col.Table + "." + col.Column
						if val, ok := op.currentLeftRow[qualifiedKey]; ok {
							values[i] = val
						} else {
							values[i] = op.currentLeftRow[colKey]
						}
					} else {
						values[i] = op.currentLeftRow[colKey]
					}
				}
				for i := range op.rightColumns {
					values[numLeft+i] = nil
				}
				outputChunk.AppendRow(values)

				if outputChunk.Count() >= outputChunk.Capacity() {
					return outputChunk, nil
				}
				continue
			}
		}
	}
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalHashJoinOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// buildHashTable consumes all rows from the right child and builds the hash table.
func (op *PhysicalHashJoinOperator) buildHashTable() error {
	for {
		chunk, err := op.right.Next()
		if err != nil {
			return err
		}
		if chunk == nil {
			// No more right rows
			break
		}

		// Process each row in the chunk
		for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
			rowMap := op.rowMapFromChunk(chunk, rowIdx, op.rightColumns)

			// Extract join key
			joinKey, err := op.extractJoinKey(rowMap)
			if err != nil {
				return err
			}

			// Skip NULL keys (they don't match in equi-joins)
			if joinKey == "" {
				continue
			}

			// Add to hash table
			op.hashTable[joinKey] = append(op.hashTable[joinKey], rowMap)
		}
	}

	return nil
}

// extractJoinKey extracts the join key from a row based on the join condition.
// For equi-joins, the condition should be a binary equality expression.
// Returns empty string for NULL keys.
func (op *PhysicalHashJoinOperator) extractJoinKey(row map[string]any) (string, error) {
	if op.condition == nil {
		// Cross join - no condition
		return "cross-join-key", nil
	}

	// For equi-join, extract the join column value
	binExpr, ok := op.condition.(*binder.BoundBinaryExpr)
	if !ok {
		return "", &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "hash join requires equality condition",
		}
	}

	// Try to evaluate both sides of the join condition
	// For a.id = b.id, when building hash table from b, we get b.id
	// When probing from a, we get a.id
	leftVal, leftErr := op.executor.evaluateExpr(op.ctx, binExpr.Left, row)
	if leftErr == nil && leftVal != nil {
		return formatValue(leftVal), nil
	}

	rightVal, rightErr := op.executor.evaluateExpr(op.ctx, binExpr.Right, row)
	if rightErr == nil && rightVal != nil {
		return formatValue(rightVal), nil
	}

	// Both sides failed or returned nil
	return "", nil
}

// rowMapFromChunk extracts a row from a DataChunk into a map[string]any.
func (op *PhysicalHashJoinOperator) rowMapFromChunk(
	chunk *storage.DataChunk,
	rowIdx int,
	columns []planner.ColumnBinding,
) map[string]any {
	rowMap := make(map[string]any)

	for colIdx := 0; colIdx < chunk.ColumnCount() && colIdx < len(columns); colIdx++ {
		value := chunk.GetValue(rowIdx, colIdx)
		col := columns[colIdx]

		// Add column by simple name
		if col.Column != "" {
			rowMap[col.Column] = value
		}

		// Add column by table-qualified name
		if col.Table != "" && col.Column != "" {
			qualifiedName := col.Table + "." + col.Column
			rowMap[qualifiedName] = value
		}
	}

	return rowMap
}
