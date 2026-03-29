package executor

import (
	"fmt"
	"strconv"
	"strings"

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

	// Hash table state - stores rows as []any slices for direct index access
	hashTable      map[string][][]any // key -> list of matching right rows (as value arrays)
	hashTableBuilt bool

	// Probe state
	leftChunk       *storage.DataChunk
	leftRowIdx      int
	currentLeftVals []any           // current left row values (direct array)
	currentLeftMap  map[string]any  // reusable map for left row expression eval
	currentMatches  [][]any         // matched right rows (value arrays)
	currentMatchIdx int
	finished        bool

	// Reusable map for expression evaluation (avoids allocation per row)
	reusableRowMap map[string]any
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
	types := make(
		[]dukdb.TypeInfo,
		numLeft+numRight,
	)

	for i, col := range leftColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[i] = &basicTypeInfo{
				typ: col.Type,
			}
		} else {
			types[i] = info
		}
	}

	for i, col := range rightColumns {
		info, err := dukdb.NewTypeInfo(col.Type)
		if err != nil {
			types[numLeft+i] = &basicTypeInfo{
				typ: col.Type,
			}
		} else {
			types[numLeft+i] = info
		}
	}

	// Pre-size the reusable map
	mapSize := numLeft
	if numRight > mapSize {
		mapSize = numRight
	}

	return &PhysicalHashJoinOperator{
		left:         left,
		right:        right,
		leftColumns:  leftColumns,
		rightColumns: rightColumns,
		joinType:     joinType,
		condition:    condition,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		hashTable: make(
			map[string][][]any,
		),
		hashTableBuilt:  false,
		currentMatches:  nil,
		currentMatchIdx: 0,
		finished:        false,
		reusableRowMap:  make(map[string]any, mapSize*2),
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
	outputTypes := make(
		[]dukdb.Type,
		numLeft+numRight,
	)

	for i, col := range op.leftColumns {
		outputTypes[i] = col.Type
	}
	for i, col := range op.rightColumns {
		outputTypes[numLeft+i] = col.Type
	}

	outputChunk := storage.NewDataChunkWithCapacity(
		outputTypes,
		storage.StandardVectorSize,
	)

	// Reusable output row to avoid allocation per match
	outputRow := make([]any, numLeft+numRight)

	for {
		// Try to emit from current matches
		if op.currentMatches != nil &&
			op.currentMatchIdx < len(op.currentMatches) {
			// Get current left row and right match
			rightVals := op.currentMatches[op.currentMatchIdx]
			op.currentMatchIdx++

			// Combine rows: direct array copy, no map lookups
			copy(outputRow[:numLeft], op.currentLeftVals)
			copy(outputRow[numLeft:], rightVals)

			outputChunk.AppendRow(outputRow)

			// If output chunk is full, return it
			if outputChunk.Count() >= outputChunk.Capacity() {
				return outputChunk, nil
			}

			continue
		}

		// Need new left row
		if op.leftChunk == nil ||
			op.leftRowIdx >= op.leftChunk.Count() {
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
			// Extract left row values directly (no map)
			if op.currentLeftVals == nil || len(op.currentLeftVals) != numLeft {
				op.currentLeftVals = make([]any, numLeft)
			}
			for colIdx := 0; colIdx < op.leftChunk.ColumnCount() && colIdx < numLeft; colIdx++ {
				op.currentLeftVals[colIdx] = op.leftChunk.GetValue(op.leftRowIdx, colIdx)
			}

			// Build map only for join key extraction (reuse the map)
			op.populateRowMap(op.reusableRowMap, op.currentLeftVals, op.leftColumns)

			op.leftRowIdx++

			// Extract join key from left row
			joinKey, err := op.extractJoinKey(op.reusableRowMap)
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

			// For SEMI join: emit left row once if there are any matches, otherwise skip
			if op.joinType == planner.JoinTypeSemi {
				if len(op.currentMatches) > 0 {
					// Emit left row only (no right columns for semi-join)
					semiRow := make([]any, numLeft)
					copy(semiRow, op.currentLeftVals)
					outputChunk.AppendRow(semiRow)

					if outputChunk.Count() >= outputChunk.Capacity() {
						return outputChunk, nil
					}
				}
				// Move to next left row (don't process all matches for semi-join)
				continue
			}

			// If no matches and it's an INNER join, skip this left row
			if len(op.currentMatches) == 0 &&
				op.joinType == planner.JoinTypeInner {
				continue
			}

			// If no matches but it's a LEFT join, emit left row with NULLs for right
			if len(op.currentMatches) == 0 &&
				op.joinType == planner.JoinTypeLeft {
				copy(outputRow[:numLeft], op.currentLeftVals)
				for i := range op.rightColumns {
					outputRow[numLeft+i] = nil
				}
				outputChunk.AppendRow(outputRow)

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
	numRight := len(op.rightColumns)
	rowMap := make(map[string]any, numRight*2)

	for {
		chunk, err := op.right.Next()
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}

		// Process each row in the chunk
		for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
			// Extract row values as a flat slice (no map allocation per row)
			rowVals := make([]any, numRight)
			for colIdx := 0; colIdx < chunk.ColumnCount() && colIdx < numRight; colIdx++ {
				rowVals[colIdx] = chunk.GetValue(rowIdx, colIdx)
			}

			// Populate reusable map for join key extraction
			op.populateRowMap(rowMap, rowVals, op.rightColumns)

			// Extract join key
			joinKey, err := op.extractJoinKey(rowMap)
			if err != nil {
				return err
			}

			// Skip NULL keys (they don't match in equi-joins)
			if joinKey == "" {
				continue
			}

			// Add to hash table (store flat []any, not map)
			op.hashTable[joinKey] = append(
				op.hashTable[joinKey],
				rowVals,
			)
		}
	}

	return nil
}

// populateRowMap fills an existing map with column values, avoiding allocation.
// The map is cleared and repopulated each time.
func (op *PhysicalHashJoinOperator) populateRowMap(
	rowMap map[string]any,
	vals []any,
	columns []planner.ColumnBinding,
) {
	// Clear existing entries
	for k := range rowMap {
		delete(rowMap, k)
	}

	for colIdx := 0; colIdx < len(vals) && colIdx < len(columns); colIdx++ {
		value := vals[colIdx]
		col := columns[colIdx]

		if col.Column != "" {
			rowMap[col.Column] = value
		}
		if col.Table != "" && col.Column != "" {
			rowMap[col.Table+"."+col.Column] = value
		}
	}
}

// extractJoinKey extracts the join key from a row based on the join condition.
// For equi-joins, the condition should be a binary equality expression.
// Returns empty string for NULL keys.
func (op *PhysicalHashJoinOperator) extractJoinKey(
	row map[string]any,
) (string, error) {
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
	leftVal, leftErr := op.executor.evaluateExpr(
		op.ctx,
		binExpr.Left,
		row,
	)
	if leftErr == nil && leftVal != nil {
		return fastFormatValue(leftVal), nil
	}

	rightVal, rightErr := op.executor.evaluateExpr(
		op.ctx,
		binExpr.Right,
		row,
	)
	if rightErr == nil && rightVal != nil {
		return fastFormatValue(rightVal), nil
	}

	// Both sides failed or returned nil
	return "", nil
}

// fastFormatValue converts a value to a string key efficiently,
// avoiding fmt.Sprintf for common types.
func fastFormatValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "<null>"
	case int:
		return strconv.Itoa(val)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'g', -1, 32)
	case string:
		return val
	case bool:
		if val {
			return "T"
		}
		return "F"
	case []byte:
		// Use a strings.Builder for byte slices
		var b strings.Builder
		b.Grow(len(val) * 2)
		for _, by := range val {
			b.WriteByte("0123456789abcdef"[by>>4])
			b.WriteByte("0123456789abcdef"[by&0x0f])
		}
		return b.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// rowMapFromChunk extracts a row from a DataChunk into a map[string]any.
// Kept for backward compatibility.
func (op *PhysicalHashJoinOperator) rowMapFromChunk(
	chunk *storage.DataChunk,
	rowIdx int,
	columns []planner.ColumnBinding,
) map[string]any {
	rowMap := make(map[string]any, len(columns)*2)

	for colIdx := 0; colIdx < chunk.ColumnCount() && colIdx < len(columns); colIdx++ {
		value := chunk.GetValue(rowIdx, colIdx)
		col := columns[colIdx]

		if col.Column != "" {
			rowMap[col.Column] = value
		}
		if col.Table != "" && col.Column != "" {
			qualifiedName := col.Table + "." + col.Column
			rowMap[qualifiedName] = value
		}
	}

	return rowMap
}
