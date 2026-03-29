// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ---------- Core Data Structures (Tasks 4.3-4.5) ----------

// WindowState holds state for window function evaluation.
type WindowState struct {
	Partitions     map[string]*WindowPartition // Partitions indexed by key
	PartitionOrder []string                    // Order of partition keys for deterministic iteration
}

// WindowPartition holds rows for a single partition.
type WindowPartition struct {
	Rows           []WindowRow // Rows in partition (sorted by ORDER BY if specified)
	PeerBoundaries []int       // Indexes where peer groups start (for RANK/RANGE/GROUPS)
}

// WindowRow holds a row with its original index and computed values.
type WindowRow struct {
	OriginalIndex int   // Position in original input
	Values        []any // Column values
	WindowResults []any // Computed window function results
}

// FrameBounds represents computed frame boundaries for a row.
type FrameBounds struct {
	Start        int   // First row in frame (inclusive)
	End          int   // Last row in frame (inclusive)
	ExcludedRows []int // Rows to exclude based on EXCLUDE clause
}

// ---------- PhysicalWindowExecutor (Tasks 4.1-4.2) ----------

// PhysicalWindowExecutor implements the PhysicalOperator interface for window functions.
// It evaluates window expressions over partitioned and ordered data.
type PhysicalWindowExecutor struct {
	window       *planner.PhysicalWindow
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	executor     *Executor
	ctx          *ExecutionContext

	// State management
	state       *WindowState
	outputRows  []WindowRow
	outputIndex int
	outputTypes []dukdb.TypeInfo
	finished    bool
}

// NewPhysicalWindowExecutor creates a new PhysicalWindowExecutor.
func NewPhysicalWindowExecutor(
	window *planner.PhysicalWindow,
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalWindowExecutor, error) {
	// Build output types: child columns + window result columns
	childTypes := child.GetTypes()
	outputTypes := make([]dukdb.TypeInfo, len(childTypes)+len(window.WindowExprs))

	// Copy child types
	copy(outputTypes, childTypes)

	// Add window result types
	for i, windowExpr := range window.WindowExprs {
		typ := windowExpr.ResType
		info, err := dukdb.NewTypeInfo(typ)
		if err != nil {
			outputTypes[len(childTypes)+i] = &basicTypeInfo{typ: typ}
		} else {
			outputTypes[len(childTypes)+i] = info
		}
	}

	return &PhysicalWindowExecutor{
		window:       window,
		child:        child,
		childColumns: childColumns,
		executor:     executor,
		ctx:          ctx,
		outputTypes:  outputTypes,
		finished:     false,
	}, nil
}

// Next returns the next DataChunk of results.
// Window functions are blocking operators - we materialize all input,
// compute window functions, then emit results in chunks.
func (w *PhysicalWindowExecutor) Next() (*storage.DataChunk, error) {
	// On first call, materialize, sort, and evaluate window functions
	if w.state == nil && !w.finished {
		// Phase 1: Materialize all child rows into partitions
		state, err := w.materialize()
		if err != nil {
			return nil, err
		}
		w.state = state

		// Phase 2: Sort each partition by ORDER BY
		w.sortPartitions()

		// Phase 3: Compute peer group boundaries
		w.computePeerBoundaries()

		// Phase 4: Evaluate window functions
		if err := w.evaluateWindow(); err != nil {
			return nil, err
		}

		// Phase 5: Flatten partitions back to original row order
		w.outputRows = w.flattenAndSort()
	}

	// Check if we're done
	if w.finished || w.outputIndex >= len(w.outputRows) {
		w.finished = true
		return nil, nil
	}

	// Build output types for DataChunk
	numChildCols := len(w.childColumns)
	numWindowCols := len(w.window.WindowExprs)
	outputDukdbTypes := make([]dukdb.Type, numChildCols+numWindowCols)

	for i, col := range w.childColumns {
		outputDukdbTypes[i] = col.Type
	}
	for i, windowExpr := range w.window.WindowExprs {
		outputDukdbTypes[numChildCols+i] = windowExpr.ResType
	}

	// Create output chunk
	chunkSize := storage.StandardVectorSize
	remaining := len(w.outputRows) - w.outputIndex
	if remaining < chunkSize {
		chunkSize = remaining
	}

	chunk := storage.NewDataChunkWithCapacity(outputDukdbTypes, chunkSize)

	// Emit rows
	for i := 0; i < chunkSize && w.outputIndex < len(w.outputRows); i++ {
		row := w.outputRows[w.outputIndex]
		// Combine original values with window results
		combinedValues := make([]any, len(row.Values)+len(row.WindowResults))
		copy(combinedValues, row.Values)
		copy(combinedValues[len(row.Values):], row.WindowResults)
		chunk.AppendRow(combinedValues)
		w.outputIndex++
	}

	return chunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (w *PhysicalWindowExecutor) GetTypes() []dukdb.TypeInfo {
	return w.outputTypes
}

// ---------- Phase 1: Materialization (Tasks 4.6-4.10) ----------

// materialize collects all child rows and groups them by partition key.
func (w *PhysicalWindowExecutor) materialize() (*WindowState, error) {
	state := &WindowState{
		Partitions:     make(map[string]*WindowPartition),
		PartitionOrder: make([]string, 0),
	}

	globalIndex := 0

	// Consume all input from child operator
	for {
		inputChunk, err := w.child.Next()
		if err != nil {
			return nil, err
		}
		if inputChunk == nil {
			break
		}

		// Process each row in the chunk
		for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
			// Extract row values
			values := make([]any, inputChunk.ColumnCount())
			for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
				values[colIdx] = inputChunk.GetValue(rowIdx, colIdx)
			}

			// Compute partition key
			partitionKey := w.computePartitionKey(values)

			// Add to partition
			if _, exists := state.Partitions[partitionKey]; !exists {
				state.Partitions[partitionKey] = &WindowPartition{
					Rows: make([]WindowRow, 0),
				}
				state.PartitionOrder = append(state.PartitionOrder, partitionKey)
			}

			state.Partitions[partitionKey].Rows = append(
				state.Partitions[partitionKey].Rows,
				WindowRow{
					OriginalIndex: globalIndex,
					Values:        values,
					WindowResults: make([]any, len(w.window.WindowExprs)),
				},
			)
			globalIndex++
		}
	}

	return state, nil
}

// computePartitionKey generates a unique key for grouping rows by PARTITION BY expressions.
// NULL values in partition columns form a separate partition where NULL = NULL.
func (w *PhysicalWindowExecutor) computePartitionKey(values []any) string {
	if len(w.window.WindowExprs) == 0 {
		return "" // Single partition for entire dataset
	}

	// Use first window expression's partition by (all should be same if optimized)
	windowExpr := w.window.WindowExprs[0]
	if len(windowExpr.PartitionBy) == 0 {
		return "" // Single partition for entire dataset
	}

	var buf strings.Builder
	for i, partExpr := range windowExpr.PartitionBy {
		if i > 0 {
			buf.WriteString("|")
		}

		// Build row map for expression evaluation
		rowMap := w.buildRowMap(values)

		// Evaluate partition expression
		val, err := w.executor.evaluateExpr(w.ctx, partExpr, rowMap)
		if err != nil {
			// On error, use empty string for this part
			buf.WriteString("\x00ERROR\x00")
			continue
		}

		if val == nil {
			buf.WriteString("\x00NULL\x00") // Special NULL marker
		} else {
			// Use type-specific serialization for correct hashing
			buf.WriteString(w.serializeValue(val))
		}
	}

	return buf.String()
}

// buildRowMap creates a row map for expression evaluation.
func (w *PhysicalWindowExecutor) buildRowMap(values []any) map[string]any {
	rowMap := make(map[string]any)
	for i, val := range values {
		if i < len(w.childColumns) {
			col := w.childColumns[i]
			if col.Column != "" {
				rowMap[col.Column] = val
			}
			if col.Table != "" && col.Column != "" {
				rowMap[col.Table+"."+col.Column] = val
			}
		}
	}
	return rowMap
}

// serializeValue produces a comparable string for any value.
// Uses tuple hashing: h = 0; for val in key: h = h*31 + hash(val)
func (w *PhysicalWindowExecutor) serializeValue(val any) string {
	switch v := val.(type) {
	case int64:
		return fmt.Sprintf("I:%020d", v)
	case int32:
		return fmt.Sprintf("I:%020d", int64(v))
	case int:
		return fmt.Sprintf("I:%020d", int64(v))
	case float64:
		return fmt.Sprintf("F:%v", v)
	case float32:
		return fmt.Sprintf("F:%v", float64(v))
	case string:
		return fmt.Sprintf("S:%s", v)
	case bool:
		if v {
			return "B:T"
		}
		return "B:F"
	default:
		return fmt.Sprintf("X:%v", v)
	}
}

// ---------- Phase 2: Sorting (Tasks 4.11-4.14) ----------

// sortPartitions sorts each partition by ORDER BY expressions.
// Uses pre-extracted sort keys to avoid map allocations during O(n log n) comparisons.
func (w *PhysicalWindowExecutor) sortPartitions() {
	if len(w.window.WindowExprs) == 0 {
		return
	}

	// Use first window expression's order by
	windowExpr := w.window.WindowExprs[0]
	if len(windowExpr.OrderBy) == 0 {
		return // No sorting needed
	}

	numKeys := len(windowExpr.OrderBy)

	for _, partition := range w.state.Partitions {
		rows := partition.Rows
		if len(rows) == 0 {
			continue
		}

		// Pre-extract sort keys for all rows in this partition (O(n), done once)
		sortKeys := make([][]any, len(rows))
		for i, row := range rows {
			rowMap := w.buildRowMap(row.Values)
			keys := make([]any, numKeys)
			for k, order := range windowExpr.OrderBy {
				val, err := w.executor.evaluateExpr(w.ctx, order.Expr, rowMap)
				if err != nil {
					val = nil
				}
				keys[k] = val
			}
			sortKeys[i] = keys
		}

		// Sort using pre-extracted keys - no map allocations during comparison
		sort.SliceStable(rows, func(i, j int) bool {
			return w.compareCachedKeys(sortKeys[i], sortKeys[j], windowExpr.OrderBy) < 0
		})

		// Also reorder sortKeys to match sorted rows (they were sorted in-place)
		// Actually sort.SliceStable sorts the rows slice directly, so sortKeys
		// indices no longer match. We need to sort both together.
		// Let's use an index-based approach instead.
	}

	// Redo with index-based sorting for correctness
	for _, partition := range w.state.Partitions {
		rows := partition.Rows
		if len(rows) == 0 {
			continue
		}

		numRows := len(rows)

		// Pre-extract sort keys for all rows in this partition (O(n), done once)
		type indexedRow struct {
			idx  int
			keys []any
		}
		indexed := make([]indexedRow, numRows)
		for i, row := range rows {
			rowMap := w.buildRowMap(row.Values)
			keys := make([]any, numKeys)
			for k, order := range windowExpr.OrderBy {
				val, err := w.executor.evaluateExpr(w.ctx, order.Expr, rowMap)
				if err != nil {
					val = nil
				}
				keys[k] = val
			}
			indexed[i] = indexedRow{idx: i, keys: keys}
		}

		// Sort indices using pre-extracted keys
		sort.SliceStable(indexed, func(i, j int) bool {
			return w.compareCachedKeys(indexed[i].keys, indexed[j].keys, windowExpr.OrderBy) < 0
		})

		// Reorder rows according to sorted indices
		sorted := make([]WindowRow, numRows)
		for i, ir := range indexed {
			sorted[i] = rows[ir.idx]
		}
		copy(rows, sorted)
	}
}

// compareCachedKeys compares two pre-extracted sort key arrays.
// No map allocations or expression evaluation happen here.
func (w *PhysicalWindowExecutor) compareCachedKeys(
	a, b []any,
	orderBy []binder.BoundWindowOrder,
) int {
	for k, order := range orderBy {
		valA := a[k]
		valB := b[k]

		// Handle NULL ordering
		if valA == nil && valB == nil {
			continue
		}
		if valA == nil {
			if order.NullsFirst {
				return -1
			}
			return 1
		}
		if valB == nil {
			if order.NullsFirst {
				return 1
			}
			return -1
		}

		cmp := compareValues(valA, valB)
		if cmp != 0 {
			if order.Desc {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

// compareByOrderBy compares two rows by ORDER BY expressions.
// Handles NULL values (NULLS FIRST or NULLS LAST per column) and DESC ordering.
// Kept for use outside the hot sort path (e.g., frame computation).
func (w *PhysicalWindowExecutor) compareByOrderBy(
	a, b WindowRow,
	orderBy []binder.BoundWindowOrder,
) int {
	for _, order := range orderBy {
		rowMapA := w.buildRowMap(a.Values)
		rowMapB := w.buildRowMap(b.Values)

		valA, errA := w.executor.evaluateExpr(w.ctx, order.Expr, rowMapA)
		valB, errB := w.executor.evaluateExpr(w.ctx, order.Expr, rowMapB)

		// Handle evaluation errors as NULL
		if errA != nil {
			valA = nil
		}
		if errB != nil {
			valB = nil
		}

		// Handle NULL ordering (Task 4.13)
		if valA == nil && valB == nil {
			continue // Equal
		}
		if valA == nil {
			// NULL compared to non-NULL
			if order.NullsFirst {
				return -1 // NULL comes first
			}
			return 1 // NULL comes last (default)
		}
		if valB == nil {
			// Non-NULL compared to NULL
			if order.NullsFirst {
				return 1 // NULL comes first, so non-NULL comes after
			}
			return -1 // NULL comes last, so non-NULL comes before
		}

		// Compare non-NULL values
		cmp := compareValues(valA, valB)
		if cmp != 0 {
			// Handle DESC ordering (Task 4.14)
			if order.Desc {
				return -cmp
			}
			return cmp
		}
	}

	return 0 // All columns equal
}

// ---------- Phase 3: Peer Group Computation (Task 4.12) ----------

// computePeerBoundaries computes peer group boundaries for RANK/RANGE/GROUPS frames.
// Peer groups are rows that have the same ORDER BY values.
// Uses pre-extracted sort keys to avoid map allocations during O(n) comparisons.
func (w *PhysicalWindowExecutor) computePeerBoundaries() {
	if len(w.window.WindowExprs) == 0 {
		return
	}

	windowExpr := w.window.WindowExprs[0]
	numKeys := len(windowExpr.OrderBy)

	for _, partition := range w.state.Partitions {
		partition.PeerBoundaries = make([]int, 0)

		if len(partition.Rows) == 0 {
			continue
		}

		// First row always starts a new peer group
		partition.PeerBoundaries = append(partition.PeerBoundaries, 0)

		if numKeys == 0 {
			continue
		}

		// Pre-extract sort keys for peer comparison (O(n), done once)
		peerKeys := make([][]any, len(partition.Rows))
		for i, row := range partition.Rows {
			rowMap := w.buildRowMap(row.Values)
			keys := make([]any, numKeys)
			for k, order := range windowExpr.OrderBy {
				val, err := w.executor.evaluateExpr(w.ctx, order.Expr, rowMap)
				if err != nil {
					val = nil
				}
				keys[k] = val
			}
			peerKeys[i] = keys
		}

		// Compare adjacent rows using cached keys (no maps during comparison)
		for i := 1; i < len(partition.Rows); i++ {
			if w.compareCachedKeys(peerKeys[i-1], peerKeys[i], windowExpr.OrderBy) != 0 {
				partition.PeerBoundaries = append(partition.PeerBoundaries, i)
			}
		}
	}
}

// getPeerGroupForRow returns the index of the peer group that contains the given row.
func (w *PhysicalWindowExecutor) getPeerGroupForRow(partition *WindowPartition, rowIdx int) int {
	for i := len(partition.PeerBoundaries) - 1; i >= 0; i-- {
		if partition.PeerBoundaries[i] <= rowIdx {
			return i
		}
	}
	return 0
}

// getPeerGroupStart returns the starting index of the peer group at peerGroupIdx.
func (w *PhysicalWindowExecutor) getPeerGroupStart(
	partition *WindowPartition,
	peerGroupIdx int,
) int {
	if peerGroupIdx < 0 || peerGroupIdx >= len(partition.PeerBoundaries) {
		return 0
	}
	return partition.PeerBoundaries[peerGroupIdx]
}

// getPeerGroupEnd returns the ending index (inclusive) of the peer group at peerGroupIdx.
func (w *PhysicalWindowExecutor) getPeerGroupEnd(partition *WindowPartition, peerGroupIdx int) int {
	if peerGroupIdx < 0 {
		return -1
	}
	if peerGroupIdx >= len(partition.PeerBoundaries)-1 {
		return len(partition.Rows) - 1
	}
	return partition.PeerBoundaries[peerGroupIdx+1] - 1
}

// ---------- Phase 4: Frame Computation (Tasks 4.15-4.34) ----------

// Note: The following frame computation functions are implemented in Part 1 (tasks 4.15-4.34)
// and will be used by window function evaluators in Part 2 (tasks 4.35+).
// They are exported here as the public API for frame computation.

// computeFrameBounds computes the frame boundaries for a row.
//
//nolint:unused // Will be used by window function evaluators in Part 2
func (w *PhysicalWindowExecutor) computeFrameBounds(
	partition *WindowPartition,
	rowIdx int,
	windowExpr *binder.BoundWindowExpr,
) FrameBounds {
	frame := windowExpr.Frame
	partitionSize := len(partition.Rows)

	// Default frame based on ORDER BY presence
	if frame == nil {
		if len(windowExpr.OrderBy) > 0 {
			// With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			frame = &parser.WindowFrame{
				Type: parser.FrameTypeRange,
				Start: parser.WindowBound{
					Type: parser.BoundUnboundedPreceding,
				},
				End: parser.WindowBound{
					Type: parser.BoundCurrentRow,
				},
				Exclude: parser.ExcludeNoOthers,
			}
		} else {
			// Without ORDER BY: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			frame = &parser.WindowFrame{
				Type: parser.FrameTypeRows,
				Start: parser.WindowBound{
					Type: parser.BoundUnboundedPreceding,
				},
				End: parser.WindowBound{
					Type: parser.BoundUnboundedFollowing,
				},
				Exclude: parser.ExcludeNoOthers,
			}
		}
	}

	var start, end int

	switch frame.Type {
	case parser.FrameTypeRows:
		start, end = w.computeRowsFrameBounds(partition, rowIdx, frame)
	case parser.FrameTypeRange:
		start, end = w.computeRangeFrameBounds(partition, rowIdx, frame, windowExpr)
	case parser.FrameTypeGroups:
		start, end = w.computeGroupsFrameBounds(partition, rowIdx, frame)
	default:
		// Default to entire partition
		start = 0
		end = partitionSize - 1
	}

	// Clamp bounds to partition
	if start < 0 {
		start = 0
	}
	if end >= partitionSize {
		end = partitionSize - 1
	}
	if end < start {
		end = start - 1 // Empty frame
	}

	// Compute excluded rows (Tasks 4.30-4.34)
	excludedRows := w.computeExcludedRows(partition, rowIdx, frame.Exclude)

	return FrameBounds{
		Start:        start,
		End:          end,
		ExcludedRows: excludedRows,
	}
}

// computeRowsFrameBounds computes ROWS frame boundaries (Tasks 4.15-4.20).
//
//nolint:unused // Will be used by computeFrameBounds
func (w *PhysicalWindowExecutor) computeRowsFrameBounds(
	partition *WindowPartition,
	rowIdx int,
	frame *parser.WindowFrame,
) (start, end int) {
	partitionSize := len(partition.Rows)

	// Compute start bound (Tasks 4.16-4.19)
	switch frame.Start.Type {
	case parser.BoundUnboundedPreceding: // Task 4.16
		start = 0
	case parser.BoundCurrentRow: // Task 4.18
		start = rowIdx
	case parser.BoundPreceding: // Task 4.19
		offset := w.evaluateOffset(frame.Start.Offset)
		start = rowIdx - offset
		if start < 0 {
			start = 0
		}
	case parser.BoundFollowing:
		offset := w.evaluateOffset(frame.Start.Offset)
		start = rowIdx + offset
		if start >= partitionSize {
			start = partitionSize
		}
	case parser.BoundUnboundedFollowing:
		// Invalid for start bound, treat as full partition
		start = 0
	}

	// Compute end bound (Tasks 4.17-4.20)
	switch frame.End.Type {
	case parser.BoundUnboundedFollowing: // Task 4.17
		end = partitionSize - 1
	case parser.BoundCurrentRow: // Task 4.18
		end = rowIdx
	case parser.BoundPreceding:
		offset := w.evaluateOffset(frame.End.Offset)
		end = rowIdx - offset
		if end < 0 {
			end = -1 // Empty frame
		}
	case parser.BoundFollowing: // Task 4.20
		offset := w.evaluateOffset(frame.End.Offset)
		end = rowIdx + offset
		if end >= partitionSize {
			end = partitionSize - 1
		}
	case parser.BoundUnboundedPreceding:
		// Invalid for end bound, treat as empty frame
		end = -1
	}

	return start, end
}

// computeRangeFrameBounds computes RANGE frame boundaries (Tasks 4.21-4.24).
// RANGE frame uses logical value comparison based on ORDER BY values.
//
//nolint:unused // Will be used by computeFrameBounds
func (w *PhysicalWindowExecutor) computeRangeFrameBounds(
	partition *WindowPartition,
	rowIdx int,
	frame *parser.WindowFrame,
	windowExpr *binder.BoundWindowExpr,
) (start, end int) {
	partitionSize := len(partition.Rows)
	peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)

	// Compute start bound (Tasks 4.22-4.24)
	switch frame.Start.Type {
	case parser.BoundUnboundedPreceding: // Task 4.23
		start = 0
	case parser.BoundCurrentRow: // Task 4.22
		// For RANGE CURRENT ROW, start at the beginning of the peer group
		start = w.getPeerGroupStart(partition, peerGroupIdx)
	case parser.BoundPreceding: // Task 4.24
		// N PRECEDING: find rows where ORDER BY value >= current - N
		offset := w.evaluateOffset(frame.Start.Offset)
		start = w.findRangeStart(partition, rowIdx, offset, windowExpr)
	case parser.BoundFollowing:
		// N FOLLOWING: find rows where ORDER BY value >= current + N
		offset := w.evaluateOffset(frame.Start.Offset)
		start = w.findRangeEnd(partition, rowIdx, offset, windowExpr) + 1
		if start >= partitionSize {
			start = partitionSize
		}
	case parser.BoundUnboundedFollowing:
		// Invalid for start bound, treat as full partition
		start = 0
	}

	// Compute end bound (Tasks 4.22-4.24)
	switch frame.End.Type {
	case parser.BoundUnboundedFollowing: // Task 4.23
		end = partitionSize - 1
	case parser.BoundCurrentRow: // Task 4.22
		// For RANGE CURRENT ROW, end at the end of the peer group
		end = w.getPeerGroupEnd(partition, peerGroupIdx)
	case parser.BoundPreceding:
		// N PRECEDING: find rows where ORDER BY value <= current - N
		offset := w.evaluateOffset(frame.End.Offset)
		end = w.findRangeStart(partition, rowIdx, offset, windowExpr) - 1
		if end < 0 {
			end = -1
		}
	case parser.BoundFollowing: // Task 4.24
		// N FOLLOWING: find rows where ORDER BY value <= current + N
		offset := w.evaluateOffset(frame.End.Offset)
		end = w.findRangeEnd(partition, rowIdx, offset, windowExpr)
	case parser.BoundUnboundedPreceding:
		// Invalid for end bound, treat as empty frame
		end = -1
	}

	return start, end
}

// findRangeStart finds the first row where ORDER BY value >= current - offset.
//
//nolint:unused // Will be used by computeRangeFrameBounds
func (w *PhysicalWindowExecutor) findRangeStart(
	partition *WindowPartition,
	rowIdx int,
	offset int,
	windowExpr *binder.BoundWindowExpr,
) int {
	if len(windowExpr.OrderBy) == 0 {
		return 0
	}

	// Get current row's ORDER BY value
	rowMap := w.buildRowMap(partition.Rows[rowIdx].Values)
	currentVal, err := w.executor.evaluateExpr(w.ctx, windowExpr.OrderBy[0].Expr, rowMap)
	if err != nil || currentVal == nil {
		return 0
	}

	// Compute target value (current - offset)
	targetVal := w.subtractOffset(currentVal, offset)

	// Find first row >= target
	for i := 0; i < len(partition.Rows); i++ {
		rm := w.buildRowMap(partition.Rows[i].Values)
		val, err := w.executor.evaluateExpr(w.ctx, windowExpr.OrderBy[0].Expr, rm)
		if err != nil {
			continue
		}
		if val != nil && compareValues(val, targetVal) >= 0 {
			return i
		}
	}
	return len(partition.Rows)
}

// findRangeEnd finds the last row where ORDER BY value <= current + offset.
//
//nolint:unused // Will be used by computeRangeFrameBounds
func (w *PhysicalWindowExecutor) findRangeEnd(
	partition *WindowPartition,
	rowIdx int,
	offset int,
	windowExpr *binder.BoundWindowExpr,
) int {
	if len(windowExpr.OrderBy) == 0 {
		return len(partition.Rows) - 1
	}

	// Get current row's ORDER BY value
	rowMap := w.buildRowMap(partition.Rows[rowIdx].Values)
	currentVal, err := w.executor.evaluateExpr(w.ctx, windowExpr.OrderBy[0].Expr, rowMap)
	if err != nil || currentVal == nil {
		return len(partition.Rows) - 1
	}

	// Compute target value (current + offset)
	targetVal := w.addOffset(currentVal, offset)

	// Find last row <= target
	lastIdx := -1
	for i := 0; i < len(partition.Rows); i++ {
		rm := w.buildRowMap(partition.Rows[i].Values)
		val, err := w.executor.evaluateExpr(w.ctx, windowExpr.OrderBy[0].Expr, rm)
		if err != nil {
			continue
		}
		if val != nil && compareValues(val, targetVal) <= 0 {
			lastIdx = i
		}
	}
	return lastIdx
}

// subtractOffset subtracts an offset from a value.
func (w *PhysicalWindowExecutor) subtractOffset(val any, offset int) any {
	switch v := val.(type) {
	case int64:
		return v - int64(offset)
	case int32:
		return int32(int(v) - offset)
	case int:
		return v - offset
	case float64:
		return v - float64(offset)
	case float32:
		return float32(float64(v) - float64(offset))
	default:
		return val
	}
}

// addOffset adds an offset to a value.
func (w *PhysicalWindowExecutor) addOffset(val any, offset int) any {
	switch v := val.(type) {
	case int64:
		return v + int64(offset)
	case int32:
		return int32(int(v) + offset)
	case int:
		return v + offset
	case float64:
		return v + float64(offset)
	case float32:
		return float32(float64(v) + float64(offset))
	default:
		return val
	}
}

// computeGroupsFrameBounds computes GROUPS frame boundaries (Tasks 4.25-4.29).
// GROUPS frame counts peer groups (not individual rows) for offset.
func (w *PhysicalWindowExecutor) computeGroupsFrameBounds(
	partition *WindowPartition,
	rowIdx int,
	frame *parser.WindowFrame,
) (start, end int) {
	partitionSize := len(partition.Rows)
	currentPeerGroup := w.getPeerGroupForRow(partition, rowIdx)
	numPeerGroups := len(partition.PeerBoundaries)

	// Compute start bound (Tasks 4.25-4.27)
	switch frame.Start.Type {
	case parser.BoundUnboundedPreceding:
		start = 0
	case parser.BoundCurrentRow: // Task 4.29
		// CURRENT ROW = include entire current peer group
		start = w.getPeerGroupStart(partition, currentPeerGroup)
	case parser.BoundPreceding: // Task 4.27
		// N PRECEDING = include N peer groups before current group
		offset := w.evaluateOffset(frame.Start.Offset)
		targetGroup := currentPeerGroup - offset
		if targetGroup < 0 {
			targetGroup = 0
		}
		start = w.getPeerGroupStart(partition, targetGroup)
	case parser.BoundFollowing: // Task 4.28
		// N FOLLOWING = start N peer groups after current group
		offset := w.evaluateOffset(frame.Start.Offset)
		targetGroup := currentPeerGroup + offset
		if targetGroup >= numPeerGroups {
			start = partitionSize // Empty frame
		} else {
			start = w.getPeerGroupStart(partition, targetGroup)
		}
	case parser.BoundUnboundedFollowing:
		// Invalid for start bound, treat as full partition
		start = 0
	}

	// Compute end bound (Tasks 4.25-4.28)
	switch frame.End.Type {
	case parser.BoundUnboundedFollowing:
		end = partitionSize - 1
	case parser.BoundCurrentRow: // Task 4.29
		// CURRENT ROW = include entire current peer group
		end = w.getPeerGroupEnd(partition, currentPeerGroup)
	case parser.BoundPreceding:
		// N PRECEDING = end N peer groups before current group
		offset := w.evaluateOffset(frame.End.Offset)
		targetGroup := currentPeerGroup - offset
		if targetGroup < 0 {
			end = -1 // Empty frame
		} else {
			end = w.getPeerGroupEnd(partition, targetGroup)
		}
	case parser.BoundFollowing: // Task 4.28
		// N FOLLOWING = include N peer groups after current group
		offset := w.evaluateOffset(frame.End.Offset)
		targetGroup := currentPeerGroup + offset
		if targetGroup >= numPeerGroups {
			targetGroup = numPeerGroups - 1
		}
		end = w.getPeerGroupEnd(partition, targetGroup)
	case parser.BoundUnboundedPreceding:
		// Invalid for end bound, treat as empty frame
		end = -1
	}

	return start, end
}

// evaluateOffset evaluates a frame offset expression and returns the integer value.
func (w *PhysicalWindowExecutor) evaluateOffset(offsetExpr parser.Expr) int {
	if offsetExpr == nil {
		return 1 // Default offset is 1
	}

	// Try to evaluate as literal
	if lit, ok := offsetExpr.(*parser.Literal); ok {
		return int(toInt64Value(lit.Value))
	}

	return 1 // Default if evaluation fails
}

// ---------- EXCLUDE Clause (Tasks 4.30-4.34) ----------

// computeExcludedRows computes which rows to exclude based on EXCLUDE clause.
//
//nolint:unused // Will be used by computeFrameBounds
func (w *PhysicalWindowExecutor) computeExcludedRows(
	partition *WindowPartition,
	rowIdx int,
	exclude parser.ExcludeMode,
) []int {
	switch exclude {
	case parser.ExcludeNoOthers: // Task 4.31
		// No exclusion (default)
		return nil

	case parser.ExcludeCurrentRow: // Task 4.32
		// Exclude current row from frame
		return []int{rowIdx}

	case parser.ExcludeGroup: // Task 4.33
		// Exclude all peer group rows from frame
		peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)
		start := w.getPeerGroupStart(partition, peerGroupIdx)
		end := w.getPeerGroupEnd(partition, peerGroupIdx)
		excluded := make([]int, 0, end-start+1)
		for i := start; i <= end; i++ {
			excluded = append(excluded, i)
		}
		return excluded

	case parser.ExcludeTies: // Task 4.34
		// Exclude peer group rows except current row
		peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)
		start := w.getPeerGroupStart(partition, peerGroupIdx)
		end := w.getPeerGroupEnd(partition, peerGroupIdx)
		excluded := make([]int, 0, end-start)
		for i := start; i <= end; i++ {
			if i != rowIdx {
				excluded = append(excluded, i)
			}
		}
		return excluded

	default:
		return nil
	}
}

// isRowExcluded checks if a row index is in the excluded rows list.
func (w *PhysicalWindowExecutor) isRowExcluded(excludedRows []int, rowIdx int) bool {
	for _, excluded := range excludedRows {
		if excluded == rowIdx {
			return true
		}
	}
	return false
}

// ---------- Output Phase ----------

// flattenAndSort flattens all partitions back to original row order.
func (w *PhysicalWindowExecutor) flattenAndSort() []WindowRow {
	// Count total rows
	total := 0
	for _, partition := range w.state.Partitions {
		total += len(partition.Rows)
	}

	if total == 0 {
		return nil
	}

	// Collect all rows
	allRows := make([]WindowRow, 0, total)
	for _, partitionKey := range w.state.PartitionOrder {
		partition := w.state.Partitions[partitionKey]
		allRows = append(allRows, partition.Rows...)
	}

	// Sort by original index to restore input order
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].OriginalIndex < allRows[j].OriginalIndex
	})

	return allRows
}

// ---------- Phase 2: Window Function Evaluation (Tasks 4.35-4.69) ----------

// evaluateWindow evaluates all window functions for all partitions.
// This is called after materialization, sorting, and peer boundary computation.
func (w *PhysicalWindowExecutor) evaluateWindow() error {
	// Iterate over all partitions
	for _, partitionKey := range w.state.PartitionOrder {
		partition := w.state.Partitions[partitionKey]

		// For each row in the partition
		for rowIdx := range partition.Rows {
			// For each window expression
			for exprIdx, windowExpr := range w.window.WindowExprs {
				// Compute frame bounds for this row
				frame := w.computeFrameBounds(partition, rowIdx, windowExpr)

				// Evaluate the window function
				result := w.evaluateWindowFunction(windowExpr, partition, rowIdx, frame)

				// Store result
				partition.Rows[rowIdx].WindowResults[exprIdx] = result
			}
		}
	}

	return nil
}

// evaluateWindowFunction dispatches to the appropriate evaluator based on function type.
func (w *PhysicalWindowExecutor) evaluateWindowFunction(
	windowExpr *binder.BoundWindowExpr,
	partition *WindowPartition,
	rowIdx int,
	frame FrameBounds,
) any {
	funcName := strings.ToUpper(windowExpr.FunctionName)
	rowMap := w.buildRowMap(partition.Rows[rowIdx].Values)

	switch funcName {
	// Ranking functions (Tasks 4.35-4.38)
	case "ROW_NUMBER":
		return w.evaluateRowNumber(partition, rowIdx)
	case "RANK":
		return w.evaluateRank(partition, rowIdx)
	case "DENSE_RANK":
		return w.evaluateDenseRank(partition, rowIdx)
	case "NTILE":
		buckets := int64(1)
		if len(windowExpr.Args) > 0 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[0], rowMap); err == nil &&
				val != nil {
				buckets = toInt64Value(val)
			}
		}
		return w.evaluateNtile(partition, rowIdx, buckets)

	// Value functions (Tasks 4.39-4.48)
	case "LAG":
		offset := 1
		var defaultVal any
		if len(windowExpr.Args) > 1 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[1], rowMap); err == nil &&
				val != nil {
				offset = int(toInt64Value(val))
			}
		}
		if len(windowExpr.Args) > 2 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[2], rowMap); err == nil {
				defaultVal = val
			}
		}
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateLag(partition, rowIdx, expr, offset, defaultVal, windowExpr.IgnoreNulls)

	case "LEAD":
		offset := 1
		var defaultVal any
		if len(windowExpr.Args) > 1 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[1], rowMap); err == nil &&
				val != nil {
				offset = int(toInt64Value(val))
			}
		}
		if len(windowExpr.Args) > 2 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[2], rowMap); err == nil {
				defaultVal = val
			}
		}
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateLead(partition, rowIdx, expr, offset, defaultVal, windowExpr.IgnoreNulls)

	case "FIRST_VALUE":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateFirstValue(partition, frame, expr, windowExpr.IgnoreNulls)

	case "LAST_VALUE":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateLastValue(partition, frame, expr, windowExpr.IgnoreNulls)

	case "NTH_VALUE":
		n := 1
		if len(windowExpr.Args) > 1 {
			if val, err := w.executor.evaluateExpr(w.ctx, windowExpr.Args[1], rowMap); err == nil &&
				val != nil {
				n = int(toInt64Value(val))
			}
		}
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateNthValue(partition, frame, expr, n, windowExpr.IgnoreNulls)

	// Distribution functions (Tasks 4.49-4.51)
	case "PERCENT_RANK":
		return w.evaluatePercentRank(partition, rowIdx)
	case "CUME_DIST":
		return w.evaluateCumeDist(partition, rowIdx)

	// Aggregate window functions (Tasks 4.52-4.62)
	case "SUM":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateWindowSum(partition, frame, expr, windowExpr.Filter, windowExpr.Distinct)

	case "COUNT":
		var expr *binder.BoundExpr
		countStar := len(windowExpr.Args) == 0
		if !countStar {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateWindowCount(
			partition,
			frame,
			expr,
			windowExpr.Filter,
			windowExpr.Distinct,
			countStar,
		)

	case "AVG":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateWindowAvg(partition, frame, expr, windowExpr.Filter, windowExpr.Distinct)

	case "MIN":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateWindowMin(partition, frame, expr, windowExpr.Filter)

	case "MAX":
		var expr *binder.BoundExpr
		if len(windowExpr.Args) > 0 {
			expr = &windowExpr.Args[0]
		}
		return w.evaluateWindowMax(partition, frame, expr, windowExpr.Filter)

	default:
		return nil
	}
}

// ---------- Ranking Function Evaluators (Tasks 4.35-4.38) ----------

// evaluateRowNumber returns the sequential row number within the partition (1-based).
func (w *PhysicalWindowExecutor) evaluateRowNumber(_ *WindowPartition, rowIdx int) int64 {
	return int64(rowIdx + 1)
}

// evaluateRank returns the rank with gaps for ties.
// Rows with equal ORDER BY values get the same rank; the next rank skips accordingly.
func (w *PhysicalWindowExecutor) evaluateRank(partition *WindowPartition, rowIdx int) int64 {
	// Find which peer group this row belongs to
	peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)

	// Rank is the position of the first row in this peer group + 1
	return int64(w.getPeerGroupStart(partition, peerGroupIdx) + 1)
}

// evaluateDenseRank returns the rank without gaps.
// Each distinct ORDER BY value gets consecutive rank numbers.
func (w *PhysicalWindowExecutor) evaluateDenseRank(partition *WindowPartition, rowIdx int) int64 {
	// Dense rank is simply the peer group index + 1
	peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)
	return int64(peerGroupIdx + 1)
}

// evaluateNtile distributes rows into the specified number of buckets.
// If rows don't divide evenly, earlier buckets get one extra row.
func (w *PhysicalWindowExecutor) evaluateNtile(
	partition *WindowPartition,
	rowIdx int,
	buckets int64,
) int64 {
	if buckets <= 0 {
		buckets = 1
	}

	n := int64(len(partition.Rows))
	if n == 0 {
		return 1
	}

	// Calculate bucket assignment
	// If n = 10 and buckets = 3: buckets 1,2 get 4 rows, bucket 3 gets 2 rows
	// row 0-3 -> bucket 1, row 4-7 -> bucket 2, row 8-9 -> bucket 3

	// Each bucket gets at least n/buckets rows
	baseSize := n / buckets
	// The first (n % buckets) buckets get one extra row
	extra := n % buckets

	// Calculate which bucket this row falls into
	idx := int64(rowIdx)

	// Rows in the "extra" buckets (larger buckets)
	largerBucketRows := extra * (baseSize + 1)

	if idx < largerBucketRows {
		// Row is in one of the larger buckets
		return (idx / (baseSize + 1)) + 1
	}

	// Row is in one of the smaller buckets
	idx -= largerBucketRows
	return extra + (idx / baseSize) + 1
}

// ---------- Value Function Evaluators (Tasks 4.39-4.48) ----------

// evaluateLag returns the value from a row that is offset rows before the current row.
func (w *PhysicalWindowExecutor) evaluateLag(
	partition *WindowPartition,
	rowIdx int,
	expr *binder.BoundExpr,
	offset int,
	defaultVal any,
	ignoreNulls bool,
) any {
	if expr == nil {
		return defaultVal
	}

	if ignoreNulls {
		// Skip NULL values when looking back
		nonNullCount := 0
		for i := rowIdx - 1; i >= 0; i-- {
			rowMap := w.buildRowMap(partition.Rows[i].Values)
			val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
			if err != nil {
				continue
			}
			if val != nil {
				nonNullCount++
				if nonNullCount == offset {
					return val
				}
			}
		}
		return defaultVal
	}

	// Standard LAG without IGNORE NULLS
	targetIdx := rowIdx - offset
	if targetIdx < 0 {
		return defaultVal
	}

	rowMap := w.buildRowMap(partition.Rows[targetIdx].Values)
	val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
	if err != nil {
		return defaultVal
	}

	return val
}

// evaluateLead returns the value from a row that is offset rows after the current row.
func (w *PhysicalWindowExecutor) evaluateLead(
	partition *WindowPartition,
	rowIdx int,
	expr *binder.BoundExpr,
	offset int,
	defaultVal any,
	ignoreNulls bool,
) any {
	if expr == nil {
		return defaultVal
	}

	partitionSize := len(partition.Rows)

	if ignoreNulls {
		// Skip NULL values when looking forward
		nonNullCount := 0
		for i := rowIdx + 1; i < partitionSize; i++ {
			rowMap := w.buildRowMap(partition.Rows[i].Values)
			val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
			if err != nil {
				continue
			}
			if val != nil {
				nonNullCount++
				if nonNullCount == offset {
					return val
				}
			}
		}
		return defaultVal
	}

	// Standard LEAD without IGNORE NULLS
	targetIdx := rowIdx + offset
	if targetIdx >= partitionSize {
		return defaultVal
	}

	rowMap := w.buildRowMap(partition.Rows[targetIdx].Values)
	val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
	if err != nil {
		return defaultVal
	}

	return val
}

// evaluateFirstValue returns the first value in the frame.
func (w *PhysicalWindowExecutor) evaluateFirstValue(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	ignoreNulls bool,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)
		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil {
			continue
		}

		if ignoreNulls && val == nil {
			continue
		}

		return val
	}

	return nil
}

// evaluateLastValue returns the last value in the frame.
func (w *PhysicalWindowExecutor) evaluateLastValue(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	ignoreNulls bool,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	for i := frame.End; i >= frame.Start; i-- {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)
		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil {
			continue
		}

		if ignoreNulls && val == nil {
			continue
		}

		return val
	}

	return nil
}

// evaluateNthValue returns the Nth value in the frame (1-based indexing).
func (w *PhysicalWindowExecutor) evaluateNthValue(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	n int,
	ignoreNulls bool,
) any {
	if expr == nil || n <= 0 {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	count := 0
	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)
		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil {
			continue
		}

		if ignoreNulls && val == nil {
			continue
		}

		count++
		if count == n {
			return val
		}
	}

	return nil
}

// ---------- Distribution Function Evaluators (Tasks 4.49-4.51) ----------

// evaluatePercentRank returns (rank - 1) / (n - 1) where rank uses gaps.
// Returns 0.0 if partition has only one row.
func (w *PhysicalWindowExecutor) evaluatePercentRank(
	partition *WindowPartition,
	rowIdx int,
) float64 {
	n := len(partition.Rows)
	if n <= 1 {
		return 0.0
	}

	rank := w.evaluateRank(partition, rowIdx)
	return float64(rank-1) / float64(n-1)
}

// evaluateCumeDist returns the cumulative distribution: (rows at or before current) / n.
// "At or before" includes all rows in the same peer group.
func (w *PhysicalWindowExecutor) evaluateCumeDist(partition *WindowPartition, rowIdx int) float64 {
	n := len(partition.Rows)
	if n == 0 {
		return 1.0
	}

	// Find the peer group this row belongs to
	peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)

	// Get the end of this peer group (inclusive)
	peerGroupEnd := w.getPeerGroupEnd(partition, peerGroupIdx)

	// Count of rows at or before = end of peer group + 1 (0-based to 1-based)
	rowsAtOrBefore := peerGroupEnd + 1

	return float64(rowsAtOrBefore) / float64(n)
}

// ---------- Aggregate Window Function Evaluators (Tasks 4.52-4.62) ----------

// evaluateWindowSum computes SUM over the window frame.
func (w *PhysicalWindowExecutor) evaluateWindowSum(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	filter binder.BoundExpr,
	distinct bool,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	var sum float64
	hasValue := false
	seen := make(map[string]bool)

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)

		// Check FILTER condition
		if filter != nil {
			filterVal, err := w.executor.evaluateExpr(w.ctx, filter, rowMap)
			if err != nil || !toBool(filterVal) {
				continue
			}
		}

		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil || val == nil {
			continue
		}

		// Handle DISTINCT
		if distinct {
			key := fmt.Sprintf("%v", val)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		sum += toFloat64Value(val)
		hasValue = true
	}

	if !hasValue {
		return nil
	}

	return sum
}

// evaluateWindowCount computes COUNT over the window frame.
func (w *PhysicalWindowExecutor) evaluateWindowCount(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	filter binder.BoundExpr,
	distinct bool,
	countStar bool,
) int64 {
	// Empty frame
	if frame.Start > frame.End {
		return 0
	}

	count := int64(0)
	seen := make(map[string]bool)

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)

		// Check FILTER condition
		if filter != nil {
			filterVal, err := w.executor.evaluateExpr(w.ctx, filter, rowMap)
			if err != nil || !toBool(filterVal) {
				continue
			}
		}

		if countStar {
			// COUNT(*) counts all rows
			count++
			continue
		}

		if expr == nil {
			count++
			continue
		}

		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil || val == nil {
			// COUNT(expr) excludes NULLs
			continue
		}

		// Handle DISTINCT
		if distinct {
			key := fmt.Sprintf("%v", val)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		count++
	}

	return count
}

// evaluateWindowAvg computes AVG over the window frame.
func (w *PhysicalWindowExecutor) evaluateWindowAvg(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	filter binder.BoundExpr,
	distinct bool,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	var sum float64
	count := 0
	seen := make(map[string]bool)

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)

		// Check FILTER condition
		if filter != nil {
			filterVal, err := w.executor.evaluateExpr(w.ctx, filter, rowMap)
			if err != nil || !toBool(filterVal) {
				continue
			}
		}

		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil || val == nil {
			continue
		}

		// Handle DISTINCT
		if distinct {
			key := fmt.Sprintf("%v", val)
			if seen[key] {
				continue
			}
			seen[key] = true
		}

		sum += toFloat64Value(val)
		count++
	}

	if count == 0 {
		return nil
	}

	return sum / float64(count)
}

// evaluateWindowMin computes MIN over the window frame.
func (w *PhysicalWindowExecutor) evaluateWindowMin(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	filter binder.BoundExpr,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	var minVal any

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)

		// Check FILTER condition
		if filter != nil {
			filterVal, err := w.executor.evaluateExpr(w.ctx, filter, rowMap)
			if err != nil || !toBool(filterVal) {
				continue
			}
		}

		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil || val == nil {
			continue
		}

		if minVal == nil || compareValues(val, minVal) < 0 {
			minVal = val
		}
	}

	return minVal
}

// evaluateWindowMax computes MAX over the window frame.
func (w *PhysicalWindowExecutor) evaluateWindowMax(
	partition *WindowPartition,
	frame FrameBounds,
	expr *binder.BoundExpr,
	filter binder.BoundExpr,
) any {
	if expr == nil {
		return nil
	}

	// Empty frame
	if frame.Start > frame.End {
		return nil
	}

	var maxVal any

	for i := frame.Start; i <= frame.End; i++ {
		// Check if row is excluded
		if w.isRowExcluded(frame.ExcludedRows, i) {
			continue
		}

		rowMap := w.buildRowMap(partition.Rows[i].Values)

		// Check FILTER condition
		if filter != nil {
			filterVal, err := w.executor.evaluateExpr(w.ctx, filter, rowMap)
			if err != nil || !toBool(filterVal) {
				continue
			}
		}

		val, err := w.executor.evaluateExpr(w.ctx, *expr, rowMap)
		if err != nil || val == nil {
			continue
		}

		if maxVal == nil || compareValues(val, maxVal) > 0 {
			maxVal = val
		}
	}

	return maxVal
}

// Note: basicTypeInfo is defined in physical_scan.go and reused here.
