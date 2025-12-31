package executor

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// PhysicalLimitOperator implements the PhysicalOperator interface for LIMIT/OFFSET.
// It is a streaming operator that skips OFFSET rows and then produces at most LIMIT rows.
type PhysicalLimitOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	limit        int64
	offset       int64
	types        []dukdb.TypeInfo
	skipped      int64 // Tracks how many rows have been skipped so far
	emitted      int64 // Tracks how many rows have been emitted so far
}

// NewPhysicalLimitOperator creates a new PhysicalLimitOperator.
func NewPhysicalLimitOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	limit int64,
	offset int64,
) (*PhysicalLimitOperator, error) {
	// Get types from child
	types := child.GetTypes()

	return &PhysicalLimitOperator{
		child:        child,
		childColumns: childColumns,
		limit:        limit,
		offset:       offset,
		types:        types,
		skipped:      0,
		emitted:      0,
	}, nil
}

// Next returns the next DataChunk with LIMIT/OFFSET applied, or nil if no more data.
// This is a streaming operator that processes chunks on-the-fly.
func (op *PhysicalLimitOperator) Next() (*storage.DataChunk, error) {
	// If we've already emitted enough rows (and have a limit), stop
	if op.limit >= 0 && op.emitted >= op.limit {
		return nil, nil
	}

	for {
		// Get next chunk from child
		inputChunk, err := op.child.Next()
		if err != nil {
			return nil, err
		}
		if inputChunk == nil {
			// No more input data
			return nil, nil
		}

		// Skip rows for OFFSET
		startIdx := 0
		if op.offset > 0 && op.skipped < op.offset {
			rowsToSkip := op.offset - op.skipped
			chunkSize := int64(inputChunk.Count())

			if rowsToSkip >= chunkSize {
				// Skip entire chunk
				op.skipped += chunkSize
				continue
			}

			// Skip partial chunk
			startIdx = int(rowsToSkip)
			op.skipped += rowsToSkip
		}

		// Determine how many rows to emit from this chunk
		remainingInChunk := inputChunk.Count() - startIdx
		var rowsToEmit int

		if op.limit < 0 {
			// No limit, emit all remaining rows in chunk
			rowsToEmit = remainingInChunk
		} else {
			// Calculate how many more rows we can emit
			remainingToEmit := op.limit - op.emitted
			if int64(remainingInChunk) <= remainingToEmit {
				rowsToEmit = remainingInChunk
			} else {
				rowsToEmit = int(remainingToEmit)
			}
		}

		// If no rows to emit, we're done
		if rowsToEmit <= 0 {
			return nil, nil
		}

		// Create output chunk with selected rows
		outputChunk := storage.NewDataChunkWithCapacity(
			inputChunk.Types(),
			rowsToEmit,
		)

		for i := 0; i < rowsToEmit; i++ {
			rowIdx := startIdx + i
			values := make([]any, inputChunk.ColumnCount())
			for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
				values[colIdx] = inputChunk.GetValue(rowIdx, colIdx)
			}
			outputChunk.AppendRow(values)
		}

		// Update emitted count
		op.emitted += int64(rowsToEmit)

		// Return the output chunk (if it has rows)
		if outputChunk.Count() > 0 {
			return outputChunk, nil
		}

		// Otherwise continue to next chunk
	}
}

// GetTypes returns the TypeInfo for each column produced by this operator.
// Limit doesn't change the schema, so it returns the same types as the child.
func (op *PhysicalLimitOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}
