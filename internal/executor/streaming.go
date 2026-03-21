package executor

import (
	"context"
	"database/sql/driver"
	"io"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// ExecuteStreaming executes a physical plan and returns a StreamingResult that
// delivers rows one at a time via a scanNext closure.
//
// For the initial implementation, this materializes the results using the
// existing Execute path and wraps them in a streaming interface. True lazy
// chunk-by-chunk evaluation (where rows are pulled from the physical operator
// pipeline on demand) is future work -- it requires refactoring every executor
// method to separate "build operator" from "iterate and collect".
//
// Context cancellation is checked between rows so that callers can cancel
// mid-stream.
func (e *Executor) ExecuteStreaming(
	ctx context.Context,
	plan planner.PhysicalPlan,
	args []driver.NamedValue,
) (*dukdb.StreamingResult, error) {
	// Materialize results using the existing path.
	result, err := e.Execute(ctx, plan, args)
	if err != nil {
		return nil, err
	}

	_, cancel := context.WithCancel(ctx)

	pos := 0
	columns := result.Columns
	rows := result.Rows

	// The scanNext closure converts materialized map rows directly into
	// driver.Value slices. This is the DataChunk-to-driver.Value conversion
	// path referenced by task 1.3 -- in the current materialized-then-wrapped
	// approach, the conversion happens here rather than from raw DataChunks.
	scanNext := func(dest []driver.Value) error {
		// Check context cancellation between row fetches.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if pos >= len(rows) {
			return io.EOF
		}
		row := rows[pos]
		for i, col := range columns {
			dest[i] = row[col]
		}
		pos++

		return nil
	}

	return dukdb.NewStreamingResult(columns, scanNext, cancel), nil
}
