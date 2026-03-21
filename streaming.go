package dukdb

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
)

// StreamingResult delivers query results one row at a time from a chunked pipeline.
// It wraps a scanNext closure that pulls rows from DataChunks without requiring
// the caller to know about internal storage types.
//
// The scanNext closure is constructed by ExecuteStreaming in the executor package.
// It captures all chunk iteration state (current chunk, row position) internally,
// so StreamingResult itself does not import internal/storage.
type StreamingResult struct {
	mu       sync.Mutex
	columns  []string
	scanNext func(dest []driver.Value) error // closure that reads one row
	closed   bool
	cancel   context.CancelFunc // cancels the query context
}

// NewStreamingResult creates a new StreamingResult.
// scanNext is called to populate each row -- it should return io.EOF when done.
// cancel is called on Close() to stop the underlying query.
func NewStreamingResult(
	columns []string,
	scanNext func(dest []driver.Value) error,
	cancel context.CancelFunc,
) *StreamingResult {
	return &StreamingResult{
		columns:  columns,
		scanNext: scanNext,
		cancel:   cancel,
	}
}

// Columns returns the result column names.
func (sr *StreamingResult) Columns() []string {
	return sr.columns
}

// ScanNext reads the next row into dest. Returns io.EOF when exhausted.
// The dest slice must have length >= len(Columns()).
func (sr *StreamingResult) ScanNext(dest []driver.Value) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return io.EOF
	}

	return sr.scanNext(dest)
}

// Close releases resources and cancels the query context.
// Safe to call multiple times.
func (sr *StreamingResult) Close() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return nil
	}
	sr.closed = true
	if sr.cancel != nil {
		sr.cancel()
	}

	return nil
}
