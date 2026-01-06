// Package parallel provides parallel query execution infrastructure.
// This file defines error types for the parallel package.
package parallel

import "errors"

// Error definitions for parallel execution.
var (
	// ErrNoDataReader is returned when a scan is attempted without a data reader.
	ErrNoDataReader = errors.New("no data reader configured")

	// ErrTableNotFound is returned when the requested table does not exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrRowGroupNotFound is returned when the requested row group does not exist.
	ErrRowGroupNotFound = errors.New("row group not found")

	// ErrInvalidProjection is returned when a projection index is out of bounds.
	ErrInvalidProjection = errors.New("invalid projection index")

	// ErrScanCancelled is returned when a scan is cancelled via context.
	ErrScanCancelled = errors.New("scan cancelled")
)
