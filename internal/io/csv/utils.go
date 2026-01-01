// Package csv provides CSV file reading and writing capabilities for dukdb-go.
// This file contains utility types and functions for the CSV package.
package csv

import (
	"fmt"
	"io"
)

// combinedCloser closes both a decompressor and underlying file.
// This ensures proper cleanup when reading compressed files.
type combinedCloser struct {
	decompressor io.Closer
	file         io.Closer
}

// Close closes both the decompressor and the underlying file.
// The first error encountered is returned.
func (c *combinedCloser) Close() error {
	var firstErr error

	if c.decompressor != nil {
		if err := c.decompressor.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if c.file != nil {
		if err := c.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// generateColumnNames creates default column names like "column0", "column1", etc.
func generateColumnNames(count int) []string {
	names := make([]string, count)

	for i := range count {
		names[i] = fmt.Sprintf("column%d", i)
	}

	return names
}
