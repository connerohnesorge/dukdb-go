// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file contains options for the Parquet reader and writer.
package parquet

import (
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for reader options.
const (
	// DefaultMaxRowsPerChunk is the default maximum number of rows per DataChunk.
	DefaultMaxRowsPerChunk = storage.StandardVectorSize
)

// ReaderOptions contains configuration options for the Parquet reader.
type ReaderOptions struct {
	// Columns to read (nil = all columns).
	// If specified, only these columns are read from the file.
	Columns []string

	// MaxRowsPerChunk limits the number of rows returned per DataChunk.
	// Default is DefaultMaxRowsPerChunk (2048).
	MaxRowsPerChunk int
}

// DefaultReaderOptions returns ReaderOptions with sensible defaults.
func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{
		Columns:         nil, // Read all columns
		MaxRowsPerChunk: DefaultMaxRowsPerChunk,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *ReaderOptions) applyDefaults() {
	if o.MaxRowsPerChunk <= 0 {
		o.MaxRowsPerChunk = DefaultMaxRowsPerChunk
	}
}
