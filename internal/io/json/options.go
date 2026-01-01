// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file contains options for the JSON reader and writer.
package json

import (
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for reader options.
const (
	// FormatAuto indicates automatic format detection.
	FormatAuto = "auto"
	// FormatArray indicates JSON array format: [{}, {}, ...]
	FormatArray = "array"
	// FormatNDJSON indicates newline-delimited JSON format (one object per line).
	FormatNDJSON = "newline_delimited"
	// FormatNDJSONShort is an alias for FormatNDJSON.
	FormatNDJSONShort = "ndjson"

	// DefaultSampleSize is the default number of objects to sample for type inference.
	DefaultSampleSize = 1000
	// DefaultMaxRowsPerChunk is the default maximum number of rows per DataChunk.
	DefaultMaxRowsPerChunk = storage.StandardVectorSize
	// DefaultMaxDepth is the default maximum depth for nested objects.
	DefaultMaxDepth = 64
	// DefaultMaxObjectSize is the default maximum object size in bytes (10MB).
	DefaultMaxObjectSize = 10 * 1024 * 1024
)

// ReaderOptions contains configuration options for the JSON reader.
type ReaderOptions struct {
	// Format specifies the JSON format: "array" or "newline_delimited".
	// Default is "array".
	Format string

	// MaxDepth is the maximum nesting depth for JSON objects.
	// Objects nested deeper than this are serialized as VARCHAR.
	// Default is DefaultMaxDepth (64).
	MaxDepth int

	// MaxObjectSize is the maximum size in bytes for a single JSON object.
	// Objects larger than this are rejected.
	// Default is DefaultMaxObjectSize (10MB).
	MaxObjectSize int

	// SampleSize is the number of objects to sample for schema and type inference.
	// Default is DefaultSampleSize (1000).
	SampleSize int

	// MaxRowsPerChunk limits the number of rows returned per DataChunk.
	// Default is DefaultMaxRowsPerChunk (2048).
	MaxRowsPerChunk int

	// DateFormat specifies the expected format for date strings.
	// If empty, multiple common formats are tried.
	DateFormat string

	// TimestampFormat specifies the expected format for timestamp strings.
	// If empty, multiple common formats are tried.
	TimestampFormat string

	// Compression specifies the compression type.
	// If not set, auto-detection is used.
	Compression fileio.Compression

	// IgnoreErrors indicates whether to skip malformed objects instead of failing.
	IgnoreErrors bool
}

// DefaultReaderOptions returns ReaderOptions with sensible defaults.
func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{
		Format:          FormatAuto,
		MaxDepth:        DefaultMaxDepth,
		MaxObjectSize:   DefaultMaxObjectSize,
		SampleSize:      DefaultSampleSize,
		MaxRowsPerChunk: DefaultMaxRowsPerChunk,
		DateFormat:      "",
		TimestampFormat: "",
		Compression:     fileio.CompressionNone,
		IgnoreErrors:    false,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *ReaderOptions) applyDefaults() {
	if o.Format == "" {
		o.Format = FormatAuto
	}

	// Normalize "ndjson" to "newline_delimited".
	if o.Format == FormatNDJSONShort {
		o.Format = FormatNDJSON
	}

	if o.MaxDepth <= 0 {
		o.MaxDepth = DefaultMaxDepth
	}

	if o.MaxObjectSize <= 0 {
		o.MaxObjectSize = DefaultMaxObjectSize
	}

	if o.SampleSize <= 0 {
		o.SampleSize = DefaultSampleSize
	}

	if o.MaxRowsPerChunk <= 0 {
		o.MaxRowsPerChunk = DefaultMaxRowsPerChunk
	}
}
