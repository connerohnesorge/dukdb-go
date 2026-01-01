// Package csv provides CSV file reading and writing capabilities for dukdb-go.
package csv

import (
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for reader options.
const (
	// DefaultSampleSize is the default number of rows to sample for type inference.
	DefaultSampleSize = 1000
	// DefaultMaxRowsPerChunk is the default maximum number of rows per DataChunk.
	DefaultMaxRowsPerChunk = storage.StandardVectorSize
	// DefaultQuote is the default quote character.
	DefaultQuote = '"'
	// DefaultDelimiter is the default field delimiter.
	DefaultDelimiter = ','
)

// ReaderOptions contains configuration options for the CSV reader.
type ReaderOptions struct {
	// Delimiter is the field separator character.
	// If zero, auto-detection is attempted.
	Delimiter rune

	// Quote is the character used to quote fields containing special characters.
	// Default is double quote (").
	Quote rune

	// Header indicates whether the first row contains column names.
	// If false and not auto-detected, columns are named "column0", "column1", etc.
	Header bool

	// NullStr is the string representation of NULL values.
	// Empty string matches empty fields.
	NullStr string

	// Skip is the number of rows to skip before reading data (or header).
	Skip int

	// Comment is the character that marks comment lines to skip.
	// Lines starting with this character are ignored.
	// A zero value means no comment character is set.
	Comment rune

	// SampleSize is the number of rows to sample for delimiter and header detection.
	// Default is DefaultSampleSize (1000).
	SampleSize int

	// MaxRowsPerChunk limits the number of rows returned per DataChunk.
	// Default is DefaultMaxRowsPerChunk (2048).
	MaxRowsPerChunk int

	// IgnoreErrors indicates whether to skip malformed rows instead of failing.
	IgnoreErrors bool
}

// DefaultReaderOptions returns ReaderOptions with sensible defaults.
func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{
		Delimiter:       0, // Auto-detect
		Quote:           DefaultQuote,
		Header:          true,
		NullStr:         "",
		Skip:            0,
		Comment:         0,
		SampleSize:      DefaultSampleSize,
		MaxRowsPerChunk: DefaultMaxRowsPerChunk,
		IgnoreErrors:    false,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *ReaderOptions) applyDefaults() {
	if o.Quote == 0 {
		o.Quote = DefaultQuote
	}

	if o.SampleSize <= 0 {
		o.SampleSize = DefaultSampleSize
	}

	if o.MaxRowsPerChunk <= 0 {
		o.MaxRowsPerChunk = DefaultMaxRowsPerChunk
	}
}
