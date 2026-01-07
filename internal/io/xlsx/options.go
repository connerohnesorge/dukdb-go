// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file contains options for the XLSX reader and writer.
package xlsx

import (
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for reader options.
const (
	// DefaultChunkSize is the default maximum number of rows per DataChunk.
	DefaultChunkSize = storage.StandardVectorSize
	// DefaultSheetIndex indicates that no specific sheet index is set.
	DefaultSheetIndex = -1
	// DefaultCompressionLevel is the default ZIP compression level.
	DefaultCompressionLevel = 6
)

// ReaderOptions contains configuration options for the XLSX reader.
type ReaderOptions struct {
	// Sheet is the name of the sheet to read.
	// If empty, the first sheet is used unless SheetIndex is specified.
	Sheet string

	// SheetIndex is the 0-based index of the sheet to read.
	// A value of -1 means not specified (use Sheet name or default to first sheet).
	SheetIndex int

	// Range is the cell range to read in A1 notation (e.g., "A1:D100").
	// If empty, the entire used range is read.
	Range string

	// StartRow is the first row to read (1-based).
	// A value of 0 means start from the beginning.
	StartRow int

	// EndRow is the last row to read (1-based).
	// A value of 0 means read all remaining rows.
	EndRow int

	// StartCol is the first column to read (e.g., "A").
	// If empty, start from column A.
	StartCol string

	// EndCol is the last column to read (e.g., "Z").
	// If empty, read all columns with data.
	EndCol string

	// Header indicates whether the first row contains column names.
	// Default is true.
	Header bool

	// Skip is the number of rows to skip before the header row.
	Skip int

	// Columns provides explicit column type mappings.
	// Keys are column names, values are type names (e.g., "VARCHAR", "INTEGER").
	Columns map[string]string

	// InferTypes indicates whether to auto-detect column types.
	// Default is true.
	InferTypes bool

	// DateFormat is a hint for parsing date values.
	// If empty, standard Excel date formats are used.
	DateFormat string

	// ChunkSize is the maximum number of rows per DataChunk.
	// Default is DefaultChunkSize (2048).
	ChunkSize int

	// EmptyAsNull indicates whether to treat empty cells as NULL.
	// Default is true.
	EmptyAsNull bool

	// NullValues is a list of additional strings to treat as NULL.
	NullValues []string
}

// DefaultReaderOptions returns ReaderOptions with sensible defaults.
func DefaultReaderOptions() *ReaderOptions {
	return &ReaderOptions{
		Sheet:       "",
		SheetIndex:  DefaultSheetIndex,
		Range:       "",
		StartRow:    0,
		EndRow:      0,
		StartCol:    "",
		EndCol:      "",
		Header:      true,
		Skip:        0,
		Columns:     nil,
		InferTypes:  true,
		DateFormat:  "",
		ChunkSize:   DefaultChunkSize,
		EmptyAsNull: true,
		NullValues:  nil,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *ReaderOptions) applyDefaults() {
	if o.SheetIndex == 0 && o.Sheet == "" {
		o.SheetIndex = DefaultSheetIndex
	}

	if o.ChunkSize <= 0 {
		o.ChunkSize = DefaultChunkSize
	}
}

// WriterOptions contains configuration options for the XLSX writer.
type WriterOptions struct {
	// SheetName is the name for the sheet.
	// Default is "Sheet1".
	SheetName string

	// Header indicates whether to write column names as the first row.
	// Default is true.
	Header bool

	// DateFormat is the Excel format string for date values.
	// If empty, a standard date format is used.
	DateFormat string

	// TimeFormat is the Excel format string for time values.
	// If empty, a standard time format is used.
	TimeFormat string

	// AutoWidth indicates whether to auto-calculate column widths.
	// Default is true.
	AutoWidth bool

	// CompressionLevel is the ZIP compression level (0-9).
	// Default is 6.
	CompressionLevel int
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		SheetName:        "Sheet1",
		Header:           true,
		DateFormat:       "",
		TimeFormat:       "",
		AutoWidth:        true,
		CompressionLevel: DefaultCompressionLevel,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *WriterOptions) applyDefaults() {
	if o.SheetName == "" {
		o.SheetName = "Sheet1"
	}

	if o.CompressionLevel < 0 {
		o.CompressionLevel = 0
	} else if o.CompressionLevel > 9 {
		o.CompressionLevel = 9
	}
}
