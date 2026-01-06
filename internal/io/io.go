// Package io provides file I/O interfaces and utilities for reading and writing
// various file formats (CSV, JSON, Parquet) to and from DataChunks.
package io

import (
	"io"

	"github.com/dukdb/dukdb-go/internal/storage"
)

// Format represents the file format type.
type Format int

// String constants for format and compression types.
const (
	unknownStr = "unknown"
)

const (
	// FormatUnknown indicates the format could not be determined.
	FormatUnknown Format = iota
	// FormatCSV indicates CSV (Comma-Separated Values) format.
	FormatCSV
	// FormatJSON indicates JSON array format.
	FormatJSON
	// FormatNDJSON indicates newline-delimited JSON format.
	FormatNDJSON
	// FormatParquet indicates Apache Parquet format.
	FormatParquet
	// FormatArrow indicates Apache Arrow IPC file format.
	FormatArrow
	// FormatArrowStream indicates Apache Arrow IPC stream format.
	FormatArrowStream
)

// String returns the string representation of a Format.
func (f Format) String() string {
	switch f {
	case FormatUnknown:
		return unknownStr
	case FormatCSV:
		return "csv"
	case FormatJSON:
		return "json"
	case FormatNDJSON:
		return "ndjson"
	case FormatParquet:
		return "parquet"
	case FormatArrow:
		return "arrow"
	case FormatArrowStream:
		return "arrow_stream"
	}

	return unknownStr
}

// Compression represents the compression type for file I/O.
type Compression int

const (
	// CompressionNone indicates no compression.
	CompressionNone Compression = iota
	// CompressionGZIP indicates GZIP compression.
	CompressionGZIP
	// CompressionZSTD indicates ZSTD compression.
	CompressionZSTD
	// CompressionSnappy indicates Snappy compression.
	CompressionSnappy
	// CompressionLZ4 indicates LZ4 compression.
	CompressionLZ4
	// CompressionBrotli indicates Brotli compression.
	CompressionBrotli
)

// String returns the string representation of a Compression type.
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZIP:
		return "gzip"
	case CompressionZSTD:
		return "zstd"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionBrotli:
		return "brotli"
	}

	return unknownStr
}

// ReaderOptions contains common options for all file readers.
type ReaderOptions struct {
	// Path is the file path to read from. Required if Reader is nil.
	Path string

	// Reader is an optional io.Reader to read from instead of a file path.
	Reader io.Reader

	// Format specifies the file format. If not set, auto-detection is used.
	Format Format

	// Compression specifies the compression type. If not set, auto-detection is used.
	Compression Compression

	// SampleSize is the number of rows to sample for type inference.
	// Default is 1000 if not specified.
	SampleSize int

	// MaxRowsPerChunk limits the number of rows per DataChunk.
	// Default is storage.StandardVectorSize (2048).
	MaxRowsPerChunk int

	// IgnoreErrors indicates whether to skip malformed rows instead of failing.
	IgnoreErrors bool

	// Encoding specifies the character encoding (e.g., "utf-8", "latin1").
	// Default is "utf-8".
	Encoding string
}

// WriterOptions contains common options for all file writers.
type WriterOptions struct {
	// Path is the file path to write to. Required if Writer is nil.
	Path string

	// Writer is an optional io.Writer to write to instead of a file path.
	Writer io.Writer

	// Format specifies the file format.
	Format Format

	// Compression specifies the compression type to use.
	Compression Compression

	// Overwrite indicates whether to overwrite existing files.
	Overwrite bool
}

// FileReader is the interface for reading file formats into DataChunks.
// Implementations should read data in chunks for memory efficiency.
type FileReader interface {
	// ReadChunk reads the next chunk of data from the file.
	// Returns the DataChunk and any error encountered.
	// Returns io.EOF when no more data is available.
	ReadChunk() (*storage.DataChunk, error)

	// Schema returns the column names after the reader has been initialized.
	// This may require reading a portion of the file for schema inference.
	Schema() ([]string, error)

	// Close releases any resources held by the reader.
	Close() error
}

// FileWriter is the interface for writing DataChunks to file formats.
type FileWriter interface {
	// WriteChunk writes a DataChunk to the file.
	// Returns any error encountered during writing.
	WriteChunk(chunk *storage.DataChunk) error

	// SetSchema sets the column names for the output file.
	// Must be called before WriteChunk if the format requires headers.
	SetSchema(columns []string) error

	// Close flushes any buffered data and releases resources.
	Close() error
}
