// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file contains type definitions, options, and constants for Arrow IPC I/O.
package arrow

import (
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for reader/writer options.
const (
	// DefaultMaxRowsPerChunk is the default maximum number of rows per DataChunk.
	DefaultMaxRowsPerChunk = storage.StandardVectorSize
)

// Arrow IPC file format constants.
const (
	// ArrowFileMagic is the magic bytes at the start and end of Arrow IPC files.
	// The file format starts with "ARROW1" followed by padding.
	ArrowFileMagic = "ARROW1"

	// ArrowFileMagicLen is the length of the Arrow file magic bytes.
	ArrowFileMagicLen = 6

	// ArrowStreamMagic indicates the start of an Arrow IPC stream.
	// Streams start with a schema message (continuation indicator 0xFFFFFFFF).
	ArrowStreamContinuation uint32 = 0xFFFFFFFF
)

// Compression represents the compression type for Arrow IPC files.
type Compression int

const (
	// CompressionNone indicates no compression.
	CompressionNone Compression = iota
	// CompressionLZ4 indicates LZ4 frame compression.
	CompressionLZ4
	// CompressionZSTD indicates ZSTD compression.
	CompressionZSTD
)

// String returns the string representation of a Compression type.
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

// ToIPCCodec converts a Compression to an Arrow IPC codec.
func (c Compression) ToIPCCodec() ipc.Option {
	switch c {
	case CompressionLZ4:
		return ipc.WithLZ4()
	case CompressionZSTD:
		return ipc.WithZstd()
	default:
		return nil
	}
}

// ReaderOptions contains configuration options for Arrow IPC readers.
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

// WriterOptions contains configuration options for Arrow IPC writers.
type WriterOptions struct {
	// Compression specifies the compression type to use.
	// Default is CompressionNone.
	Compression Compression

	// MaxRowsPerBatch limits the number of rows per record batch.
	// Default is DefaultMaxRowsPerChunk (2048).
	MaxRowsPerBatch int
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		Compression:     CompressionNone,
		MaxRowsPerBatch: DefaultMaxRowsPerChunk,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *WriterOptions) applyDefaults() {
	if o.MaxRowsPerBatch <= 0 {
		o.MaxRowsPerBatch = DefaultMaxRowsPerChunk
	}
}
