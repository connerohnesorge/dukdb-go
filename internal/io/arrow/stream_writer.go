// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file implements the FileWriter interface for Arrow IPC stream format.
package arrow

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// StreamWriter implements the FileWriter interface for Arrow IPC streams.
// Unlike the file writer, stream writers write data sequentially without
// a footer, making them suitable for streaming scenarios where the total
// number of record batches is not known in advance.
//
// The stream format writes:
// 1. Schema message (at the start)
// 2. Record batches (as they are written)
// 3. End-of-stream marker (on Close)
//
// No footer with block offsets is written, so the stream cannot be randomly accessed.
type StreamWriter struct {
	// rawWriter is the underlying io.Writer for output.
	rawWriter io.Writer
	// closer is used to close file handles if writing to a file.
	closer io.Closer
	// opts contains the writer configuration options.
	opts *WriterOptions
	// alloc is the memory allocator for Arrow operations.
	alloc memory.Allocator
	// columns holds column names set via SetSchema.
	columns []string
	// columnTypes holds the DuckDB type for each column.
	columnTypes []dukdb.Type
	// arrowSchema is the Arrow schema built from column info.
	arrowSchema *arrow.Schema
	// streamWriter is the Arrow IPC stream writer.
	streamWriter *ipc.Writer
	// initialized tracks whether the writer has been initialized.
	initialized bool
}

// NewStreamWriter creates a new Arrow IPC stream writer to an io.Writer.
// If opts is nil, DefaultWriterOptions are used.
// The writer must be closed after use to finalize the stream.
func NewStreamWriter(w io.Writer, opts *WriterOptions) (*StreamWriter, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	return createStreamWriter(w, nil, writerOpts)
}

// NewStreamWriterToPath creates a new Arrow IPC stream writer to a file path.
// If opts is nil, DefaultWriterOptions are used.
// If the file already exists, returns an error.
// The writer must be closed after use to finalize the stream.
func NewStreamWriterToPath(path string, opts *WriterOptions) (*StreamWriter, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	// Check if file exists.
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("arrow: file %q already exists", path)
	}

	// Create the output file.
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to create file: %w", err)
	}

	return createStreamWriter(file, file, writerOpts)
}

// NewStreamWriterToPathOverwrite creates a new Arrow IPC stream writer to a file path,
// overwriting if the file already exists.
func NewStreamWriterToPathOverwrite(path string, opts *WriterOptions) (*StreamWriter, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	// Create the output file (will overwrite existing).
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to create file: %w", err)
	}

	return createStreamWriter(file, file, writerOpts)
}

// createStreamWriter creates a StreamWriter with the given configuration.
// This is the internal constructor used by the public constructors.
func createStreamWriter(w io.Writer, closer io.Closer, opts *WriterOptions) (*StreamWriter, error) {
	return &StreamWriter{
		rawWriter:   w,
		closer:      closer,
		opts:        opts,
		alloc:       memory.NewGoAllocator(),
		columns:     nil,
		columnTypes: nil,
		initialized: false,
	}, nil
}

// SetSchema sets the column names for the output stream.
// Must be called before WriteChunk if custom column names are needed.
// If not called, column names are auto-generated as "column0", "column1", etc.
func (w *StreamWriter) SetSchema(columns []string) error {
	w.columns = make([]string, len(columns))
	copy(w.columns, columns)

	return nil
}

// SetTypes sets the column types for the output stream.
// Must be called before WriteChunk to define the Arrow schema.
// If not called, types are inferred from the first DataChunk.
func (w *StreamWriter) SetTypes(types []dukdb.Type) error {
	w.columnTypes = make([]dukdb.Type, len(types))
	copy(w.columnTypes, types)

	return nil
}

// WriteChunk writes a DataChunk to the Arrow IPC stream.
// Returns nil for nil or empty chunks. Thread-safety: not safe for concurrent use.
func (w *StreamWriter) WriteChunk(chunk *storage.DataChunk) error {
	if chunk == nil || chunk.Count() == 0 {
		return nil
	}

	// Infer schema from first chunk if not explicitly set.
	if w.columns == nil {
		w.columns = generateColumnNames(chunk.ColumnCount())
	}

	if w.columnTypes == nil {
		w.columnTypes = chunk.Types()
	}

	// Initialize writer on first chunk with data.
	if !w.initialized {
		if err := w.initializeWriter(); err != nil {
			return err
		}
	}

	// Convert DataChunk to Arrow RecordBatch.
	record, err := DataChunkToRecordBatch(chunk, w.arrowSchema, w.alloc)
	if err != nil {
		return fmt.Errorf("arrow: failed to convert chunk to record batch: %w", err)
	}
	defer record.Release()

	// Write the record batch.
	if err := w.streamWriter.Write(record); err != nil {
		return fmt.Errorf("arrow: failed to write record batch: %w", err)
	}

	return nil
}

// initializeWriter initializes the Arrow stream writer with schema and options.
// Called lazily on first WriteChunk to allow schema/type configuration.
func (w *StreamWriter) initializeWriter() error {
	schema, err := w.buildArrowSchema()
	if err != nil {
		return err
	}

	w.arrowSchema = schema

	// Build IPC writer options.
	ipcOpts := []ipc.Option{
		ipc.WithSchema(schema),
		ipc.WithAllocator(w.alloc),
	}

	// Add compression if specified.
	if compOpt := w.opts.Compression.ToIPCCodec(); compOpt != nil {
		ipcOpts = append(ipcOpts, compOpt)
	}

	// Create the IPC stream writer.
	streamWriter := ipc.NewWriter(w.rawWriter, ipcOpts...)

	w.streamWriter = streamWriter
	w.initialized = true

	return nil
}

// buildArrowSchema builds an Arrow schema from column names and types.
func (w *StreamWriter) buildArrowSchema() (*arrow.Schema, error) {
	if len(w.columns) == 0 {
		return nil, errors.New("arrow: no columns defined")
	}

	if len(w.columns) != len(w.columnTypes) {
		return nil, fmt.Errorf("arrow: column count (%d) does not match type count (%d)",
			len(w.columns), len(w.columnTypes))
	}

	fields := make([]arrow.Field, len(w.columns))
	for i, colName := range w.columns {
		arrowType, err := DuckDBTypeToArrow(w.columnTypes[i])
		if err != nil {
			return nil, fmt.Errorf("arrow: column %q: %w", colName, err)
		}

		fields[i] = arrow.Field{
			Name:     colName,
			Type:     arrowType,
			Nullable: true, // DuckDB columns are nullable by default
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// Close flushes any buffered data and releases resources.
// Must be called to finalize the Arrow IPC stream.
// After Close, the writer should not be used.
func (w *StreamWriter) Close() error {
	var firstErr error

	// Close the stream writer.
	if w.streamWriter != nil {
		if err := w.streamWriter.Close(); err != nil {
			firstErr = fmt.Errorf("arrow: failed to close IPC stream writer: %w", err)
		}

		w.streamWriter = nil
	}

	// Close underlying file handle if present.
	if w.closer != nil {
		if err := w.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Verify StreamWriter implements FileWriter interface at compile time.
var _ fileio.FileWriter = (*StreamWriter)(nil)
