// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file implements the FileWriter interface for Arrow IPC file format.
package arrow

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Writer implements the FileWriter interface for Arrow IPC files.
// It writes DataChunks to Arrow IPC file format with configurable compression
// and proper memory management. The footer is automatically written on Close().
//
// The writer uses the Arrow Go library for the underlying IPC serialization.
// It builds a schema from the column names and types provided via SetSchema
// and SetTypes, or infers them from the first DataChunk.
type Writer struct {
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
	// fileWriter is the Arrow IPC file writer.
	fileWriter *ipc.FileWriter
	// initialized tracks whether the writer has been initialized.
	initialized bool
}

// NewWriter creates a new Arrow IPC file writer to an io.Writer.
// If opts is nil, DefaultWriterOptions are used.
// The writer must be closed after use to write the footer.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	return createWriter(w, nil, writerOpts)
}

// NewWriterToPath creates a new Arrow IPC file writer to a file path.
// If opts is nil, DefaultWriterOptions are used.
// If Overwrite is false and the file exists, returns an error.
// The writer must be closed after use to finalize the file.
func NewWriterToPath(path string, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	// Check if file exists when overwrite is not specified.
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("arrow: file %q already exists", path)
	}

	// Create the output file.
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to create file: %w", err)
	}

	return createWriter(file, file, writerOpts)
}

// NewWriterToPathOverwrite creates a new Arrow IPC file writer to a file path,
// overwriting if the file already exists.
func NewWriterToPathOverwrite(path string, opts *WriterOptions) (*Writer, error) {
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

	return createWriter(file, file, writerOpts)
}

// createWriter creates a Writer with the given configuration.
// This is the internal constructor used by both NewWriter and NewWriterToPath.
func createWriter(w io.Writer, closer io.Closer, opts *WriterOptions) (*Writer, error) {
	return &Writer{
		rawWriter:   w,
		closer:      closer,
		opts:        opts,
		alloc:       memory.NewGoAllocator(),
		columns:     nil,
		columnTypes: nil,
		initialized: false,
	}, nil
}

// SetSchema sets the column names for the output file.
// Must be called before WriteChunk if custom column names are needed.
// If not called, column names are auto-generated as "column0", "column1", etc.
func (w *Writer) SetSchema(columns []string) error {
	w.columns = make([]string, len(columns))
	copy(w.columns, columns)

	return nil
}

// SetTypes sets the column types for the output file.
// Must be called before WriteChunk to define the Arrow schema.
// If not called, types are inferred from the first DataChunk.
func (w *Writer) SetTypes(types []dukdb.Type) error {
	w.columnTypes = make([]dukdb.Type, len(types))
	copy(w.columnTypes, types)

	return nil
}

// WriteChunk writes a DataChunk to the Arrow IPC file.
// Returns nil for nil chunks. Thread-safety: not safe for concurrent use.
func (w *Writer) WriteChunk(chunk *storage.DataChunk) error {
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
	if err := w.fileWriter.Write(record); err != nil {
		return fmt.Errorf("arrow: failed to write record batch: %w", err)
	}

	return nil
}

// initializeWriter initializes the Arrow file writer with schema and options.
// Called lazily on first WriteChunk to allow schema/type configuration.
func (w *Writer) initializeWriter() error {
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

	// Create the IPC file writer.
	fileWriter, err := ipc.NewFileWriter(w.rawWriter, ipcOpts...)
	if err != nil {
		return fmt.Errorf("arrow: failed to create IPC writer: %w", err)
	}

	w.fileWriter = fileWriter
	w.initialized = true

	return nil
}

// buildArrowSchema builds an Arrow schema from column names and types.
func (w *Writer) buildArrowSchema() (*arrow.Schema, error) {
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
// Must be called to finalize the Arrow IPC file with proper footer.
// After Close, the writer should not be used.
func (w *Writer) Close() error {
	var firstErr error

	// Close the file writer to write footer.
	if w.fileWriter != nil {
		if err := w.fileWriter.Close(); err != nil {
			firstErr = fmt.Errorf("arrow: failed to close IPC writer: %w", err)
		}

		w.fileWriter = nil
	}

	// Close underlying file handle if present.
	if w.closer != nil {
		if err := w.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// generateColumnNames generates column names for a given count.
func generateColumnNames(count int) []string {
	names := make([]string, count)
	for i := range names {
		names[i] = fmt.Sprintf("column%d", i)
	}
	return names
}

// RecordBatchBuilder helps build Arrow RecordBatches efficiently.
// It reuses Arrow builders across batches to reduce allocations.
type RecordBatchBuilder struct {
	alloc    memory.Allocator
	schema   *arrow.Schema
	builders []array.Builder
}

// NewRecordBatchBuilder creates a new RecordBatchBuilder for the given schema.
func NewRecordBatchBuilder(schema *arrow.Schema, alloc memory.Allocator) *RecordBatchBuilder {
	allocator := alloc
	if allocator == nil {
		allocator = memory.NewGoAllocator()
	}

	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)
	}

	return &RecordBatchBuilder{
		alloc:    allocator,
		schema:   schema,
		builders: builders,
	}
}

// Build creates a RecordBatch from the given DataChunk.
// The returned RecordBatch must be released by the caller.
func (b *RecordBatchBuilder) Build(chunk *storage.DataChunk) (arrow.RecordBatch, error) {
	if chunk == nil {
		return nil, errors.New("chunk cannot be nil")
	}

	numCols := len(b.schema.Fields())
	if chunk.ColumnCount() != numCols {
		return nil, fmt.Errorf("column count mismatch: chunk has %d, schema has %d",
			chunk.ColumnCount(), numCols)
	}

	numRows := chunk.Count()

	// Reset builders and reserve capacity.
	for _, builder := range b.builders {
		builder.Reserve(numRows)
	}

	// Append values from chunk to builders.
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		for colIdx := 0; colIdx < numCols; colIdx++ {
			val := chunk.GetValue(rowIdx, colIdx)
			if err := appendValueToBuilder(b.builders[colIdx], val, b.schema.Field(colIdx).Type); err != nil {
				return nil, fmt.Errorf("row %d, column %d: %w", rowIdx, colIdx, err)
			}
		}
	}

	// Build arrays from builders.
	arrays := make([]arrow.Array, numCols)
	for i, builder := range b.builders {
		arrays[i] = builder.NewArray()
	}

	// Create record batch.
	return array.NewRecordBatch(b.schema, arrays, int64(numRows)), nil
}

// Release releases all resources held by the builder.
func (b *RecordBatchBuilder) Release() {
	for _, builder := range b.builders {
		if builder != nil {
			builder.Release()
		}
	}
	b.builders = nil
}

// Verify Writer implements FileWriter interface at compile time.
var _ fileio.FileWriter = (*Writer)(nil)
