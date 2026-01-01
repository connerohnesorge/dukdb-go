// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file implements the FileWriter interface for Parquet format, enabling export of
// query results and DataChunks to Parquet files with configurable compression.
package parquet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// Default configuration values for the Parquet writer.
const (
	// DefaultRowGroupSize is the default number of rows per row group.
	// Row groups are the unit of parallelism in Parquet; larger groups
	// improve compression but increase memory usage during writes.
	DefaultRowGroupSize = 100000
	// DefaultCodec is the default compression codec. SNAPPY provides
	// a good balance of compression ratio and speed.
	DefaultCodec = "SNAPPY"
)

// WriterOptions contains configuration options for the Parquet writer.
// All fields have sensible defaults if left unset.
type WriterOptions struct {
	// Codec specifies the compression codec to use.
	// Supported values: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI.
	// Default is SNAPPY, which provides good compression with fast speed.
	Codec string

	// CompressionLevel specifies the compression level (for codecs that support it).
	// Only applicable for GZIP, ZSTD, and BROTLI. Higher levels provide
	// better compression at the cost of slower write speed.
	CompressionLevel int

	// RowGroupSize is the number of rows per row group.
	// Default is 100000. Larger values improve compression but use more memory.
	RowGroupSize int

	// Overwrite indicates whether to overwrite existing files.
	// If false and the file exists, NewWriterToPath returns an error.
	Overwrite bool
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
// Returns SNAPPY compression with 100k rows per row group.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		Codec:            DefaultCodec,
		CompressionLevel: 0,
		RowGroupSize:     DefaultRowGroupSize,
		Overwrite:        false,
	}
}

// applyDefaults fills in zero values with defaults.
// Called internally to ensure all options have valid values.
func (o *WriterOptions) applyDefaults() {
	if o.Codec == "" {
		o.Codec = DefaultCodec
	}

	if o.RowGroupSize <= 0 {
		o.RowGroupSize = DefaultRowGroupSize
	}
}

// Writer implements the FileWriter interface for Parquet files.
// It writes DataChunks to Parquet format with configurable compression
// and row group sizing. Rows are buffered until the row group size is
// reached, then flushed to the output.
//
// The writer uses parquet-go for the underlying Parquet serialization.
// It builds a dynamic schema based on the column names and types provided
// via SetSchema and SetTypes, or infers them from the first DataChunk.
type Writer struct {
	// rawWriter is the underlying io.Writer for output.
	rawWriter io.Writer
	// closer is used to close file handles if writing to a file.
	closer io.Closer
	// opts contains the writer configuration options.
	opts *WriterOptions
	// columns holds column names set via SetSchema.
	columns []string
	// columnTypes holds the DuckDB type for each column.
	columnTypes []dukdb.Type
	// buffer holds rows until row group size is reached.
	buffer *parquet.Buffer
	// rowCount tracks total rows written to current buffer.
	rowCount int
	// initialized tracks whether the writer has been initialized.
	initialized bool
	// schema holds the generated parquet schema.
	schema *parquet.Schema
	// parquetWriter is the underlying parquet writer.
	//nolint:staticcheck // Using deprecated Writer for dynamic schema support.
	parquetWriter *parquet.Writer
}

// NewWriter creates a new Parquet writer to an io.Writer.
// If opts is nil, DefaultWriterOptions are used.
// The writer must be closed after use to flush remaining data.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	return createParquetWriter(w, nil, writerOpts)
}

// NewWriterToPath creates a new Parquet writer to a file path.
// If opts is nil, DefaultWriterOptions are used.
// If Overwrite is false and the file exists, returns an error.
// The writer must be closed after use to finalize the file.
func NewWriterToPath(path string, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	// Check if file exists when overwrite is disabled.
	if !writerOpts.Overwrite {
		if _, err := os.Stat(path); err == nil {
			return nil, fmt.Errorf("parquet: file %q already exists (use Overwrite option)", path)
		}
	}

	// Create the output file.
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: failed to create file: %w", err)
	}

	return createParquetWriter(file, file, writerOpts)
}

// createParquetWriter creates a Writer with the given configuration.
// This is the internal constructor used by both NewWriter and NewWriterToPath.
func createParquetWriter(w io.Writer, closer io.Closer, opts *WriterOptions) (*Writer, error) {
	return &Writer{
		rawWriter:   w,
		closer:      closer,
		opts:        opts,
		columns:     nil,
		columnTypes: nil,
		buffer:      nil,
		rowCount:    0,
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
// Must be called before WriteChunk to define the Parquet schema.
// If not called, types are inferred from the first DataChunk.
func (w *Writer) SetTypes(types []dukdb.Type) error {
	w.columnTypes = make([]dukdb.Type, len(types))
	copy(w.columnTypes, types)

	return nil
}

// WriteChunk writes a DataChunk to the Parquet file.
// Rows are buffered until RowGroupSize is reached, then written as a row group.
// Returns nil for nil chunks. Thread-safety: not safe for concurrent use.
func (w *Writer) WriteChunk(chunk *storage.DataChunk) error {
	if chunk == nil {
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

	// Write each row to the buffer.
	for rowIdx := range chunk.Count() {
		row := w.chunkRowToStruct(chunk, rowIdx)

		if err := w.buffer.Write(row); err != nil {
			return fmt.Errorf("parquet: failed to write row: %w", err)
		}

		w.rowCount++

		// Flush buffer when row group size is reached.
		if w.rowCount < w.opts.RowGroupSize {
			continue
		}

		if err := w.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

// chunkRowToStruct converts a row from a DataChunk to a map.
// The map keys are column names and values are Parquet-compatible types.
func (w *Writer) chunkRowToStruct(chunk *storage.DataChunk, rowIdx int) any {
	row := make(map[string]any)

	for colIdx, colName := range w.columns {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			row[colName] = nil

			continue
		}

		// Check validity mask for NULL values.
		if !vec.Validity().IsValid(rowIdx) {
			row[colName] = nil

			continue
		}

		value := chunk.GetValue(rowIdx, colIdx)
		if value == nil {
			row[colName] = nil

			continue
		}

		// Get column type for proper conversion.
		var colType dukdb.Type
		if colIdx < len(w.columnTypes) {
			colType = w.columnTypes[colIdx]
		}

		row[colName] = convertValueToParquet(value, colType)
	}

	return row
}

// flushBuffer writes the buffered rows to the Parquet file as a row group.
// Called automatically when buffer reaches RowGroupSize, and on Close.
func (w *Writer) flushBuffer() error {
	if w.buffer == nil || w.rowCount == 0 {
		return nil
	}

	if _, err := w.parquetWriter.WriteRowGroup(w.buffer); err != nil {
		return fmt.Errorf("parquet: failed to write row group: %w", err)
	}

	w.buffer.Reset()
	w.rowCount = 0

	return nil
}

// initializeWriter initializes the parquet writer with schema and options.
// Called lazily on first WriteChunk to allow schema/type configuration.
func (w *Writer) initializeWriter() error {
	schema, err := w.buildSchema()
	if err != nil {
		return err
	}

	w.schema = schema

	codec, err := w.getCompressionCodec()
	if err != nil {
		return err
	}

	// Create the parquet writer with configured options.
	//nolint:staticcheck // Using deprecated Writer for dynamic schema support.
	w.parquetWriter = parquet.NewWriter(
		w.rawWriter,
		schema,
		parquet.Compression(codec),
	)

	// Create buffer with matching schema for row group accumulation.
	w.buffer = parquet.NewBuffer(schema)
	w.initialized = true

	return nil
}

// buildSchema builds a Parquet schema from column names and types.
// Uses reflection to create a dynamic struct type for schema generation.
func (w *Writer) buildSchema() (*parquet.Schema, error) {
	if len(w.columns) == 0 {
		return nil, errors.New("parquet: no columns defined")
	}

	// Build struct fields for each column.
	fields := make([]reflect.StructField, len(w.columns))

	for i, colName := range w.columns {
		var colType dukdb.Type
		if i < len(w.columnTypes) {
			colType = w.columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}

		// Create exported struct field with parquet tag.
		fields[i] = reflect.StructField{
			Name: toExportedName(colName),
			Type: duckDBTypeToGoType(colType),
			Tag:  reflect.StructTag(`parquet:"` + colName + `"`),
		}
	}

	// Create struct type and derive schema.
	structType := reflect.StructOf(fields)
	structPtr := reflect.New(structType)

	return parquet.SchemaOf(structPtr.Interface()), nil
}

// getCompressionCodec returns the parquet compression codec for the configured codec string.
// Returns an error for unsupported codec names.
func (w *Writer) getCompressionCodec() (compress.Codec, error) {
	switch strings.ToUpper(w.opts.Codec) {
	case "UNCOMPRESSED", "NONE":
		return &parquet.Uncompressed, nil
	case "SNAPPY":
		return &parquet.Snappy, nil
	case "GZIP":
		return &parquet.Gzip, nil
	case "ZSTD":
		return &parquet.Zstd, nil
	case "LZ4", "LZ4_RAW":
		return &parquet.Lz4Raw, nil
	case "BROTLI":
		return &parquet.Brotli, nil
	default:
		return nil, fmt.Errorf("parquet: unsupported compression codec: %s", w.opts.Codec)
	}
}

// Close flushes any buffered data and releases resources.
// Must be called to finalize the Parquet file with proper footer.
// After Close, the writer should not be used.
func (w *Writer) Close() error {
	// Flush any remaining buffered rows.
	if w.initialized && w.rowCount > 0 {
		if err := w.flushBuffer(); err != nil {
			if w.closer != nil {
				_ = w.closer.Close()
			}

			return err
		}
	}

	// Close the parquet writer to write footer.
	if w.parquetWriter != nil {
		if err := w.parquetWriter.Close(); err != nil {
			if w.closer != nil {
				_ = w.closer.Close()
			}

			return fmt.Errorf("parquet: failed to close writer: %w", err)
		}
	}

	// Close underlying file handle if present.
	if w.closer != nil {
		return w.closer.Close()
	}

	return nil
}

// Verify Writer implements FileWriter interface at compile time.
var _ fileio.FileWriter = (*Writer)(nil)
