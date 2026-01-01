// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file implements the FileWriter interface for JSON array and NDJSON formats.
package json

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Default values for writer options.
const (
	// DefaultWriterFormat is the default output format (JSON array).
	DefaultWriterFormat = FormatArray
	// DefaultIndent is the default indentation string for pretty printing.
	DefaultIndent = "  "
	// DefaultDateFormat is the default format for DATE columns.
	DefaultDateFormat = "2006-01-02"
	// DefaultTimestampWriteFormat is the default format for TIMESTAMP columns.
	DefaultTimestampWriteFormat = time.RFC3339
)

// WriterOptions contains configuration options for the JSON writer.
type WriterOptions struct {
	// Format specifies the output format: "array" for JSON array, "newline_delimited" for NDJSON.
	// Default is "array".
	Format string

	// Pretty enables pretty printing with indentation.
	// Default is false.
	Pretty bool

	// Indent is the indentation string used when Pretty is true.
	// Default is "  " (two spaces).
	Indent string

	// DateFormat is the format string for DATE columns.
	// Default is "2006-01-02" (ISO 8601 date).
	DateFormat string

	// TimestampFormat is the format string for TIMESTAMP columns.
	// Default is time.RFC3339.
	TimestampFormat string

	// Compression specifies the compression type for file output.
	Compression fileio.Compression
}

// DefaultWriterOptions returns WriterOptions with sensible defaults.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		Format:          DefaultWriterFormat,
		Pretty:          false,
		Indent:          DefaultIndent,
		DateFormat:      DefaultDateFormat,
		TimestampFormat: DefaultTimestampWriteFormat,
		Compression:     fileio.CompressionNone,
	}
}

// applyDefaults fills in zero values with defaults.
func (o *WriterOptions) applyDefaults() {
	if o.Format == "" {
		o.Format = DefaultWriterFormat
	}

	// Normalize "ndjson" to "newline_delimited".
	if o.Format == FormatNDJSONShort {
		o.Format = FormatNDJSON
	}

	if o.Indent == "" {
		o.Indent = DefaultIndent
	}

	if o.DateFormat == "" {
		o.DateFormat = DefaultDateFormat
	}

	if o.TimestampFormat == "" {
		o.TimestampFormat = DefaultTimestampWriteFormat
	}
}

// Writer implements the FileWriter interface for JSON files.
// It writes DataChunks to JSON array or NDJSON format.
type Writer struct {
	// rawWriter is the underlying io.Writer.
	rawWriter io.Writer
	// closer handles cleanup of file handles and compressors.
	closer io.Closer
	// opts contains user-specified options.
	opts *WriterOptions
	// columns holds column names (set via SetSchema).
	columns []string
	// columnTypes holds the type for each column (derived from first chunk).
	columnTypes []dukdb.Type
	// rowsWritten tracks the number of rows written (for array format comma handling).
	rowsWritten int
	// arrayStarted tracks whether the opening bracket has been written (for array format).
	arrayStarted bool
}

// NewWriter creates a new JSON writer to an io.Writer.
// If opts is nil, default options are used.
func NewWriter(w io.Writer, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	return createJSONWriter(w, nil, writerOpts)
}

// NewWriterToPath creates a new JSON writer to a file path.
// Handles compression based on options or file extension.
func NewWriterToPath(path string, opts *WriterOptions) (*Writer, error) {
	writerOpts := opts
	if writerOpts == nil {
		writerOpts = DefaultWriterOptions()
	}

	writerOpts.applyDefaults()

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("json: failed to create file: %w", err)
	}

	// Detect compression from path if not explicitly set
	compression := writerOpts.Compression
	if compression == fileio.CompressionNone {
		compression = fileio.DetectCompressionFromPath(path)
	}

	if compression == fileio.CompressionNone {
		return createJSONWriter(file, file, writerOpts)
	}

	// Wrap with compression
	compWriter, err := fileio.NewCompressWriter(file, compression)
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("json: failed to create compressor: %w", err)
	}

	closer := &combinedWriteCloser{
		compressor: compWriter,
		file:       file,
	}

	return createJSONWriter(compWriter, closer, writerOpts)
}

// createJSONWriter creates a Writer with the given configuration.
func createJSONWriter(w io.Writer, closer io.Closer, opts *WriterOptions) (*Writer, error) {
	return &Writer{
		rawWriter:    w,
		closer:       closer,
		opts:         opts,
		columns:      nil,
		columnTypes:  nil,
		rowsWritten:  0,
		arrayStarted: false,
	}, nil
}

// SetSchema sets the column names for the output file.
func (w *Writer) SetSchema(columns []string) error {
	w.columns = make([]string, len(columns))
	copy(w.columns, columns)

	return nil
}

// WriteChunk writes a DataChunk to the JSON file.
// On the first call, column types are inferred from the chunk.
func (w *Writer) WriteChunk(chunk *storage.DataChunk) error {
	if chunk == nil {
		return nil
	}

	// Infer column types from the first chunk
	if w.columnTypes == nil {
		w.columnTypes = chunk.Types()
	}

	// Generate default column names if not set via SetSchema
	if w.columns == nil {
		w.columns = generateColumnNames(chunk.ColumnCount())
	}

	// Write array start bracket on first chunk (for array format)
	if w.opts.Format == FormatArray && !w.arrayStarted {
		if err := w.writeArrayStart(); err != nil {
			return err
		}
	}

	// Write each row
	for rowIdx := range chunk.Count() {
		if err := w.writeRow(chunk, rowIdx); err != nil {
			return err
		}

		w.rowsWritten++
	}

	return nil
}

// writeArrayStart writes the opening bracket for JSON array format.
func (w *Writer) writeArrayStart() error {
	var err error

	if w.opts.Pretty {
		_, err = w.rawWriter.Write([]byte("[\n"))
	} else {
		_, err = w.rawWriter.Write([]byte("["))
	}

	if err != nil {
		return fmt.Errorf("json: failed to write array start: %w", err)
	}

	w.arrayStarted = true

	return nil
}

// writeRow writes a single row as a JSON object.
func (w *Writer) writeRow(chunk *storage.DataChunk, rowIdx int) error {
	obj := make(map[string]any)

	for colIdx, colName := range w.columns {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			obj[colName] = nil

			continue
		}

		// Check if value is NULL
		if !vec.Validity().IsValid(rowIdx) {
			obj[colName] = nil

			continue
		}

		value := chunk.GetValue(rowIdx, colIdx)
		if value == nil {
			obj[colName] = nil

			continue
		}

		// Format the value based on type
		var colType dukdb.Type
		if colIdx < len(w.columnTypes) {
			colType = w.columnTypes[colIdx]
		}

		obj[colName] = w.formatValue(value, colType)
	}

	return w.writeObject(obj)
}

// writeObject writes a JSON object to the output.
func (w *Writer) writeObject(obj map[string]any) error {
	var data []byte

	var err error

	if w.opts.Pretty {
		data, err = json.MarshalIndent(obj, w.getIndentPrefix(), w.opts.Indent)
	} else {
		data, err = json.Marshal(obj)
	}

	if err != nil {
		return fmt.Errorf("json: failed to marshal object: %w", err)
	}

	return w.writeObjectData(data)
}

// getIndentPrefix returns the prefix for indented JSON output.
func (w *Writer) getIndentPrefix() string {
	if w.opts.Format == FormatArray {
		return w.opts.Indent
	}

	return ""
}

// writeObjectData writes the marshaled JSON data with appropriate formatting.
func (w *Writer) writeObjectData(data []byte) error {
	if w.opts.Format == FormatNDJSON {
		return w.writeNDJSONObject(data)
	}

	return w.writeArrayObject(data)
}

// writeNDJSONObject writes an object in NDJSON format (one per line).
func (w *Writer) writeNDJSONObject(data []byte) error {
	// For NDJSON, each object is on its own line
	_, err := w.rawWriter.Write(data)
	if err != nil {
		return fmt.Errorf("json: failed to write object: %w", err)
	}

	_, err = w.rawWriter.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("json: failed to write newline: %w", err)
	}

	return nil
}

// writeArrayObject writes an object in JSON array format.
func (w *Writer) writeArrayObject(data []byte) error {
	// Add comma before all objects except the first
	if w.rowsWritten > 0 {
		var sep string
		if w.opts.Pretty {
			sep = ",\n"
		} else {
			sep = ","
		}

		if _, err := w.rawWriter.Write([]byte(sep)); err != nil {
			return fmt.Errorf("json: failed to write separator: %w", err)
		}
	}

	// Write the object
	if w.opts.Pretty {
		// Indent the entire object
		_, err := w.rawWriter.Write(data)
		if err != nil {
			return fmt.Errorf("json: failed to write object: %w", err)
		}
	} else {
		_, err := w.rawWriter.Write(data)
		if err != nil {
			return fmt.Errorf("json: failed to write object: %w", err)
		}
	}

	return nil
}

// formatValue converts a value to its JSON representation.
//
//nolint:exhaustive // We handle common types; others fall through to default.
func (w *Writer) formatValue(value any, typ dukdb.Type) any {
	if value == nil {
		return nil
	}

	switch typ {
	case dukdb.TYPE_BOOLEAN:
		if b, ok := value.(bool); ok {
			return b
		}

	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER:
		return formatIntegerValue(value)

	case dukdb.TYPE_BIGINT:
		if v, ok := value.(int64); ok {
			return v
		}

	case dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER:
		return formatUnsignedValue(value)

	case dukdb.TYPE_UBIGINT:
		if v, ok := value.(uint64); ok {
			return v
		}

	case dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE:
		return w.formatFloatValue(value)

	case dukdb.TYPE_DATE:
		if t, ok := value.(time.Time); ok {
			return t.Format(w.opts.DateFormat)
		}

	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ,
		dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS:
		if t, ok := value.(time.Time); ok {
			return t.Format(w.opts.TimestampFormat)
		}

	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		if t, ok := value.(time.Time); ok {
			return t.Format("15:04:05")
		}

	case dukdb.TYPE_INTERVAL:
		if interval, ok := value.(dukdb.Interval); ok {
			return formatIntervalJSON(interval)
		}

	case dukdb.TYPE_DECIMAL:
		if decimal, ok := value.(dukdb.Decimal); ok {
			return decimal.Float64()
		}

	case dukdb.TYPE_UUID:
		if uuid, ok := value.(dukdb.UUID); ok {
			return uuid.String()
		}
	}

	// Default: use the value as-is (for VARCHAR and other types)
	return value
}

// formatIntegerValue formats signed integer types.
func formatIntegerValue(value any) any {
	switch v := value.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return value
	}
}

// formatUnsignedValue formats unsigned integer types.
func formatUnsignedValue(value any) any {
	switch v := value.(type) {
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case uint:
		return uint64(v)
	default:
		return value
	}
}

// formatFloatValue formats floating point values, handling special cases.
func (w *Writer) formatFloatValue(value any) any {
	var f float64

	switch v := value.(type) {
	case float32:
		f = float64(v)
	case float64:
		f = v
	default:
		return value
	}

	// Handle special float values
	if math.IsInf(f, 1) {
		return "Infinity"
	}

	if math.IsInf(f, -1) {
		return "-Infinity"
	}

	if math.IsNaN(f) {
		return "NaN"
	}

	return f
}

// formatIntervalJSON formats an Interval as a string for JSON output.
func formatIntervalJSON(interval dukdb.Interval) string {
	var parts []string

	if interval.Months != 0 {
		years := interval.Months / 12
		months := interval.Months % 12

		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}

		if months != 0 {
			parts = append(parts, fmt.Sprintf("%d months", months))
		}
	}

	if interval.Days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", interval.Days))
	}

	if interval.Micros != 0 {
		hours := interval.Micros / (3600 * 1000000)
		remaining := interval.Micros % (3600 * 1000000)
		minutes := remaining / (60 * 1000000)
		remaining = remaining % (60 * 1000000)
		seconds := remaining / 1000000
		micros := remaining % 1000000

		if hours != 0 || minutes != 0 || seconds != 0 || micros != 0 {
			if micros != 0 {
				parts = append(
					parts,
					fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros),
				)
			} else {
				parts = append(parts, fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds))
			}
		}
	}

	if len(parts) == 0 {
		return "00:00:00"
	}

	return strings.Join(parts, " ")
}

// generateColumnNames generates default column names (column0, column1, ...).
func generateColumnNames(count int) []string {
	columns := make([]string, count)
	for i := range count {
		columns[i] = fmt.Sprintf("column%d", i)
	}

	return columns
}

// Close flushes any buffered data and releases resources.
func (w *Writer) Close() error {
	// Write closing bracket for JSON array format
	if w.opts.Format == FormatArray {
		if err := w.writeArrayEnd(); err != nil {
			if w.closer != nil {
				_ = w.closer.Close()
			}

			return err
		}
	}

	// Close underlying resources
	if w.closer != nil {
		return w.closer.Close()
	}

	return nil
}

// writeArrayEnd writes the closing bracket for JSON array format.
func (w *Writer) writeArrayEnd() error {
	// If no array was started (no data written), write empty array
	if !w.arrayStarted {
		_, err := w.rawWriter.Write([]byte("[]"))
		if err != nil {
			return fmt.Errorf("json: failed to write empty array: %w", err)
		}

		return nil
	}

	var err error

	if w.opts.Pretty {
		_, err = w.rawWriter.Write([]byte("\n]"))
	} else {
		_, err = w.rawWriter.Write([]byte("]"))
	}

	if err != nil {
		return fmt.Errorf("json: failed to write array end: %w", err)
	}

	return nil
}

// combinedWriteCloser closes both a compressor and underlying file.
type combinedWriteCloser struct {
	compressor io.WriteCloser
	file       io.Closer
}

// Close closes both the compressor and the underlying file.
// The compressor must be closed first to flush compressed data.
func (c *combinedWriteCloser) Close() error {
	var firstErr error

	if c.compressor != nil {
		if err := c.compressor.Close(); err != nil && firstErr == nil {
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

// Verify Writer implements FileWriter interface.
var _ fileio.FileWriter = (*Writer)(nil)
