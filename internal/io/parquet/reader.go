// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file implements the FileReader interface for Parquet format.
package parquet

import (
	"errors"
	"fmt"
	"io"
	"os"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/parquet-go/parquet-go"
)

// ReaderAtSeeker combines io.ReaderAt and io.Seeker interfaces
// which are required by parquet-go for reading Parquet files.
type ReaderAtSeeker interface {
	io.ReaderAt
	io.Seeker
}

// Reader implements the FileReader interface for Parquet files.
// It reads Parquet data into DataChunks with type mapping from Parquet to DuckDB types.
// The reader supports lazy initialization, column projection, and chunked reading
// to efficiently process large Parquet files with minimal memory usage.
type Reader struct {
	// file is the underlying parquet file reader.
	file *parquet.File
	// closer handles cleanup of file handles.
	closer io.Closer
	// opts contains user-specified options.
	opts *ReaderOptions
	// columns holds column names to read.
	columns []string
	// columnTypes holds the DuckDB type for each column.
	columnTypes []dukdb.Type
	// columnIndices maps column names to their indices in the Parquet file.
	columnIndices []int
	// initialized tracks whether schema detection has been performed.
	initialized bool
	// currentRowGroup is the index of the current row group being read.
	currentRowGroup int
	// currentRowInGroup is the current row position within the row group.
	currentRowInGroup int64
	// rowGroupReader is the current row group reader.
	rowGroupReader parquet.RowGroup
	// rows is the row reader for the current row group.
	rows parquet.Rows
	// totalRowGroups is the total number of row groups in the file.
	totalRowGroups int
	// eof indicates end of file has been reached.
	eof bool
}

// NewReader creates a new Parquet reader from a ReaderAtSeeker.
// The reader must support both io.ReaderAt and io.Seeker interfaces.
// If opts is nil, default options are used.
func NewReader(r ReaderAtSeeker, size int64, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	pf, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("parquet: failed to open file: %w", err)
	}

	return createReader(pf, nil, readerOpts)
}

// NewReaderFromPath creates a new Parquet reader from a file path.
func NewReaderFromPath(path string, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet: failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("parquet: failed to stat file: %w", err)
	}

	pf, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("parquet: failed to open parquet file: %w", err)
	}

	return createReader(pf, file, readerOpts)
}

// createReader creates a Reader with the given configuration.
func createReader(pf *parquet.File, closer io.Closer, opts *ReaderOptions) (*Reader, error) {
	return &Reader{
		file:           pf,
		closer:         closer,
		opts:           opts,
		totalRowGroups: len(pf.RowGroups()),
	}, nil
}

// Schema returns the column names after the reader has been initialized.
func (r *Reader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columns, nil
}

// Types returns the column types after the reader has been initialized.
func (r *Reader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columnTypes, nil
}

// ReadChunk reads the next chunk of data from the Parquet file.
// Returns io.EOF when no more data is available.
func (r *Reader) ReadChunk() (*storage.DataChunk, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	if r.eof {
		return nil, io.EOF
	}

	chunk := storage.NewDataChunkWithCapacity(r.columnTypes, r.opts.MaxRowsPerChunk)
	rowsRead := r.readRows(chunk)

	if rowsRead == 0 {
		return nil, io.EOF
	}

	chunk.SetCount(rowsRead)

	return chunk, nil
}

// readRows reads rows into the chunk, handling row group transitions.
func (r *Reader) readRows(chunk *storage.DataChunk) int {
	rowsRead := 0

	for rowsRead < r.opts.MaxRowsPerChunk && !r.eof {
		if err := r.ensureRowReader(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return rowsRead
		}

		n, err := r.readRowsIntoChunk(chunk, rowsRead, r.opts.MaxRowsPerChunk-rowsRead)
		rowsRead += n

		if err != nil && errors.Is(err, io.EOF) {
			r.advanceToNextRowGroup()
		}
	}

	return rowsRead
}

// Close releases any resources held by the reader.
func (r *Reader) Close() error {
	var firstErr error

	if r.rows != nil {
		if err := r.rows.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if r.closer != nil {
		if err := r.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// ensureInitialized performs lazy initialization (schema detection).
func (r *Reader) ensureInitialized() error {
	if r.initialized {
		return nil
	}

	schema := r.file.Schema()
	fields := schema.Fields()

	if len(r.opts.Columns) > 0 {
		return r.initWithProjection(fields)
	}

	return r.initAllColumns(fields)
}

// initWithProjection initializes the reader with column projection.
func (r *Reader) initWithProjection(fields []parquet.Field) error {
	r.columns = make([]string, 0, len(r.opts.Columns))
	r.columnIndices = make([]int, 0, len(r.opts.Columns))
	r.columnTypes = make([]dukdb.Type, 0, len(r.opts.Columns))

	for _, colName := range r.opts.Columns {
		idx, field := r.findColumn(fields, colName)
		if idx < 0 {
			return fmt.Errorf("parquet: column %q not found in schema", colName)
		}

		r.columns = append(r.columns, colName)
		r.columnIndices = append(r.columnIndices, idx)
		r.columnTypes = append(r.columnTypes, parquetTypeToDuckDB(field))
	}

	r.initialized = true

	return nil
}

// findColumn finds a column by name in the field list.
// Returns the index and field, or -1 and nil if not found.
func (*Reader) findColumn(fields []parquet.Field, name string) (int, parquet.Field) {
	for i, field := range fields {
		if field.Name() == name {
			return i, field
		}
	}

	return -1, nil
}

// initAllColumns initializes the reader to read all columns.
func (r *Reader) initAllColumns(fields []parquet.Field) error {
	r.columns = make([]string, len(fields))
	r.columnIndices = make([]int, len(fields))
	r.columnTypes = make([]dukdb.Type, len(fields))

	for i, field := range fields {
		r.columns[i] = field.Name()
		r.columnIndices[i] = i
		r.columnTypes[i] = parquetTypeToDuckDB(field)
	}

	r.initialized = true

	return nil
}

// ensureRowReader ensures we have a valid row reader for the current row group.
func (r *Reader) ensureRowReader() error {
	if r.rows != nil {
		return nil
	}

	if r.currentRowGroup >= r.totalRowGroups {
		r.eof = true

		return io.EOF
	}

	rowGroups := r.file.RowGroups()
	r.rowGroupReader = rowGroups[r.currentRowGroup]
	r.rows = r.rowGroupReader.Rows()
	r.currentRowInGroup = 0

	return nil
}

// advanceToNextRowGroup moves to the next row group.
func (r *Reader) advanceToNextRowGroup() {
	if r.rows != nil {
		_ = r.rows.Close()
		r.rows = nil
	}

	r.rowGroupReader = nil
	r.currentRowGroup++
	r.currentRowInGroup = 0

	if r.currentRowGroup >= r.totalRowGroups {
		r.eof = true
	}
}

// readRowsIntoChunk reads rows from the current row reader into the chunk.
func (r *Reader) readRowsIntoChunk(chunk *storage.DataChunk, startIdx, maxRows int) (int, error) {
	schema := r.file.Schema()
	rowBuf := make([]parquet.Row, 1)
	rowsRead := 0

	for rowsRead < maxRows {
		n, err := r.rows.ReadRows(rowBuf)
		if n > 0 {
			r.processRow(chunk, rowBuf[0], startIdx+rowsRead, schema)
			rowsRead++
		}

		if err != nil {
			return rowsRead, err
		}

		if n == 0 {
			return rowsRead, io.EOF
		}
	}

	return rowsRead, nil
}

// processRow processes a single row and adds values to the chunk.
func (r *Reader) processRow(
	chunk *storage.DataChunk,
	row parquet.Row,
	rowIdx int,
	schema *parquet.Schema,
) {
	fields := schema.Fields()

	for colIdx, schemaIdx := range r.columnIndices {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			continue
		}

		val := r.getColumnValue(row, schemaIdx, fields)
		if val == nil {
			vec.Validity().SetInvalid(rowIdx)
		} else {
			vec.SetValue(rowIdx, val)
		}
	}
}

// getColumnValue extracts and converts a column value from a Parquet row.
// Searches the row for the value at the specified column index and converts it.
func (*Reader) getColumnValue(row parquet.Row, colIdx int, fields []parquet.Field) any {
	if colIdx >= len(fields) {
		return nil
	}

	for _, v := range row {
		if v.Column() == colIdx {
			return convertParquetValue(v, fields[colIdx])
		}
	}

	return nil
}

// Verify Reader implements FileReader interface.
var _ fileio.FileReader = (*Reader)(nil)
