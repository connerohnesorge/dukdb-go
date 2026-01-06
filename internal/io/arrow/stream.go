// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file implements the ArrowStreamReader for Arrow IPC stream format.
package arrow

import (
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

// StreamReader implements the FileReader interface for Arrow IPC streams.
// Unlike the file reader, stream readers only support sequential access
// and cannot seek to specific record batches.
type StreamReader struct {
	// streamReader is the Arrow IPC stream reader.
	streamReader *ipc.Reader
	// closer handles cleanup of file handles.
	closer io.Closer
	// opts contains user-specified options.
	opts *ReaderOptions
	// alloc is the memory allocator for Arrow operations.
	alloc memory.Allocator
	// columns holds column names to read.
	columns []string
	// columnTypes holds the DuckDB type for each column.
	columnTypes []dukdb.Type
	// columnIndices maps requested columns to their indices in the Arrow schema.
	columnIndices []int
	// initialized tracks whether schema detection has been performed.
	initialized bool
	// currentRecord is the current Arrow RecordBatch.
	currentRecord arrow.RecordBatch
	// currentRowInBatch is the current row position within the record batch.
	currentRowInBatch int64
	// rowsInCurrentBatch is the total rows in the current batch.
	rowsInCurrentBatch int64
	// eof indicates end of stream has been reached.
	eof bool
}

// NewStreamReader creates a new Arrow IPC stream reader from an io.Reader.
// If opts is nil, default options are used.
func NewStreamReader(r io.Reader, opts *ReaderOptions) (*StreamReader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	alloc := memory.NewGoAllocator()

	streamReader, err := ipc.NewReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open IPC stream: %w", err)
	}

	return createStreamReader(streamReader, nil, readerOpts, alloc)
}

// NewStreamReaderFromPath creates a new Arrow IPC stream reader from a file path.
func NewStreamReaderFromPath(path string, opts *ReaderOptions) (*StreamReader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open file: %w", err)
	}

	alloc := memory.NewGoAllocator()

	streamReader, err := ipc.NewReader(file, ipc.WithAllocator(alloc))
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arrow: failed to open IPC stream: %w", err)
	}

	return createStreamReader(streamReader, file, readerOpts, alloc)
}

// createStreamReader creates a StreamReader with the given configuration.
func createStreamReader(
	streamReader *ipc.Reader,
	closer io.Closer,
	opts *ReaderOptions,
	alloc memory.Allocator,
) (*StreamReader, error) {
	return &StreamReader{
		streamReader: streamReader,
		closer:       closer,
		opts:         opts,
		alloc:        alloc,
	}, nil
}

// Schema returns the column names after the reader has been initialized.
func (r *StreamReader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}
	return r.columns, nil
}

// Types returns the column types after the reader has been initialized.
func (r *StreamReader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}
	return r.columnTypes, nil
}

// ArrowSchema returns the underlying Arrow schema.
func (r *StreamReader) ArrowSchema() *arrow.Schema {
	return r.streamReader.Schema()
}

// Next advances to the next record batch.
// Returns true if a record is available, false otherwise.
func (r *StreamReader) Next() bool {
	if r.eof {
		return false
	}

	// Release previous record
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Read next record batch
	if !r.streamReader.Next() {
		r.eof = true
		return false
	}

	record := r.streamReader.Record()
	if record == nil {
		r.eof = true
		return false
	}

	record.Retain()
	r.currentRecord = record
	r.rowsInCurrentBatch = record.NumRows()
	r.currentRowInBatch = 0

	return true
}

// Record returns the current record batch.
// Returns nil if Next() has not been called or returned false.
// The caller should NOT release this record; it is managed by the StreamReader.
func (r *StreamReader) Record() arrow.RecordBatch {
	return r.currentRecord
}

// Err returns any error that occurred during iteration.
func (r *StreamReader) Err() error {
	return r.streamReader.Err()
}

// ReadChunk reads the next chunk of data from the Arrow IPC stream.
// Returns io.EOF when no more data is available.
func (r *StreamReader) ReadChunk() (*storage.DataChunk, error) {
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

// readRows reads rows into the chunk, handling record batch transitions.
func (r *StreamReader) readRows(chunk *storage.DataChunk) int {
	rowsRead := 0

	for rowsRead < r.opts.MaxRowsPerChunk && !r.eof {
		if err := r.ensureRecordBatch(); err != nil {
			if err == io.EOF {
				break
			}
			return rowsRead
		}

		n := r.readRowsFromBatch(chunk, rowsRead, r.opts.MaxRowsPerChunk-rowsRead)
		rowsRead += n

		if r.currentRowInBatch >= r.rowsInCurrentBatch {
			r.advanceToNextBatch()
		}
	}

	return rowsRead
}

// readRowsFromBatch reads rows from the current record batch into the chunk.
func (r *StreamReader) readRowsFromBatch(chunk *storage.DataChunk, startIdx, maxRows int) int {
	rowsRead := 0

	for rowsRead < maxRows && r.currentRowInBatch < r.rowsInCurrentBatch {
		r.processRow(chunk, startIdx+rowsRead)
		rowsRead++
		r.currentRowInBatch++
	}

	return rowsRead
}

// processRow processes a single row and adds values to the chunk.
func (r *StreamReader) processRow(chunk *storage.DataChunk, rowIdx int) {
	for colIdx, schemaIdx := range r.columnIndices {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			continue
		}

		arr := r.currentRecord.Column(schemaIdx)
		if arr.IsNull(int(r.currentRowInBatch)) {
			vec.Validity().SetInvalid(rowIdx)
		} else {
			val := extractArrowValue(arr, int(r.currentRowInBatch))
			vec.SetValue(rowIdx, val)
		}
	}
}

// Close releases any resources held by the reader.
func (r *StreamReader) Close() error {
	var firstErr error

	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	if r.streamReader != nil {
		r.streamReader.Release()
		r.streamReader = nil
	}

	if r.closer != nil {
		if err := r.closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// ensureInitialized performs lazy initialization (schema detection).
func (r *StreamReader) ensureInitialized() error {
	if r.initialized {
		return nil
	}

	schema := r.streamReader.Schema()
	fields := schema.Fields()

	if len(r.opts.Columns) > 0 {
		return r.initWithProjection(fields)
	}

	return r.initAllColumns(fields)
}

// initWithProjection initializes the reader with column projection.
func (r *StreamReader) initWithProjection(fields []arrow.Field) error {
	r.columns = make([]string, 0, len(r.opts.Columns))
	r.columnIndices = make([]int, 0, len(r.opts.Columns))
	r.columnTypes = make([]dukdb.Type, 0, len(r.opts.Columns))

	for _, colName := range r.opts.Columns {
		idx, field := r.findColumn(fields, colName)
		if idx < 0 {
			return fmt.Errorf("arrow: column %q not found in schema", colName)
		}

		duckType, err := ArrowTypeToDuckDB(field.Type)
		if err != nil {
			return fmt.Errorf("arrow: column %q: %w", colName, err)
		}

		r.columns = append(r.columns, colName)
		r.columnIndices = append(r.columnIndices, idx)
		r.columnTypes = append(r.columnTypes, duckType)
	}

	r.initialized = true
	return nil
}

// findColumn finds a column by name in the field list.
// Returns the index and field, or -1 and empty field if not found.
func (*StreamReader) findColumn(fields []arrow.Field, name string) (int, arrow.Field) {
	for i, field := range fields {
		if field.Name == name {
			return i, field
		}
	}
	return -1, arrow.Field{}
}

// initAllColumns initializes the reader to read all columns.
func (r *StreamReader) initAllColumns(fields []arrow.Field) error {
	r.columns = make([]string, len(fields))
	r.columnIndices = make([]int, len(fields))
	r.columnTypes = make([]dukdb.Type, len(fields))

	for i, field := range fields {
		duckType, err := ArrowTypeToDuckDB(field.Type)
		if err != nil {
			return fmt.Errorf("arrow: column %q: %w", field.Name, err)
		}

		r.columns[i] = field.Name
		r.columnIndices[i] = i
		r.columnTypes[i] = duckType
	}

	r.initialized = true
	return nil
}

// ensureRecordBatch ensures we have a valid record batch for the current position.
func (r *StreamReader) ensureRecordBatch() error {
	if r.currentRecord != nil && r.currentRowInBatch < r.rowsInCurrentBatch {
		return nil
	}

	if r.eof {
		return io.EOF
	}

	// Try to read next batch
	if !r.Next() {
		return io.EOF
	}

	return nil
}

// advanceToNextBatch prepares for moving to the next record batch.
func (r *StreamReader) advanceToNextBatch() {
	// The actual advancement happens in ensureRecordBatch -> Next()
	// Here we just reset position tracking
	r.currentRowInBatch = r.rowsInCurrentBatch // Mark current batch as exhausted
}

// Verify StreamReader implements FileReader interface.
var _ fileio.FileReader = (*StreamReader)(nil)
