// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file implements the FileReader interface for Arrow IPC file format.
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

// Reader implements the FileReader interface for Arrow IPC files.
// It reads Arrow IPC data into DataChunks with type mapping from Arrow to DuckDB types.
// The reader supports lazy initialization, column projection, and chunked reading
// to efficiently process large Arrow IPC files with minimal memory usage.
type Reader struct {
	// fileReader is the Arrow IPC file reader.
	fileReader *ipc.FileReader
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
	// currentBatch is the index of the current record batch being read.
	currentBatch int
	// currentRecord is the current Arrow RecordBatch.
	currentRecord arrow.RecordBatch
	// currentRowInBatch is the current row position within the record batch.
	currentRowInBatch int64
	// rowsInCurrentBatch is the total rows in the current batch.
	rowsInCurrentBatch int64
	// totalBatches is the total number of record batches in the file.
	totalBatches int
	// eof indicates end of file has been reached.
	eof bool
}

// ReadAtSeeker combines io.Reader, io.ReaderAt, and io.Seeker interfaces
// which are required by the Arrow IPC file reader.
type ReadAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

// NewReader creates a new Arrow IPC file reader from a ReadAtSeeker.
// The reader must support io.Reader, io.ReaderAt, and io.Seeker interfaces.
// If opts is nil, default options are used.
func NewReader(r ReadAtSeeker, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	alloc := memory.NewGoAllocator()

	fileReader, err := ipc.NewFileReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open IPC file: %w", err)
	}

	return createReader(fileReader, nil, readerOpts, alloc)
}

// NewReaderFromPath creates a new Arrow IPC file reader from a file path.
func NewReaderFromPath(path string, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}
	readerOpts.applyDefaults()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arrow: failed to stat file: %w", err)
	}

	alloc := memory.NewGoAllocator()

	fileReader, err := ipc.NewFileReader(file, ipc.WithAllocator(alloc))
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("arrow: failed to open IPC file: %w", err)
	}

	_ = info // suppress unused variable warning; size is embedded in FileReader

	return createReader(fileReader, file, readerOpts, alloc)
}

// createReader creates a Reader with the given configuration.
func createReader(
	fileReader *ipc.FileReader,
	closer io.Closer,
	opts *ReaderOptions,
	alloc memory.Allocator,
) (*Reader, error) {
	return &Reader{
		fileReader:   fileReader,
		closer:       closer,
		opts:         opts,
		alloc:        alloc,
		totalBatches: fileReader.NumRecords(),
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

// ArrowSchema returns the underlying Arrow schema.
func (r *Reader) ArrowSchema() *arrow.Schema {
	return r.fileReader.Schema()
}

// NumRecordBatches returns the number of record batches in the file.
func (r *Reader) NumRecordBatches() int {
	return r.totalBatches
}

// RecordBatchAt returns the record batch at the given index.
// The caller is responsible for releasing the returned RecordBatch.
func (r *Reader) RecordBatchAt(idx int) (arrow.RecordBatch, error) {
	if idx < 0 || idx >= r.totalBatches {
		return nil, fmt.Errorf("arrow: batch index %d out of range [0, %d)", idx, r.totalBatches)
	}

	record, err := r.fileReader.Record(idx)
	if err != nil {
		return nil, fmt.Errorf("arrow: failed to read record batch %d: %w", idx, err)
	}

	record.Retain()
	return record, nil
}

// ReadChunk reads the next chunk of data from the Arrow IPC file.
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

// readRows reads rows into the chunk, handling record batch transitions.
func (r *Reader) readRows(chunk *storage.DataChunk) int {
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
func (r *Reader) readRowsFromBatch(chunk *storage.DataChunk, startIdx, maxRows int) int {
	rowsRead := 0

	for rowsRead < maxRows && r.currentRowInBatch < r.rowsInCurrentBatch {
		r.processRow(chunk, startIdx+rowsRead)
		rowsRead++
		r.currentRowInBatch++
	}

	return rowsRead
}

// processRow processes a single row and adds values to the chunk.
func (r *Reader) processRow(chunk *storage.DataChunk, rowIdx int) {
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
func (r *Reader) Close() error {
	var firstErr error

	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	if r.fileReader != nil {
		if err := r.fileReader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.fileReader = nil
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

	schema := r.fileReader.Schema()
	fields := schema.Fields()

	if len(r.opts.Columns) > 0 {
		return r.initWithProjection(fields)
	}

	return r.initAllColumns(fields)
}

// initWithProjection initializes the reader with column projection.
func (r *Reader) initWithProjection(fields []arrow.Field) error {
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
func (*Reader) findColumn(fields []arrow.Field, name string) (int, arrow.Field) {
	for i, field := range fields {
		if field.Name == name {
			return i, field
		}
	}
	return -1, arrow.Field{}
}

// initAllColumns initializes the reader to read all columns.
func (r *Reader) initAllColumns(fields []arrow.Field) error {
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
func (r *Reader) ensureRecordBatch() error {
	if r.currentRecord != nil && r.currentRowInBatch < r.rowsInCurrentBatch {
		return nil
	}

	if r.currentBatch >= r.totalBatches {
		r.eof = true
		return io.EOF
	}

	// Release previous record batch
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Read next record batch
	record, err := r.fileReader.Record(r.currentBatch)
	if err != nil {
		return fmt.Errorf("arrow: failed to read record batch %d: %w", r.currentBatch, err)
	}

	record.Retain()
	r.currentRecord = record
	r.rowsInCurrentBatch = record.NumRows()
	r.currentRowInBatch = 0

	return nil
}

// advanceToNextBatch moves to the next record batch.
func (r *Reader) advanceToNextBatch() {
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	r.currentBatch++
	r.currentRowInBatch = 0
	r.rowsInCurrentBatch = 0

	if r.currentBatch >= r.totalBatches {
		r.eof = true
	}
}

// Verify Reader implements FileReader interface.
var _ fileio.FileReader = (*Reader)(nil)
