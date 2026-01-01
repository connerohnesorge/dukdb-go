package csv

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Reader implements the FileReader interface for CSV files.
// It reads CSV data into DataChunks with type inference. The reader supports
// lazy initialization - schema detection and type inference happen on first
// read or schema access.
type Reader struct {
	// reader is the underlying Go CSV reader.
	reader *csv.Reader
	// closer handles cleanup of file handles and decompressors.
	closer io.Closer
	// opts contains user-specified and detected options.
	opts *ReaderOptions
	// columns holds column names (from header or generated).
	columns []string
	// columnTypes holds the inferred type for each column.
	columnTypes []dukdb.Type
	// initialized tracks whether schema detection has been performed.
	initialized bool
	// eof indicates end of file has been reached.
	eof bool
	// bufferedRows holds rows read during detection but not yet returned.
	bufferedRows [][]string
	// inferrer handles type inference for columns.
	inferrer *TypeInferrer
}

// NewReader creates a new CSV reader from an io.Reader.
// If opts is nil, default options are used. The delimiter is auto-detected
// if not specified in the options. Note that the io.Reader is consumed
// and should not be used after calling this function.
func NewReader(r io.Reader, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	bufReader := bufio.NewReader(r)
	delimiter := readerOpts.Delimiter

	if delimiter == 0 {
		detected, err := detectDelimiter(bufReader, readerOpts.SampleSize)
		if err != nil {
			return nil, fmt.Errorf("csv: failed to detect delimiter: %w", err)
		}

		readerOpts.Delimiter = detected
	}

	return createReader(bufReader, nil, readerOpts)
}

// NewReaderFromPath creates a new CSV reader from a file path.
// Handles compression detection and decompression automatically.
func NewReaderFromPath(path string, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("csv: failed to open file: %w", err)
	}

	compression := fileio.DetectCompressionFromPath(path)
	if compression == fileio.CompressionNone {
		var detectedReader io.Reader
		compression, detectedReader, err = fileio.DetectCompression(file)

		if err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("csv: failed to detect compression: %w", err)
		}

		return newReaderWithDecompression(detectedReader, file, compression, readerOpts)
	}

	return newReaderWithDecompression(file, file, compression, readerOpts)
}

// newReaderWithDecompression creates a reader with optional decompression.
func newReaderWithDecompression(
	r io.Reader,
	fileCloser io.Closer,
	compression fileio.Compression,
	opts *ReaderOptions,
) (*Reader, error) {
	if compression == fileio.CompressionNone {
		return createReaderFromIO(r, fileCloser, opts)
	}

	decompReader, err := fileio.NewDecompressReader(r, compression)
	if err != nil {
		if fileCloser != nil {
			_ = fileCloser.Close()
		}

		return nil, fmt.Errorf("csv: failed to create decompressor: %w", err)
	}

	closer := &combinedCloser{
		decompressor: decompReader,
		file:         fileCloser,
	}

	return createReaderFromIO(decompReader, closer, opts)
}

// createReaderFromIO creates a reader from an io.Reader with delimiter detection.
func createReaderFromIO(r io.Reader, closer io.Closer, opts *ReaderOptions) (*Reader, error) {
	opts.applyDefaults()

	bufReader := bufio.NewReader(r)
	delimiter := opts.Delimiter

	if delimiter == 0 {
		detected, err := detectDelimiter(bufReader, opts.SampleSize)
		if err != nil {
			if closer != nil {
				_ = closer.Close()
			}

			return nil, fmt.Errorf("csv: failed to detect delimiter: %w", err)
		}

		delimiter = detected
		opts.Delimiter = delimiter
	}

	return createReader(bufReader, closer, opts)
}

// createReader creates a Reader with the given configuration.
// This is the final step in reader creation after delimiter detection.
// It configures the underlying csv.Reader with the appropriate settings.
func createReader(bufReader *bufio.Reader, closer io.Closer, opts *ReaderOptions) (*Reader, error) {
	csvReader := csv.NewReader(bufReader)
	csvReader.Comma = opts.Delimiter
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.ReuseRecord = false

	if opts.Comment != 0 {
		csvReader.Comment = opts.Comment
	}

	if opts.IgnoreErrors {
		csvReader.FieldsPerRecord = -1
	}

	return &Reader{
		reader:       csvReader,
		closer:       closer,
		opts:         opts,
		columns:      nil,
		columnTypes:  nil,
		initialized:  false,
		eof:          false,
		bufferedRows: nil,
		inferrer:     NewTypeInferrer(opts.SampleSize),
	}, nil
}

// Schema returns the column names after the reader has been initialized.
// This method triggers lazy initialization if not already done, which may
// involve reading the first row to detect column names from the header.
func (r *Reader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columns, nil
}

// Types returns the inferred column types after the reader has been initialized.
// This method triggers lazy initialization if not already done.
func (r *Reader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columnTypes, nil
}

// ReadChunk reads the next chunk of data from the CSV file.
// Returns io.EOF when no more data is available.
func (r *Reader) ReadChunk() (*storage.DataChunk, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	// Check if we have buffered rows to return, even if EOF was reached during sampling.
	if r.eof && len(r.bufferedRows) == 0 {
		return nil, io.EOF
	}

	chunk := storage.NewDataChunkWithCapacity(r.columnTypes, r.opts.MaxRowsPerChunk)
	rowsRead := r.readBufferedRows(chunk)
	rowsRead = r.readMoreRows(chunk, rowsRead)

	if rowsRead == 0 {
		return nil, io.EOF
	}

	chunk.SetCount(rowsRead)

	return chunk, nil
}

// readBufferedRows consumes buffered rows from schema detection.
// These rows were read during initialization but need to be returned as data.
func (r *Reader) readBufferedRows(chunk *storage.DataChunk) int {
	rowsRead := 0
	maxRows := r.opts.MaxRowsPerChunk

	for len(r.bufferedRows) > 0 && rowsRead < maxRows {
		row := r.bufferedRows[0]
		r.bufferedRows = r.bufferedRows[1:]

		if err := r.addRowToChunk(chunk, row, rowsRead); err != nil {
			if r.opts.IgnoreErrors {
				continue
			}
		}

		rowsRead++
	}

	return rowsRead
}

// readMoreRows reads additional rows from the CSV reader.
func (r *Reader) readMoreRows(chunk *storage.DataChunk, startCount int) int {
	rowsRead := startCount
	for rowsRead < r.opts.MaxRowsPerChunk {
		record, err := r.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.eof = true
			}
			if !r.opts.IgnoreErrors {
				break
			}

			continue
		}
		if err := r.addRowToChunk(chunk, record, rowsRead); err != nil && !r.opts.IgnoreErrors {
			continue
		}
		rowsRead++
	}

	return rowsRead
}

// addRowToChunk adds a CSV row to the DataChunk at the specified index.
// It maps each field in the record to its corresponding column vector,
// converting string values to the appropriate typed values based on
// the inferred column types.
// Missing fields are treated as NULL, and values matching NullStr are NULL.
func (r *Reader) addRowToChunk(chunk *storage.DataChunk, record []string, rowIdx int) error {
	numCols := len(r.columns)

	for colIdx := range numCols {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			return fmt.Errorf("csv: nil vector at column %d", colIdx)
		}

		if colIdx >= len(record) {
			vec.Validity().SetInvalid(rowIdx)

			continue
		}

		value := record[colIdx]
		if value == r.opts.NullStr {
			vec.Validity().SetInvalid(rowIdx)

			continue
		}

		// Convert value to the appropriate type based on column type.
		colType := r.columnTypes[colIdx]
		typedValue, ok := ConvertValue(value, colType)

		if !ok {
			// Conversion failed - treat as NULL or use original string if ignoring errors.
			if r.opts.IgnoreErrors {
				vec.SetValue(rowIdx, value)
			} else {
				vec.Validity().SetInvalid(rowIdx)
			}

			continue
		}

		if typedValue == nil {
			vec.Validity().SetInvalid(rowIdx)

			continue
		}

		vec.SetValue(rowIdx, typedValue)
	}

	return nil
}

// Close releases any resources held by the reader.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}

	return nil
}

// ensureInitialized performs lazy initialization (schema detection).
func (r *Reader) ensureInitialized() error {
	if r.initialized {
		return nil
	}

	if err := r.skipRows(); err != nil {
		return err
	}

	if r.eof {
		r.initialized = true

		return nil
	}

	return r.initializeSchema()
}

// skipRows skips the configured number of initial rows.
func (r *Reader) skipRows() error {
	for range r.opts.Skip {
		_, err := r.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.eof = true

				return nil
			}

			return fmt.Errorf("csv: error skipping rows: %w", err)
		}
	}

	return nil
}

// initializeSchema reads the first row and sets up column names and types.
// It samples rows for type inference, then buffers those rows for later reading.
func (r *Reader) initializeSchema() error {
	firstRow, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.eof = true
			r.columns = make([]string, 0)
			r.columnTypes = make([]dukdb.Type, 0)
			r.initialized = true

			return nil
		}

		return fmt.Errorf("csv: error reading first row: %w", err)
	}

	// Determine column names from header or generate them.
	if r.opts.Header {
		r.columns = make([]string, len(firstRow))
		copy(r.columns, firstRow)
	} else {
		r.columns = generateColumnNames(len(firstRow))
		// First row is data, not header - include it in samples.
		r.bufferedRows = append(r.bufferedRows, firstRow)
	}

	// Sample rows for type inference.
	sampleRows := r.sampleRowsForInference()

	// Infer types from sampled rows.
	if len(sampleRows) > 0 {
		r.columnTypes = r.inferrer.InferTypes(sampleRows)
	} else {
		// No data rows available, default to VARCHAR.
		r.columnTypes = make([]dukdb.Type, len(r.columns))
		for i := range r.columnTypes {
			r.columnTypes[i] = dukdb.TYPE_VARCHAR
		}
	}

	r.initialized = true

	return nil
}

// sampleRowsForInference reads rows for type inference.
// The sampled rows are buffered for later reading as data.
func (r *Reader) sampleRowsForInference() [][]string {
	// Start with any already buffered rows (e.g., first data row when no header).
	sampleRows := make([][]string, 0, r.opts.SampleSize)
	sampleRows = append(sampleRows, r.bufferedRows...)

	// Read additional rows up to sample size.
	for len(sampleRows) < r.opts.SampleSize {
		record, err := r.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.eof = true
			}

			break
		}

		sampleRows = append(sampleRows, record)
		r.bufferedRows = append(r.bufferedRows, record)
	}

	return sampleRows
}

// Verify Reader implements FileReader interface.
var _ fileio.FileReader = (*Reader)(nil)
