// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file implements the FileReader interface for JSON array and NDJSON formats.
package json

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Reader implements the FileReader interface for JSON files.
// It reads JSON array or NDJSON data into DataChunks with type inference.
// The reader supports lazy initialization - schema detection and type inference
// happen on first read or schema access.
type Reader struct {
	// decoder is the JSON streaming decoder (used for JSON array format).
	decoder *json.Decoder
	// scanner is the line scanner (used for NDJSON format).
	scanner *bufio.Scanner
	// bufReader is the underlying buffered reader.
	bufReader *bufio.Reader
	// closer handles cleanup of file handles and decompressors.
	closer io.Closer
	// opts contains user-specified and detected options.
	opts *ReaderOptions
	// columns holds column names detected from JSON keys.
	columns []string
	// columnTypes holds the inferred type for each column.
	columnTypes []dukdb.Type
	// columnIndex maps column name to index for fast lookup.
	columnIndex map[string]int
	// initialized tracks whether schema detection has been performed.
	initialized bool
	// eof indicates end of data has been reached.
	eof bool
	// inArray tracks whether we're inside the JSON array (for array format).
	inArray bool
	// bufferedObjects holds objects read during detection but not yet returned.
	bufferedObjects []map[string]any
	// inferrer handles type inference for columns.
	inferrer *TypeInferrer
}

// NewReader creates a new JSON reader from an io.Reader.
// If opts is nil, default options are used. The reader is consumed
// and should not be used after calling this function.
func NewReader(r io.Reader, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	// Wrap in buffered reader for efficiency.
	bufReader := bufio.NewReader(r)

	return createReader(bufReader, nil, readerOpts)
}

// NewReaderFromPath creates a new JSON reader from a file path.
// Handles compression detection and decompression automatically.
func NewReaderFromPath(path string, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("json: failed to open file: %w", err)
	}

	compression := fileio.DetectCompressionFromPath(path)
	if compression == fileio.CompressionNone {
		var detectedReader io.Reader
		compression, detectedReader, err = fileio.DetectCompression(file)

		if err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("json: failed to detect compression: %w", err)
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
		bufReader := bufio.NewReader(r)

		return createReader(bufReader, fileCloser, opts)
	}

	decompReader, err := fileio.NewDecompressReader(r, compression)
	if err != nil {
		if fileCloser != nil {
			_ = fileCloser.Close()
		}

		return nil, fmt.Errorf("json: failed to create decompressor: %w", err)
	}

	closer := &combinedCloser{
		decompressor: decompReader,
		file:         fileCloser,
	}

	bufReader := bufio.NewReader(decompReader)

	return createReader(bufReader, closer, opts)
}

// createReader creates a Reader with the given configuration.
func createReader(bufReader *bufio.Reader, closer io.Closer, opts *ReaderOptions) (*Reader, error) {
	return &Reader{
		decoder:         nil, // will be created during initialization if needed
		scanner:         nil, // will be created during initialization if needed
		bufReader:       bufReader,
		closer:          closer,
		opts:            opts,
		columns:         nil,
		columnTypes:     nil,
		columnIndex:     nil,
		initialized:     false,
		eof:             false,
		inArray:         false,
		bufferedObjects: nil,
		inferrer:        NewTypeInferrer(opts.SampleSize, opts.DateFormat, opts.TimestampFormat),
	}, nil
}

// Schema returns the column names after the reader has been initialized.
// This method triggers lazy initialization if not already done.
func (r *Reader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columns, nil
}

// Types returns the inferred column types after the reader has been initialized.
func (r *Reader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columnTypes, nil
}

// ReadChunk reads the next chunk of data from the JSON file.
// Returns io.EOF when no more data is available.
func (r *Reader) ReadChunk() (*storage.DataChunk, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	// Check if we have buffered objects to return, even if EOF was reached during sampling.
	if r.eof && len(r.bufferedObjects) == 0 {
		return nil, io.EOF
	}

	chunk := storage.NewDataChunkWithCapacity(r.columnTypes, r.opts.MaxRowsPerChunk)
	rowsRead := r.readBufferedObjects(chunk)
	rowsRead = r.readMoreObjects(chunk, rowsRead)

	if rowsRead == 0 {
		return nil, io.EOF
	}

	chunk.SetCount(rowsRead)

	return chunk, nil
}

// readBufferedObjects consumes buffered objects from schema detection.
func (r *Reader) readBufferedObjects(chunk *storage.DataChunk) int {
	rowsRead := 0
	maxRows := r.opts.MaxRowsPerChunk

	for len(r.bufferedObjects) > 0 && rowsRead < maxRows {
		obj := r.bufferedObjects[0]
		r.bufferedObjects = r.bufferedObjects[1:]

		if err := r.addObjectToChunk(chunk, obj, rowsRead); err != nil {
			if r.opts.IgnoreErrors {
				continue
			}
		}

		rowsRead++
	}

	return rowsRead
}

// readMoreObjects reads additional objects from the JSON source.
func (r *Reader) readMoreObjects(chunk *storage.DataChunk, startCount int) int {
	rowsRead := startCount
	for rowsRead < r.opts.MaxRowsPerChunk && !r.eof {
		obj, err := r.readNextObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.eof = true
			}

			if !r.opts.IgnoreErrors {
				break
			}

			continue
		}

		if obj == nil {
			r.eof = true

			break
		}

		if err := r.addObjectToChunk(chunk, obj, rowsRead); err != nil && !r.opts.IgnoreErrors {
			continue
		}

		rowsRead++
	}

	return rowsRead
}

// readNextObject reads the next JSON object.
// For array format, reads from the JSON decoder.
// For NDJSON format, reads the next line and parses it.
func (r *Reader) readNextObject() (map[string]any, error) {
	if r.opts.Format == FormatNDJSON {
		return r.readNextNDJSONObject()
	}

	return r.readNextArrayObject()
}

// readNextArrayObject reads the next JSON object from a JSON array.
func (r *Reader) readNextArrayObject() (map[string]any, error) {
	// Read next token.
	if !r.decoder.More() {
		// End of array.
		return nil, nil
	}

	var obj map[string]any
	if err := r.decoder.Decode(&obj); err != nil {
		return nil, fmt.Errorf("json: failed to decode object: %w", err)
	}

	return obj, nil
}


// addObjectToChunk adds a JSON object to the DataChunk at the specified index.
func (r *Reader) addObjectToChunk(chunk *storage.DataChunk, obj map[string]any, rowIdx int) error {
	for colIdx, colName := range r.columns {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			return fmt.Errorf("json: nil vector at column %d", colIdx)
		}

		value, exists := obj[colName]
		if !exists || value == nil {
			vec.Validity().SetInvalid(rowIdx)

			continue
		}

		// Convert value to the appropriate type.
		colType := r.columnTypes[colIdx]
		typedValue, ok := convertValue(value, colType, r.opts.DateFormat, r.opts.TimestampFormat)

		if !ok {
			if r.opts.IgnoreErrors {
				// Try to convert to string as fallback.
				strVal, _ := convertToVarchar(value)
				vec.SetValue(rowIdx, strVal)
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

// ensureInitialized performs lazy initialization (schema detection and format detection).
func (r *Reader) ensureInitialized() error {
	if r.initialized {
		return nil
	}

	// Handle format auto-detection if needed.
	if r.opts.Format == FormatAuto {
		detectedFormat, err := r.detectFormat()
		if err != nil {
			return err
		}

		r.opts.Format = detectedFormat
	}

	// Initialize based on format.
	if r.opts.Format == FormatArray {
		if err := r.initArrayFormat(); err != nil {
			return err
		}
	} else {
		r.initNDJSONFormat()
	}

	return r.initializeSchema()
}



// initializeSchema reads sample objects and sets up column names and types.
func (r *Reader) initializeSchema() error {
	// Sample objects for schema and type inference.
	sampleObjects := r.sampleObjectsForInference()

	if len(sampleObjects) == 0 {
		// No data - empty schema.
		r.columns = make([]string, 0)
		r.columnTypes = make([]dukdb.Type, 0)
		r.columnIndex = make(map[string]int)
		r.initialized = true

		return nil
	}

	// Extract column names from all sampled objects.
	columnSet := make(map[string]struct{})
	for _, obj := range sampleObjects {
		for key := range obj {
			columnSet[key] = struct{}{}
		}
	}

	// Sort columns for consistent ordering.
	r.columns = make([]string, 0, len(columnSet))
	for col := range columnSet {
		r.columns = append(r.columns, col)
	}

	sort.Strings(r.columns)

	// Build column index.
	r.columnIndex = make(map[string]int)
	for i, col := range r.columns {
		r.columnIndex[col] = i
	}

	// Infer types from sampled objects.
	r.columnTypes = r.inferrer.InferTypes(r.columns, sampleObjects)

	r.initialized = true

	return nil
}

// sampleObjectsForInference reads objects for type inference.
// The sampled objects are buffered for later reading as data.
func (r *Reader) sampleObjectsForInference() []map[string]any {
	sampleObjects := make([]map[string]any, 0, r.opts.SampleSize)

	for len(sampleObjects) < r.opts.SampleSize && !r.eof {
		obj, err := r.readNextObject()
		if err != nil {
			r.handleSampleError(err)

			break
		}

		if obj == nil {
			r.eof = true

			break
		}

		sampleObjects = append(sampleObjects, obj)
		r.bufferedObjects = append(r.bufferedObjects, obj)
	}

	return sampleObjects
}

// handleSampleError processes errors encountered during sampling.
func (r *Reader) handleSampleError(err error) {
	if errors.Is(err, io.EOF) {
		r.eof = true

		return
	}

	// On non-EOF error with IgnoreErrors=false, stop reading entirely.
	if !r.opts.IgnoreErrors {
		r.eof = true
	}
}

// Verify Reader implements FileReader interface.
var _ fileio.FileReader = (*Reader)(nil)
