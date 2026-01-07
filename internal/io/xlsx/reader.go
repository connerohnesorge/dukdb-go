// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file implements the FileReader interface for XLSX format.
package xlsx

import (
	"bytes"
	"fmt"
	"io"
	"os"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/xuri/excelize/v2"
)

// Reader implements the FileReader interface for XLSX files.
// It reads Excel XLSX data into DataChunks. The reader supports
// lazy initialization - schema detection happens on first read or schema access.
// It uses streaming row iteration for memory efficiency on large files.
type Reader struct {
	// file is the underlying excelize file handle.
	file *excelize.File
	// closer handles cleanup of temporary files created by excelize.
	closer io.Closer
	// opts contains user-specified options.
	opts *ReaderOptions
	// columns holds column names (from header or generated).
	columns []string
	// columnTypes holds the type for each column (currently VARCHAR for all).
	columnTypes []dukdb.Type
	// rows is the streaming row iterator.
	rows *excelize.Rows
	// sheet is the name of the sheet being read.
	sheet string
	// currentRow is the 1-based current row position.
	currentRow int
	// initialized tracks whether schema detection has been performed.
	initialized bool
	// eof indicates end of file has been reached.
	eof bool
	// startCol is the 0-based starting column index for range filtering.
	startCol int
	// endCol is the 0-based ending column index for range filtering (-1 means no limit).
	endCol int
	// startRow is the 1-based starting row for range filtering.
	startRow int
	// endRow is the 1-based ending row for range filtering (0 means no limit).
	endRow int
}

// NewReader creates a new XLSX reader from an io.Reader.
// If opts is nil, default options are used.
// Note: excelize requires the entire file to be read into memory
// for random access, but we use streaming row iteration for efficient reading.
func NewReader(r io.Reader, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	// Read all data into a buffer since excelize needs random access
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("xlsx: failed to read data: %w", err)
	}

	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("xlsx: failed to open xlsx: %w", err)
	}

	return createReader(f, nil, readerOpts)
}

// NewReaderFromPath creates a new XLSX reader from a file path.
func NewReaderFromPath(path string, opts *ReaderOptions) (*Reader, error) {
	readerOpts := opts
	if readerOpts == nil {
		readerOpts = DefaultReaderOptions()
	}

	readerOpts.applyDefaults()

	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, fmt.Errorf("xlsx: failed to open file %q: %w", path, err)
	}

	// Create a file closer that will be used when the reader is closed
	file, err := os.Open(path)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("xlsx: failed to open file for closing: %w", err)
	}
	// We don't actually need the file handle, just close it
	_ = file.Close()

	return createReader(f, nil, readerOpts)
}

// createReader creates a Reader with the given configuration.
func createReader(f *excelize.File, closer io.Closer, opts *ReaderOptions) (*Reader, error) {
	reader := &Reader{
		file:   f,
		closer: closer,
		opts:   opts,
		endCol: -1, // No limit by default
	}

	// Determine which sheet to read
	sheet, err := reader.determineSheet()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	reader.sheet = sheet

	// Parse range constraints if specified
	if err := reader.parseRangeConstraints(); err != nil {
		_ = f.Close()
		return nil, err
	}

	return reader, nil
}

// determineSheet determines which sheet to read based on options.
func (r *Reader) determineSheet() (string, error) {
	sheets := r.file.GetSheetList()
	if len(sheets) == 0 {
		return "", fmt.Errorf("xlsx: workbook contains no sheets")
	}

	// If a sheet name is specified, use it
	if r.opts.Sheet != "" {
		for _, s := range sheets {
			if s == r.opts.Sheet {
				return s, nil
			}
		}
		return "", fmt.Errorf("xlsx: sheet %q not found", r.opts.Sheet)
	}

	// If a sheet index is specified, use it
	if r.opts.SheetIndex >= 0 {
		if r.opts.SheetIndex >= len(sheets) {
			return "", fmt.Errorf("xlsx: sheet index %d out of range (0-%d)",
				r.opts.SheetIndex, len(sheets)-1)
		}
		return sheets[r.opts.SheetIndex], nil
	}

	// Default to first sheet
	return sheets[0], nil
}

// parseRangeConstraints parses range options into internal state.
func (r *Reader) parseRangeConstraints() error {
	// Parse A1 notation range if specified
	if r.opts.Range != "" {
		startCol, startRow, endCol, endRow, err := ParseRange(r.opts.Range)
		if err != nil {
			return fmt.Errorf("xlsx: invalid range: %w", err)
		}
		r.startCol = startCol
		r.startRow = startRow
		r.endCol = endCol
		r.endRow = endRow
		return nil
	}

	// Parse individual column constraints
	if r.opts.StartCol != "" {
		idx := ColumnLettersToIndex(r.opts.StartCol)
		if idx < 0 {
			return fmt.Errorf("xlsx: invalid start column %q", r.opts.StartCol)
		}
		r.startCol = idx
	}

	if r.opts.EndCol != "" {
		idx := ColumnLettersToIndex(r.opts.EndCol)
		if idx < 0 {
			return fmt.Errorf("xlsx: invalid end column %q", r.opts.EndCol)
		}
		r.endCol = idx
	}

	// Parse row constraints
	if r.opts.StartRow > 0 {
		r.startRow = r.opts.StartRow
	}
	if r.opts.EndRow > 0 {
		r.endRow = r.opts.EndRow
	}

	return nil
}

// Schema returns the column names after the reader has been initialized.
// This method triggers lazy initialization if not already done.
func (r *Reader) Schema() ([]string, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columns, nil
}

// Types returns the column types after the reader has been initialized.
// Types are inferred from cell data when opts.InferTypes is true.
func (r *Reader) Types() ([]dukdb.Type, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.columnTypes, nil
}

// ReadChunk reads the next chunk of data from the XLSX file.
// Returns io.EOF when no more data is available.
func (r *Reader) ReadChunk() (*storage.DataChunk, error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	if r.eof {
		return nil, io.EOF
	}

	chunk := storage.NewDataChunkWithCapacity(r.columnTypes, r.opts.ChunkSize)
	rowsRead := 0

	for rowsRead < r.opts.ChunkSize && !r.eof {
		if !r.rows.Next() {
			r.eof = true
			break
		}

		r.currentRow++

		// Skip rows before startRow
		if r.startRow > 0 && r.currentRow < r.startRow {
			continue
		}

		// Stop if we've passed endRow
		if r.endRow > 0 && r.currentRow > r.endRow {
			r.eof = true
			break
		}

		row, err := r.rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("xlsx: failed to read row %d: %w", r.currentRow, err)
		}

		// Filter columns based on range constraints
		filteredRow := r.filterColumns(row)

		// Add row to chunk
		r.addRowToChunk(chunk, filteredRow, rowsRead)
		rowsRead++
	}

	if rowsRead == 0 {
		return nil, io.EOF
	}

	chunk.SetCount(rowsRead)
	return chunk, nil
}

// filterColumns filters the row based on column constraints.
func (r *Reader) filterColumns(row []string) []string {
	if r.startCol == 0 && r.endCol < 0 {
		// No column filtering needed
		return row
	}

	endIdx := len(row) - 1
	if r.endCol >= 0 && r.endCol < endIdx {
		endIdx = r.endCol
	}

	startIdx := r.startCol
	if startIdx > endIdx {
		return nil
	}

	// Ensure we don't go past the row length
	if startIdx >= len(row) {
		return nil
	}
	if endIdx >= len(row) {
		endIdx = len(row) - 1
	}

	return row[startIdx : endIdx+1]
}

// addRowToChunk adds a row to the DataChunk at the specified index.
func (r *Reader) addRowToChunk(chunk *storage.DataChunk, row []string, rowIdx int) {
	numCols := len(r.columns)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		vec := chunk.GetVector(colIdx)
		if vec == nil {
			continue
		}

		if colIdx >= len(row) {
			// Column beyond row data - treat as empty cell
			if r.opts.EmptyAsNull {
				vec.Validity().SetInvalid(rowIdx)
			} else {
				vec.SetValue(rowIdx, "")
			}
			continue
		}

		value := row[colIdx]

		// Handle empty cells
		if value == "" {
			if r.opts.EmptyAsNull {
				vec.Validity().SetInvalid(rowIdx)
			} else {
				vec.SetValue(rowIdx, "")
			}
			continue
		}

		// Check if value matches any configured NULL values
		if r.isNullValue(value) {
			vec.Validity().SetInvalid(rowIdx)
			continue
		}

		// Convert value to the appropriate type
		convertedValue := convertValueToType(value, r.columnTypes[colIdx], r.opts.EmptyAsNull)
		if convertedValue == nil {
			vec.Validity().SetInvalid(rowIdx)
			continue
		}

		vec.SetValue(rowIdx, convertedValue)
	}
}

// isNullValue checks if a value should be treated as NULL.
func (r *Reader) isNullValue(value string) bool {
	if r.opts.NullValues == nil {
		return false
	}

	for _, nullVal := range r.opts.NullValues {
		if value == nullVal {
			return true
		}
	}

	return false
}

// Close releases any resources held by the reader.
func (r *Reader) Close() error {
	var firstErr error

	if r.rows != nil {
		if err := r.rows.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if r.file != nil {
		if err := r.file.Close(); err != nil && firstErr == nil {
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

	// Create streaming row iterator
	rows, err := r.file.Rows(r.sheet)
	if err != nil {
		return fmt.Errorf("xlsx: failed to create row iterator for sheet %q: %w", r.sheet, err)
	}
	r.rows = rows
	r.currentRow = 0

	// Skip configured number of rows
	if err := r.skipRows(); err != nil {
		return err
	}

	// Initialize schema from header or generate column names
	if err := r.initializeSchema(); err != nil {
		return err
	}

	r.initialized = true
	return nil
}

// skipRows skips the configured number of initial rows.
func (r *Reader) skipRows() error {
	for i := 0; i < r.opts.Skip; i++ {
		if !r.rows.Next() {
			r.eof = true
			return nil
		}
		r.currentRow++
	}

	return nil
}

// initializeSchema reads the first row and sets up column names and types.
func (r *Reader) initializeSchema() error {
	// If header is expected, read the first row as column names
	if r.opts.Header {
		if !r.rows.Next() {
			// Empty file - no data
			r.eof = true
			r.columns = make([]string, 0)
			r.columnTypes = make([]dukdb.Type, 0)
			return nil
		}
		r.currentRow++

		headerRow, err := r.rows.Columns()
		if err != nil {
			return fmt.Errorf("xlsx: failed to read header row: %w", err)
		}

		// Filter columns if range is specified
		filteredHeader := r.filterColumns(headerRow)

		// Use header values as column names
		r.columns = make([]string, len(filteredHeader))
		copy(r.columns, filteredHeader)

		// Generate default names for empty headers
		for i, name := range r.columns {
			if name == "" {
				r.columns[i] = fmt.Sprintf("column%d", i)
			}
		}
	} else {
		// No header - peek at first row to determine column count
		// Then reset by creating a new iterator
		if !r.rows.Next() {
			r.eof = true
			r.columns = make([]string, 0)
			r.columnTypes = make([]dukdb.Type, 0)
			return nil
		}

		firstRow, err := r.rows.Columns()
		if err != nil {
			return fmt.Errorf("xlsx: failed to read first row: %w", err)
		}

		// Filter columns if range is specified
		filteredRow := r.filterColumns(firstRow)
		colCount := len(filteredRow)

		// Generate column names
		r.columns = generateColumnNames(colCount)

		// We need to rewind and re-read this row
		// Close current iterator and create a new one
		_ = r.rows.Close()

		rows, err := r.file.Rows(r.sheet)
		if err != nil {
			return fmt.Errorf("xlsx: failed to recreate row iterator: %w", err)
		}
		r.rows = rows
		r.currentRow = 0

		// Skip rows again
		if err := r.skipRows(); err != nil {
			return err
		}
	}

	// Initialize column types
	r.columnTypes = make([]dukdb.Type, len(r.columns))

	// Apply explicit column types if specified
	explicitTypesApplied := make([]bool, len(r.columns))
	if r.opts.Columns != nil {
		for i, colName := range r.columns {
			if typeName, ok := r.opts.Columns[colName]; ok {
				if typ, valid := parseTypeFromString(typeName); valid {
					r.columnTypes[i] = typ
					explicitTypesApplied[i] = true
				}
			}
		}
	}

	// Infer types for columns without explicit types
	if r.opts.InferTypes {
		if err := r.inferColumnTypes(explicitTypesApplied); err != nil {
			return err
		}
	} else {
		// Default to VARCHAR for all columns without explicit types
		for i := range r.columnTypes {
			if !explicitTypesApplied[i] {
				r.columnTypes[i] = dukdb.TYPE_VARCHAR
			}
		}
	}

	return nil
}

// inferColumnTypes samples data rows to infer column types.
// It skips columns that already have explicit types applied.
func (r *Reader) inferColumnTypes(explicitTypesApplied []bool) error {
	// Determine which row to start sampling from
	// We need to account for skip + header row
	dataStartRow := r.opts.Skip + 1 // 1-based
	if r.opts.Header {
		dataStartRow++ // Header is row after skip
	}

	// Generate sample row numbers
	sampleSize := TypeSampleSize
	if r.opts.ChunkSize > 0 && r.opts.ChunkSize < sampleSize {
		sampleSize = r.opts.ChunkSize
	}

	sampleRows := make([]int, 0, sampleSize)
	for i := 0; i < sampleSize; i++ {
		rowNum := dataStartRow + i
		// Respect row range constraints
		if r.startRow > 0 && rowNum < r.startRow {
			continue
		}
		if r.endRow > 0 && rowNum > r.endRow {
			break
		}
		sampleRows = append(sampleRows, rowNum)
	}

	// Infer type for each column
	for colIdx := range r.columns {
		if explicitTypesApplied[colIdx] {
			continue
		}

		// Calculate the actual column index in the sheet (accounting for range filtering)
		actualCol := r.startCol + colIdx

		// Infer type from sampled cells
		inferredType := inferTypesFromColumn(r.file, r.sheet, actualCol, sampleRows)
		r.columnTypes[colIdx] = inferredType
	}

	return nil
}

// generateColumnNames creates default column names like "column0", "column1", etc.
func generateColumnNames(count int) []string {
	names := make([]string, count)
	for i := range count {
		names[i] = fmt.Sprintf("column%d", i)
	}
	return names
}

// Verify Reader implements FileReader interface.
var _ fileio.FileReader = (*Reader)(nil)
