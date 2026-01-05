package duckdb

import (
	"errors"
	"fmt"
	"sync"
)

// Table scanner error definitions.
var (
	// ErrScannerClosed indicates the scanner has been closed.
	ErrScannerClosed = errors.New("scanner is closed")

	// ErrNoMoreRows indicates there are no more rows to scan.
	ErrNoMoreRows = errors.New("no more rows")

	// ErrInvalidProjection indicates the projection contains invalid column indices.
	ErrInvalidProjection = errors.New("invalid projection")

	// ErrRowGroupIndexOutOfRange indicates the row group index is outside valid bounds.
	ErrRowGroupIndexOutOfRange = errors.New("row group index out of range")
)

// ScanResult represents the batch result of scanning a single row group.
// It contains all projected columns from the row group.
type ScanResult struct {
	// Columns contains the projected column data.
	// The order matches the projection indices, or all columns if no projection.
	Columns []*ColumnData

	// RowCount is the number of rows in this result.
	RowCount uint64

	// RowGroupID is the index of the row group this result came from.
	RowGroupID int
}

// GetValue returns the value at the given row and column index.
// The colIdx is the index into the projection (not the table column index).
func (r *ScanResult) GetValue(rowIdx uint64, colIdx int) (any, bool) {
	if colIdx < 0 || colIdx >= len(r.Columns) {
		return nil, false
	}
	return r.Columns[colIdx].GetValue(rowIdx)
}

// IsNull returns true if the value at the given row and column is NULL.
func (r *ScanResult) IsNull(rowIdx uint64, colIdx int) bool {
	if colIdx < 0 || colIdx >= len(r.Columns) {
		return true
	}
	return r.Columns[colIdx].IsNull(rowIdx)
}

// TableScanner scans a table across multiple row groups with projection pushdown.
// It supports both row-by-row iteration and batch scanning of entire row groups.
type TableScanner struct {
	// blockManager is used to read blocks from the file.
	blockManager *BlockManager

	// tableEntry contains table metadata (columns, types).
	tableEntry *TableCatalogEntry

	// rowGroups contains pointers to all row groups for the table.
	rowGroups []*RowGroupPointer

	// projection contains column indices to read (nil = all columns).
	projection []int

	// columnTypes contains the logical type of each table column.
	columnTypes []LogicalTypeID

	// currentRGIdx is the index of the current row group being scanned.
	currentRGIdx int

	// currentReader is the reader for the current row group.
	currentReader *RowGroupReader

	// currentRowIdx is the current row index within the current row group.
	currentRowIdx uint64

	// totalRows is the total number of rows scanned so far.
	totalRows uint64

	// closed indicates whether the scanner has been closed.
	closed bool

	// mu protects concurrent access to the scanner.
	mu sync.Mutex
}

// NewTableScanner creates a new TableScanner for reading a table across multiple row groups.
// The scanner supports projection pushdown - only specified columns are read from disk.
func NewTableScanner(
	bm *BlockManager,
	table *TableCatalogEntry,
	rowGroups []*RowGroupPointer,
) *TableScanner {
	// Extract column types from table definition
	columnTypes := make([]LogicalTypeID, len(table.Columns))
	for i, col := range table.Columns {
		columnTypes[i] = col.Type
	}

	return &TableScanner{
		blockManager: bm,
		tableEntry:   table,
		rowGroups:    rowGroups,
		projection:   nil, // Default: read all columns
		columnTypes:  columnTypes,
		currentRGIdx: -1, // Not started yet
		currentRowIdx: 0,
		totalRows:    0,
		closed:       false,
	}
}

// SetProjection sets which columns to read.
// Pass nil to read all columns.
// Column indices must be valid for the table schema.
func (s *TableScanner) SetProjection(columnIdxs []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrScannerClosed
	}

	// Validate projection indices
	if columnIdxs != nil {
		for _, idx := range columnIdxs {
			if idx < 0 || idx >= len(s.tableEntry.Columns) {
				return fmt.Errorf("%w: column index %d out of range (max %d)",
					ErrInvalidProjection, idx, len(s.tableEntry.Columns)-1)
			}
		}
	}

	// Make a copy to prevent external modification
	if columnIdxs == nil {
		s.projection = nil
	} else {
		s.projection = make([]int, len(columnIdxs))
		copy(s.projection, columnIdxs)
	}

	return nil
}

// Projection returns the current projection (nil if reading all columns).
func (s *TableScanner) Projection() []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.projection == nil {
		return nil
	}

	// Return a copy to prevent external modification
	result := make([]int, len(s.projection))
	copy(result, s.projection)
	return result
}

// Next advances to the next row and returns true if there is a row to read.
// Returns false when all rows have been scanned or the scanner is closed.
func (s *TableScanner) Next() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	// If no current reader, try to advance to first row group
	if s.currentReader == nil {
		if !s.advanceRowGroupLocked() {
			return false
		}
	}

	// Check if we have more rows in current row group
	if s.currentRowIdx < s.currentReader.TupleCount() {
		return true
	}

	// Try to advance to next row group
	if !s.advanceRowGroupLocked() {
		return false
	}

	return s.currentRowIdx < s.currentReader.TupleCount()
}

// GetRow returns the current row's values for projected columns.
// Returns an error if no current row exists.
func (s *TableScanner) GetRow() ([]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrScannerClosed
	}

	if s.currentReader == nil {
		return nil, ErrNoMoreRows
	}

	if s.currentRowIdx >= s.currentReader.TupleCount() {
		return nil, ErrNoMoreRows
	}

	// Get the column indices to read
	colIndices := s.getProjectedIndicesLocked()

	// Read projected columns
	columns, err := s.readProjectedColumnsLocked()
	if err != nil {
		return nil, err
	}

	// Build result row
	result := make([]any, len(colIndices))
	for i := range colIndices {
		val, valid := columns[i].GetValue(s.currentRowIdx)
		if !valid {
			result[i] = nil
		} else {
			result[i] = val
		}
	}

	return result, nil
}

// GetColumn returns a specific column value from the current row.
// The colIdx is an index into the projection array, not the table column index.
func (s *TableScanner) GetColumn(colIdx int) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrScannerClosed
	}

	if s.currentReader == nil {
		return nil, ErrNoMoreRows
	}

	if s.currentRowIdx >= s.currentReader.TupleCount() {
		return nil, ErrNoMoreRows
	}

	// Map projection index to table column index
	colIndices := s.getProjectedIndicesLocked()
	if colIdx < 0 || colIdx >= len(colIndices) {
		return nil, fmt.Errorf("%w: %d (max %d)",
			ErrColumnIndexOutOfRange, colIdx, len(colIndices)-1)
	}

	tableColIdx := colIndices[colIdx]

	// Read the column
	colData, err := s.currentReader.ReadColumn(tableColIdx)
	if err != nil {
		return nil, err
	}

	val, valid := colData.GetValue(s.currentRowIdx)
	if !valid {
		return nil, nil // NULL value
	}

	return val, nil
}

// Advance moves to the next row after reading current values.
// This should be called after processing the current row with GetRow/GetColumn.
func (s *TableScanner) Advance() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.currentReader == nil {
		return
	}

	s.currentRowIdx++
	s.totalRows++
}

// Reset resets the scanner to the beginning.
// After reset, call Next() to move to the first row.
func (s *TableScanner) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	// Clear the current reader
	if s.currentReader != nil {
		s.currentReader.ClearCache()
		s.currentReader = nil
	}

	s.currentRGIdx = -1
	s.currentRowIdx = 0
	s.totalRows = 0
}

// Close releases resources held by the scanner.
// After calling Close, the scanner cannot be used.
func (s *TableScanner) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.closed = true

	if s.currentReader != nil {
		s.currentReader.ClearAllCaches()
		s.currentReader = nil
	}

	s.rowGroups = nil
	s.projection = nil
}

// ScanRowGroup scans an entire row group by index and returns batch results.
// This is more efficient than row-by-row iteration for bulk operations.
func (s *TableScanner) ScanRowGroup(rgIdx int) (*ScanResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrScannerClosed
	}

	if rgIdx < 0 || rgIdx >= len(s.rowGroups) {
		return nil, fmt.Errorf("%w: %d (max %d)",
			ErrRowGroupIndexOutOfRange, rgIdx, len(s.rowGroups)-1)
	}

	// Create a reader for this row group
	rgp := s.rowGroups[rgIdx]
	reader := NewRowGroupReader(s.blockManager, rgp, s.columnTypes)

	// Get the column indices to read
	colIndices := s.getProjectedIndicesLocked()

	// Read all projected columns
	columns, err := reader.ReadColumns(colIndices)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row group %d: %w", rgIdx, err)
	}

	return &ScanResult{
		Columns:    columns,
		RowCount:   rgp.TupleCount,
		RowGroupID: rgIdx,
	}, nil
}

// ScanAll scans all row groups and returns combined results.
// Results are returned in row group order.
func (s *TableScanner) ScanAll() ([]*ScanResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrScannerClosed
	}

	results := make([]*ScanResult, 0, len(s.rowGroups))

	for rgIdx := range s.rowGroups {
		// Unlock to allow concurrent access during scan
		s.mu.Unlock()
		result, err := s.ScanRowGroup(rgIdx)
		s.mu.Lock()

		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

// TotalRows returns the total number of rows scanned so far.
func (s *TableScanner) TotalRows() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalRows
}

// RowGroupCount returns the total number of row groups in the table.
func (s *TableScanner) RowGroupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rowGroups)
}

// CurrentRowGroupIndex returns the index of the current row group being scanned.
// Returns -1 if no row group is currently active.
func (s *TableScanner) CurrentRowGroupIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentRGIdx
}

// TotalTableRows returns the total number of rows across all row groups.
func (s *TableScanner) TotalTableRows() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var total uint64
	for _, rg := range s.rowGroups {
		total += rg.TupleCount
	}
	return total
}

// advanceRowGroupLocked advances to the next row group.
// Must be called with lock held.
// Returns false if there are no more row groups.
func (s *TableScanner) advanceRowGroupLocked() bool {
	s.currentRGIdx++

	if s.currentRGIdx >= len(s.rowGroups) {
		s.currentReader = nil
		return false // No more row groups
	}

	// Create new reader for next row group
	s.currentReader = NewRowGroupReader(
		s.blockManager,
		s.rowGroups[s.currentRGIdx],
		s.columnTypes,
	)
	s.currentRowIdx = 0

	return true
}

// getProjectedIndicesLocked returns the column indices to read.
// If no projection is set, returns all column indices.
// Must be called with lock held.
func (s *TableScanner) getProjectedIndicesLocked() []int {
	if s.projection != nil {
		return s.projection
	}

	// Return all column indices
	indices := make([]int, len(s.tableEntry.Columns))
	for i := range indices {
		indices[i] = i
	}
	return indices
}

// readProjectedColumnsLocked reads the projected columns from the current row group.
// Must be called with lock held.
func (s *TableScanner) readProjectedColumnsLocked() ([]*ColumnData, error) {
	if s.currentReader == nil {
		return nil, ErrNoMoreRows
	}

	colIndices := s.getProjectedIndicesLocked()
	return s.currentReader.ReadColumns(colIndices)
}

// TableScannerIterator provides a simplified row-by-row interface for TableScanner.
// It implements an iterator pattern without requiring explicit Advance calls.
type TableScannerIterator struct {
	scanner    *TableScanner
	row        []any
	err        error
	hasRow     bool
}

// NewTableScannerIterator creates an iterator over the table scanner.
func NewTableScannerIterator(scanner *TableScanner) *TableScannerIterator {
	return &TableScannerIterator{
		scanner: scanner,
	}
}

// Next advances to the next row and returns true if there is a row.
// If an error occurs during scanning, it can be retrieved via Err().
func (it *TableScannerIterator) Next() bool {
	if it.err != nil {
		return false
	}

	// Advance if we already read a row
	if it.hasRow {
		it.scanner.Advance()
	}

	if !it.scanner.Next() {
		it.hasRow = false
		return false
	}

	it.row, it.err = it.scanner.GetRow()
	if it.err != nil {
		it.hasRow = false
		return false
	}

	it.hasRow = true
	return true
}

// Row returns the current row's values.
// Returns nil if there is no current row.
func (it *TableScannerIterator) Row() []any {
	if !it.hasRow {
		return nil
	}
	return it.row
}

// Err returns any error that occurred during iteration.
func (it *TableScannerIterator) Err() error {
	return it.err
}

// Close releases resources and closes the underlying scanner.
func (it *TableScannerIterator) Close() {
	if it.scanner != nil {
		it.scanner.Close()
	}
}
