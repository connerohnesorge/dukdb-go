package storage

import (
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
)

// RowGroupSize is the number of rows per row group.
const RowGroupSize = 122880 // 120K rows per row group

// Table represents a table's data in columnar storage.
type Table struct {
	mu          sync.RWMutex
	name        string
	columnTypes []dukdb.Type
	rowGroups   []*RowGroup
	totalRows   int64
}

// NewTable creates a new Table with the given column types.
func NewTable(
	name string,
	columnTypes []dukdb.Type,
) *Table {
	return &Table{
		name:        name,
		columnTypes: columnTypes,
		rowGroups:   make([]*RowGroup, 0),
		totalRows:   0,
	}
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// ColumnCount returns the number of columns.
func (t *Table) ColumnCount() int {
	return len(t.columnTypes)
}

// ColumnTypes returns the column types.
func (t *Table) ColumnTypes() []dukdb.Type {
	return t.columnTypes
}

// RowCount returns the total number of rows.
func (t *Table) RowCount() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.totalRows
}

// RowGroupCount returns the number of row groups.
func (t *Table) RowGroupCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rowGroups)
}

// GetRowGroup returns the row group at the given index.
func (t *Table) GetRowGroup(idx int) *RowGroup {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if idx < 0 || idx >= len(t.rowGroups) {
		return nil
	}
	return t.rowGroups[idx]
}

// AppendChunk appends a data chunk to the table.
func (t *Table) AppendChunk(
	chunk *DataChunk,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get or create the current row group
	var currentRG *RowGroup
	if len(t.rowGroups) == 0 ||
		t.rowGroups[len(t.rowGroups)-1].IsFull() {
		currentRG = NewRowGroup(
			t.columnTypes,
			RowGroupSize,
		)
		t.rowGroups = append(
			t.rowGroups,
			currentRG,
		)
	} else {
		currentRG = t.rowGroups[len(t.rowGroups)-1]
	}

	// Append rows from the chunk
	for row := 0; row < chunk.Count(); row++ {
		if currentRG.IsFull() {
			currentRG = NewRowGroup(
				t.columnTypes,
				RowGroupSize,
			)
			t.rowGroups = append(
				t.rowGroups,
				currentRG,
			)
		}

		values := make([]any, chunk.ColumnCount())
		for col := 0; col < chunk.ColumnCount(); col++ {
			values[col] = chunk.GetValue(row, col)
		}
		currentRG.AppendRow(values)
		t.totalRows++
	}

	return nil
}

// AppendRow appends a single row to the table.
func (t *Table) AppendRow(values []any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get or create the current row group
	var currentRG *RowGroup
	if len(t.rowGroups) == 0 ||
		t.rowGroups[len(t.rowGroups)-1].IsFull() {
		currentRG = NewRowGroup(
			t.columnTypes,
			RowGroupSize,
		)
		t.rowGroups = append(
			t.rowGroups,
			currentRG,
		)
	} else {
		currentRG = t.rowGroups[len(t.rowGroups)-1]
	}

	currentRG.AppendRow(values)
	t.totalRows++

	return nil
}

// Scan creates a scanner for iterating over the table.
// It snapshots the row groups to avoid holding locks during iteration.
func (t *Table) Scan() *TableScanner {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Snapshot row groups to avoid deadlocks during concurrent access
	rowGroups := make([]*RowGroup, len(t.rowGroups))
	copy(rowGroups, t.rowGroups)

	return &TableScanner{
		rowGroups:   rowGroups,
		currentRG:   0,
		currentRow:  0,
		columnTypes: t.columnTypes,
	}
}

// RowGroup represents a group of rows stored in columnar format.
type RowGroup struct {
	columns  []*Vector
	count    int
	capacity int
}

// NewRowGroup creates a new RowGroup with the given column types.
func NewRowGroup(
	columnTypes []dukdb.Type,
	capacity int,
) *RowGroup {
	columns := make([]*Vector, len(columnTypes))
	for i, t := range columnTypes {
		columns[i] = NewVector(t, capacity)
	}
	return &RowGroup{
		columns:  columns,
		count:    0,
		capacity: capacity,
	}
}

// Count returns the number of rows in the row group.
func (rg *RowGroup) Count() int {
	return rg.count
}

// Capacity returns the maximum number of rows.
func (rg *RowGroup) Capacity() int {
	return rg.capacity
}

// IsFull returns whether the row group is full.
func (rg *RowGroup) IsFull() bool {
	return rg.count >= rg.capacity
}

// GetColumn returns the column at the given index.
func (rg *RowGroup) GetColumn(idx int) *Vector {
	if idx < 0 || idx >= len(rg.columns) {
		return nil
	}
	return rg.columns[idx]
}

// AppendRow appends a row of values to the row group.
func (rg *RowGroup) AppendRow(values []any) bool {
	if rg.count >= rg.capacity {
		return false
	}
	if len(values) != len(rg.columns) {
		return false
	}

	for i, val := range values {
		rg.columns[i].SetValue(rg.count, val)
	}
	rg.count++

	// Update vector counts
	for _, col := range rg.columns {
		col.SetCount(rg.count)
	}

	return true
}

// GetValue returns the value at the given row and column.
func (rg *RowGroup) GetValue(row, col int) any {
	if col < 0 || col >= len(rg.columns) {
		return nil
	}
	return rg.columns[col].GetValue(row)
}

// GetChunk returns a DataChunk containing rows from startRow to startRow+count.
func (rg *RowGroup) GetChunk(
	startRow, count int,
) *DataChunk {
	if startRow >= rg.count {
		return nil
	}

	if startRow+count > rg.count {
		count = rg.count - startRow
	}

	types := make([]dukdb.Type, len(rg.columns))
	for i, col := range rg.columns {
		types[i] = col.Type()
	}

	chunk := NewDataChunkWithCapacity(
		types,
		count,
	)
	for row := 0; row < count; row++ {
		values := make([]any, len(rg.columns))
		for col, v := range rg.columns {
			values[col] = v.GetValue(
				startRow + row,
			)
		}
		chunk.AppendRow(values)
	}

	return chunk
}

// TableScanner provides iteration over table rows.
// It uses a snapshot of row groups to avoid holding locks during iteration.
type TableScanner struct {
	rowGroups   []*RowGroup // Snapshot of row groups
	currentRG   int
	currentRow  int
	columnTypes []dukdb.Type
}

// Next advances to the next chunk and returns it.
// Returns nil when there are no more chunks.
// No locks are held during iteration since row groups are snapshotted.
func (s *TableScanner) Next() *DataChunk {
	if s.currentRG >= len(s.rowGroups) {
		return nil
	}

	rg := s.rowGroups[s.currentRG]

	// Calculate how many rows to read
	remaining := rg.Count() - s.currentRow
	if remaining <= 0 {
		// Move to next row group
		s.currentRG++
		s.currentRow = 0
		return s.Next()
	}

	// Read a chunk
	chunkSize := StandardVectorSize
	if remaining < chunkSize {
		chunkSize = remaining
	}

	chunk := rg.GetChunk(s.currentRow, chunkSize)
	s.currentRow += chunkSize

	return chunk
}

// Reset resets the scanner to the beginning.
func (s *TableScanner) Reset() {
	s.currentRG = 0
	s.currentRow = 0
}
