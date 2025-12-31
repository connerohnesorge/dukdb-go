package storage

import (
	"fmt"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
)

// RowGroupSize is the number of rows per row group.
const RowGroupSize = 122880 // 120K rows per row group

// RowID is a unique identifier for a row in a table.
// It is a monotonic counter that is assigned to each row on insertion.
type RowID uint64

// Bitmap tracks boolean values using a bit array.
// Used for tracking tombstones (deleted rows).
type Bitmap struct {
	bits []uint64
	size uint64
}

// NewBitmap creates a new Bitmap with the given initial size.
func NewBitmap(size uint64) *Bitmap {
	entries := (size + 63) / 64
	return &Bitmap{
		bits: make([]uint64, entries),
		size: size,
	}
}

// Get returns the bit value at the given index.
func (b *Bitmap) Get(idx RowID) bool {
	if uint64(idx) >= b.size {
		return false
	}
	entry := uint64(idx) / 64
	bit := uint64(1) << (uint64(idx) % 64)
	return b.bits[entry]&bit != 0
}

// Set sets the bit value at the given index.
func (b *Bitmap) Set(idx RowID, value bool) {
	// Expand bitmap if needed
	for uint64(idx) >= b.size {
		b.Grow(b.size * 2)
	}

	entry := uint64(idx) / 64
	bit := uint64(1) << (uint64(idx) % 64)

	if value {
		b.bits[entry] |= bit
	} else {
		b.bits[entry] &^= bit
	}
}

// Grow expands the bitmap to the new size.
func (b *Bitmap) Grow(newSize uint64) {
	if newSize <= b.size {
		return
	}

	newEntries := (newSize + 63) / 64
	oldEntries := uint64(len(b.bits))

	if newEntries > oldEntries {
		newBits := make([]uint64, newEntries)
		copy(newBits, b.bits)
		b.bits = newBits
	}

	b.size = newSize
}

// Table represents a table's data in columnar storage.
type Table struct {
	mu          sync.RWMutex
	name        string
	columnTypes []dukdb.Type
	rowGroups   []*RowGroup
	totalRows   int64

	// RowID tracking for DML operations
	nextRowID  uint64   // Monotonic counter for RowID generation
	tombstones *Bitmap  // Tracks deleted rows
	rowIDMap   map[RowID]*rowLocation // Maps RowID to physical location
}

// rowLocation identifies the physical location of a row.
type rowLocation struct {
	rowGroupIdx int
	rowIdx      int
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
		nextRowID:   0,
		tombstones:  NewBitmap(1024), // Start with space for 1024 rows
		rowIDMap:    make(map[RowID]*rowLocation),
	}
}

// generateRowID generates a unique RowID for a new row.
func (t *Table) generateRowID() RowID {
	id := RowID(t.nextRowID)
	t.nextRowID++
	return id
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
	var currentRGIdx int
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
		currentRGIdx = len(t.rowGroups) - 1
	} else {
		currentRGIdx = len(t.rowGroups) - 1
		currentRG = t.rowGroups[currentRGIdx]
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
			currentRGIdx = len(t.rowGroups) - 1
		}

		values := make([]any, chunk.ColumnCount())
		for col := 0; col < chunk.ColumnCount(); col++ {
			values[col] = chunk.GetValue(row, col)
		}

		// Generate RowID and track location
		rowID := t.generateRowID()
		rowIdxInGroup := currentRG.count
		currentRG.AppendRow(values)

		// Map RowID to physical location
		t.rowIDMap[rowID] = &rowLocation{
			rowGroupIdx: currentRGIdx,
			rowIdx:      rowIdxInGroup,
		}

		t.totalRows++
	}

	return nil
}

// InsertChunk inserts a data chunk into the table and returns the number of rows inserted.
// This is a wrapper around AppendChunk that provides a row count return value,
// which is useful for DML operations that need to report RowsAffected.
func (t *Table) InsertChunk(chunk *DataChunk) (int, error) {
	rowsBefore := t.totalRows
	err := t.AppendChunk(chunk)
	if err != nil {
		return 0, err
	}
	rowsInserted := int(t.totalRows - rowsBefore)
	return rowsInserted, nil
}

// AppendRow appends a single row to the table.
func (t *Table) AppendRow(values []any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get or create the current row group
	var currentRG *RowGroup
	var currentRGIdx int
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
		currentRGIdx = len(t.rowGroups) - 1
	} else {
		currentRGIdx = len(t.rowGroups) - 1
		currentRG = t.rowGroups[currentRGIdx]
	}

	// Generate RowID and track location
	rowID := t.generateRowID()
	rowIdxInGroup := currentRG.count
	currentRG.AppendRow(values)

	// Map RowID to physical location
	t.rowIDMap[rowID] = &rowLocation{
		rowGroupIdx: currentRGIdx,
		rowIdx:      rowIdxInGroup,
	}

	t.totalRows++

	return nil
}

// DeleteRows marks rows as deleted using tombstone marking.
// This does not physically remove the rows, allowing for MVCC and efficient rollback.
func (t *Table) DeleteRows(rowIDs []RowID) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range rowIDs {
		// Verify the row exists and is not already deleted
		if _, exists := t.rowIDMap[id]; !exists {
			return fmt.Errorf("row with RowID %d does not exist", id)
		}

		if t.tombstones.Get(id) {
			// Already deleted, skip
			continue
		}

		// Mark as deleted
		t.tombstones.Set(id, true)
	}

	return nil
}

// UpdateRows updates rows in-place using RowID lookup.
// The values map contains column indices to new values.
func (t *Table) UpdateRows(rowIDs []RowID, columnValues map[int]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range rowIDs {
		// Check if row exists and is not deleted
		loc, exists := t.rowIDMap[id]
		if !exists {
			return fmt.Errorf("row with RowID %d does not exist", id)
		}

		if t.tombstones.Get(id) {
			return fmt.Errorf("cannot update deleted row with RowID %d", id)
		}

		// Get the row group and update values
		if loc.rowGroupIdx >= len(t.rowGroups) {
			return fmt.Errorf("invalid row group index for RowID %d", id)
		}

		rg := t.rowGroups[loc.rowGroupIdx]

		for colIdx, value := range columnValues {
			if colIdx < 0 || colIdx >= len(rg.columns) {
				return fmt.Errorf("invalid column index %d", colIdx)
			}

			rg.columns[colIdx].SetValue(loc.rowIdx, value)
		}
	}

	return nil
}

// UpdateRow updates a single row identified by RowID with all column values.
// This is used for WAL recovery to replay UPDATE entries.
func (t *Table) UpdateRow(rowID RowID, values []any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if row exists and is not deleted
	loc, exists := t.rowIDMap[rowID]
	if !exists {
		return fmt.Errorf("row with RowID %d does not exist", rowID)
	}

	if t.tombstones.Get(rowID) {
		return fmt.Errorf("cannot update deleted row with RowID %d", rowID)
	}

	// Get the row group and update values
	if loc.rowGroupIdx >= len(t.rowGroups) {
		return fmt.Errorf("invalid row group index for RowID %d", rowID)
	}

	rg := t.rowGroups[loc.rowGroupIdx]

	if len(values) != len(rg.columns) {
		return fmt.Errorf("value count %d does not match column count %d", len(values), len(rg.columns))
	}

	for colIdx, value := range values {
		rg.columns[colIdx].SetValue(loc.rowIdx, value)
	}

	return nil
}

// DeleteRow marks a single row as deleted using tombstone marking.
// This is used for WAL recovery to replay DELETE entries.
func (t *Table) DeleteRow(rowID RowID) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Verify the row exists
	if _, exists := t.rowIDMap[rowID]; !exists {
		return fmt.Errorf("row with RowID %d does not exist", rowID)
	}

	// Check if already deleted
	if t.tombstones.Get(rowID) {
		// Already deleted, this is idempotent - not an error
		return nil
	}

	// Mark as deleted
	t.tombstones.Set(rowID, true)
	return nil
}

// GetRow retrieves a row by its RowID.
// Returns nil if the row doesn't exist or is deleted.
func (t *Table) GetRow(rowID RowID) []any {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if row exists
	loc, exists := t.rowIDMap[rowID]
	if !exists {
		return nil
	}

	// Check if row is deleted
	if t.tombstones.Get(rowID) {
		return nil
	}

	// Retrieve row values
	if loc.rowGroupIdx >= len(t.rowGroups) {
		return nil
	}

	rg := t.rowGroups[loc.rowGroupIdx]
	values := make([]any, len(rg.columns))

	for i, col := range rg.columns {
		values[i] = col.GetValue(loc.rowIdx)
	}

	return values
}

// ContainsRow checks if a row with the given RowID exists and is not deleted.
func (t *Table) ContainsRow(rowID RowID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, exists := t.rowIDMap[rowID]
	return exists && !t.tombstones.Get(rowID)
}

// IsTombstoned checks if a row with the given RowID is marked as deleted.
// Returns true if the row exists and is tombstoned, false otherwise.
func (t *Table) IsTombstoned(rowID RowID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, exists := t.rowIDMap[rowID]
	return exists && t.tombstones.Get(rowID)
}

// RowExists checks if a row with the given RowID exists (regardless of tombstone status).
func (t *Table) RowExists(rowID RowID) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, exists := t.rowIDMap[rowID]
	return exists
}

// NextRowID returns the next RowID that will be assigned.
func (t *Table) NextRowID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nextRowID
}

// Scan creates a scanner for iterating over the table.
// It snapshots the row groups to avoid holding locks during iteration.
func (t *Table) Scan() *TableScanner {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Snapshot row groups to avoid deadlocks during concurrent access
	rowGroups := make(
		[]*RowGroup,
		len(t.rowGroups),
	)
	copy(rowGroups, t.rowGroups)

	// Create a reverse map from physical location to RowID for tombstone checking
	locationToRowID := make(map[int]map[int]RowID)
	for rowID, loc := range t.rowIDMap {
		if locationToRowID[loc.rowGroupIdx] == nil {
			locationToRowID[loc.rowGroupIdx] = make(map[int]RowID)
		}
		locationToRowID[loc.rowGroupIdx][loc.rowIdx] = rowID
	}

	return &TableScanner{
		rowGroups:       rowGroups,
		currentRG:       0,
		currentRow:      0,
		columnTypes:     t.columnTypes,
		tombstones:      t.tombstones,
		locationToRowID: locationToRowID,
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
	for row := range count {
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
	rowGroups       []*RowGroup // Snapshot of row groups
	currentRG       int
	currentRow      int
	columnTypes     []dukdb.Type
	tombstones      *Bitmap                // Tombstone bitmap for filtering deleted rows
	locationToRowID map[int]map[int]RowID  // Maps physical location to RowID

	// Metadata about the last returned chunk for RowID tracking
	lastChunkRowIDs []RowID // RowIDs of rows in the last returned chunk
}

// Next advances to the next chunk and returns it, skipping tombstoned rows.
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

	// Read a chunk, filtering out tombstoned rows
	types := make([]dukdb.Type, len(rg.columns))
	for i, col := range rg.columns {
		types[i] = col.Type()
	}

	chunk := NewDataChunkWithCapacity(types, StandardVectorSize)

	// Clear previous chunk's RowID tracking
	s.lastChunkRowIDs = make([]RowID, 0, StandardVectorSize)

	// Iterate through rows and only add non-tombstoned ones
	rowsAdded := 0
	for s.currentRow < rg.Count() && rowsAdded < StandardVectorSize {
		// Look up the RowID for this physical location
		var rowID RowID
		var hasRowID bool
		if s.locationToRowID != nil {
			if rowIDMap, exists := s.locationToRowID[s.currentRG]; exists {
				rowID, hasRowID = rowIDMap[s.currentRow]
			}
		}

		// Check if this row is tombstoned
		var isTombstoned bool
		if hasRowID && s.tombstones != nil {
			isTombstoned = s.tombstones.Get(rowID)
		}

		if !isTombstoned {
			// Add this row to the chunk
			values := make([]any, len(rg.columns))
			for col, v := range rg.columns {
				values[col] = v.GetValue(s.currentRow)
			}
			chunk.AppendRow(values)
			rowsAdded++

			// Track the RowID for this row in the chunk
			if hasRowID {
				s.lastChunkRowIDs = append(s.lastChunkRowIDs, rowID)
			}
		}

		s.currentRow++
	}

	// If we added no rows, move to next row group
	if rowsAdded == 0 {
		s.currentRG++
		s.currentRow = 0
		return s.Next()
	}

	return chunk
}

// Reset resets the scanner to the beginning.
func (s *TableScanner) Reset() {
	s.currentRG = 0
	s.currentRow = 0
}

// GetRowID returns the RowID for a specific row in the last returned chunk.
// Returns nil if the rowIdx is out of bounds or if RowID tracking is not available.
func (s *TableScanner) GetRowID(rowIdx int) *RowID {
	if rowIdx < 0 || rowIdx >= len(s.lastChunkRowIDs) {
		return nil
	}
	rowID := s.lastChunkRowIDs[rowIdx]
	return &rowID
}
