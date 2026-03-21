package storage

import (
	"fmt"
	"sync"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
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

// MVCCTransactionContext provides transaction context for versioned operations.
// This interface is implemented by MVCCTransaction in the engine package.
// It is defined here in storage to avoid circular imports between storage and engine.
//
// The interface matches the methods available on MVCCTransaction that are needed
// for versioned table operations:
//   - ID() for identifying the transaction creating/modifying versions
//   - GetStartTS() for visibility calculations
//   - GetCommitTS() for committing versions
//   - RecordRead/RecordWrite for conflict detection
//   - VisibilityChecker() for finding visible versions
type MVCCTransactionContext interface {
	// ID returns the unique identifier for this transaction.
	ID() uint64

	// GetStartTS returns the MVCC start timestamp for this transaction.
	GetStartTS() uint64

	// GetCommitTS returns the commit timestamp for this transaction.
	// Returns 0 if the transaction has not yet committed.
	GetCommitTS() uint64

	// RecordRead tracks that this transaction has read a row from a table.
	RecordRead(table string, rowID uint64)

	// RecordWrite tracks that this transaction has written a row to a table.
	RecordWrite(table string, rowID uint64)

	// VisibilityChecker returns the appropriate visibility checker for this
	// transaction's isolation level.
	VisibilityChecker() VisibilityChecker
}

// Table represents a table's data in columnar storage.
type Table struct {
	mu          sync.RWMutex
	name        string
	columnTypes []dukdb.Type
	rowGroups   []*RowGroup
	totalRows   int64

	// RowID tracking for DML operations
	nextRowID  uint64                 // Monotonic counter for RowID generation
	tombstones *Bitmap                // Tracks deleted rows
	rowIDMap   map[RowID]*rowLocation // Maps RowID to physical location

	// MVCC version tracking for isolation levels
	rowVersions map[RowID]*VersionInfo // Maps RowID to version info

	// MVCC version chains for snapshot isolation
	versions   map[RowID]*VersionChain // rowID -> version chain
	versionsMu sync.RWMutex            // Protects versions map
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
		rowVersions: make(map[RowID]*VersionInfo),
		versions:    make(map[RowID]*VersionChain),
	}
}

// generateRowID generates a unique RowID for a new row.
func (t *Table) generateRowID() RowID {
	id := RowID(t.nextRowID)
	t.nextRowID++
	return id
}

// Truncate removes all rows from the table, returning the number of rows removed.
func (t *Table) Truncate() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	prevRows := t.totalRows
	t.rowGroups = make([]*RowGroup, 0)
	t.totalRows = 0
	t.nextRowID = 0
	t.tombstones = NewBitmap(1024)
	t.rowIDMap = make(map[RowID]*rowLocation)
	t.rowVersions = make(map[RowID]*VersionInfo)

	t.versionsMu.Lock()
	t.versions = make(map[RowID]*VersionChain)
	t.versionsMu.Unlock()

	return prevRows
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// Rename updates the table's name.
func (t *Table) Rename(newName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.name = newName
}

// AddColumn adds a new column to the table.
func (t *Table) AddColumn(columnType dukdb.Type) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.columnTypes = append(t.columnTypes, columnType)

	// Add a new NULL-filled vector to all existing row groups
	for _, rg := range t.rowGroups {
		rg.AddColumn(columnType, rg.count)
	}
}

// SetColumnType updates the type of the column at the given index.
// This only updates the column type metadata; data conversion must be done separately
// by iterating over row groups.
func (t *Table) SetColumnType(columnIndex int, newType dukdb.Type) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnIndex < 0 || columnIndex >= len(t.columnTypes) {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "column index out of range",
		}
	}

	t.columnTypes[columnIndex] = newType
	return nil
}

// DropColumn removes a column from the table at the given index.
func (t *Table) DropColumn(columnIndex int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if columnIndex < 0 || columnIndex >= len(t.columnTypes) {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  "column index out of range",
		}
	}

	// Remove column type
	t.columnTypes = append(t.columnTypes[:columnIndex], t.columnTypes[columnIndex+1:]...)

	// Remove data from all row groups
	for _, rg := range t.rowGroups {
		rg.DropColumn(columnIndex)
	}

	return nil
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
		// Compress the full row group before creating a new one
		if len(t.rowGroups) > 0 {
			oldRG := t.rowGroups[len(t.rowGroups)-1]
			oldRG.mu.Lock()
			oldRG.Compress()
			oldRG.mu.Unlock()
		}
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
			currentRG.mu.Lock()
			currentRG.Compress()
			currentRG.mu.Unlock()
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
	t.mu.Lock()
	rowsBefore := t.totalRows
	t.mu.Unlock()

	err := t.AppendChunk(chunk)
	if err != nil {
		return 0, err
	}

	t.mu.RLock()
	rowsInserted := int(t.totalRows - rowsBefore)
	t.mu.RUnlock()

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
		return fmt.Errorf(
			"value count %d does not match column count %d",
			len(values),
			len(rg.columns),
		)
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

// ClearTombstone marks a row as not deleted (undo delete).
// This is used for transaction rollback of DELETE operations.
func (t *Table) ClearTombstone(rowID RowID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tombstones.Set(rowID, false)
}

// ClearTombstones marks multiple rows as not deleted (undo delete).
// This is used for transaction rollback of DELETE operations.
func (t *Table) ClearTombstones(rowIDs []RowID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rowID := range rowIDs {
		t.tombstones.Set(rowID, false)
	}
}

// RestoreRows restores rows to their before-image values.
// This is used for transaction rollback of UPDATE operations.
// The beforeImage maps column index to a slice of values (one per rowID).
func (t *Table) RestoreRows(rowIDs []uint64, beforeImage map[int][]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, rowID := range rowIDs {
		loc, ok := t.rowIDMap[RowID(rowID)]
		if !ok {
			continue
		}

		if loc.rowGroupIdx >= len(t.rowGroups) {
			continue
		}

		rg := t.rowGroups[loc.rowGroupIdx]

		for colIdx, values := range beforeImage {
			if colIdx >= 0 && colIdx < len(rg.columns) && i < len(values) {
				rg.columns[colIdx].SetValue(loc.rowIdx, values[i])
			}
		}
	}
	return nil
}

// HardDeleteRows removes rows by setting tombstones for them.
// This is used for transaction rollback of INSERT operations.
// Unlike DeleteRows which validates existence, this silently ignores missing rows.
func (t *Table) HardDeleteRows(rowIDs []uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range rowIDs {
		rowID := RowID(id)
		// Only mark as deleted if the row exists
		if _, exists := t.rowIDMap[rowID]; exists {
			t.tombstones.Set(rowID, true)
		}
	}
}

// SetRowVersion sets the version info for a row.
// This should be called when creating or modifying a row to track MVCC metadata.
func (t *Table) SetRowVersion(rowID RowID, version *VersionInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rowVersions[rowID] = version
}

// GetRowVersion returns the version info for a row.
// Returns nil if no version info is tracked for this row.
func (t *Table) GetRowVersion(rowID RowID) *VersionInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rowVersions[rowID]
}

// SetRowDeleted marks a row as deleted by a specific transaction.
// This updates both the tombstone and the version info.
func (t *Table) SetRowDeleted(rowID RowID, deletedTxnID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tombstones.Set(rowID, true)
	if version, exists := t.rowVersions[rowID]; exists {
		version.DeletedTxnID = deletedTxnID
	}
}

// Scan creates a scanner for iterating over the table.
// It snapshots the row groups and their counts to avoid holding locks during iteration.
func (t *Table) Scan() *TableScanner {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Snapshot row groups to avoid deadlocks during concurrent access
	rowGroups := make(
		[]*RowGroup,
		len(t.rowGroups),
	)
	copy(rowGroups, t.rowGroups)

	// Snapshot row group counts to provide a consistent view of data
	// This ensures we only read rows that existed at scan creation time
	rowGroupCounts := make([]int, len(rowGroups))
	for i, rg := range rowGroups {
		rowGroupCounts[i] = rg.count
	}

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
		rowGroupCounts:  rowGroupCounts,
		currentRG:       0,
		currentRow:      0,
		columnTypes:     t.columnTypes,
		tombstones:      t.tombstones,
		locationToRowID: locationToRowID,
	}
}

// ScanWithVisibility creates a scanner that uses MVCC visibility checking.
// The visibility checker determines which row versions are visible to the transaction.
// If visibility or txnCtx is nil, the scanner behaves like Scan().
func (t *Table) ScanWithVisibility(
	visibility VisibilityChecker,
	txnCtx TransactionContext,
) *TableScanner {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Snapshot row groups to avoid deadlocks during concurrent access
	rowGroups := make(
		[]*RowGroup,
		len(t.rowGroups),
	)
	copy(rowGroups, t.rowGroups)

	// Snapshot row group counts to provide a consistent view of data
	rowGroupCounts := make([]int, len(rowGroups))
	for i, rg := range rowGroups {
		rowGroupCounts[i] = rg.count
	}

	// Create a reverse map from physical location to RowID
	locationToRowID := make(map[int]map[int]RowID)
	for rowID, loc := range t.rowIDMap {
		if locationToRowID[loc.rowGroupIdx] == nil {
			locationToRowID[loc.rowGroupIdx] = make(map[int]RowID)
		}
		locationToRowID[loc.rowGroupIdx][loc.rowIdx] = rowID
	}

	// Snapshot row versions for visibility checking
	var rowVersions map[RowID]*VersionInfo
	if visibility != nil && txnCtx != nil {
		rowVersions = make(map[RowID]*VersionInfo, len(t.rowVersions))
		for k, v := range t.rowVersions {
			// Copy the version info to avoid race conditions
			vCopy := *v
			rowVersions[k] = &vCopy
		}
	}

	return &TableScanner{
		rowGroups:       rowGroups,
		rowGroupCounts:  rowGroupCounts,
		currentRG:       0,
		currentRow:      0,
		columnTypes:     t.columnTypes,
		tombstones:      t.tombstones,
		locationToRowID: locationToRowID,
		rowVersions:     rowVersions,
		visibility:      visibility,
		txnCtx:          txnCtx,
	}
}

// RowGroup represents a group of rows stored in columnar format.
type RowGroup struct {
	mu         sync.RWMutex
	columns    []*Vector
	count      int
	capacity   int
	compressed []*CompressedSegment // one per column, nil = uncompressed
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
	rg.mu.RLock()
	defer rg.mu.RUnlock()
	return rg.count
}

// Capacity returns the maximum number of rows.
func (rg *RowGroup) Capacity() int {
	return rg.capacity
}

// IsFull returns whether the row group is full.
func (rg *RowGroup) IsFull() bool {
	rg.mu.RLock()
	defer rg.mu.RUnlock()
	return rg.count >= rg.capacity
}

// Compress analyzes and compresses each column in the row group.
// Must be called under write lock.
func (rg *RowGroup) Compress() {
	if rg.compressed != nil {
		return // Already compressed
	}
	rg.compressed = make([]*CompressedSegment, len(rg.columns))
	for i, col := range rg.columns {
		col.SetCount(rg.count) // Ensure count is set before compression
		seg := AnalyzeAndCompress(col)
		if seg != nil {
			rg.compressed[i] = seg
		}
	}
}

// IsCompressed returns whether the row group has been compressed.
func (rg *RowGroup) IsCompressed() bool {
	rg.mu.RLock()
	defer rg.mu.RUnlock()
	return rg.compressed != nil
}

// GetColumn returns the column at the given index.
func (rg *RowGroup) GetColumn(idx int) *Vector {
	rg.mu.RLock()
	defer rg.mu.RUnlock()
	if idx < 0 || idx >= len(rg.columns) {
		return nil
	}

	return rg.columns[idx]
}

// SetColumn replaces the column vector at the given index.
func (rg *RowGroup) SetColumn(idx int, vec *Vector) {
	rg.mu.Lock()
	defer rg.mu.Unlock()
	if idx < 0 || idx >= len(rg.columns) {
		return
	}
	rg.columns[idx] = vec
}

// DropColumn removes a column from the row group at the given index.
func (rg *RowGroup) DropColumn(columnIndex int) {
	rg.mu.Lock()
	defer rg.mu.Unlock()
	if columnIndex < 0 || columnIndex >= len(rg.columns) {
		return
	}
	rg.columns = append(rg.columns[:columnIndex], rg.columns[columnIndex+1:]...)
}

// AddColumn adds a new column to the row group with NULL values for existing rows.
func (rg *RowGroup) AddColumn(columnType dukdb.Type, existingRows int) {
	rg.mu.Lock()
	defer rg.mu.Unlock()
	// Create a new vector for the column
	vec := NewVector(columnType, rg.capacity)
	vec.SetCount(existingRows)

	// Mark all existing rows as NULL for this new column
	for i := 0; i < existingRows; i++ {
		vec.SetValue(i, nil)
	}

	rg.columns = append(rg.columns, vec)
}

// AppendRow appends a row of values to the row group.
func (rg *RowGroup) AppendRow(values []any) bool {
	rg.mu.Lock()
	defer rg.mu.Unlock()
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
	rg.mu.RLock()
	defer rg.mu.RUnlock()
	if col < 0 || col >= len(rg.columns) {
		return nil
	}

	return rg.columns[col].GetValue(row)
}

// GetChunk returns a DataChunk containing rows from startRow to startRow+count.
func (rg *RowGroup) GetChunk(
	startRow, count int,
) *DataChunk {
	rg.mu.RLock()
	defer rg.mu.RUnlock()
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
	rowGroupCounts  []int       // Snapshotted counts for each row group at scan creation time
	currentRG       int
	currentRow      int
	columnTypes     []dukdb.Type
	tombstones      *Bitmap               // Tombstone bitmap for filtering deleted rows
	locationToRowID map[int]map[int]RowID // Maps physical location to RowID

	// Metadata about the last returned chunk for RowID tracking
	lastChunkRowIDs []RowID // RowIDs of rows in the last returned chunk

	// MVCC visibility support
	rowVersions map[RowID]*VersionInfo // Snapshot of row version info (nil if visibility not used)
	visibility  VisibilityChecker      // Visibility checker for MVCC (nil for basic scan)
	txnCtx      TransactionContext     // Transaction context for visibility checks (nil for basic scan)
}

// Next advances to the next chunk and returns it, skipping tombstoned rows.
// Returns nil when there are no more chunks.
// Holds a read lock on the row group during iteration for thread safety.
func (s *TableScanner) Next() *DataChunk {
	if s.currentRG >= len(s.rowGroups) {
		return nil
	}

	rg := s.rowGroups[s.currentRG]
	// Use snapshotted count to ensure consistent view of data
	rgCount := s.rowGroupCounts[s.currentRG]

	// Calculate how many rows to read
	remaining := rgCount - s.currentRow
	if remaining <= 0 {
		// Move to next row group
		s.currentRG++
		s.currentRow = 0

		return s.Next()
	}

	// Hold read lock during the entire read operation
	rg.mu.RLock()

	// Read a chunk, filtering out tombstoned rows
	types := make([]dukdb.Type, len(rg.columns))
	for i, col := range rg.columns {
		types[i] = col.Type()
	}

	chunk := NewDataChunkWithCapacity(types, StandardVectorSize)

	// Clear previous chunk's RowID tracking
	s.lastChunkRowIDs = make([]RowID, 0, StandardVectorSize)

	// Iterate through rows and only add non-tombstoned ones
	// Use snapshotted count to avoid reading rows added after scan creation
	rowsAdded := 0
	for s.currentRow < rgCount && rowsAdded < StandardVectorSize {
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

		// Determine visibility based on MVCC rules if visibility checker is set
		isVisible := true
		if !isTombstoned && s.visibility != nil && s.txnCtx != nil && hasRowID {
			// Get version info for this row
			if version, exists := s.rowVersions[rowID]; exists {
				isVisible = s.visibility.IsVisible(*version, s.txnCtx)
			} else {
				// No version info - create a default that marks it as created by system
				// and visible to everyone (legacy rows before MVCC was enabled)
				isVisible = true
			}
		}

		if !isTombstoned && isVisible {
			// Add this row to the chunk
			values := make([]any, len(rg.columns))
			for col, v := range rg.columns {
				if rg.compressed != nil && rg.compressed[col] != nil {
					// Decompress single row from compressed segment
					tmpVec := DecompressSegment(rg.compressed[col], s.currentRow, 1)
					if tmpVec != nil && tmpVec.Count() > 0 {
						values[col] = tmpVec.GetValue(0)
					}
				} else {
					values[col] = v.GetValue(s.currentRow)
				}
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

	rg.mu.RUnlock()

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

// =============================================================================
// Versioned Table Operations for MVCC
// =============================================================================
//
// The following methods provide MVCC-aware table operations that integrate with
// the version chain system. These methods are used by transactions that require
// snapshot isolation or serializable isolation.
//
// Each method:
//   - Operates on the versions map (VersionChain per row) for MVCC visibility
//   - Records reads/writes in the transaction for conflict detection
//   - Also updates the regular storage (rowIDMap, tombstones) for backwards compatibility

// InsertVersioned inserts a new row with MVCC versioning.
// It creates a new VersionedRow and adds it to a new VersionChain for the row.
// The row is also stored in regular storage for backwards compatibility with
// non-MVCC operations.
//
// Parameters:
//   - txn: The transaction context for this operation
//   - values: The column values for the new row
//
// Returns:
//   - RowID: The unique identifier assigned to the new row
//   - error: An error if the insert fails
//
// The new row will be visible to the inserting transaction immediately, but
// will not be visible to other transactions until the inserting transaction
// commits (depending on their isolation level).
func (t *Table) InsertVersioned(txn MVCCTransactionContext, values []any) (RowID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Generate a new RowID
	rowID := t.generateRowID()

	// Create the versioned row
	version := &VersionedRow{
		Data:      make([]any, len(values)),
		RowID:     uint64(rowID),
		TxnID:     txn.ID(),
		CommitTS:  0, // Will be set when transaction commits
		DeletedBy: 0,
		DeleteTS:  0,
		PrevPtr:   nil,
	}
	copy(version.Data, values)

	// Create a new version chain with this version
	t.versionsMu.Lock()
	chain := NewVersionChain(uint64(rowID))
	chain.AddVersion(version)
	t.versions[rowID] = chain
	t.versionsMu.Unlock()

	// Record the write in the transaction for conflict detection
	txn.RecordWrite(t.name, uint64(rowID))

	// Also store in regular storage for backwards compatibility
	// Get or create the current row group
	var currentRG *RowGroup
	var currentRGIdx int
	if len(t.rowGroups) == 0 || t.rowGroups[len(t.rowGroups)-1].IsFull() {
		// Compress the full row group before creating a new one
		if len(t.rowGroups) > 0 {
			oldRG := t.rowGroups[len(t.rowGroups)-1]
			oldRG.mu.Lock()
			oldRG.Compress()
			oldRG.mu.Unlock()
		}
		currentRG = NewRowGroup(t.columnTypes, RowGroupSize)
		t.rowGroups = append(t.rowGroups, currentRG)
		currentRGIdx = len(t.rowGroups) - 1
	} else {
		currentRGIdx = len(t.rowGroups) - 1
		currentRG = t.rowGroups[currentRGIdx]
	}

	rowIdxInGroup := currentRG.count
	currentRG.AppendRow(values)

	// Map RowID to physical location
	t.rowIDMap[rowID] = &rowLocation{
		rowGroupIdx: currentRGIdx,
		rowIdx:      rowIdxInGroup,
	}

	t.totalRows++

	return rowID, nil
}

// ReadVersioned reads a row using MVCC visibility rules.
// It traverses the version chain to find the version visible to the given transaction.
//
// Parameters:
//   - txn: The transaction context for this operation
//   - rowID: The unique identifier of the row to read
//
// Returns:
//   - []any: The column values of the visible version, or nil if not found/visible
//   - error: An error if the row does not exist or no visible version is found
//
// The method uses the transaction's visibility checker to determine which version
// of the row is visible based on the transaction's isolation level and snapshot.
func (t *Table) ReadVersioned(txn MVCCTransactionContext, rowID RowID) ([]any, error) {
	t.versionsMu.RLock()
	chain, exists := t.versions[rowID]
	t.versionsMu.RUnlock()

	if !exists {
		// No version chain - try regular storage for legacy rows
		t.mu.RLock()
		defer t.mu.RUnlock()

		// Check if row exists in regular storage
		loc, exists := t.rowIDMap[rowID]
		if !exists {
			return nil, fmt.Errorf("row with RowID %d does not exist", rowID)
		}

		// Check tombstone
		if t.tombstones.Get(rowID) {
			return nil, fmt.Errorf("row with RowID %d has been deleted", rowID)
		}

		// Record the read
		txn.RecordRead(t.name, uint64(rowID))

		// Return from regular storage
		if loc.rowGroupIdx >= len(t.rowGroups) {
			return nil, fmt.Errorf("invalid row group index for RowID %d", rowID)
		}
		rg := t.rowGroups[loc.rowGroupIdx]
		values := make([]any, len(rg.columns))
		for i, col := range rg.columns {
			values[i] = col.GetValue(loc.rowIdx)
		}
		return values, nil
	}

	// Find the visible version using the transaction's visibility checker
	checker := txn.VisibilityChecker()

	// We need to implement a TransactionContext adapter for the visibility check
	// Since we're in storage package and can't directly access engine.MVCCTransaction,
	// we need to use the interface-based approach with version info
	chain.RLock()
	defer chain.RUnlock()

	// Traverse from head (newest) to oldest
	current := chain.Head
	for current != nil {
		// Check if this version is visible to the transaction
		versionInfo := current.ToVersionInfo()

		// Create a minimal transaction context for visibility check
		txnCtx := &versionedTxnContext{
			txnID:             txn.ID(),
			startTS:           txn.GetStartTS(),
			commitTS:          txn.GetCommitTS(),
			visibilityChecker: checker,
		}

		if checker.IsVisible(versionInfo, txnCtx) {
			// Found visible version
			txn.RecordRead(t.name, uint64(rowID))

			// Return a copy of the data
			result := make([]any, len(current.Data))
			copy(result, current.Data)
			return result, nil
		}

		current = current.PrevPtr
	}

	return nil, fmt.Errorf("no visible version found for row with RowID %d", rowID)
}

// versionedTxnContext is a minimal TransactionContext implementation for visibility checks.
// It provides the transaction state needed by VisibilityChecker.IsVisible().
type versionedTxnContext struct {
	txnID             uint64
	startTS           uint64
	commitTS          uint64
	visibilityChecker VisibilityChecker
}

func (c *versionedTxnContext) GetTxnID() uint64 {
	return c.txnID
}

func (c *versionedTxnContext) GetIsolationLevel() parser.IsolationLevel {
	// The visibility checker already encapsulates the isolation level behavior
	return parser.IsolationLevelSerializable // Default
}

func (c *versionedTxnContext) GetStartTime() time.Time {
	// Convert timestamp to time
	return time.Unix(0, int64(c.startTS))
}

func (c *versionedTxnContext) GetStatementTime() time.Time {
	// For versioned operations, use the same as start time
	return time.Unix(0, int64(c.startTS))
}

func (c *versionedTxnContext) IsCommitted(txnID uint64) bool {
	// A transaction is committed if it has a non-zero commit timestamp
	// For our purposes, we check if the txnID matches our transaction
	// and whether we have a commit timestamp
	if txnID == c.txnID {
		return c.commitTS != 0
	}
	// For other transactions, we assume they are committed if they have
	// a commit timestamp. This is a simplification - in a full implementation,
	// this would check against a global transaction table.
	return true // Conservative: assume other transactions are committed
}

func (c *versionedTxnContext) IsAborted(txnID uint64) bool {
	// In a full implementation, this would check against a global transaction table
	return false // Conservative: assume no transactions are aborted
}

func (c *versionedTxnContext) GetSnapshot() *Snapshot {
	// For simple versioned operations, return nil (no snapshot)
	return nil
}

func (c *versionedTxnContext) WasActiveAtSnapshot(txnID uint64) bool {
	// Without a proper snapshot, we can't determine this
	return false
}

// UpdateVersioned updates a row with MVCC versioning.
// It creates a new VersionedRow with the updated values and adds it to the
// head of the version chain. The old version remains in the chain for
// snapshot isolation.
//
// Parameters:
//   - txn: The transaction context for this operation
//   - rowID: The unique identifier of the row to update
//   - values: The new column values for the row
//
// Returns:
//   - error: An error if the row does not exist or is deleted
//
// The update creates a new version at the head of the chain. The old version
// remains accessible to transactions that should see it based on their snapshot.
func (t *Table) UpdateVersioned(txn MVCCTransactionContext, rowID RowID, values []any) error {
	t.versionsMu.Lock()
	chain, exists := t.versions[rowID]
	if !exists {
		t.versionsMu.Unlock()
		return fmt.Errorf("row with RowID %d does not exist in version chain", rowID)
	}
	t.versionsMu.Unlock()

	// Check if the row has been deleted
	chain.Lock()
	head := chain.Head
	if head != nil && head.DeletedBy != 0 {
		chain.Unlock()
		return fmt.Errorf("cannot update deleted row with RowID %d", rowID)
	}

	// Create a new version with the updated values
	newVersion := &VersionedRow{
		Data:      make([]any, len(values)),
		RowID:     uint64(rowID),
		TxnID:     txn.ID(),
		CommitTS:  0, // Will be set when transaction commits
		DeletedBy: 0,
		DeleteTS:  0,
		PrevPtr:   head, // Link to previous version
	}
	copy(newVersion.Data, values)

	// Add to the head of the chain (without using AddVersion to avoid double-locking)
	chain.Head = newVersion
	chain.Unlock()

	// Record the write in the transaction for conflict detection
	txn.RecordWrite(t.name, uint64(rowID))

	// Also update regular storage for backwards compatibility
	t.mu.Lock()
	defer t.mu.Unlock()

	loc, exists := t.rowIDMap[rowID]
	if exists && loc.rowGroupIdx < len(t.rowGroups) {
		rg := t.rowGroups[loc.rowGroupIdx]
		for colIdx, value := range values {
			if colIdx < len(rg.columns) {
				rg.columns[colIdx].SetValue(loc.rowIdx, value)
			}
		}
	}

	return nil
}

// DeleteVersioned marks a row as deleted with MVCC versioning.
// It sets the DeletedBy field on the head version to the transaction ID.
// The row will no longer be visible to transactions based on their
// visibility rules.
//
// Parameters:
//   - txn: The transaction context for this operation
//   - rowID: The unique identifier of the row to delete
//
// Returns:
//   - error: An error if the row does not exist or is already deleted
//
// The delete marks the version for deletion but does not physically remove it.
// The version remains in the chain for snapshot isolation and will be cleaned
// up by the vacuum process when no active transactions need it.
func (t *Table) DeleteVersioned(txn MVCCTransactionContext, rowID RowID) error {
	t.versionsMu.Lock()
	chain, exists := t.versions[rowID]
	if !exists {
		t.versionsMu.Unlock()
		return fmt.Errorf("row with RowID %d does not exist in version chain", rowID)
	}
	t.versionsMu.Unlock()

	// Mark the head version as deleted
	chain.Lock()
	head := chain.Head
	if head == nil {
		chain.Unlock()
		return fmt.Errorf("no version found for row with RowID %d", rowID)
	}

	if head.DeletedBy != 0 {
		chain.Unlock()
		return fmt.Errorf("row with RowID %d is already deleted", rowID)
	}

	head.DeletedBy = txn.ID()
	// DeleteTS will be set when the transaction commits
	chain.Unlock()

	// Record the write in the transaction for conflict detection
	txn.RecordWrite(t.name, uint64(rowID))

	// Also mark tombstone for backwards compatibility
	t.mu.Lock()
	t.tombstones.Set(rowID, true)
	t.mu.Unlock()

	return nil
}

// CommitVersions commits all versions created by the given transaction.
// It sets the CommitTS on all versions that were created by this transaction
// and sets DeleteTS on any versions that were deleted by this transaction.
//
// Parameters:
//   - txn: The transaction context that is committing
//   - commitTS: The commit timestamp to assign to the versions
//
// This method should be called when a transaction commits to make its changes
// visible to other transactions according to their visibility rules.
func (t *Table) CommitVersions(txn MVCCTransactionContext, commitTS uint64) {
	t.versionsMu.RLock()
	defer t.versionsMu.RUnlock()

	txnID := txn.ID()

	// Iterate through all version chains
	for _, chain := range t.versions {
		chain.Lock()

		// Traverse the chain and commit versions belonging to this transaction
		current := chain.Head
		for current != nil {
			// Commit created versions
			if current.TxnID == txnID && current.CommitTS == 0 {
				current.CommitTS = commitTS
			}

			// Commit deleted versions
			if current.DeletedBy == txnID && current.DeleteTS == 0 {
				current.DeleteTS = commitTS
			}

			current = current.PrevPtr
		}

		chain.Unlock()
	}
}

// GetVersionChains returns all version chains in the table.
// This is primarily used for vacuum/garbage collection purposes.
//
// Returns:
//   - []*VersionChain: A slice of all version chains in the table
//
// The returned slice contains pointers to the actual chains, not copies.
// The caller should be careful about modifying the chains.
func (t *Table) GetVersionChains() []*VersionChain {
	t.versionsMu.RLock()
	defer t.versionsMu.RUnlock()

	chains := make([]*VersionChain, 0, len(t.versions))
	for _, chain := range t.versions {
		chains = append(chains, chain)
	}

	return chains
}
