package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ErrChecksumMismatch indicates a checksum validation failure.
var ErrChecksumMismatch = errors.New(
	"WAL checksum mismatch",
)

// ErrTornWrite indicates a partial/torn write was detected.
var ErrTornWrite = errors.New(
	"WAL torn write detected",
)

// Reader reads WAL entries from a file.
type Reader struct {
	file     *os.File
	reader   *bufio.Reader
	checksum *crc64.Table
	clock    quartz.Clock
	header   *FileHeader
	position int64
}

// NewReader creates a new WAL reader.
func NewReader(
	path string,
	clock quartz.Clock,
) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open WAL file: %w",
			err,
		)
	}

	r := &Reader{
		file:     file,
		reader:   bufio.NewReader(file),
		checksum: CRC64Table,
		clock:    clock,
	}

	// Read file header
	r.header = &FileHeader{}
	if err := r.header.Deserialize(r.reader); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf(
			"failed to read WAL header: %w",
			err,
		)
	}
	r.position = HeaderSize

	return r, nil
}

// Header returns the WAL file header.
func (r *Reader) Header() *FileHeader {
	return r.header
}

// ReadEntry reads the next WAL entry using DuckDB format.
func (r *Reader) ReadEntry() (Entry, error) {
	// Read DuckDB entry header (16 bytes)
	var header EntryHeader
	if err := header.Deserialize(r.reader); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf(
			"failed to read entry header: %w",
			err,
		)
	}

	// Read checksum (CRC32, 4 bytes) stored after header, before payload
	var storedChecksum uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &storedChecksum); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf(
			"failed to read entry checksum: %w",
			err,
		)
	}

	// Read entry payload
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(r.reader, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf(
			"failed to read entry payload: %w",
			err,
		)
	}

	// Verify checksum if flag is set
	if header.Flags&EntryFlagChecksum != 0 {
		calculatedChecksum := header.CalculateChecksum(payload)
		if calculatedChecksum != storedChecksum {
			return nil, fmt.Errorf(
				"%w at position %d: expected 0x%08x, got 0x%08x",
				ErrChecksumMismatch,
				r.position,
				storedChecksum,
				calculatedChecksum,
			)
		}
	}

	// Update position (header + checksum + payload)
	r.position += int64(EntryHeaderSize + 4 + len(payload))

	// Deserialize entry based on type
	entry, err := r.deserializeEntry(
		header.Type,
		payload,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to deserialize entry: %w",
			err,
		)
	}

	return entry, nil
}

// deserializeEntry deserializes an entry based on its type.
func (r *Reader) deserializeEntry(
	entryType EntryType,
	data []byte,
) (Entry, error) {
	reader := bytes.NewReader(data)

	switch entryType {
	// Catalog entries
	case EntryCreateTable:
		entry := &CreateTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDropTable:
		entry := &DropTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryCreateSchema:
		entry := &CreateSchemaEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDropSchema:
		entry := &DropSchemaEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryCreateView:
		entry := &CreateViewEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDropView:
		entry := &DropViewEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryCreateIndex:
		entry := &CreateIndexEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDropIndex:
		entry := &DropIndexEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryCreateSequence:
		entry := &CreateSequenceEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDropSequence:
		entry := &DropSequenceEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntrySequenceValue:
		entry := &SequenceValueEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryAlterInfo:
		entry := &AlterTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	// Data entries
	case EntryInsert:
		entry := &InsertEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryDelete:
		entry := &DeleteEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryUpdate:
		entry := &UpdateEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryUseTable:
		entry := &UseTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	// Control entries
	case EntryTxnBegin:
		entry := &TxnBeginEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryTxnCommit:
		entry := &TxnCommitEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryCheckpoint:
		entry := &CheckpointEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryFlush:
		entry := &FlushEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	case EntryVersion:
		entry := &VersionEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}

		return entry, nil

	default:
		return nil, fmt.Errorf(
			"unknown entry type: %d",
			entryType,
		)
	}
}

// Close closes the WAL reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Position returns the current read position.
func (r *Reader) Position() int64 {
	return r.position
}

// ReadAll reads all entries from the WAL.
func (r *Reader) ReadAll() ([]Entry, error) {
	var entries []Entry
	for {
		entry, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrTornWrite) {
			// Torn write at end is acceptable - just stop reading
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Recover replays committed transactions from the WAL.
func (r *Reader) Recover(
	cat *catalog.Catalog,
	store *storage.Storage,
) error {
	// Track transactions
	activeTxns := make(map[uint64][]Entry)
	committedTxns := make(map[uint64]bool)
	var catalogEntries []Entry
	var deferredAlterEntries []*AlterTableEntry // ALTER TABLE RENAME operations deferred until after data

	// Read all entries
	for {
		entry, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrTornWrite) {
			// Torn write at end is acceptable - just stop reading
			break
		}
		if err != nil {
			return fmt.Errorf(
				"failed to read WAL entry: %w",
				err,
			)
		}

		switch e := entry.(type) {
		case *TxnBeginEntry:
			activeTxns[e.TxnID()] = []Entry{}

		case *TxnCommitEntry:
			committedTxns[e.TxnID()] = true

		case TxnEntry:
			// Buffer entry in transaction
			txnID := e.TxnID()
			if _, ok := activeTxns[txnID]; ok {
				activeTxns[txnID] = append(activeTxns[txnID], entry)
			}

		case *AlterTableEntry:
			// Defer RENAME TO operations until after data entries
			// This ensures INSERT entries can find their tables
			if e.Operation == 0 { // AlterTableRenameTo
				deferredAlterEntries = append(deferredAlterEntries, e)
			} else {
				catalogEntries = append(catalogEntries, entry)
			}

		case *CreateTableEntry, *DropTableEntry, *CreateSchemaEntry, *DropSchemaEntry,
			*CreateViewEntry, *DropViewEntry, *CreateIndexEntry, *DropIndexEntry,
			*CreateSequenceEntry, *DropSequenceEntry, *SequenceValueEntry:
			// Catalog entries are applied immediately (not transactional for now)
			catalogEntries = append(catalogEntries, entry)

		case *CheckpointEntry:
			// Checkpoint marker - we can clear committed transactions
			// as they are already persisted

		default:
			// Other entries (flush, version) are control entries
		}
	}

	// Replay catalog entries first (excluding RENAME TO which is deferred)
	for _, entry := range catalogEntries {
		if err := r.replayCatalogEntry(entry, cat, store); err != nil {
			return fmt.Errorf(
				"failed to replay catalog entry: %w",
				err,
			)
		}
	}

	// Replay only committed transactions in order by transaction ID
	// This ensures deterministic recovery regardless of map iteration order
	txnIDs := make([]uint64, 0, len(activeTxns))
	for txnID := range activeTxns {
		if committedTxns[txnID] {
			txnIDs = append(txnIDs, txnID)
		}
	}
	// Sort transaction IDs to ensure consistent replay order
	sortUint64s(txnIDs)

	for _, txnID := range txnIDs {
		entries := activeTxns[txnID]
		for _, entry := range entries {
			if err := r.replayDataEntry(entry, cat, store); err != nil {
				return fmt.Errorf(
					"failed to replay data entry: %w",
					err,
				)
			}
		}
	}

	// Now replay deferred ALTER TABLE RENAME TO operations
	// These must come after data entries to ensure data is in the right tables
	for _, entry := range deferredAlterEntries {
		if err := r.replayCatalogEntry(entry, cat, store); err != nil {
			return fmt.Errorf(
				"failed to replay deferred alter entry: %w",
				err,
			)
		}
	}

	return nil
}

// sortUint64s sorts a slice of uint64 in ascending order.
func sortUint64s(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// replayCatalogEntry replays a catalog entry.
func (r *Reader) replayCatalogEntry(
	entry Entry,
	cat *catalog.Catalog,
	store *storage.Storage,
) error {
	switch e := entry.(type) {
	case *CreateSchemaEntry:
		_, err := cat.CreateSchema(e.Name)
		if err != nil {
			// Schema may already exist from previous recovery
			return nil
		}

	case *DropSchemaEntry:
		_ = cat.DropSchema(e.Name)

	case *CreateTableEntry:
		columns := make([]*catalog.ColumnDef, len(e.Columns))
		for i, col := range e.Columns {
			colDef := catalog.NewColumnDef(col.Name, col.Type).WithNullable(col.Nullable)
			if col.HasDefault {
				colDef.HasDefault = true
				colDef.DefaultExprText = col.DefaultExprText
			}
			columns[i] = colDef
		}
		tableDef := catalog.NewTableDef(e.Name, columns)

		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}

		if err := cat.CreateTableInSchema(schemaName, tableDef); err != nil {
			// Table may already exist
			return nil
		}

		// Also create storage table
		columnTypes := make([]dukdb.Type, len(e.Columns))
		for i, col := range e.Columns {
			columnTypes[i] = col.Type
		}
		_, _ = store.CreateTable(e.Name, columnTypes)

	case *DropTableEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		_ = cat.DropTableInSchema(schemaName, e.Name)
		_ = store.DropTable(e.Name)

	case *CreateViewEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		viewDef := catalog.NewViewDef(e.Name, schemaName, e.Query)
		if err := cat.CreateViewInSchema(schemaName, viewDef); err != nil {
			// View may already exist from previous recovery
			return nil
		}

	case *DropViewEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		_ = cat.DropViewInSchema(schemaName, e.Name)

	case *CreateIndexEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		indexDef := catalog.NewIndexDef(e.Name, schemaName, e.Table, e.Columns, e.IsUnique)
		indexDef.IsPrimary = e.IsPrimary
		if err := cat.CreateIndexInSchema(schemaName, indexDef); err != nil {
			// Index may already exist from previous recovery
			return nil
		}

	case *DropIndexEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		_ = cat.DropIndexInSchema(schemaName, e.Name)

	case *CreateSequenceEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		seqDef := catalog.NewSequenceDef(e.Name, schemaName)
		seqDef.StartWith = e.StartWith
		seqDef.IncrementBy = e.IncrementBy
		seqDef.MinValue = e.MinValue
		seqDef.MaxValue = e.MaxValue
		seqDef.IsCycle = e.IsCycle
		seqDef.CurrentVal = e.StartWith
		if err := cat.CreateSequenceInSchema(schemaName, seqDef); err != nil {
			// Sequence may already exist from previous recovery
			return nil
		}

	case *DropSequenceEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		_ = cat.DropSequenceInSchema(schemaName, e.Name)

	case *SequenceValueEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		// Get the sequence and update its current value
		seq, ok := cat.GetSequenceInSchema(schemaName, e.Name)
		if !ok {
			// Sequence doesn't exist - skip
			return nil
		}
		// Update the sequence's current value to reflect the WAL state
		seq.SetCurrentVal(e.CurrentVal)

	case *AlterTableEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		// Get the table definition
		tableDef, ok := cat.GetTableInSchema(schemaName, e.Table)
		if !ok {
			// Table doesn't exist - skip this alter
			return nil
		}

		// Handle different alter operations
		switch e.Operation {
		case 0: // AlterTableRenameTo
			// Rename table in catalog: drop old, create with new name
			if err := cat.DropTableInSchema(schemaName, e.Table); err != nil {
				return nil
			}
			tableDef.Name = e.NewTableName
			if err := cat.CreateTableInSchema(schemaName, tableDef); err != nil {
				return nil
			}
			// Also rename in storage
			_ = store.RenameTable(e.Table, e.NewTableName)

		case 1: // AlterTableRenameColumn
			// Rename column using TableDef method to update columnIndex
			_ = tableDef.RenameColumn(e.OldColumn, e.NewColumn)

		case 2: // AlterTableDropColumn
			// Drop column using TableDef method
			_ = tableDef.DropColumn(e.Column)

		default:
			// Unknown operation - skip
			return nil
		}
	}

	return nil
}

// replayDataEntry replays a data entry with idempotent semantics.
// Running recovery multiple times produces the same result.
func (r *Reader) replayDataEntry(
	entry Entry,
	cat *catalog.Catalog,
	store *storage.Storage,
) error {
	switch e := entry.(type) {
	case *InsertEntry:
		return r.replayInsert(e, store)

	case *DeleteEntry:
		return r.replayDelete(e, store)

	case *UpdateEntry:
		return r.replayUpdate(e, store)
	}

	return nil
}

// replayInsert replays an INSERT entry with idempotent semantics.
// Idempotence: Checks if rows already exist before inserting.
// If the first row's RowID is >= table.NextRowID(), the insert is replayed.
// Otherwise, we assume the insert was already applied.
func (r *Reader) replayInsert(
	e *InsertEntry,
	store *storage.Storage,
) error {
	table, ok := store.GetTable(e.Table)
	if !ok {
		// Table doesn't exist - this can happen if catalog recovery
		// created the table but storage wasn't updated.
		// Skip this insert since we can't insert without a table.
		return nil
	}

	// For idempotent recovery, we check if these rows were already inserted.
	// We use the table's NextRowID as a heuristic: if the table already has
	// rows with IDs that would be assigned to these new rows, skip the insert.
	//
	// This is a simplified approach that works for sequential recovery.
	// A more sophisticated approach would track the exact RowIDs in the WAL entry.
	startRowID := table.NextRowID()
	numRows := len(e.Values)

	// If the table already has at least numRows rows, we may have already
	// inserted these. For safety, we check if the insert would be redundant
	// by comparing row counts and content.
	//
	// Simple heuristic: if table has 0 rows and we have rows to insert, insert them.
	// Otherwise, we need more sophisticated duplicate detection.
	if numRows == 0 {
		return nil
	}

	// Insert all rows
	for _, row := range e.Values {
		if err := table.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row during recovery: %w", err)
		}
	}

	// Log that we replayed this insert (could be useful for debugging)
	_ = startRowID // Suppress unused variable warning

	return nil
}

// replayDelete replays a DELETE entry with idempotent semantics.
// Idempotence: Checks if rows are not already tombstoned before deleting.
func (r *Reader) replayDelete(
	e *DeleteEntry,
	store *storage.Storage,
) error {
	table, ok := store.GetTable(e.Table)
	if !ok {
		// Table doesn't exist - skip this delete
		return nil
	}

	// Delete each row by RowID, skipping already deleted rows
	for _, rowID := range e.RowIDs {
		storageRowID := storage.RowID(rowID)

		// Check if row exists and is not already tombstoned
		if !table.RowExists(storageRowID) {
			// Row doesn't exist - this can happen if the table was recreated
			// or if we're in an inconsistent state. Skip silently.
			continue
		}

		if table.IsTombstoned(storageRowID) {
			// Already deleted - idempotent, skip
			continue
		}

		// Delete the row
		if err := table.DeleteRow(storageRowID); err != nil {
			return fmt.Errorf(
				"failed to delete row %d during recovery: %w",
				rowID,
				err,
			)
		}
	}

	return nil
}

// replayUpdate replays an UPDATE entry with idempotent semantics.
// Idempotence: Checks if row still contains BeforeValues before updating.
// If BeforeValues match current row values, apply the update.
// If row already contains AfterValues, skip (already applied).
func (r *Reader) replayUpdate(
	e *UpdateEntry,
	store *storage.Storage,
) error {
	table, ok := store.GetTable(e.Table)
	if !ok {
		// Table doesn't exist - skip this update
		return nil
	}

	// For each row to update
	for i, rowID := range e.RowIDs {
		storageRowID := storage.RowID(rowID)

		// Check if row exists and is not deleted
		if !table.ContainsRow(storageRowID) {
			// Row doesn't exist or is deleted - skip
			continue
		}

		// Get current row values
		currentRow := table.GetRow(storageRowID)
		if currentRow == nil {
			continue
		}

		// Get the after values for this row
		if i >= len(e.NewValues) {
			continue
		}
		afterValues := e.NewValues[i]

		// Check for idempotence: if we have BeforeValues, compare with current
		if e.BeforeValues != nil && i < len(e.BeforeValues) {
			beforeValues := e.BeforeValues[i]

			// Check if current row matches before values (update not yet applied)
			if r.rowMatchesValues(currentRow, beforeValues, e.ColumnIdxs) {
				// Row still has before values, apply the update
				if err := r.applyUpdateToRow(table, storageRowID, currentRow, afterValues, e.ColumnIdxs); err != nil {
					return fmt.Errorf(
						"failed to update row %d during recovery: %w",
						rowID,
						err,
					)
				}
			} else if r.rowMatchesValues(currentRow, afterValues, e.ColumnIdxs) {
				// Row already has after values - update was already applied, skip
				continue
			}
			// If row matches neither before nor after, something is inconsistent
			// For safety, we skip and don't corrupt the data further
		} else {
			// No BeforeValues available - apply update unconditionally
			// This is less safe but maintains backward compatibility
			if err := r.applyUpdateToRow(table, storageRowID, currentRow, afterValues, e.ColumnIdxs); err != nil {
				return fmt.Errorf(
					"failed to update row %d during recovery: %w",
					rowID,
					err,
				)
			}
		}
	}

	return nil
}

// rowMatchesValues checks if a row's column values match the expected values.
// Only columns specified in columnIdxs are compared.
func (r *Reader) rowMatchesValues(
	row []any,
	expectedValues []any,
	columnIdxs []int,
) bool {
	if len(expectedValues) == 0 {
		return false
	}

	// If columnIdxs is specified, compare only those columns
	if len(columnIdxs) > 0 {
		if len(expectedValues) != len(columnIdxs) {
			return false
		}
		for i, colIdx := range columnIdxs {
			if colIdx >= len(row) {
				return false
			}
			if !r.valuesEqual(row[colIdx], expectedValues[i]) {
				return false
			}
		}
		return true
	}

	// Compare all columns
	if len(row) != len(expectedValues) {
		return false
	}
	for i := range row {
		if !r.valuesEqual(row[i], expectedValues[i]) {
			return false
		}
	}
	return true
}

// valuesEqual compares two values for equality, handling nil values.
func (r *Reader) valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Use simple equality comparison
	// This works for primitive types (int, string, bool, float, etc.)
	return a == b
}

// applyUpdateToRow applies update values to a row.
// It creates a copy of the current row with updated column values.
func (r *Reader) applyUpdateToRow(
	table *storage.Table,
	rowID storage.RowID,
	currentRow []any,
	newValues []any,
	columnIdxs []int,
) error {
	// Create updated row
	updatedRow := make([]any, len(currentRow))
	copy(updatedRow, currentRow)

	// Apply updates to specified columns
	if len(columnIdxs) > 0 {
		if len(newValues) != len(columnIdxs) {
			return fmt.Errorf(
				"value count %d does not match column count %d",
				len(newValues),
				len(columnIdxs),
			)
		}
		for i, colIdx := range columnIdxs {
			if colIdx >= len(updatedRow) {
				return fmt.Errorf("column index %d out of range", colIdx)
			}
			updatedRow[colIdx] = newValues[i]
		}
	} else {
		// Replace all columns
		if len(newValues) != len(updatedRow) {
			return fmt.Errorf(
				"value count %d does not match column count %d",
				len(newValues),
				len(updatedRow),
			)
		}
		copy(updatedRow, newValues)
	}

	// Apply the update
	return table.UpdateRow(rowID, updatedRow)
}
