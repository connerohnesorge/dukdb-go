// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements DuckDBWriter which coordinates
// all writing operations for creating DuckDB-compatible database files.
package duckdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// DuckDBWriter error definitions.
var (
	// ErrDuckDBWriterClosed indicates the writer has been closed.
	ErrDuckDBWriterClosed = errors.New("duckdb writer is closed")

	// ErrFileExists indicates the file already exists when creating.
	ErrFileExists = errors.New("file already exists")

	// ErrFileNotFound indicates the file does not exist when opening.
	ErrFileNotFound = errors.New("file not found")

	// ErrInvalidSchema indicates an invalid schema name was provided.
	ErrInvalidSchema = errors.New("invalid schema name")

	// ErrTableExists indicates the table already exists.
	ErrTableExists = errors.New("table already exists")

	// ErrViewExists indicates the view already exists.
	ErrViewExists = errors.New("view already exists")

	// ErrIndexExists indicates the index already exists.
	ErrIndexExists = errors.New("index already exists")

	// ErrSequenceExists indicates the sequence already exists.
	ErrSequenceExists = errors.New("sequence already exists")
)

// DuckDBWriter coordinates all writing operations for creating DuckDB-compatible
// database files. It manages the file header, catalog metadata, and row group
// data with proper checksums and format compliance.
//
// The writer maintains a catalog of database objects and writes row groups
// for tables. On checkpoint or close, all modifications are persisted to disk
// with the proper DuckDB file format structure.
type DuckDBWriter struct {
	// file is the underlying file handle.
	file *os.File

	// path is the file path.
	path string

	// blockManager handles block allocation and I/O.
	blockManager *BlockManager

	// catalog contains the database catalog metadata.
	catalog *DuckDBCatalog

	// tableRowGroups maps table OID to row group pointers.
	// The table OID is the index of the table in catalog.Tables.
	tableRowGroups map[uint64][]*RowGroupPointer

	// rowGroupWriters maps table OID to active row group writers.
	rowGroupWriters map[uint64]*RowGroupWriter

	// headerSlot is the current database header slot (1 or 2).
	// DuckDB alternates between the two slots for crash recovery.
	headerSlot int

	// iteration is incremented on each checkpoint.
	// Used to determine which header is current.
	iteration uint64

	// modified indicates whether the file has been modified since last checkpoint.
	modified bool

	// closed indicates whether the writer has been closed.
	closed bool

	// mu protects concurrent access to the writer.
	mu sync.Mutex
}

// NewDuckDBWriter creates a new DuckDB file at the specified path.
// The file must not already exist. Use OpenDuckDBWriter to open an existing file.
//
// The file is created with:
//   - File header with magic bytes and version
//   - Two database headers (for crash recovery)
//   - Empty catalog
//
// Returns an error if the file already exists or cannot be created.
func NewDuckDBWriter(path string) (*DuckDBWriter, error) {
	// Check if file exists
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileExists, path)
	}

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Initialize the file with headers
	if err := initializeDuckDBFile(file); err != nil {
		_ = file.Close()
		_ = os.Remove(path)

		return nil, fmt.Errorf("failed to initialize file: %w", err)
	}

	// Create block manager
	blockManager := NewBlockManager(file, DefaultBlockSize, DefaultCacheCapacity)

	return &DuckDBWriter{
		file:            file,
		path:            path,
		blockManager:    blockManager,
		catalog:         &DuckDBCatalog{},
		tableRowGroups:  make(map[uint64][]*RowGroupPointer),
		rowGroupWriters: make(map[uint64]*RowGroupWriter),
		headerSlot:      1,
		iteration:       1,
		modified:        false,
		closed:          false,
	}, nil
}

// OpenDuckDBWriter opens an existing DuckDB file for writing.
// The file must exist and have valid DuckDB headers.
//
// Returns an error if the file does not exist, is not a valid DuckDB file,
// or cannot be opened.
func OpenDuckDBWriter(path string) (*DuckDBWriter, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, path)
	}

	// Open the file for read/write
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Read and validate file header
	fileHeader, err := ReadFileHeader(file)
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	if err := ValidateFileHeader(fileHeader); err != nil {
		_ = file.Close()

		return nil, err
	}

	// Get the active database header
	dbHeader, slot, err := GetActiveHeader(file)
	if err != nil {
		_ = file.Close()

		return nil, err
	}

	// Validate database header
	if err := ValidateDatabaseHeader(dbHeader); err != nil {
		_ = file.Close()

		return nil, err
	}

	// Create block manager with the correct block size
	blockSize := dbHeader.BlockAllocSize
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	blockManager := NewBlockManager(file, blockSize, DefaultCacheCapacity)
	blockManager.SetBlockCount(dbHeader.BlockCount)

	// TODO: Load existing catalog from metadata blocks
	// For now, start with an empty catalog
	catalog := &DuckDBCatalog{}

	return &DuckDBWriter{
		file:            file,
		path:            path,
		blockManager:    blockManager,
		catalog:         catalog,
		tableRowGroups:  make(map[uint64][]*RowGroupPointer),
		rowGroupWriters: make(map[uint64]*RowGroupWriter),
		headerSlot:      slot,
		iteration:       dbHeader.Iteration,
		modified:        false,
		closed:          false,
	}, nil
}

// initializeDuckDBFile writes the initial headers to a new file.
func initializeDuckDBFile(file *os.File) error {
	// Write file header with magic bytes
	fileHeader := NewFileHeader()
	if err := WriteFileHeader(file, fileHeader); err != nil {
		return fmt.Errorf("failed to write file header: %w", err)
	}

	// Write initial database headers (both slots)
	dbHeader := NewDatabaseHeader()

	if err := WriteDatabaseHeader(file, dbHeader, DatabaseHeader1Offset); err != nil {
		return fmt.Errorf("failed to write database header 1: %w", err)
	}

	if err := WriteDatabaseHeader(file, dbHeader, DatabaseHeader2Offset); err != nil {
		return fmt.Errorf("failed to write database header 2: %w", err)
	}

	// Sync to ensure headers are written
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// CreateTable adds a new table to the catalog.
// The table is added immediately but not persisted until Checkpoint is called.
//
// Returns an error if the table already exists or the writer is closed.
func (w *DuckDBWriter) CreateTable(table *TableCatalogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Check if table already exists
	for _, t := range w.catalog.Tables {
		if t.Name == table.Name && t.Schema == table.Schema {
			return fmt.Errorf("%w: %s.%s", ErrTableExists, table.Schema, table.Name)
		}
	}

	w.catalog.AddTable(table)
	w.modified = true

	return nil
}

// CreateView adds a new view to the catalog.
// The view is added immediately but not persisted until Checkpoint is called.
//
// Returns an error if the view already exists or the writer is closed.
func (w *DuckDBWriter) CreateView(view *ViewCatalogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Check if view already exists
	for _, v := range w.catalog.Views {
		if v.Name == view.Name && v.Schema == view.Schema {
			return fmt.Errorf("%w: %s.%s", ErrViewExists, view.Schema, view.Name)
		}
	}

	w.catalog.AddView(view)
	w.modified = true

	return nil
}

// CreateIndex adds a new index to the catalog.
// The index is added immediately but not persisted until Checkpoint is called.
//
// Returns an error if the index already exists or the writer is closed.
func (w *DuckDBWriter) CreateIndex(index *IndexCatalogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Check if index already exists
	for _, idx := range w.catalog.Indexes {
		if idx.Name == index.Name && idx.Schema == index.Schema {
			return fmt.Errorf("%w: %s.%s", ErrIndexExists, index.Schema, index.Name)
		}
	}

	w.catalog.AddIndex(index)
	w.modified = true

	return nil
}

// CreateSequence adds a new sequence to the catalog.
// The sequence is added immediately but not persisted until Checkpoint is called.
//
// Returns an error if the sequence already exists or the writer is closed.
func (w *DuckDBWriter) CreateSequence(sequence *SequenceCatalogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Check if sequence already exists
	for _, seq := range w.catalog.Sequences {
		if seq.Name == sequence.Name && seq.Schema == sequence.Schema {
			return fmt.Errorf("%w: %s.%s", ErrSequenceExists, sequence.Schema, sequence.Name)
		}
	}

	w.catalog.AddSequence(sequence)
	w.modified = true

	return nil
}

// CreateSchema adds a new schema to the catalog.
// The schema is added immediately but not persisted until Checkpoint is called.
func (w *DuckDBWriter) CreateSchema(schema *SchemaCatalogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Check if schema already exists
	for _, s := range w.catalog.Schemas {
		if s.Name == schema.Name {
			return fmt.Errorf("schema already exists: %s", schema.Name)
		}
	}

	w.catalog.AddSchema(schema)
	w.modified = true

	return nil
}

// DropTable removes a table from the catalog.
// The table is removed immediately but the change is not persisted until Checkpoint.
//
// Returns an error if the table does not exist or the writer is closed.
func (w *DuckDBWriter) DropTable(schema, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Find and remove the table
	for i, t := range w.catalog.Tables {
		if t.Name == name && t.Schema == schema {
			// Remove the table
			w.catalog.Tables = append(w.catalog.Tables[:i], w.catalog.Tables[i+1:]...)
			w.modified = true

			// Close any active row group writer for this table
			if rgw, ok := w.rowGroupWriters[uint64(i)]; ok {
				_, _ = rgw.Close()
				delete(w.rowGroupWriters, uint64(i))
			}

			// Clear row group pointers for this table
			delete(w.tableRowGroups, uint64(i))

			return nil
		}
	}

	return fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, name)
}

// DropView removes a view from the catalog.
func (w *DuckDBWriter) DropView(schema, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	for i, v := range w.catalog.Views {
		if v.Name == name && v.Schema == schema {
			w.catalog.Views = append(w.catalog.Views[:i], w.catalog.Views[i+1:]...)
			w.modified = true
			return nil
		}
	}

	return fmt.Errorf("view not found: %s.%s", schema, name)
}

// DropIndex removes an index from the catalog.
func (w *DuckDBWriter) DropIndex(schema, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	for i, idx := range w.catalog.Indexes {
		if idx.Name == name && idx.Schema == schema {
			w.catalog.Indexes = append(w.catalog.Indexes[:i], w.catalog.Indexes[i+1:]...)
			w.modified = true
			return nil
		}
	}

	return fmt.Errorf("index not found: %s.%s", schema, name)
}

// DropSequence removes a sequence from the catalog.
func (w *DuckDBWriter) DropSequence(schema, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	for i, seq := range w.catalog.Sequences {
		if seq.Name == name && seq.Schema == schema {
			w.catalog.Sequences = append(w.catalog.Sequences[:i], w.catalog.Sequences[i+1:]...)
			w.modified = true
			return nil
		}
	}

	return fmt.Errorf("sequence not found: %s.%s", schema, name)
}

// InsertRows inserts rows into a table.
// Rows are buffered in a RowGroupWriter and flushed when the row group is full.
//
// Parameters:
//   - tableOID: The table object ID (index in catalog.Tables)
//   - rows: Slice of rows, each row is a slice of column values
//
// Returns an error if the table does not exist or the writer is closed.
func (w *DuckDBWriter) InsertRows(tableOID uint64, rows [][]any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Validate table OID
	if tableOID >= uint64(len(w.catalog.Tables)) {
		return fmt.Errorf("%w: table OID %d", ErrTableNotFound, tableOID)
	}

	// Get or create row group writer for this table
	rgw, err := w.getOrCreateRowGroupWriter(tableOID)
	if err != nil {
		return err
	}

	// Append rows to the row group writer
	for _, row := range rows {
		if err := rgw.AppendRow(row); err != nil {
			if errors.Is(err, ErrRowGroupFull) {
				// Flush the current row group
				if err := w.flushRowGroupWriter(tableOID); err != nil {
					return fmt.Errorf("failed to flush row group: %w", err)
				}

				// Create new row group writer and retry
				rgw, err = w.getOrCreateRowGroupWriter(tableOID)
				if err != nil {
					return err
				}

				if err := rgw.AppendRow(row); err != nil {
					return fmt.Errorf("failed to append row after flush: %w", err)
				}
			} else {
				return fmt.Errorf("failed to append row: %w", err)
			}
		}
	}

	w.modified = true
	return nil
}

// getOrCreateRowGroupWriter returns an existing row group writer or creates a new one.
// Must be called with w.mu held.
func (w *DuckDBWriter) getOrCreateRowGroupWriter(tableOID uint64) (*RowGroupWriter, error) {
	// Check for existing writer
	if rgw, ok := w.rowGroupWriters[tableOID]; ok {
		return rgw, nil
	}

	// Get table metadata
	table := w.catalog.Tables[tableOID]

	// Extract column types and modifiers
	types := make([]LogicalTypeID, len(table.Columns))
	mods := make([]*TypeModifiers, len(table.Columns))
	for i, col := range table.Columns {
		types[i] = col.Type
		mods[i] = &col.TypeModifiers
	}

	// Calculate row start for this table
	var rowStart uint64
	if rgs, ok := w.tableRowGroups[tableOID]; ok && len(rgs) > 0 {
		lastRG := rgs[len(rgs)-1]
		rowStart = lastRG.RowStart + lastRG.TupleCount
	}

	// Create new row group writer
	rgw := NewRowGroupWriter(w.blockManager, tableOID, types, mods, rowStart)
	w.rowGroupWriters[tableOID] = rgw

	return rgw, nil
}

// flushRowGroupWriter flushes the row group writer for a table and records the row group pointer.
// Must be called with w.mu held.
func (w *DuckDBWriter) flushRowGroupWriter(tableOID uint64) error {
	rgw, ok := w.rowGroupWriters[tableOID]
	if !ok {
		return nil // No writer to flush
	}

	// Flush the row group
	rgp, err := rgw.Flush()
	if err != nil {
		return err
	}

	if rgp != nil {
		// Record the row group pointer
		w.tableRowGroups[tableOID] = append(w.tableRowGroups[tableOID], rgp)
	}

	// Keep the same writer for continued use (it's reset after flush)
	return nil
}

// flushAllRowGroupWriters flushes all active row group writers.
// Must be called with w.mu held.
func (w *DuckDBWriter) flushAllRowGroupWriters() error {
	for tableOID, rgw := range w.rowGroupWriters {
		if rgw.RowCount() > 0 {
			rgp, err := rgw.Flush()
			if err != nil {
				return fmt.Errorf("failed to flush table %d: %w", tableOID, err)
			}
			if rgp != nil {
				w.tableRowGroups[tableOID] = append(w.tableRowGroups[tableOID], rgp)
			}
		}
	}
	return nil
}

// Checkpoint writes all pending changes to disk.
// This includes:
//  1. Flushing all row group writers
//  2. Writing the catalog with row group pointers
//  3. Updating the database header (alternating slot)
//  4. Syncing to disk
//
// Returns an error if any write operation fails.
func (w *DuckDBWriter) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	if !w.modified {
		return nil // Nothing to checkpoint
	}

	return w.checkpointLocked()
}

// checkpointLocked performs the checkpoint operation.
// Must be called with w.mu held.
func (w *DuckDBWriter) checkpointLocked() error {
	// 1. Flush all row group writers
	if err := w.flushAllRowGroupWriters(); err != nil {
		return fmt.Errorf("failed to flush row groups: %w", err)
	}

	// 2. Write catalog with row group pointers
	catalogWriter := NewCatalogWriter(w.blockManager, w.catalog)
	catalogWriter.SetDuckDBCompatMode(true) // Enable BinarySerializer for DuckDB CLI compatibility
	for tableOID, rgps := range w.tableRowGroups {
		for _, rgp := range rgps {
			if err := catalogWriter.AddRowGroupPointer(tableOID, rgp); err != nil {
				return fmt.Errorf("failed to add row group pointer: %w", err)
			}
		}
	}

	metaBlock, err := catalogWriter.Write()
	if err != nil {
		return fmt.Errorf("failed to write catalog: %w", err)
	}

	// 3. Update database header (alternate slot)
	w.headerSlot = GetNextHeaderSlot(w.headerSlot)
	w.iteration++

	dbHeader := &DatabaseHeader{
		Iteration:                  w.iteration,
		MetaBlock:                  metaBlock.BlockID,
		FreeList:                   InvalidBlockID,
		BlockCount:                 w.blockManager.BlockCount(),
		BlockAllocSize:             w.blockManager.BlockSize(),
		VectorSize:                 DefaultVectorSize,
		SerializationCompatibility: SerializationCompatibilityVersion,
	}

	offset := GetHeaderOffset(w.headerSlot)
	if err := WriteDatabaseHeader(w.file, dbHeader, offset); err != nil {
		return fmt.Errorf("failed to write database header: %w", err)
	}

	// 4. Sync to disk
	if err := w.blockManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync block manager: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	w.modified = false
	return nil
}

// Close checkpoints any pending changes and closes the file.
// After Close is called, the writer cannot be used.
//
// It is safe to call Close multiple times.
func (w *DuckDBWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Checkpoint if modified
	if w.modified {
		if err := w.checkpointLocked(); err != nil {
			// Still mark as closed to prevent further use
			w.closed = true
			_ = w.file.Close()

			return fmt.Errorf("failed to checkpoint: %w", err)
		}
	}

	// Close row group writers
	for _, rgw := range w.rowGroupWriters {
		_, _ = rgw.Close()
	}
	w.rowGroupWriters = nil

	// Close block manager
	if err := w.blockManager.Close(); err != nil {
		w.closed = true
		_ = w.file.Close()

		return fmt.Errorf("failed to close block manager: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		w.closed = true
		return fmt.Errorf("failed to close file: %w", err)
	}

	w.closed = true
	return nil
}

// Path returns the file path.
func (w *DuckDBWriter) Path() string {
	return w.path
}

// Catalog returns the current catalog.
// Note: Modifications to the returned catalog should be done through
// CreateTable, CreateView, etc. methods.
func (w *DuckDBWriter) Catalog() *DuckDBCatalog {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.catalog
}

// IsModified returns true if the file has been modified since the last checkpoint.
func (w *DuckDBWriter) IsModified() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.modified
}

// IsClosed returns true if the writer has been closed.
func (w *DuckDBWriter) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.closed
}

// TableCount returns the number of tables in the catalog.
func (w *DuckDBWriter) TableCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.catalog.Tables)
}

// GetTableOID returns the OID for a table by name and schema.
// Returns -1 if the table is not found.
func (w *DuckDBWriter) GetTableOID(schema, name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i, t := range w.catalog.Tables {
		if t.Name == name && t.Schema == schema {
			return i
		}
	}
	return -1
}

// GetTable returns the table with the given name, or nil if not found.
func (w *DuckDBWriter) GetTable(name string) *TableCatalogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.catalog.GetTable(name)
}

// Iteration returns the current checkpoint iteration number.
func (w *DuckDBWriter) Iteration() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.iteration
}

// HeaderSlot returns the current database header slot (1 or 2).
func (w *DuckDBWriter) HeaderSlot() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.headerSlot
}

// BlockCount returns the total number of allocated blocks.
func (w *DuckDBWriter) BlockCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.blockManager.BlockCount()
}

// RowGroupCount returns the total number of row groups for a table.
func (w *DuckDBWriter) RowGroupCount(tableOID uint64) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	if rgs, ok := w.tableRowGroups[tableOID]; ok {
		return len(rgs)
	}
	return 0
}

// TotalRowCount returns the total number of rows for a table.
func (w *DuckDBWriter) TotalRowCount(tableOID uint64) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	var count uint64

	// Count rows in flushed row groups
	if rgs, ok := w.tableRowGroups[tableOID]; ok {
		for _, rg := range rgs {
			count += rg.TupleCount
		}
	}

	// Count rows in active row group writer
	if rgw, ok := w.rowGroupWriters[tableOID]; ok {
		count += rgw.RowCount()
	}

	return count
}

// Flush flushes any buffered row data without a full checkpoint.
// This writes row groups but does not update the database header.
// Use Checkpoint for a full durable write.
func (w *DuckDBWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	return w.flushAllRowGroupWriters()
}

// ForceCheckpoint forces a checkpoint even if there are no modifications.
// This is useful for testing or ensuring data is persisted.
func (w *DuckDBWriter) ForceCheckpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrDuckDBWriterClosed
	}

	// Mark as modified to force checkpoint
	w.modified = true
	return w.checkpointLocked()
}

// CreateTableSimple is a convenience method to create a table with columns.
// It creates a TableCatalogEntry and adds it to the catalog.
func (w *DuckDBWriter) CreateTableSimple(name string, columns []ColumnDefinition) error {
	table := NewTableCatalogEntry(name)
	for _, col := range columns {
		table.AddColumn(col)
	}
	return w.CreateTable(table)
}

// Create is a convenience function that creates a new DuckDB file.
// It is an alias for NewDuckDBWriter.
func Create(path string) (*DuckDBWriter, error) {
	return NewDuckDBWriter(path)
}

// Open is a convenience function that opens an existing DuckDB file for writing.
// It is an alias for OpenDuckDBWriter.
func Open(path string) (*DuckDBWriter, error) {
	return OpenDuckDBWriter(path)
}
