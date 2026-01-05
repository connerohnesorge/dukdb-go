// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements CatalogWriter for serializing
// catalog metadata to DuckDB format blocks.
package duckdb

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// CatalogWriter error definitions.
var (
	// ErrCatalogWriterClosed indicates the catalog writer has been closed.
	ErrCatalogWriterClosed = errors.New("catalog writer is closed")

	// ErrEmptyCatalog indicates the catalog has no entries to write.
	ErrEmptyCatalog = errors.New("catalog is empty")

	// ErrTableNotFound indicates a table was not found in the catalog.
	ErrTableNotFound = errors.New("table not found in catalog")

	// ErrInvalidCatalogEntry indicates an invalid catalog entry was encountered.
	ErrInvalidCatalogEntry = errors.New("invalid catalog entry")
)

// AddSchema adds a schema entry to the catalog.
func (c *DuckDBCatalog) AddSchema(schema *SchemaCatalogEntry) {
	c.Schemas = append(c.Schemas, schema)
}

// AddTable adds a table entry to the catalog.
func (c *DuckDBCatalog) AddTable(table *TableCatalogEntry) {
	c.Tables = append(c.Tables, table)
}

// AddView adds a view entry to the catalog.
func (c *DuckDBCatalog) AddView(view *ViewCatalogEntry) {
	c.Views = append(c.Views, view)
}

// AddIndex adds an index entry to the catalog.
func (c *DuckDBCatalog) AddIndex(index *IndexCatalogEntry) {
	c.Indexes = append(c.Indexes, index)
}

// AddSequence adds a sequence entry to the catalog.
func (c *DuckDBCatalog) AddSequence(sequence *SequenceCatalogEntry) {
	c.Sequences = append(c.Sequences, sequence)
}

// AddType adds a custom type entry to the catalog.
func (c *DuckDBCatalog) AddType(typeEntry *TypeCatalogEntry) {
	c.Types = append(c.Types, typeEntry)
}

// EntryCount returns the total number of catalog entries.
func (c *DuckDBCatalog) EntryCount() int {
	return len(c.Schemas) + len(c.Tables) + len(c.Views) +
		len(c.Indexes) + len(c.Sequences) + len(c.Types)
}

// IsEmpty returns true if the catalog has no entries.
func (c *DuckDBCatalog) IsEmpty() bool {
	return c.EntryCount() == 0
}

// GetTable returns the table with the given name, or nil if not found.
func (c *DuckDBCatalog) GetTable(name string) *TableCatalogEntry {
	for _, t := range c.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// GetTableByOID returns the table with the given OID, or nil if not found.
// OID is determined by the table's position in the Tables slice.
func (c *DuckDBCatalog) GetTableByOID(oid uint64) *TableCatalogEntry {
	if oid < uint64(len(c.Tables)) {
		return c.Tables[oid]
	}
	return nil
}

// tableRowGroupEntry tracks row group pointers for a specific table.
type tableRowGroupEntry struct {
	tableOID  uint64
	rowGroups []*RowGroupPointer
}

// CatalogWriter writes catalog metadata to DuckDB format blocks.
// It serializes all catalog entries (schemas, tables, views, indexes,
// sequences, types) to metadata blocks and creates a master catalog
// index block that the database header points to.
type CatalogWriter struct {
	// blockManager handles block allocation and I/O.
	blockManager *BlockManager

	// catalog is the catalog to serialize.
	catalog *DuckDBCatalog

	// metaBlocks tracks written metadata block IDs.
	metaBlocks []uint64

	// tableRowGroups maps table OID to row group pointers.
	tableRowGroups map[uint64]*tableRowGroupEntry

	// closed indicates whether the writer has been closed.
	closed bool

	// mu protects concurrent access.
	mu sync.Mutex
}

// NewCatalogWriter creates a new CatalogWriter for serializing catalog metadata.
//
// Parameters:
//   - bm: BlockManager for block allocation and I/O
//   - catalog: The DuckDBCatalog to serialize
func NewCatalogWriter(bm *BlockManager, catalog *DuckDBCatalog) *CatalogWriter {
	return &CatalogWriter{
		blockManager:   bm,
		catalog:        catalog,
		metaBlocks:     make([]uint64, 0),
		tableRowGroups: make(map[uint64]*tableRowGroupEntry),
		closed:         false,
	}
}

// AddRowGroupPointer registers a row group for a table.
// Row group pointers are stored separately from table catalog entries
// and are written to their own metadata blocks.
//
// Parameters:
//   - tableOID: The table object ID (index in catalog.Tables)
//   - rgp: The row group pointer to register
func (w *CatalogWriter) AddRowGroupPointer(tableOID uint64, rgp *RowGroupPointer) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrCatalogWriterClosed
	}

	// Verify table exists
	if tableOID >= uint64(len(w.catalog.Tables)) {
		return fmt.Errorf("%w: table OID %d does not exist", ErrTableNotFound, tableOID)
	}

	// Get or create table row group entry
	entry, exists := w.tableRowGroups[tableOID]
	if !exists {
		entry = &tableRowGroupEntry{
			tableOID:  tableOID,
			rowGroups: make([]*RowGroupPointer, 0),
		}
		w.tableRowGroups[tableOID] = entry
	}

	entry.rowGroups = append(entry.rowGroups, rgp)
	return nil
}

// Write serializes the entire catalog to metadata blocks.
// Returns the MetaBlockPointer for the database header, which points
// to the catalog index block containing references to all catalog entries.
//
// The write process:
//  1. Write all schemas
//  2. Write all tables with row group pointers
//  3. Write all views
//  4. Write all indexes
//  5. Write all sequences
//  6. Write all custom types
//  7. Write catalog index block pointing to all entries
func (w *CatalogWriter) Write() (MetaBlockPointer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return MetaBlockPointer{}, ErrCatalogWriterClosed
	}

	// 1. Write schemas
	for _, schema := range w.catalog.Schemas {
		if err := w.writeSchemaEntry(schema); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write schema %s: %w", schema.Name, err)
		}
	}

	// 2. Write tables with row group pointers
	for i, table := range w.catalog.Tables {
		tableOID := uint64(i)
		if err := w.writeTableEntry(table, tableOID); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write table %s: %w", table.Name, err)
		}
	}

	// 3. Write views
	for _, view := range w.catalog.Views {
		if err := w.writeViewEntry(view); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write view %s: %w", view.Name, err)
		}
	}

	// 4. Write indexes
	for _, index := range w.catalog.Indexes {
		if err := w.writeIndexEntry(index); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write index %s: %w", index.Name, err)
		}
	}

	// 5. Write sequences
	for _, sequence := range w.catalog.Sequences {
		if err := w.writeSequenceEntry(sequence); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write sequence %s: %w", sequence.Name, err)
		}
	}

	// 6. Write custom types
	for _, typeEntry := range w.catalog.Types {
		if err := w.writeTypeEntry(typeEntry); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write type %s: %w", typeEntry.Name, err)
		}
	}

	// 7. Write catalog index block
	return w.writeCatalogIndex()
}

// writeEntry writes a single catalog entry to a metadata block.
// It serializes the entry, allocates a block, writes the data,
// and tracks the block ID.
func (w *CatalogWriter) writeEntry(entry CatalogEntry) error {
	// Serialize entry
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	if err := SerializeCatalogEntry(bw, entry); err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Allocate block and write
	blockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return fmt.Errorf("failed to allocate block: %w", err)
	}

	block := &Block{
		ID:   blockID,
		Type: BlockMetaData,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(block.Data, buf.Bytes())

	if err := w.blockManager.WriteBlock(block); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	w.metaBlocks = append(w.metaBlocks, blockID)
	return nil
}

// writeSchemaEntry writes a schema catalog entry.
func (w *CatalogWriter) writeSchemaEntry(schema *SchemaCatalogEntry) error {
	return w.writeEntry(schema)
}

// writeViewEntry writes a view catalog entry.
func (w *CatalogWriter) writeViewEntry(view *ViewCatalogEntry) error {
	return w.writeEntry(view)
}

// writeIndexEntry writes an index catalog entry.
func (w *CatalogWriter) writeIndexEntry(index *IndexCatalogEntry) error {
	return w.writeEntry(index)
}

// writeSequenceEntry writes a sequence catalog entry.
func (w *CatalogWriter) writeSequenceEntry(sequence *SequenceCatalogEntry) error {
	return w.writeEntry(sequence)
}

// writeTypeEntry writes a custom type catalog entry.
func (w *CatalogWriter) writeTypeEntry(typeEntry *TypeCatalogEntry) error {
	return w.writeEntry(typeEntry)
}

// writeTableEntry writes table metadata including row group pointers.
// Tables need special handling to include row group metadata that points
// to where the table data is stored.
func (w *CatalogWriter) writeTableEntry(table *TableCatalogEntry, tableOID uint64) error {
	// Serialize table entry
	var tableBuf bytes.Buffer
	tableWriter := NewBinaryWriter(&tableBuf)

	if err := table.Serialize(tableWriter); err != nil {
		return fmt.Errorf("failed to serialize table: %w", err)
	}

	// Serialize row group pointers for this table
	rowGroupBlockID, err := w.writeRowGroupPointers(tableOID)
	if err != nil {
		return fmt.Errorf("failed to write row groups: %w", err)
	}

	// Create combined buffer with table data and row group pointer
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// Write the table entry data
	bw.WriteBytes(tableBuf.Bytes())

	// Write the row group metadata block ID (or 0 if no row groups)
	bw.WritePropertyID(PropTableStorage)
	bw.WriteUint64(rowGroupBlockID)

	// Allocate block and write
	blockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return fmt.Errorf("failed to allocate block: %w", err)
	}

	block := &Block{
		ID:   blockID,
		Type: BlockMetaData,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(block.Data, buf.Bytes())

	if err := w.blockManager.WriteBlock(block); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	w.metaBlocks = append(w.metaBlocks, blockID)
	return nil
}

// writeRowGroupPointers writes all row group pointers for a table to a metadata block.
// Returns the block ID where row group pointers are stored, or 0 if no row groups.
func (w *CatalogWriter) writeRowGroupPointers(tableOID uint64) (uint64, error) {
	entry, exists := w.tableRowGroups[tableOID]
	if !exists || len(entry.rowGroups) == 0 {
		return 0, nil // No row groups for this table
	}

	// Serialize all row group pointers
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// Write number of row groups
	bw.WriteUint64(uint64(len(entry.rowGroups)))

	// Write each row group pointer
	for _, rgp := range entry.rowGroups {
		if err := rgp.Serialize(bw); err != nil {
			return 0, fmt.Errorf("failed to serialize row group pointer: %w", err)
		}
	}

	if bw.Err() != nil {
		return 0, bw.Err()
	}

	// Allocate block and write
	blockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate block for row groups: %w", err)
	}

	block := &Block{
		ID:   blockID,
		Type: BlockMetaData,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(block.Data, buf.Bytes())

	if err := w.blockManager.WriteBlock(block); err != nil {
		return 0, fmt.Errorf("failed to write row group block: %w", err)
	}

	return blockID, nil
}

// writeCatalogIndex writes the master catalog index block.
// This block is what the database header MetaBlock points to.
// It contains references to all catalog entry blocks.
func (w *CatalogWriter) writeCatalogIndex() (MetaBlockPointer, error) {
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// Write catalog version/magic for validation
	bw.WriteUint32(uint32(CurrentVersion))

	// Write counts for each entry type (for efficient reading)
	bw.WriteUint32(uint32(len(w.catalog.Schemas)))
	bw.WriteUint32(uint32(len(w.catalog.Tables)))
	bw.WriteUint32(uint32(len(w.catalog.Views)))
	bw.WriteUint32(uint32(len(w.catalog.Indexes)))
	bw.WriteUint32(uint32(len(w.catalog.Sequences)))
	bw.WriteUint32(uint32(len(w.catalog.Types)))

	// Write total number of catalog entry blocks
	bw.WriteUint64(uint64(len(w.metaBlocks)))

	// Write block IDs of all catalog entries
	for _, blockID := range w.metaBlocks {
		bw.WriteUint64(blockID)
	}

	if bw.Err() != nil {
		return MetaBlockPointer{}, bw.Err()
	}

	// Allocate and write index block
	blockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to allocate catalog index block: %w", err)
	}

	block := &Block{
		ID:   blockID,
		Type: BlockMetaData,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(block.Data, buf.Bytes())

	if err := w.blockManager.WriteBlock(block); err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to write catalog index block: %w", err)
	}

	return MetaBlockPointer{BlockID: blockID, Offset: 0}, nil
}

// Close closes the catalog writer and releases resources.
// This does NOT flush any pending writes - call Write() first if needed.
func (w *CatalogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	w.metaBlocks = nil
	w.tableRowGroups = nil

	return nil
}

// MetaBlockCount returns the number of metadata blocks written.
func (w *CatalogWriter) MetaBlockCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.metaBlocks)
}

// GetMetaBlocks returns a copy of the metadata block IDs that have been written.
func (w *CatalogWriter) GetMetaBlocks() []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	blocks := make([]uint64, len(w.metaBlocks))
	copy(blocks, w.metaBlocks)
	return blocks
}

// GetRowGroupCount returns the total number of row groups registered.
func (w *CatalogWriter) GetRowGroupCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	count := 0
	for _, entry := range w.tableRowGroups {
		count += len(entry.rowGroups)
	}
	return count
}

// GetTableRowGroupCount returns the number of row groups for a specific table.
func (w *CatalogWriter) GetTableRowGroupCount(tableOID uint64) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry, exists := w.tableRowGroups[tableOID]; exists {
		return len(entry.rowGroups)
	}
	return 0
}

// GetTableOIDs returns a list of table OIDs that have row groups registered.
func (w *CatalogWriter) GetTableOIDs() []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	oids := make([]uint64, 0, len(w.tableRowGroups))
	for oid := range w.tableRowGroups {
		oids = append(oids, oid)
	}
	sort.Slice(oids, func(i, j int) bool { return oids[i] < oids[j] })
	return oids
}

// Catalog returns the catalog being written.
func (w *CatalogWriter) Catalog() *DuckDBCatalog {
	return w.catalog
}

// BlockManager returns the block manager used by this writer.
func (w *CatalogWriter) BlockManager() *BlockManager {
	return w.blockManager
}

// IsClosed returns whether the writer has been closed.
func (w *CatalogWriter) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.closed
}

// SerializeCatalogToBytes serializes the catalog to bytes without writing to blocks.
// This is useful for testing or for cases where you need the raw bytes.
func SerializeCatalogToBytes(catalog *DuckDBCatalog) ([]byte, error) {
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// Write catalog version
	bw.WriteUint32(uint32(CurrentVersion))

	// Write counts
	bw.WriteUint32(uint32(len(catalog.Schemas)))
	bw.WriteUint32(uint32(len(catalog.Tables)))
	bw.WriteUint32(uint32(len(catalog.Views)))
	bw.WriteUint32(uint32(len(catalog.Indexes)))
	bw.WriteUint32(uint32(len(catalog.Sequences)))
	bw.WriteUint32(uint32(len(catalog.Types)))

	// Write schemas
	for _, schema := range catalog.Schemas {
		if err := schema.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize schema %s: %w", schema.Name, err)
		}
	}

	// Write tables
	for _, table := range catalog.Tables {
		if err := table.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize table %s: %w", table.Name, err)
		}
	}

	// Write views
	for _, view := range catalog.Views {
		if err := view.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize view %s: %w", view.Name, err)
		}
	}

	// Write indexes
	for _, index := range catalog.Indexes {
		if err := index.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize index %s: %w", index.Name, err)
		}
	}

	// Write sequences
	for _, sequence := range catalog.Sequences {
		if err := sequence.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize sequence %s: %w", sequence.Name, err)
		}
	}

	// Write types
	for _, typeEntry := range catalog.Types {
		if err := typeEntry.Serialize(bw); err != nil {
			return nil, fmt.Errorf("failed to serialize type %s: %w", typeEntry.Name, err)
		}
	}

	if bw.Err() != nil {
		return nil, bw.Err()
	}

	return buf.Bytes(), nil
}
