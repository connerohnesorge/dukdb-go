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

	// binaryFormatBlocks tracks block IDs allocated during WriteBinaryFormat.
	// These are the metadata blocks that need to be registered in MetadataManager state.
	binaryFormatBlocks []uint64

	// tableRowGroups maps table OID to row group pointers.
	tableRowGroups map[uint64]*tableRowGroupEntry

	// closed indicates whether the writer has been closed.
	closed bool

	// duckdbCompatMode enables DuckDB CLI compatibility mode.
	// When true, metadata is only written if there are row groups.
	// This allows DuckDB CLI to open files as empty databases.
	duckdbCompatMode bool

	// useBinarySerializer enables BinarySerializer format.
	// When true, uses DuckDB-compatible BinarySerializer instead of custom format.
	useBinarySerializer bool

	// metadataManager manages metadata sub-blocks for BinarySerializer format.
	metadataManager *MetadataBlockManager

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
		blockManager:     bm,
		catalog:          catalog,
		metaBlocks:       make([]uint64, 0),
		tableRowGroups:   make(map[uint64]*tableRowGroupEntry),
		closed:           false,
		duckdbCompatMode: false,
	}
}

// SetDuckDBCompatMode enables or disables DuckDB CLI compatibility mode.
// When enabled, metadata is only written if there are row groups with actual data.
// This also enables BinarySerializer format for DuckDB-compatible serialization.
// By default, compatibility mode is disabled.
func (w *CatalogWriter) SetDuckDBCompatMode(enabled bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.duckdbCompatMode = enabled
	w.useBinarySerializer = enabled // Use BinarySerializer when in compat mode
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
// For an empty catalog (no entries), returns an invalid MetaBlockPointer
// with BlockID set to InvalidBlockID. This signals to DuckDB that there
// is no catalog metadata to load.
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

	// For empty catalogs, return an invalid pointer so DuckDB won't try to load metadata
	if w.catalog.IsEmpty() {
		return MetaBlockPointer{BlockID: InvalidBlockID, BlockIndex: 0, Offset: 0}, nil
	}

	// If binary serializer mode is enabled, use new format
	// This format is compatible with DuckDB CLI and writes table definitions
	// even for empty tables (with empty table_pointer and zero row count).
	if w.useBinarySerializer {
		return w.WriteBinaryFormat()
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
//
// DuckDB metadata block format:
// - Bytes 0-7: Next block pointer (InvalidBlockID if last/only block)
// - Bytes 8+: Serialized catalog entry
func (w *CatalogWriter) writeEntry(entry CatalogEntry) error {
	// Serialize entry
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// First 8 bytes: next block pointer (InvalidBlockID = no more blocks)
	bw.WriteUint64(InvalidBlockID)

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
//
// DuckDB metadata block format:
// - Bytes 0-7: Next block pointer (InvalidBlockID if last/only block)
// - Bytes 8+: Serialized table entry data
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

	// First 8 bytes: next block pointer (InvalidBlockID = no more blocks)
	bw.WriteUint64(InvalidBlockID)

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
//
// DuckDB metadata block format:
// - Bytes 0-7: Next block pointer (InvalidBlockID if last/only block)
// - Bytes 8+: Row group pointer data
func (w *CatalogWriter) writeRowGroupPointers(tableOID uint64) (uint64, error) {
	entry, exists := w.tableRowGroups[tableOID]
	if !exists || len(entry.rowGroups) == 0 {
		return 0, nil // No row groups for this table
	}

	// Serialize all row group pointers
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// First 8 bytes: next block pointer (InvalidBlockID = no more blocks)
	bw.WriteUint64(InvalidBlockID)

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
//
// DuckDB metadata block format:
// - Bytes 0-7: Next block pointer (InvalidBlockID if last/only block)
// - Bytes 8+: Serialized catalog data
func (w *CatalogWriter) writeCatalogIndex() (MetaBlockPointer, error) {
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// First 8 bytes: next block pointer (InvalidBlockID = no more blocks)
	// This is required by DuckDB's metadata block format
	bw.WriteUint64(InvalidBlockID)

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

	return MetaBlockPointer{BlockID: blockID, BlockIndex: 0, Offset: 0}, nil
}

// writeTableStorageMetadata computes table storage info without writing to disk yet.
// Returns a map of tableOID -> TableStorageInfo for use during catalog serialization.
//
// The actual table storage data will be written as part of WriteBinaryFormat() to
// ensure it's placed in sub-blocks of block 0, which is required for DuckDB CLI compatibility.
//
// The caller must hold w.mu when calling this method.
func (w *CatalogWriter) writeTableStorageMetadata() (map[uint64]TableStorageInfo, error) {
	storageMap := make(map[uint64]TableStorageInfo)

	// For tables with row groups, we'll write storage to block 0's sub-blocks.
	// DuckDB uses sub-block 1 for table storage (sub-block 0 is catalog entries).
	// The table_pointer uses InvalidIndex (0x100000000000000) which decodes to
	// block_id=0, block_index=1, pointing to sub-block 1 of block 0.
	//
	// For now, all tables share the same table_pointer (pointing to sub-block 1).
	// The actual storage data will be written in WriteBinaryFormat().

	for tableOID, entry := range w.tableRowGroups {
		if len(entry.rowGroups) == 0 {
			continue
		}

		// Calculate total rows for this table
		totalRows := uint64(0)
		for _, rg := range entry.rowGroups {
			totalRows += rg.TupleCount
		}

		// Store the storage info with table_pointer pointing to block 0, sub-block 1
		// The Offset=8 skips the 8-byte next_ptr header at the start of each sub-block
		storageMap[tableOID] = TableStorageInfo{
			TablePointer: MetaBlockPointer{
				BlockID:    0,          // Block 0 (catalog block)
				BlockIndex: 1,          // Sub-block 1 (after catalog entries in sub-block 0)
				Offset:     8,          // After the 8-byte next_ptr header
			},
			TotalRows: totalRows,
		}
	}

	return storageMap, nil
}

// writeRowGroupPointersToBlock writes row group pointers to a metadata block.
// Returns the block ID where row group pointers are stored.
//
// DuckDB's table data format at the tablePointer location:
// 1. TableStatistics (BinarySerializer wrapped with Begin/End)
// 2. row_group_count (raw uint64, 8 bytes - NOT varint!)
// 3. For each row group: BinarySerializer wrapped RowGroupPointer
//
// Each RowGroupPointer object has:
// - Field 100: row_start (uint64 varint)
// - Field 101: tuple_count (uint64 varint)
// - Field 102: data_pointers (vector<MetaBlockPointer>)
// - Field 103: delete_pointers (vector<MetaBlockPointer>) - empty
// - Terminator 0xFFFF
func (w *CatalogWriter) writeRowGroupPointersToBlock(rowGroups []*RowGroupPointer) (uint64, error) {
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// First 8 bytes: next block pointer (InvalidBlockID = no more blocks)
	bw.WriteUint64(InvalidBlockID)

	// Write minimal TableStatistics using BinarySerializer format.
	// The reader's skipTableStatistics() expects at least 50 bytes of TableStatistics
	// before searching for the terminator pattern. We pad with zeros to meet this.
	//
	// TableStatistics format:
	// - Field 100: column_stats (list of ColumnStatistics)
	// - Object terminator (0xFFFF)
	serializer := NewBinarySerializer(&buf)

	// Write Field 100 (column_stats) as empty list
	serializer.OnPropertyBegin(100, "column_stats")
	serializer.OnListBegin(0) // Empty column stats list

	// Add padding to ensure TableStatistics is at least 50 bytes
	// Reader starts at offset 8 (after next_ptr header), then searches from offset 8+50=58
	// Current position: 8 (next_ptr) + 2 (field ID) + 1 (list count) = 11 bytes
	// Need terminator at or after offset 58, so: 58 - 11 = 47 bytes of padding minimum
	// Use 50 bytes to be safe
	padding := make([]byte, 50)
	buf.Write(padding)

	// Write object end terminator for TableStatistics (0xFFFF)
	serializer.OnObjectEnd()

	if err := serializer.Err(); err != nil {
		return 0, fmt.Errorf("failed to serialize TableStatistics: %w", err)
	}

	// Write row_group_count as raw uint64 (8 bytes) - NOT varint!
	bw.WriteUint64(uint64(len(rowGroups)))

	// Write each row group pointer in BinarySerializer format
	for _, rgp := range rowGroups {
		// Field 100: row_start
		serializer.OnPropertyBegin(100, "row_start")
		serializer.WriteUint64(rgp.RowStart)

		// Field 101: tuple_count
		serializer.OnPropertyBegin(101, "tuple_count")
		serializer.WriteUint64(rgp.TupleCount)

		// Field 102: data_pointers (vector<MetaBlockPointer>)
		// Each MetaBlockPointer is a nested object with:
		// - Field 100: block_pointer (uint64) - encoded as (block_id & mask) | (block_index << 56)
		// - Field 101: offset (uint32) - optional
		// - Terminator 0xFFFF
		serializer.OnPropertyBegin(102, "data_pointers")
		serializer.OnListBegin(uint64(len(rgp.DataPointers)))
		for _, dp := range rgp.DataPointers {
			// Write nested MetaBlockPointer object
			// Field 100: block_pointer
			serializer.OnPropertyBegin(100, "block_pointer")
			encoded := dp.Encode()
			serializer.WriteUint64(encoded)

			// Field 101: offset (if non-zero)
			if dp.Offset != 0 {
				serializer.OnPropertyBegin(101, "offset")
				serializer.WriteUint32(uint32(dp.Offset))
			}

			// MetaBlockPointer terminator
			serializer.OnObjectEnd()
		}

		// Field 103: delete_pointers (empty vector)
		serializer.OnPropertyBegin(103, "delete_pointers")
		serializer.OnListBegin(0)

		// RowGroupPointer object end terminator
		serializer.OnObjectEnd()
	}

	if err := serializer.Err(); err != nil {
		return 0, fmt.Errorf("failed to serialize row group pointers: %w", err)
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

// WriteBinaryFormat serializes the catalog using DuckDB's BinarySerializer format.
// This produces DuckDB CLI-compatible output by writing to metadata sub-blocks.
//
// DuckDB's metadata sub-block format (16-byte header):
// - First 8 bytes: Previous/metadata pointer (InvalidBlockID for first block)
// - Next 8 bytes: Next sub-block pointer (InvalidBlockID if last)
// - Remaining bytes: Actual serialized catalog data
//
// The MetaBlockPointer in database header encodes: BlockID (bits 0-55) + BlockIndex (bits 56-63)
//
// The caller must hold w.mu when calling this method.
//
// NOTE: Currently, table storage metadata (row group pointers) is NOT written because
// DuckDB expects a complex format that includes column segment metadata, statistics,
// HyperLogLog structures, etc. Tables with data will appear empty when opened by DuckDB CLI.
// Full row data compatibility is a work in progress.
func (w *CatalogWriter) WriteBinaryFormat() (MetaBlockPointer, error) {
	// For empty catalogs, return invalid pointer
	if w.catalog.IsEmpty() {
		return MetaBlockPointer{BlockID: InvalidBlockID}, nil
	}

	// Compute table storage info (row group pointers) for tables with data.
	// This returns the storage info (table pointer and total rows) for use during catalog serialization.
	// The actual storage data will be written to sub-block 1 of block 0 after catalog entries.
	tableStorageMap, err := w.writeTableStorageMetadata()
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to compute table storage metadata: %w", err)
	}

	// Collect row groups for all tables that have data
	var allRowGroups []*RowGroupPointer
	for tableOID := range tableStorageMap {
		if entry, exists := w.tableRowGroups[tableOID]; exists {
			allRowGroups = append(allRowGroups, entry.rowGroups...)
		}
	}

	// Step 2: Serialize catalog entries to a buffer
	var buf bytes.Buffer

	// DuckDB's metadata block layout in the raw file:
	// - Bytes 0-7: Checksum (handled by BlockManager)
	// - Bytes 8-15: Next block pointer (InvalidBlockID for last/single block)
	// - Bytes 16+: Catalog data
	//
	// Since BlockManager handles the checksum, Block.Data contains:
	// - Bytes 0-7: Next block pointer
	// - Bytes 8+: Catalog data
	//
	// This matches native DuckDB's format where the checksum is separate.
	var headerBytes [8]byte
	// Next block pointer (InvalidBlockID for single block)
	headerBytes[0] = 0xFF
	headerBytes[1] = 0xFF
	headerBytes[2] = 0xFF
	headerBytes[3] = 0xFF
	headerBytes[4] = 0xFF
	headerBytes[5] = 0xFF
	headerBytes[6] = 0xFF
	headerBytes[7] = 0xFF
	buf.Write(headerBytes[:])

	// Create BinarySerializer writing to the buffer
	serializer := NewBinarySerializer(&buf)

	// Collect all catalog entries
	entries := w.collectCatalogEntries()

	// Serialize catalog entries list
	// DuckDB format: Property 100 with list of catalog entries
	serializer.OnPropertyBegin(PropCatalogEntryData, "catalog_entries")
	serializer.OnListBegin(uint64(len(entries)))

	for _, entry := range entries {
		serializer.OnObjectBegin()

		// For table entries, pass the storage info
		// NOTE: For multi-table databases, all tables share the same table_pointer
		// pointing to SB1. The storage metadata for all tables is serialized together
		// in a single sub-block chain (SB1→SB2→SB3→...).
		var storageInfo *TableStorageInfo
		if table, isTable := entry.(*TableCatalogEntry); isTable {
			// Find this table's OID (index in catalog.Tables)
			tableOID := uint64(0)
			for i, t := range w.catalog.Tables {
				if t == table {
					tableOID = uint64(i)
					break
				}
			}

			// Get storage info if available
			if info, exists := tableStorageMap[tableOID]; exists {
				storageInfo = &info
			}
			// If no storage info (empty table), storageInfo remains nil
			// and serializeTableDataPointer will use InvalidIndex
		}

		SerializeCatalogEntryBinary(serializer, entry, storageInfo)
		// DuckDB writes TWO terminators after each entry:
		// 1. CreateInfo terminator (written by SerializeCatalogEntryBinary via entry serializer's OnObjectEnd())
		// 2. Entry terminator (written here)
		// DuckDB's ReadList calls OnObjectEnd() after each element, which consumes the entry terminator.
		serializer.OnObjectEnd()
	}

	// Add terminator for the root object
	serializer.OnObjectEnd()

	// Check for serialization errors
	if err := serializer.Err(); err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to serialize catalog: %w", err)
	}

	// Get the serialized data
	data := buf.Bytes()

	// Calculate how many blocks we need
	blockDataSize := int(w.blockManager.BlockSize()) - BlockChecksumSize
	numBlocks := (len(data) + blockDataSize - 1) / blockDataSize
	if numBlocks == 0 {
		numBlocks = 1
	}

	// Allocate blocks for catalog data
	// IMPORTANT: The first block MUST be block 0 because the table_pointer in
	// catalog entries uses InvalidIndex (0x100000000000000) which encodes to
	// block 0, sub-block 1. The table storage metadata is written to block 0's
	// sub-blocks by writeFreeListWithMetadataManager().
	blockIDs := make([]uint64, numBlocks)
	blockIDs[0] = w.blockManager.AllocateMetadataBlock() // Always use block 0 for catalog
	for i := 1; i < numBlocks; i++ {
		blockID, err := w.blockManager.AllocateBlock()
		if err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to allocate block %d/%d: %w", i+1, numBlocks, err)
		}
		blockIDs[i] = blockID
	}

	// Track these blocks for MetadataManager state
	w.binaryFormatBlocks = blockIDs

	// Write data to blocks with chaining
	// The 8-byte header format (in Block.Data) is:
	// - Bytes 0-7: Next block pointer (InvalidBlockID for last block)
	// The checksum is handled separately by BlockManager.
	for i := 0; i < numBlocks; i++ {
		start := i * blockDataSize
		end := start + blockDataSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		// For the first block, the 8-byte header is already prepended
		// For subsequent blocks, we need to prepend a new 8-byte header
		var blockData []byte
		if i == 0 {
			// First block already has 8-byte header prepended
			blockData = make([]byte, blockDataSize)
			copy(blockData, chunk)

			// Update next_ptr (bytes 0-7) if there are more blocks
			if numBlocks > 1 {
				nextPtr := blockIDs[1]
				blockData[0] = byte(nextPtr)
				blockData[1] = byte(nextPtr >> 8)
				blockData[2] = byte(nextPtr >> 16)
				blockData[3] = byte(nextPtr >> 24)
				blockData[4] = byte(nextPtr >> 32)
				blockData[5] = byte(nextPtr >> 40)
				blockData[6] = byte(nextPtr >> 48)
				blockData[7] = byte(nextPtr >> 56)
			}
		} else {
			// Subsequent blocks need 8-byte header prepended
			blockData = make([]byte, blockDataSize)

			// Bytes 0-7: Next block pointer
			nextPtr := InvalidBlockID
			if i < numBlocks-1 {
				nextPtr = blockIDs[i+1]
			}
			blockData[0] = byte(nextPtr)
			blockData[1] = byte(nextPtr >> 8)
			blockData[2] = byte(nextPtr >> 16)
			blockData[3] = byte(nextPtr >> 24)
			blockData[4] = byte(nextPtr >> 32)
			blockData[5] = byte(nextPtr >> 40)
			blockData[6] = byte(nextPtr >> 48)
			blockData[7] = byte(nextPtr >> 56)

			// Copy the actual data after 8-byte header
			copy(blockData[8:], chunk)
		}

		// For block 0, also write table storage data to sub-block 1 if we have row groups
		// This is needed for DuckDBWriter which doesn't call writeFreeListWithMetadataManager()
		if i == 0 && len(allRowGroups) > 0 {
			// The MetadataBlockReader uses 4088-byte segments:
			// - segmentOffset = BlockIndex * 4088
			// So sub-block 1 starts at offset 4088 in Block.Data (which excludes checksum)
			sb1Offset := MetadataSubBlockSize - BlockChecksumSize // 4088

			// Serialize table storage data (TableStatistics + row_group_count + row groups)
			storageData, err := w.serializeTableStorageData(allRowGroups)
			if err != nil {
				return MetaBlockPointer{}, fmt.Errorf("failed to serialize table storage: %w", err)
			}

			// Copy storage data to sub-block 1 position
			if sb1Offset+len(storageData) <= len(blockData) {
				copy(blockData[sb1Offset:], storageData)
			}
		}

		block := &Block{
			ID:   blockIDs[i],
			Type: BlockMetaData,
			Data: blockData,
		}

		if err := w.blockManager.WriteBlock(block); err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to write metadata block %d: %w", blockIDs[i], err)
		}
	}

	// Return pointer to first block with block_index=0
	return MetaBlockPointer{BlockID: blockIDs[0], BlockIndex: 0, Offset: 0}, nil
}

// serializeTableStorageData serializes table storage data (TableStatistics + row groups) for sub-block 1.
// This creates the data that goes at the table_pointer location.
//
// Format:
// 1. 8-byte next_ptr header (pointing to next sub-block or InvalidBlockID)
// 2. TableStatistics (BinarySerializer format with 50 bytes padding)
// 3. row_group_count (raw uint64, 8 bytes)
// 4. Each row group as BinarySerializer objects
func (w *CatalogWriter) serializeTableStorageData(rowGroups []*RowGroupPointer) ([]byte, error) {
	var buf bytes.Buffer
	bw := NewBinaryWriter(&buf)

	// First 8 bytes: next block pointer (InvalidBlockID = no continuation)
	bw.WriteUint64(InvalidBlockID)

	// Write minimal TableStatistics using BinarySerializer format.
	// The reader's skipTableStatistics() expects at least 50 bytes of TableStatistics
	// before searching for the terminator pattern.
	serializer := NewBinarySerializer(&buf)

	// Write Field 100 (column_stats) as empty list
	serializer.OnPropertyBegin(100, "column_stats")
	serializer.OnListBegin(0) // Empty column stats list

	// Add padding to ensure TableStatistics is at least 50 bytes
	padding := make([]byte, 50)
	buf.Write(padding)

	// Write object end terminator for TableStatistics (0xFFFF)
	serializer.OnObjectEnd()

	if err := serializer.Err(); err != nil {
		return nil, fmt.Errorf("failed to serialize TableStatistics: %w", err)
	}

	// Write row_group_count as raw uint64 (8 bytes) - NOT varint!
	bw.WriteUint64(uint64(len(rowGroups)))

	// Write each row group pointer in BinarySerializer format
	for _, rgp := range rowGroups {
		// Field 100: row_start
		serializer.OnPropertyBegin(100, "row_start")
		serializer.WriteUint64(rgp.RowStart)

		// Field 101: tuple_count
		serializer.OnPropertyBegin(101, "tuple_count")
		serializer.WriteUint64(rgp.TupleCount)

		// Field 102: data_pointers (vector<MetaBlockPointer>)
		serializer.OnPropertyBegin(102, "data_pointers")
		serializer.OnListBegin(uint64(len(rgp.DataPointers)))
		for _, dp := range rgp.DataPointers {
			// Write nested MetaBlockPointer object
			// Field 100: block_pointer
			serializer.OnPropertyBegin(100, "block_pointer")
			encoded := dp.Encode()
			serializer.WriteUint64(encoded)

			// Field 101: offset (if non-zero)
			if dp.Offset != 0 {
				serializer.OnPropertyBegin(101, "offset")
				serializer.WriteUint32(uint32(dp.Offset))
			}

			// MetaBlockPointer terminator
			serializer.OnObjectEnd()
		}

		// Field 103: delete_pointers (empty vector)
		serializer.OnPropertyBegin(103, "delete_pointers")
		serializer.OnListBegin(0)

		// RowGroupPointer object end terminator
		serializer.OnObjectEnd()
	}

	if err := serializer.Err(); err != nil {
		return nil, fmt.Errorf("failed to serialize row group pointers: %w", err)
	}

	if bw.Err() != nil {
		return nil, bw.Err()
	}

	return buf.Bytes(), nil
}

// collectCatalogEntries collects all catalog entries in the correct order.
// The caller must hold w.mu when calling this method.
func (w *CatalogWriter) collectCatalogEntries() []CatalogEntry {
	entries := make([]CatalogEntry, 0, w.catalog.EntryCount()+1) // +1 for potential auto-added schema

	// Add schemas first
	for _, schema := range w.catalog.Schemas {
		entries = append(entries, schema)
	}

	// Auto-add "main" schema if there are tables but no "main" schema
	// DuckDB expects a schema entry before table entries
	if len(w.catalog.Tables) > 0 {
		hasMainSchema := false
		for _, schema := range w.catalog.Schemas {
			if schema.Name == "main" {
				hasMainSchema = true
				break
			}
		}
		if !hasMainSchema {
			mainSchema := NewSchemaCatalogEntry("main")
			entries = append(entries, mainSchema)
		}
	}

	// Add tables
	for _, table := range w.catalog.Tables {
		entries = append(entries, table)
	}

	// Add views
	for _, view := range w.catalog.Views {
		entries = append(entries, view)
	}

	// Add indexes
	for _, index := range w.catalog.Indexes {
		entries = append(entries, index)
	}

	// Add sequences
	for _, seq := range w.catalog.Sequences {
		entries = append(entries, seq)
	}

	// Add types
	for _, typ := range w.catalog.Types {
		entries = append(entries, typ)
	}

	return entries
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

// GetBinaryFormatBlocks returns the block IDs allocated during WriteBinaryFormat.
// These blocks need to be registered in the MetadataManager state for DuckDB compatibility.
func (w *CatalogWriter) GetBinaryFormatBlocks() []uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	blocks := make([]uint64, len(w.binaryFormatBlocks))
	copy(blocks, w.binaryFormatBlocks)
	return blocks
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

// getTableStorageSubBlockCount returns the number of sub-blocks needed for a table's storage metadata.
// This matches the logic in DuckDBStorage.getTableStorageSubBlockCount.
func (w *CatalogWriter) getTableStorageSubBlockCount(columnCount int, columns []ColumnDefinition) int {
	if columnCount == 0 {
		return 0
	}
	if columnCount <= 2 {
		return columnCount
	}
	if columnCount == 5 {
		// 5-column tables need 4 sub-blocks (SB1-SB4)
		return 4
	}
	// 3-4 column tables need 3 sub-blocks
	return 3
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
