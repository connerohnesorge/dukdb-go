package duckdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

// MetadataBlockReader implements io.Reader for chained metadata blocks.
// It handles reading across multiple 256KB blocks linked by next-block pointers.
//
// DuckDB's catalog metadata format uses full 256KB blocks:
// - Raw file has: [checksum:8][next_ptr:8][data:~262KB]
// - BlockManager strips checksum, so Block.Data has: [next_ptr:8][data:~262KB]
// - The MetaBlockPointer in the database header encodes block_id (bits 0-55) + block_index (bits 56-63)
// - For catalog metadata, block_index is typically 0
type MetadataBlockReader struct {
	blockManager *BlockManager
	data         []byte // Current block's data (after header)
	offset       int    // Current position within data
	nextBlockID  uint64 // Next block in chain, InvalidBlockID if none
}

// MetadataBlockHeaderSize is the size of the header at the start of Block.Data.
// This contains just the next_ptr (8 bytes). The checksum is handled separately by BlockManager.
const MetadataBlockHeaderSize = 8

// NewMetadataBlockReader creates a new MetadataBlockReader starting at the given block.
// The startBlockID is an encoded MetaBlockPointer with block_id and block_index.
// The block_index determines which 4KB sub-block within the 256KB block to start reading from.
func NewMetadataBlockReader(bm *BlockManager, startBlockID uint64) (*MetadataBlockReader, error) {
	// Decode the MetaBlockPointer to get block_id and block_index
	ptr := DecodeMetaBlockPointer(startBlockID)

	// Read the storage block
	block, err := bm.ReadBlock(ptr.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata block %d: %w", ptr.BlockID, err)
	}

	// Calculate the sub-block offset within the 256KB block
	// Note: Block.Data already has the 8-byte checksum stripped
	// But the checksum removal doesn't change internal offsets from DuckDB's perspective
	//
	// Raw block layout (on disk):
	//   [checksum:8][sub-block 0: 4096-8 bytes][sub-block 1: 4096 bytes][...]
	// After BlockManager strips checksum:
	//   Block.Data: [sub-block 0 data: 4088 bytes][sub-block 1: 4096 bytes][...]
	//
	// Each sub-block has:
	//   [next_ptr:8][content: remaining]
	//
	// For sub-block 0: starts at offset 0 in Block.Data (after checksum stripped)
	// For sub-block N: starts at offset (N * 4096) - 8 in Block.Data

	subBlockOffset := int(ptr.BlockIndex) * MetadataSubBlockSize
	if ptr.BlockIndex > 0 {
		// Adjust for the stripped checksum at the beginning
		subBlockOffset -= BlockChecksumSize
	}

	if subBlockOffset < 0 || subBlockOffset+MetadataBlockHeaderSize > len(block.Data) {
		return nil, fmt.Errorf("sub-block %d offset out of range in block %d", ptr.BlockIndex, ptr.BlockID)
	}

	// Read next block pointer from the sub-block header
	nextBlockID := binary.LittleEndian.Uint64(block.Data[subBlockOffset : subBlockOffset+8])

	// Data starts after the 8-byte next_ptr header in the sub-block
	dataStart := subBlockOffset + MetadataBlockHeaderSize
	dataEnd := subBlockOffset + MetadataSubBlockSize
	if ptr.BlockIndex == 0 {
		// Sub-block 0 is shorter because checksum was stripped
		dataEnd = MetadataSubBlockSize - BlockChecksumSize
	}
	if dataEnd > len(block.Data) {
		dataEnd = len(block.Data)
	}

	r := &MetadataBlockReader{
		blockManager: bm,
		data:         block.Data[dataStart:dataEnd],
		offset:       0,
		nextBlockID:  nextBlockID,
	}

	return r, nil
}

// Read implements io.Reader, reading from chained metadata blocks.
func (r *MetadataBlockReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Check if current block is exhausted
		if r.offset >= len(r.data) {
			// Try to load next block
			if r.nextBlockID == InvalidBlockID {
				// No more blocks
				if n == 0 {
					return 0, io.EOF
				}
				return n, nil
			}

			// Load next block
			if err := r.loadNextBlock(); err != nil {
				if n == 0 {
					return 0, err
				}
				return n, nil
			}
		}

		// Copy what we can from current block
		toCopy := min(len(p)-n, len(r.data)-r.offset)
		copy(p[n:], r.data[r.offset:r.offset+toCopy])
		r.offset += toCopy
		n += toCopy
	}

	return n, nil
}

// loadNextBlock loads the next block in the chain.
func (r *MetadataBlockReader) loadNextBlock() error {
	if r.nextBlockID == InvalidBlockID {
		return io.EOF
	}

	block, err := r.blockManager.ReadBlock(r.nextBlockID)
	if err != nil {
		return fmt.Errorf("failed to read next metadata block %d: %w", r.nextBlockID, err)
	}

	if len(block.Data) < MetadataBlockHeaderSize {
		return fmt.Errorf("metadata block too small: %d bytes", len(block.Data))
	}

	// Read next block pointer (first 8 bytes)
	r.nextBlockID = uint64(block.Data[0]) |
		uint64(block.Data[1])<<8 |
		uint64(block.Data[2])<<16 |
		uint64(block.Data[3])<<24 |
		uint64(block.Data[4])<<32 |
		uint64(block.Data[5])<<40 |
		uint64(block.Data[6])<<48 |
		uint64(block.Data[7])<<56

	// Data starts after the 8-byte next_ptr header
	r.data = block.Data[MetadataBlockHeaderSize:]
	r.offset = 0

	return nil
}

// MetadataReader reads catalog entries from DuckDB metadata blocks.
// It uses BinaryDeserializer for reading primitives and maintains
// compatibility with the existing catalog reading logic.
type MetadataReader struct {
	deserializer *BinaryDeserializer
	blockReader  *MetadataBlockReader

	// Allow peeking 1 field ahead (for backward compatibility)
	hasBufferedField bool
	bufferedField    uint16
}

// ddbFieldTerminator marks the end of an object in DuckDB's binary format.
const ddbFieldTerminator uint16 = 0xFFFF

// Field IDs used in the DuckDB checkpoint format.
const (
	ddbFieldCatalogEntries uint16 = 100
	ddbFieldCatalogType    uint16 = 99
	ddbFieldEntryWrapper   uint16 = 100 // Wrapper field for each entry type
)

// CreateInfo property IDs (100-109) for DuckDB format.
const (
	ddbPropCreateType       uint16 = 100 // CatalogType
	ddbPropCreateCatalog    uint16 = 101 // catalog name (string)
	ddbPropCreateSchema     uint16 = 102 // schema name (string)
	ddbPropCreateTemporary  uint16 = 103 // temporary (bool)
	ddbPropCreateInternal   uint16 = 104 // internal (bool)
	ddbPropCreateOnConflict uint16 = 105 // on_conflict (uint8)
	ddbPropCreateSQL        uint16 = 106 // sql (string)
	ddbPropCreateComment    uint16 = 107 // comment (Value)
	ddbPropCreateTags       uint16 = 108 // tags (map)
	ddbPropCreateDeps       uint16 = 109 // dependencies (array)
)

// Table-specific property IDs (200+) for DuckDB format.
const (
	ddbPropTableName        uint16 = 200 // table name (string)
	ddbPropTableColumns     uint16 = 201 // columns (ColumnList)
	ddbPropTableConstraints uint16 = 202 // constraints (array)
	ddbPropTableQuery       uint16 = 203 // query (SelectStatement) - optional
)

// ColumnDefinition property IDs for DuckDB format.
const (
	ddbPropColName        uint16 = 100 // name (string)
	ddbPropColType        uint16 = 101 // type (LogicalType object)
	ddbPropColExpression  uint16 = 102 // expression (ParsedExpression) - optional
	ddbPropColCategory    uint16 = 103 // category (TableColumnType)
	ddbPropColCompression uint16 = 104 // compression_type
	ddbPropColComment     uint16 = 105 // comment (Value) - optional
	ddbPropColTags        uint16 = 106 // tags (map) - optional
)

// ColumnList property IDs for DuckDB format.
const (
	ddbPropColumnListColumns uint16 = 100 // columns (vector<ColumnDefinition>)
)

// LogicalType property IDs for DuckDB format.
const (
	ddbPropLogicalTypeID   uint16 = 100 // id (LogicalTypeId)
	ddbPropLogicalTypeInfo uint16 = 101 // type_info (ExtraTypeInfo) - optional
)

// ExtraTypeInfo property IDs for DuckDB format.
const (
	ddbPropExtraTypeInfoType      uint16 = 100 // type (ExtraTypeInfoType)
	ddbPropExtraTypeInfoAlias     uint16 = 101 // alias (string) - optional
	ddbPropExtraTypeInfoModifiers uint16 = 102 // modifiers (deleted)
	ddbPropExtraTypeInfoExtension uint16 = 103 // extension_info - optional
)

// DecimalTypeInfo property IDs for DuckDB format.
const (
	ddbPropDecimalWidth uint16 = 200 // width (uint8)
	ddbPropDecimalScale uint16 = 201 // scale (uint8)
)

// ListTypeInfo property IDs for DuckDB format.
const (
	ddbPropListChildType uint16 = 200 // child_type (LogicalType)
)

// ArrayTypeInfo property IDs for DuckDB format.
const (
	ddbPropArrayChildType uint16 = 200 // child_type (LogicalType)
	ddbPropArraySize      uint16 = 201 // size (uint32)
)

// StructTypeInfo property IDs for DuckDB format.
const (
	ddbPropStructChildTypes uint16 = 200 // child_types (child_list_t<LogicalType>)
)

// Constraint property IDs for DuckDB format.
const (
	ddbPropConstraintType uint16 = 100 // type (ConstraintType)
)

// NotNullConstraint property IDs for DuckDB format.
const (
	ddbPropNotNullIndex uint16 = 200 // index (LogicalIndex)
)

// UniqueConstraint property IDs for DuckDB format.
const (
	ddbPropUniquePrimaryKey uint16 = 200 // is_primary_key (bool)
	ddbPropUniqueIndex      uint16 = 201 // index (LogicalIndex)
	ddbPropUniqueColumns    uint16 = 202 // columns (vector<string>)
)

// CheckConstraint property IDs for DuckDB format.
const (
	ddbPropCheckExpression uint16 = 200 // expression (ParsedExpression)
)

// ForeignKeyConstraint property IDs for DuckDB format.
const (
	ddbPropFKPKColumns uint16 = 200 // pk_columns (vector<string>)
	ddbPropFKFKColumns uint16 = 201 // fk_columns (vector<string>)
	ddbPropFKType      uint16 = 202 // fk_type (ForeignKeyType)
	ddbPropFKSchema    uint16 = 203 // schema (string)
	ddbPropFKTable     uint16 = 204 // table (string)
	ddbPropFKPKKeys    uint16 = 205 // pk_keys (vector<PhysicalIndex>)
	ddbPropFKFKKeys    uint16 = 206 // fk_keys (vector<PhysicalIndex>)
)

// View property IDs for DuckDB format.
const (
	ddbPropViewName           uint16 = 200 // view_name (string)
	ddbPropViewAliases        uint16 = 201 // aliases (vector<string>)
	ddbPropViewTypes          uint16 = 202 // types (vector<LogicalType>)
	ddbPropViewQuery          uint16 = 203 // query (SelectStatement)
	ddbPropViewNames          uint16 = 204 // names (vector<string>)
	ddbPropViewColumnComments uint16 = 205 // column_comments (vector<Value>)
)

// Sequence property IDs for DuckDB format.
const (
	ddbPropSeqName       uint16 = 200 // name (string)
	ddbPropSeqUsageCount uint16 = 201 // usage_count (uint64)
	ddbPropSeqIncrement  uint16 = 202 // increment (int64)
	ddbPropSeqMinValue   uint16 = 203 // min_value (int64)
	ddbPropSeqMaxValue   uint16 = 204 // max_value (int64)
	ddbPropSeqStartValue uint16 = 205 // start_value (int64)
	ddbPropSeqCycle      uint16 = 206 // cycle (bool)
)

// Index property IDs for DuckDB format.
const (
	ddbPropIndexName       uint16 = 200 // name (string)
	ddbPropIndexTableName  uint16 = 201 // table (string)
	ddbPropIndexTypeOld    uint16 = 202 // index_type (deleted)
	ddbPropIndexConstraint uint16 = 203 // constraint_type (IndexConstraintType)
	ddbPropIndexExprs      uint16 = 204 // parsed_expressions
	ddbPropIndexScanTypes  uint16 = 205 // scan_types
	ddbPropIndexNames      uint16 = 206 // names (vector<string>)
	ddbPropIndexColumnIDs  uint16 = 207 // column_ids (vector<column_t>)
	ddbPropIndexOptions    uint16 = 208 // options (map)
	ddbPropIndexTypeName   uint16 = 209 // index_type_name (string)
)

// Type entry property IDs for DuckDB format.
const (
	ddbPropTypeEntryName uint16 = 200 // name (string)
	ddbPropTypeEntryType uint16 = 201 // logical_type (LogicalType)
)

// ExtraTypeInfoType represents the type of extra type info.
type ExtraTypeInfoType uint8

// ExtraTypeInfoType constants.
const (
	ExtraTypeInfoInvalid        ExtraTypeInfoType = 0
	ExtraTypeInfoGeneric        ExtraTypeInfoType = 1
	ExtraTypeInfoDecimal        ExtraTypeInfoType = 2
	ExtraTypeInfoString         ExtraTypeInfoType = 3
	ExtraTypeInfoList           ExtraTypeInfoType = 4
	ExtraTypeInfoStruct         ExtraTypeInfoType = 5
	ExtraTypeInfoEnum           ExtraTypeInfoType = 6
	ExtraTypeInfoUser           ExtraTypeInfoType = 7
	ExtraTypeInfoAggregateState ExtraTypeInfoType = 8
	ExtraTypeInfoArray          ExtraTypeInfoType = 9
	ExtraTypeInfoAny            ExtraTypeInfoType = 10
	ExtraTypeInfoIntegerLiteral ExtraTypeInfoType = 11
	ExtraTypeInfoTemplate       ExtraTypeInfoType = 12
	ExtraTypeInfoGeo            ExtraTypeInfoType = 13
)

// TableColumnCategory represents the category of a table column.
type TableColumnCategory uint8

// TableColumnCategory constants.
const (
	TableColumnStandard  TableColumnCategory = 0
	TableColumnGenerated TableColumnCategory = 1
)

// ddbConstraintType represents DuckDB's internal constraint type enum.
type ddbConstraintType uint8

// ddbConstraintType constants.
const (
	ddbConstraintInvalid    ddbConstraintType = 0
	ddbConstraintNotNull    ddbConstraintType = 1
	ddbConstraintCheck      ddbConstraintType = 2
	ddbConstraintUnique     ddbConstraintType = 3
	ddbConstraintForeignKey ddbConstraintType = 4
)

// ddbForeignKeyType represents the type of foreign key relationship.
type ddbForeignKeyType uint8

// ddbForeignKeyType constants.
const (
	ddbFKTypePrimaryKeyTable    ddbForeignKeyType = 0
	ddbFKTypeForeignKeyTable    ddbForeignKeyType = 1
	ddbFKTypeSelfReferenceTable ddbForeignKeyType = 2
)

// NewMetadataReader creates a new metadata reader starting at the given block.
func NewMetadataReader(bm *BlockManager, startBlockID uint64) (*MetadataReader, error) {
	return NewMetadataReaderWithOffset(bm, startBlockID, 0)
}

// NewMetadataReaderWithOffset creates a new metadata reader starting at the given block
// and seeking to the specified offset.
// The offset is relative to the start of the sub-block (INCLUDING the 8-byte header).
// So an offset of 8 means the first byte of content, offset of 10 means 2 bytes into content.
func NewMetadataReaderWithOffset(bm *BlockManager, startBlockID uint64, offset uint64) (*MetadataReader, error) {
	// Create the block reader that handles block chaining
	blockReader, err := NewMetadataBlockReader(bm, startBlockID)
	if err != nil {
		return nil, err
	}

	// Skip to the specified offset
	// The offset is relative to sub-block start (including header)
	// Since BlockReader.data already starts at content (after header), we subtract the header size
	if offset > 0 {
		if offset < MetadataBlockHeaderSize {
			return nil, fmt.Errorf("offset %d points into sub-block header (header is %d bytes)",
				offset, MetadataBlockHeaderSize)
		}
		contentOffset := int(offset - MetadataBlockHeaderSize)
		if contentOffset > len(blockReader.data) {
			return nil, fmt.Errorf("offset %d exceeds sub-block data length %d (content offset %d)",
				offset, len(blockReader.data)+MetadataBlockHeaderSize, contentOffset)
		}
		blockReader.offset = contentOffset
	}

	// Create a BinaryDeserializer on top of the block reader
	deserializer := NewBinaryDeserializer(blockReader)

	return &MetadataReader{
		deserializer: deserializer,
		blockReader:  blockReader,
	}, nil
}

// Helper methods for test code to access internal state
// These provide backward compatibility with test code that accessed internal fields

// offset returns the current offset in the block reader (for debugging).
func (r *MetadataReader) offset() int {
	return r.blockReader.offset
}

// data returns the current block data (for debugging).
func (r *MetadataReader) data() []byte {
	return r.blockReader.data
}

// remaining returns bytes remaining in current block (for debugging).
func (r *MetadataReader) remaining() int {
	return len(r.blockReader.data) - r.blockReader.offset
}


// ReadFieldID reads a uint16 field ID, consuming any buffered field first.
func (r *MetadataReader) ReadFieldID() (uint16, error) {
	return r.NextField()
}

// PeekField returns the next field ID without consuming it.
func (r *MetadataReader) PeekField() (uint16, error) {
	if !r.hasBufferedField {
		// Check for existing error first
		if err := r.deserializer.Err(); err != nil {
			return 0, err
		}

		field, ok := r.deserializer.PeekFieldID()
		if !ok {
			return 0, r.deserializer.Err()
		}
		r.bufferedField = field
		r.hasBufferedField = true
	}
	return r.bufferedField, nil
}

// ConsumeField consumes a buffered field.
func (r *MetadataReader) ConsumeField() {
	if !r.hasBufferedField {
		return
	}
	r.hasBufferedField = false
	// Actually consume from deserializer
	r.deserializer.ReadFieldID()
}

// NextField reads and returns the next field ID.
func (r *MetadataReader) NextField() (uint16, error) {
	if r.hasBufferedField {
		r.hasBufferedField = false
		// Need to consume from deserializer since we peeked
		r.deserializer.ReadFieldID()
		return r.bufferedField, nil
	}
	fieldID := r.deserializer.ReadFieldID()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	return fieldID, nil
}

// ReadVarint reads a variable-length integer (LEB128 encoded).
func (r *MetadataReader) ReadVarint() (uint64, error) {
	val := r.deserializer.ReadUint64()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	return val, nil
}

// ReadSignedVarint reads a signed variable-length integer (zigzag+varint).
func (r *MetadataReader) ReadSignedVarint() (int64, error) {
	val := r.deserializer.ReadInt64()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	return val, nil
}

// ReadString reads a varint-length prefixed string.
func (r *MetadataReader) ReadString() (string, error) {
	str := r.deserializer.ReadString()
	if err := r.deserializer.Err(); err != nil {
		return "", err
	}
	return str, nil
}

// ReadUint8 reads a varint-encoded uint8.
func (r *MetadataReader) ReadUint8() (uint8, error) {
	val := r.deserializer.ReadUint8()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	return val, nil
}

// ReadBool reads a single byte as a boolean.
func (r *MetadataReader) ReadBool() (bool, error) {
	val := r.deserializer.ReadBool()
	if err := r.deserializer.Err(); err != nil {
		return false, err
	}
	return val, nil
}

// ReadFloat32 reads a 4-byte float.
func (r *MetadataReader) ReadFloat32() (float32, error) {
	// BinaryDeserializer doesn't have ReadFloat32, so we read bytes directly
	bytes := r.deserializer.ReadBytes()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	if len(bytes) != 4 {
		return 0, fmt.Errorf("expected 4 bytes for float32, got %d", len(bytes))
	}
	bits := uint32(bytes[0]) | uint32(bytes[1])<<8 | uint32(bytes[2])<<16 | uint32(bytes[3])<<24
	return *(*float32)((unsafe.Pointer)(&bits)), nil
}

// ReadFloat64 reads an 8-byte double.
func (r *MetadataReader) ReadFloat64() (float64, error) {
	// BinaryDeserializer doesn't have ReadFloat64, so we read bytes directly
	bytes := r.deserializer.ReadBytes()
	if err := r.deserializer.Err(); err != nil {
		return 0, err
	}
	if len(bytes) != 8 {
		return 0, fmt.Errorf("expected 8 bytes for float64, got %d", len(bytes))
	}
	bits := uint64(bytes[0]) | uint64(bytes[1])<<8 | uint64(bytes[2])<<16 | uint64(bytes[3])<<24 |
		uint64(bytes[4])<<32 | uint64(bytes[5])<<40 | uint64(bytes[6])<<48 | uint64(bytes[7])<<56
	return *(*float64)((unsafe.Pointer)(&bits)), nil
}

// SkipToField skips the serialization stream until it finds the specified field ID
// at the current nesting level. This is useful for skipping complex nested structures.
// After calling this, the reader is positioned at the beginning of the target field's value.
// NOTE: This method directly manipulates the underlying block reader for efficiency.
func (r *MetadataReader) SkipToField(targetFieldID uint16) error {
	// Get direct access to the block data for efficient scanning
	data := r.blockReader.data
	maxBytes := 10000 // Maximum bytes to scan

	// Convert targetFieldID to bytes (little-endian)
	targetLow := byte(targetFieldID & 0xFF)
	targetHigh := byte(targetFieldID >> 8)

	scanned := 0
	for scanned < maxBytes {
		// Scan for the target field ID pattern
		remaining := len(data) - r.blockReader.offset
		if remaining < 2 {
			return fmt.Errorf("reached end of data before finding field %d", targetFieldID)
		}

		// Check if current position has the target field ID
		if data[r.blockReader.offset] == targetLow && data[r.blockReader.offset+1] == targetHigh {
			// Found it! Advance past the field ID
			r.blockReader.offset += 2
			r.hasBufferedField = false // Clear any buffered field
			return nil
		}

		// Advance one byte and continue scanning
		r.blockReader.offset++
		scanned++
	}

	return fmt.Errorf("exceeded maximum scan bytes (%d) while looking for field %d", maxBytes, targetFieldID)
}

// OnPropertyBegin starts reading a property with the given field ID.
func (r *MetadataReader) OnPropertyBegin(fieldID uint16) error {
	field, err := r.NextField()
	if err != nil {
		return err
	}
	if field != fieldID {
		return fmt.Errorf("field ID mismatch: expected %d, got %d", fieldID, field)
	}
	return nil
}

// OnOptionalPropertyBegin checks if an optional property is present.
func (r *MetadataReader) OnOptionalPropertyBegin(fieldID uint16) (bool, error) {
	nextField, err := r.PeekField()
	if err != nil {
		return false, err
	}
	present := nextField == fieldID
	if present {
		r.ConsumeField()
	}
	return present, nil
}

// SkipToTerminator skips fields until the terminator is found.
func (r *MetadataReader) SkipToTerminator() error {
	depth := 1
	for depth > 0 {
		fieldID, err := r.ReadFieldID()
		if err != nil {
			return err
		}
		if fieldID == ddbFieldTerminator {
			depth--
			continue
		}
		// Skip the field value based on what we know about field IDs
		// This is a best-effort skip - may fail on unknown types
		if err := r.skipFieldValue(fieldID); err != nil {
			return err
		}
	}
	return nil
}

// skipFieldValue attempts to skip a field value.
func (r *MetadataReader) skipFieldValue(fieldID uint16) error {
	// Most fields are varints or strings
	// We try to detect which type based on context

	// Check if this looks like a string (starts with a reasonable length)
	// or a varint (small value)
	val, err := r.ReadVarint()
	if err != nil {
		return err
	}

	// If the value looks like a string length (reasonable size), skip that many bytes
	// This is heuristic - may not always be correct
	if val > 0 && val < 1000000 {
		// Could be a string length or just a number - need context
		// For safety, we don't skip additional bytes here
		_ = val
	}

	return nil
}

// skipValue skips a value of unknown type by reading until terminator or known pattern.
func (r *MetadataReader) skipValue() error {
	// Read one varint - this handles most simple cases
	_, err := r.ReadVarint()
	return err
}

// skipOptionalValue skips an optional nullable value.
func (r *MetadataReader) skipOptionalNullable() error {
	present, err := r.ReadBool()
	if err != nil {
		return err
	}
	if present {
		// Skip the object
		return r.SkipToTerminator()
	}
	return nil
}

// ReadCatalogEntries reads all catalog entries from the metadata.
func (r *MetadataReader) ReadCatalogEntries() ([]CatalogEntry, error) {
	// Read field 100 (catalog_entries)
	fieldID, err := r.ReadFieldID()
	if err != nil {
		return nil, fmt.Errorf("failed to read catalog_entries field ID: %w", err)
	}
	if fieldID != ddbFieldCatalogEntries {
		return nil, fmt.Errorf("expected catalog_entries field (100), got %d", fieldID)
	}

	// Read list count
	count, err := r.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read catalog entry count: %w", err)
	}

	entries := make([]CatalogEntry, 0, count)

	for i := uint64(0); i < count; i++ {
		entry, err := r.ReadCatalogEntry()
		if err != nil {
			// Return the error instead of swallowing it
			return nil, fmt.Errorf("failed to read entry %d: %w", i, err)
		}
		if entry != nil {
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// ReadCatalogEntry reads a single catalog entry.
func (r *MetadataReader) ReadCatalogEntry() (CatalogEntry, error) {
	// Read field 99 (catalog_type)
	fieldID, err := r.ReadFieldID()
	if err != nil {
		return nil, err
	}
	if fieldID != ddbFieldCatalogType {
		return nil, fmt.Errorf("expected type field (99), got %d", fieldID)
	}

	catalogType, err := r.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read catalog type: %w", err)
	}

	// Read field 100 (entry wrapper)
	fieldID, err = r.ReadFieldID()
	if err != nil {
		return nil, err
	}
	if fieldID != ddbFieldEntryWrapper {
		return nil, fmt.Errorf("expected entry wrapper field (100), got %d", fieldID)
	}

	// Read nullable bool - indicates if the pointer is non-null
	isPresent, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("failed to read nullable bool: %w", err)
	}
	if !isPresent {
		// Null entry - skip
		return nil, nil
	}

	var entry CatalogEntry

	switch CatalogType(catalogType) {
	case CatalogSchemaEntry:
		entry, err = r.readSchemaEntry()
	case CatalogTableEntry:
		entry, err = r.readTableEntry()
	case CatalogViewEntry:
		entry, err = r.readViewEntry()
	case CatalogIndexEntry:
		entry, err = r.readIndexEntry()
	case CatalogSequenceEntry:
		entry, err = r.readSequenceEntry()
	case CatalogTypeEntry:
		entry, err = r.readTypeEntry()
	default:
		// Skip unknown entry types
		if err := r.SkipToTerminator(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	// After the catalog entry (e.g., CreateTableInfo), DuckDB may write additional
	// storage metadata (row counts, block allocation, index data, etc.) that uses
	// field IDs 100-109 range. We read fields 101 and 102, skip others.
	// - Field 99 (ddbFieldCatalogType): start of the next catalog entry
	// - End of data
	// The storage data may contain nested objects with their own terminators.
	storageMeta, err := r.readStorageMetadata()
	if err != nil {
		return nil, err
	}

	// Attach storage metadata to table entries
	if tableEntry, ok := entry.(*TableCatalogEntry); ok && storageMeta != nil {
		tableEntry.StorageMetadata = storageMeta
	}

	return entry, nil
}

// readMetaBlockPointer reads a MetaBlockPointer from a nested object.
// The MetaBlockPointer is serialized as an object with:
// - Field 100: block_pointer (uint64) - encoded as (block_id & 0x00FFFFFFFFFFFFFF) | (block_index << 56)
// - Field 101: offset (uint32) - OPTIONAL (omitted if 0 due to WritePropertyWithDefault)
// - Terminated with 0xFFFF
func (r *MetadataReader) readMetaBlockPointer() (MetaBlockPointer, error) {
	var ptr MetaBlockPointer

	// Read field 100 (block_pointer)
	if err := r.OnPropertyBegin(100); err != nil {
		return ptr, fmt.Errorf("expected field 100 for block_pointer: %w", err)
	}
	blockPointer, err := r.ReadVarint()
	if err != nil {
		return ptr, fmt.Errorf("failed to read block_pointer: %w", err)
	}

	// Decode the packed block_pointer value
	ptr.BlockID = blockPointer & 0x00FFFFFFFFFFFFFF
	ptr.BlockIndex = uint8(blockPointer >> 56)

	// Check if field 101 (offset) is present or if we have terminator
	fieldID, err := r.PeekField()
	if err != nil {
		return ptr, fmt.Errorf("failed to peek after block_pointer: %w", err)
	}

	if fieldID == 101 {
		// Read optional offset field
		r.ConsumeField()
		offset, err := r.ReadVarint()
		if err != nil {
			return ptr, fmt.Errorf("failed to read offset: %w", err)
		}
		ptr.Offset = offset
		// After consuming field 101, need to peek again for terminator
		fieldID, err = r.PeekField()
		if err != nil {
			return ptr, fmt.Errorf("failed to peek for terminator after offset: %w", err)
		}
	} else {
		// Offset defaults to 0
		ptr.Offset = 0
	}

	// Read terminator
	if fieldID == ddbFieldTerminator {
		r.ConsumeField()
	} else {
		return ptr, fmt.Errorf("expected MetaBlockPointer terminator (0xFFFF), got field %d", fieldID)
	}

	return ptr, nil
}

// StorageMetadata contains table storage information read from catalog entries.
type StorageMetadata struct {
	TablePointer MetaBlockPointer
	TotalRows    uint64
}

// readStorageMetadata reads storage-related fields that appear after catalog entries.
// DuckDB writes table storage info (row counts, block allocations, index metadata)
// after the CreateTableInfo object. We read fields 101 and 102, skip others.
//
// Storage metadata fields (from SingleFileTableDataWriter::FinalizeTable):
// - Field 101: table_pointer (MetaBlockPointer - nested object with terminator)
// - Field 102: total_rows (idx_t/uint64 - varint)
// - Field 103: index_pointers (vector<BlockPointer> - deprecated, array of structs)
// - Field 104: index_storage_infos (vector<IndexStorageInfo> - array of nested objects)
func (r *MetadataReader) readStorageMetadata() (*StorageMetadata, error) {
	storageMeta := &StorageMetadata{
		TablePointer: MetaBlockPointer{BlockID: InvalidBlockID},
		TotalRows:    0,
	}
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			// EOF or other error - end of metadata
			return storageMeta, nil
		}

		if fieldID == ddbFieldCatalogType {
			// Found the next catalog entry (field 99) - don't consume it
			return storageMeta, nil
		}

		if fieldID == ddbFieldTerminator {
			// Consume the terminator and continue
			r.ConsumeField()
			continue
		}

		// Field 0 can indicate padding/uninitialized data at end of metadata - treat as EOF
		if fieldID == 0 {
			return storageMeta, nil
		}

		// Consume the field ID
		r.ConsumeField()

		// Read the field value based on field ID
		switch fieldID {
		case 101:
			// table_pointer: MetaBlockPointer - nested object with field 100 (block_pointer) and 101 (offset)
			// Read the nested MetaBlockPointer object
			tablePtr, readErr := r.readMetaBlockPointer()
			if readErr != nil {
				return nil, fmt.Errorf("failed to read table_pointer: %w", readErr)
			}
			storageMeta.TablePointer = tablePtr

		case 102:
			// total_rows: idx_t (uint64) - varint encoded
			totalRows, readErr := r.ReadVarint()
			if readErr != nil {
				return nil, fmt.Errorf("failed to read total_rows: %w", readErr)
			}
			storageMeta.TotalRows = totalRows

		case 103:
			// index_pointers: vector<BlockPointer> - deprecated array
			// Read array count
			count, readErr := r.ReadVarint()
			if readErr != nil {
				return nil, readErr
			}
			// Each BlockPointer has 2 fields: block_id (uint64) and offset (uint32)
			// Both are nested objects with terminators
			for i := uint64(0); i < count; i++ {
				if err := r.SkipToTerminator(); err != nil {
					return nil, err
				}
			}

		case 104:
			// index_storage_infos: vector<IndexStorageInfo> - array of nested objects
			// Read array count
			count, readErr := r.ReadVarint()
			if readErr != nil {
				return nil, readErr
			}
			// Each IndexStorageInfo is a nested object with terminator
			for i := uint64(0); i < count; i++ {
				if err := r.SkipToTerminator(); err != nil {
					return nil, err
				}
			}

		default:
			// Unknown field >= 100 - try to skip conservatively
			// For fields in the 100+ range, they could be varints or nested objects
			// Try reading a varint first (most common case)
			if _, err := r.ReadVarint(); err != nil {
				// If that fails, it might be a nested object
				// Return the error - we can't safely skip unknown types
				return nil, err
			}
		}
	}
}

// readCreateInfo reads the common CreateInfo fields.
func (r *MetadataReader) readCreateInfo(info *CreateInfo) error {
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return err
		}

		if fieldID == ddbFieldTerminator || fieldID >= 200 {
			// End of CreateInfo fields, don't consume
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropCreateType:
			_, err = r.ReadVarint() // Already know the type
		case ddbPropCreateCatalog:
			info.Catalog, err = r.ReadString()
		case ddbPropCreateSchema:
			info.Schema, err = r.ReadString()
		case ddbPropCreateTemporary:
			info.Temporary, err = r.ReadBool()
		case ddbPropCreateInternal:
			info.Internal, err = r.ReadBool()
		case ddbPropCreateOnConflict:
			v, e := r.ReadVarint()
			info.OnConflict = OnCreateConflict(v)
			err = e
		case ddbPropCreateSQL:
			info.SQL, err = r.ReadString()
		case ddbPropCreateComment:
			// Value is complex - skip it for now
			err = r.skipValue()
		case ddbPropCreateTags:
			// Map of string->string - skip for now
			info.Tags, err = r.readStringMap()
		case ddbPropCreateDeps:
			// Dependencies array - skip for now
			count, e := r.ReadVarint()
			if e != nil {
				err = e
			} else {
				for j := uint64(0); j < count; j++ {
					if e := r.SkipToTerminator(); e != nil {
						err = e
						break
					}
				}
			}
		default:
			// Unknown field - try to skip
			err = r.skipValue()
		}

		if err != nil {
			return fmt.Errorf("readCreateInfo: field %d error: %w", fieldID, err)
		}
	}

	return nil
}

// readStringMap reads a map<string, string>.
func (r *MetadataReader) readStringMap() (map[string]string, error) {
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, count)
	for i := uint64(0); i < count; i++ {
		// Each entry is an object with key (field 0) and value (field 1)
		if err := r.OnPropertyBegin(0); err != nil {
			return nil, err
		}
		key, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		if err := r.OnPropertyBegin(1); err != nil {
			return nil, err
		}
		value, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		// Read object terminator
		term, err := r.NextField()
		if err != nil {
			return nil, err
		}
		if term != ddbFieldTerminator {
			return nil, fmt.Errorf("expected object terminator, got %d", term)
		}

		result[key] = value
	}

	return result, nil
}

// readSchemaEntry reads a schema catalog entry.
func (r *MetadataReader) readSchemaEntry() (*SchemaCatalogEntry, error) {
	schema := &SchemaCatalogEntry{}

	if err := r.readCreateInfo(&schema.CreateInfo); err != nil {
		return nil, err
	}

	// Schema entries only have CreateInfo fields - the schema name is in CreateInfo.Schema
	schema.Name = schema.CreateInfo.Schema

	// Read to terminator
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()
		// Skip any remaining fields
		if err := r.skipValue(); err != nil {
			return nil, err
		}
	}

	// If schema name is empty, use "main" as default
	if schema.Name == "" {
		schema.Name = "main"
	}

	return schema, nil
}

// readTableEntry reads a table catalog entry.
func (r *MetadataReader) readTableEntry() (*TableCatalogEntry, error) {
	table := &TableCatalogEntry{}

	if err := r.readCreateInfo(&table.CreateInfo); err != nil {
		return nil, fmt.Errorf("readTableEntry: readCreateInfo failed: %w", err)
	}

	// Read table-specific fields
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, fmt.Errorf("readTableEntry: PeekField failed: %w", err)
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropTableName:
			table.Name, err = r.ReadString()
		case ddbPropTableColumns:
			table.Columns, err = r.readColumnList()
			if err != nil {
				return nil, fmt.Errorf("readTableEntry: readColumnList failed (field 201): %w", err)
			}
		case ddbPropTableConstraints:
			table.Constraints, err = r.readConstraintList()
		case ddbPropTableQuery:
			// Optional SelectStatement - skip if present
			err = r.skipOptionalNullable()
		default:
			err = fmt.Errorf("readTableEntry: unexpected field %d", fieldID)
		}

		if err != nil {
			return nil, err
		}
	}

	return table, nil
}

// readColumnList reads a ColumnList object.
func (r *MetadataReader) readColumnList() ([]ColumnDefinition, error) {
	// ColumnList has field 100 = columns (vector<ColumnDefinition>)
	fieldID, err := r.PeekField()
	if err != nil {
		return nil, err
	}

	if fieldID != ddbPropColumnListColumns {
		// Maybe empty or different format
		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			return []ColumnDefinition{}, nil
		}
		return nil, fmt.Errorf("expected columns field (100), got %d", fieldID)
	}

	r.ConsumeField()

	// Read vector count
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnDefinition, 0, count)
	for i := uint64(0); i < count; i++ {
		col, err := r.readColumnDefinition()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	// ColumnList IS an object (it has a Serialize method), so it HAS a terminator
	term, err := r.NextField()
	if err != nil {
		return columns, err
	}
	if term != ddbFieldTerminator {
		return nil, fmt.Errorf("expected ColumnList terminator, got %d", term)
	}

	return columns, nil
}

// readColumnDefinition reads a single ColumnDefinition.
func (r *MetadataReader) readColumnDefinition() (ColumnDefinition, error) {
	col := ColumnDefinition{
		Nullable: true, // Default
	}

	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return col, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropColName:
			col.Name, err = r.ReadString()
		case ddbPropColType:
			col.Type, col.TypeModifiers, err = r.readLogicalType()
		case ddbPropColExpression:
			// Optional ParsedExpression - skip
			err = r.skipOptionalNullable()
		case ddbPropColCategory:
			cat, e := r.ReadVarint()
			col.Generated = TableColumnCategory(cat) == TableColumnGenerated
			err = e
		case ddbPropColCompression:
			comp, e := r.ReadVarint()
			col.CompressionType = CompressionType(comp)
			err = e
		case ddbPropColComment:
			// Value - skip by reading through the object (Value has complex serialization)
			err = r.SkipToTerminator()
		case ddbPropColTags:
			// Map of string->string
			_, err = r.readStringMap()
		default:
			err = r.skipValue()
		}

		if err != nil {
			return col, err
		}
	}

	return col, nil
}

// readLogicalType reads a LogicalType object.
func (r *MetadataReader) readLogicalType() (LogicalTypeID, TypeModifiers, error) {
	var typeID LogicalTypeID
	var mods TypeModifiers

	// Read type ID (field 100)
	if err := r.OnPropertyBegin(ddbPropLogicalTypeID); err != nil {
		return typeID, mods, fmt.Errorf("readLogicalType: OnPropertyBegin(100) failed: %w", err)
	}
	id, err := r.ReadVarint()
	if err != nil {
		return typeID, mods, err
	}
	typeID = LogicalTypeID(id)

	// Check for optional type_info (field 101)
	present, err := r.OnOptionalPropertyBegin(ddbPropLogicalTypeInfo)
	if err != nil {
		return typeID, mods, fmt.Errorf("readLogicalType: OnOptionalPropertyBegin(101) failed: %w", err)
	}

	if present {
		// Read ExtraTypeInfo (nullable)
		isPresent, err := r.ReadBool()
		if err != nil {
			return typeID, mods, err
		}
		if isPresent {
			mods, err = r.readExtraTypeInfo()
			if err != nil {
				return typeID, mods, err
			}
		}
	}

	// LogicalType IS an object (has Serialize method), so it HAS a terminator
	term, err := r.NextField() // Use NextField to respect buffered field from PeekField
	if err != nil {
		return typeID, mods, err
	}
	if term != ddbFieldTerminator {
		return typeID, mods, fmt.Errorf("expected LogicalType terminator, got %d (typeID=%d, present101=%v)", term, typeID, present)
	}

	return typeID, mods, nil
}

// readExtraTypeInfo reads ExtraTypeInfo from the stream.
func (r *MetadataReader) readExtraTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read type (field 100)
	if err := r.OnPropertyBegin(ddbPropExtraTypeInfoType); err != nil {
		return mods, err
	}
	typeVal, err := r.ReadVarint()
	if err != nil {
		return mods, err
	}
	extraType := ExtraTypeInfoType(typeVal)

	// Read optional alias (field 101)
	present, err := r.OnOptionalPropertyBegin(ddbPropExtraTypeInfoAlias)
	if err != nil {
		return mods, err
	}
	if present {
		_, err = r.ReadString() // Alias - we don't store it
		if err != nil {
			return mods, err
		}
	}

	// Skip deleted modifiers field (102) if present
	present, err = r.OnOptionalPropertyBegin(ddbPropExtraTypeInfoModifiers)
	if err != nil {
		return mods, err
	}
	if present {
		// Vector of Values - skip
		count, err := r.ReadVarint()
		if err != nil {
			return mods, err
		}
		for i := uint64(0); i < count; i++ {
			if err := r.SkipToTerminator(); err != nil {
				return mods, err
			}
		}
	}

	// Skip optional extension_info (field 103)
	present, err = r.OnOptionalPropertyBegin(ddbPropExtraTypeInfoExtension)
	if err != nil {
		return mods, err
	}
	if present {
		if err := r.skipOptionalNullable(); err != nil {
			return mods, err
		}
	}

	// Read type-specific fields based on extraType
	switch extraType {
	case ExtraTypeInfoDecimal:
		mods, err = r.readDecimalTypeInfo()
	case ExtraTypeInfoList:
		mods, err = r.readListTypeInfo()
	case ExtraTypeInfoArray:
		mods, err = r.readArrayTypeInfo()
	case ExtraTypeInfoStruct:
		mods, err = r.readStructTypeInfo()
	case ExtraTypeInfoString:
		mods, err = r.readStringTypeInfo()
	case ExtraTypeInfoEnum:
		mods, err = r.readEnumTypeInfo()
	default:
		// For other types, just read to terminator
		err = r.SkipToTerminator()
	}

	return mods, err
}

// readDecimalTypeInfo reads DecimalTypeInfo fields.
func (r *MetadataReader) readDecimalTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read width (field 200)
	present, err := r.OnOptionalPropertyBegin(ddbPropDecimalWidth)
	if err != nil {
		return mods, err
	}
	if present {
		width, err := r.ReadVarint()
		if err != nil {
			return mods, err
		}
		mods.Width = uint8(width)
	}

	// Read scale (field 201)
	present, err = r.OnOptionalPropertyBegin(ddbPropDecimalScale)
	if err != nil {
		return mods, err
	}
	if present {
		scale, err := r.ReadVarint()
		if err != nil {
			return mods, err
		}
		mods.Scale = uint8(scale)
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return mods, err
	}
	if term != ddbFieldTerminator {
		return mods, fmt.Errorf("expected DecimalTypeInfo terminator, got %d", term)
	}

	return mods, nil
}

// readListTypeInfo reads ListTypeInfo fields.
func (r *MetadataReader) readListTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read child_type (field 200)
	if err := r.OnPropertyBegin(ddbPropListChildType); err != nil {
		return mods, err
	}

	childType, childMods, err := r.readLogicalType()
	if err != nil {
		return mods, err
	}

	mods.ChildTypeID = childType
	if childMods.Width != 0 || childMods.Scale != 0 {
		mods.ChildType = &childMods
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return mods, err
	}
	if term != ddbFieldTerminator {
		return mods, fmt.Errorf("expected ListTypeInfo terminator, got %d", term)
	}

	return mods, nil
}

// readArrayTypeInfo reads ArrayTypeInfo fields.
func (r *MetadataReader) readArrayTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read child_type (field 200)
	if err := r.OnPropertyBegin(ddbPropArrayChildType); err != nil {
		return mods, err
	}

	childType, childMods, err := r.readLogicalType()
	if err != nil {
		return mods, err
	}

	mods.ChildTypeID = childType
	if childMods.Width != 0 || childMods.Scale != 0 {
		mods.ChildType = &childMods
	}

	// Read size (field 201)
	present, err := r.OnOptionalPropertyBegin(ddbPropArraySize)
	if err != nil {
		return mods, err
	}
	if present {
		size, err := r.ReadVarint()
		if err != nil {
			return mods, err
		}
		mods.Length = uint32(size)
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return mods, err
	}
	if term != ddbFieldTerminator {
		return mods, fmt.Errorf("expected ArrayTypeInfo terminator, got %d", term)
	}

	return mods, nil
}

// readStructTypeInfo reads StructTypeInfo fields.
func (r *MetadataReader) readStructTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read child_types (field 200) - this is a vector of pairs (name, LogicalType)
	present, err := r.OnOptionalPropertyBegin(ddbPropStructChildTypes)
	if err != nil {
		return mods, err
	}
	if present {
		count, err := r.ReadVarint()
		if err != nil {
			return mods, err
		}

		mods.StructFields = make([]StructField, 0, count)
		for i := uint64(0); i < count; i++ {
			// Each element is a pair {first: string, second: LogicalType}
			if err := r.OnPropertyBegin(0); err != nil { // first
				return mods, err
			}
			name, err := r.ReadString()
			if err != nil {
				return mods, err
			}

			if err := r.OnPropertyBegin(1); err != nil { // second
				return mods, err
			}
			fieldType, fieldMods, err := r.readLogicalType()
			if err != nil {
				return mods, err
			}

			// Read pair terminator
			term, err := r.NextField()
			if err != nil {
				return mods, err
			}
			if term != ddbFieldTerminator {
				return mods, fmt.Errorf("expected pair terminator, got %d", term)
			}

			field := StructField{
				Name: name,
				Type: fieldType,
			}
			if fieldMods.Width != 0 || fieldMods.Scale != 0 {
				field.TypeModifiers = &fieldMods
			}
			mods.StructFields = append(mods.StructFields, field)
		}
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return mods, err
	}
	if term != ddbFieldTerminator {
		return mods, fmt.Errorf("expected StructTypeInfo terminator, got %d", term)
	}

	return mods, nil
}

// readStringTypeInfo reads StringTypeInfo fields.
func (r *MetadataReader) readStringTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// Read collation (field 200)
	present, err := r.OnOptionalPropertyBegin(200)
	if err != nil {
		return mods, err
	}
	if present {
		mods.Collation, err = r.ReadString()
		if err != nil {
			return mods, err
		}
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return mods, err
	}
	if term != ddbFieldTerminator {
		return mods, fmt.Errorf("expected StringTypeInfo terminator, got %d", term)
	}

	return mods, nil
}

// readEnumTypeInfo reads EnumTypeInfo fields.
func (r *MetadataReader) readEnumTypeInfo() (TypeModifiers, error) {
	var mods TypeModifiers

	// EnumTypeInfo has special serialization - it stores the enum values
	// For now, skip to terminator
	if err := r.SkipToTerminator(); err != nil {
		return mods, err
	}

	return mods, nil
}

// readConstraintList reads a vector of Constraints.
func (r *MetadataReader) readConstraintList() ([]Constraint, error) {
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	constraints := make([]Constraint, 0, count)
	for i := uint64(0); i < count; i++ {
		// Each constraint in the vector is wrapped with a nullable bool
		isPresent, err := r.ReadBool()
		if err != nil {
			return constraints, err
		}

		if isPresent {
			constraint, err := r.readConstraint()
			if err != nil {
				return constraints, err
			}
			if constraint != nil {
				constraints = append(constraints, *constraint)
			}
		}
		// Note: The constraint's own terminator (read by readConstraint)
		// serves as the only terminator. No additional wrapper terminator.
	}

	return constraints, nil
}

// readConstraint reads a single Constraint.
func (r *MetadataReader) readConstraint() (*Constraint, error) {
	// Read type (field 100)
	if err := r.OnPropertyBegin(ddbPropConstraintType); err != nil {
		return nil, err
	}
	typeVal, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	constraintType := ddbConstraintType(typeVal)
	constraint := &Constraint{}

	switch constraintType {
	case ddbConstraintNotNull:
		constraint.Type = ConstraintTypeNotNull
		// Read index (field 200)
		if err := r.OnPropertyBegin(ddbPropNotNullIndex); err != nil {
			return nil, err
		}
		idx, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}
		constraint.ColumnIndices = []uint64{idx}

	case ddbConstraintUnique:
		// Read is_primary_key (field 200)
		present, err := r.OnOptionalPropertyBegin(ddbPropUniquePrimaryKey)
		if err != nil {
			return nil, err
		}
		isPrimary := false
		if present {
			isPrimary, err = r.ReadBool()
			if err != nil {
				return nil, err
			}
		}
		if isPrimary {
			constraint.Type = ConstraintTypePrimaryKey
		} else {
			constraint.Type = ConstraintTypeUnique
		}

		// Read index (field 201)
		if err := r.OnPropertyBegin(ddbPropUniqueIndex); err != nil {
			return nil, err
		}
		idx, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}
		constraint.ColumnIndices = []uint64{idx}

		// Read columns (field 202)
		present, err = r.OnOptionalPropertyBegin(ddbPropUniqueColumns)
		if err != nil {
			return nil, err
		}
		if present {
			count, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			for j := uint64(0); j < count; j++ {
				_, err := r.ReadString() // Column names
				if err != nil {
					return nil, err
				}
			}
		}

	case ddbConstraintCheck:
		constraint.Type = ConstraintTypeCheck
		// Read expression (field 200)
		present, err := r.OnOptionalPropertyBegin(ddbPropCheckExpression)
		if err != nil {
			return nil, err
		}
		if present {
			// Skip the expression - it's a complex ParsedExpression
			if err := r.skipOptionalNullable(); err != nil {
				return nil, err
			}
		}

	case ddbConstraintForeignKey:
		constraint.Type = ConstraintTypeForeignKey
		fkInfo := &ForeignKeyInfo{}

		// Read pk_columns (field 200)
		present, err := r.OnOptionalPropertyBegin(ddbPropFKPKColumns)
		if err != nil {
			return nil, err
		}
		if present {
			count, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			for j := uint64(0); j < count; j++ {
				_, err := r.ReadString()
				if err != nil {
					return nil, err
				}
			}
		}

		// Read fk_columns (field 201)
		present, err = r.OnOptionalPropertyBegin(ddbPropFKFKColumns)
		if err != nil {
			return nil, err
		}
		if present {
			count, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			for j := uint64(0); j < count; j++ {
				_, err := r.ReadString()
				if err != nil {
					return nil, err
				}
			}
		}

		// Read fk_type (field 202)
		if err := r.OnPropertyBegin(ddbPropFKType); err != nil {
			return nil, err
		}
		_, err = r.ReadVarint()
		if err != nil {
			return nil, err
		}

		// Read schema (field 203)
		present, err = r.OnOptionalPropertyBegin(ddbPropFKSchema)
		if err != nil {
			return nil, err
		}
		if present {
			fkInfo.ReferencedSchema, err = r.ReadString()
			if err != nil {
				return nil, err
			}
		}

		// Read table (field 204)
		present, err = r.OnOptionalPropertyBegin(ddbPropFKTable)
		if err != nil {
			return nil, err
		}
		if present {
			fkInfo.ReferencedTable, err = r.ReadString()
			if err != nil {
				return nil, err
			}
		}

		// Read pk_keys (field 205)
		present, err = r.OnOptionalPropertyBegin(ddbPropFKPKKeys)
		if err != nil {
			return nil, err
		}
		if present {
			count, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			for j := uint64(0); j < count; j++ {
				_, err := r.ReadVarint() // PhysicalIndex
				if err != nil {
					return nil, err
				}
			}
		}

		// Read fk_keys (field 206)
		present, err = r.OnOptionalPropertyBegin(ddbPropFKFKKeys)
		if err != nil {
			return nil, err
		}
		if present {
			count, err := r.ReadVarint()
			if err != nil {
				return nil, err
			}
			for j := uint64(0); j < count; j++ {
				_, err := r.ReadVarint() // PhysicalIndex
				if err != nil {
					return nil, err
				}
			}
		}

		constraint.ForeignKey = fkInfo

	default:
		// Unknown constraint type - skip to terminator
		if err := r.SkipToTerminator(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Read terminator
	term, err := r.NextField()
	if err != nil {
		return constraint, err
	}
	if term != ddbFieldTerminator {
		return nil, fmt.Errorf("expected Constraint terminator, got %d", term)
	}

	return constraint, nil
}

// readViewEntry reads a view catalog entry.
func (r *MetadataReader) readViewEntry() (*ViewCatalogEntry, error) {
	view := &ViewCatalogEntry{}

	if err := r.readCreateInfo(&view.CreateInfo); err != nil {
		return nil, err
	}

	// Read view-specific fields
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropViewName:
			view.Name, err = r.ReadString()
		case ddbPropViewAliases:
			view.Aliases, err = r.readStringVector()
		case ddbPropViewTypes:
			view.Types, err = r.readLogicalTypeVector()
		case ddbPropViewQuery:
			// SelectStatement - skip
			err = r.skipOptionalNullable()
		case ddbPropViewNames:
			_, err = r.readStringVector()
		case ddbPropViewColumnComments:
			// Vector of Values - skip
			count, e := r.ReadVarint()
			if e != nil {
				err = e
			} else {
				for j := uint64(0); j < count; j++ {
					if e := r.SkipToTerminator(); e != nil {
						err = e
						break
					}
				}
			}
		default:
			err = r.skipValue()
		}

		if err != nil {
			return nil, err
		}
	}

	return view, nil
}

// readStringVector reads a vector of strings.
func (r *MetadataReader) readStringVector() ([]string, error) {
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		s, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}

	return result, nil
}

// readLogicalTypeVector reads a vector of LogicalTypes.
func (r *MetadataReader) readLogicalTypeVector() ([]LogicalTypeID, error) {
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	result := make([]LogicalTypeID, 0, count)
	for i := uint64(0); i < count; i++ {
		typeID, _, err := r.readLogicalType()
		if err != nil {
			return nil, err
		}
		result = append(result, typeID)
	}

	return result, nil
}

// readIndexEntry reads an index catalog entry.
func (r *MetadataReader) readIndexEntry() (*IndexCatalogEntry, error) {
	index := &IndexCatalogEntry{}

	if err := r.readCreateInfo(&index.CreateInfo); err != nil {
		return nil, err
	}

	// Read index-specific fields
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropIndexName:
			index.Name, err = r.ReadString()
		case ddbPropIndexTableName:
			index.TableName, err = r.ReadString()
		case ddbPropIndexTypeOld:
			// Deleted field - skip
			_, err = r.ReadVarint()
		case ddbPropIndexConstraint:
			constraint, e := r.ReadVarint()
			index.Constraint = IndexConstraintType(constraint)
			err = e
		case ddbPropIndexExprs:
			// Vector of ParsedExpression - skip
			count, e := r.ReadVarint()
			if e != nil {
				err = e
			} else {
				for j := uint64(0); j < count; j++ {
					if e := r.skipOptionalNullable(); e != nil {
						err = e
						break
					}
				}
			}
		case ddbPropIndexScanTypes:
			_, err = r.readLogicalTypeVector()
		case ddbPropIndexNames:
			_, err = r.readStringVector()
		case ddbPropIndexColumnIDs:
			index.ColumnIDs, err = r.readUint64Vector()
		case ddbPropIndexOptions:
			// Map - skip
			_, err = r.readStringMap()
		case ddbPropIndexTypeName:
			typeName, e := r.ReadString()
			if e != nil {
				err = e
			} else if typeName == "ART" {
				index.IndexType = IndexTypeART
			} else {
				index.IndexType = IndexTypeHash
			}
		default:
			err = r.skipValue()
		}

		if err != nil {
			return nil, err
		}
	}

	return index, nil
}

// readUint64Vector reads a vector of uint64 values.
func (r *MetadataReader) readUint64Vector() ([]uint64, error) {
	count, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	result := make([]uint64, 0, count)
	for i := uint64(0); i < count; i++ {
		v, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}

	return result, nil
}

// readSequenceEntry reads a sequence catalog entry.
func (r *MetadataReader) readSequenceEntry() (*SequenceCatalogEntry, error) {
	seq := &SequenceCatalogEntry{}

	if err := r.readCreateInfo(&seq.CreateInfo); err != nil {
		return nil, err
	}

	// Read sequence-specific fields
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropSeqName:
			seq.Name, err = r.ReadString()
		case ddbPropSeqUsageCount:
			_, err = r.ReadVarint() // usage_count
		case ddbPropSeqIncrement:
			seq.Increment, err = r.ReadSignedVarint()
		case ddbPropSeqMinValue:
			seq.MinValue, err = r.ReadSignedVarint()
		case ddbPropSeqMaxValue:
			seq.MaxValue, err = r.ReadSignedVarint()
		case ddbPropSeqStartValue:
			seq.StartWith, err = r.ReadSignedVarint()
		case ddbPropSeqCycle:
			seq.Cycle, err = r.ReadBool()
		default:
			err = r.skipValue()
		}

		if err != nil {
			return nil, err
		}
	}

	return seq, nil
}

// readTypeEntry reads a type catalog entry.
func (r *MetadataReader) readTypeEntry() (*TypeCatalogEntry, error) {
	typeEntry := &TypeCatalogEntry{}

	if err := r.readCreateInfo(&typeEntry.CreateInfo); err != nil {
		return nil, err
	}

	// Read type-specific fields
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return nil, err
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			break
		}

		r.ConsumeField()

		switch fieldID {
		case ddbPropTypeEntryName:
			typeEntry.Name, err = r.ReadString()
		case ddbPropTypeEntryType:
			typeEntry.TypeID, typeEntry.TypeModifiers, err = r.readLogicalType()
		default:
			err = r.skipValue()
		}

		if err != nil {
			return nil, err
		}
	}

	return typeEntry, nil
}

// ReadCatalogFromMetadata reads the full catalog from the metadata block.
func ReadCatalogFromMetadata(bm *BlockManager, metaBlockID uint64) (*DuckDBCatalog, error) {
	reader, err := NewMetadataReader(bm, metaBlockID)
	if err != nil {
		return nil, err
	}

	entries, err := reader.ReadCatalogEntries()
	if err != nil {
		return nil, fmt.Errorf("failed to read catalog entries: %w", err)
	}

	catalog := NewDuckDBCatalog()

	// Process entries in order - schemas first, then other objects
	for _, entry := range entries {
		if schema, ok := entry.(*SchemaCatalogEntry); ok {
			catalog.AddSchema(schema)
		}
	}

	// Then tables, views, etc.
	for _, entry := range entries {
		switch e := entry.(type) {
		case *TableCatalogEntry:
			catalog.AddTable(e)
		case *ViewCatalogEntry:
			catalog.AddView(e)
		case *IndexCatalogEntry:
			catalog.AddIndex(e)
		case *SequenceCatalogEntry:
			catalog.AddSequence(e)
		case *TypeCatalogEntry:
			catalog.AddType(e)
		}
	}

	return catalog, nil
}

// ReadColumnDataPointer reads ColumnData metadata from a MetaBlockPointer and extracts
// the first DataPointer. ColumnData is the serialization format used in RowGroupPointer.data_pointers.
//
// ColumnData (PersistentColumnData) format in BinarySerializer:
// - Field 100: data_pointers (vector<DataPointer>) - the actual segment pointers
// - Field 101: validity (child ColumnData) - for nullable columns
//
// Each DataPointer has:
// - Field 100: row_start (optional, usually 0 and omitted)
// - Field 101: tuple_count
// - Field 102: block_pointer (nested BlockPointer with fields 100, 101)
// - Field 103: compression_type
// - Field 104: statistics (optional)
// - Field 105: segment_state (optional)
// - Terminator 0xFFFF
func ReadColumnDataPointer(bm *BlockManager, mbp MetaBlockPointer) (*DataPointer, error) {
	// Create a metadata reader at the ColumnData location
	encodedPointer := mbp.Encode()
	reader, err := NewMetadataReaderWithOffset(bm, encodedPointer, mbp.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata reader for ColumnData: %w", err)
	}

	// Read Field 100: data_pointers (vector<DataPointer>)
	if err := reader.OnPropertyBegin(100); err != nil {
		return nil, fmt.Errorf("expected field 100 for data_pointers: %w", err)
	}

	// Read the count of data pointers
	dataPointerCount, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read data_pointers count: %w", err)
	}

	if dataPointerCount == 0 {
		return nil, fmt.Errorf("ColumnData has no data_pointers")
	}

	// Read the first DataPointer
	dp, err := readDataPointer(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read DataPointer: %w", err)
	}

	// Skip remaining DataPointers if count > 1
	for i := uint64(1); i < dataPointerCount; i++ {
		if _, err := readDataPointer(reader); err != nil {
			return nil, fmt.Errorf("failed to skip DataPointer %d: %w", i, err)
		}
	}

	// Check for field 101 (validity child ColumnData) or terminator
	fieldID, err := reader.PeekField()
	if err != nil {
		// EOF or error, just return what we have
		return dp, nil
	}

	if fieldID == 101 {
		// Read field 101: validity child ColumnData
		reader.ConsumeField()

		// The validity is a nested ColumnData structure, read its data_pointers
		validityDP, err := readValidityColumnData(reader)
		if err != nil {
			// Log but don't fail - validity is optional
			// Some columns may not have validity even with has_null=true
			return dp, nil
		}
		dp.ValidityPointer = validityDP

		// Mark that this column has a validity mask
		dp.SegmentState.HasValidityMask = true
	}

	return dp, nil
}

// readValidityColumnData reads a nested validity ColumnData structure.
// Validity ColumnData format:
// - Field 100: data_pointers (vector<DataPointer>) - validity mask location
// - Terminator 0xFFFF
func readValidityColumnData(reader *MetadataReader) (*DataPointer, error) {
	// Read Field 100: data_pointers
	if err := reader.OnPropertyBegin(100); err != nil {
		return nil, fmt.Errorf("expected field 100 for validity data_pointers: %w", err)
	}

	// Read count
	count, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read validity data_pointers count: %w", err)
	}

	if count == 0 {
		return nil, fmt.Errorf("validity ColumnData has no data_pointers")
	}

	// Read the first validity DataPointer
	validityDP, err := readDataPointer(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read validity DataPointer: %w", err)
	}

	// Skip remaining DataPointers if count > 1
	for i := uint64(1); i < count; i++ {
		if _, err := readDataPointer(reader); err != nil {
			// Just skip errors for additional pointers
			break
		}
	}

	// Skip to terminator
	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			break
		}
		reader.ConsumeField()
		if fieldID == ddbFieldTerminator {
			break
		}
		// Skip unknown fields
		if _, err := reader.ReadVarint(); err != nil {
			break
		}
	}

	return validityDP, nil
}

// readDataPointer reads a single DataPointer from the reader.
// DataPointer format:
// - Field 100: row_start (optional, default 0)
// - Field 101: tuple_count
// - Field 102: block_pointer (BlockPointer)
// - Field 103: compression_type
// - Field 104: statistics (optional)
// - Field 105: segment_state (optional)
// - Terminator 0xFFFF
func readDataPointer(reader *MetadataReader) (*DataPointer, error) {
	dp := &DataPointer{}

	// Peek first field - could be 100 (row_start) or 101 (tuple_count) if row_start is omitted
	fieldID, err := reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek first field: %w", err)
	}

	// Handle optional Field 100: row_start
	if fieldID == 100 {
		reader.ConsumeField()
		dp.RowStart, err = reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read row_start: %w", err)
		}
		fieldID, err = reader.PeekField()
		if err != nil {
			return nil, fmt.Errorf("failed to peek after row_start: %w", err)
		}
	}

	// Read Field 101: tuple_count
	if fieldID != 101 {
		return nil, fmt.Errorf("expected field 101 for tuple_count, got %d", fieldID)
	}
	reader.ConsumeField()
	dp.TupleCount, err = reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read tuple_count: %w", err)
	}

	// Read Field 102: block_pointer (nested BlockPointer)
	if err := reader.OnPropertyBegin(102); err != nil {
		return nil, fmt.Errorf("expected field 102 for block_pointer: %w", err)
	}

	// BlockPointer has field 100 (block_id) and optional field 101 (offset, defaults to 0)
	if err := reader.OnPropertyBegin(100); err != nil {
		return nil, fmt.Errorf("expected field 100 for block_id in BlockPointer: %w", err)
	}
	dp.Block.BlockID, err = reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read block_id: %w", err)
	}

	// Check for optional offset field or terminator
	fieldID, err = reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek for offset in BlockPointer: %w", err)
	}

	if fieldID == 101 {
		// Read optional offset field
		reader.ConsumeField()
		offset, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read offset: %w", err)
		}
		dp.Block.Offset = uint32(offset)

		// Read terminator
		fieldID, err = reader.PeekField()
		if err != nil {
			return nil, fmt.Errorf("failed to peek BlockPointer terminator: %w", err)
		}
	} else {
		// No offset field, default to 0
		dp.Block.Offset = 0
	}

	// Read BlockPointer terminator
	if fieldID != ddbFieldTerminator {
		return nil, fmt.Errorf("expected BlockPointer terminator, got field %d", fieldID)
	}
	reader.ConsumeField()

	// Read Field 103: compression_type
	if err := reader.OnPropertyBegin(103); err != nil {
		return nil, fmt.Errorf("expected field 103 for compression_type: %w", err)
	}
	compressionType, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read compression_type: %w", err)
	}
	dp.Compression = CompressionType(compressionType)

	// Read remaining optional fields (statistics, segment_state) until terminator
	for {
		fieldID, err = reader.PeekField()
		if err != nil {
			return nil, fmt.Errorf("failed to peek for optional fields: %w", err)
		}

		if fieldID == ddbFieldTerminator {
			reader.ConsumeField()
			break
		}

		reader.ConsumeField()
		switch fieldID {
		case 104: // statistics
			// Read statistics - important for CONSTANT compression
			if err := readStatisticsInto(reader, &dp.Statistics); err != nil {
				return nil, fmt.Errorf("failed to read statistics: %w", err)
			}
		case 105: // segment_state
			// Read segment state for validity mask info
			if err := readSegmentStateInto(reader, &dp.SegmentState); err != nil {
				return nil, fmt.Errorf("failed to read segment_state: %w", err)
			}
		default:
			// Unknown field, try to skip as varint
			if _, err := reader.ReadVarint(); err != nil {
				return nil, fmt.Errorf("failed to skip unknown field %d: %w", fieldID, err)
			}
		}
	}

	return dp, nil
}

// skipNestedStructure skips a nested BinarySerializer structure by reading until terminator.
// This function handles the BinarySerializer format where objects are terminated by 0xFFFF.
// Some fields may contain nested objects or lists which also need to be skipped.
func skipNestedStructure(reader *MetadataReader) error {
	// Read fields until we hit the terminator for this structure
	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			// If we hit EOF while reading, just return - the structure may be at the end
			return nil
		}
		reader.ConsumeField()

		if fieldID == ddbFieldTerminator {
			return nil
		}

		// Skip the field value
		// Most statistics fields are simple varints or nested objects
		switch fieldID {
		case 103: // type_stats in BaseStatistics - nested object
			// This is a nested object, recursively skip it
			if err := skipNestedStructure(reader); err != nil {
				return err
			}
		default:
			// Try to read as varint - this works for most simple fields
			// If it fails, we might have a more complex structure
			if _, err := reader.ReadVarint(); err != nil {
				// Try to recover by skipping until terminator
				return skipToTerminator(reader)
			}
		}
	}
}

// skipToTerminator reads and discards bytes until we find a terminator.
// This is a recovery mechanism for unexpected field formats.
func skipToTerminator(reader *MetadataReader) error {
	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			return nil // EOF is acceptable
		}
		reader.ConsumeField()

		if fieldID == ddbFieldTerminator {
			return nil
		}

		// Try to skip any value as varint
		_, _ = reader.ReadVarint()
	}
}

// readStatisticsInto reads statistics from a nested structure into BaseStatistics.
// Statistics format in BinarySerializer:
// - Field 100: has_stats (bool)
// - Field 101: has_null (bool)
// - Field 102: type_stats (nested - contains min/max for numeric types)
// - Terminator 0xFFFF
//
// For CONSTANT compression, the type_stats contains the constant value as min (and max).
func readStatisticsInto(reader *MetadataReader, stats *BaseStatistics) error {
	// Collect raw bytes for statistics data that we may need later
	var rawStatBytes []byte

	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			return nil // EOF is acceptable at end of statistics
		}

		if fieldID == ddbFieldTerminator {
			reader.ConsumeField()
			break
		}

		reader.ConsumeField()
		switch fieldID {
		case 100: // has_stats
			val, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read has_stats: %w", err)
			}
			stats.HasStats = (val != 0)
		case 101: // has_null
			val, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read has_null: %w", err)
			}
			stats.HasNull = (val != 0)
		case 102: // type_stats - contains min/max values
			// Read the nested type_stats structure and extract min/max bytes
			statBytes, err := readTypeStats(reader)
			if err != nil {
				return fmt.Errorf("failed to read type_stats: %w", err)
			}
			rawStatBytes = append(rawStatBytes, statBytes...)
		default:
			// Skip unknown field
			if _, err := reader.ReadVarint(); err != nil {
				// Try to skip to terminator
				return skipToTerminator(reader)
			}
		}
	}

	if len(rawStatBytes) > 0 {
		stats.StatData = rawStatBytes
	}
	return nil
}

// readTypeStats reads a type_stats nested structure and returns the raw min/max bytes.
// NumericStats format in DuckDB BinarySerializer:
// - Field 100: has_min (bool)
// - Field 101: min_value (raw value, typically as varint for integers)
// - Field 102: has_max (bool)
// - Field 103: max_value (raw value)
// - Terminator 0xFFFF
func readTypeStats(reader *MetadataReader) ([]byte, error) {
	var result []byte
	var hasMin bool

	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			return result, nil // EOF acceptable
		}

		if fieldID == ddbFieldTerminator {
			reader.ConsumeField()
			return result, nil
		}

		reader.ConsumeField()
		switch fieldID {
		case 100: // has_min
			val, err := reader.ReadVarint()
			if err != nil {
				return result, err
			}
			hasMin = (val != 0)
		case 101: // min_value - this is what we need for CONSTANT compression
			if hasMin {
				// Read the min value - for integers it's stored as a varint
				val, err := reader.ReadVarint()
				if err != nil {
					return result, err
				}
				// Store as 4-byte little-endian (for int32 types)
				// TODO: handle different sizes based on type
				valBytes := make([]byte, 4)
				valBytes[0] = byte(val)
				valBytes[1] = byte(val >> 8)
				valBytes[2] = byte(val >> 16)
				valBytes[3] = byte(val >> 24)
				result = append(result, valBytes...)
			} else {
				// Skip if no min
				_, _ = reader.ReadVarint()
			}
		case 102: // has_max
			_, err := reader.ReadVarint()
			if err != nil {
				return result, err
			}
		case 103: // max_value
			// Skip max value (same as min for CONSTANT)
			_, _ = reader.ReadVarint()
		default:
			// Skip unknown field
			if _, err := reader.ReadVarint(); err != nil {
				return result, skipToTerminator(reader)
			}
		}
	}
}

// readStatValueBytes reads raw value bytes from statistics.
// For numeric types, this is typically a fixed-size value (4 or 8 bytes).
// The value is stored as raw bytes, not as a varint.
func readStatValueBytes(reader *MetadataReader) ([]byte, error) {
	// Statistics values are stored as raw bytes prefixed with a length
	// Read the length
	length, err := reader.ReadVarint()
	if err != nil {
		// Try reading as a fixed 4-byte value (common for int32)
		// This is a fallback for older formats
		result := make([]byte, 4)
		for i := 0; i < 4; i++ {
			b, err := reader.ReadUint8()
			if err != nil {
				return nil, err
			}
			result[i] = b
		}
		return result, nil
	}

	if length == 0 {
		return nil, nil
	}

	// Read the raw bytes
	result := make([]byte, length)
	for i := uint64(0); i < length; i++ {
		b, err := reader.ReadUint8()
		if err != nil {
			return result[:i], nil // Return what we have
		}
		result[i] = b
	}
	return result, nil
}

// readSegmentStateInto reads segment state from a nested structure.
// SegmentState format in BinarySerializer:
// - Field 100: has_validity_mask (bool)
// - Field 101: validity_block_id (if has_validity_mask)
// - Field 102: validity_offset (if has_validity_mask)
// - Field 103: state_data_len + state_data bytes
// - Terminator 0xFFFF
func readSegmentStateInto(reader *MetadataReader, state *ColumnSegmentState) error {
	for {
		fieldID, err := reader.PeekField()
		if err != nil {
			return nil // EOF acceptable
		}

		if fieldID == ddbFieldTerminator {
			reader.ConsumeField()
			return nil
		}

		reader.ConsumeField()
		switch fieldID {
		case 100: // has_validity_mask
			val, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read has_validity_mask: %w", err)
			}
			state.HasValidityMask = (val != 0)
		case 101: // validity_block_id
			blockID, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read validity_block_id: %w", err)
			}
			state.ValidityBlock.BlockID = blockID
		case 102: // validity_offset
			offset, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read validity_offset: %w", err)
			}
			state.ValidityBlock.Offset = uint32(offset)
		case 103: // state_data
			length, err := reader.ReadVarint()
			if err != nil {
				return fmt.Errorf("failed to read state_data length: %w", err)
			}
			if length > 0 {
				state.StateData = make([]byte, length)
				for i := uint64(0); i < length; i++ {
					b, err := reader.ReadUint8()
					if err != nil {
						return fmt.Errorf("failed to read state_data byte %d: %w", i, err)
					}
					state.StateData[i] = b
				}
			}
		default:
			// Skip unknown field
			if _, err := reader.ReadVarint(); err != nil {
				return skipToTerminator(reader)
			}
		}
	}
}

// ReadDataPointerFromMetadata reads a DataPointer from a MetaBlockPointer location.
// The segment metadata is stored in BinarySerializer format, not as a simple binary struct.
//
// ColumnSegment format in BinarySerializer:
// - Field 100: tuple_count (uint64 varint)
// - Field 101: block_id (uint64 varint)
// - Field 102: offset (uint32 varint)
// - Field 103: compression_type (uint8 varint)
// - Field 104: has_statistics (bool - stored as varint 0/1)
// - If has_statistics:
//   - Field 105: has_null (bool)
//   - Field 106: null_count (uint64)
//   - Field 107: distinct_count (uint64)
//   - Field 108: stat_data_len (uint32) + stat_data bytes
// - Field 109: has_validity_mask (bool)
// - If has_validity_mask:
//   - Field 110: validity_block_id (uint64)
//   - Field 111: validity_offset (uint32)
// - Field 112: state_data_len (uint32) + state_data bytes
// - Terminator 0xFFFF
func ReadDataPointerFromMetadata(bm *BlockManager, mbp MetaBlockPointer) (*DataPointer, error) {
	// Create a metadata reader at the pointer location
	encodedPointer := mbp.Encode()
	reader, err := NewMetadataReaderWithOffset(bm, encodedPointer, mbp.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata reader: %w", err)
	}

	dp := &DataPointer{}

	// Read Field 100: tuple_count
	if err := reader.OnPropertyBegin(100); err != nil {
		return nil, fmt.Errorf("expected field 100 for tuple_count: %w", err)
	}
	dp.TupleCount, err = reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read tuple_count: %w", err)
	}

	// Read Field 101: block_id
	if err := reader.OnPropertyBegin(101); err != nil {
		return nil, fmt.Errorf("expected field 101 for block_id: %w", err)
	}
	dp.Block.BlockID, err = reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read block_id: %w", err)
	}

	// Read Field 102: offset
	if err := reader.OnPropertyBegin(102); err != nil {
		return nil, fmt.Errorf("expected field 102 for offset: %w", err)
	}
	offset, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read offset: %w", err)
	}
	dp.Block.Offset = uint32(offset)

	// Read Field 103: compression_type
	if err := reader.OnPropertyBegin(103); err != nil {
		return nil, fmt.Errorf("expected field 103 for compression_type: %w", err)
	}
	compressionType, err := reader.ReadVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read compression_type: %w", err)
	}
	dp.Compression = CompressionType(compressionType)

	// Peek at next field to see if we have statistics
	fieldID, err := reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek after compression_type: %w", err)
	}

	// Read Field 104: has_statistics (optional)
	if fieldID == 104 {
		reader.ConsumeField()
		hasStats, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read has_statistics: %w", err)
		}
		dp.Statistics.HasStats = (hasStats != 0)

		if dp.Statistics.HasStats {
			// Read Field 105: has_null
			if err := reader.OnPropertyBegin(105); err != nil {
				return nil, fmt.Errorf("expected field 105 for has_null: %w", err)
			}
			hasNull, err := reader.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read has_null: %w", err)
			}
			dp.Statistics.HasNull = (hasNull != 0)

			// Read Field 106: null_count
			if err := reader.OnPropertyBegin(106); err != nil {
				return nil, fmt.Errorf("expected field 106 for null_count: %w", err)
			}
			dp.Statistics.NullCount, err = reader.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read null_count: %w", err)
			}

			// Read Field 107: distinct_count
			if err := reader.OnPropertyBegin(107); err != nil {
				return nil, fmt.Errorf("expected field 107 for distinct_count: %w", err)
			}
			dp.Statistics.DistinctCount, err = reader.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read distinct_count: %w", err)
			}

			// Peek for Field 108: stat_data_len
			fieldID, err = reader.PeekField()
			if err != nil {
				return nil, fmt.Errorf("failed to peek for stat_data: %w", err)
			}
			if fieldID == 108 {
				reader.ConsumeField()
				statDataLen, err := reader.ReadVarint()
				if err != nil {
					return nil, fmt.Errorf("failed to read stat_data_len: %w", err)
				}
				if statDataLen > 0 {
					dp.Statistics.StatData = make([]byte, statDataLen)
					for i := uint64(0); i < statDataLen; i++ {
						b, err := reader.ReadUint8()
						if err != nil {
							return nil, fmt.Errorf("failed to read stat_data byte %d: %w", i, err)
						}
						dp.Statistics.StatData[i] = b
					}
				}
			}
		}
	}

	// Peek for Field 109: has_validity_mask
	fieldID, err = reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek for has_validity_mask: %w", err)
	}

	if fieldID == 109 {
		reader.ConsumeField()
		hasValidityMask, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read has_validity_mask: %w", err)
		}
		dp.SegmentState.HasValidityMask = (hasValidityMask != 0)

		if dp.SegmentState.HasValidityMask {
			// Read Field 110: validity_block_id
			if err := reader.OnPropertyBegin(110); err != nil {
				return nil, fmt.Errorf("expected field 110 for validity_block_id: %w", err)
			}
			dp.SegmentState.ValidityBlock.BlockID, err = reader.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read validity_block_id: %w", err)
			}

			// Read Field 111: validity_offset
			if err := reader.OnPropertyBegin(111); err != nil {
				return nil, fmt.Errorf("expected field 111 for validity_offset: %w", err)
			}
			offset, err := reader.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to read validity_offset: %w", err)
			}
			dp.SegmentState.ValidityBlock.Offset = uint32(offset)
		}
	}

	// Peek for Field 112: state_data_len
	fieldID, err = reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek for state_data: %w", err)
	}

	if fieldID == 112 {
		reader.ConsumeField()
		stateDataLen, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read state_data_len: %w", err)
		}
		if stateDataLen > 0 {
			dp.SegmentState.StateData = make([]byte, stateDataLen)
			for i := uint64(0); i < stateDataLen; i++ {
				b, err := reader.ReadUint8()
				if err != nil {
					return nil, fmt.Errorf("failed to read state_data byte %d: %w", i, err)
				}
				dp.SegmentState.StateData[i] = b
			}
		}
	}

	// Read terminator
	fieldID, err = reader.PeekField()
	if err != nil {
		return nil, fmt.Errorf("failed to peek for terminator: %w", err)
	}
	if fieldID != ddbFieldTerminator {
		return nil, fmt.Errorf("expected terminator (0xFFFF), got field %d", fieldID)
	}
	reader.ConsumeField()

	return dp, nil
}

// ReadRowGroupsFromTablePointer reads row group pointers from the table storage block.
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
// - Field 103: delete_pointers (vector<MetaBlockPointer>)
// - Field 104: has_metadata_blocks (bool) - optional
// - Field 105: extra_metadata_blocks (vector<idx_t>) - optional
// - Terminator 0xFFFF
func ReadRowGroupsFromTablePointer(bm *BlockManager, tablePointer MetaBlockPointer, totalRows uint64, columnCount int) ([]*RowGroupPointer, error) {
	// Check if the table has any data
	if tablePointer.BlockID == InvalidBlockID || totalRows == 0 {
		return nil, nil
	}

	// Create a metadata reader starting at the table pointer location
	// Note: The offset in the MetaBlockPointer must be handled specially since
	// Encode() doesn't preserve the offset field
	encodedPointer := tablePointer.Encode()
	reader, err := NewMetadataReaderWithOffset(bm, encodedPointer, tablePointer.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata reader for table pointer: %w", err)
	}

	// Step 1: Skip TableStatistics
	// TableStatistics is wrapped in BinarySerializer Begin/End
	// We need to skip all nested structures until we find the outer terminator
	if err := reader.skipTableStatistics(); err != nil {
		return nil, fmt.Errorf("failed to skip TableStatistics: %w", err)
	}

	// Step 2: Read row_group_count as raw uint64 (8 bytes)
	rowGroupCount, err := reader.readRawUint64()
	if err != nil {
		return nil, fmt.Errorf("failed to read row_group_count: %w", err)
	}

	if rowGroupCount == 0 {
		return nil, nil
	}

	// Step 3: Read each RowGroupPointer
	// Each RowGroupPointer is wrapped in BinarySerializer Begin/End
	var rowGroups []*RowGroupPointer

	for i := uint64(0); i < rowGroupCount; i++ {

		// Read Field 100: row_start
		if err := reader.OnPropertyBegin(100); err != nil {
			return nil, fmt.Errorf("expected field 100 for row_start in row group %d: %w", i, err)
		}
		rowStart, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read row_start in row group %d: %w", i, err)
		}

		// Read Field 101: tuple_count
		if err := reader.OnPropertyBegin(101); err != nil {
			return nil, fmt.Errorf("expected field 101 for tuple_count in row group %d: %w", i, err)
		}
		tupleCount, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read tuple_count in row group %d: %w", i, err)
		}

		// Read Field 102: data_pointers (vector<MetaBlockPointer>)
		if err := reader.OnPropertyBegin(102); err != nil {
			return nil, fmt.Errorf("expected field 102 for data_pointers in row group %d: %w", i, err)
		}
		// Read the count of data pointers
		dataPointerCount, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read data_pointers count in row group %d: %w", i, err)
		}
		dataPointers := make([]MetaBlockPointer, dataPointerCount)
		for j := uint64(0); j < dataPointerCount; j++ {
			ptr, readErr := reader.readMetaBlockPointer()
			if readErr != nil {
				return nil, fmt.Errorf("failed to read column %d segment pointer in row group %d: %w", j, i, readErr)
			}
			dataPointers[j] = ptr
		}

		// Read Field 103: delete_pointers (vector<MetaBlockPointer>)
		if err := reader.OnPropertyBegin(103); err != nil {
			return nil, fmt.Errorf("expected field 103 for delete_pointers in row group %d: %w", i, err)
		}
		// Read the count of delete pointers
		deletePointerCount, err := reader.ReadVarint()
		if err != nil {
			return nil, fmt.Errorf("failed to read delete_pointers count in row group %d: %w", i, err)
		}
		// Skip delete pointers (we don't use them)
		for j := uint64(0); j < deletePointerCount; j++ {
			if _, readErr := reader.readMetaBlockPointer(); readErr != nil {
				return nil, fmt.Errorf("failed to skip delete pointer %d in row group %d: %w", j, i, readErr)
			}
		}

		// Skip remaining optional fields until terminator
		// Fields 104 and 105 are optional
		for {
			fieldID, err := reader.PeekField()
			if err != nil {
				return nil, fmt.Errorf("failed to peek field after delete_pointers in row group %d: %w", i, err)
			}
			if fieldID == ddbFieldTerminator {
				reader.ConsumeField() // Consume the terminator
				break
			}
			// Skip this field
			reader.ConsumeField()
			switch fieldID {
			case 104:
				// bool - read as varint (1 byte typically)
				_, _ = reader.ReadVarint()
			case 105:
				// vector<idx_t> - read count then skip
				extraCount, _ := reader.ReadVarint()
				for k := uint64(0); k < extraCount; k++ {
					_, _ = reader.ReadVarint()
				}
			default:
				// Unknown field - try to skip
				_, _ = reader.ReadVarint()
			}
		}

		rowGroup := &RowGroupPointer{
			RowStart:     rowStart,
			TupleCount:   tupleCount,
			DataPointers: dataPointers,
		}
		rowGroups = append(rowGroups, rowGroup)
	}

	return rowGroups, nil
}

// skipTableStatisticsBytes reads data and finds the offset where row_group_count is located.
// It returns the number of bytes from the current position to the row_group_count.
//
// TableStatistics is complex and contains nested structures including HyperLogLog data (~3KB per column).
// Instead of trying to parse it, we search for the pattern that marks the end:
// - 0xFF 0xFF (terminator)
// - row_group_count as raw uint64 (8 bytes)
// - Followed by Field 100 (0x64 0x00) which is the first field of RowGroupPointer
//
// We search for: [0xFF 0xFF][uint64 row_group_count][0x64][0x00] pattern.
func (r *MetadataReader) skipTableStatistics() error {
	minTableStatsSize := 50 // TableStatistics is at least this big

	// We need to search in the block data
	// First, search within the current block's data
	data := r.blockReader.data
	startOffset := r.blockReader.offset

	// Search for the pattern: [0xFF 0xFF][uint64 row_group_count][0x64 0x00]
	for i := startOffset + minTableStatsSize; i+12 < len(data); i++ {
		// Look for 0xFF 0xFF terminator
		if data[i] == 0xff && data[i+1] == 0xff {
			// Check if followed by a valid row_group_count + Field 100
			potentialCount := binary.LittleEndian.Uint64(data[i+2:])
			if potentialCount >= 1 && potentialCount < 1000000 {
				// Check if followed by Field 100 (0x64 0x00)
				if data[i+10] == 0x64 && data[i+11] == 0x00 {
					// Found it! Position reader at the row_group_count (i+2)
					r.blockReader.offset = i + 2
					return nil
				}
			}
		}
	}

	// If not found in current block, we need to load more data
	// This happens when TableStatistics spans multiple sub-blocks
	// Load data from next blocks and search there

	// Save the current data for searching across block boundaries
	currentData := data

	// Continue reading into subsequent blocks
	for {
		// Try to load next block
		if r.blockReader.nextBlockID == InvalidBlockID {
			break
		}

		oldDataLen := len(currentData)
		if err := r.blockReader.loadNextBlock(); err != nil {
			break
		}

		// Append new block's data
		newData := r.blockReader.data
		combinedData := make([]byte, oldDataLen+len(newData))
		copy(combinedData, currentData)
		copy(combinedData[oldDataLen:], newData)
		currentData = combinedData

		// Search in the combined data
		for i := oldDataLen - 12; i+12 < len(currentData); i++ {
			if i < minTableStatsSize {
				continue
			}
			// Look for 0xFF 0xFF terminator
			if currentData[i] == 0xff && currentData[i+1] == 0xff {
				// Check if followed by a valid row_group_count + Field 100
				potentialCount := binary.LittleEndian.Uint64(currentData[i+2:])
				if potentialCount >= 1 && potentialCount < 1000000 {
					// Check if followed by Field 100 (0x64 0x00)
					if currentData[i+10] == 0x64 && currentData[i+11] == 0x00 {
						// Found it! Calculate position within current block
						posInNewBlock := i + 2 - oldDataLen
						if posInNewBlock >= 0 {
							r.blockReader.offset = posInNewBlock
						} else {
							// The row_group_count starts in the previous block
							// This is a boundary case - the pattern spans blocks
							// For now, handle by positioning at start and skipping
							r.blockReader.offset = 0
							skipBytes := posInNewBlock + len(r.blockReader.data)
							r.blockReader.offset = skipBytes
						}
						return nil
					}
				}
			}
		}

		// Limit how much we search to avoid infinite loops
		if len(currentData) > 200*1024 {
			break
		}
	}

	return fmt.Errorf("could not find row_group_count pattern (searched %d bytes from offset %d)",
		len(currentData), startOffset)
}

// skipTableStatsFieldValue skips the value of a field within TableStatistics.
// This handles nested objects, vectors, etc.
func (r *MetadataReader) skipTableStatsFieldValue(fieldID uint16) error {
	// For most fields, we just read a varint or the field leads to a nested object
	// We peek ahead to see if it's a nested object (next field starts with 100 or similar)
	// vs a simple value.

	// Actually, the challenge is that BinarySerializer format doesn't have explicit type markers.
	// We have to know the schema to properly skip.
	//
	// For TableStatistics, the known structure is:
	// Field 100: vector<shared_ptr<ColumnStatistics>> - has count, then nullable+object for each
	// Field 101: optional unique_ptr<BlockingSample> - has nullable byte
	//
	// For now, let's use a simpler approach: just read through the data until we hit terminators

	// Check the first byte to see if this is a nested structure
	// - If it's a field ID (e.g., 0x64 0x00 for Field 100), we're entering a nested object
	// - If it's a small value, it could be a count or bool

	nextByte := r.blockReader.data[r.blockReader.offset]

	// Vectors start with a count (varint)
	// Nested objects start with a field ID (which is 2 bytes, e.g., 0x64 0x00)
	// Nullable values start with a bool (0x00 or 0x01)

	// For Field 100 (column_stats), it's a vector
	if fieldID == 100 {
		// Read vector count
		count, err := r.ReadVarint()
		if err != nil {
			return fmt.Errorf("failed to read vector count: %w", err)
		}
		// Skip each element (each is shared_ptr<ColumnStatistics>)
		for i := uint64(0); i < count; i++ {
			// shared_ptr has nullable byte
			present, err := r.ReadBool()
			if err != nil {
				return fmt.Errorf("failed to read nullable present: %w", err)
			}
			if present {
				// Skip the object (ColumnStatistics) by reading until terminator
				if err := r.skipNestedObject(); err != nil {
					return fmt.Errorf("failed to skip ColumnStatistics: %w", err)
				}
			}
		}
		return nil
	}

	// For Field 101 (table_sample), it's optional unique_ptr
	if fieldID == 101 {
		// Optional with default - check if present
		present, err := r.ReadBool()
		if err != nil {
			return fmt.Errorf("failed to read optional present: %w", err)
		}
		if present {
			// Skip the ReservoirSample object
			if err := r.skipNestedObject(); err != nil {
				return fmt.Errorf("failed to skip ReservoirSample: %w", err)
			}
		}
		return nil
	}

	// For unknown fields, try to read as varint
	_, err := r.ReadVarint()
	if err != nil {
		// If varint fails, try to peek for nested object
		nextFieldID, peekErr := r.PeekField()
		if peekErr != nil {
			return err
		}
		if nextFieldID < 300 && nextFieldID != ddbFieldTerminator {
			// Looks like a nested object
			return r.skipNestedObject()
		}
		return err
	}
	_ = nextByte // suppress unused warning
	return nil
}

// skipNestedObject skips a BinarySerializer nested object until its terminator.
func (r *MetadataReader) skipNestedObject() error {
	for {
		fieldID, err := r.PeekField()
		if err != nil {
			return fmt.Errorf("failed to peek field: %w", err)
		}

		if fieldID == ddbFieldTerminator {
			r.ConsumeField()
			return nil
		}

		r.ConsumeField()

		// Skip this field's value
		// This is recursive for nested objects
		if err := r.skipAnyFieldValue(); err != nil {
			return fmt.Errorf("failed to skip field %d: %w", fieldID, err)
		}
	}
}

// skipAnyFieldValue attempts to skip any type of field value.
// This is best-effort and may not work for all complex types.
func (r *MetadataReader) skipAnyFieldValue() error {
	// Try to peek at what follows
	if r.blockReader.offset >= len(r.blockReader.data) {
		return io.EOF
	}

	nextByte := r.blockReader.data[r.blockReader.offset]

	// Check if next bytes look like a field ID
	if r.blockReader.offset+1 < len(r.blockReader.data) {
		potentialFieldID := uint16(nextByte) | uint16(r.blockReader.data[r.blockReader.offset+1])<<8
		if potentialFieldID == ddbFieldTerminator {
			// Don't consume - let caller handle
			return nil
		}
		if potentialFieldID >= 100 && potentialFieldID < 300 {
			// This looks like a nested object, recurse
			return r.skipNestedObject()
		}
	}

	// Otherwise, assume it's a varint value
	// But first check if it could be raw bytes (like HyperLogLog data)
	if nextByte&0x80 != 0 {
		// Has continuation bit - might be large varint or length-prefixed data
		val, err := r.ReadVarint()
		if err != nil {
			return err
		}
		// If this varint is large and followed by non-field data, it might be a length prefix
		if val > 100 && val < 10000 && r.blockReader.offset+int(val) < len(r.blockReader.data) {
			// Check if data after val bytes looks like fields
			checkOffset := r.blockReader.offset + int(val)
			if checkOffset < len(r.blockReader.data)-1 {
				potentialFieldID := uint16(r.blockReader.data[checkOffset]) | uint16(r.blockReader.data[checkOffset+1])<<8
				if potentialFieldID == ddbFieldTerminator || (potentialFieldID >= 100 && potentialFieldID < 300) {
					// This was a length-prefixed raw data block, skip it
					r.blockReader.offset += int(val)
					return nil
				}
			}
		}
		return nil
	}

	// Simple varint
	_, err := r.ReadVarint()
	return err
}

// readRawUint64 reads 8 bytes as a little-endian uint64 directly from the block reader.
func (r *MetadataReader) readRawUint64() (uint64, error) {
	if r.blockReader.offset+8 > len(r.blockReader.data) {
		return 0, io.EOF
	}
	val := binary.LittleEndian.Uint64(r.blockReader.data[r.blockReader.offset:])
	r.blockReader.offset += 8
	return val, nil
}
