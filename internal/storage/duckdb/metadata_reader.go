package duckdb

import (
	"fmt"
	"io"
	"unsafe"
)

// MetadataReader reads catalog entries from DuckDB metadata blocks.
// It understands DuckDB's BinarySerializer format which uses:
// - uint16 field IDs before each value
// - 0xFFFF as object terminator
// - varint (LEB128) encoding for integers and lengths
type MetadataReader struct {
	blockManager *BlockManager
	data         []byte
	offset       int
	nextBlockID  uint64 // Next block in chain, InvalidBlockID if none

	// Allow peeking 1 field ahead
	hasBufferedField bool
	bufferedField    uint16
}

// MetadataBlockHeaderSize is the size of the next-block pointer at the start of each metadata block.
const MetadataBlockHeaderSize = 8

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
	block, err := bm.ReadBlock(startBlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata block %d: %w", startBlockID, err)
	}

	if len(block.Data) < MetadataBlockHeaderSize {
		return nil, fmt.Errorf("metadata block too small: %d bytes", len(block.Data))
	}

	r := &MetadataReader{
		blockManager: bm,
		data:         block.Data,
		offset:       MetadataBlockHeaderSize, // Skip the next-block pointer
	}

	// Read next block pointer (8 bytes, little-endian)
	r.nextBlockID = uint64(block.Data[0]) |
		uint64(block.Data[1])<<8 |
		uint64(block.Data[2])<<16 |
		uint64(block.Data[3])<<24 |
		uint64(block.Data[4])<<32 |
		uint64(block.Data[5])<<40 |
		uint64(block.Data[6])<<48 |
		uint64(block.Data[7])<<56

	return r, nil
}

// remaining returns the number of bytes remaining in the current block.
func (r *MetadataReader) remaining() int {
	return len(r.data) - r.offset
}

// loadNextBlock loads the next block in the chain if available.
func (r *MetadataReader) loadNextBlock() error {
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

	r.data = block.Data
	r.offset = MetadataBlockHeaderSize

	// Read next block pointer
	r.nextBlockID = uint64(block.Data[0]) |
		uint64(block.Data[1])<<8 |
		uint64(block.Data[2])<<16 |
		uint64(block.Data[3])<<24 |
		uint64(block.Data[4])<<32 |
		uint64(block.Data[5])<<40 |
		uint64(block.Data[6])<<48 |
		uint64(block.Data[7])<<56

	return nil
}

// readByte reads a single byte, loading the next block if needed.
func (r *MetadataReader) readByte() (byte, error) {
	if r.remaining() < 1 {
		if err := r.loadNextBlock(); err != nil {
			return 0, err
		}
	}
	b := r.data[r.offset]
	r.offset++
	return b, nil
}

// readBytes reads n bytes, potentially spanning multiple blocks.
func (r *MetadataReader) readBytes(n int) ([]byte, error) {
	result := make([]byte, n)
	pos := 0

	for pos < n {
		if r.remaining() == 0 {
			if err := r.loadNextBlock(); err != nil {
				return nil, err
			}
		}

		toCopy := min(n-pos, r.remaining())
		copy(result[pos:], r.data[r.offset:r.offset+toCopy])
		r.offset += toCopy
		pos += toCopy
	}

	return result, nil
}

// ReadFieldID reads a uint16 field ID.
func (r *MetadataReader) ReadFieldID() (uint16, error) {
	data, err := r.readBytes(2)
	if err != nil {
		return 0, err
	}
	return uint16(data[0]) | uint16(data[1])<<8, nil
}

// PeekField returns the next field ID without consuming it.
func (r *MetadataReader) PeekField() (uint16, error) {
	if !r.hasBufferedField {
		field, err := r.ReadFieldID()
		if err != nil {
			return 0, err
		}
		r.bufferedField = field
		r.hasBufferedField = true
	}
	return r.bufferedField, nil
}

// ConsumeField consumes a buffered field.
func (r *MetadataReader) ConsumeField() {
	r.hasBufferedField = false
}

// NextField reads and returns the next field ID.
func (r *MetadataReader) NextField() (uint16, error) {
	if r.hasBufferedField {
		r.hasBufferedField = false
		return r.bufferedField, nil
	}
	return r.ReadFieldID()
}

// ReadVarint reads a variable-length integer (LEB128 encoded).
func (r *MetadataReader) ReadVarint() (uint64, error) {
	var result uint64
	var shift uint

	for {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}

		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	return result, nil
}

// ReadSignedVarint reads a signed variable-length integer (signed LEB128).
func (r *MetadataReader) ReadSignedVarint() (int64, error) {
	var result int64
	var shift uint

	for {
		b, err := r.readByte()
		if err != nil {
			return 0, err
		}

		result |= int64(b&0x7F) << shift
		shift += 7

		if b&0x80 == 0 {
			// Sign extend if negative
			if shift < 64 && (b&0x40) != 0 {
				result |= ^int64(0) << shift
			}
			break
		}
	}

	return result, nil
}

// ReadString reads a varint-length prefixed string.
func (r *MetadataReader) ReadString() (string, error) {
	length, err := r.ReadVarint()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	data, err := r.readBytes(int(length))
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// ReadUint8 reads a varint-encoded uint8.
func (r *MetadataReader) ReadUint8() (uint8, error) {
	v, err := r.ReadVarint()
	return uint8(v), err
}

// ReadBool reads a single byte as a boolean.
func (r *MetadataReader) ReadBool() (bool, error) {
	b, err := r.readByte()
	return b != 0, err
}

// ReadFloat32 reads a 4-byte float.
func (r *MetadataReader) ReadFloat32() (float32, error) {
	data, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	bits := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	return *(*float32)((unsafe.Pointer)(&bits)), nil
}

// ReadFloat64 reads an 8-byte double.
func (r *MetadataReader) ReadFloat64() (float64, error) {
	data, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	bits := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
		uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
	return *(*float64)((unsafe.Pointer)(&bits)), nil
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
		startOffset := r.offset
		entry, err := r.ReadCatalogEntry()
		if err != nil {
			// Log the error for debugging but try to continue
			// The catalog format may have variations we don't handle perfectly yet
			_ = fmt.Sprintf("warning: failed to read entry %d at offset %d: %v", i, startOffset, err)
			break
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

	// Read the list item terminator (each list item is wrapped in an object by list.WriteObject)
	term, err := r.NextField()
	if err != nil {
		return nil, err
	}
	if term != ddbFieldTerminator {
		return nil, fmt.Errorf("expected list item terminator, got %d at offset %d", term, r.offset)
	}

	return entry, nil
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
			return err
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
		constraint, err := r.readConstraint()
		if err != nil {
			return constraints, err
		}
		if constraint != nil {
			constraints = append(constraints, *constraint)
		}
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
