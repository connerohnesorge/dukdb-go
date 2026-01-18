package duckdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// Row group reader error definitions.
var (
	// ErrColumnIndexOutOfRange indicates the column index is outside valid bounds.
	ErrColumnIndexOutOfRange = errors.New("column index out of range")

	// ErrInvalidMetaBlockPointer indicates the meta block pointer is invalid.
	ErrInvalidMetaBlockPointer = errors.New("invalid meta block pointer")

	// ErrDecompressionFailed indicates decompression of column data failed.
	ErrDecompressionFailed = errors.New("decompression failed")

	// ErrInvalidColumnData indicates the column data is invalid or corrupted.
	ErrInvalidColumnData = errors.New("invalid column data")
)

// ColumnData represents decompressed column data from a row group segment.
// It contains the raw decompressed data, validity mask for NULL tracking,
// and metadata about the column values.
type ColumnData struct {
	// Data contains the raw decompressed data bytes.
	// Values are stored in little-endian format.
	Data []byte

	// Validity tracks NULL values for this column.
	// If nil, all values are valid (no NULLs).
	Validity *ValidityMask

	// TupleCount is the number of tuples in this column segment.
	TupleCount uint64

	// TypeID is the logical type of the column.
	TypeID LogicalTypeID

	// Statistics contains per-segment statistics if available.
	Statistics *BaseStatistics

	// ChildData contains decompressed child column data for complex types.
	// For LIST types: contains the list element values
	// For STRUCT/MAP: would contain field/key-value data
	ChildData *ColumnData

	// Offsets contains offset data for LIST types.
	// Each offset indicates where each list's elements end in ChildData.
	// The offsets are cumulative, so list[i] contains elements from
	// Offsets[i-1] (or 0 for i=0) to Offsets[i].
	Offsets []uint64

	// ChildTypeID stores the logical type of child elements for LIST types.
	ChildTypeID LogicalTypeID

	// ChildrenData contains decompressed child column data for STRUCT types.
	// Each element corresponds to a struct field in the same order as StructFields.
	ChildrenData []*ColumnData

	// StructFields contains the field definitions for STRUCT types.
	// Each element contains the field name and type information.
	StructFields []StructField
}

// GetValue retrieves the value at the given row index.
// Returns (value, isValid) where isValid is false if the value is NULL.
// The returned value type depends on the column's TypeID.
func (c *ColumnData) GetValue(rowIdx uint64) (any, bool) {
	if rowIdx >= c.TupleCount {
		return nil, false
	}

	// Check validity
	if c.IsNull(rowIdx) {
		return nil, false
	}

	// Handle complex types specially
	switch c.TypeID {
	case TypeList:
		return c.getListValue(rowIdx)
	case TypeStruct:
		return c.getStructValue(rowIdx)
	case TypeMap:
		return c.getMapValue(rowIdx)
	}

	// Get value based on type
	valueSize := GetTypeSize(c.TypeID)
	if valueSize <= 0 {
		// Variable-size type (VARCHAR, BLOB, etc.)
		return c.getVariableSizeValue(rowIdx)
	}

	offset := rowIdx * uint64(valueSize)
	if offset+uint64(valueSize) > uint64(len(c.Data)) {
		return nil, false
	}

	return c.decodeFixedValue(c.Data[offset : offset+uint64(valueSize)]), true
}

// getListValue retrieves a LIST value at the given row index.
// Returns the list elements as []interface{}.
func (c *ColumnData) getListValue(rowIdx uint64) (any, bool) {
	// Ensure we have offset data
	if c.Offsets == nil || len(c.Offsets) == 0 {
		return nil, false
	}

	// Bounds check on rowIdx
	if rowIdx >= uint64(len(c.Offsets)) {
		return nil, false
	}

	// Get start and end offsets for this list
	var startOffset uint64
	if rowIdx > 0 {
		startOffset = c.Offsets[rowIdx-1]
	}
	endOffset := c.Offsets[rowIdx]

	// Empty list case
	if startOffset == endOffset {
		return []interface{}{}, true
	}

	// Ensure we have child data
	if c.ChildData == nil {
		return nil, false
	}

	// Extract list elements from child data
	listLen := endOffset - startOffset

	// Sanity check - prevent unreasonably large allocations
	const maxListSize = 1 << 30 // 1 billion elements max
	if listLen > maxListSize {
		return nil, false
	}

	elements := make([]interface{}, listLen)

	for i := uint64(0); i < listLen; i++ {
		childIdx := startOffset + i
		val, valid := c.ChildData.GetValue(childIdx)
		if valid {
			elements[i] = val
		} else {
			// NULL element in list
			elements[i] = nil
		}
	}

	return elements, true
}

// getStructValue retrieves a STRUCT value at the given row index.
// Returns the struct fields as map[string]interface{}.
func (c *ColumnData) getStructValue(rowIdx uint64) (any, bool) {
	// Ensure we have child data for struct fields
	if c.ChildrenData == nil || len(c.ChildrenData) == 0 {
		return nil, false
	}

	// Ensure we have field definitions
	if c.StructFields == nil || len(c.StructFields) != len(c.ChildrenData) {
		return nil, false
	}

	// Build the struct as a map
	result := make(map[string]interface{}, len(c.StructFields))

	for i, field := range c.StructFields {
		childCol := c.ChildrenData[i]
		if childCol == nil {
			result[field.Name] = nil
			continue
		}

		// Get the value from the child column at the same row index
		val, valid := childCol.GetValue(rowIdx)
		if valid {
			result[field.Name] = val
		} else {
			// NULL field value
			result[field.Name] = nil
		}
	}

	return result, true
}

// getMapValue retrieves a MAP value at the given row index.
// Returns the map as map[K]V where K is typically string and V is the value type.
// DuckDB stores MAP as LIST(STRUCT(key, value)) internally.
func (c *ColumnData) getMapValue(rowIdx uint64) (any, bool) {
	// Check validity FIRST - NULL MAP should return nil, not empty map
	// Note: This is called from GetValue which already checks IsNull,
	// but we double-check here for safety and clarity.
	if c.IsNull(rowIdx) {
		return nil, false
	}

	// MAP is stored as LIST(STRUCT(key, value))
	// So we first get the list of structs, then convert to a Go map

	// Ensure we have offset data (like LIST)
	if c.Offsets == nil || len(c.Offsets) == 0 {
		// No offsets means empty map (not NULL - we checked above)
		return make(map[string]interface{}), true
	}

	// Bounds check on rowIdx
	if rowIdx >= uint64(len(c.Offsets)) {
		return nil, false
	}

	// Get start and end offsets for this map's entries
	var startOffset uint64
	if rowIdx > 0 {
		startOffset = c.Offsets[rowIdx-1]
	}
	endOffset := c.Offsets[rowIdx]

	// Empty map case
	if startOffset == endOffset {
		return make(map[string]interface{}), true
	}

	// Ensure we have child data (the struct entries)
	if c.ChildData == nil {
		return nil, false
	}

	// Extract map entries from child data
	entryCount := endOffset - startOffset

	// Sanity check
	const maxMapSize = 1 << 30
	if entryCount > maxMapSize {
		return nil, false
	}

	result := make(map[string]interface{}, entryCount)

	for i := uint64(0); i < entryCount; i++ {
		childIdx := startOffset + i
		entry, valid := c.ChildData.GetValue(childIdx)
		if !valid || entry == nil {
			continue
		}

		// Each entry should be a STRUCT with "key" and "value" fields
		// or an ordered map with first field as key, second as value
		switch e := entry.(type) {
		case map[string]interface{}:
			// Standard struct format with "key" and "value" fields
			if key, ok := e["key"]; ok {
				keyStr := fmt.Sprintf("%v", key)
				result[keyStr] = e["value"]
			} else if len(e) == 2 {
				// Alternative: first field is key, second is value
				// This handles anonymous struct fields
				var keyVal, valueVal interface{}
				idx := 0
				for _, v := range e {
					if idx == 0 {
						keyVal = v
					} else {
						valueVal = v
					}
					idx++
				}
				if keyVal != nil {
					keyStr := fmt.Sprintf("%v", keyVal)
					result[keyStr] = valueVal
				}
			}
		}
	}

	return result, true
}

// IsNull returns true if the value at the given row index is NULL.
func (c *ColumnData) IsNull(rowIdx uint64) bool {
	if c.Validity == nil {
		return false
	}
	return !c.Validity.IsValid(rowIdx)
}

// getVariableSizeValue retrieves a variable-size value (VARCHAR, BLOB).
// Variable-size values are stored with length prefix.
func (c *ColumnData) getVariableSizeValue(rowIdx uint64) (any, bool) {
	// For variable-size types, we need to iterate through values
	// Each value is stored as: [uint32 length][bytes...]
	offset := uint64(0)
	for i := uint64(0); i < rowIdx; i++ {
		if offset+4 > uint64(len(c.Data)) {
			return nil, false
		}
		length := binary.LittleEndian.Uint32(c.Data[offset:])
		offset += 4 + uint64(length)
	}

	if offset+4 > uint64(len(c.Data)) {
		return nil, false
	}
	length := binary.LittleEndian.Uint32(c.Data[offset:])
	offset += 4

	if offset+uint64(length) > uint64(len(c.Data)) {
		return nil, false
	}

	if c.TypeID == TypeBlob {
		result := make([]byte, length)
		copy(result, c.Data[offset:offset+uint64(length)])
		return result, true
	}

	// VARCHAR and other string types
	return string(c.Data[offset : offset+uint64(length)]), true
}

// decodeFixedValue decodes a fixed-size value from bytes.
func (c *ColumnData) decodeFixedValue(data []byte) any {
	switch c.TypeID {
	case TypeBoolean:
		return data[0] != 0

	case TypeTinyInt:
		return int8(data[0])

	case TypeSmallInt:
		return int16(binary.LittleEndian.Uint16(data))

	case TypeInteger:
		return int32(binary.LittleEndian.Uint32(data))

	case TypeBigInt:
		return int64(binary.LittleEndian.Uint64(data))

	case TypeUTinyInt:
		return data[0]

	case TypeUSmallInt:
		return binary.LittleEndian.Uint16(data)

	case TypeUInteger:
		return binary.LittleEndian.Uint32(data)

	case TypeUBigInt:
		return binary.LittleEndian.Uint64(data)

	case TypeFloat:
		bits := binary.LittleEndian.Uint32(data)
		return float32FromBits(bits)

	case TypeDouble:
		bits := binary.LittleEndian.Uint64(data)
		return float64FromBits(bits)

	case TypeDate:
		// DATE is stored as int32 days since epoch (1970-01-01)
		days := int32(binary.LittleEndian.Uint32(data))
		return time.Unix(int64(days)*86400, 0).UTC()

	case TypeTime:
		// TIME is stored as int64 microseconds since midnight
		// Return as time.Duration to match duckdb-go API
		micros := int64(binary.LittleEndian.Uint64(data))
		return time.Duration(micros) * time.Microsecond

	case TypeTimeNS:
		// TIME (nanoseconds) is stored as int64 nanoseconds since midnight
		nanos := int64(binary.LittleEndian.Uint64(data))
		return time.Duration(nanos) * time.Nanosecond

	case TypeTimeTZ:
		// TIME WITH TIMEZONE is stored as int64 microseconds since midnight
		// Return as time.Duration to match duckdb-go API
		micros := int64(binary.LittleEndian.Uint64(data))
		return time.Duration(micros) * time.Microsecond

	case TypeTimestamp:
		// TIMESTAMP is stored as int64 microseconds since epoch
		micros := int64(binary.LittleEndian.Uint64(data))
		secs := micros / 1_000_000
		remainingMicros := micros % 1_000_000
		return time.Unix(secs, remainingMicros*1000).UTC()

	case TypeTimestampS:
		// TIMESTAMP_S is stored as int64 seconds since epoch
		secs := int64(binary.LittleEndian.Uint64(data))
		return time.Unix(secs, 0).UTC()

	case TypeTimestampMS:
		// TIMESTAMP_MS is stored as int64 milliseconds since epoch
		millis := int64(binary.LittleEndian.Uint64(data))
		secs := millis / 1000
		remainingMillis := millis % 1000
		return time.Unix(secs, remainingMillis*1_000_000).UTC()

	case TypeTimestampNS:
		// TIMESTAMP_NS is stored as int64 nanoseconds since epoch
		nanos := int64(binary.LittleEndian.Uint64(data))
		secs := nanos / 1_000_000_000
		remainingNanos := nanos % 1_000_000_000
		return time.Unix(secs, remainingNanos).UTC()

	case TypeTimestampTZ:
		// TIMESTAMP WITH TIMEZONE is stored as int64 microseconds since epoch (UTC)
		micros := int64(binary.LittleEndian.Uint64(data))
		secs := micros / 1_000_000
		remainingMicros := micros % 1_000_000
		return time.Unix(secs, remainingMicros*1000).UTC()

	case TypeHugeInt, TypeUHugeInt:
		// HUGEINT is 128-bit, stored as two uint64s (low, high)
		low := binary.LittleEndian.Uint64(data[0:8])
		high := binary.LittleEndian.Uint64(data[8:16])
		return [2]uint64{low, high}

	case TypeUUID:
		// UUID is 128-bit, stored in DuckDB as HUGEINT with special format:
		// - Bytes are stored in reverse order (little-endian for 128-bit)
		// - The high bit is flipped (XOR 0x80) to make comparisons work correctly
		// Return as canonical string format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

		// First, reverse the bytes and flip the high bit
		uuid := make([]byte, 16)
		for i := 0; i < 16; i++ {
			uuid[i] = data[15-i]
		}
		// Flip the high bit of the first byte (most significant)
		uuid[0] ^= 0x80

		return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
			uuid[0], uuid[1], uuid[2], uuid[3],
			uuid[4], uuid[5],
			uuid[6], uuid[7],
			uuid[8], uuid[9],
			uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15])

	case TypeInterval:
		// INTERVAL is stored as months(int32) + days(int32) + micros(int64)
		months := int32(binary.LittleEndian.Uint32(data[0:4]))
		days := int32(binary.LittleEndian.Uint32(data[4:8]))
		micros := int64(binary.LittleEndian.Uint64(data[8:16]))
		return Interval{Months: months, Days: days, Micros: micros}

	default:
		// Return raw bytes for unknown types
		result := make([]byte, len(data))
		copy(result, data)
		return result
	}
}

// RowGroupReader reads a single row group with lazy column loading.
// Columns are only loaded from disk when requested.
type RowGroupReader struct {
	// blockManager is used to read blocks from the file.
	blockManager *BlockManager

	// rowGroupPtr contains metadata about the row group.
	rowGroupPtr *RowGroupPointer

	// columnTypes contains the logical type of each column.
	columnTypes []LogicalTypeID

	// columnModifiers contains type modifiers for each column (for complex types).
	// This includes child type info for LIST, key/value types for MAP, etc.
	columnModifiers []TypeModifiers

	// dataPointerCache lazily caches resolved DataPointers.
	dataPointerCache map[int]*DataPointer

	// columnCache caches decompressed column data.
	columnCache map[int]*ColumnData

	// mu protects concurrent access to caches.
	mu sync.RWMutex
}

// NewRowGroupReader creates a new RowGroupReader for reading a row group.
// The modifiers parameter is optional and can be nil if no complex types are used.
func NewRowGroupReader(
	bm *BlockManager,
	rgp *RowGroupPointer,
	types []LogicalTypeID,
) *RowGroupReader {
	return &RowGroupReader{
		blockManager:     bm,
		rowGroupPtr:      rgp,
		columnTypes:      types,
		columnModifiers:  nil, // Use NewRowGroupReaderWithModifiers for complex types
		dataPointerCache: make(map[int]*DataPointer),
		columnCache:      make(map[int]*ColumnData),
	}
}

// NewRowGroupReaderWithModifiers creates a RowGroupReader with type modifiers for complex types.
// This is needed for LIST, STRUCT, MAP columns to properly decode child types.
func NewRowGroupReaderWithModifiers(
	bm *BlockManager,
	rgp *RowGroupPointer,
	types []LogicalTypeID,
	modifiers []TypeModifiers,
) *RowGroupReader {
	return &RowGroupReader{
		blockManager:     bm,
		rowGroupPtr:      rgp,
		columnTypes:      types,
		columnModifiers:  modifiers,
		dataPointerCache: make(map[int]*DataPointer),
		columnCache:      make(map[int]*ColumnData),
	}
}

// RowGroupPointerRef returns the underlying RowGroupPointer.
func (r *RowGroupReader) RowGroupPointerRef() *RowGroupPointer {
	return r.rowGroupPtr
}

// TupleCount returns the number of tuples in this row group.
func (r *RowGroupReader) TupleCount() uint64 {
	return r.rowGroupPtr.TupleCount
}

// ColumnCount returns the number of columns in this row group.
func (r *RowGroupReader) ColumnCount() int {
	return len(r.rowGroupPtr.DataPointers)
}

// ReadColumn reads and decompresses a single column.
// The result is cached for subsequent reads.
func (r *RowGroupReader) ReadColumn(colIdx int) (*ColumnData, error) {
	// Validate column index
	if colIdx < 0 || colIdx >= len(r.rowGroupPtr.DataPointers) {
		return nil, fmt.Errorf("%w: %d (max %d)",
			ErrColumnIndexOutOfRange, colIdx, len(r.rowGroupPtr.DataPointers)-1)
	}

	// Check cache first (read lock)
	r.mu.RLock()
	if cached, ok := r.columnCache[colIdx]; ok {
		r.mu.RUnlock()
		return cached, nil
	}
	r.mu.RUnlock()

	// Need to load - acquire write lock
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check cache after acquiring lock
	if cached, ok := r.columnCache[colIdx]; ok {
		return cached, nil
	}

	// Resolve the DataPointer
	dp, err := r.resolveDataPointerLocked(colIdx)
	if err != nil {
		return nil, err
	}

	// Get the type for this column
	var typeID LogicalTypeID
	if colIdx < len(r.columnTypes) {
		typeID = r.columnTypes[colIdx]
	}

	// Get type modifiers for complex types (LIST, STRUCT, MAP)
	var modifiers *TypeModifiers
	if r.columnModifiers != nil && colIdx < len(r.columnModifiers) {
		modifiers = &r.columnModifiers[colIdx]
		// Set child type on DataPointer for LIST types
		if typeID == TypeList && modifiers.ChildTypeID != 0 {
			dp.ChildTypeID = modifiers.ChildTypeID
		}
	}

	// Decompress the column data
	colData, err := decompressColumn(r.blockManager, dp, typeID)
	if err != nil {
		return nil, fmt.Errorf("%w: column %d: %v", ErrDecompressionFailed, colIdx, err)
	}

	// For STRUCT types, populate field information from modifiers
	if typeID == TypeStruct && modifiers != nil && len(modifiers.StructFields) > 0 {
		colData.StructFields = modifiers.StructFields

		// STRUCT columns inherit TupleCount from the row group
		if colData.TupleCount == 0 {
			colData.TupleCount = r.rowGroupPtr.TupleCount
		}

		// If we have a ChildPointer, we need to decompress each struct field
		// In DuckDB, STRUCT fields are stored as chained child column data
		// Pass the STRUCT's DataPointer so decompressStructFields can access all children
		if dp.ChildPointer != nil || len(dp.ChildPointers) > 0 {
			childrenData, fieldsErr := decompressStructFields(
				r.blockManager,
				dp,
				modifiers.StructFields,
				colData.TupleCount, // Use the correct tuple count
			)
			if fieldsErr != nil {
				// Log but don't fail - struct fields may not be fully readable yet
				colData.ChildrenData = nil
			} else {
				colData.ChildrenData = childrenData
			}
		}
	}

	// For LIST types, ensure the child has proper type definitions if it's a complex type
	if typeID == TypeList && modifiers != nil {
		// LIST child may be a STRUCT (e.g., LIST(STRUCT(a INTEGER)))
		if colData.ChildData != nil && colData.ChildData.TypeID == TypeStruct {
			// Get the struct fields from modifiers.ChildType
			if modifiers.ChildType != nil && len(modifiers.ChildType.StructFields) > 0 {
				colData.ChildData.StructFields = modifiers.ChildType.StructFields

				// Calculate the correct tuple count for the child STRUCT
				// The tuple count is the total number of list elements (last offset value)
				childTupleCount := colData.ChildData.TupleCount
				if childTupleCount == 0 && len(colData.Offsets) > 0 {
					childTupleCount = colData.Offsets[len(colData.Offsets)-1]
				}
				// Also fix the ChildData's TupleCount
				if colData.ChildData.TupleCount == 0 {
					colData.ChildData.TupleCount = childTupleCount
				}

				// Decompress the STRUCT's child columns (the struct fields)
				// For LIST(STRUCT), the struct's fields are stored as children of the STRUCT child
				// The ChildPointer points to STRUCT metadata, and we need to decompress its fields
				if dp.ChildPointer != nil &&
					(dp.ChildPointer.ChildPointer != nil || len(dp.ChildPointer.ChildPointers) > 0) {
					childrenData, fieldsErr := decompressStructFields(
						r.blockManager,
						dp.ChildPointer, // The STRUCT's DataPointer
						modifiers.ChildType.StructFields,
						childTupleCount,
					)
					if fieldsErr == nil && len(childrenData) > 0 {
						colData.ChildData.ChildrenData = childrenData
					}
				} else if dp.ChildPointer != nil {
					// The STRUCT may have its children stored differently
					// Try to decompress fields even without ChildPointer/ChildPointers
					childrenData, fieldsErr := decompressStructFields(
						r.blockManager,
						dp.ChildPointer,
						modifiers.ChildType.StructFields,
						childTupleCount,
					)
					if fieldsErr == nil && len(childrenData) > 0 {
						colData.ChildData.ChildrenData = childrenData
					}
				}
			}
		}
	}

	// For MAP types, ensure the child STRUCT has proper field definitions and decompress its children
	if typeID == TypeMap && modifiers != nil {
		// MAP child is STRUCT with key and value fields
		if colData.ChildData != nil && colData.ChildData.TypeID == TypeStruct {
			// Get the key and value types from modifiers
			// DuckDB stores MAP as LIST(STRUCT(key, value)), so the types might be in:
			// 1. modifiers.KeyTypeID/ValueTypeID (direct)
			// 2. modifiers.ChildType.StructFields (when ChildType is STRUCT)
			var keyTypeID, valueTypeID LogicalTypeID
			var keyTypeMods, valueTypeMods *TypeModifiers

			if modifiers.KeyTypeID != 0 && modifiers.KeyTypeID != TypeInvalid {
				// Direct key/value types available
				keyTypeID = modifiers.KeyTypeID
				valueTypeID = modifiers.ValueTypeID
				keyTypeMods = modifiers.KeyType
				valueTypeMods = modifiers.ValueType
			} else if modifiers.ChildType != nil && len(modifiers.ChildType.StructFields) >= 2 {
				// Extract from STRUCT child (key and value fields)
				keyTypeID = modifiers.ChildType.StructFields[0].Type
				valueTypeID = modifiers.ChildType.StructFields[1].Type
				keyTypeMods = modifiers.ChildType.StructFields[0].TypeModifiers
				valueTypeMods = modifiers.ChildType.StructFields[1].TypeModifiers
			} else {
				// Default to VARCHAR/INTEGER for unknown types
				keyTypeID = TypeVarchar
				valueTypeID = TypeInteger
			}

			// Set the key/value field definitions on the child STRUCT
			structFields := []StructField{
				{Name: "key", Type: keyTypeID, TypeModifiers: keyTypeMods},
				{Name: "value", Type: valueTypeID, TypeModifiers: valueTypeMods},
			}
			colData.ChildData.StructFields = structFields

			// Decompress the STRUCT's child columns (key and value) using the MAP's ChildPointer
			// dp.ChildPointer points to the STRUCT's metadata which contains the field DataPointers
			if dp.ChildPointer != nil &&
				(dp.ChildPointer.ChildPointer != nil || len(dp.ChildPointer.ChildPointers) > 0) {
				childrenData, fieldsErr := decompressStructFields(
					r.blockManager,
					dp.ChildPointer, // Use the MAP's ChildPointer (which is the STRUCT's DataPointer)
					structFields,
					colData.ChildData.TupleCount,
				)
				if fieldsErr == nil && len(childrenData) > 0 {
					colData.ChildData.ChildrenData = childrenData
				}
			}
		}
	}

	// Cache and return
	r.columnCache[colIdx] = colData
	return colData, nil
}

// ReadColumns reads multiple columns for projection pushdown.
// Columns are read in parallel when possible.
func (r *RowGroupReader) ReadColumns(colIdxs []int) ([]*ColumnData, error) {
	if len(colIdxs) == 0 {
		return []*ColumnData{}, nil
	}

	results := make([]*ColumnData, len(colIdxs))
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for i, colIdx := range colIdxs {
		wg.Add(1)
		go func(i, colIdx int) {
			defer wg.Done()

			col, err := r.ReadColumn(colIdx)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			results[i] = col
		}(i, colIdx)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return results, nil
}

// resolveDataPointerLocked loads the DataPointer while holding the lock.
// The MetaBlockPointer in RowGroupPointer.DataPointers points to ColumnData metadata,
// which contains field 100 with vector<DataPointer> for the actual column segments.
func (r *RowGroupReader) resolveDataPointerLocked(colIdx int) (*DataPointer, error) {
	// Check cache first
	if dp, ok := r.dataPointerCache[colIdx]; ok {
		return dp, nil
	}

	// Validate column index
	if colIdx < 0 || colIdx >= len(r.rowGroupPtr.DataPointers) {
		return nil, fmt.Errorf("%w: %d", ErrColumnIndexOutOfRange, colIdx)
	}

	// Get the MetaBlockPointer for this column's ColumnData metadata
	mbp := r.rowGroupPtr.DataPointers[colIdx]

	// Check if the meta block pointer is valid
	if !mbp.IsValid() {
		return nil, fmt.Errorf("%w: column %d", ErrInvalidMetaBlockPointer, colIdx)
	}

	// Read the ColumnData from the MetaBlockPointer location
	// ColumnData serialization format:
	// - Field 100: data_pointers (vector<DataPointer>) - the actual segment pointers
	// - Field 101: validity (child ColumnData) - for nullable columns
	dp, err := ReadColumnDataPointer(r.blockManager, mbp)
	if err != nil {
		return nil, fmt.Errorf("failed to read ColumnData for column %d: %w", colIdx, err)
	}

	// Set row start and tuple count from the row group
	dp.RowStart = r.rowGroupPtr.RowStart
	if dp.TupleCount == 0 {
		dp.TupleCount = r.rowGroupPtr.TupleCount
	}

	// Cache for future access
	r.dataPointerCache[colIdx] = dp
	return dp, nil
}

// ClearCache clears all cached column data to free memory.
// DataPointer cache is preserved as it's small.
func (r *RowGroupReader) ClearCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.columnCache = make(map[int]*ColumnData)
}

// ClearAllCaches clears both column data and DataPointer caches.
func (r *RowGroupReader) ClearAllCaches() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.columnCache = make(map[int]*ColumnData)
	r.dataPointerCache = make(map[int]*DataPointer)
}

// decompressColumn decompresses column data from blocks using the DataPointer.
func decompressColumn(
	blockManager *BlockManager,
	dp *DataPointer,
	typeID LogicalTypeID,
) (*ColumnData, error) {
	// Handle empty segment
	if dp.TupleCount == 0 {
		return &ColumnData{
			Data:       []byte{},
			Validity:   nil,
			TupleCount: 0,
			TypeID:     typeID,
			Statistics: &dp.Statistics,
		}, nil
	}

	// Handle complex types specially
	switch typeID {
	case TypeList:
		return decompressListColumn(blockManager, dp)
	case TypeStruct:
		return decompressStructColumn(blockManager, dp)
	case TypeMap:
		// MAP is stored as LIST(STRUCT(key, value)) internally
		return decompressMapColumn(blockManager, dp)
	}

	// Get value size for decompression
	valueSize := GetTypeSize(typeID)

	// Handle CONSTANT compression - may use statistics instead of data block
	if dp.Compression == CompressionConstant {
		data, err := decompressConstantColumn(blockManager, dp, typeID, valueSize)
		if err != nil {
			return nil, err
		}
		// Read validity mask if present
		var validity *ValidityMask
		if dp.ValidityPointer != nil {
			// Validity is stored in a separate child ColumnData (field 101)
			validity, err = readValidityFromPointer(blockManager, dp.ValidityPointer, dp.TupleCount)
			if err != nil {
				return nil, fmt.Errorf("failed to read validity from pointer: %w", err)
			}
		} else if dp.SegmentState.HasValidityMask {
			// Validity is stored in segment state (legacy format)
			validity, err = readValidityMask(blockManager, dp.SegmentState, dp.TupleCount)
			if err != nil {
				return nil, fmt.Errorf("failed to read validity mask: %w", err)
			}
		} else if dp.Statistics.HasStats && dp.Statistics.HasNull && len(dp.Statistics.StatData) == 0 {
			// All-NULL column: HasStats=true AND HasNull=true but no statistics data means ALL values are NULL
			// Note: We must check HasStats because HasNull may be set even when HasStats=false,
			// and in that case it doesn't mean all values are NULL.
			validity = NewValidityMaskAllNull(dp.TupleCount)
		}
		return &ColumnData{
			Data:       data,
			Validity:   validity,
			TupleCount: dp.TupleCount,
			TypeID:     typeID,
			Statistics: &dp.Statistics,
		}, nil
	}

	// Read the block containing the compressed data
	block, err := blockManager.ReadBlock(dp.Block.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read data block %d: %w", dp.Block.BlockID, err)
	}

	// Validate offset is within block
	if uint64(dp.Block.Offset) >= uint64(len(block.Data)) {
		return nil, fmt.Errorf("data offset %d exceeds block size %d",
			dp.Block.Offset, len(block.Data))
	}

	// Extract segment data starting at offset
	compressedData := block.Data[dp.Block.Offset:]

	// Handle VARCHAR specially - DuckDB uses different formats for different compressions
	var data []byte
	if dp.Compression == CompressionUncompressed && valueSize <= 0 {
		data, err = decodeUncompressedStrings(compressedData, dp.TupleCount, blockManager)
		if err != nil {
			return nil, fmt.Errorf("failed to decode uncompressed strings: %w", err)
		}
	} else if dp.Compression == CompressionDictionary && valueSize <= 0 {
		// Dictionary-compressed VARCHAR
		data, err = decodeDictionaryStrings(compressedData, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to decode dictionary strings: %w", err)
		}
	} else {
		// Standard decompression
		data, err = Decompress(dp.Compression, compressedData, valueSize, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("decompression failed for %s: %w",
				dp.Compression.String(), err)
		}
	}

	// Read validity mask if present
	var validity *ValidityMask
	if dp.ValidityPointer != nil {
		// Validity is stored in a separate child ColumnData (field 101)
		validity, err = readValidityFromPointer(blockManager, dp.ValidityPointer, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read validity from pointer: %w", err)
		}
	} else if dp.SegmentState.HasValidityMask {
		// Validity is stored in segment state (legacy format)
		validity, err = readValidityMask(blockManager, dp.SegmentState, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read validity mask: %w", err)
		}
	} else if dp.Statistics.HasNull && dp.Compression == CompressionRLE {
		// For RLE compression with nulls, validity mask is embedded in the compressed data
		// DuckDB stores the validity mask after the RLE metadata
		// NOTE: We only do this for RLE, not BITPACKING. For BITPACKING, the validity
		// should come from the ValidityPointer field if there are actual NULLs.
		// The HasNull flag is conservative (may be true even without actual NULLs).
		validity, err = extractEmbeddedValidityMask(compressedData, dp.Compression, valueSize, dp.TupleCount)
		if err != nil {
			// Log but don't fail - validity extraction is best-effort
			validity = nil
		}
	} else if dp.Statistics.HasNull && dp.Compression == CompressionUncompressed && typeID == TypeBoolean {
		// For BOOLEAN columns with UNCOMPRESSED data and nulls but no validity pointer,
		// validity mask is embedded after the data values (8-byte aligned)
		// NOTE: This is specific to BOOLEAN type - other types like TINYINT don't embed validity
		// even though they also have valueSize=1
		dataSize := uint64(valueSize) * dp.TupleCount
		alignedDataSize := (dataSize + 7) / 8 * 8 // Round up to 8-byte boundary

		// Only try to extract validity if there's actually data beyond the aligned offset
		if uint64(len(compressedData)) > alignedDataSize {
			validityData := compressedData[alignedDataSize:]
			// Check if the validity data looks valid (first byte shouldn't be all 1s for columns with nulls)
			// This helps avoid false positives where we're reading padding as validity
			if len(validityData) > 0 && validityData[0] != 0xFF {
				validity, err = decodeValidityMask(validityData, dp.TupleCount)
				if err != nil {
					// Log but don't fail - validity extraction is best-effort
					validity = nil
				}
			}
		}
	}

	// Note: dp.Statistics.HasNull indicates the column is nullable (can contain NULLs),
	// NOT that all values ARE NULL. If there's no validity pointer and no embedded validity mask,
	// all values are VALID (no actual NULLs exist in this segment).
	//
	// For all-NULL columns, DuckDB sets up a ValidityPointer with CONSTANT compression
	// and HasStats=true with empty StatData (indicating constant value 0 = all NULL).
	// So we don't need special handling here - the validity will be read from ValidityPointer above.

	return &ColumnData{
		Data:       data,
		Validity:   validity,
		TupleCount: dp.TupleCount,
		TypeID:     typeID,
		Statistics: &dp.Statistics,
	}, nil
}

// decompressListColumn decompresses a LIST column.
// LIST columns in DuckDB are stored with:
// 1. Offset data (uint64 cumulative offsets indicating list boundaries) in the main DataPointer
// 2. Child element data stored either in a separate ChildPointer OR inline after the offsets
// 3. Validity: NULL mask for list values (stored in ValidityPointer)
//
// The ChildPointer field of the DataPointer contains the child element data's DataPointer,
// which is recursively decompressed to get the child ColumnData.
//
// For UNCOMPRESSED LIST columns without ChildPointer, child data is stored INLINE
// immediately after the offset array in the same data segment.
func decompressListColumn(
	blockManager *BlockManager,
	dp *DataPointer,
) (*ColumnData, error) {
	// Read validity mask if present (for NULL list values)
	var validity *ValidityMask
	var err error
	if dp.ValidityPointer != nil {
		validity, err = readValidityFromPointer(blockManager, dp.ValidityPointer, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read list validity: %w", err)
		}
	}

	// Read the data block containing the offsets
	block, err := blockManager.ReadBlock(dp.Block.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read list data block %d: %w", dp.Block.BlockID, err)
	}

	if uint64(dp.Block.Offset) >= uint64(len(block.Data)) {
		return nil, fmt.Errorf(
			"list offset %d exceeds block size %d",
			dp.Block.Offset,
			len(block.Data),
		)
	}

	blockData := block.Data[dp.Block.Offset:]

	// Decompress the offset array using the appropriate compression method
	// LIST offsets are stored as uint64 values (8 bytes each)
	offsetData, err := Decompress(dp.Compression, blockData, 8, dp.TupleCount)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress list offsets: %w", err)
	}

	// Parse offsets from decompressed data
	offsets := make([]uint64, dp.TupleCount)
	for i := uint64(0); i < dp.TupleCount; i++ {
		if (i+1)*8 <= uint64(len(offsetData)) {
			offsets[i] = binary.LittleEndian.Uint64(offsetData[i*8:])
		}
	}

	// Determine child type
	childTypeID := dp.ChildTypeID
	if childTypeID == 0 {
		childTypeID = TypeInteger // Default fallback
	}

	// Decompress child data using the ChildPointer
	var childData *ColumnData
	if dp.ChildPointer != nil {
		// Recursively decompress the child column data
		childData, err = decompressColumn(blockManager, dp.ChildPointer, childTypeID)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress list child data: %w", err)
		}
	} else if dp.Compression == CompressionUncompressed && len(offsets) > 0 {
		// For UNCOMPRESSED LIST columns without ChildPointer, child data is stored inline
		// immediately after the offset array in the same data segment.
		//
		// Data layout:
		// - Offset array: TupleCount * 8 bytes (uint64 cumulative offsets)
		// - Child data: follows immediately after offsets
		//
		// The total number of child elements is given by the last offset value.
		totalChildElements := offsets[len(offsets)-1]

		if totalChildElements > 0 {
			// Calculate where child data starts
			offsetArraySize := dp.TupleCount * 8
			childDataStart := offsetArraySize

			if uint64(len(blockData)) > childDataStart {
				childBlockData := blockData[childDataStart:]

				// Read inline child data based on child type
				childData, err = readInlineChildData(childBlockData, childTypeID, totalChildElements, blockManager)
				if err != nil {
					return nil, fmt.Errorf("failed to read inline child data: %w", err)
				}
			}
		} else {
			// No child elements - create empty child data
			childData = &ColumnData{
				Data:       []byte{},
				TupleCount: 0,
				TypeID:     childTypeID,
			}
		}
	}
	// NOTE: For compressed LIST columns (RLE, BITPACKING) without ChildPointer,
	// the child data format is more complex and not yet supported.

	return &ColumnData{
		Data:        nil, // Offsets are stored in the Offsets field
		Validity:    validity,
		TupleCount:  dp.TupleCount,
		TypeID:      TypeList,
		Statistics:  &dp.Statistics,
		Offsets:     offsets,
		ChildData:   childData,
		ChildTypeID: childTypeID,
	}, nil
}

// readInlineChildData reads child element data that is stored inline after the offset array.
// This handles both fixed-size types (INTEGER, BIGINT, etc.) and variable-size types (VARCHAR).
func readInlineChildData(
	data []byte,
	childTypeID LogicalTypeID,
	elementCount uint64,
	blockManager *BlockManager,
) (*ColumnData, error) {
	childValueSize := GetTypeSize(childTypeID)

	if childValueSize > 0 {
		// Fixed-size child type (INTEGER, BIGINT, FLOAT, etc.)
		requiredSize := elementCount * uint64(childValueSize)
		if uint64(len(data)) < requiredSize {
			return nil, fmt.Errorf("inline child data too short: need %d bytes, have %d",
				requiredSize, len(data))
		}

		childBytes := make([]byte, requiredSize)
		copy(childBytes, data[:requiredSize])

		return &ColumnData{
			Data:       childBytes,
			TupleCount: elementCount,
			TypeID:     childTypeID,
		}, nil
	}

	// Variable-size child type (VARCHAR, BLOB)
	// DuckDB stores variable-size list elements using a heap format similar to regular strings
	return readInlineVarcharChildren(data, elementCount, blockManager)
}

// readInlineVarcharChildren reads VARCHAR child elements stored inline in a LIST column.
// The format is similar to uncompressed strings: heap-based with cumulative offsets.
func readInlineVarcharChildren(
	data []byte,
	elementCount uint64,
	blockManager *BlockManager,
) (*ColumnData, error) {
	if elementCount == 0 {
		return &ColumnData{
			Data:       []byte{},
			TupleCount: 0,
			TypeID:     TypeVarchar,
		}, nil
	}

	// For LIST(VARCHAR), DuckDB uses a similar heap format as regular VARCHAR columns
	// Try to decode using the uncompressed strings decoder
	result, err := decodeUncompressedStrings(data, elementCount, blockManager)
	if err != nil {
		return nil, fmt.Errorf("failed to decode inline varchar children: %w", err)
	}

	return &ColumnData{
		Data:       result,
		TupleCount: elementCount,
		TypeID:     TypeVarchar,
	}, nil
}

// decompressStructColumn decompresses a STRUCT column.
// STRUCT columns in DuckDB store each field as a separate child column.
// The struct validity mask applies to the entire struct (not individual fields).
// Each field has its own child ColumnData with its own validity mask.
func decompressStructColumn(
	blockManager *BlockManager,
	dp *DataPointer,
) (*ColumnData, error) {
	// Read validity mask if present (for NULL struct values)
	var validity *ValidityMask
	var err error
	if dp.ValidityPointer != nil {
		validity, err = readValidityFromPointer(blockManager, dp.ValidityPointer, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read struct validity: %w", err)
		}
	}

	// STRUCT columns don't have their own data block - the data is stored in child columns.
	// The ChildPointer points to the first child, and additional children are chained
	// or stored in a vector of child pointers.
	//
	// Note: DuckDB's current format stores STRUCT child columns as a vector of ColumnData
	// where each element corresponds to a struct field. The field metadata (names, types)
	// comes from the TypeModifiers in the catalog.
	//
	// For now, we create a placeholder that will be populated by ReadColumn when
	// type modifiers are available.

	return &ColumnData{
		Data:         nil,
		Validity:     validity,
		TupleCount:   dp.TupleCount,
		TypeID:       TypeStruct,
		Statistics:   &dp.Statistics,
		ChildrenData: nil, // Will be populated when struct fields are known
		StructFields: nil, // Will be populated from TypeModifiers
	}, nil
}

// decompressStructFields decompresses all fields of a STRUCT column.
// In DuckDB, each struct field is stored as a separate child column.
// The STRUCT's DataPointer contains:
// - ChildPointer: the first field's DataPointer
// - ChildPointers: additional field DataPointers (for fields 1, 2, ...)
func decompressStructFields(
	blockManager *BlockManager,
	structDP *DataPointer,
	fields []StructField,
	tupleCount uint64,
) ([]*ColumnData, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	childrenData := make([]*ColumnData, len(fields))

	// Collect all child pointers from the STRUCT's DataPointer
	// The first child is in ChildPointer, additional children are in ChildPointers
	var allChildPtrs []*DataPointer
	if structDP != nil {
		if structDP.ChildPointer != nil {
			allChildPtrs = append(allChildPtrs, structDP.ChildPointer)
		}
		if structDP.ChildPointers != nil {
			allChildPtrs = append(allChildPtrs, structDP.ChildPointers...)
		}
	}

	// Decompress each field
	for i := 0; i < len(fields); i++ {
		if i >= len(allChildPtrs) {
			break
		}

		childPtr := allChildPtrs[i]
		if childPtr == nil {
			continue
		}

		fieldType := fields[i].Type

		// Set child type for complex nested types
		if fields[i].TypeModifiers != nil && fields[i].TypeModifiers.ChildTypeID != 0 {
			childPtr.ChildTypeID = fields[i].TypeModifiers.ChildTypeID
		}

		// Set tuple count on child pointer if not set
		if childPtr.TupleCount == 0 {
			childPtr.TupleCount = tupleCount
		}

		childData, err := decompressColumn(blockManager, childPtr, fieldType)
		if err != nil {
			// Log but don't fail on individual field errors
			continue
		}
		childData.TupleCount = tupleCount

		// For nested STRUCT types, we need to recursively decompress the nested struct's fields
		if fieldType == TypeStruct && fields[i].TypeModifiers != nil &&
			len(fields[i].TypeModifiers.StructFields) > 0 {
			// Set the struct field definitions
			childData.StructFields = fields[i].TypeModifiers.StructFields

			// Check if we have child pointers for the nested struct
			if childPtr.ChildPointer != nil || len(childPtr.ChildPointers) > 0 {
				// Recursively decompress the nested struct's fields
				nestedChildrenData, nestedErr := decompressStructFields(
					blockManager,
					childPtr,
					fields[i].TypeModifiers.StructFields,
					tupleCount,
				)
				if nestedErr == nil {
					childData.ChildrenData = nestedChildrenData
				}
			}
		}

		childrenData[i] = childData
	}

	return childrenData, nil
}

// decompressMapColumn decompresses a MAP column.
// MAP is stored as LIST(STRUCT(key, value)) in DuckDB.
// This means we reuse the LIST decompression but the child type is STRUCT.
func decompressMapColumn(
	blockManager *BlockManager,
	dp *DataPointer,
) (*ColumnData, error) {
	// MAP is stored identically to LIST, but the child type is STRUCT(key, value)
	// We can reuse decompressListColumn and just set the type to TypeMap

	// Read validity mask if present (for NULL map values)
	var validity *ValidityMask
	var err error
	if dp.ValidityPointer != nil {
		validity, err = readValidityFromPointer(blockManager, dp.ValidityPointer, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read map validity: %w", err)
		}
	}

	// Read the data block containing the offsets
	block, err := blockManager.ReadBlock(dp.Block.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read map data block %d: %w", dp.Block.BlockID, err)
	}

	if uint64(dp.Block.Offset) >= uint64(len(block.Data)) {
		return nil, fmt.Errorf(
			"map offset %d exceeds block size %d",
			dp.Block.Offset,
			len(block.Data),
		)
	}

	blockData := block.Data[dp.Block.Offset:]

	// Decompress the offset array using the appropriate compression method
	// MAP offsets are stored as uint64 values (8 bytes each), same as LIST
	offsetData, err := Decompress(dp.Compression, blockData, 8, dp.TupleCount)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress map offsets: %w", err)
	}

	// Parse offsets from decompressed data
	offsets := make([]uint64, dp.TupleCount)
	for i := uint64(0); i < dp.TupleCount; i++ {
		if (i+1)*8 <= uint64(len(offsetData)) {
			offsets[i] = binary.LittleEndian.Uint64(offsetData[i*8:])
		}
	}

	// The child type for MAP is STRUCT with "key" and "value" fields
	childTypeID := TypeStruct

	// Calculate total number of child elements (STRUCT entries in the MAP)
	// This is the last offset value if we have offsets
	var totalChildElements uint64
	if len(offsets) > 0 {
		totalChildElements = offsets[len(offsets)-1]
	}

	// Decompress child data using the ChildPointer
	// MAP child is STRUCT(key, value) - the actual fields will be decompressed in ReadColumn
	// because we need TypeModifiers to know the correct types
	var childData *ColumnData
	if dp.ChildPointer != nil {
		// Create placeholder STRUCT - field types will be set by ReadColumn using TypeModifiers
		childData = &ColumnData{
			TypeID:     TypeStruct,
			TupleCount: totalChildElements,
			// StructFields and ChildrenData will be populated by ReadColumn
		}

		// Read validity for STRUCT entries if present
		if dp.ChildPointer.ValidityPointer != nil {
			childData.Validity, _ = readValidityFromPointer(
				blockManager,
				dp.ChildPointer.ValidityPointer,
				totalChildElements,
			)
		}
	}

	return &ColumnData{
		Data:        nil,
		Validity:    validity,
		TupleCount:  dp.TupleCount,
		TypeID:      TypeMap,
		Statistics:  &dp.Statistics,
		Offsets:     offsets,
		ChildData:   childData,
		ChildTypeID: childTypeID,
	}, nil
}

// decompressConstantColumn handles CONSTANT compression decompression.
// For CONSTANT compression, DuckDB may store the constant value in:
// 1. A data block (if block_id is valid)
// 2. Statistics (min value) if block_id is invalid
func decompressConstantColumn(
	blockManager *BlockManager,
	dp *DataPointer,
	typeID LogicalTypeID,
	valueSize int,
) ([]byte, error) {
	// First, try to read from the data block
	block, err := blockManager.ReadBlock(dp.Block.BlockID)
	if err == nil && uint64(dp.Block.Offset) < uint64(len(block.Data)) {
		// Block is valid, use it for decompression
		compressedData := block.Data[dp.Block.Offset:]

		// Handle variable-size types (VARCHAR, BLOB)
		if valueSize <= 0 {
			return decompressConstantVarSize(compressedData, dp.TupleCount)
		}

		// Handle fixed-size types using standard decompress
		return Decompress(dp.Compression, compressedData, valueSize, dp.TupleCount)
	}

	// Block is invalid (e.g., block_id=127 doesn't exist)
	// Use the constant value from statistics instead
	if len(dp.Statistics.StatData) > 0 {
		return synthesizeConstantFromStats(dp.Statistics.StatData, typeID, valueSize, dp.TupleCount)
	}

	// No statistics available - this happens for all-NULL columns
	// where there's no meaningful constant value to store.
	// Return zero-filled data; the validity mask will mark all as NULL.
	if dp.Statistics.HasNull {
		// For fixed-size types, allocate zero-filled data
		if valueSize > 0 {
			return make([]byte, int(dp.TupleCount)*valueSize), nil
		}
		// For variable-size types, return empty data (each row will be NULL anyway)
		return []byte{}, nil
	}

	// No statistics available and not marked as has_null - this shouldn't happen
	return nil, fmt.Errorf("CONSTANT compression but no valid block (%d) and no statistics data",
		dp.Block.BlockID)
}

// decompressConstantVarSize decompresses CONSTANT compressed variable-size data.
func decompressConstantVarSize(compressedData []byte, tupleCount uint64) ([]byte, error) {
	if len(compressedData) < 4 {
		return nil, fmt.Errorf("CONSTANT compressed data too short for variable-size type")
	}

	// Read the length of the constant value
	valueLen := binary.LittleEndian.Uint32(compressedData[0:4])
	if uint64(len(compressedData)) < 4+uint64(valueLen) {
		return nil, fmt.Errorf("CONSTANT compressed data truncated: need %d bytes, have %d",
			4+valueLen, len(compressedData))
	}

	constantValue := compressedData[4 : 4+valueLen]

	// Build output in length-prefix format: [uint32 len][bytes][uint32 len][bytes]...
	resultSize := tupleCount * (4 + uint64(valueLen))
	data := make([]byte, resultSize)

	offset := uint64(0)
	for i := uint64(0); i < tupleCount; i++ {
		binary.LittleEndian.PutUint32(data[offset:], valueLen)
		offset += 4
		copy(data[offset:], constantValue)
		offset += uint64(valueLen)
	}
	return data, nil
}

// decodeUncompressedStrings decodes DuckDB's uncompressed string segment format.
// DuckDB uses a heap-based format for uncompressed strings:
// - 4 bytes: total heap size
// - 4 bytes: segment size (header + index + heap)
// - 4 bytes per string: cumulative end offset in heap (negative = overflow)
// - Heap data: concatenated string bytes (and overflow pointers for overflow strings)
//
// For overflow strings (> ~4KB), the offset is stored as a negative value (e.g., -length).
// The actual string data is in a separate overflow block, and the heap contains an 8-byte
// pointer (4-byte block_id + 4-byte offset_in_block) at the beginning of the heap for
// each overflow string.
//
// For single-string segments (e.g., 1 row), the format is different:
// - 8-byte header
// - string_t entry (4-byte length + 12-byte inline data)
func decodeUncompressedStrings(
	data []byte,
	tupleCount uint64,
	blockManager *BlockManager,
) ([]byte, error) {
	if tupleCount == 0 {
		return []byte{}, nil
	}

	if len(data) < 8 {
		return nil, fmt.Errorf("string segment too short: %d bytes", len(data))
	}

	// Check if this is a single-string segment with string_t format
	// Single string segments have: 8-byte header + 16-byte string_t
	if tupleCount == 1 {
		// For single string, use the string_t format
		// Header is 8 bytes, then length at offset 8, inline data at offset 12
		if len(data) < 24 {
			return nil, fmt.Errorf("single string segment too short: %d bytes", len(data))
		}
		length := binary.LittleEndian.Uint32(data[8:12])
		if length <= 12 {
			// Inline string
			result := binary.LittleEndian.AppendUint32(nil, length)
			result = append(result, data[12:12+length]...)
			return result, nil
		}
		// Longer string would need heap lookup - try below format
	}

	// Multi-string heap format:
	// [4-byte total_heap_size][4-byte segment_size][4-byte end_offset per string...][heap data]
	// IMPORTANT: Strings are stored in REVERSE order in the heap!
	// The index stores cumulative offsets from the END of the heap.
	// Negative offsets indicate overflow strings stored in separate blocks.
	headerSize := uint64(8)
	indexSize := tupleCount * 4
	heapStart := headerSize + indexSize

	// Read total heap size from header
	totalHeapSize := uint64(binary.LittleEndian.Uint32(data[0:4]))

	if uint64(len(data)) < heapStart+totalHeapSize {
		return nil, fmt.Errorf("string segment too short for heap: need %d, have %d",
			heapStart+totalHeapSize, len(data))
	}

	// First pass: identify overflow strings and count overflow pointers
	// Overflow pointers are stored at the BEGINNING of the heap
	overflowCount := uint64(0)
	for i := uint64(0); i < tupleCount; i++ {
		indexOffset := headerSize + i*4
		rawOffset := binary.LittleEndian.Uint32(data[indexOffset:])
		signedOffset := int32(rawOffset)
		if signedOffset < 0 {
			overflowCount++
		}
	}

	// Overflow pointers are 8 bytes each at the start of heap
	_ = overflowCount // Used to track overflow pointer positions

	// Build output in [length][data] format
	var result []byte
	prevOffset := uint64(0) // Previous cumulative offset (only for inline strings)
	overflowIdx := uint64(0)

	for i := uint64(0); i < tupleCount; i++ {
		// Read cumulative offset for this string
		indexOffset := headerSize + i*4
		rawOffset := binary.LittleEndian.Uint32(data[indexOffset:])
		signedOffset := int32(rawOffset)

		if signedOffset < 0 {
			// Overflow string - read from overflow block
			// The string length is -signedOffset
			stringLen := uint32(-signedOffset)

			// Read overflow pointer from heap
			overflowPtrOffset := heapStart + overflowIdx*8
			if overflowPtrOffset+8 > uint64(len(data)) {
				return nil, fmt.Errorf("overflow pointer %d out of bounds", overflowIdx)
			}
			overflowBlockID := binary.LittleEndian.Uint32(data[overflowPtrOffset:])
			overflowOffsetInBlock := binary.LittleEndian.Uint32(data[overflowPtrOffset+4:])

			if blockManager == nil {
				return nil, fmt.Errorf("cannot read overflow string without block manager")
			}

			// Read the overflow block
			overflowBlock, err := blockManager.ReadBlock(uint64(overflowBlockID))
			if err != nil {
				return nil, fmt.Errorf("failed to read overflow block %d: %w", overflowBlockID, err)
			}

			// The overflow block contains: [4-byte length][string data]
			overflowDataStart := uint32(overflowOffsetInBlock)
			if overflowDataStart+4 > uint32(len(overflowBlock.Data)) {
				return nil, fmt.Errorf(
					"overflow offset %d out of bounds in block %d",
					overflowDataStart,
					overflowBlockID,
				)
			}

			// Read length from overflow block (should match stringLen)
			overflowLen := binary.LittleEndian.Uint32(overflowBlock.Data[overflowDataStart:])
			if overflowLen != stringLen {
				// Use the length from the overflow block
				stringLen = overflowLen
			}

			// Read string data
			stringDataStart := overflowDataStart + 4
			if stringDataStart+stringLen > uint32(len(overflowBlock.Data)) {
				return nil, fmt.Errorf("overflow string data out of bounds: need %d, have %d",
					stringDataStart+stringLen, len(overflowBlock.Data))
			}

			result = binary.LittleEndian.AppendUint32(result, stringLen)
			result = append(
				result,
				overflowBlock.Data[stringDataStart:stringDataStart+stringLen]...)

			overflowIdx++
		} else {
			// Inline string
			currOffset := uint64(rawOffset)
			stringLen := currOffset - prevOffset

			// Strings are stored in reverse order in heap
			// String data is at: heapEnd - currOffset to heapEnd - prevOffset
			// Where heapEnd = heapStart + totalHeapSize
			heapEnd := heapStart + totalHeapSize
			stringStart := heapEnd - currOffset
			stringEnd := heapEnd - prevOffset

			if stringStart > uint64(len(data)) || stringEnd > uint64(len(data)) {
				return nil, fmt.Errorf("string %d heap access out of bounds: start=%d, end=%d, data_len=%d",
					i, stringStart, stringEnd, len(data))
			}

			result = binary.LittleEndian.AppendUint32(result, uint32(stringLen))
			result = append(result, data[stringStart:stringEnd]...)

			prevOffset = currOffset
		}
	}

	return result, nil
}

// decodeDictionaryStrings decodes dictionary-compressed VARCHAR data.
// DuckDB dictionary compression format for VARCHAR:
// 1. Header (20 bytes):
//   - dict_size (uint32): SIZE in bytes of dictionary data
//   - dict_end (uint32): Absolute end offset of entire compressed segment
//   - index_buffer_offset (uint32): Offset to index buffer
//   - index_buffer_count (uint32): Number of entries in index buffer
//   - bitpacking_width (uint32): Bit width for selection buffer
//
// 2. Selection buffer: Bit-packed indices into index buffer (one per tuple)
// 3. Index buffer: Array of uint32 offsets (from dict_end, going backwards)
// 4. Dictionary: Concatenated string data stored in REVERSE order from dict_end
//
// String extraction: index 0 is null/empty, indices 1+ contain actual strings.
// String length = indexBuffer[i] - indexBuffer[i-1]
// String position = dict_end - indexBuffer[i]
func decodeDictionaryStrings(data []byte, tupleCount uint64) ([]byte, error) {
	if tupleCount == 0 {
		return []byte{}, nil
	}

	if len(data) < 20 {
		return nil, fmt.Errorf("dictionary segment too short for header: %d bytes", len(data))
	}

	// Parse header (20 bytes)
	// dictSize := binary.LittleEndian.Uint32(data[0:4]) // Not used directly
	dictEnd := binary.LittleEndian.Uint32(data[4:8])
	indexBufferOffset := binary.LittleEndian.Uint32(data[8:12])
	indexBufferCount := binary.LittleEndian.Uint32(data[12:16])
	bitpackingWidth := binary.LittleEndian.Uint32(data[16:20])

	// Validate header
	if indexBufferOffset < 20 || indexBufferOffset > uint32(len(data)) {
		return nil, fmt.Errorf("invalid index_buffer_offset: %d", indexBufferOffset)
	}
	if indexBufferCount == 0 {
		// No unique strings - return empty result
		return []byte{}, nil
	}
	if bitpackingWidth == 0 || bitpackingWidth > 32 {
		return nil, fmt.Errorf("invalid bitpacking_width: %d", bitpackingWidth)
	}

	// Validate index buffer bounds
	indexBufferEnd := indexBufferOffset + indexBufferCount*4
	if indexBufferEnd > uint32(len(data)) {
		return nil, fmt.Errorf("index buffer exceeds data bounds")
	}

	// Clamp dict_end to data length
	if dictEnd > uint32(len(data)) {
		dictEnd = uint32(len(data))
	}

	// Read index buffer (array of uint32 offsets from dict_end, going backwards)
	indexBuffer := make([]uint32, indexBufferCount)
	for i := uint32(0); i < indexBufferCount; i++ {
		offset := indexBufferOffset + i*4
		indexBuffer[i] = binary.LittleEndian.Uint32(data[offset : offset+4])
	}

	// Build unique strings from dictionary
	// Index 0 is null/empty (offset 0)
	// Index i (i > 0): string at position (dict_end - indexBuffer[i]), length (indexBuffer[i] - indexBuffer[i-1])
	uniqueStrings := make([][]byte, indexBufferCount)
	for i := uint32(0); i < indexBufferCount; i++ {
		if i == 0 {
			// Index 0 is null/empty
			uniqueStrings[0] = []byte{}
			continue
		}

		dictOffset := indexBuffer[i]
		var strLen uint32
		if i > 0 {
			strLen = dictOffset - indexBuffer[i-1]
		}

		// String is stored at dict_end - dictOffset
		if dictOffset > dictEnd {
			return nil, fmt.Errorf("dictionary offset %d exceeds dict_end %d at index %d",
				dictOffset, dictEnd, i)
		}
		strStart := dictEnd - dictOffset
		strEndPos := strStart + strLen

		if strEndPos > uint32(len(data)) {
			return nil, fmt.Errorf(
				"string bounds exceed data at index %d: start=%d, len=%d, data_len=%d",
				i,
				strStart,
				strLen,
				len(data),
			)
		}

		uniqueStrings[i] = data[strStart:strEndPos]
	}

	// Read selection buffer and expand to full strings
	selectionBufferOffset := uint32(20) // After header
	selectionBufferSize := indexBufferOffset - selectionBufferOffset
	selectionBuffer := data[selectionBufferOffset : selectionBufferOffset+selectionBufferSize]

	// Unpack bit-packed indices
	var result []byte
	for i := uint64(0); i < tupleCount; i++ {
		// Calculate bit position
		bitPos := i * uint64(bitpackingWidth)
		bytePos := bitPos / 8
		bitOffset := uint32(bitPos % 8)

		if bytePos >= uint64(len(selectionBuffer)) {
			return nil, fmt.Errorf("selection buffer overflow at tuple %d", i)
		}

		// Read enough bytes to cover the value
		var rawValue uint64
		bytesNeeded := (bitOffset + bitpackingWidth + 7) / 8
		if bytePos+uint64(bytesNeeded) > uint64(len(selectionBuffer)) {
			// Handle edge case at end of buffer
			bytesNeeded = uint32(uint64(len(selectionBuffer)) - bytePos)
		}

		for j := uint32(0); j < bytesNeeded && bytePos+uint64(j) < uint64(len(selectionBuffer)); j++ {
			rawValue |= uint64(selectionBuffer[bytePos+uint64(j)]) << (j * 8)
		}

		// Extract the index value
		index := (rawValue >> bitOffset) & ((1 << bitpackingWidth) - 1)

		if index >= uint64(indexBufferCount) {
			return nil, fmt.Errorf("dictionary index %d out of range (max %d) at tuple %d",
				index, indexBufferCount-1, i)
		}

		// Append [length][data]
		str := uniqueStrings[index]
		result = binary.LittleEndian.AppendUint32(result, uint32(len(str)))
		result = append(result, str...)
	}

	return result, nil
}

// synthesizeConstantFromStats creates constant data from statistics min value.
func synthesizeConstantFromStats(
	statData []byte,
	typeID LogicalTypeID,
	valueSize int,
	tupleCount uint64,
) ([]byte, error) {
	if valueSize <= 0 {
		// Variable-size type - statistics contain length-prefixed data
		if len(statData) < 4 {
			return nil, fmt.Errorf("statistics data too short for variable-size constant")
		}
		valueLen := binary.LittleEndian.Uint32(statData[0:4])
		if uint64(len(statData)) < 4+uint64(valueLen) {
			// Stats data might just be the raw value without length prefix
			// Try using the entire statData as the value
			valueLen = uint32(len(statData))
			resultSize := tupleCount * (4 + uint64(valueLen))
			data := make([]byte, resultSize)
			offset := uint64(0)
			for i := uint64(0); i < tupleCount; i++ {
				binary.LittleEndian.PutUint32(data[offset:], valueLen)
				offset += 4
				copy(data[offset:], statData)
				offset += uint64(valueLen)
			}
			return data, nil
		}
		constantValue := statData[4 : 4+valueLen]
		resultSize := tupleCount * (4 + uint64(valueLen))
		data := make([]byte, resultSize)
		offset := uint64(0)
		for i := uint64(0); i < tupleCount; i++ {
			binary.LittleEndian.PutUint32(data[offset:], valueLen)
			offset += 4
			copy(data[offset:], constantValue)
			offset += uint64(valueLen)
		}
		return data, nil
	}

	// Fixed-size type - statistics contain the raw value bytes
	// The statData should contain at least valueSize bytes for the constant
	constantValue := statData
	if len(constantValue) < valueSize {
		// Pad with zeros if needed
		padded := make([]byte, valueSize)
		copy(padded, constantValue)
		constantValue = padded
	} else if len(constantValue) > valueSize {
		// Truncate to valueSize
		constantValue = constantValue[:valueSize]
	}

	// Replicate the constant value for all tuples
	data := make([]byte, tupleCount*uint64(valueSize))
	for i := uint64(0); i < tupleCount; i++ {
		copy(data[i*uint64(valueSize):], constantValue)
	}
	return data, nil
}

// extractEmbeddedValidityMask extracts validity mask embedded in RLE or Bitpacking compressed data.
// For nullable columns using these compression types without a separate validity child,
// DuckDB stores the validity mask after the compression metadata.
func extractEmbeddedValidityMask(
	compressedData []byte,
	compression CompressionType,
	valueSize int,
	tupleCount uint64,
) (*ValidityMask, error) {
	if len(compressedData) < 8 {
		return nil, fmt.Errorf("compressed data too short for validity extraction")
	}

	var validityOffset int

	switch compression {
	case CompressionRLE:
		// RLE format:
		// [8-byte metadata_offset][unique_values...][run_lengths...][4-byte padding][validity_mask]
		metadataOffset := binary.LittleEndian.Uint64(compressedData[0:8])

		if metadataOffset < 8 || metadataOffset > uint64(len(compressedData)) {
			return nil, fmt.Errorf("invalid RLE metadata offset %d", metadataOffset)
		}

		// Calculate number of unique values
		valuesRegionSize := int(metadataOffset) - 8
		if valueSize <= 0 {
			return nil, fmt.Errorf("variable-size types not supported for RLE validity extraction")
		}
		if valuesRegionSize%valueSize != 0 {
			return nil, fmt.Errorf(
				"values region size %d not divisible by valueSize %d",
				valuesRegionSize,
				valueSize,
			)
		}
		numUniqueValues := valuesRegionSize / valueSize

		// Run lengths are uint16 each
		runLengthsSize := numUniqueValues * 2

		// After run lengths, there's a 4-byte padding before validity mask
		validityOffset = int(metadataOffset) + runLengthsSize + 4

	case CompressionBitPacking:
		// Bitpacking format is more complex, try to find validity at a reasonable offset
		// For now, use a heuristic based on the data size
		// The validity mask size is ceil(tupleCount / 8) bytes
		validitySize := int((tupleCount + 7) / 8)

		// Try to find validity at the end of the data, accounting for padding
		// Round up to 8-byte boundary
		validityOffset = len(compressedData) - ((validitySize + 7) / 8 * 8)
		if validityOffset < 0 {
			validityOffset = 0
		}

	default:
		return nil, fmt.Errorf(
			"unsupported compression type for validity extraction: %s",
			compression.String(),
		)
	}

	// Validate offset
	if validityOffset >= len(compressedData) {
		return nil, fmt.Errorf(
			"validity offset %d exceeds data length %d",
			validityOffset,
			len(compressedData),
		)
	}

	// Extract and decode validity mask
	validityData := compressedData[validityOffset:]
	return decodeValidityMask(validityData, tupleCount)
}

// readValidityFromPointer reads validity mask from a child ColumnData pointer.
// In DuckDB, nullable columns store validity as a separate nested ColumnData.
// The validity is stored as a bit vector where each bit indicates if a value is valid (1) or NULL (0).
func readValidityFromPointer(
	bm *BlockManager,
	validityPtr *DataPointer,
	tupleCount uint64,
) (*ValidityMask, error) {
	// Check for CONSTANT compression on validity
	if validityPtr.Compression == CompressionConstant {
		// For CONSTANT validity compression:
		// - If HasStats=true with StatData containing non-0, all values are VALID
		// - If HasStats=true with StatData empty or containing 0, all values are NULL
		//   (empty StatData means the constant value is 0, i.e., all NULL)
		// - If HasStats=false, assume all values are VALID
		//   (this is common for nullable columns that happen to have no NULLs)

		if validityPtr.Statistics.HasStats {
			// Check if we have a non-zero constant value
			if len(validityPtr.Statistics.StatData) > 0 {
				constantValue := validityPtr.Statistics.StatData[0]
				if constantValue != 0 {
					// Non-zero constant - all values are valid
					return NewValidityMask(tupleCount), nil
				}
			}
			// HasStats=true but StatData is empty or contains 0 means all values are NULL
			// Create an all-invalid mask
			return NewValidityMaskAllNull(tupleCount), nil
		}

		// HasStats=false - all values are valid
		// This is the common case for nullable columns with no actual NULLs
		return NewValidityMask(tupleCount), nil
	}

	// Read the block containing the validity data
	block, err := bm.ReadBlock(validityPtr.Block.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read validity block %d: %w",
			validityPtr.Block.BlockID, err)
	}

	// Validate offset
	if uint64(validityPtr.Block.Offset) >= uint64(len(block.Data)) {
		return nil, fmt.Errorf("validity offset %d exceeds block size %d",
			validityPtr.Block.Offset, len(block.Data))
	}

	// Extract validity data starting at offset
	validityData := block.Data[validityPtr.Block.Offset:]

	// Decode the validity mask
	return decodeValidityMask(validityData, tupleCount)
}

// readValidityMask reads the validity mask from block or inline data.
func readValidityMask(
	bm *BlockManager,
	state ColumnSegmentState,
	tupleCount uint64,
) (*ValidityMask, error) {
	// Check if validity is stored in a separate block
	if state.ValidityBlock.IsValid() {
		// Read validity from separate block
		block, err := bm.ReadBlock(state.ValidityBlock.BlockID)
		if err != nil {
			return nil, fmt.Errorf("failed to read validity block %d: %w",
				state.ValidityBlock.BlockID, err)
		}

		// Validate offset
		if uint64(state.ValidityBlock.Offset) >= uint64(len(block.Data)) {
			return nil, fmt.Errorf("validity offset %d exceeds block size %d",
				state.ValidityBlock.Offset, len(block.Data))
		}

		return decodeValidityMask(block.Data[state.ValidityBlock.Offset:], tupleCount)
	}

	// Validity is inlined in state data
	if len(state.StateData) == 0 {
		// No validity data but HasValidityMask is true - create all-valid mask
		return NewValidityMask(tupleCount), nil
	}

	return decodeValidityMask(state.StateData, tupleCount)
}

// decodeValidityMask decodes a validity mask from raw bytes.
func decodeValidityMask(data []byte, tupleCount uint64) (*ValidityMask, error) {
	// Calculate required size
	wordCount := (tupleCount + validityBitsPerWord - 1) / validityBitsPerWord
	requiredBytes := wordCount * 8 // 8 bytes per uint64

	// Check if we have enough data
	if uint64(len(data)) < requiredBytes {
		return nil, fmt.Errorf("%w: need %d bytes, got %d",
			ErrInvalidValidityMask, requiredBytes, len(data))
	}

	// Read the uint64 words
	words := make([]uint64, wordCount)
	for i := uint64(0); i < wordCount; i++ {
		offset := i * 8
		words[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
	}

	return NewValidityMaskFromData(words, tupleCount), nil
}

// DecodeValidityMask is an alias for decodeValidityMask for external use.
func DecodeValidityMask(data []byte, tupleCount uint64) (*ValidityMask, error) {
	return decodeValidityMask(data, tupleCount)
}

// GetTypeSize returns the byte size for a logical type.
// Returns 0 for variable-size types (VARCHAR, BLOB, etc.).
func GetTypeSize(typeID LogicalTypeID) int {
	switch typeID {
	case TypeBoolean, TypeTinyInt, TypeUTinyInt:
		return 1

	case TypeSmallInt, TypeUSmallInt:
		return 2

	case TypeInteger, TypeUInteger, TypeDate, TypeFloat:
		return 4

	case TypeBigInt, TypeUBigInt, TypeDouble,
		TypeTime, TypeTimeNS, TypeTimeTZ,
		TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		return 8

	case TypeHugeInt, TypeUHugeInt, TypeUUID, TypeInterval:
		return 16

	case TypeVarchar, TypeBlob, TypeBit:
		// Variable-size types
		return 0

	case TypeDecimal:
		// Decimal size depends on precision - default to 16 for hugeint backing
		return 16

	case TypeStruct, TypeList, TypeMap, TypeArray, TypeUnion, TypeEnum:
		// Complex types - variable or depend on children
		return 0

	default:
		// Unknown type - assume 8 bytes (most common)
		return 8
	}
}

// float32FromBits converts uint32 bits to float32.
func float32FromBits(bits uint32) float32 {
	return math.Float32frombits(bits)
}

// float64FromBits converts uint64 bits to float64.
func float64FromBits(bits uint64) float64 {
	return math.Float64frombits(bits)
}

// RowIterator iterates over rows in a row group.
type RowIterator struct {
	reader     *RowGroupReader
	columns    []*ColumnData
	colIndices []int
	currentRow uint64
	tupleCount uint64
}

// NewRowIterator creates an iterator over specified columns in a row group.
func NewRowIterator(reader *RowGroupReader, colIndices []int) (*RowIterator, error) {
	// Read all requested columns
	columns, err := reader.ReadColumns(colIndices)
	if err != nil {
		return nil, err
	}

	return &RowIterator{
		reader:     reader,
		columns:    columns,
		colIndices: colIndices,
		currentRow: 0,
		tupleCount: reader.TupleCount(),
	}, nil
}

// Next advances to the next row and returns true if there is a row to read.
func (it *RowIterator) Next() bool {
	return it.currentRow < it.tupleCount
}

// Advance moves to the next row after reading current values.
func (it *RowIterator) Advance() {
	it.currentRow++
}

// GetValue returns the value at the current row for the given column index.
// The colIdx is an index into the colIndices array, not the table column index.
func (it *RowIterator) GetValue(colIdx int) (any, bool) {
	if colIdx < 0 || colIdx >= len(it.columns) {
		return nil, false
	}
	return it.columns[colIdx].GetValue(it.currentRow)
}

// CurrentRow returns the current row index.
func (it *RowIterator) CurrentRow() uint64 {
	return it.currentRow
}

// Reset resets the iterator to the beginning.
func (it *RowIterator) Reset() {
	it.currentRow = 0
}

// Close releases resources held by the iterator.
func (it *RowIterator) Close() {
	it.columns = nil
}
