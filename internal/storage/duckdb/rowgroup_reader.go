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

	return c.decodeFixedValue(c.Data[offset:offset+uint64(valueSize)]), true
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

	// dataPointerCache lazily caches resolved DataPointers.
	dataPointerCache map[int]*DataPointer

	// columnCache caches decompressed column data.
	columnCache map[int]*ColumnData

	// mu protects concurrent access to caches.
	mu sync.RWMutex
}

// NewRowGroupReader creates a new RowGroupReader for reading a row group.
func NewRowGroupReader(
	bm *BlockManager,
	rgp *RowGroupPointer,
	types []LogicalTypeID,
) *RowGroupReader {
	return &RowGroupReader{
		blockManager:     bm,
		rowGroupPtr:      rgp,
		columnTypes:      types,
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

	// Decompress the column data
	colData, err := decompressColumn(r.blockManager, dp, typeID)
	if err != nil {
		return nil, fmt.Errorf("%w: column %d: %v", ErrDecompressionFailed, colIdx, err)
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
		} else if dp.Statistics.HasNull && len(dp.Statistics.StatData) == 0 {
			// All-NULL column: HasNull=true but no statistics data means ALL values are NULL
			// Create a validity mask with all bits cleared (all NULL)
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
		data, err = decodeUncompressedStrings(compressedData, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to decode uncompressed strings: %w", err)
		}
	} else if dp.Compression == CompressionDictionary && valueSize <= 0 {
		// Dictionary-compressed VARCHAR uses a complex format that varies by DuckDB version
		// For now, return an error indicating this format is not yet supported
		// TODO: Implement full DuckDB dictionary string decompression
		return nil, fmt.Errorf("dictionary compression for VARCHAR not yet implemented")
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

	// Handle all-NULL UNCOMPRESSED variable-size columns (VARCHAR, BLOB)
	// For these columns, if HasNull=true but there's NO validity pointer and NO HasValidityMask,
	// it means ALL values are NULL. This is different from normal variable-size columns which
	// always have a ValidityPointer set (even if it's CONSTANT compression indicating all-valid).
	if validity == nil && dp.Compression == CompressionUncompressed && valueSize <= 0 &&
		dp.Statistics.HasNull && dp.ValidityPointer == nil && !dp.SegmentState.HasValidityMask {
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
// - 4 bytes: heap end offset (header_size + index_size + heap_size)
// - 4 bytes per string: cumulative end offset in heap
// - Heap data: concatenated string bytes
//
// For single-string segments (e.g., 1 row), the format is different:
// - 8-byte header
// - string_t entry (4-byte length + 12-byte inline data)
func decodeUncompressedStrings(data []byte, tupleCount uint64) ([]byte, error) {
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
	// [4-byte total_heap_size][4-byte heap_end_offset][4-byte end_offset per string...][heap data]
	// IMPORTANT: Strings are stored in REVERSE order in the heap!
	// The index stores cumulative offsets from the END of the heap.
	headerSize := uint64(8)
	indexSize := tupleCount * 4
	heapStart := headerSize + indexSize

	// Read total heap size from header
	totalHeapSize := uint64(binary.LittleEndian.Uint32(data[0:4]))

	if uint64(len(data)) < heapStart+totalHeapSize {
		return nil, fmt.Errorf("string segment too short for heap: need %d, have %d",
			heapStart+totalHeapSize, len(data))
	}

	// Build output in [length][data] format
	var result []byte
	prevOffset := uint64(0) // Previous cumulative offset

	for i := uint64(0); i < tupleCount; i++ {
		// Read cumulative offset for this string
		indexOffset := headerSize + i*4
		currOffset := uint64(binary.LittleEndian.Uint32(data[indexOffset:]))

		// Calculate string length
		stringLen := currOffset - prevOffset

		// Strings are stored in reverse order in heap
		// String i is at heap position: totalHeapSize - currOffset to totalHeapSize - prevOffset
		stringStart := totalHeapSize - currOffset
		heapStringStart := heapStart + stringStart

		if heapStringStart+stringLen > uint64(len(data)) {
			return nil, fmt.Errorf("string %d heap access out of bounds: start=%d, len=%d, data_len=%d",
				i, heapStringStart, stringLen, len(data))
		}

		// Append [length][data]
		result = binary.LittleEndian.AppendUint32(result, uint32(stringLen))
		result = append(result, data[heapStringStart:heapStringStart+stringLen]...)

		prevOffset = currOffset
	}

	return result, nil
}

// synthesizeConstantFromStats creates constant data from statistics min value.
func synthesizeConstantFromStats(statData []byte, typeID LogicalTypeID, valueSize int, tupleCount uint64) ([]byte, error) {
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
			return nil, fmt.Errorf("values region size %d not divisible by valueSize %d", valuesRegionSize, valueSize)
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
		return nil, fmt.Errorf("unsupported compression type for validity extraction: %s", compression.String())
	}

	// Validate offset
	if validityOffset >= len(compressedData) {
		return nil, fmt.Errorf("validity offset %d exceeds data length %d", validityOffset, len(compressedData))
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
		// - If block_id is invalid (127) and HasStats=false, assume all values are VALID
		//   (this is common for nullable columns that happen to have no NULLs)
		// - If HasStats=true with StatData containing 0, all values are NULL
		// - If HasStats=true with StatData containing non-0, all values are VALID

		// Check if we have valid statistics with a constant value
		if validityPtr.Statistics.HasStats && len(validityPtr.Statistics.StatData) > 0 {
			// The constant validity value is in StatData
			// If the constant is 0, all values are NULL
			// If the constant is non-0, all values are valid
			constantValue := validityPtr.Statistics.StatData[0]
			if constantValue == 0 {
				// All values are NULL - create an all-invalid mask
				mask := NewValidityMask(tupleCount)
				for i := uint64(0); i < tupleCount; i++ {
					mask.SetInvalid(i)
				}
				return mask, nil
			}
		}

		// No stats or non-zero constant - all values are valid
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
