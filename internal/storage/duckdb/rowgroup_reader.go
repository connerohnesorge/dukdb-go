package duckdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
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
		// DATE is stored as int32 days since epoch
		return int32(binary.LittleEndian.Uint32(data))

	case TypeTime, TypeTimeNS, TypeTimeTZ:
		// TIME is stored as int64 microseconds since midnight
		return int64(binary.LittleEndian.Uint64(data))

	case TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		// TIMESTAMP is stored as int64 microseconds since epoch
		return int64(binary.LittleEndian.Uint64(data))

	case TypeHugeInt, TypeUHugeInt:
		// HUGEINT is 128-bit, stored as two uint64s (low, high)
		low := binary.LittleEndian.Uint64(data[0:8])
		high := binary.LittleEndian.Uint64(data[8:16])
		return [2]uint64{low, high}

	case TypeUUID:
		// UUID is 128-bit
		result := make([]byte, 16)
		copy(result, data[:16])
		return result

	case TypeInterval:
		// INTERVAL is stored as months(int32) + days(int32) + micros(int64)
		months := int32(binary.LittleEndian.Uint32(data[0:4]))
		days := int32(binary.LittleEndian.Uint32(data[4:8]))
		micros := int64(binary.LittleEndian.Uint64(data[8:16]))
		return [3]int64{int64(months), int64(days), micros}

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
func (r *RowGroupReader) resolveDataPointerLocked(colIdx int) (*DataPointer, error) {
	// Check cache first
	if dp, ok := r.dataPointerCache[colIdx]; ok {
		return dp, nil
	}

	// Validate column index
	if colIdx < 0 || colIdx >= len(r.rowGroupPtr.DataPointers) {
		return nil, fmt.Errorf("%w: %d", ErrColumnIndexOutOfRange, colIdx)
	}

	// Get the MetaBlockPointer for this column
	mbp := r.rowGroupPtr.DataPointers[colIdx]

	// Check if the meta block pointer is valid
	if !mbp.IsValid() {
		return nil, fmt.Errorf("%w: column %d", ErrInvalidMetaBlockPointer, colIdx)
	}

	// Read the metadata block containing the serialized DataPointer
	metaBlock, err := r.blockManager.ReadBlock(mbp.BlockID)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata block %d for column %d: %w",
			mbp.BlockID, colIdx, err)
	}

	// Validate offset is within block data
	if mbp.Offset >= uint64(len(metaBlock.Data)) {
		return nil, fmt.Errorf("%w: offset %d exceeds block data size %d",
			ErrInvalidMetaBlockPointer, mbp.Offset, len(metaBlock.Data))
	}

	// Deserialize the DataPointer from the metadata block at the given offset
	dp, err := DeserializeDataPointer(metaBlock.Data[mbp.Offset:])
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize data pointer for column %d: %w",
			colIdx, err)
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

	// Get value size for decompression
	valueSize := GetTypeSize(typeID)

	// Decompress based on compression type
	data, err := Decompress(dp.Compression, compressedData, valueSize, dp.TupleCount)
	if err != nil {
		return nil, fmt.Errorf("decompression failed for %s: %w",
			dp.Compression.String(), err)
	}

	// Read validity mask if present
	var validity *ValidityMask
	if dp.SegmentState.HasValidityMask {
		validity, err = readValidityMask(blockManager, dp.SegmentState, dp.TupleCount)
		if err != nil {
			return nil, fmt.Errorf("failed to read validity mask: %w", err)
		}
	}

	return &ColumnData{
		Data:       data,
		Validity:   validity,
		TupleCount: dp.TupleCount,
		TypeID:     typeID,
		Statistics: &dp.Statistics,
	}, nil
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
