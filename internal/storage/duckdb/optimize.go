// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file contains performance optimizations for
// hot paths including buffer pools and pre-computed lookup tables.
package duckdb

import (
	"sync"
)

// Buffer pool for decompression operations to reduce allocations.
// Using sync.Pool allows buffers to be reused across decompressions.

// DecompressBuffer is a reusable buffer for decompression operations.
type DecompressBuffer struct {
	data []byte
}

// decompressBufferPool provides pooled buffers for decompression.
var decompressBufferPool = sync.Pool{
	New: func() interface{} {
		return &DecompressBuffer{
			data: make([]byte, 0, int(DefaultBlockSize)),
		}
	},
}

// GetDecompressBuffer retrieves a buffer from the pool.
// The buffer's data slice is reset to zero length but retains capacity.
func GetDecompressBuffer() *DecompressBuffer {
	buf := decompressBufferPool.Get().(*DecompressBuffer)
	buf.data = buf.data[:0]
	return buf
}

// PutDecompressBuffer returns a buffer to the pool.
func PutDecompressBuffer(buf *DecompressBuffer) {
	if buf == nil {
		return
	}
	// Don't pool excessively large buffers
	if cap(buf.data) > int(DefaultBlockSize)*4 {
		return
	}
	decompressBufferPool.Put(buf)
}

// Grow ensures the buffer has at least n bytes of capacity.
// Returns the underlying slice with length set to n.
func (b *DecompressBuffer) Grow(n int) []byte {
	if cap(b.data) < n {
		b.data = make([]byte, n)
	} else {
		b.data = b.data[:n]
	}
	return b.data
}

// GetTypeSizeFast returns the byte size for a logical type using optimized switch.
// Returns 0 for variable-size types.
// This is equivalent to GetTypeSize but inlined for performance-critical paths.
func GetTypeSizeFast(typeID LogicalTypeID) int {
	// Use switch for jump table optimization
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
	case TypeHugeInt, TypeUHugeInt, TypeUUID, TypeInterval, TypeDecimal:
		return 16
	case TypeVarchar,
		TypeBlob,
		TypeBit,
		TypeStruct,
		TypeList,
		TypeMap,
		TypeArray,
		TypeUnion,
		TypeEnum:
		return 0 // Variable-size types
	default:
		return 8 // Default - most common for unknown types
	}
}

// unpack8bit extracts count values from byte array (1 byte per value).
// This is the fast path when bit width is 8.
func unpack8bit(data []byte, count uint64) []uint64 {
	result := make([]uint64, count)
	for i := uint64(0); i < count && i < uint64(len(data)); i++ {
		result[i] = uint64(data[i])
	}
	return result
}

// unpack16bit extracts count values from byte array (2 bytes per value, little-endian).
// This is the fast path when bit width is 16.
func unpack16bit(data []byte, count uint64) []uint64 {
	result := make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		offset := i * 2
		if offset+1 < uint64(len(data)) {
			result[i] = uint64(data[offset]) | uint64(data[offset+1])<<8
		}
	}
	return result
}

// unpack32bit extracts count values from byte array (4 bytes per value, little-endian).
// This is the fast path when bit width is 32.
func unpack32bit(data []byte, count uint64) []uint64 {
	result := make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		offset := i * 4
		if offset+3 < uint64(len(data)) {
			result[i] = uint64(data[offset]) |
				uint64(data[offset+1])<<8 |
				uint64(data[offset+2])<<16 |
				uint64(data[offset+3])<<24
		}
	}
	return result
}

// unpack64bit extracts count values from byte array (8 bytes per value, little-endian).
// This is the fast path when bit width is 64.
func unpack64bit(data []byte, count uint64) []uint64 {
	result := make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		offset := i * 8
		if offset+7 < uint64(len(data)) {
			result[i] = uint64(data[offset]) |
				uint64(data[offset+1])<<8 |
				uint64(data[offset+2])<<16 |
				uint64(data[offset+3])<<24 |
				uint64(data[offset+4])<<32 |
				uint64(data[offset+5])<<40 |
				uint64(data[offset+6])<<48 |
				uint64(data[offset+7])<<56
		}
	}
	return result
}

// DecompressBitPackingFast is an optimized version of DecompressBitPackingWithParams
// that uses fast paths for common bit widths (8, 16, 32, 64).
func DecompressBitPackingFast(data []byte, bitWidth uint8, count uint64) ([]uint64, error) {
	// Validate bit width
	if bitWidth > 64 {
		return nil, ErrBitPackingInvalidBitWidth
	}

	// Handle edge case: zero count
	if count == 0 {
		return []uint64{}, nil
	}

	// Handle special case: bitWidth = 0 means all values are 0
	if bitWidth == 0 {
		return make([]uint64, count), nil
	}

	// Fast paths for byte-aligned bit widths
	switch bitWidth {
	case 8:
		return unpack8bit(data, count), nil
	case 16:
		return unpack16bit(data, count), nil
	case 32:
		return unpack32bit(data, count), nil
	case 64:
		return unpack64bit(data, count), nil
	}

	// Fall back to generic implementation for non-aligned bit widths
	return DecompressBitPackingWithParams(data, bitWidth, count)
}

// ValidityMaskBatch provides batch operations on validity masks.
// This is optimized for scanning multiple rows at once.
type ValidityMaskBatch struct {
	mask      *ValidityMask
	wordIndex uint64
	bitOffset uint64
	word      uint64
}

// NewValidityMaskBatch creates a batch iterator for validity mask operations.
func NewValidityMaskBatch(mask *ValidityMask) *ValidityMaskBatch {
	if mask == nil {
		return nil
	}
	return &ValidityMaskBatch{
		mask:      mask,
		wordIndex: 0,
		bitOffset: 0,
		word:      mask.data[0],
	}
}

// IsValidAt checks if a specific row is valid (not NULL).
// This is optimized for sequential access patterns.
func (b *ValidityMaskBatch) IsValidAt(rowIdx uint64) bool {
	if b == nil {
		return true // No mask means all valid
	}

	wordIdx := rowIdx / validityBitsPerWord
	bitIdx := rowIdx % validityBitsPerWord

	// Cache the current word for sequential access
	if wordIdx != b.wordIndex {
		b.wordIndex = wordIdx
		if wordIdx < uint64(len(b.mask.data)) {
			b.word = b.mask.data[wordIdx]
		} else {
			b.word = allValid
		}
	}

	return (b.word & (1 << bitIdx)) != 0
}

// CountValid counts the number of valid rows in the range [start, end).
// Uses popcount optimization for whole words.
func (b *ValidityMaskBatch) CountValid(start, end uint64) uint64 {
	if b == nil {
		return end - start // All valid if no mask
	}
	return b.mask.CountValid(start, end)
}

// BlockReadAhead provides read-ahead hints for sequential block access.
type BlockReadAhead struct {
	bm            *BlockManager
	lastBlockID   uint64
	readAheadSize int
	pending       []uint64
}

// NewBlockReadAhead creates a read-ahead manager for the block manager.
func NewBlockReadAhead(bm *BlockManager, readAheadSize int) *BlockReadAhead {
	if readAheadSize <= 0 {
		readAheadSize = 4 // Default read-ahead of 4 blocks
	}
	return &BlockReadAhead{
		bm:            bm,
		readAheadSize: readAheadSize,
		pending:       make([]uint64, 0, readAheadSize),
	}
}

// ReadBlock reads a block and triggers read-ahead for sequential access.
// If the access pattern is sequential (reading block N after N-1),
// this will pre-fetch the next readAheadSize blocks in the background.
func (ra *BlockReadAhead) ReadBlock(id uint64) (*Block, error) {
	// Check if this is sequential access
	if id == ra.lastBlockID+1 {
		// Trigger read-ahead for subsequent blocks
		go ra.prefetch(id + 1)
	}
	ra.lastBlockID = id

	return ra.bm.ReadBlock(id)
}

// prefetch loads blocks into the cache in the background.
func (ra *BlockReadAhead) prefetch(startID uint64) {
	for i := 0; i < ra.readAheadSize; i++ {
		blockID := startID + uint64(i)
		// ReadBlock will cache the block if successful
		_, _ = ra.bm.ReadBlock(blockID)
	}
}

// ColumnDataBatch provides batch access to column data for optimized scanning.
type ColumnDataBatch struct {
	data       []byte
	validity   *ValidityMaskBatch
	typeID     LogicalTypeID
	valueSize  int
	tupleCount uint64
}

// NewColumnDataBatch creates a batch accessor for column data.
func NewColumnDataBatch(col *ColumnData) *ColumnDataBatch {
	if col == nil {
		return nil
	}
	return &ColumnDataBatch{
		data:       col.Data,
		validity:   NewValidityMaskBatch(col.Validity),
		typeID:     col.TypeID,
		valueSize:  GetTypeSizeFast(col.TypeID),
		tupleCount: col.TupleCount,
	}
}

// GetInt64 retrieves an int64 value at the given index.
// Returns (0, false) if the value is NULL or out of bounds.
func (b *ColumnDataBatch) GetInt64(rowIdx uint64) (int64, bool) {
	if rowIdx >= b.tupleCount {
		return 0, false
	}
	if b.validity != nil && !b.validity.IsValidAt(rowIdx) {
		return 0, false
	}

	offset := rowIdx * uint64(b.valueSize)
	if offset+uint64(b.valueSize) > uint64(len(b.data)) {
		return 0, false
	}

	switch b.valueSize {
	case 1:
		return int64(int8(b.data[offset])), true
	case 2:
		return int64(int16(uint16(b.data[offset]) | uint16(b.data[offset+1])<<8)), true
	case 4:
		return int64(int32(uint32(b.data[offset]) | uint32(b.data[offset+1])<<8 |
			uint32(b.data[offset+2])<<16 | uint32(b.data[offset+3])<<24)), true
	case 8:
		return int64(uint64(b.data[offset]) | uint64(b.data[offset+1])<<8 |
			uint64(b.data[offset+2])<<16 | uint64(b.data[offset+3])<<24 |
			uint64(b.data[offset+4])<<32 | uint64(b.data[offset+5])<<40 |
			uint64(b.data[offset+6])<<48 | uint64(b.data[offset+7])<<56), true
	default:
		return 0, false
	}
}

// GetFloat64 retrieves a float64 value at the given index.
// Returns (0, false) if the value is NULL or out of bounds.
func (b *ColumnDataBatch) GetFloat64(rowIdx uint64) (float64, bool) {
	if rowIdx >= b.tupleCount {
		return 0, false
	}
	if b.validity != nil && !b.validity.IsValidAt(rowIdx) {
		return 0, false
	}

	offset := rowIdx * uint64(b.valueSize)
	if offset+uint64(b.valueSize) > uint64(len(b.data)) {
		return 0, false
	}

	switch b.valueSize {
	case 4:
		bits := uint32(b.data[offset]) | uint32(b.data[offset+1])<<8 |
			uint32(b.data[offset+2])<<16 | uint32(b.data[offset+3])<<24
		return float64(float32FromBits(bits)), true
	case 8:
		bits := uint64(b.data[offset]) | uint64(b.data[offset+1])<<8 |
			uint64(b.data[offset+2])<<16 | uint64(b.data[offset+3])<<24 |
			uint64(b.data[offset+4])<<32 | uint64(b.data[offset+5])<<40 |
			uint64(b.data[offset+6])<<48 | uint64(b.data[offset+7])<<56
		return float64FromBits(bits), true
	default:
		return 0, false
	}
}
