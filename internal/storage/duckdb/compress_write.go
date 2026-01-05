// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements compression write paths for
// writing compressed column data.
package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
)

// Compression write error messages.
var (
	// ErrCompressionFailed is returned when compression fails unexpectedly.
	ErrCompressionFailed = errors.New("compression failed")

	// ErrNotConstant is returned when data cannot be compressed using CONSTANT
	// compression because values are not all identical.
	ErrNotConstant = errors.New("data is not constant: values differ")

	// ErrEmptyData is returned when attempting to compress empty data.
	ErrEmptyData = errors.New("cannot compress empty data")
)

// Compressor is the interface for all compression algorithms.
// Implementations compress raw data bytes into a compressed format
// that can later be decompressed using the corresponding Decompressor.
type Compressor interface {
	// Compress compresses the input data.
	// Parameters:
	//   - data: The raw data bytes to compress
	//   - valueSize: Size in bytes of each value
	// Returns the compressed data or an error if compression fails.
	Compress(data []byte, valueSize int) ([]byte, error)

	// Type returns the compression type identifier for this compressor.
	Type() CompressionType
}

// CompressionHeader contains metadata about a compressed segment.
// This may be used as a prefix to compressed data or stored separately
// in the row group metadata.
type CompressionHeader struct {
	// Type is the compression algorithm used.
	Type CompressionType

	// ValueSize is the byte size of each uncompressed value.
	ValueSize uint32

	// Count is the number of values in the segment.
	Count uint64
}

// CompressConstant compresses data where all values are identical.
// This is the most efficient compression when all values are the same,
// reducing the data to just a single value regardless of count.
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The single constant value (just valueSize bytes)
//   - bool: true if compression succeeded (all values identical),
//     false if values differ
//
// Example:
//
//	// Compress 100 copies of the same int32 value
//	data := make([]byte, 400) // 100 * 4 bytes
//	for i := 0; i < 100; i++ {
//	    binary.LittleEndian.PutUint32(data[i*4:], 42)
//	}
//	compressed, ok := CompressConstant(data, 4)
//	// compressed = []byte{42, 0, 0, 0}, ok = true
func CompressConstant(data []byte, valueSize int) ([]byte, bool) {
	// Validate inputs
	if valueSize <= 0 || len(data) < valueSize {
		return nil, false
	}

	// Not worth compressing single value (no benefit)
	if len(data) < valueSize*2 {
		return nil, false
	}

	// Verify data length is a multiple of valueSize
	if len(data)%valueSize != 0 {
		return nil, false
	}

	// Extract the first value
	firstValue := data[:valueSize]

	// Check if all subsequent values are identical to the first
	for i := valueSize; i < len(data); i += valueSize {
		if !bytes.Equal(data[i:i+valueSize], firstValue) {
			return nil, false // Not all same value
		}
	}

	// All values are identical - return just the first value
	result := make([]byte, valueSize)
	copy(result, firstValue)
	return result, true
}

// TryCompressConstant attempts CONSTANT compression with error handling.
// This is a convenience wrapper around CompressConstant that returns
// an error on failure instead of a boolean.
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The compressed data (single value) if successful
//   - bool: true if compression succeeded
//
// This function never returns an error; it returns (nil, false) on failure.
func TryCompressConstant(data []byte, valueSize int) ([]byte, bool) {
	return CompressConstant(data, valueSize)
}

// MustCompressConstant compresses data using CONSTANT compression,
// returning an error if the data is not suitable for constant compression.
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The compressed data (single value)
//   - error: nil on success, ErrNotConstant if values differ,
//     ErrInvalidValueSize if valueSize is invalid,
//     ErrEmptyData if data is empty
func MustCompressConstant(data []byte, valueSize int) ([]byte, error) {
	if valueSize <= 0 {
		return nil, ErrInvalidValueSize
	}
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	if len(data) < valueSize {
		return nil, fmt.Errorf("%w: data length %d is less than value size %d",
			ErrInvalidValueSize, len(data), valueSize)
	}

	compressed, ok := CompressConstant(data, valueSize)
	if !ok {
		return nil, ErrNotConstant
	}
	return compressed, nil
}

// Compress compresses data using the specified algorithm.
// This is the main dispatch function for compression write paths.
//
// Parameters:
//   - compression: The compression algorithm to use
//   - data: The raw data bytes to compress
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The compressed data
//   - error: nil on success, ErrUnsupportedCompression if algorithm not implemented
//
// For CompressionConstant, this will fail if data is not actually constant.
// Use TryCompress for fallback behavior.
func Compress(compression CompressionType, data []byte, valueSize int) ([]byte, error) {
	switch compression {
	case CompressionUncompressed:
		// Return data as-is for uncompressed
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil

	case CompressionConstant:
		return MustCompressConstant(data, valueSize)

	case CompressionRLE:
		return CompressRLE(data, valueSize)

	case CompressionDictionary:
		return CompressDictionary(data, valueSize)

	case CompressionBitPacking:
		return CompressBitPacking(data, valueSize)

	case CompressionPFORDelta:
		return CompressPFORDelta(data, valueSize)

	case CompressionEmpty:
		// Empty compression produces no data
		return []byte{}, nil

	default:
		return nil, fmt.Errorf("%w: %s (type %d)",
			ErrUnsupportedCompression, compression.String(), compression)
	}
}

// TryCompress attempts to compress data using the specified algorithm,
// falling back to uncompressed if the specified algorithm fails or is
// not suitable for the data.
//
// Parameters:
//   - compression: The preferred compression algorithm
//   - data: The raw data bytes to compress
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The compressed data
//   - CompressionType: The actual compression type used
//   - error: nil on success (may use different compression than requested)
func TryCompress(compression CompressionType, data []byte, valueSize int) ([]byte, CompressionType, error) {
	if len(data) == 0 {
		return []byte{}, CompressionEmpty, nil
	}

	// Try the requested compression
	compressed, err := Compress(compression, data, valueSize)
	if err == nil {
		return compressed, compression, nil
	}

	// If the requested compression failed, fall back to uncompressed
	result := make([]byte, len(data))
	copy(result, data)
	return result, CompressionUncompressed, nil
}

// constantCompressor implements the Compressor interface for CONSTANT compression.
type constantCompressor struct{}

// Compress implements Compressor.Compress for CONSTANT compression.
func (c *constantCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	return MustCompressConstant(data, valueSize)
}

// Type implements Compressor.Type for CONSTANT compression.
func (c *constantCompressor) Type() CompressionType {
	return CompressionConstant
}

// NewConstantCompressor creates a new Compressor for CONSTANT compression.
func NewConstantCompressor() Compressor {
	return &constantCompressor{}
}

// uncompressedCompressor implements the Compressor interface for uncompressed data.
type uncompressedCompressor struct{}

// Compress implements Compressor.Compress for uncompressed data.
func (c *uncompressedCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// Type implements Compressor.Type for uncompressed data.
func (c *uncompressedCompressor) Type() CompressionType {
	return CompressionUncompressed
}

// NewUncompressedCompressor creates a new Compressor for uncompressed data.
func NewUncompressedCompressor() Compressor {
	return &uncompressedCompressor{}
}

// GetCompressor returns the appropriate Compressor for the given compression type.
// This allows for object-oriented usage patterns where a single compressor instance
// can be reused for multiple compressions.
//
// Returns nil if the compression type is not supported.
func GetCompressor(compression CompressionType) Compressor {
	switch compression {
	case CompressionUncompressed:
		return NewUncompressedCompressor()
	case CompressionConstant:
		return NewConstantCompressor()
	case CompressionRLE:
		return NewRLECompressor()
	case CompressionDictionary:
		return NewDictionaryCompressor()
	case CompressionBitPacking:
		return NewBitPackingCompressor()
	case CompressionPFORDelta:
		return NewPFORDeltaCompressor()
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// RLE Compression Write Path
// ---------------------------------------------------------------------------

// ErrRLENotBeneficial is returned when RLE compression does not reduce size.
var ErrRLENotBeneficial = errors.New("RLE compression not beneficial: no runs found")

// CompressRLE compresses data using run-length encoding.
// Each run of identical values is stored as (count, value) pairs,
// where count is encoded as a varint for space efficiency.
//
// Format: [varint count][value bytes][varint count][value bytes]...
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: RLE compressed data
//   - error: nil on success, ErrRLENotBeneficial if compression is not beneficial
//
// Example:
//
//	// Data: [42, 42, 42, 100, 100] as int32 values
//	// Encodes as: [varint(3)][42 as int32][varint(2)][100 as int32]
//	data := make([]byte, 20) // 5 int32 values
//	// ... fill data ...
//	compressed, err := CompressRLE(data, 4)
func CompressRLE(data []byte, valueSize int) ([]byte, error) {
	compressed, ok := TryCompressRLE(data, valueSize)
	if !ok {
		return nil, ErrRLENotBeneficial
	}
	return compressed, nil
}

// TryCompressRLE attempts RLE compression and returns whether it was beneficial.
// Returns (compressed data, true) if compression is beneficial (fewer runs than values).
// Returns (nil, false) if RLE doesn't help (no runs or compression not beneficial).
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressRLE(data []byte, valueSize int) ([]byte, bool) {
	// Validate inputs
	if valueSize <= 0 || len(data) < valueSize {
		return nil, false
	}

	// Verify data length is a multiple of valueSize
	if len(data)%valueSize != 0 {
		return nil, false
	}

	valueCount := len(data) / valueSize

	// Not worth compressing single value
	if valueCount < 2 {
		return nil, false
	}

	var buf bytes.Buffer
	runCount := 0

	i := 0
	for i < len(data) {
		// Get the current value
		runValue := data[i : i+valueSize]
		count := uint64(1)
		i += valueSize

		// Find run length - count consecutive identical values
		for i < len(data) && bytes.Equal(data[i:i+valueSize], runValue) {
			count++
			i += valueSize
		}

		// Write run: [varint count][value]
		writeUvarint(&buf, count)
		buf.Write(runValue)
		runCount++
	}

	// Only beneficial if we have fewer runs than values
	if runCount >= valueCount {
		return nil, false // RLE doesn't help
	}

	return buf.Bytes(), true
}

// writeUvarint writes a variable-length unsigned integer to a writer.
// Uses the same encoding as binary.PutUvarint.
func writeUvarint(w io.Writer, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, _ = w.Write(buf[:n]) // Ignore error: writing to bytes.Buffer always succeeds
}

// rleCompressor implements the Compressor interface for RLE compression.
type rleCompressor struct{}

// Compress implements Compressor.Compress for RLE compression.
func (c *rleCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	return CompressRLE(data, valueSize)
}

// Type implements Compressor.Type for RLE compression.
func (c *rleCompressor) Type() CompressionType {
	return CompressionRLE
}

// NewRLECompressor creates a new Compressor for RLE compression.
func NewRLECompressor() Compressor {
	return &rleCompressor{}
}

// ErrDictionaryNotBeneficial is returned when DICTIONARY compression does not reduce size.
var ErrDictionaryNotBeneficial = errors.New("DICTIONARY compression not beneficial: too many unique values")

// CompressDictionary compresses data using dictionary encoding.
// Unique values are stored in a dictionary, and each value is
// replaced with its dictionary index.
//
// Format:
//
//	[uint32 dictSize]                       - Number of dictionary entries
//	[dictSize values]                       - Dictionary values:
//	  - If valueSize > 0: each value is valueSize bytes (fixed-size)
//	  - If valueSize <= 0: each value is [uint32 length][bytes...] (variable-size strings)
//	[uint64 indexCount]                     - Number of indices
//	[indexCount uint32 indices]             - Indices into the dictionary
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize for fixed-size)
//   - valueSize: Size in bytes of each value. Use positive value for fixed-size types
//     (e.g., 4 for int32, 8 for int64). Use 0 or negative for variable-size strings.
//
// Returns:
//   - []byte: Dictionary compressed data
//   - error: nil on success, ErrDictionaryNotBeneficial if compression is not beneficial
//
// Example (fixed-size int32):
//
//	// Data: [42, 42, 100, 42, 100] as int32 values
//	// Dictionary: [42, 100] (2 entries)
//	// Indices: [0, 0, 1, 0, 1]
//	compressed, err := CompressDictionary(data, 4)
func CompressDictionary(data []byte, valueSize int) ([]byte, error) {
	compressed, ok := TryCompressDictionary(data, valueSize)
	if !ok {
		return nil, ErrDictionaryNotBeneficial
	}
	return compressed, nil
}

// TryCompressDictionary attempts DICTIONARY compression and returns whether it was beneficial.
// Returns (compressed data, true) if compression is beneficial (dictionary + indices < original).
// Returns (nil, false) if DICTIONARY doesn't help (too many unique values).
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize for fixed-size)
//   - valueSize: Size in bytes of each value. Use positive value for fixed-size types.
//     Use 0 or negative for variable-size strings.
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressDictionary(data []byte, valueSize int) ([]byte, bool) {
	if valueSize <= 0 {
		return compressDictionaryVariableSize(data)
	}
	return compressDictionaryFixedSize(data, valueSize)
}

// compressDictionaryFixedSize compresses fixed-size values using dictionary encoding.
func compressDictionaryFixedSize(data []byte, valueSize int) ([]byte, bool) {
	// Validate inputs
	if valueSize <= 0 || len(data) < valueSize {
		return nil, false
	}

	// Verify data length is a multiple of valueSize
	if len(data)%valueSize != 0 {
		return nil, false
	}

	valueCount := len(data) / valueSize

	// Not worth compressing single value or empty data
	if valueCount < 2 {
		return nil, false
	}

	// Build dictionary
	dictionary := make([][]byte, 0)
	valueToIndex := make(map[string]uint32)
	indices := make([]uint32, valueCount)

	for i := 0; i < valueCount; i++ {
		value := data[i*valueSize : (i+1)*valueSize]
		key := string(value)

		if idx, ok := valueToIndex[key]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictionary))
			// Make a copy of the value for the dictionary
			valueCopy := make([]byte, valueSize)
			copy(valueCopy, value)
			dictionary = append(dictionary, valueCopy)
			valueToIndex[key] = idx
			indices[i] = idx
		}
	}

	// Check if dictionary compression is beneficial
	// Dictionary size + indices must be smaller than original
	dictBytes := len(dictionary) * valueSize
	indicesBytes := valueCount * 4 // uint32 per index
	overhead := 4 + 8              // dictSize (uint32) + indexCount (uint64) headers
	compressedSize := overhead + dictBytes + indicesBytes

	if compressedSize >= len(data) {
		return nil, false
	}

	// Write compressed format
	var buf bytes.Buffer

	// Write dictionary size
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(dictionary))); err != nil {
		return nil, false
	}

	// Write dictionary values
	for _, v := range dictionary {
		buf.Write(v)
	}

	// Write index count
	if err := binary.Write(&buf, binary.LittleEndian, uint64(valueCount)); err != nil {
		return nil, false
	}

	// Write indices
	for _, idx := range indices {
		if err := binary.Write(&buf, binary.LittleEndian, idx); err != nil {
			return nil, false
		}
	}

	return buf.Bytes(), true
}

// compressDictionaryVariableSize compresses variable-size (string/blob) data using dictionary encoding.
// Variable-size data is expected to be a series of length-prefixed values:
// [uint32 length1][bytes1...][uint32 length2][bytes2...]...
//
// For simplicity, this implementation reads string data that has already been
// length-prefixed. If the input is raw strings without prefixes, use a wrapper
// function to add the prefixes first.
func compressDictionaryVariableSize(data []byte) ([]byte, bool) {
	// Validate input
	if len(data) == 0 {
		return nil, false
	}

	// Parse the input data to extract individual strings
	// Expected format: [uint32 length][bytes...] repeated
	values := make([][]byte, 0)
	offset := 0

	for offset < len(data) {
		// Need at least 4 bytes for length
		if offset+4 > len(data) {
			return nil, false
		}

		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Check if we have enough data
		if offset+int(length) > len(data) {
			return nil, false
		}

		// Extract the value
		value := make([]byte, length)
		copy(value, data[offset:offset+int(length)])
		values = append(values, value)
		offset += int(length)
	}

	valueCount := len(values)

	// Not worth compressing single value or empty data
	if valueCount < 2 {
		return nil, false
	}

	// Build dictionary
	dictionary := make([][]byte, 0)
	valueToIndex := make(map[string]uint32)
	indices := make([]uint32, valueCount)

	for i, value := range values {
		key := string(value)

		if idx, ok := valueToIndex[key]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictionary))
			dictionary = append(dictionary, value)
			valueToIndex[key] = idx
			indices[i] = idx
		}
	}

	// Calculate compressed size for variable-size format
	// Each dictionary entry: 4 bytes (length) + len(value) bytes
	dictBytes := 0
	for _, v := range dictionary {
		dictBytes += 4 + len(v) // uint32 length + value bytes
	}
	indicesBytes := valueCount * 4 // uint32 per index
	overhead := 4 + 8              // dictSize (uint32) + indexCount (uint64) headers
	compressedSize := overhead + dictBytes + indicesBytes

	if compressedSize >= len(data) {
		return nil, false
	}

	// Write compressed format
	var buf bytes.Buffer

	// Write dictionary size
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(dictionary))); err != nil {
		return nil, false
	}

	// Write dictionary values (with length prefix for each)
	for _, v := range dictionary {
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(v))); err != nil {
			return nil, false
		}
		buf.Write(v)
	}

	// Write index count
	if err := binary.Write(&buf, binary.LittleEndian, uint64(valueCount)); err != nil {
		return nil, false
	}

	// Write indices
	for _, idx := range indices {
		if err := binary.Write(&buf, binary.LittleEndian, idx); err != nil {
			return nil, false
		}
	}

	return buf.Bytes(), true
}

// dictionaryCompressor implements the Compressor interface for DICTIONARY compression.
type dictionaryCompressor struct{}

// Compress implements Compressor.Compress for DICTIONARY compression.
func (c *dictionaryCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	return CompressDictionary(data, valueSize)
}

// Type implements Compressor.Type for DICTIONARY compression.
func (c *dictionaryCompressor) Type() CompressionType {
	return CompressionDictionary
}

// NewDictionaryCompressor creates a new Compressor for DICTIONARY compression.
func NewDictionaryCompressor() Compressor {
	return &dictionaryCompressor{}
}

// ErrBitPackingNotBeneficial is returned when BITPACKING compression does not reduce size.
var ErrBitPackingNotBeneficial = errors.New("BITPACKING compression not beneficial: packed size >= original size")

// CompressBitPacking compresses integer data using bit packing.
// Values are stored using the minimum number of bits needed to
// represent the range of values.
//
// Format: [uint8 bitWidth][uint64 count][packed bits...]
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value (1, 2, 4, or 8 for integers)
//
// Returns:
//   - []byte: Bit-packed compressed data with header
//   - error: nil on success, ErrBitPackingNotBeneficial if compression is not beneficial
//
// Example:
//
//	// Compress values [0, 1, 2, 3] as int32
//	data := make([]byte, 16) // 4 int32 values
//	binary.LittleEndian.PutUint32(data[0:], 0)
//	binary.LittleEndian.PutUint32(data[4:], 1)
//	binary.LittleEndian.PutUint32(data[8:], 2)
//	binary.LittleEndian.PutUint32(data[12:], 3)
//	compressed, err := CompressBitPacking(data, 4)
//	// compressed = [2, 4, 0, 0, 0, 0, 0, 0, 0, 0xE4] (2 bits per value, 4 values)
func CompressBitPacking(data []byte, valueSize int) ([]byte, error) {
	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)
	if !ok {
		return nil, ErrBitPackingNotBeneficial
	}
	return compressed, nil
}

// CompressBitPackingFromUint64 compresses integers using bit-packing.
// Takes values as []uint64 and returns (compressed data, true) if compression is beneficial.
//
// Format: [uint8 bitWidth][uint64 count][packed bits...]
//
// Parameters:
//   - values: The uint64 values to compress
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func CompressBitPackingFromUint64(values []uint64) ([]byte, bool) {
	if len(values) == 0 {
		return nil, false
	}

	// Find the maximum value to determine bit width
	maxVal := uint64(0)
	for _, v := range values {
		if v > maxVal {
			maxVal = v
		}
	}

	// Calculate required bit width
	bitWidth := bitsNeededForValue(maxVal)
	if bitWidth == 0 {
		bitWidth = 1 // At least 1 bit
	}

	// Check if packing is beneficial
	// Original: 8 bytes per value (as uint64)
	// Packed: 1 (bitWidth) + 8 (count) + ceil(count * bitWidth / 8)
	packedSize := 1 + 8 + (len(values)*int(bitWidth)+7)/8
	originalSize := len(values) * 8
	if packedSize >= originalSize {
		return nil, false
	}

	// Pack the bits
	var buf bytes.Buffer
	buf.WriteByte(bitWidth)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(values))); err != nil {
		return nil, false
	}

	packed := packBits(values, bitWidth)
	buf.Write(packed)

	return buf.Bytes(), true
}

// TryCompressBitPackingFromBytes attempts BITPACKING compression from raw bytes.
// Converts bytes to uint64 based on valueSize, then packs.
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value (1, 2, 4, or 8)
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressBitPackingFromBytes(data []byte, valueSize int) ([]byte, bool) {
	// Validate inputs
	if valueSize <= 0 || len(data) < valueSize {
		return nil, false
	}

	// Verify data length is a multiple of valueSize
	if len(data)%valueSize != 0 {
		return nil, false
	}

	valueCount := len(data) / valueSize

	// Not worth compressing single value
	if valueCount < 2 {
		return nil, false
	}

	// Convert bytes to uint64 values
	values := make([]uint64, valueCount)
	for i := 0; i < valueCount; i++ {
		offset := i * valueSize
		switch valueSize {
		case 1:
			values[i] = uint64(data[offset])
		case 2:
			values[i] = uint64(binary.LittleEndian.Uint16(data[offset:]))
		case 4:
			values[i] = uint64(binary.LittleEndian.Uint32(data[offset:]))
		case 8:
			values[i] = binary.LittleEndian.Uint64(data[offset:])
		default:
			// For non-standard sizes, read bytes as little-endian value
			var v uint64
			for j := 0; j < valueSize && j < 8; j++ {
				v |= uint64(data[offset+j]) << (j * 8)
			}
			values[i] = v
		}
	}

	// Find the maximum value to determine bit width
	maxVal := uint64(0)
	for _, v := range values {
		if v > maxVal {
			maxVal = v
		}
	}

	// Calculate required bit width
	bitWidth := bitsNeededForValue(maxVal)
	if bitWidth == 0 {
		bitWidth = 1 // At least 1 bit
	}

	// Check if packing is beneficial
	// Original: valueSize bytes per value
	// Packed: 1 (bitWidth) + 8 (count) + ceil(count * bitWidth / 8)
	packedSize := 1 + 8 + (valueCount*int(bitWidth)+7)/8
	originalSize := len(data)
	if packedSize >= originalSize {
		return nil, false
	}

	// Pack the bits
	var buf bytes.Buffer
	buf.WriteByte(bitWidth)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(valueCount)); err != nil {
		return nil, false
	}

	packed := packBits(values, bitWidth)
	buf.Write(packed)

	return buf.Bytes(), true
}

// TryCompressBitPacking attempts BITPACKING compression on uint64 values.
// This is an alias for CompressBitPackingFromUint64 for API consistency.
//
// Parameters:
//   - values: The uint64 values to compress
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressBitPacking(values []uint64) ([]byte, bool) {
	return CompressBitPackingFromUint64(values)
}

// bitsNeededForValue calculates the minimum number of bits needed
// to represent the given value.
//
// For value 0, returns 1 (we always need at least 1 bit).
// For value 1, returns 1.
// For value 2-3, returns 2.
// For value 4-7, returns 3.
// etc.
func bitsNeededForValue(v uint64) uint8 {
	if v == 0 {
		return 1
	}
	return uint8(64 - bits.LeadingZeros64(v))
}

// packBits packs the given values using the specified bit width.
// Bits are packed in little-endian order (LSB first within each byte).
//
// For example, packing [0, 1, 2, 3] with bitWidth=2:
// - Value 0: bits 00
// - Value 1: bits 01
// - Value 2: bits 10
// - Value 3: bits 11
// Packed as: 0b11100100 = 0xE4 (reading LSB first)
func packBits(values []uint64, bitWidth uint8) []byte {
	if len(values) == 0 || bitWidth == 0 {
		return []byte{}
	}

	totalBits := len(values) * int(bitWidth)
	numBytes := (totalBits + 7) / 8
	result := make([]byte, numBytes)

	bitPos := 0
	for _, value := range values {
		for b := uint8(0); b < bitWidth; b++ {
			if value&(1<<b) != 0 {
				byteIdx := bitPos / 8
				bitIdx := bitPos % 8
				result[byteIdx] |= 1 << bitIdx
			}
			bitPos++
		}
	}

	return result
}

// bitpackingCompressor implements the Compressor interface for BITPACKING compression.
type bitpackingCompressor struct{}

// Compress implements Compressor.Compress for BITPACKING compression.
func (c *bitpackingCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	return CompressBitPacking(data, valueSize)
}

// Type implements Compressor.Type for BITPACKING compression.
func (c *bitpackingCompressor) Type() CompressionType {
	return CompressionBitPacking
}

// NewBitPackingCompressor creates a new Compressor for BITPACKING compression.
func NewBitPackingCompressor() Compressor {
	return &bitpackingCompressor{}
}

// ErrPFORDeltaNotBeneficial is returned when PFOR_DELTA compression does not reduce size.
var ErrPFORDeltaNotBeneficial = errors.New("PFOR_DELTA compression not beneficial: packed size >= original size")

// CompressPFORDelta compresses integer data using Packed Frame of Reference
// with Delta encoding. Values are stored as a reference value plus
// bit-packed cumulative deltas.
//
// Format: [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
//   - reference (8 bytes): The starting value (signed int64)
//   - bitWidth (1 byte): Number of bits per delta value
//   - count (8 bytes): Total number of values (including reference)
//   - bit-packed deltas: count-1 deltas packed at bitWidth bits each
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value (1, 2, 4, or 8)
//
// Returns:
//   - []byte: PFOR_DELTA compressed data with header
//   - error: nil on success, ErrPFORDeltaNotBeneficial if compression is not beneficial
//
// Example:
//
//	// Compress sorted sequence [100, 101, 102, 103, 104] as int64
//	data := make([]byte, 40) // 5 * 8 bytes
//	for i := 0; i < 5; i++ {
//	    binary.LittleEndian.PutUint64(data[i*8:], uint64(100+i))
//	}
//	compressed, err := CompressPFORDelta(data, 8)
func CompressPFORDelta(data []byte, valueSize int) ([]byte, error) {
	compressed, ok := TryCompressPFORDeltaFromBytes(data, valueSize)
	if !ok {
		return nil, ErrPFORDeltaNotBeneficial
	}
	return compressed, nil
}

// CompressPFORDeltaFromInt64 compresses int64 values using PFOR_DELTA encoding.
// Takes values as []int64 and returns (compressed data, true) if compression is beneficial.
//
// Format: [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
//
// The function calculates deltas between consecutive values and bit-packs them
// using the minimum number of bits needed for the largest delta.
//
// Parameters:
//   - values: The int64 values to compress
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
//
// Example:
//
//	// Compress ascending sequence with small deltas
//	values := []int64{100, 101, 102, 103, 104}
//	compressed, ok := CompressPFORDeltaFromInt64(values)
//	// compressed contains: [ref=100][bitWidth=1][count=5][deltas: 1,1,1,1]
func CompressPFORDeltaFromInt64(values []int64) ([]byte, bool) {
	// Need at least 2 values for delta encoding to make sense
	if len(values) < 2 {
		return nil, false
	}

	// Calculate deltas from previous value
	deltas := make([]uint64, len(values)-1)
	reference := values[0]
	prev := reference
	maxDelta := uint64(0)

	for i := 1; i < len(values); i++ {
		// Delta from previous value (must be non-negative for unsigned storage)
		// For negative deltas, we'd need zigzag encoding - for now, convert to unsigned
		delta := uint64(values[i] - prev)
		deltas[i-1] = delta
		if delta > maxDelta {
			maxDelta = delta
		}
		prev = values[i]
	}

	// Calculate bit width for deltas
	bitWidth := bitsNeededForValue(maxDelta)
	if bitWidth == 0 {
		bitWidth = 1 // At least 1 bit
	}

	// Check if compression is beneficial
	// Original: 8 bytes per value
	// PFOR_DELTA: 8 (ref) + 1 (bitWidth) + 8 (count) + packed deltas
	packedDeltaSize := (len(deltas)*int(bitWidth) + 7) / 8
	compressedSize := 8 + 1 + 8 + packedDeltaSize
	originalSize := len(values) * 8

	if compressedSize >= originalSize {
		return nil, false
	}

	// Write compressed format
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, reference); err != nil {
		return nil, false
	}
	buf.WriteByte(bitWidth)
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(values))); err != nil {
		return nil, false
	}

	packedDeltas := packBits(deltas, bitWidth)
	buf.Write(packedDeltas)

	return buf.Bytes(), true
}

// TryCompressPFORDeltaFromBytes attempts PFOR_DELTA compression from raw bytes.
// Converts bytes to int64 based on valueSize, then compresses.
//
// Parameters:
//   - data: The raw data bytes (must be a multiple of valueSize)
//   - valueSize: Size in bytes of each value (1, 2, 4, or 8)
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressPFORDeltaFromBytes(data []byte, valueSize int) ([]byte, bool) {
	// Validate inputs
	if valueSize <= 0 || len(data) < valueSize {
		return nil, false
	}

	// Verify data length is a multiple of valueSize
	if len(data)%valueSize != 0 {
		return nil, false
	}

	valueCount := len(data) / valueSize

	// Not worth compressing fewer than 2 values
	if valueCount < 2 {
		return nil, false
	}

	// Convert bytes to int64 values
	values := make([]int64, valueCount)
	for i := 0; i < valueCount; i++ {
		offset := i * valueSize
		switch valueSize {
		case 1:
			// Treat as signed int8
			values[i] = int64(int8(data[offset]))
		case 2:
			// Treat as signed int16
			values[i] = int64(int16(binary.LittleEndian.Uint16(data[offset:])))
		case 4:
			// Treat as signed int32
			values[i] = int64(int32(binary.LittleEndian.Uint32(data[offset:])))
		case 8:
			// Treat as signed int64
			values[i] = int64(binary.LittleEndian.Uint64(data[offset:]))
		default:
			// For non-standard sizes, read bytes as little-endian unsigned value
			var v uint64
			for j := 0; j < valueSize && j < 8; j++ {
				v |= uint64(data[offset+j]) << (j * 8)
			}
			values[i] = int64(v)
		}
	}

	return CompressPFORDeltaFromInt64(values)
}

// TryCompressPFORDelta attempts PFOR_DELTA compression on int64 values.
// This is an alias for CompressPFORDeltaFromInt64 for API consistency.
//
// Parameters:
//   - values: The int64 values to compress
//
// Returns:
//   - []byte: The compressed data if successful, nil otherwise
//   - bool: true if compression succeeded and is beneficial, false otherwise
func TryCompressPFORDelta(values []int64) ([]byte, bool) {
	return CompressPFORDeltaFromInt64(values)
}

// pfordeltaCompressor implements the Compressor interface for PFOR_DELTA compression.
type pfordeltaCompressor struct{}

// Compress implements Compressor.Compress for PFOR_DELTA compression.
func (c *pfordeltaCompressor) Compress(data []byte, valueSize int) ([]byte, error) {
	return CompressPFORDelta(data, valueSize)
}

// Type implements Compressor.Type for PFOR_DELTA compression.
func (c *pfordeltaCompressor) Type() CompressionType {
	return CompressionPFORDelta
}

// NewPFORDeltaCompressor creates a new Compressor for PFOR_DELTA compression.
func NewPFORDeltaCompressor() Compressor {
	return &pfordeltaCompressor{}
}
