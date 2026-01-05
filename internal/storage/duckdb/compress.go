package duckdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Decompression error messages.
var (
	// ErrConstantDataTooShort is returned when constant compressed data is shorter
	// than the expected value size.
	ErrConstantDataTooShort = errors.New("constant compressed data too short for value size")

	// ErrUnsupportedCompression is returned when attempting to decompress data
	// using an unsupported or unimplemented compression algorithm.
	ErrUnsupportedCompression = errors.New("unsupported compression type")

	// ErrInvalidValueSize is returned when the value size is zero or negative.
	ErrInvalidValueSize = errors.New("invalid value size: must be positive")

	// ErrRLEDataTruncated is returned when RLE compressed data ends prematurely
	// before all runs can be read.
	ErrRLEDataTruncated = errors.New("RLE compressed data truncated")

	// ErrRLEInvalidVarint is returned when an invalid varint is encountered
	// in RLE compressed data.
	ErrRLEInvalidVarint = errors.New("RLE compressed data contains invalid varint")

	// ErrDictionaryDataTruncated is returned when dictionary compressed data ends prematurely.
	ErrDictionaryDataTruncated = errors.New("dictionary compressed data truncated")

	// ErrDictionaryIndexOutOfBounds is returned when a dictionary index references
	// a non-existent dictionary entry.
	ErrDictionaryIndexOutOfBounds = errors.New("dictionary index out of bounds")

	// ErrBitPackingDataTruncated is returned when bit-packed data ends prematurely
	// before all values can be read.
	ErrBitPackingDataTruncated = errors.New("bitpacking compressed data truncated")

	// ErrBitPackingInvalidBitWidth is returned when the bit width is invalid (> 64).
	ErrBitPackingInvalidBitWidth = errors.New("bitpacking invalid bit width")

	// ErrPFORDeltaDataTruncated is returned when PFOR_DELTA compressed data is shorter
	// than the required header size (17 bytes minimum: 8 bytes reference + 1 byte bitWidth + 8 bytes count).
	ErrPFORDeltaDataTruncated = errors.New("PFOR_DELTA compressed data truncated")
)

// Decompressor is the interface for all decompression algorithms.
// Implementations decompress data from DuckDB's compressed format back to
// raw bytes that can be interpreted as column values.
type Decompressor interface {
	// Decompress expands compressed data to the original uncompressed bytes.
	// Parameters:
	//   - data: The compressed data bytes
	//   - valueSize: Size in bytes of each value
	//   - count: Number of values to decompress
	// Returns the decompressed bytes or an error if decompression fails.
	Decompress(data []byte, valueSize int, count uint64) ([]byte, error)
}

// DecompressConstant expands a single value to count copies.
// This is used for CONSTANT compression where all values in a segment are identical.
// The compressed data contains just the single value bytes, and this function
// expands it to fill an array with count copies of that value.
//
// Parameters:
//   - data: The compressed data containing the single constant value
//   - valueSize: Size in bytes of each value (e.g., 4 for int32, 8 for int64)
//   - count: Number of values to expand to
//
// Returns:
//   - []byte: Decompressed data containing count copies of the value
//   - error: ErrConstantDataTooShort if data is smaller than valueSize,
//     ErrInvalidValueSize if valueSize is not positive
//
// Example:
//
//	// Decompress a constant int32 value of 42 to 100 copies
//	data := []byte{42, 0, 0, 0}  // Little-endian int32
//	result, err := DecompressConstant(data, 4, 100)
//	// result contains 400 bytes (100 * 4 bytes per int32)
func DecompressConstant(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Validate valueSize
	if valueSize <= 0 {
		return nil, ErrInvalidValueSize
	}

	// Check that we have enough data for the constant value
	if len(data) < valueSize {
		return nil, fmt.Errorf("%w: need %d bytes, got %d",
			ErrConstantDataTooShort, valueSize, len(data))
	}

	// Handle edge case of zero count
	if count == 0 {
		return []byte{}, nil
	}

	// Extract the constant value
	value := data[:valueSize]

	// Allocate result buffer for count copies of the value
	resultSize := count * uint64(valueSize)
	result := make([]byte, resultSize)

	// Copy the value count times
	for i := uint64(0); i < count; i++ {
		offset := i * uint64(valueSize)
		copy(result[offset:], value)
	}

	return result, nil
}

// DecompressRLE decompresses run-length encoded data.
// RLE compression stores sequences of repeated values as (count, value) pairs,
// where count is encoded as a varint for space efficiency.
//
// Format: repeated (varint count, value bytes) pairs
// Each run consists of:
//   - A varint-encoded run count (number of times the value repeats)
//   - The value bytes (size determined by valueSize parameter)
//
// Parameters:
//   - data: The RLE compressed data containing (varint count, value) pairs
//   - valueSize: Size in bytes of each value (e.g., 4 for int32, 8 for int64)
//   - count: Expected number of values after decompression (used for validation)
//
// Returns:
//   - []byte: Decompressed data containing count copies of expanded values
//   - error: ErrInvalidValueSize if valueSize is not positive,
//     ErrRLEInvalidVarint if varint decoding fails,
//     ErrRLEDataTruncated if data ends before a complete run can be read
//
// Example:
//
//	// RLE data: run of 3x value 42 (int32), then run of 2x value 100 (int32)
//	// varint(3) = 0x03, value 42 as int32 LE = [42, 0, 0, 0]
//	// varint(2) = 0x02, value 100 as int32 LE = [100, 0, 0, 0]
//	data := []byte{0x03, 42, 0, 0, 0, 0x02, 100, 0, 0, 0}
//	result, err := DecompressRLE(data, 4, 5)
//	// result contains: [42,0,0,0, 42,0,0,0, 42,0,0,0, 100,0,0,0, 100,0,0,0]
func DecompressRLE(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Validate valueSize
	if valueSize <= 0 {
		return nil, ErrInvalidValueSize
	}

	// Handle edge case of empty data
	if len(data) == 0 {
		// Empty data with count 0 is valid
		if count == 0 {
			return []byte{}, nil
		}
		// Empty data with non-zero count is invalid
		return nil, fmt.Errorf("%w: expected %d values but got empty data",
			ErrRLEDataTruncated, count)
	}

	r := bytes.NewReader(data)
	var result []byte

	// Pre-allocate result buffer if count is known
	// This is an optimization hint; we'll grow if needed
	if count > 0 {
		result = make([]byte, 0, count*uint64(valueSize))
	}

	for r.Len() > 0 {
		// Read run count as varint
		runCount, err := binary.ReadUvarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("%w: incomplete varint at end of data",
					ErrRLEInvalidVarint)
			}
			return nil, fmt.Errorf("%w: %v", ErrRLEInvalidVarint, err)
		}

		// Read the value bytes (always present, even for count=0)
		value := make([]byte, valueSize)
		n, err := io.ReadFull(r, value)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("%w: expected %d value bytes, got %d",
					ErrRLEDataTruncated, valueSize, n)
			}
			return nil, fmt.Errorf("%w: %v", ErrRLEDataTruncated, err)
		}

		// Expand the run: append value runCount times
		// (count=0 simply contributes nothing to the result)
		for i := uint64(0); i < runCount; i++ {
			result = append(result, value...)
		}
	}

	return result, nil
}

// DecompressDictionary decompresses dictionary-encoded data.
// Dictionary compression stores unique values in a dictionary and uses
// indices to reference them. This is effective when there are few unique
// values relative to total values.
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
//   - data: The dictionary compressed data
//   - valueSize: Size in bytes of each value. Use positive value for fixed-size types
//     (e.g., 4 for int32, 8 for int64). Use 0 or negative for variable-size strings.
//   - count: Expected number of values after decompression (used for validation/hints)
//
// Returns:
//   - []byte: Decompressed data with values expanded from dictionary
//   - error: ErrInvalidValueSize if valueSize is invalid for fixed types,
//     ErrDictionaryDataTruncated if data ends prematurely,
//     ErrDictionaryIndexOutOfBounds if an index is out of range
//
// Example (fixed-size int32):
//
//	// Dictionary with 2 entries: 42 and 100
//	// Indices: [0, 0, 1, 0, 1] -> [42, 42, 100, 42, 100]
//	data := buildDictData([]int32{42, 100}, []uint32{0, 0, 1, 0, 1})
//	result, err := DecompressDictionary(data, 4, 5)
//
// Example (variable-size strings):
//
//	// Dictionary: ["hello", "world"]
//	// Indices: [0, 1, 0] -> ["hello", "world", "hello"]
//	data := buildStringDictData([]string{"hello", "world"}, []uint32{0, 1, 0})
//	result, err := DecompressDictionary(data, 0, 3)
func DecompressDictionary(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Handle empty data
	if len(data) == 0 {
		if count == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: expected %d values but got empty data",
			ErrDictionaryDataTruncated, count)
	}

	r := bytes.NewReader(data)

	// Read dictionary size
	var dictSize uint32
	if err := binary.Read(r, binary.LittleEndian, &dictSize); err != nil {
		return nil, fmt.Errorf("%w: failed to read dictionary size: %v",
			ErrDictionaryDataTruncated, err)
	}

	// Read dictionary values
	dictionary := make([][]byte, dictSize)
	for i := uint32(0); i < dictSize; i++ {
		if valueSize > 0 {
			// Fixed-size values
			value := make([]byte, valueSize)
			n, err := io.ReadFull(r, value)
			if err != nil {
				return nil, fmt.Errorf("%w: failed to read dictionary entry %d: expected %d bytes, got %d",
					ErrDictionaryDataTruncated, i, valueSize, n)
			}
			dictionary[i] = value
		} else {
			// Variable-size (strings): read uint32 length then bytes
			var length uint32
			if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
				return nil, fmt.Errorf("%w: failed to read string length for dictionary entry %d: %v",
					ErrDictionaryDataTruncated, i, err)
			}

			value := make([]byte, length)
			if length > 0 {
				n, err := io.ReadFull(r, value)
				if err != nil {
					return nil, fmt.Errorf("%w: failed to read string value for dictionary entry %d: expected %d bytes, got %d",
						ErrDictionaryDataTruncated, i, length, n)
				}
			}
			dictionary[i] = value
		}
	}

	// Read index count
	var indexCount uint64
	if err := binary.Read(r, binary.LittleEndian, &indexCount); err != nil {
		return nil, fmt.Errorf("%w: failed to read index count: %v",
			ErrDictionaryDataTruncated, err)
	}

	// Handle empty index list
	if indexCount == 0 {
		return nil, nil
	}

	// Pre-calculate result size for fixed-size values
	var result []byte
	if valueSize > 0 {
		result = make([]byte, 0, indexCount*uint64(valueSize))
	}

	// Read indices and expand values
	for i := uint64(0); i < indexCount; i++ {
		var idx uint32
		if err := binary.Read(r, binary.LittleEndian, &idx); err != nil {
			return nil, fmt.Errorf("%w: failed to read index %d: %v",
				ErrDictionaryDataTruncated, i, err)
		}

		// Validate index bounds
		if idx >= dictSize {
			return nil, fmt.Errorf("%w: index %d at position %d out of range (dict size: %d)",
				ErrDictionaryIndexOutOfBounds, idx, i, dictSize)
		}

		result = append(result, dictionary[idx]...)
	}

	return result, nil
}

// DecompressBitPacking decompresses bit-packed integer data.
// Bit-packing stores integers using only the minimum number of bits needed,
// reducing storage for small values.
//
// Format: [uint8 bitWidth][uint64 count][packed bits...]
//
// The packed bits are stored in little-endian order, where bits are extracted
// starting from the least significant bit of each byte.
//
// Parameters:
//   - data: The bit-packed compressed data
//   - valueSize: Size in bytes of each target value (1, 2, 4, or 8)
//   - count: Expected number of values (used for validation, actual count is in data)
//
// Returns:
//   - []byte: Decompressed data as byte slice
//   - error: ErrBitPackingDataTruncated if data is too short,
//     ErrBitPackingInvalidBitWidth if bit width exceeds 64
//
// Example:
//
//	// Bit-pack 4 values [0, 1, 2, 3] using 2 bits each
//	// Binary: 00, 01, 10, 11 -> packed as 0b11100100 = 0xE4
//	data := []byte{2, 4, 0, 0, 0, 0, 0, 0, 0, 0xE4}  // bitWidth=2, count=4, packed byte
//	result, err := DecompressBitPacking(data, 1, 4)
func DecompressBitPacking(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Decompress to uint64 slice first
	values, err := DecompressBitPackingToUint64(data)
	if err != nil {
		return nil, err
	}

	// Convert to bytes with target size
	return BitPackedToBytes(values, valueSize), nil
}

// DecompressBitPackingToUint64 decompresses bit-packed integers to uint64 slice.
// This is the core decompression function that extracts bit-packed values.
//
// Format: [uint8 bitWidth][uint64 count][packed bits...]
//
// Parameters:
//   - data: The bit-packed compressed data
//
// Returns:
//   - []uint64: Unpacked values
//   - error: ErrBitPackingDataTruncated if data is too short,
//     ErrBitPackingInvalidBitWidth if bit width exceeds 64
func DecompressBitPackingToUint64(data []byte) ([]uint64, error) {
	// Minimum header size: 1 byte bitWidth + 8 bytes count = 9 bytes
	if len(data) < 9 {
		return nil, fmt.Errorf("%w: need at least 9 bytes for header, got %d",
			ErrBitPackingDataTruncated, len(data))
	}

	bitWidth := data[0]
	count := binary.LittleEndian.Uint64(data[1:9])
	packedData := data[9:]

	// Validate bit width
	if bitWidth > 64 {
		return nil, fmt.Errorf("%w: %d (max 64)", ErrBitPackingInvalidBitWidth, bitWidth)
	}

	// Handle edge case: zero count
	if count == 0 {
		return []uint64{}, nil
	}

	// Handle special case: bitWidth = 0 means all values are 0
	if bitWidth == 0 {
		return make([]uint64, count), nil
	}

	// Calculate expected packed data size
	totalBits := uint64(bitWidth) * count
	expectedBytes := (totalBits + 7) / 8 // Round up to nearest byte

	if uint64(len(packedData)) < expectedBytes {
		return nil, fmt.Errorf("%w: need %d bytes for %d values at %d bits each, got %d",
			ErrBitPackingDataTruncated, expectedBytes, count, bitWidth, len(packedData))
	}

	result := make([]uint64, count)
	bitPos := 0

	for i := uint64(0); i < count; i++ {
		var value uint64

		// Extract bitWidth bits for this value
		for b := uint8(0); b < bitWidth; b++ {
			byteIdx := bitPos / 8
			bitIdx := bitPos % 8

			// Check bounds (should not happen if expectedBytes check passed, but be safe)
			if byteIdx >= len(packedData) {
				return nil, fmt.Errorf("%w: unexpected end of data at bit position %d",
					ErrBitPackingDataTruncated, bitPos)
			}

			// Extract bit from packed data (little-endian bit order)
			if packedData[byteIdx]&(1<<bitIdx) != 0 {
				value |= 1 << b
			}
			bitPos++
		}
		result[i] = value
	}

	return result, nil
}

// BitPackedToBytes converts unpacked uint64 values to byte slice.
// The target size determines how many bytes per value in the output.
//
// Parameters:
//   - values: The unpacked uint64 values
//   - targetSize: Target byte size per value (1, 2, 4, or 8)
//
// Returns:
//   - []byte: Byte slice with values encoded at targetSize bytes each
//
// For targetSize:
//   - 1: Values are truncated to uint8
//   - 2: Values are truncated to uint16, stored little-endian
//   - 4: Values are truncated to uint32, stored little-endian
//   - 8: Values stored as uint64, little-endian
//
// If targetSize is not 1, 2, 4, or 8, it defaults to 8.
func BitPackedToBytes(values []uint64, targetSize int) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	// Normalize target size
	switch targetSize {
	case 1, 2, 4, 8:
		// Valid sizes
	default:
		targetSize = 8 // Default to 8 bytes
	}

	result := make([]byte, len(values)*targetSize)

	for i, v := range values {
		offset := i * targetSize
		switch targetSize {
		case 1:
			result[offset] = byte(v)
		case 2:
			binary.LittleEndian.PutUint16(result[offset:], uint16(v))
		case 4:
			binary.LittleEndian.PutUint32(result[offset:], uint32(v))
		case 8:
			binary.LittleEndian.PutUint64(result[offset:], v)
		}
	}

	return result
}

// DecompressPFORDelta decompresses PFOR_DELTA encoded data.
// PFOR_DELTA (Packed Frame of Reference with Delta) combines delta encoding
// with frame of reference encoding for efficient integer compression.
//
// Unlike plain FOR (Frame of Reference) where each value = reference + offset,
// PFOR_DELTA uses cumulative deltas: each value = previous_value + delta.
// This is very effective for sorted or nearly-sorted integer sequences.
//
// Format: [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
//   - reference (8 bytes): The starting value (signed int64)
//   - bitWidth (1 byte): Number of bits per delta value
//   - count (8 bytes): Number of values to reconstruct
//   - bit-packed deltas: Packed unsigned deltas from previous value
//
// Parameters:
//   - data: The PFOR_DELTA compressed data
//   - valueSize: Size in bytes of each value (used for output conversion)
//   - count: Expected number of values (for validation, actual count is in data)
//
// Returns:
//   - []byte: Decompressed data as little-endian int64 values
//   - error: ErrPFORDeltaDataTruncated if data is too short,
//     or errors from bit-packing decompression
//
// Example:
//
//	// Sorted sequence [100, 101, 102, 103, 104] stored as:
//	// reference=100, deltas=[1, 1, 1, 1] (each delta is 1)
//	// Note: first value IS the reference, so count-1 deltas are stored
//	result, err := DecompressPFORDelta(data, 8, 5)
func DecompressPFORDelta(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count = 17 bytes minimum
	if len(data) < 17 {
		return nil, fmt.Errorf("%w: need at least 17 bytes for header, got %d",
			ErrPFORDeltaDataTruncated, len(data))
	}

	// Read reference (starting value) - signed int64
	reference := int64(binary.LittleEndian.Uint64(data[0:8]))

	// Read bit width for deltas
	bitWidth := data[8]

	// Read count of values
	valueCount := binary.LittleEndian.Uint64(data[9:17])

	// Handle edge case: count=0
	if valueCount == 0 {
		return []byte{}, nil
	}

	// The first value is the reference itself, so we need valueCount-1 deltas
	deltaCount := valueCount - 1

	// Handle case with only one value (no deltas needed)
	if deltaCount == 0 {
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, uint64(reference))
		return result, nil
	}

	// Read bit-packed deltas using the raw data after header
	packedData := data[17:]
	deltas, err := DecompressBitPackingWithParams(packedData, bitWidth, deltaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress PFOR_DELTA deltas: %w", err)
	}

	// Apply deltas cumulatively to reconstruct values
	result := make([]byte, valueCount*8)
	current := reference

	// First value is the reference
	binary.LittleEndian.PutUint64(result[0:8], uint64(current))

	// Apply each delta to get subsequent values
	for i, delta := range deltas {
		current += int64(delta) // Delta from previous value (cumulative)
		offset := (i + 1) * 8
		binary.LittleEndian.PutUint64(result[offset:offset+8], uint64(current))
	}

	return result, nil
}

// DecompressPFORDeltaToInt64 decompresses PFOR_DELTA data directly to []int64.
// This is a convenience function that returns values as a slice of int64
// instead of raw bytes.
//
// Parameters:
//   - data: The PFOR_DELTA compressed data
//
// Returns:
//   - []int64: Decompressed values
//   - error: Decompression error
func DecompressPFORDeltaToInt64(data []byte) ([]int64, error) {
	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count = 17 bytes minimum
	if len(data) < 17 {
		return nil, fmt.Errorf("%w: need at least 17 bytes for header, got %d",
			ErrPFORDeltaDataTruncated, len(data))
	}

	// Read reference (starting value) - signed int64
	reference := int64(binary.LittleEndian.Uint64(data[0:8]))

	// Read bit width for deltas
	bitWidth := data[8]

	// Read count of values
	valueCount := binary.LittleEndian.Uint64(data[9:17])

	// Handle edge case: count=0
	if valueCount == 0 {
		return []int64{}, nil
	}

	// The first value is the reference itself, so we need valueCount-1 deltas
	deltaCount := valueCount - 1

	// Handle case with only one value (no deltas needed)
	if deltaCount == 0 {
		return []int64{reference}, nil
	}

	// Read bit-packed deltas using the raw data after header
	packedData := data[17:]
	deltas, err := DecompressBitPackingWithParams(packedData, bitWidth, deltaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress PFOR_DELTA deltas: %w", err)
	}

	// Apply deltas cumulatively to reconstruct values
	result := make([]int64, valueCount)
	current := reference

	// First value is the reference
	result[0] = current

	// Apply each delta to get subsequent values
	for i, delta := range deltas {
		current += int64(delta) // Delta from previous value (cumulative)
		result[i+1] = current
	}

	return result, nil
}

// DecompressBitPackingWithParams decompresses bit-packed integers using explicit parameters.
// Unlike DecompressBitPackingToUint64 which reads bitWidth and count from the data header,
// this function accepts them as parameters. This is useful when the parameters are known
// from an outer format (like PFOR_DELTA).
//
// Parameters:
//   - data: The raw bit-packed data (no header)
//   - bitWidth: Number of bits per value (0-64)
//   - count: Number of values to extract
//
// Returns:
//   - []uint64: Unpacked values
//   - error: ErrBitPackingDataTruncated if data is too short,
//     ErrBitPackingInvalidBitWidth if bit width exceeds 64
func DecompressBitPackingWithParams(data []byte, bitWidth uint8, count uint64) ([]uint64, error) {
	// Validate bit width
	if bitWidth > 64 {
		return nil, fmt.Errorf("%w: %d (max 64)", ErrBitPackingInvalidBitWidth, bitWidth)
	}

	// Handle edge case: zero count
	if count == 0 {
		return []uint64{}, nil
	}

	// Handle special case: bitWidth = 0 means all values are 0
	if bitWidth == 0 {
		return make([]uint64, count), nil
	}

	// Calculate expected packed data size
	totalBits := uint64(bitWidth) * count
	expectedBytes := (totalBits + 7) / 8 // Round up to nearest byte

	if uint64(len(data)) < expectedBytes {
		return nil, fmt.Errorf("%w: need %d bytes for %d values at %d bits each, got %d",
			ErrBitPackingDataTruncated, expectedBytes, count, bitWidth, len(data))
	}

	result := make([]uint64, count)
	bitPos := 0

	for i := uint64(0); i < count; i++ {
		var value uint64

		// Extract bitWidth bits for this value
		for b := uint8(0); b < bitWidth; b++ {
			byteIdx := bitPos / 8
			bitIdx := bitPos % 8

			// Check bounds (should not happen if expectedBytes check passed, but be safe)
			if byteIdx >= len(data) {
				return nil, fmt.Errorf("%w: unexpected end of data at bit position %d",
					ErrBitPackingDataTruncated, bitPos)
			}

			// Extract bit from packed data (little-endian bit order)
			if data[byteIdx]&(1<<bitIdx) != 0 {
				value |= 1 << b
			}
			bitPos++
		}
		result[i] = value
	}

	return result, nil
}

// DecodeZigZag converts an unsigned zigzag-encoded value to a signed int64.
// Zigzag encoding maps signed integers to unsigned integers so that small
// magnitude values (positive or negative) have small encodings:
//
//	0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
//
// This is useful for delta encoding where deltas may be positive or negative.
//
// Parameters:
//   - v: The unsigned zigzag-encoded value
//
// Returns:
//   - int64: The decoded signed value
//
// Example:
//
//	DecodeZigZag(0) = 0
//	DecodeZigZag(1) = -1
//	DecodeZigZag(2) = 1
//	DecodeZigZag(3) = -2
//	DecodeZigZag(4) = 2
func DecodeZigZag(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}

// EncodeZigZag converts a signed int64 to an unsigned zigzag-encoded value.
// This is the inverse of DecodeZigZag.
//
// Parameters:
//   - v: The signed value to encode
//
// Returns:
//   - uint64: The zigzag-encoded unsigned value
//
// Example:
//
//	EncodeZigZag(0) = 0
//	EncodeZigZag(-1) = 1
//	EncodeZigZag(1) = 2
//	EncodeZigZag(-2) = 3
//	EncodeZigZag(2) = 4
func EncodeZigZag(v int64) uint64 {
	return uint64((v >> 63) ^ (v << 1))
}

// Decompress dispatches to the appropriate decompressor based on compression type.
// This is the main entry point for decompressing column segment data.
//
// Parameters:
//   - compression: The compression type used to compress the data
//   - data: The compressed data bytes
//   - valueSize: Size in bytes of each value
//   - count: Number of values to decompress
//
// Returns:
//   - []byte: Decompressed data
//   - error: Decompression-specific error or ErrUnsupportedCompression
//
// Supported compression types:
//   - CompressionUncompressed: Returns data as-is (no decompression needed)
//   - CompressionConstant: Expands single value to count copies
//   - CompressionRLE: Run-length encoding with varint counts
//   - CompressionDictionary: Dictionary encoding with index lookup
//   - CompressionBitPacking: Bit-packed integers (not yet implemented)
//   - CompressionPFORDelta: PFOR with delta encoding (not yet implemented)
func Decompress(compression CompressionType, data []byte, valueSize int, count uint64) ([]byte, error) {
	switch compression {
	case CompressionUncompressed:
		// Uncompressed data is returned as-is
		// Caller is responsible for ensuring data has the correct size
		return data, nil

	case CompressionConstant:
		return DecompressConstant(data, valueSize, count)

	case CompressionRLE:
		return DecompressRLE(data, valueSize, count)

	case CompressionDictionary:
		return DecompressDictionary(data, valueSize, count)

	case CompressionBitPacking:
		return DecompressBitPacking(data, valueSize, count)

	case CompressionPFORDelta:
		return DecompressPFORDelta(data, valueSize, count)

	case CompressionEmpty:
		// Empty compression means no data - return empty slice
		return []byte{}, nil

	default:
		return nil, fmt.Errorf("%w: %s (type %d)",
			ErrUnsupportedCompression, compression.String(), compression)
	}
}

// constantDecompressor implements the Decompressor interface for CONSTANT compression.
type constantDecompressor struct{}

// Decompress implements Decompressor.Decompress for CONSTANT compression.
func (d *constantDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return DecompressConstant(data, valueSize, count)
}

// NewConstantDecompressor creates a new Decompressor for CONSTANT compression.
func NewConstantDecompressor() Decompressor {
	return &constantDecompressor{}
}

// rleDecompressor implements the Decompressor interface for RLE compression.
type rleDecompressor struct{}

// Decompress implements Decompressor.Decompress for RLE compression.
func (d *rleDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return DecompressRLE(data, valueSize, count)
}

// NewRLEDecompressor creates a new Decompressor for RLE compression.
func NewRLEDecompressor() Decompressor {
	return &rleDecompressor{}
}

// dictionaryDecompressor implements the Decompressor interface for DICTIONARY compression.
type dictionaryDecompressor struct{}

// Decompress implements Decompressor.Decompress for DICTIONARY compression.
func (d *dictionaryDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return DecompressDictionary(data, valueSize, count)
}

// NewDictionaryDecompressor creates a new Decompressor for DICTIONARY compression.
func NewDictionaryDecompressor() Decompressor {
	return &dictionaryDecompressor{}
}

// uncompressedDecompressor implements the Decompressor interface for uncompressed data.
type uncompressedDecompressor struct{}

// Decompress implements Decompressor.Decompress for uncompressed data.
func (d *uncompressedDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return data, nil
}

// NewUncompressedDecompressor creates a new Decompressor for uncompressed data.
func NewUncompressedDecompressor() Decompressor {
	return &uncompressedDecompressor{}
}

// bitpackingDecompressor implements the Decompressor interface for BITPACKING compression.
type bitpackingDecompressor struct{}

// Decompress implements Decompressor.Decompress for BITPACKING compression.
func (d *bitpackingDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return DecompressBitPacking(data, valueSize, count)
}

// NewBitPackingDecompressor creates a new Decompressor for BITPACKING compression.
func NewBitPackingDecompressor() Decompressor {
	return &bitpackingDecompressor{}
}

// pfordeltaDecompressor implements the Decompressor interface for PFOR_DELTA compression.
type pfordeltaDecompressor struct{}

// Decompress implements Decompressor.Decompress for PFOR_DELTA compression.
func (d *pfordeltaDecompressor) Decompress(data []byte, valueSize int, count uint64) ([]byte, error) {
	return DecompressPFORDelta(data, valueSize, count)
}

// NewPFORDeltaDecompressor creates a new Decompressor for PFOR_DELTA compression.
func NewPFORDeltaDecompressor() Decompressor {
	return &pfordeltaDecompressor{}
}

// GetDecompressor returns the appropriate Decompressor for the given compression type.
// This allows for object-oriented usage patterns where a single decompressor instance
// can be reused for multiple decompressions.
//
// Returns nil if the compression type is not supported.
func GetDecompressor(compression CompressionType) Decompressor {
	switch compression {
	case CompressionUncompressed:
		return NewUncompressedDecompressor()
	case CompressionConstant:
		return NewConstantDecompressor()
	case CompressionRLE:
		return NewRLEDecompressor()
	case CompressionDictionary:
		return NewDictionaryDecompressor()
	case CompressionBitPacking:
		return NewBitPackingDecompressor()
	case CompressionPFORDelta:
		return NewPFORDeltaDecompressor()
	default:
		return nil
	}
}
