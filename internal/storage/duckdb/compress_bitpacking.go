package duckdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Bitpacking mode error messages.
var (
	// ErrBitpackingModeInvalid is returned when an invalid bitpacking mode is specified.
	ErrBitpackingModeInvalid = errors.New("invalid bitpacking mode")

	// ErrBitpackingModeDataTruncated is returned when bitpacking mode data is too short.
	ErrBitpackingModeDataTruncated = errors.New("bitpacking mode data truncated")

	// ErrBitpackingModeConstantDeltaOverflow is returned when constant delta computation overflows.
	ErrBitpackingModeConstantDeltaOverflow = errors.New("bitpacking constant delta overflow")
)

// DecompressBitPackingWithMode decompresses bit-packed data with a specific mode.
// BITPACKING in DuckDB has several submodes that optimize for different data patterns.
//
// Format varies by mode:
//   - CONSTANT: [value bytes] - single value repeated
//   - CONSTANT_DELTA: [first value (8 bytes)][delta value (8 bytes)] - arithmetic sequence
//   - FOR: [reference (8 bytes)][uint8 bitWidth][uint64 count][bit-packed offsets] - frame of reference
//   - DELTA_FOR: Same as PFOR_DELTA - cumulative deltas from reference
//   - AUTO: Not valid for decompression, returns error
//
// Parameters:
//   - data: The compressed data bytes
//   - mode: The bitpacking mode used for compression
//   - valueSize: Size in bytes of each target value
//   - count: Number of values to decompress
//
// Returns:
//   - []byte: Decompressed data as byte slice
//   - error: Mode-specific errors if decompression fails
func DecompressBitPackingWithMode(
	data []byte,
	mode BitpackingMode,
	valueSize int,
	count uint64,
) ([]byte, error) {
	switch mode {
	case BitpackingAuto:
		// AUTO mode should have been resolved to a specific mode before storage
		return nil, fmt.Errorf(
			"%w: AUTO mode is not valid for decompression",
			ErrBitpackingModeInvalid,
		)

	case BitpackingConstant:
		return DecompressBitPackingConstant(data, valueSize, count)

	case BitpackingConstantDelta:
		return DecompressBitPackingConstantDelta(data, valueSize, count)

	case BitpackingFOR:
		return DecompressBitPackingFOR(data, valueSize)

	case BitpackingDeltaFOR:
		return DecompressBitPackingDeltaFOR(data, valueSize)

	default:
		return nil, fmt.Errorf("%w: unknown mode %d", ErrBitpackingModeInvalid, mode)
	}
}

// DecompressBitPackingConstant decompresses bitpacking CONSTANT mode data.
// In CONSTANT mode, all values are the same. Only a single value is stored.
//
// Format: [value bytes]
//
// Parameters:
//   - data: The compressed data containing a single value
//   - valueSize: Size in bytes of the value (1, 2, 4, or 8)
//   - count: Number of values to expand to
//
// Returns:
//   - []byte: Decompressed data with count copies of the value
//   - error: ErrBitpackingModeDataTruncated if data is too short
//
// Example:
//
//	// All 100 values are 42
//	data := []byte{42, 0, 0, 0, 0, 0, 0, 0}  // int64 value 42
//	result, err := DecompressBitPackingConstant(data, 8, 100)
//	// result contains 100 copies of int64(42)
func DecompressBitPackingConstant(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Validate inputs
	if valueSize <= 0 {
		return nil, ErrInvalidValueSize
	}

	if len(data) < valueSize {
		return nil, fmt.Errorf("%w: need %d bytes for value, got %d",
			ErrBitpackingModeDataTruncated, valueSize, len(data))
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

// DecompressBitPackingConstantDelta decompresses bitpacking CONSTANT_DELTA mode data.
// In CONSTANT_DELTA mode, values form an arithmetic sequence with a constant delta.
// value[i] = first + i * delta
//
// Format: [first value (8 bytes)][delta value (8 bytes)]
//
// Parameters:
//   - data: The compressed data containing first value and delta
//   - valueSize: Size in bytes of each target value (output conversion)
//   - count: Number of values to generate
//
// Returns:
//   - []byte: Decompressed data containing the arithmetic sequence
//   - error: ErrBitpackingModeDataTruncated if data is too short
//
// Example:
//
//	// Arithmetic sequence: 10, 13, 16, 19, 22 (first=10, delta=3)
//	data := make([]byte, 16)
//	binary.LittleEndian.PutUint64(data[0:8], 10)  // first
//	binary.LittleEndian.PutUint64(data[8:16], 3)  // delta
//	result, err := DecompressBitPackingConstantDelta(data, 8, 5)
func DecompressBitPackingConstantDelta(data []byte, valueSize int, count uint64) ([]byte, error) {
	// Need at least 16 bytes: 8 for first value + 8 for delta
	if len(data) < 16 {
		return nil, fmt.Errorf("%w: need 16 bytes for header, got %d",
			ErrBitpackingModeDataTruncated, len(data))
	}

	// Handle edge case of zero count
	if count == 0 {
		return []byte{}, nil
	}

	// Read first value and delta as signed int64
	first := int64(binary.LittleEndian.Uint64(data[0:8]))
	delta := int64(binary.LittleEndian.Uint64(data[8:16]))

	// Generate arithmetic sequence
	values := make([]int64, count)
	for i := uint64(0); i < count; i++ {
		values[i] = first + int64(i)*delta
	}

	// Convert to bytes with target size
	return int64ToBytes(values, valueSize), nil
}

// DecompressBitPackingFOR decompresses bitpacking FOR (Frame of Reference) mode data.
// In FOR mode, values are stored as offsets from a reference (minimum) value.
// value[i] = reference + offset[i]
//
// This is different from DELTA_FOR/PFOR_DELTA where deltas are cumulative.
// In FOR mode, each offset is independent from the reference.
//
// Format: [reference (8 bytes)][uint8 bitWidth][uint64 count][bit-packed offsets]
//
// Parameters:
//   - data: The compressed data
//   - valueSize: Size in bytes of each target value (output conversion)
//
// Returns:
//   - []byte: Decompressed data containing values
//   - error: ErrBitpackingModeDataTruncated if data is too short
//
// Example:
//
//	// Values: [100, 105, 102, 107, 103] with reference=100
//	// Offsets: [0, 5, 2, 7, 3] bit-packed with bitWidth=3
//	result, err := DecompressBitPackingFOR(data, 8)
func DecompressBitPackingFOR(data []byte, valueSize int) ([]byte, error) {
	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count = 17 bytes minimum
	if len(data) < 17 {
		return nil, fmt.Errorf("%w: need at least 17 bytes for header, got %d",
			ErrBitpackingModeDataTruncated, len(data))
	}

	// Read reference (minimum value) - signed int64
	reference := int64(binary.LittleEndian.Uint64(data[0:8]))

	// Read bit width for offsets
	bitWidth := data[8]

	// Read count of values
	count := binary.LittleEndian.Uint64(data[9:17])

	// Handle edge case: count=0
	if count == 0 {
		return []byte{}, nil
	}

	// Handle special case: bitWidth=0 means all offsets are 0
	// So all values equal the reference
	if bitWidth == 0 {
		values := make([]int64, count)
		for i := uint64(0); i < count; i++ {
			values[i] = reference
		}
		return int64ToBytes(values, valueSize), nil
	}

	// Read bit-packed offsets using the raw data after header
	packedData := data[17:]
	offsets, err := DecompressBitPackingWithParams(packedData, bitWidth, count)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress FOR offsets: %w", err)
	}

	// Apply reference to each offset: value[i] = reference + offset[i]
	values := make([]int64, count)
	for i, offset := range offsets {
		values[i] = reference + int64(offset)
	}

	return int64ToBytes(values, valueSize), nil
}

// DecompressBitPackingDeltaFOR decompresses bitpacking DELTA_FOR mode data.
// DELTA_FOR is equivalent to PFOR_DELTA: delta encoding with frame of reference.
// Values are cumulative deltas from the reference.
// value[0] = reference
// value[i] = value[i-1] + delta[i-1]
//
// Format: [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
//
// Parameters:
//   - data: The compressed data
//   - valueSize: Size in bytes of each target value (output conversion)
//
// Returns:
//   - []byte: Decompressed data containing values
//   - error: ErrBitpackingModeDataTruncated or decompression errors
//
// Example:
//
//	// Sorted sequence [100, 101, 102, 103, 104] stored as:
//	// reference=100, deltas=[1, 1, 1, 1]
//	result, err := DecompressBitPackingDeltaFOR(data, 8)
func DecompressBitPackingDeltaFOR(data []byte, valueSize int) ([]byte, error) {
	// This is the same as PFOR_DELTA decompression
	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count = 17 bytes minimum
	if len(data) < 17 {
		return nil, fmt.Errorf("%w: need at least 17 bytes for header, got %d",
			ErrBitpackingModeDataTruncated, len(data))
	}

	// Read reference (starting value) - signed int64
	reference := int64(binary.LittleEndian.Uint64(data[0:8]))

	// Read bit width for deltas
	bitWidth := data[8]

	// Read count of values
	count := binary.LittleEndian.Uint64(data[9:17])

	// Handle edge case: count=0
	if count == 0 {
		return []byte{}, nil
	}

	// The first value is the reference itself, so we need count-1 deltas
	deltaCount := count - 1

	// Handle case with only one value (no deltas needed)
	if deltaCount == 0 {
		values := []int64{reference}
		return int64ToBytes(values, valueSize), nil
	}

	// Handle special case: bitWidth=0 means all deltas are 0
	// So all values equal the reference
	if bitWidth == 0 {
		values := make([]int64, count)
		for i := uint64(0); i < count; i++ {
			values[i] = reference
		}
		return int64ToBytes(values, valueSize), nil
	}

	// Read bit-packed deltas using the raw data after header
	packedData := data[17:]
	deltas, err := DecompressBitPackingWithParams(packedData, bitWidth, deltaCount)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress DELTA_FOR deltas: %w", err)
	}

	// Apply deltas cumulatively to reconstruct values
	values := make([]int64, count)
	current := reference

	// First value is the reference
	values[0] = current

	// Apply each delta to get subsequent values
	for i, delta := range deltas {
		current += int64(delta) // Delta from previous value (cumulative)
		values[i+1] = current
	}

	return int64ToBytes(values, valueSize), nil
}

// DecompressBitPackingWithModeToInt64 decompresses bit-packed data with mode to []int64.
// This is a convenience function that returns values as a slice of int64
// instead of raw bytes.
//
// Parameters:
//   - data: The compressed data bytes
//   - mode: The bitpacking mode used for compression
//   - count: Number of values to decompress
//
// Returns:
//   - []int64: Decompressed values
//   - error: Mode-specific errors if decompression fails
func DecompressBitPackingWithModeToInt64(
	data []byte,
	mode BitpackingMode,
	count uint64,
) ([]int64, error) {
	result, err := DecompressBitPackingWithMode(data, mode, 8, count)
	if err != nil {
		return nil, err
	}

	return bytesToInt64(result), nil
}

// DecompressBitPackingConstantToInt64 decompresses CONSTANT mode to []int64.
func DecompressBitPackingConstantToInt64(data []byte, count uint64) ([]int64, error) {
	result, err := DecompressBitPackingConstant(data, 8, count)
	if err != nil {
		return nil, err
	}

	return bytesToInt64(result), nil
}

// DecompressBitPackingConstantDeltaToInt64 decompresses CONSTANT_DELTA mode to []int64.
func DecompressBitPackingConstantDeltaToInt64(data []byte, count uint64) ([]int64, error) {
	result, err := DecompressBitPackingConstantDelta(data, 8, count)
	if err != nil {
		return nil, err
	}

	return bytesToInt64(result), nil
}

// DecompressBitPackingFORToInt64 decompresses FOR mode to []int64.
func DecompressBitPackingFORToInt64(data []byte) ([]int64, error) {
	result, err := DecompressBitPackingFOR(data, 8)
	if err != nil {
		return nil, err
	}

	return bytesToInt64(result), nil
}

// DecompressBitPackingDeltaFORToInt64 decompresses DELTA_FOR mode to []int64.
func DecompressBitPackingDeltaFORToInt64(data []byte) ([]int64, error) {
	result, err := DecompressBitPackingDeltaFOR(data, 8)
	if err != nil {
		return nil, err
	}

	return bytesToInt64(result), nil
}

// int64ToBytes converts a slice of int64 values to a byte slice with target size.
// The target size determines how values are truncated/stored.
//
// Parameters:
//   - values: The int64 values to convert
//   - targetSize: Target byte size per value (1, 2, 4, or 8)
//
// Returns:
//   - []byte: Byte slice with values encoded at targetSize bytes each
func int64ToBytes(values []int64, targetSize int) []byte {
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
			binary.LittleEndian.PutUint64(result[offset:], uint64(v))
		}
	}

	return result
}

// bytesToInt64 converts a byte slice to a slice of int64 values.
// Assumes the bytes are little-endian encoded 8-byte values.
//
// Parameters:
//   - data: The byte slice to convert
//
// Returns:
//   - []int64: The decoded int64 values
func bytesToInt64(data []byte) []int64 {
	if len(data) == 0 {
		return []int64{}
	}

	count := len(data) / 8
	result := make([]int64, count)

	for i := 0; i < count; i++ {
		offset := i * 8
		result[i] = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	}

	return result
}

// bitpackingModeDecompressor implements the Decompressor interface for bitpacking with mode.
type bitpackingModeDecompressor struct {
	mode BitpackingMode
}

// Decompress implements Decompressor.Decompress for bitpacking with specific mode.
func (d *bitpackingModeDecompressor) Decompress(
	data []byte,
	valueSize int,
	count uint64,
) ([]byte, error) {
	return DecompressBitPackingWithMode(data, d.mode, valueSize, count)
}

// NewBitPackingModeDecompressor creates a new Decompressor for bitpacking with a specific mode.
func NewBitPackingModeDecompressor(mode BitpackingMode) Decompressor {
	return &bitpackingModeDecompressor{mode: mode}
}
