package duckdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// CONSTANT Mode Tests
// =============================================================================

func TestDecompressBitPackingConstant_Int64(t *testing.T) {
	// Test decompressing a constant int64 value
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 42)
	valueSize := 8
	count := uint64(10)

	result, err := DecompressBitPackingConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 80, len(result)) // 10 values * 8 bytes

	// Verify each value
	for i := uint64(0); i < count; i++ {
		offset := int(i * 8)
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(42), value, "value at index %d should be 42", i)
	}
}

func TestDecompressBitPackingConstant_Int32(t *testing.T) {
	// Test decompressing a constant int32 value
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 12345678)
	valueSize := 4
	count := uint64(5)

	result, err := DecompressBitPackingConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 20, len(result)) // 5 values * 4 bytes

	// Verify each value
	for i := uint64(0); i < count; i++ {
		offset := int(i * 4)
		value := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(12345678), value, "value at index %d should be 12345678", i)
	}
}

func TestDecompressBitPackingConstant_CountZero(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 42)

	result, err := DecompressBitPackingConstant(data, 8, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPackingConstant_DataTooShort(t *testing.T) {
	data := []byte{1, 2, 3} // Only 3 bytes, need 8

	result, err := DecompressBitPackingConstant(data, 8, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeDataTruncated)
}

func TestDecompressBitPackingConstant_InvalidValueSize(t *testing.T) {
	data := make([]byte, 8)

	result, err := DecompressBitPackingConstant(data, 0, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrInvalidValueSize)
}

func TestDecompressBitPackingConstantToInt64(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 999)

	result, err := DecompressBitPackingConstantToInt64(data, 5)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))

	for i, v := range result {
		assert.Equal(t, int64(999), v, "value at index %d should be 999", i)
	}
}

// =============================================================================
// CONSTANT_DELTA Mode Tests
// =============================================================================

func TestDecompressBitPackingConstantDelta_ArithmeticSequence(t *testing.T) {
	// Arithmetic sequence: 10, 13, 16, 19, 22 (first=10, delta=3)
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 10) // first
	binary.LittleEndian.PutUint64(data[8:16], 3) // delta

	result, err := DecompressBitPackingConstantDelta(data, 8, 5)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 5 values * 8 bytes

	expected := []int64{10, 13, 16, 19, 22}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingConstantDelta_NegativeDelta(t *testing.T) {
	// Descending sequence: 100, 95, 90, 85, 80 (first=100, delta=-5)
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 100)
	// Store -5 as two's complement representation
	negativeFive := int64(-5)
	binary.LittleEndian.PutUint64(data[8:16], uint64(negativeFive))

	result, err := DecompressBitPackingConstantDelta(data, 8, 5)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result))

	expected := []int64{100, 95, 90, 85, 80}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingConstantDelta_ZeroDelta(t *testing.T) {
	// All same values: 50, 50, 50, 50 (first=50, delta=0)
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 50) // first
	binary.LittleEndian.PutUint64(data[8:16], 0) // delta

	result, err := DecompressBitPackingConstantDelta(data, 8, 4)

	require.NoError(t, err)
	assert.Equal(t, 32, len(result))

	for i := 0; i < 4; i++ {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(50), value, "value at index %d should be 50", i)
	}
}

func TestDecompressBitPackingConstantDelta_SingleValue(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 42) // first
	binary.LittleEndian.PutUint64(data[8:16], 1) // delta (irrelevant for single value)

	result, err := DecompressBitPackingConstantDelta(data, 8, 1)

	require.NoError(t, err)
	assert.Equal(t, 8, len(result))

	value := int64(binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, int64(42), value)
}

func TestDecompressBitPackingConstantDelta_CountZero(t *testing.T) {
	data := make([]byte, 16)

	result, err := DecompressBitPackingConstantDelta(data, 8, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPackingConstantDelta_DataTooShort(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8} // Only 8 bytes, need 16

	result, err := DecompressBitPackingConstantDelta(data, 8, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeDataTruncated)
}

func TestDecompressBitPackingConstantDeltaToInt64(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 1)  // first
	binary.LittleEndian.PutUint64(data[8:16], 2) // delta

	result, err := DecompressBitPackingConstantDeltaToInt64(data, 5)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))

	expected := []int64{1, 3, 5, 7, 9}
	for i, exp := range expected {
		assert.Equal(t, exp, result[i], "value at index %d should be %d", i, exp)
	}
}

// =============================================================================
// FOR Mode Tests
// =============================================================================

// Helper to build FOR mode data
func buildFORData(reference int64, offsets []uint64, bitWidth uint8) []byte {
	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count
	count := uint64(len(offsets))
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], uint64(reference))
	data[8] = bitWidth
	binary.LittleEndian.PutUint64(data[9:17], count)

	// Bit-pack the offsets
	if bitWidth > 0 && count > 0 {
		packedData := packBitsForTest(offsets, bitWidth)
		data = append(data, packedData...)
	}

	return data
}

// packBitsForTest is a helper to bit-pack values for test data generation.
// This uses the same algorithm as the production packBits function.
func packBitsForTest(values []uint64, bitWidth uint8) []byte {
	if len(values) == 0 || bitWidth == 0 {
		return nil
	}

	totalBits := uint64(bitWidth) * uint64(len(values))
	byteCount := (totalBits + 7) / 8
	result := make([]byte, byteCount)

	bitPos := 0
	for _, v := range values {
		for b := uint8(0); b < bitWidth; b++ {
			if v&(1<<b) != 0 {
				byteIdx := bitPos / 8
				bitIdx := bitPos % 8
				result[byteIdx] |= 1 << bitIdx
			}
			bitPos++
		}
	}

	return result
}

func TestDecompressBitPackingFOR_SimpleOffsets(t *testing.T) {
	// Values: [100, 105, 102, 107, 103] with reference=100
	// Offsets: [0, 5, 2, 7, 3] needing 3 bits each
	offsets := []uint64{0, 5, 2, 7, 3}
	data := buildFORData(100, offsets, 3)

	result, err := DecompressBitPackingFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 5 values * 8 bytes

	expected := []int64{100, 105, 102, 107, 103}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingFOR_NegativeReference(t *testing.T) {
	// Values: [-100, -95, -98] with reference=-100
	// Offsets: [0, 5, 2] needing 3 bits each
	offsets := []uint64{0, 5, 2}
	data := buildFORData(-100, offsets, 3)

	result, err := DecompressBitPackingFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 24, len(result))

	expected := []int64{-100, -95, -98}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingFOR_ZeroBitWidth(t *testing.T) {
	// All values equal to reference
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], 42) // reference
	data[8] = 0                                  // bitWidth = 0
	binary.LittleEndian.PutUint64(data[9:17], 5) // count = 5

	result, err := DecompressBitPackingFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result))

	for i := 0; i < 5; i++ {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(42), value, "all values should be 42")
	}
}

func TestDecompressBitPackingFOR_CountZero(t *testing.T) {
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], 100) // reference
	data[8] = 4                                   // bitWidth
	binary.LittleEndian.PutUint64(data[9:17], 0)  // count = 0

	result, err := DecompressBitPackingFOR(data, 8)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPackingFOR_DataTooShort(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8} // Only 8 bytes, need 17

	result, err := DecompressBitPackingFOR(data, 8)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeDataTruncated)
}

func TestDecompressBitPackingFOR_SingleValue(t *testing.T) {
	offsets := []uint64{7}
	data := buildFORData(1000, offsets, 3)

	result, err := DecompressBitPackingFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 8, len(result))

	value := int64(binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, int64(1007), value)
}

func TestDecompressBitPackingFORToInt64(t *testing.T) {
	offsets := []uint64{1, 2, 3}
	data := buildFORData(100, offsets, 2)

	result, err := DecompressBitPackingFORToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 3, len(result))

	expected := []int64{101, 102, 103}
	for i, exp := range expected {
		assert.Equal(t, exp, result[i], "value at index %d should be %d", i, exp)
	}
}

// =============================================================================
// DELTA_FOR Mode Tests
// =============================================================================

// Helper to build DELTA_FOR data (same format as PFOR_DELTA)
func buildDeltaFORData(reference int64, deltas []uint64, bitWidth uint8) []byte {
	// The count includes the reference itself
	count := uint64(len(deltas)) + 1
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], uint64(reference))
	data[8] = bitWidth
	binary.LittleEndian.PutUint64(data[9:17], count)

	// Bit-pack the deltas
	if bitWidth > 0 && len(deltas) > 0 {
		packedData := packBitsForTest(deltas, bitWidth)
		data = append(data, packedData...)
	}

	return data
}

func TestDecompressBitPackingDeltaFOR_SortedSequence(t *testing.T) {
	// Sorted sequence: [100, 101, 102, 103, 104]
	// reference=100, deltas=[1, 1, 1, 1]
	deltas := []uint64{1, 1, 1, 1}
	data := buildDeltaFORData(100, deltas, 1)

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 5 values * 8 bytes

	expected := []int64{100, 101, 102, 103, 104}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingDeltaFOR_VariableDeltas(t *testing.T) {
	// Sequence: [10, 12, 17, 18, 25]
	// reference=10, deltas=[2, 5, 1, 7]
	deltas := []uint64{2, 5, 1, 7}
	data := buildDeltaFORData(10, deltas, 3)

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result))

	expected := []int64{10, 12, 17, 18, 25}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressBitPackingDeltaFOR_ZeroBitWidth(t *testing.T) {
	// All values equal to reference (all deltas are 0)
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], 42) // reference
	data[8] = 0                                  // bitWidth = 0
	binary.LittleEndian.PutUint64(data[9:17], 5) // count = 5

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result))

	for i := 0; i < 5; i++ {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(42), value, "all values should be 42")
	}
}

func TestDecompressBitPackingDeltaFOR_SingleValue(t *testing.T) {
	// Just the reference value
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], 999) // reference
	data[8] = 1                                   // bitWidth (unused for single value)
	binary.LittleEndian.PutUint64(data[9:17], 1)  // count = 1

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	require.NoError(t, err)
	assert.Equal(t, 8, len(result))

	value := int64(binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, int64(999), value)
}

func TestDecompressBitPackingDeltaFOR_CountZero(t *testing.T) {
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], 100) // reference
	data[8] = 4                                   // bitWidth
	binary.LittleEndian.PutUint64(data[9:17], 0)  // count = 0

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPackingDeltaFOR_DataTooShort(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8} // Only 8 bytes, need 17

	result, err := DecompressBitPackingDeltaFOR(data, 8)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeDataTruncated)
}

func TestDecompressBitPackingDeltaFORToInt64(t *testing.T) {
	deltas := []uint64{3, 3, 3}
	data := buildDeltaFORData(0, deltas, 2)

	result, err := DecompressBitPackingDeltaFORToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 4, len(result))

	expected := []int64{0, 3, 6, 9}
	for i, exp := range expected {
		assert.Equal(t, exp, result[i], "value at index %d should be %d", i, exp)
	}
}

// =============================================================================
// DecompressBitPackingWithMode Tests
// =============================================================================

func TestDecompressBitPackingWithMode_Constant(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 77)

	result, err := DecompressBitPackingWithMode(data, BitpackingConstant, 8, 3)

	require.NoError(t, err)
	assert.Equal(t, 24, len(result))

	for i := 0; i < 3; i++ {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(77), value)
	}
}

func TestDecompressBitPackingWithMode_ConstantDelta(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 5)  // first
	binary.LittleEndian.PutUint64(data[8:16], 5) // delta

	result, err := DecompressBitPackingWithMode(data, BitpackingConstantDelta, 8, 4)

	require.NoError(t, err)
	assert.Equal(t, 32, len(result))

	expected := []int64{5, 10, 15, 20}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value)
	}
}

func TestDecompressBitPackingWithMode_FOR(t *testing.T) {
	offsets := []uint64{0, 1, 2}
	data := buildFORData(50, offsets, 2)

	result, err := DecompressBitPackingWithMode(data, BitpackingFOR, 8, 3)

	require.NoError(t, err)
	assert.Equal(t, 24, len(result))

	expected := []int64{50, 51, 52}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value)
	}
}

func TestDecompressBitPackingWithMode_DeltaFOR(t *testing.T) {
	deltas := []uint64{2, 2}
	data := buildDeltaFORData(100, deltas, 2)

	result, err := DecompressBitPackingWithMode(data, BitpackingDeltaFOR, 8, 3)

	require.NoError(t, err)
	assert.Equal(t, 24, len(result))

	expected := []int64{100, 102, 104}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value)
	}
}

func TestDecompressBitPackingWithMode_Auto_Error(t *testing.T) {
	data := make([]byte, 8)

	result, err := DecompressBitPackingWithMode(data, BitpackingAuto, 8, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeInvalid)
}

func TestDecompressBitPackingWithMode_InvalidMode(t *testing.T) {
	data := make([]byte, 8)

	result, err := DecompressBitPackingWithMode(data, BitpackingMode(99), 8, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitpackingModeInvalid)
}

func TestDecompressBitPackingWithModeToInt64(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 123)

	result, err := DecompressBitPackingWithModeToInt64(data, BitpackingConstant, 3)

	require.NoError(t, err)
	assert.Equal(t, 3, len(result))

	for _, v := range result {
		assert.Equal(t, int64(123), v)
	}
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestInt64ToBytes(t *testing.T) {
	values := []int64{100, 200, 300}

	result := int64ToBytes(values, 8)

	assert.Equal(t, 24, len(result))
	assert.Equal(t, int64(100), int64(binary.LittleEndian.Uint64(result[0:8])))
	assert.Equal(t, int64(200), int64(binary.LittleEndian.Uint64(result[8:16])))
	assert.Equal(t, int64(300), int64(binary.LittleEndian.Uint64(result[16:24])))
}

func TestInt64ToBytes_Int32(t *testing.T) {
	values := []int64{1, 2, 3}

	result := int64ToBytes(values, 4)

	assert.Equal(t, 12, len(result))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(result[0:4]))
	assert.Equal(t, uint32(2), binary.LittleEndian.Uint32(result[4:8]))
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(result[8:12]))
}

func TestInt64ToBytes_Int16(t *testing.T) {
	values := []int64{10, 20}

	result := int64ToBytes(values, 2)

	assert.Equal(t, 4, len(result))
	assert.Equal(t, uint16(10), binary.LittleEndian.Uint16(result[0:2]))
	assert.Equal(t, uint16(20), binary.LittleEndian.Uint16(result[2:4]))
}

func TestInt64ToBytes_Int8(t *testing.T) {
	values := []int64{1, 2, 3, 4}

	result := int64ToBytes(values, 1)

	assert.Equal(t, 4, len(result))
	assert.Equal(t, byte(1), result[0])
	assert.Equal(t, byte(2), result[1])
	assert.Equal(t, byte(3), result[2])
	assert.Equal(t, byte(4), result[3])
}

func TestInt64ToBytes_InvalidSizeDefaultsTo8(t *testing.T) {
	values := []int64{42}

	result := int64ToBytes(values, 3) // Invalid size, should default to 8

	assert.Equal(t, 8, len(result))
	assert.Equal(t, int64(42), int64(binary.LittleEndian.Uint64(result[0:8])))
}

func TestInt64ToBytes_Empty(t *testing.T) {
	result := int64ToBytes([]int64{}, 8)

	assert.Empty(t, result)
}

func TestBytesToInt64(t *testing.T) {
	data := make([]byte, 24)
	binary.LittleEndian.PutUint64(data[0:8], 100)
	binary.LittleEndian.PutUint64(data[8:16], 200)
	binary.LittleEndian.PutUint64(data[16:24], 300)

	result := bytesToInt64(data)

	assert.Equal(t, 3, len(result))
	assert.Equal(t, int64(100), result[0])
	assert.Equal(t, int64(200), result[1])
	assert.Equal(t, int64(300), result[2])
}

func TestBytesToInt64_Empty(t *testing.T) {
	result := bytesToInt64([]byte{})

	assert.Empty(t, result)
}

func TestBytesToInt64_PartialValue(t *testing.T) {
	// Only 12 bytes - should produce 1 int64 value
	data := make([]byte, 12)
	binary.LittleEndian.PutUint64(data[0:8], 42)

	result := bytesToInt64(data)

	assert.Equal(t, 1, len(result))
	assert.Equal(t, int64(42), result[0])
}

// =============================================================================
// Decompressor Interface Tests
// =============================================================================

func TestNewBitPackingModeDecompressor_Constant(t *testing.T) {
	d := NewBitPackingModeDecompressor(BitpackingConstant)

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 555)

	result, err := d.Decompress(data, 8, 2)

	require.NoError(t, err)
	assert.Equal(t, 16, len(result))

	for i := 0; i < 2; i++ {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, int64(555), value)
	}
}

func TestNewBitPackingModeDecompressor_FOR(t *testing.T) {
	d := NewBitPackingModeDecompressor(BitpackingFOR)

	offsets := []uint64{3, 3, 3}
	data := buildFORData(10, offsets, 2)

	result, err := d.Decompress(data, 8, 3)

	require.NoError(t, err)
	assert.Equal(t, 24, len(result))

	expected := []int64{13, 13, 13}
	for i, exp := range expected {
		offset := i * 8
		value := int64(binary.LittleEndian.Uint64(result[offset : offset+8]))
		assert.Equal(t, exp, value)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkDecompressBitPackingConstant_Small(b *testing.B) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingConstant(data, 8, 100)
	}
}

func BenchmarkDecompressBitPackingConstant_Large(b *testing.B) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 42)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingConstant(data, 8, 122880)
	}
}

func BenchmarkDecompressBitPackingConstantDelta(b *testing.B) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], 0)
	binary.LittleEndian.PutUint64(data[8:16], 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingConstantDelta(data, 8, 10000)
	}
}

func BenchmarkDecompressBitPackingFOR(b *testing.B) {
	offsets := make([]uint64, 10000)
	for i := range offsets {
		offsets[i] = uint64(i % 256)
	}
	data := buildFORData(1000, offsets, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingFOR(data, 8)
	}
}

func BenchmarkDecompressBitPackingDeltaFOR(b *testing.B) {
	deltas := make([]uint64, 9999)
	for i := range deltas {
		deltas[i] = 1
	}
	data := buildDeltaFORData(0, deltas, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingDeltaFOR(data, 8)
	}
}
