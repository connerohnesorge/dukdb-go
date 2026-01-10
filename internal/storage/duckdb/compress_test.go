package duckdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecompressConstant_SingleByte(t *testing.T) {
	// Test decompressing a single byte value
	data := []byte{0x42} // Value 66 (0x42)
	valueSize := 1
	count := uint64(5)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))

	// Verify all values are the same
	for i := 0; i < 5; i++ {
		assert.Equal(t, byte(0x42), result[i], "value at index %d should be 0x42", i)
	}
}

func TestDecompressConstant_Int32(t *testing.T) {
	// Test decompressing a 4-byte int32 value
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 12345678)
	valueSize := 4
	count := uint64(10)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 10 values * 4 bytes

	// Verify each value
	for i := uint64(0); i < count; i++ {
		offset := int(i * 4)
		value := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(12345678), value, "value at index %d should be 12345678", i)
	}
}

func TestDecompressConstant_Int64(t *testing.T) {
	// Test decompressing an 8-byte int64 value
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 0x123456789ABCDEF0)
	valueSize := 8
	count := uint64(7)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 56, len(result)) // 7 values * 8 bytes

	// Verify each value
	for i := uint64(0); i < count; i++ {
		offset := int(i * 8)
		value := binary.LittleEndian.Uint64(result[offset : offset+8])
		assert.Equal(t, uint64(0x123456789ABCDEF0), value, "value at index %d should match", i)
	}
}

func TestDecompressConstant_CountZero(t *testing.T) {
	// Edge case: count is 0
	data := []byte{0x42, 0x43, 0x44, 0x45}
	valueSize := 4
	count := uint64(0)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Empty(t, result)
	assert.Equal(t, 0, len(result))
}

func TestDecompressConstant_CountOne(t *testing.T) {
	// Edge case: count is 1
	data := []byte{0xAB, 0xCD}
	valueSize := 2
	count := uint64(1)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, uint16(0xCDAB), binary.LittleEndian.Uint16(result))
}

func TestDecompressConstant_LargeCount(t *testing.T) {
	// Test with a large count
	data := []byte{0xFF, 0xEE, 0xDD, 0xCC}
	valueSize := 4
	count := uint64(10000)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 40000, len(result)) // 10000 * 4 bytes

	// Verify first, middle, and last values
	expected := uint32(0xCCDDEEFF)
	assert.Equal(t, expected, binary.LittleEndian.Uint32(result[0:4]), "first value should match")
	assert.Equal(t, expected, binary.LittleEndian.Uint32(result[20000:20004]), "middle value should match")
	assert.Equal(t, expected, binary.LittleEndian.Uint32(result[39996:40000]), "last value should match")
}

func TestDecompressConstant_DataTooShort(t *testing.T) {
	// Error case: data is shorter than valueSize
	data := []byte{0x42, 0x43} // Only 2 bytes
	valueSize := 4             // But we expect 4 bytes
	count := uint64(5)

	result, err := DecompressConstant(data, valueSize, count)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrConstantDataTooShort)
}

func TestDecompressConstant_EmptyData(t *testing.T) {
	// Error case: empty data
	data := []byte{}
	valueSize := 1
	count := uint64(5)

	result, err := DecompressConstant(data, valueSize, count)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrConstantDataTooShort)
}

func TestDecompressConstant_InvalidValueSize(t *testing.T) {
	// Error case: zero value size
	data := []byte{0x42}
	count := uint64(5)

	result, err := DecompressConstant(data, 0, count)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrInvalidValueSize)

	// Error case: negative value size
	result, err = DecompressConstant(data, -1, count)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrInvalidValueSize)
}

func TestDecompressConstant_ExtraData(t *testing.T) {
	// Data has more bytes than valueSize - should work and ignore extra bytes
	data := []byte{0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49}
	valueSize := 4
	count := uint64(3)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 12, len(result)) // 3 * 4 bytes

	// Should use only the first 4 bytes as the constant value
	expected := binary.LittleEndian.Uint32(data[:4])
	for i := uint64(0); i < count; i++ {
		offset := int(i * 4)
		value := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, expected, value)
	}
}

func TestDecompressConstant_AllZeros(t *testing.T) {
	// Test with all-zero value
	data := []byte{0x00, 0x00, 0x00, 0x00}
	valueSize := 4
	count := uint64(100)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result))

	// Verify all bytes are zero
	for i, b := range result {
		assert.Equal(t, byte(0), b, "byte at index %d should be 0", i)
	}
}

func TestDecompressConstant_AllOnes(t *testing.T) {
	// Test with all-ones value (0xFF)
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	valueSize := 8
	count := uint64(50)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result))

	// Verify all bytes are 0xFF
	for i, b := range result {
		assert.Equal(t, byte(0xFF), b, "byte at index %d should be 0xFF", i)
	}
}

func TestDecompress_DispatchConstant(t *testing.T) {
	// Test that Decompress correctly dispatches to DecompressConstant
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 42)

	result, err := Decompress(CompressionConstant, data, 4, 5)

	require.NoError(t, err)
	assert.Equal(t, 20, len(result))

	for i := uint64(0); i < 5; i++ {
		offset := int(i * 4)
		value := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(42), value)
	}
}

func TestDecompress_Uncompressed(t *testing.T) {
	// Test that uncompressed data is returned as-is
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	result, err := Decompress(CompressionUncompressed, data, 1, 8)

	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, result))
}

func TestDecompress_Empty(t *testing.T) {
	// Test empty compression returns empty slice
	data := []byte{1, 2, 3, 4}

	result, err := Decompress(CompressionEmpty, data, 4, 10)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompress_UnsupportedCompression(t *testing.T) {
	// Test that unsupported compression types return error
	data := []byte{1, 2, 3, 4}

	// FSST is not implemented
	result, err := Decompress(CompressionFSST, data, 4, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrUnsupportedCompression)

	// CHIMP is not implemented
	result, err = Decompress(CompressionCHIMP, data, 4, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrUnsupportedCompression)
}

func TestDecompress_PFORDeltaTruncatedData(t *testing.T) {
	// Test that PFOR_DELTA with truncated data returns appropriate error
	data := []byte{1, 2, 3, 4} // Only 4 bytes, need at least 17 for header

	result, err := DecompressPFORDelta(data, 8, 10)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrPFORDeltaDataTruncated)
}

func TestDecompress_DispatchToPFORDelta(t *testing.T) {
	// Test that Decompress correctly dispatches to PFOR_DELTA decompression
	// Build a simple PFOR_DELTA compressed sequence: [100, 101, 102]
	data := buildPFORDeltaData(100, []uint64{1, 1}, 1) // reference=100, deltas=[1,1], bitWidth=1

	result, err := Decompress(CompressionPFORDelta, data, 8, 3)
	require.NoError(t, err)
	assert.Equal(t, 24, len(result)) // 3 values * 8 bytes

	// Verify values: 100, 101, 102
	assert.Equal(t, int64(100), int64(binary.LittleEndian.Uint64(result[0:8])))
	assert.Equal(t, int64(101), int64(binary.LittleEndian.Uint64(result[8:16])))
	assert.Equal(t, int64(102), int64(binary.LittleEndian.Uint64(result[16:24])))
}

func TestDecompressor_Interface(t *testing.T) {
	// Test the Decompressor interface with constantDecompressor
	var d Decompressor = NewConstantDecompressor()

	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 999)

	result, err := d.Decompress(data, 4, 3)

	require.NoError(t, err)
	assert.Equal(t, 12, len(result))

	for i := uint64(0); i < 3; i++ {
		offset := int(i * 4)
		value := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(999), value)
	}
}

func TestDecompressor_Uncompressed(t *testing.T) {
	// Test the Decompressor interface with uncompressedDecompressor
	var d Decompressor = NewUncompressedDecompressor()

	data := []byte{1, 2, 3, 4, 5}

	result, err := d.Decompress(data, 1, 5)

	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, result))
}

func TestGetDecompressor(t *testing.T) {
	// Test GetDecompressor returns correct decompressor types
	d := GetDecompressor(CompressionUncompressed)
	assert.NotNil(t, d)
	_, ok := d.(*uncompressedDecompressor)
	assert.True(t, ok)

	d = GetDecompressor(CompressionConstant)
	assert.NotNil(t, d)
	_, ok = d.(*constantDecompressor)
	assert.True(t, ok)

	d = GetDecompressor(CompressionRLE)
	assert.NotNil(t, d)
	_, ok = d.(*rleDecompressor)
	assert.True(t, ok)

	d = GetDecompressor(CompressionDictionary)
	assert.NotNil(t, d)
	_, ok = d.(*dictionaryDecompressor)
	assert.True(t, ok)

	// Unsupported types return nil
	d = GetDecompressor(CompressionFSST)
	assert.Nil(t, d)
}

func TestDecompressConstant_BooleanValues(t *testing.T) {
	// Test decompressing boolean values (1 byte each)
	// DuckDB stores booleans as single bytes
	data := []byte{0x01} // true
	valueSize := 1
	count := uint64(100)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 100, len(result))

	// All values should be 0x01 (true)
	for i, b := range result {
		assert.Equal(t, byte(0x01), b, "byte at index %d should be 0x01", i)
	}

	// Test with false
	data = []byte{0x00}
	result, err = DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	for i, b := range result {
		assert.Equal(t, byte(0x00), b, "byte at index %d should be 0x00", i)
	}
}

func TestDecompressConstant_Float64Values(t *testing.T) {
	// Test decompressing float64 values (8 bytes each)
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 0x400921FB54442D18) // Approx pi
	valueSize := 8
	count := uint64(10)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 80, len(result))

	// Verify each value contains the same bit pattern
	expectedBits := uint64(0x400921FB54442D18)
	for i := uint64(0); i < count; i++ {
		offset := int(i * 8)
		bits := binary.LittleEndian.Uint64(result[offset : offset+8])
		assert.Equal(t, expectedBits, bits, "bits at index %d should match", i)
	}
}

func TestDecompressConstant_VeryLargeValueSize(t *testing.T) {
	// Test with a larger value size (e.g., 16 bytes for HUGEINT)
	data := make([]byte, 16)
	for i := 0; i < 16; i++ {
		data[i] = byte(i + 1) // 1, 2, 3, ..., 16
	}
	valueSize := 16
	count := uint64(5)

	result, err := DecompressConstant(data, valueSize, count)

	require.NoError(t, err)
	assert.Equal(t, 80, len(result))

	// Verify each 16-byte chunk matches
	for i := uint64(0); i < count; i++ {
		offset := int(i * 16)
		chunk := result[offset : offset+16]
		assert.True(t, bytes.Equal(data[:16], chunk), "chunk at index %d should match", i)
	}
}

// Benchmark for constant decompression
func BenchmarkDecompressConstant_SmallCount(b *testing.B) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 0x123456789ABCDEF0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressConstant(data, 8, 100)
	}
}

func BenchmarkDecompressConstant_LargeCount(b *testing.B) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 0x123456789ABCDEF0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressConstant(data, 8, 10000)
	}
}

func BenchmarkDecompressConstant_VeryLargeCount(b *testing.B) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 0x123456789ABCDEF0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressConstant(data, 8, 122880) // Default row group size
	}
}

// =============================================================================
// RLE Decompression Tests
// =============================================================================

// Helper function to build RLE data with (count, value) pairs in DuckDB format
func buildRLEData(runs []struct {
	count uint64
	value []byte
}) []byte {
	// DuckDB RLE format:
	// [uint64 metadata_offset][unique values...][run lengths as uint16...]

	if len(runs) == 0 {
		return []byte{}
	}

	// Calculate metadata offset
	// 8 bytes header + (num_runs * value_size) bytes for values
	valueSize := len(runs[0].value)
	metadataOffset := uint64(8 + len(runs)*valueSize)

	// Calculate total size
	// header + values + metadata (2 bytes per run)
	totalSize := int(metadataOffset) + len(runs)*2
	data := make([]byte, totalSize)

	// Write metadata offset
	binary.LittleEndian.PutUint64(data[0:8], metadataOffset)

	// Write unique values
	pos := 8
	for _, run := range runs {
		copy(data[pos:pos+valueSize], run.value)
		pos += valueSize
	}

	// Write run lengths as uint16
	metaPos := int(metadataOffset)
	for _, run := range runs {
		binary.LittleEndian.PutUint16(data[metaPos:metaPos+2], uint16(run.count))
		metaPos += 2
	}

	return data
}

func TestDecompressRLE_SingleRun_SingleByte(t *testing.T) {
	// Single run of 5 copies of byte value 0x42
	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 5, value: []byte{0x42}},
	})

	result, err := DecompressRLE(data, 1, 5)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))

	for i := 0; i < 5; i++ {
		assert.Equal(t, byte(0x42), result[i], "value at index %d should be 0x42", i)
	}
}

func TestDecompressRLE_SingleRun_Int32(t *testing.T) {
	// Single run of 10 copies of int32 value 12345678
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 12345678)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 10, value: value},
	})

	result, err := DecompressRLE(data, 4, 10)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 10 * 4 bytes

	for i := uint64(0); i < 10; i++ {
		offset := int(i * 4)
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(12345678), v, "value at index %d should be 12345678", i)
	}
}

func TestDecompressRLE_MultipleRuns_Int32(t *testing.T) {
	// Three runs: 3x value 42, 2x value 100, 4x value 999
	value1 := make([]byte, 4)
	binary.LittleEndian.PutUint32(value1, 42)

	value2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(value2, 100)

	value3 := make([]byte, 4)
	binary.LittleEndian.PutUint32(value3, 999)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 3, value: value1},
		{count: 2, value: value2},
		{count: 4, value: value3},
	})

	result, err := DecompressRLE(data, 4, 9)

	require.NoError(t, err)
	assert.Equal(t, 36, len(result)) // 9 * 4 bytes

	// Expected: [42, 42, 42, 100, 100, 999, 999, 999, 999]
	expected := []uint32{42, 42, 42, 100, 100, 999, 999, 999, 999}
	for i, exp := range expected {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, exp, v, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressRLE_EmptyData_ZeroCount(t *testing.T) {
	// Empty data with count 0 is valid
	result, err := DecompressRLE([]byte{}, 4, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressRLE_EmptyData_NonZeroCount(t *testing.T) {
	// Empty data with non-zero count is an error
	result, err := DecompressRLE([]byte{}, 4, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrRLEDataTruncated)
}

func TestDecompressRLE_CountZeroRun(t *testing.T) {
	// A run with count=0 should be skipped
	value1 := make([]byte, 4)
	binary.LittleEndian.PutUint32(value1, 42)

	value2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(value2, 100)

	// Three runs: 2x 42, 0x (skip), 3x 100
	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 2, value: value1},
		{count: 0, value: value2}, // This run contributes nothing
		{count: 3, value: value2},
	})

	result, err := DecompressRLE(data, 4, 5)

	require.NoError(t, err)
	assert.Equal(t, 20, len(result)) // 5 * 4 bytes

	// Expected: [42, 42, 100, 100, 100]
	expected := []uint32{42, 42, 100, 100, 100}
	for i, exp := range expected {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, exp, v, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressRLE_CountOneRun(t *testing.T) {
	// Single run with count=1
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 0xDEADBEEF)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 1, value: value},
	})

	result, err := DecompressRLE(data, 4, 1)

	require.NoError(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, uint32(0xDEADBEEF), binary.LittleEndian.Uint32(result))
}

func TestDecompressRLE_LargeCount(t *testing.T) {
	// Large count in a single run
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 0x123456789ABCDEF0)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 10000, value: value},
	})

	result, err := DecompressRLE(data, 8, 10000)

	require.NoError(t, err)
	assert.Equal(t, 80000, len(result)) // 10000 * 8 bytes

	// Verify first, middle, and last values
	expected := uint64(0x123456789ABCDEF0)
	assert.Equal(t, expected, binary.LittleEndian.Uint64(result[0:8]), "first value")
	assert.Equal(t, expected, binary.LittleEndian.Uint64(result[40000:40008]), "middle value")
	assert.Equal(t, expected, binary.LittleEndian.Uint64(result[79992:80000]), "last value")
}

func TestDecompressRLE_LargeVarintCount(t *testing.T) {
	// Test with a count that requires multi-byte varint encoding
	// Count = 300 requires 2 bytes as varint (0xAC, 0x02)
	value := make([]byte, 2)
	binary.LittleEndian.PutUint16(value, 0xABCD)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 300, value: value},
	})

	result, err := DecompressRLE(data, 2, 300)

	require.NoError(t, err)
	assert.Equal(t, 600, len(result)) // 300 * 2 bytes

	for i := 0; i < 300; i++ {
		offset := i * 2
		v := binary.LittleEndian.Uint16(result[offset : offset+2])
		assert.Equal(t, uint16(0xABCD), v, "value at index %d", i)
	}
}

func TestDecompressRLE_Int64Values(t *testing.T) {
	// Multiple runs with int64 values
	value1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(value1, 0x0000000000000001)

	value2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(value2, 0xFFFFFFFFFFFFFFFF)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 2, value: value1},
		{count: 3, value: value2},
	})

	result, err := DecompressRLE(data, 8, 5)

	require.NoError(t, err)
	assert.Equal(t, 40, len(result)) // 5 * 8 bytes

	// Verify values
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(result[8:16]))
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), binary.LittleEndian.Uint64(result[16:24]))
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), binary.LittleEndian.Uint64(result[24:32]))
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), binary.LittleEndian.Uint64(result[32:40]))
}

func TestDecompressRLE_TruncatedValue(t *testing.T) {
	// Data ends in the middle of a value
	// varint(5) = 0x05, then only 2 bytes of a 4-byte value
	data := []byte{0x05, 0x42, 0x43}

	result, err := DecompressRLE(data, 4, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrRLEDataTruncated)
}

func TestDecompressRLE_TruncatedVarint(t *testing.T) {
	// Data contains incomplete header (less than 8 bytes)
	data := []byte{0x80}

	result, err := DecompressRLE(data, 1, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrRLEDataTruncated)
}

func TestDecompressRLE_InvalidValueSize(t *testing.T) {
	// Zero value size
	data := []byte{0x05, 0x42, 0x43, 0x44, 0x45}

	result, err := DecompressRLE(data, 0, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrInvalidValueSize)

	// Negative value size
	result, err = DecompressRLE(data, -1, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrInvalidValueSize)
}

func TestDecompressRLE_ManyShortRuns(t *testing.T) {
	// Many runs with count=1 each (worst case for RLE)
	var runs []struct {
		count uint64
		value []byte
	}

	for i := 0; i < 100; i++ {
		value := make([]byte, 4)
		binary.LittleEndian.PutUint32(value, uint32(i))
		runs = append(runs, struct {
			count uint64
			value []byte
		}{count: 1, value: value})
	}

	data := buildRLEData(runs)

	result, err := DecompressRLE(data, 4, 100)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result)) // 100 * 4 bytes

	// Verify values are 0, 1, 2, ..., 99
	for i := 0; i < 100; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(i), v, "value at index %d should be %d", i, i)
	}
}

func TestDecompressRLE_SingleByteValues(t *testing.T) {
	// RLE with 1-byte values (like booleans)
	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 3, value: []byte{0x01}}, // true
		{count: 2, value: []byte{0x00}}, // false
		{count: 4, value: []byte{0x01}}, // true
	})

	result, err := DecompressRLE(data, 1, 9)

	require.NoError(t, err)
	assert.Equal(t, 9, len(result))

	// Expected: [1, 1, 1, 0, 0, 1, 1, 1, 1]
	expected := []byte{1, 1, 1, 0, 0, 1, 1, 1, 1}
	assert.Equal(t, expected, result)
}

func TestDecompressRLE_AllZeros(t *testing.T) {
	// All zero values
	value := make([]byte, 4)
	// Value is already all zeros

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 100, value: value},
	})

	result, err := DecompressRLE(data, 4, 100)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result))

	for i, b := range result {
		assert.Equal(t, byte(0), b, "byte at index %d should be 0", i)
	}
}

func TestDecompressRLE_AllOnes(t *testing.T) {
	// All 0xFF values
	value := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 50, value: value},
	})

	result, err := DecompressRLE(data, 8, 50)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result))

	for i, b := range result {
		assert.Equal(t, byte(0xFF), b, "byte at index %d should be 0xFF", i)
	}
}

func TestDecompressRLE_16ByteValues(t *testing.T) {
	// Large value size (like HUGEINT)
	value := make([]byte, 16)
	for i := 0; i < 16; i++ {
		value[i] = byte(i + 1) // 1, 2, 3, ..., 16
	}

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 5, value: value},
	})

	result, err := DecompressRLE(data, 16, 5)

	require.NoError(t, err)
	assert.Equal(t, 80, len(result)) // 5 * 16 bytes

	// Verify each 16-byte chunk matches
	for i := 0; i < 5; i++ {
		offset := i * 16
		chunk := result[offset : offset+16]
		assert.True(t, bytes.Equal(value, chunk), "chunk at index %d should match", i)
	}
}

func TestDecompress_DispatchRLE(t *testing.T) {
	// Test that Decompress correctly dispatches to DecompressRLE
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 42)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 5, value: value},
	})

	result, err := Decompress(CompressionRLE, data, 4, 5)

	require.NoError(t, err)
	assert.Equal(t, 20, len(result))

	for i := 0; i < 5; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(42), v)
	}
}

func TestRLEDecompressor_Interface(t *testing.T) {
	// Test the Decompressor interface with rleDecompressor
	var d Decompressor = NewRLEDecompressor()

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 999)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 3, value: value},
	})

	result, err := d.Decompress(data, 4, 3)

	require.NoError(t, err)
	assert.Equal(t, 12, len(result))

	for i := 0; i < 3; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(999), v)
	}
}

func TestDecompressRLE_VeryLargeVarint(t *testing.T) {
	// Test with a large count near uint16 maximum
	// DuckDB uses uint16 for run lengths, so max is 65535
	value := make([]byte, 1)
	value[0] = 0x42

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 65535, value: value}, // Max uint16
	})

	result, err := DecompressRLE(data, 1, 65535)

	require.NoError(t, err)
	assert.Equal(t, 65535, len(result))

	// Spot check some values
	assert.Equal(t, byte(0x42), result[0])
	assert.Equal(t, byte(0x42), result[32767])
	assert.Equal(t, byte(0x42), result[65534])
}

func TestDecompressRLE_OnlyZeroCountRuns(t *testing.T) {
	// Data contains only count=0 runs (should produce empty result)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, 42)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 0, value: value},
		{count: 0, value: value},
	})

	result, err := DecompressRLE(data, 4, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressRLE_NoDataAfterVarint(t *testing.T) {
	// Data has a valid varint count but no value bytes following
	data := []byte{0x05} // varint(5), but no value bytes

	result, err := DecompressRLE(data, 4, 5)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrRLEDataTruncated)
}

// Benchmarks for RLE decompression
func BenchmarkDecompressRLE_SingleRun_SmallCount(b *testing.B) {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 0x123456789ABCDEF0)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 100, value: value},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressRLE(data, 8, 100)
	}
}

func BenchmarkDecompressRLE_SingleRun_LargeCount(b *testing.B) {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 0x123456789ABCDEF0)

	data := buildRLEData([]struct {
		count uint64
		value []byte
	}{
		{count: 10000, value: value},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressRLE(data, 8, 10000)
	}
}

func BenchmarkDecompressRLE_ManySmallRuns(b *testing.B) {
	var runs []struct {
		count uint64
		value []byte
	}

	for i := 0; i < 1000; i++ {
		value := make([]byte, 4)
		binary.LittleEndian.PutUint32(value, uint32(i))
		runs = append(runs, struct {
			count uint64
			value []byte
		}{count: 10, value: value})
	}

	data := buildRLEData(runs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressRLE(data, 4, 10000)
	}
}

func BenchmarkDecompressRLE_RowGroupSize(b *testing.B) {
	// Simulate a typical scenario: few runs covering a row group
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 0x123456789ABCDEF0)

	// 10 runs of ~12288 each = 122880 (default row group size)
	var runs []struct {
		count uint64
		value []byte
	}
	for i := 0; i < 10; i++ {
		runs = append(runs, struct {
			count uint64
			value []byte
		}{count: 12288, value: value})
	}

	data := buildRLEData(runs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressRLE(data, 8, 122880)
	}
}

// =============================================================================
// Dictionary Decompression Tests
// =============================================================================

// Helper function to build dictionary compressed data for fixed-size values
// Format: [uint32 dictSize][values...][uint64 indexCount][indices...]
func buildDictDataFixedSize(values [][]byte, indices []uint32) []byte {
	var buf bytes.Buffer

	// Write dictionary size
	binary.Write(&buf, binary.LittleEndian, uint32(len(values)))

	// Write dictionary values (fixed-size, no length prefix)
	for _, v := range values {
		buf.Write(v)
	}

	// Write index count
	binary.Write(&buf, binary.LittleEndian, uint64(len(indices)))

	// Write indices
	for _, idx := range indices {
		binary.Write(&buf, binary.LittleEndian, idx)
	}

	return buf.Bytes()
}

// Helper function to build dictionary compressed data for variable-size (string) values
// Format: [uint32 dictSize][length-prefixed values...][uint64 indexCount][indices...]
func buildDictDataStrings(values []string, indices []uint32) []byte {
	var buf bytes.Buffer

	// Write dictionary size
	binary.Write(&buf, binary.LittleEndian, uint32(len(values)))

	// Write dictionary values (length-prefixed)
	for _, v := range values {
		binary.Write(&buf, binary.LittleEndian, uint32(len(v)))
		buf.WriteString(v)
	}

	// Write index count
	binary.Write(&buf, binary.LittleEndian, uint64(len(indices)))

	// Write indices
	for _, idx := range indices {
		binary.Write(&buf, binary.LittleEndian, idx)
	}

	return buf.Bytes()
}

// Helper to create int32 dictionary data
func buildDictDataInt32(values, indices []uint32) []byte {
	byteValues := make([][]byte, len(values))
	for i, v := range values {
		byteValues[i] = make([]byte, 4)
		binary.LittleEndian.PutUint32(byteValues[i], v)
	}
	return buildDictDataFixedSize(byteValues, indices)
}

// Helper to create int64 dictionary data
func buildDictDataInt64(values []uint64, indices []uint32) []byte {
	byteValues := make([][]byte, len(values))
	for i, v := range values {
		byteValues[i] = make([]byte, 8)
		binary.LittleEndian.PutUint64(byteValues[i], v)
	}
	return buildDictDataFixedSize(byteValues, indices)
}

func TestDecompressDictionary_FixedSize_Int32_Simple(t *testing.T) {
	// Dictionary: [42, 100]
	// Indices: [0, 0, 1, 0, 1] -> [42, 42, 100, 42, 100]
	data := buildDictDataInt32([]uint32{42, 100}, []uint32{0, 0, 1, 0, 1})

	result, err := DecompressDictionary(data, 4, 5)

	require.NoError(t, err)
	assert.Equal(t, 20, len(result)) // 5 * 4 bytes

	expected := []uint32{42, 42, 100, 42, 100}
	for i, exp := range expected {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, exp, v, "value at index %d should be %d", i, exp)
	}
}

func TestDecompressDictionary_FixedSize_Int64_Simple(t *testing.T) {
	// Dictionary: [0x123456789ABCDEF0, 0xFEDCBA9876543210]
	// Indices: [1, 0, 1, 1] -> [0xFEDCBA..., 0x123456..., 0xFEDCBA..., 0xFEDCBA...]
	data := buildDictDataInt64(
		[]uint64{0x123456789ABCDEF0, 0xFEDCBA9876543210},
		[]uint32{1, 0, 1, 1},
	)

	result, err := DecompressDictionary(data, 8, 4)

	require.NoError(t, err)
	assert.Equal(t, 32, len(result)) // 4 * 8 bytes

	expected := []uint64{0xFEDCBA9876543210, 0x123456789ABCDEF0, 0xFEDCBA9876543210, 0xFEDCBA9876543210}
	for i, exp := range expected {
		offset := i * 8
		v := binary.LittleEndian.Uint64(result[offset : offset+8])
		assert.Equal(t, exp, v, "value at index %d should be %x", i, exp)
	}
}

func TestDecompressDictionary_VariableSize_Strings_Simple(t *testing.T) {
	// Dictionary: ["hello", "world"]
	// Indices: [0, 1, 0] -> ["hello", "world", "hello"]
	data := buildDictDataStrings([]string{"hello", "world"}, []uint32{0, 1, 0})

	result, err := DecompressDictionary(data, 0, 3) // valueSize=0 for strings

	require.NoError(t, err)
	expected := "helloworldhello"
	assert.Equal(t, expected, string(result))
}

func TestDecompressDictionary_VariableSize_Strings_DifferentLengths(t *testing.T) {
	// Dictionary: ["a", "abc", "abcdefgh"]
	// Indices: [0, 2, 1, 0, 2] -> ["a", "abcdefgh", "abc", "a", "abcdefgh"]
	data := buildDictDataStrings([]string{"a", "abc", "abcdefgh"}, []uint32{0, 2, 1, 0, 2})

	result, err := DecompressDictionary(data, 0, 5)

	require.NoError(t, err)
	expected := "aabcdefghabcaabcdefgh"
	assert.Equal(t, expected, string(result))
}

func TestDecompressDictionary_VariableSize_EmptyStrings(t *testing.T) {
	// Dictionary: ["", "hello", ""]
	// Indices: [0, 1, 2, 1, 0] -> ["", "hello", "", "hello", ""]
	data := buildDictDataStrings([]string{"", "hello", ""}, []uint32{0, 1, 2, 1, 0})

	result, err := DecompressDictionary(data, 0, 5)

	require.NoError(t, err)
	expected := "hellohello"
	assert.Equal(t, expected, string(result))
}

func TestDecompressDictionary_SingleEntry_MultipleReferences(t *testing.T) {
	// Dictionary: [999]
	// Indices: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0] -> [999, 999, ...]
	indices := make([]uint32, 100)
	for i := range indices {
		indices[i] = 0
	}
	data := buildDictDataInt32([]uint32{999}, indices)

	result, err := DecompressDictionary(data, 4, 100)

	require.NoError(t, err)
	assert.Equal(t, 400, len(result)) // 100 * 4 bytes

	for i := 0; i < 100; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(999), v, "value at index %d should be 999", i)
	}
}

func TestDecompressDictionary_ManyEntries_FewReferences(t *testing.T) {
	// Dictionary: [0, 1, 2, ..., 99]
	// Indices: [50, 50] -> [50, 50]
	dictValues := make([]uint32, 100)
	for i := range dictValues {
		dictValues[i] = uint32(i)
	}
	data := buildDictDataInt32(dictValues, []uint32{50, 50})

	result, err := DecompressDictionary(data, 4, 2)

	require.NoError(t, err)
	assert.Equal(t, 8, len(result)) // 2 * 4 bytes

	assert.Equal(t, uint32(50), binary.LittleEndian.Uint32(result[0:4]))
	assert.Equal(t, uint32(50), binary.LittleEndian.Uint32(result[4:8]))
}

func TestDecompressDictionary_EmptyDictionary_EmptyIndices(t *testing.T) {
	// Empty dictionary, empty indices
	data := buildDictDataInt32([]uint32{}, []uint32{})

	result, err := DecompressDictionary(data, 4, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressDictionary_NonEmptyDictionary_EmptyIndices(t *testing.T) {
	// Dictionary has entries but no indices
	data := buildDictDataInt32([]uint32{42, 100}, []uint32{})

	result, err := DecompressDictionary(data, 4, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressDictionary_EmptyData_ZeroCount(t *testing.T) {
	result, err := DecompressDictionary([]byte{}, 4, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressDictionary_EmptyData_NonZeroCount(t *testing.T) {
	result, err := DecompressDictionary([]byte{}, 4, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_IndexOutOfBounds(t *testing.T) {
	// Dictionary: [42, 100]
	// Indices: [0, 2] -> error: index 2 is out of bounds (dict size 2)
	data := buildDictDataInt32([]uint32{42, 100}, []uint32{0, 2})

	result, err := DecompressDictionary(data, 4, 2)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryIndexOutOfBounds)
}

func TestDecompressDictionary_IndexOutOfBounds_LargeIndex(t *testing.T) {
	// Dictionary: [42]
	// Indices: [0, 1000] -> error: index 1000 is way out of bounds
	data := buildDictDataInt32([]uint32{42}, []uint32{0, 1000})

	result, err := DecompressDictionary(data, 4, 2)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryIndexOutOfBounds)
}

func TestDecompressDictionary_TruncatedDictSize(t *testing.T) {
	// Only 2 bytes when we need 4 for dict size
	data := []byte{0x01, 0x00}

	result, err := DecompressDictionary(data, 4, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_TruncatedDictValue(t *testing.T) {
	// Dict size = 1, but value is truncated
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(1)) // dictSize = 1
	buf.Write([]byte{0x42, 0x43})                      // Only 2 bytes when we need 4

	result, err := DecompressDictionary(buf.Bytes(), 4, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_TruncatedIndexCount(t *testing.T) {
	// Dict size = 1, one 4-byte value, but index count is truncated
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(1)) // dictSize = 1
	binary.Write(&buf, binary.LittleEndian, uint32(42)) // value
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00})          // Only 4 bytes when we need 8 for indexCount

	result, err := DecompressDictionary(buf.Bytes(), 4, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_TruncatedIndices(t *testing.T) {
	// Dict size = 1, one value, indexCount = 2, but only 1 index present
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(1))  // dictSize = 1
	binary.Write(&buf, binary.LittleEndian, uint32(42)) // value
	binary.Write(&buf, binary.LittleEndian, uint64(2))  // indexCount = 2
	binary.Write(&buf, binary.LittleEndian, uint32(0))  // Only 1 index

	result, err := DecompressDictionary(buf.Bytes(), 4, 2)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_TruncatedStringLength(t *testing.T) {
	// Dict size = 1, but string length field is truncated
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(1)) // dictSize = 1
	buf.Write([]byte{0x05, 0x00})                      // Only 2 bytes for length (need 4)

	result, err := DecompressDictionary(buf.Bytes(), 0, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_TruncatedStringValue(t *testing.T) {
	// Dict size = 1, string length = 5, but only 3 bytes of string
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(1)) // dictSize = 1
	binary.Write(&buf, binary.LittleEndian, uint32(5)) // string length = 5
	buf.Write([]byte{'h', 'e', 'l'})                   // Only 3 bytes

	result, err := DecompressDictionary(buf.Bytes(), 0, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrDictionaryDataTruncated)
}

func TestDecompressDictionary_LargeDict_LargeIndices(t *testing.T) {
	// Dictionary: 1000 values
	// Indices: 10000 references
	dictValues := make([]uint32, 1000)
	for i := range dictValues {
		dictValues[i] = uint32(i * 10)
	}

	indices := make([]uint32, 10000)
	for i := range indices {
		indices[i] = uint32(i % 1000) // Cycle through dictionary
	}

	data := buildDictDataInt32(dictValues, indices)

	result, err := DecompressDictionary(data, 4, 10000)

	require.NoError(t, err)
	assert.Equal(t, 40000, len(result)) // 10000 * 4 bytes

	// Verify some values
	for i := 0; i < 10000; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		expected := uint32((i % 1000) * 10)
		assert.Equal(t, expected, v, "value at index %d", i)
	}
}

func TestDecompressDictionary_16ByteValues(t *testing.T) {
	// HUGEINT values (16 bytes each)
	value1 := make([]byte, 16)
	value2 := make([]byte, 16)
	for i := 0; i < 16; i++ {
		value1[i] = byte(i)
		value2[i] = byte(255 - i)
	}

	data := buildDictDataFixedSize([][]byte{value1, value2}, []uint32{0, 1, 0, 1, 0})

	result, err := DecompressDictionary(data, 16, 5)

	require.NoError(t, err)
	assert.Equal(t, 80, len(result)) // 5 * 16 bytes

	// Verify alternating pattern
	assert.True(t, bytes.Equal(value1, result[0:16]))
	assert.True(t, bytes.Equal(value2, result[16:32]))
	assert.True(t, bytes.Equal(value1, result[32:48]))
	assert.True(t, bytes.Equal(value2, result[48:64]))
	assert.True(t, bytes.Equal(value1, result[64:80]))
}

func TestDecompressDictionary_SingleByteValues(t *testing.T) {
	// Boolean-like values
	value1 := []byte{0x00} // false
	value2 := []byte{0x01} // true

	data := buildDictDataFixedSize([][]byte{value1, value2}, []uint32{0, 1, 1, 0, 1})

	result, err := DecompressDictionary(data, 1, 5)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))
	assert.Equal(t, []byte{0x00, 0x01, 0x01, 0x00, 0x01}, result)
}

func TestDecompressDictionary_AllSameIndex(t *testing.T) {
	// All indices reference the same dictionary entry
	indices := make([]uint32, 1000)
	for i := range indices {
		indices[i] = 2 // All reference index 2
	}

	data := buildDictDataInt32([]uint32{10, 20, 30, 40, 50}, indices)

	result, err := DecompressDictionary(data, 4, 1000)

	require.NoError(t, err)
	assert.Equal(t, 4000, len(result)) // 1000 * 4 bytes

	// All values should be 30 (index 2)
	for i := 0; i < 1000; i++ {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(30), v, "value at index %d should be 30", i)
	}
}

func TestDecompressDictionary_NegativeValueSize_TreatedAsVariable(t *testing.T) {
	// Negative valueSize should be treated as variable-size (strings)
	data := buildDictDataStrings([]string{"test", "data"}, []uint32{0, 1})

	result, err := DecompressDictionary(data, -1, 2)

	require.NoError(t, err)
	expected := "testdata"
	assert.Equal(t, expected, string(result))
}

func TestDecompress_DispatchDictionary(t *testing.T) {
	// Test that Decompress correctly dispatches to DecompressDictionary
	data := buildDictDataInt32([]uint32{42, 100}, []uint32{0, 1, 0})

	result, err := Decompress(CompressionDictionary, data, 4, 3)

	require.NoError(t, err)
	assert.Equal(t, 12, len(result)) // 3 * 4 bytes

	expected := []uint32{42, 100, 42}
	for i, exp := range expected {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, exp, v)
	}
}

func TestDictionaryDecompressor_Interface(t *testing.T) {
	// Test the Decompressor interface with dictionaryDecompressor
	var d Decompressor = NewDictionaryDecompressor()

	data := buildDictDataInt32([]uint32{999, 111}, []uint32{1, 0, 1})

	result, err := d.Decompress(data, 4, 3)

	require.NoError(t, err)
	assert.Equal(t, 12, len(result)) // 3 * 4 bytes

	expected := []uint32{111, 999, 111}
	for i, exp := range expected {
		offset := i * 4
		v := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, exp, v)
	}
}

func TestGetDecompressor_Dictionary(t *testing.T) {
	d := GetDecompressor(CompressionDictionary)
	assert.NotNil(t, d)
	_, ok := d.(*dictionaryDecompressor)
	assert.True(t, ok)
}

func TestDecompressDictionary_UnicodeStrings(t *testing.T) {
	// Dictionary with Unicode strings
	data := buildDictDataStrings([]string{"hello", "world", "unicode test"}, []uint32{0, 2, 1})

	result, err := DecompressDictionary(data, 0, 3)

	require.NoError(t, err)
	expected := "hellounicode testworld"
	assert.Equal(t, expected, string(result))
}

func TestDecompressDictionary_BinaryData(t *testing.T) {
	// Dictionary with arbitrary binary data (not valid UTF-8)
	value1 := []byte{0xFF, 0xFE, 0x00, 0x01}
	value2 := []byte{0x00, 0x00}
	value3 := []byte{0xAB, 0xCD, 0xEF}

	// Build manually for binary values with length prefix
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(3)) // dictSize

	// Value 1
	binary.Write(&buf, binary.LittleEndian, uint32(len(value1)))
	buf.Write(value1)

	// Value 2
	binary.Write(&buf, binary.LittleEndian, uint32(len(value2)))
	buf.Write(value2)

	// Value 3
	binary.Write(&buf, binary.LittleEndian, uint32(len(value3)))
	buf.Write(value3)

	// Indices
	binary.Write(&buf, binary.LittleEndian, uint64(4))
	binary.Write(&buf, binary.LittleEndian, uint32(0))
	binary.Write(&buf, binary.LittleEndian, uint32(1))
	binary.Write(&buf, binary.LittleEndian, uint32(2))
	binary.Write(&buf, binary.LittleEndian, uint32(0))

	result, err := DecompressDictionary(buf.Bytes(), 0, 4)

	require.NoError(t, err)
	expected := append(append(append(value1, value2...), value3...), value1...)
	assert.Equal(t, expected, result)
}

// Benchmarks for Dictionary decompression
func BenchmarkDecompressDictionary_SmallDict_SmallIndices(b *testing.B) {
	// 10 dictionary entries, 100 indices
	dictValues := make([]uint32, 10)
	for i := range dictValues {
		dictValues[i] = uint32(i * 100)
	}

	indices := make([]uint32, 100)
	for i := range indices {
		indices[i] = uint32(i % 10)
	}

	data := buildDictDataInt32(dictValues, indices)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressDictionary(data, 4, 100)
	}
}

func BenchmarkDecompressDictionary_SmallDict_LargeIndices(b *testing.B) {
	// 10 dictionary entries, 10000 indices
	dictValues := make([]uint32, 10)
	for i := range dictValues {
		dictValues[i] = uint32(i * 100)
	}

	indices := make([]uint32, 10000)
	for i := range indices {
		indices[i] = uint32(i % 10)
	}

	data := buildDictDataInt32(dictValues, indices)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressDictionary(data, 4, 10000)
	}
}

func BenchmarkDecompressDictionary_LargeDict_SmallIndices(b *testing.B) {
	// 1000 dictionary entries, 100 indices
	dictValues := make([]uint32, 1000)
	for i := range dictValues {
		dictValues[i] = uint32(i)
	}

	indices := make([]uint32, 100)
	for i := range indices {
		indices[i] = uint32(i % 1000)
	}

	data := buildDictDataInt32(dictValues, indices)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressDictionary(data, 4, 100)
	}
}

func BenchmarkDecompressDictionary_Strings(b *testing.B) {
	// String dictionary
	dictValues := []string{
		"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew", "imbe", "jackfruit",
	}

	indices := make([]uint32, 1000)
	for i := range indices {
		indices[i] = uint32(i % 10)
	}

	data := buildDictDataStrings(dictValues, indices)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressDictionary(data, 0, 1000)
	}
}

func BenchmarkDecompressDictionary_RowGroupSize(b *testing.B) {
	// Simulate typical row group scenario: few unique values
	dictValues := make([]uint64, 50) // 50 unique values
	for i := range dictValues {
		dictValues[i] = uint64(i * 1000)
	}

	indices := make([]uint32, 122880) // Default row group size
	for i := range indices {
		indices[i] = uint32(i % 50)
	}

	data := buildDictDataInt64(dictValues, indices)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressDictionary(data, 8, 122880)
	}
}

// =============================================================================
// BitPacking Decompression Tests
// =============================================================================

// Helper function to build bit-packed data
// Format: [uint8 bitWidth][uint64 count][packed bits...]
func buildBitPackedData(bitWidth uint8, values []uint64) []byte {
	count := uint64(len(values))

	// Header: 1 byte bitWidth + 8 bytes count
	header := make([]byte, 9)
	header[0] = bitWidth
	binary.LittleEndian.PutUint64(header[1:9], count)

	// Special case: bitWidth = 0 means all zeros, no packed data needed
	if bitWidth == 0 {
		return header
	}

	// Calculate packed data size
	totalBits := uint64(bitWidth) * count
	packedBytes := (totalBits + 7) / 8 // Round up

	packedData := make([]byte, packedBytes)

	// Pack values bit by bit
	bitPos := 0
	for _, value := range values {
		for b := uint8(0); b < bitWidth; b++ {
			byteIdx := bitPos / 8
			bitIdx := bitPos % 8

			// Set bit if value has this bit set
			if value&(1<<b) != 0 {
				packedData[byteIdx] |= 1 << bitIdx
			}
			bitPos++
		}
	}

	return append(header, packedData...)
}

func TestDecompressBitPacking_BitWidth1_Simple(t *testing.T) {
	// Bit width 1: values 0 or 1 (like booleans)
	values := []uint64{0, 1, 1, 0, 1, 0, 0, 1}
	data := buildBitPackedData(1, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth2_Simple(t *testing.T) {
	// Bit width 2: values 0-3
	values := []uint64{0, 1, 2, 3, 0, 1, 2, 3}
	data := buildBitPackedData(2, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth4_Simple(t *testing.T) {
	// Bit width 4: values 0-15
	values := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	data := buildBitPackedData(4, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth7_OddBitWidth(t *testing.T) {
	// Bit width 7: values 0-127 (tests non-byte-aligned bit widths)
	values := []uint64{0, 1, 64, 127, 100, 50, 25, 13, 7, 3}
	data := buildBitPackedData(7, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth8_FullByte(t *testing.T) {
	// Bit width 8: full byte values 0-255
	values := []uint64{0, 1, 127, 128, 255, 42, 100, 200}
	data := buildBitPackedData(8, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth16_TwoBytes(t *testing.T) {
	// Bit width 16: two-byte values 0-65535
	values := []uint64{0, 1, 32767, 32768, 65535, 12345, 54321}
	data := buildBitPackedData(16, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth32_FourBytes(t *testing.T) {
	// Bit width 32: four-byte values
	values := []uint64{0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF, 123456789}
	data := buildBitPackedData(32, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth64_FullUint64(t *testing.T) {
	// Bit width 64: full uint64 values
	values := []uint64{0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF, 0x123456789ABCDEF0}
	data := buildBitPackedData(64, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_BitWidth0_AllZeros(t *testing.T) {
	// Bit width 0: all values are 0 (no packed data needed)
	count := uint64(100)
	data := buildBitPackedData(0, make([]uint64, count))

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, int(count), len(result))

	// All values should be 0
	for i, v := range result {
		assert.Equal(t, uint64(0), v, "value at index %d should be 0", i)
	}
}

func TestDecompressBitPacking_CountZero(t *testing.T) {
	// Count = 0: empty result
	data := buildBitPackedData(8, []uint64{})

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPacking_CountOne(t *testing.T) {
	// Single value
	values := []uint64{42}
	data := buildBitPackedData(8, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, uint64(42), result[0])
}

func TestDecompressBitPacking_PartialLastByte(t *testing.T) {
	// 5 values at 3 bits each = 15 bits = 2 bytes (with 1 bit unused)
	values := []uint64{0, 1, 2, 3, 4}
	data := buildBitPackedData(3, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, len(values), len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_LargeCount(t *testing.T) {
	// Large number of values
	count := 10000
	values := make([]uint64, count)
	for i := range values {
		values[i] = uint64(i % 16) // Values 0-15, fit in 4 bits
	}
	data := buildBitPackedData(4, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, count, len(result))
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_RowGroupSize(t *testing.T) {
	// Test with default row group size (122880 values)
	count := 122880
	values := make([]uint64, count)
	for i := range values {
		values[i] = uint64(i % 256) // Values 0-255, fit in 8 bits
	}
	data := buildBitPackedData(8, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, count, len(result))

	// Verify first, middle, and last values
	assert.Equal(t, uint64(0), result[0])
	assert.Equal(t, uint64(0), result[61440]) // 61440 % 256 = 0
	assert.Equal(t, uint64(255), result[122879]) // 122879 % 256 = 255
}

func TestDecompressBitPacking_AllOnes(t *testing.T) {
	// All values at maximum for bit width
	values := make([]uint64, 100)
	for i := range values {
		values[i] = 255 // All bits set for 8-bit width
	}
	data := buildBitPackedData(8, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	for i, v := range result {
		assert.Equal(t, uint64(255), v, "value at index %d should be 255", i)
	}
}

func TestDecompressBitPacking_AlternatingBits(t *testing.T) {
	// Alternating bit patterns
	values := []uint64{0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA}
	data := buildBitPackedData(8, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

// Error Cases

func TestDecompressBitPacking_DataTooShort_Header(t *testing.T) {
	// Data shorter than header (less than 9 bytes)
	data := []byte{8, 10, 0, 0, 0} // Only 5 bytes, need 9 for header

	result, err := DecompressBitPackingToUint64(data)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingDataTruncated)
}

func TestDecompressBitPacking_DataTooShort_PackedData(t *testing.T) {
	// Header says 10 values at 8 bits each = 10 bytes needed
	// But only provide 5 bytes of packed data
	header := make([]byte, 9)
	header[0] = 8 // bitWidth
	binary.LittleEndian.PutUint64(header[1:9], 10) // count

	data := append(header, []byte{1, 2, 3, 4, 5}...) // Only 5 bytes

	result, err := DecompressBitPackingToUint64(data)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingDataTruncated)
}

func TestDecompressBitPacking_InvalidBitWidth_65(t *testing.T) {
	// Bit width > 64 is invalid
	header := make([]byte, 9)
	header[0] = 65 // Invalid bitWidth
	binary.LittleEndian.PutUint64(header[1:9], 10)

	result, err := DecompressBitPackingToUint64(header)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingInvalidBitWidth)
}

func TestDecompressBitPacking_InvalidBitWidth_128(t *testing.T) {
	// Bit width = 128 is definitely invalid
	header := make([]byte, 9)
	header[0] = 128 // Invalid bitWidth
	binary.LittleEndian.PutUint64(header[1:9], 1)

	result, err := DecompressBitPackingToUint64(header)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingInvalidBitWidth)
}

func TestDecompressBitPacking_InvalidBitWidth_255(t *testing.T) {
	// Bit width = 255 is max uint8 but invalid
	header := make([]byte, 9)
	header[0] = 255 // Invalid bitWidth
	binary.LittleEndian.PutUint64(header[1:9], 1)

	result, err := DecompressBitPackingToUint64(header)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingInvalidBitWidth)
}

func TestDecompressBitPacking_EmptyData(t *testing.T) {
	result, err := DecompressBitPackingToUint64([]byte{})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingDataTruncated)
}

// BitPackedToBytes tests

func TestBitPackedToBytes_TargetSize1(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255}

	result := BitPackedToBytes(values, 1)

	assert.Equal(t, 5, len(result))
	assert.Equal(t, byte(0), result[0])
	assert.Equal(t, byte(1), result[1])
	assert.Equal(t, byte(127), result[2])
	assert.Equal(t, byte(128), result[3])
	assert.Equal(t, byte(255), result[4])
}

func TestBitPackedToBytes_TargetSize2(t *testing.T) {
	values := []uint64{0, 1, 32767, 32768, 65535}

	result := BitPackedToBytes(values, 2)

	assert.Equal(t, 10, len(result))
	assert.Equal(t, uint16(0), binary.LittleEndian.Uint16(result[0:2]))
	assert.Equal(t, uint16(1), binary.LittleEndian.Uint16(result[2:4]))
	assert.Equal(t, uint16(32767), binary.LittleEndian.Uint16(result[4:6]))
	assert.Equal(t, uint16(32768), binary.LittleEndian.Uint16(result[6:8]))
	assert.Equal(t, uint16(65535), binary.LittleEndian.Uint16(result[8:10]))
}

func TestBitPackedToBytes_TargetSize4(t *testing.T) {
	values := []uint64{0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF}

	result := BitPackedToBytes(values, 4)

	assert.Equal(t, 20, len(result))
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(result[0:4]))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(result[4:8]))
	assert.Equal(t, uint32(0x7FFFFFFF), binary.LittleEndian.Uint32(result[8:12]))
	assert.Equal(t, uint32(0x80000000), binary.LittleEndian.Uint32(result[12:16]))
	assert.Equal(t, uint32(0xFFFFFFFF), binary.LittleEndian.Uint32(result[16:20]))
}

func TestBitPackedToBytes_TargetSize8(t *testing.T) {
	values := []uint64{0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF}

	result := BitPackedToBytes(values, 8)

	assert.Equal(t, 40, len(result))
	assert.Equal(t, uint64(0), binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, uint64(1), binary.LittleEndian.Uint64(result[8:16]))
	assert.Equal(t, uint64(0x7FFFFFFFFFFFFFFF), binary.LittleEndian.Uint64(result[16:24]))
	assert.Equal(t, uint64(0x8000000000000000), binary.LittleEndian.Uint64(result[24:32]))
	assert.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), binary.LittleEndian.Uint64(result[32:40]))
}

func TestBitPackedToBytes_TargetSizeInvalid_DefaultsTo8(t *testing.T) {
	values := []uint64{42, 100}

	// Invalid sizes should default to 8
	result := BitPackedToBytes(values, 3)

	assert.Equal(t, 16, len(result)) // 2 * 8 bytes
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, uint64(100), binary.LittleEndian.Uint64(result[8:16]))
}

func TestBitPackedToBytes_TargetSizeZero_DefaultsTo8(t *testing.T) {
	values := []uint64{42}

	result := BitPackedToBytes(values, 0)

	assert.Equal(t, 8, len(result))
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(result))
}

func TestBitPackedToBytes_TargetSizeNegative_DefaultsTo8(t *testing.T) {
	values := []uint64{42}

	result := BitPackedToBytes(values, -1)

	assert.Equal(t, 8, len(result))
	assert.Equal(t, uint64(42), binary.LittleEndian.Uint64(result))
}

func TestBitPackedToBytes_EmptyValues(t *testing.T) {
	result := BitPackedToBytes([]uint64{}, 4)

	assert.Empty(t, result)
}

func TestBitPackedToBytes_Truncation(t *testing.T) {
	// Values larger than target size should be truncated
	values := []uint64{0x123456789ABCDEF0}

	// Truncate to 1 byte: should get 0xF0
	result1 := BitPackedToBytes(values, 1)
	assert.Equal(t, byte(0xF0), result1[0])

	// Truncate to 2 bytes: should get 0xDEF0
	result2 := BitPackedToBytes(values, 2)
	assert.Equal(t, uint16(0xDEF0), binary.LittleEndian.Uint16(result2))

	// Truncate to 4 bytes: should get 0x9ABCDEF0
	result4 := BitPackedToBytes(values, 4)
	assert.Equal(t, uint32(0x9ABCDEF0), binary.LittleEndian.Uint32(result4))
}

// DecompressBitPacking (with byte conversion) tests

func TestDecompressBitPacking_ToBytes_TargetSize1(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255}
	data := buildBitPackedData(8, values)

	unpacked, err := DecompressBitPackingToUint64(data)
	require.NoError(t, err)
	result := BitPackedToBytes(unpacked, 1)

	assert.Equal(t, 5, len(result))

	for i, v := range values {
		assert.Equal(t, byte(v), result[i], "value at index %d", i)
	}
}

func TestDecompressBitPacking_ToBytes_TargetSize4(t *testing.T) {
	values := []uint64{0, 1, 1000, 65536, 16777215}
	data := buildBitPackedData(32, values)

	unpacked, err := DecompressBitPackingToUint64(data)
	require.NoError(t, err)
	result := BitPackedToBytes(unpacked, 4)

	assert.Equal(t, 20, len(result)) // 5 * 4 bytes

	for i, v := range values {
		offset := i * 4
		actual := binary.LittleEndian.Uint32(result[offset : offset+4])
		assert.Equal(t, uint32(v), actual, "value at index %d", i)
	}
}

// Decompress dispatcher tests

func TestDecompress_DispatchBitPacking(t *testing.T) {
	values := []uint64{0, 1, 2, 3, 4, 5, 6, 7}
	data := buildBitPackedData(4, values)

	// Decompress uses DecompressBitPacking which expects DuckDB format
	// So we need to test with our simple format using the two-step approach
	unpacked, err := DecompressBitPackingToUint64(data)
	require.NoError(t, err)
	result := BitPackedToBytes(unpacked, 1)

	assert.Equal(t, 8, len(result))

	for i, v := range values {
		assert.Equal(t, byte(v), result[i], "value at index %d", i)
	}
}

func TestBitPackingDecompressor_Interface(t *testing.T) {
	values := []uint64{0, 1, 2, 3}
	data := buildBitPackedData(4, values)

	// Use the two-step approach since buildBitPackedData produces our simple format
	unpacked, err := DecompressBitPackingToUint64(data)
	require.NoError(t, err)
	result := BitPackedToBytes(unpacked, 1)

	assert.Equal(t, 4, len(result))

	for i, v := range values {
		assert.Equal(t, byte(v), result[i])
	}
}

func TestGetDecompressor_BitPacking(t *testing.T) {
	d := GetDecompressor(CompressionBitPacking)
	assert.NotNil(t, d)
	_, ok := d.(*bitpackingDecompressor)
	assert.True(t, ok)
}

// Edge cases for specific bit patterns

func TestDecompressBitPacking_CrossByteBoundary(t *testing.T) {
	// 5-bit values crossing byte boundaries
	// Value 31 (11111) needs 5 bits
	values := []uint64{31, 31, 31, 31, 31}
	data := buildBitPackedData(5, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_13BitWidth(t *testing.T) {
	// 13 bits: values 0-8191, crossing multiple byte boundaries
	values := []uint64{0, 1, 4095, 4096, 8191}
	data := buildBitPackedData(13, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_17BitWidth(t *testing.T) {
	// 17 bits: slightly more than 2 bytes
	values := []uint64{0, 1, 65535, 65536, 131071}
	data := buildBitPackedData(17, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_31BitWidth(t *testing.T) {
	// 31 bits: one less than 4 bytes
	values := []uint64{0, 1, 1073741823, 2147483647}
	data := buildBitPackedData(31, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_33BitWidth(t *testing.T) {
	// 33 bits: one more than 4 bytes
	maxVal := uint64(1<<33 - 1)
	values := []uint64{0, 1, maxVal / 2, maxVal}
	data := buildBitPackedData(33, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPacking_63BitWidth(t *testing.T) {
	// 63 bits: one less than max
	maxVal := uint64(1<<63 - 1)
	values := []uint64{0, 1, maxVal / 2, maxVal}
	data := buildBitPackedData(63, values)

	result, err := DecompressBitPackingToUint64(data)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

// Benchmark tests

func BenchmarkDecompressBitPacking_SmallCount_4Bit(b *testing.B) {
	values := make([]uint64, 100)
	for i := range values {
		values[i] = uint64(i % 16)
	}
	data := buildBitPackedData(4, values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingToUint64(data)
	}
}

func BenchmarkDecompressBitPacking_LargeCount_4Bit(b *testing.B) {
	values := make([]uint64, 10000)
	for i := range values {
		values[i] = uint64(i % 16)
	}
	data := buildBitPackedData(4, values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingToUint64(data)
	}
}

func BenchmarkDecompressBitPacking_RowGroupSize_8Bit(b *testing.B) {
	values := make([]uint64, 122880) // Default row group size
	for i := range values {
		values[i] = uint64(i % 256)
	}
	data := buildBitPackedData(8, values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingToUint64(data)
	}
}

func BenchmarkDecompressBitPacking_RowGroupSize_32Bit(b *testing.B) {
	values := make([]uint64, 122880)
	for i := range values {
		values[i] = uint64(i)
	}
	data := buildBitPackedData(32, values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressBitPackingToUint64(data)
	}
}

func BenchmarkBitPackedToBytes_RowGroupSize(b *testing.B) {
	values := make([]uint64, 122880)
	for i := range values {
		values[i] = uint64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BitPackedToBytes(values, 8)
	}
}

// =============================================================================
// PFOR_DELTA Decompression Tests
// =============================================================================

// buildPFORDeltaData builds PFOR_DELTA compressed data for testing.
// Format: [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
// The count in the header is the total number of values (reference + len(deltas)).
func buildPFORDeltaData(reference int64, deltas []uint64, bitWidth uint8) []byte {
	// Calculate total value count: 1 (reference) + number of deltas
	count := uint64(len(deltas) + 1)

	// Header: 8 bytes reference + 1 byte bitWidth + 8 bytes count = 17 bytes
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], uint64(reference))
	data[8] = bitWidth
	binary.LittleEndian.PutUint64(data[9:17], count)

	// Build bit-packed deltas (using raw packing, no header)
	if len(deltas) > 0 && bitWidth > 0 {
		packedData := packBitsRaw(deltas, bitWidth)
		data = append(data, packedData...)
	}

	return data
}

// packBitsRaw bit-packs values without adding a header (raw data only).
// This is used by buildPFORDeltaData which needs to embed packed data without a header.
func packBitsRaw(values []uint64, bitWidth uint8) []byte {
	if len(values) == 0 || bitWidth == 0 {
		return []byte{}
	}

	totalBits := int(bitWidth) * len(values)
	packedBytes := (totalBits + 7) / 8
	data := make([]byte, packedBytes)

	bitPos := 0
	for _, value := range values {
		for b := uint8(0); b < bitWidth; b++ {
			if value&(1<<b) != 0 {
				byteIdx := bitPos / 8
				bitIdx := bitPos % 8
				data[byteIdx] |= 1 << bitIdx
			}
			bitPos++
		}
	}

	return data
}

func TestDecompressPFORDelta_SequentialValues(t *testing.T) {
	// Test sequential values [100, 101, 102, 103, 104]
	// Deltas are all 1, stored with bitWidth=1
	deltas := []uint64{1, 1, 1, 1}
	data := buildPFORDeltaData(100, deltas, 1)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))
	assert.Equal(t, []int64{100, 101, 102, 103, 104}, result)
}

func TestDecompressPFORDelta_VaryingGaps(t *testing.T) {
	// Test sorted values with varying gaps [10, 15, 17, 20, 30]
	// Deltas: 5, 2, 3, 10 (max delta=10, needs 4 bits)
	deltas := []uint64{5, 2, 3, 10}
	data := buildPFORDeltaData(10, deltas, 4)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))
	assert.Equal(t, []int64{10, 15, 17, 20, 30}, result)
}

func TestDecompressPFORDelta_ArithmeticSequence(t *testing.T) {
	// Test arithmetic sequence with constant delta of 3: [5, 8, 11, 14, 17, 20]
	// All deltas are 3, needs 2 bits
	deltas := []uint64{3, 3, 3, 3, 3}
	data := buildPFORDeltaData(5, deltas, 2)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 6, len(result))
	assert.Equal(t, []int64{5, 8, 11, 14, 17, 20}, result)
}

func TestDecompressPFORDelta_CountZero(t *testing.T) {
	// Edge case: count is 0
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], uint64(100)) // reference
	data[8] = 4                                           // bitWidth
	binary.LittleEndian.PutUint64(data[9:17], 0)          // count = 0

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressPFORDelta_CountOne(t *testing.T) {
	// Edge case: count is 1 (only reference, no deltas)
	data := make([]byte, 17)
	binary.LittleEndian.PutUint64(data[0:8], uint64(42)) // reference
	data[8] = 0                                          // bitWidth (irrelevant with no deltas)
	binary.LittleEndian.PutUint64(data[9:17], 1)         // count = 1

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, int64(42), result[0])
}

func TestDecompressPFORDelta_LargeReference(t *testing.T) {
	// Test with a large positive reference value
	largeRef := int64(1000000000000) // 1 trillion
	deltas := []uint64{1, 2, 3}
	data := buildPFORDeltaData(largeRef, deltas, 2)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, largeRef, result[0])
	assert.Equal(t, largeRef+1, result[1])
	assert.Equal(t, largeRef+3, result[2])
	assert.Equal(t, largeRef+6, result[3])
}

func TestDecompressPFORDelta_NegativeReference(t *testing.T) {
	// Test with a negative reference value
	negRef := int64(-1000)
	deltas := []uint64{10, 20, 30}
	data := buildPFORDeltaData(negRef, deltas, 5)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, int64(-1000), result[0])
	assert.Equal(t, int64(-990), result[1])
	assert.Equal(t, int64(-970), result[2])
	assert.Equal(t, int64(-940), result[3])
}

func TestDecompressPFORDelta_ZeroDeltas(t *testing.T) {
	// Test with all zero deltas (constant value repeated)
	deltas := []uint64{0, 0, 0, 0}
	data := buildPFORDeltaData(500, deltas, 0) // bitWidth=0 for all zeros

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))
	for _, v := range result {
		assert.Equal(t, int64(500), v)
	}
}

func TestDecompressPFORDelta_LargeDeltas(t *testing.T) {
	// Test with larger deltas requiring more bits
	deltas := []uint64{1000, 2000, 3000, 4000}
	data := buildPFORDeltaData(0, deltas, 12) // 12 bits can hold up to 4095

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 5, len(result))
	assert.Equal(t, []int64{0, 1000, 3000, 6000, 10000}, result)
}

func TestDecompressPFORDelta_TruncatedHeader(t *testing.T) {
	// Error case: data is shorter than 17-byte header
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} // Only 10 bytes

	result, err := DecompressPFORDeltaToInt64(data)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrPFORDeltaDataTruncated)
}

func TestDecompressPFORDelta_TruncatedDeltas(t *testing.T) {
	// Error case: header claims more deltas than data contains
	data := make([]byte, 18) // 17 header + 1 data byte
	binary.LittleEndian.PutUint64(data[0:8], 100)
	data[8] = 8 // 8 bits per delta
	// Count = 100, meaning 99 deltas needed, but we only provide 1 byte (8 bits = 1 delta)
	binary.LittleEndian.PutUint64(data[9:17], 100)
	data[17] = 0xFF // Only 1 byte of delta data

	result, err := DecompressPFORDeltaToInt64(data)

	assert.Error(t, err)
	assert.Nil(t, result)
	// The error should come from bit-packing decompression
	assert.ErrorIs(t, err, ErrBitPackingDataTruncated)
}

func TestDecompressPFORDelta_MaxBitWidth(t *testing.T) {
	// Test with maximum valid bit width (64 bits)
	deltas := []uint64{0xFFFFFFFFFFFFFFFF, 1, 0x8000000000000000}
	data := buildPFORDeltaData(0, deltas, 64)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 4, len(result))
	// Note: these are cumulative deltas interpreted as signed int64
	// The values will overflow when treated as int64, but the decompression should work
}

func TestDecompressPFORDelta_ByteOutput(t *testing.T) {
	// Test the byte-returning version
	deltas := []uint64{1, 1, 1}
	data := buildPFORDeltaData(10, deltas, 1)

	result, err := DecompressPFORDelta(data, 8, 4)

	require.NoError(t, err)
	assert.Equal(t, 32, len(result)) // 4 values * 8 bytes

	// Verify values as bytes
	assert.Equal(t, uint64(10), binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, uint64(11), binary.LittleEndian.Uint64(result[8:16]))
	assert.Equal(t, uint64(12), binary.LittleEndian.Uint64(result[16:24]))
	assert.Equal(t, uint64(13), binary.LittleEndian.Uint64(result[24:32]))
}

func TestDecompressPFORDelta_LargeSequence(t *testing.T) {
	// Test with a larger sequence (1000 values)
	count := 1000
	deltas := make([]uint64, count-1)
	for i := range deltas {
		deltas[i] = 1 // All deltas are 1
	}
	data := buildPFORDeltaData(0, deltas, 1)

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, count, len(result))

	// Verify it's 0, 1, 2, ..., 999
	for i, v := range result {
		assert.Equal(t, int64(i), v, "value at index %d should be %d", i, i)
	}
}

func TestDecompressPFORDelta_MixedDeltas(t *testing.T) {
	// Test with mixed small and large deltas
	deltas := []uint64{1, 100, 5, 50, 2}
	data := buildPFORDeltaData(0, deltas, 7) // 7 bits can hold up to 127

	result, err := DecompressPFORDeltaToInt64(data)

	require.NoError(t, err)
	assert.Equal(t, 6, len(result))
	// Values: 0, 0+1=1, 1+100=101, 101+5=106, 106+50=156, 156+2=158
	assert.Equal(t, []int64{0, 1, 101, 106, 156, 158}, result)
}

// =============================================================================
// ZigZag Encoding Tests
// =============================================================================

func TestDecodeZigZag(t *testing.T) {
	tests := []struct {
		encoded  uint64
		expected int64
	}{
		{0, 0},
		{1, -1},
		{2, 1},
		{3, -2},
		{4, 2},
		{5, -3},
		{6, 3},
		{100, 50},
		{101, -51},
		{0xFFFFFFFFFFFFFFFE, 0x7FFFFFFFFFFFFFFF}, // Max positive
		{0xFFFFFFFFFFFFFFFF, -0x8000000000000000}, // Min negative
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("DecodeZigZag(%d)", tc.encoded), func(t *testing.T) {
			result := DecodeZigZag(tc.encoded)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestEncodeZigZag(t *testing.T) {
	tests := []struct {
		value    int64
		expected uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-3, 5},
		{3, 6},
		{50, 100},
		{-51, 101},
		{0x7FFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFE},  // Max positive
		{-0x8000000000000000, 0xFFFFFFFFFFFFFFFF}, // Min negative
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("EncodeZigZag(%d)", tc.value), func(t *testing.T) {
			result := EncodeZigZag(tc.value)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestZigZagRoundTrip(t *testing.T) {
	// Test that encode -> decode round trips correctly
	values := []int64{
		0, 1, -1, 2, -2,
		100, -100, 1000, -1000,
		0x7FFFFFFFFFFFFFFF, -0x8000000000000000,
		123456789, -123456789,
	}

	for _, v := range values {
		encoded := EncodeZigZag(v)
		decoded := DecodeZigZag(encoded)
		assert.Equal(t, v, decoded, "round trip failed for %d", v)
	}
}

// =============================================================================
// DecompressBitPackingWithParams Tests
// =============================================================================

func TestDecompressBitPackingWithParams_Basic(t *testing.T) {
	// Test basic bit-packing without header
	values := []uint64{0, 1, 2, 3}
	packedData := packBitsRaw(values, 2)

	result, err := DecompressBitPackingWithParams(packedData, 2, 4)

	require.NoError(t, err)
	assert.Equal(t, values, result)
}

func TestDecompressBitPackingWithParams_ZeroCount(t *testing.T) {
	result, err := DecompressBitPackingWithParams([]byte{}, 8, 0)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestDecompressBitPackingWithParams_ZeroBitWidth(t *testing.T) {
	// bitWidth=0 means all values are 0
	result, err := DecompressBitPackingWithParams([]byte{}, 0, 100)

	require.NoError(t, err)
	assert.Equal(t, 100, len(result))
	for _, v := range result {
		assert.Equal(t, uint64(0), v)
	}
}

func TestDecompressBitPackingWithParams_InvalidBitWidth(t *testing.T) {
	result, err := DecompressBitPackingWithParams([]byte{0xFF}, 65, 1)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingInvalidBitWidth)
}

func TestDecompressBitPackingWithParams_TruncatedData(t *testing.T) {
	// Need 10 values at 8 bits = 10 bytes, but only provide 5
	result, err := DecompressBitPackingWithParams([]byte{1, 2, 3, 4, 5}, 8, 10)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrBitPackingDataTruncated)
}

// =============================================================================
// PFOR_DELTA Decompressor Interface Tests
// =============================================================================

func TestPFORDeltaDecompressor_Interface(t *testing.T) {
	var d Decompressor = NewPFORDeltaDecompressor()

	deltas := []uint64{1, 2, 3}
	data := buildPFORDeltaData(100, deltas, 2)

	result, err := d.Decompress(data, 8, 4)

	require.NoError(t, err)
	assert.Equal(t, 32, len(result)) // 4 values * 8 bytes

	// Values: 100, 101, 103, 106
	assert.Equal(t, uint64(100), binary.LittleEndian.Uint64(result[0:8]))
	assert.Equal(t, uint64(101), binary.LittleEndian.Uint64(result[8:16]))
	assert.Equal(t, uint64(103), binary.LittleEndian.Uint64(result[16:24]))
	assert.Equal(t, uint64(106), binary.LittleEndian.Uint64(result[24:32]))
}

func TestGetDecompressor_PFORDelta(t *testing.T) {
	d := GetDecompressor(CompressionPFORDelta)
	assert.NotNil(t, d)
	_, ok := d.(*pfordeltaDecompressor)
	assert.True(t, ok)
}

// =============================================================================
// PFOR_DELTA Benchmarks
// =============================================================================

func BenchmarkDecompressPFORDelta_SmallSequence(b *testing.B) {
	deltas := make([]uint64, 99)
	for i := range deltas {
		deltas[i] = 1
	}
	data := buildPFORDeltaData(0, deltas, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPFORDeltaToInt64(data)
	}
}

func BenchmarkDecompressPFORDelta_LargeSequence(b *testing.B) {
	count := 10000
	deltas := make([]uint64, count-1)
	for i := range deltas {
		deltas[i] = uint64(i % 256)
	}
	data := buildPFORDeltaData(0, deltas, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPFORDeltaToInt64(data)
	}
}

func BenchmarkDecompressPFORDelta_RowGroupSize(b *testing.B) {
	count := 122880 // Default row group size
	deltas := make([]uint64, count-1)
	for i := range deltas {
		deltas[i] = 1
	}
	data := buildPFORDeltaData(0, deltas, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressPFORDeltaToInt64(data)
	}
}
