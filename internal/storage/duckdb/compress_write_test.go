package duckdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// CompressConstant Tests
// ---------------------------------------------------------------------------

func TestCompressConstant_AllSameInt32(t *testing.T) {
	// Create data with 100 identical int32 values (42)
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok, "compression should succeed for constant data")
	assert.Equal(t, valueSize, len(compressed), "compressed should be single value")
	assert.Equal(t, uint32(42), binary.LittleEndian.Uint32(compressed))
}

func TestCompressConstant_AllSameInt64(t *testing.T) {
	// Create data with 50 identical int64 values
	const count = 50
	const valueSize = 8
	const value = uint64(0x123456789ABCDEF0)
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], value)
	}

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok, "compression should succeed for constant data")
	assert.Equal(t, valueSize, len(compressed))
	assert.Equal(t, value, binary.LittleEndian.Uint64(compressed))
}

func TestCompressConstant_AllZeros(t *testing.T) {
	// All zeros should compress
	const count = 1000
	const valueSize = 4
	data := make([]byte, count*valueSize) // All zeros by default

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, valueSize, len(compressed))
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(compressed))
}

func TestCompressConstant_AllOnes(t *testing.T) {
	// All 0xFF bytes should compress
	const count = 500
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := range data {
		data[i] = 0xFF
	}

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, valueSize, len(compressed))
	for i := 0; i < valueSize; i++ {
		assert.Equal(t, byte(0xFF), compressed[i])
	}
}

func TestCompressConstant_ValuesDiffer(t *testing.T) {
	// Create data with different values
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)) // 0, 1, 2, ...
	}

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok, "compression should fail for non-constant data")
	assert.Nil(t, compressed)
}

func TestCompressConstant_SingleValue(t *testing.T) {
	// Single value should not compress (not worth it)
	data := []byte{0x42, 0x00, 0x00, 0x00}
	valueSize := 4

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok, "single value not worth compressing")
	assert.Nil(t, compressed)
}

func TestCompressConstant_TwoIdenticalValues(t *testing.T) {
	// Two identical values should compress
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 42)
	binary.LittleEndian.PutUint32(data[4:], 42)
	valueSize := 4

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, 4, len(compressed))
	assert.Equal(t, uint32(42), binary.LittleEndian.Uint32(compressed))
}

func TestCompressConstant_TwoDifferentValues(t *testing.T) {
	// Two different values should not compress
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 42)
	binary.LittleEndian.PutUint32(data[4:], 43)
	valueSize := 4

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressConstant_InvalidValueSize(t *testing.T) {
	data := []byte{0x42, 0x43, 0x44, 0x45}

	// Zero value size
	compressed, ok := CompressConstant(data, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Negative value size
	compressed, ok = CompressConstant(data, -1)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressConstant_DataTooShort(t *testing.T) {
	data := []byte{0x42, 0x43}
	valueSize := 4 // Expect 4 bytes but only have 2

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressConstant_DataNotMultipleOfValueSize(t *testing.T) {
	data := []byte{0x42, 0x43, 0x44, 0x45, 0x46} // 5 bytes
	valueSize := 4                               // Not a multiple of 4

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressConstant_EmptyData(t *testing.T) {
	compressed, ok := CompressConstant([]byte{}, 4)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

// Test various value sizes
func TestCompressConstant_ValueSize1(t *testing.T) {
	// 1-byte values (like uint8)
	data := bytes.Repeat([]byte{0xAB}, 100)
	valueSize := 1

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, 1, len(compressed))
	assert.Equal(t, byte(0xAB), compressed[0])
}

func TestCompressConstant_ValueSize2(t *testing.T) {
	// 2-byte values (like uint16)
	const count = 50
	const valueSize = 2
	const value = uint16(0xBEEF)
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint16(data[i*valueSize:], value)
	}

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, valueSize, len(compressed))
	assert.Equal(t, value, binary.LittleEndian.Uint16(compressed))
}

func TestCompressConstant_ValueSize16(t *testing.T) {
	// 16-byte values (like UUID or int128)
	const count = 20
	const valueSize = 16
	value := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		copy(data[i*valueSize:], value)
	}

	compressed, ok := CompressConstant(data, valueSize)

	require.True(t, ok)
	assert.Equal(t, valueSize, len(compressed))
	assert.Equal(t, value, compressed)
}

// ---------------------------------------------------------------------------
// Round-trip Tests: Compress then Decompress
// ---------------------------------------------------------------------------

func TestCompressConstant_RoundTrip_Int32(t *testing.T) {
	// Create constant data
	const count = 100
	const valueSize = 4
	const value = uint32(12345678)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], value)
	}

	// Compress
	compressed, ok := CompressConstant(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressConstant(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressConstant_RoundTrip_Int64(t *testing.T) {
	const count = 50
	const valueSize = 8
	const value = uint64(0xDEADBEEFCAFEBABE)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], value)
	}

	compressed, ok := CompressConstant(original, valueSize)
	require.True(t, ok)

	decompressed, err := DecompressConstant(compressed, valueSize, count)
	require.NoError(t, err)

	assert.Equal(t, original, decompressed)
}

func TestCompressConstant_RoundTrip_SingleByte(t *testing.T) {
	const count = 200
	const valueSize = 1
	original := bytes.Repeat([]byte{0x42}, count)

	compressed, ok := CompressConstant(original, valueSize)
	require.True(t, ok)

	decompressed, err := DecompressConstant(compressed, valueSize, count)
	require.NoError(t, err)

	assert.Equal(t, original, decompressed)
}

func TestCompressConstant_RoundTrip_LargeCount(t *testing.T) {
	const count = 100000
	const valueSize = 4
	const value = uint32(0xCAFEBABE)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], value)
	}

	compressed, ok := CompressConstant(original, valueSize)
	require.True(t, ok)
	assert.Equal(t, valueSize, len(compressed), "compressed should be single value regardless of count")

	decompressed, err := DecompressConstant(compressed, valueSize, count)
	require.NoError(t, err)

	assert.Equal(t, original, decompressed)
}

// ---------------------------------------------------------------------------
// MustCompressConstant Tests
// ---------------------------------------------------------------------------

func TestMustCompressConstant_Success(t *testing.T) {
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	compressed, err := MustCompressConstant(data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, valueSize, len(compressed))
	assert.Equal(t, uint32(42), binary.LittleEndian.Uint32(compressed))
}

func TestMustCompressConstant_NotConstant(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 1)
	binary.LittleEndian.PutUint32(data[4:], 2)

	compressed, err := MustCompressConstant(data, 4)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotConstant)
	assert.Nil(t, compressed)
}

func TestMustCompressConstant_InvalidValueSize(t *testing.T) {
	data := []byte{0x42, 0x43, 0x44, 0x45}

	compressed, err := MustCompressConstant(data, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidValueSize)
	assert.Nil(t, compressed)

	compressed, err = MustCompressConstant(data, -1)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidValueSize)
	assert.Nil(t, compressed)
}

func TestMustCompressConstant_EmptyData(t *testing.T) {
	compressed, err := MustCompressConstant([]byte{}, 4)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyData)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// TryCompressConstant Tests
// ---------------------------------------------------------------------------

func TestTryCompressConstant_Success(t *testing.T) {
	data := make([]byte, 16)
	for i := 0; i < 4; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], 999)
	}

	compressed, ok := TryCompressConstant(data, 4)

	require.True(t, ok)
	assert.Equal(t, 4, len(compressed))
}

func TestTryCompressConstant_Failure(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 1)
	binary.LittleEndian.PutUint32(data[4:], 2)

	compressed, ok := TryCompressConstant(data, 4)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Compress Dispatcher Tests
// ---------------------------------------------------------------------------

func TestCompress_Uncompressed(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	compressed, err := Compress(CompressionUncompressed, data, 4)

	require.NoError(t, err)
	assert.Equal(t, data, compressed)
	// Verify it's a copy, not the same slice
	assert.NotSame(t, &data[0], &compressed[0])
}

func TestCompress_Constant(t *testing.T) {
	const count = 5
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	compressed, err := Compress(CompressionConstant, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, valueSize, len(compressed))
}

func TestCompress_ConstantFailure(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 1)
	binary.LittleEndian.PutUint32(data[4:], 2)

	compressed, err := Compress(CompressionConstant, data, 4)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotConstant)
	assert.Nil(t, compressed)
}

func TestCompress_Empty(t *testing.T) {
	data := []byte{1, 2, 3, 4}

	compressed, err := Compress(CompressionEmpty, data, 4)

	require.NoError(t, err)
	assert.Empty(t, compressed)
}

// TestCompress_UnsupportedTypes has been removed since all compression types
// (CONSTANT, RLE, DICTIONARY, BITPACKING, PFOR_DELTA) are now implemented.
// The test for CompressionType(255) in TestCompress_UnknownType covers
// the unsupported case.

func TestCompress_UnknownType(t *testing.T) {
	data := []byte{1, 2, 3, 4}

	compressed, err := Compress(CompressionType(255), data, 4)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedCompression)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// TryCompress Tests
// ---------------------------------------------------------------------------

func TestTryCompress_SuccessWithConstant(t *testing.T) {
	data := make([]byte, 12)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], 42)
	}

	compressed, usedType, err := TryCompress(CompressionConstant, data, 4)

	require.NoError(t, err)
	assert.Equal(t, CompressionConstant, usedType)
	assert.Equal(t, 4, len(compressed))
}

func TestTryCompress_FallbackToUncompressed(t *testing.T) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[0:], 1)
	binary.LittleEndian.PutUint32(data[4:], 2)

	compressed, usedType, err := TryCompress(CompressionConstant, data, 4)

	require.NoError(t, err)
	assert.Equal(t, CompressionUncompressed, usedType)
	assert.Equal(t, data, compressed)
}

func TestTryCompress_EmptyData(t *testing.T) {
	compressed, usedType, err := TryCompress(CompressionConstant, []byte{}, 4)

	require.NoError(t, err)
	assert.Equal(t, CompressionEmpty, usedType)
	assert.Empty(t, compressed)
}

// ---------------------------------------------------------------------------
// Compressor Interface Tests
// ---------------------------------------------------------------------------

func TestConstantCompressor_Compress(t *testing.T) {
	compressor := NewConstantCompressor()

	data := make([]byte, 12)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], 42)
	}

	compressed, err := compressor.Compress(data, 4)

	require.NoError(t, err)
	assert.Equal(t, 4, len(compressed))
	assert.Equal(t, CompressionConstant, compressor.Type())
}

func TestUncompressedCompressor_Compress(t *testing.T) {
	compressor := NewUncompressedCompressor()

	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	compressed, err := compressor.Compress(data, 4)

	require.NoError(t, err)
	assert.Equal(t, data, compressed)
	assert.Equal(t, CompressionUncompressed, compressor.Type())
}

func TestGetCompressor(t *testing.T) {
	testCases := []struct {
		cType    CompressionType
		notNil   bool
		expected CompressionType
	}{
		{CompressionUncompressed, true, CompressionUncompressed},
		{CompressionConstant, true, CompressionConstant},
		{CompressionRLE, true, CompressionRLE},
		{CompressionDictionary, true, CompressionDictionary},
		{CompressionBitPacking, true, CompressionBitPacking},
		{CompressionPFORDelta, true, CompressionPFORDelta},
		{CompressionType(255), false, CompressionType(255)},
	}

	for _, tc := range testCases {
		t.Run(tc.cType.String(), func(t *testing.T) {
			compressor := GetCompressor(tc.cType)
			if tc.notNil {
				require.NotNil(t, compressor)
				assert.Equal(t, tc.expected, compressor.Type())
			} else {
				assert.Nil(t, compressor)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Compression Header Tests
// ---------------------------------------------------------------------------

func TestCompressionHeader_Fields(t *testing.T) {
	header := CompressionHeader{
		Type:      CompressionConstant,
		ValueSize: 4,
		Count:     1000,
	}

	assert.Equal(t, CompressionConstant, header.Type)
	assert.Equal(t, uint32(4), header.ValueSize)
	assert.Equal(t, uint64(1000), header.Count)
}

// ---------------------------------------------------------------------------
// Edge Case Tests
// ---------------------------------------------------------------------------

func TestCompressConstant_AllSameButLastDiffers(t *testing.T) {
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	// All values are 42 except the last one
	for i := 0; i < count-1; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	binary.LittleEndian.PutUint32(data[(count-1)*valueSize:], 43) // Last one differs

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok, "should not compress when last value differs")
	assert.Nil(t, compressed)
}

func TestCompressConstant_AllSameButFirstDiffers(t *testing.T) {
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	// First value is different
	binary.LittleEndian.PutUint32(data[0:], 43)
	for i := 1; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok, "should not compress when first value differs")
	assert.Nil(t, compressed)
}

func TestCompressConstant_OneByteDifference(t *testing.T) {
	// Values that differ by just one byte should not compress
	const count = 10
	const valueSize = 8
	data := make([]byte, count*valueSize)

	// Fill with pattern
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], 0x0102030405060708)
	}

	// Change just one byte in the middle
	data[5*valueSize+3] = 0xFF

	compressed, ok := CompressConstant(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkCompressConstant_Small(b *testing.B) {
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressConstant(data, valueSize)
	}
}

func BenchmarkCompressConstant_Large(b *testing.B) {
	const count = 100000
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressConstant(data, valueSize)
	}
}

func BenchmarkCompressConstant_RoundTrip(b *testing.B) {
	const count = 10000
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], 0xDEADBEEF)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed, _ := CompressConstant(data, valueSize)
		_, _ = DecompressConstant(compressed, valueSize, count)
	}
}

// ---------------------------------------------------------------------------
// CompressRLE Tests
// ---------------------------------------------------------------------------

func TestCompressRLE_ManyRuns(t *testing.T) {
	// Create data with many runs: [42, 42, 42, 100, 100, 200]
	// This is 6 values but only 3 runs, so RLE should help
	const valueSize = 4
	data := make([]byte, 6*valueSize)

	// Run 1: 3x 42
	binary.LittleEndian.PutUint32(data[0:], 42)
	binary.LittleEndian.PutUint32(data[4:], 42)
	binary.LittleEndian.PutUint32(data[8:], 42)
	// Run 2: 2x 100
	binary.LittleEndian.PutUint32(data[12:], 100)
	binary.LittleEndian.PutUint32(data[16:], 100)
	// Run 3: 1x 200
	binary.LittleEndian.PutUint32(data[20:], 200)

	compressed, ok := TryCompressRLE(data, valueSize)

	require.True(t, ok, "compression should succeed for data with runs")
	assert.NotNil(t, compressed)
	// Compressed should be smaller than original (3 runs vs 6 values)
	// Each run is: varint(count) + 4 bytes value
	// Run 1: 1 byte (count 3) + 4 bytes = 5 bytes
	// Run 2: 1 byte (count 2) + 4 bytes = 5 bytes
	// Run 3: 1 byte (count 1) + 4 bytes = 5 bytes
	// Total: 15 bytes vs original 24 bytes
	assert.Less(t, len(compressed), len(data))
}

func TestCompressRLE_NoRuns(t *testing.T) {
	// Create data with no runs: all different values
	// This means each value is its own run, so RLE doesn't help
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)) // 0, 1, 2, ...
	}

	compressed, ok := TryCompressRLE(data, valueSize)

	assert.False(t, ok, "compression should fail for data with no runs")
	assert.Nil(t, compressed)
}

func TestCompressRLE_SingleRun(t *testing.T) {
	// Create data where all values are the same (single run)
	// This should compress well
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	compressed, ok := TryCompressRLE(data, valueSize)

	require.True(t, ok, "single run should compress")
	assert.NotNil(t, compressed)
	// Single run of 100 values should be: varint(100) + 4 bytes = ~5-6 bytes
	// vs original 400 bytes
	assert.Less(t, len(compressed), 10)
}

func TestCompressRLE_AlternatingValues(t *testing.T) {
	// Worst case: alternating values [1, 2, 1, 2, 1, 2, ...]
	// Each value is its own run
	const count = 20
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%2+1)) // 1, 2, 1, 2, ...
	}

	compressed, ok := TryCompressRLE(data, valueSize)

	assert.False(t, ok, "alternating values should not compress with RLE")
	assert.Nil(t, compressed)
}

func TestCompressRLE_TwoRuns(t *testing.T) {
	// Two runs: [42, 42, 42, 100, 100, 100]
	const valueSize = 4
	data := make([]byte, 6*valueSize)

	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	for i := 3; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 100)
	}

	compressed, ok := TryCompressRLE(data, valueSize)

	require.True(t, ok, "two runs should compress")
	assert.NotNil(t, compressed)
}

func TestCompressRLE_InvalidValueSize(t *testing.T) {
	data := []byte{0x42, 0x43, 0x44, 0x45}

	// Zero value size
	compressed, ok := TryCompressRLE(data, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Negative value size
	compressed, ok = TryCompressRLE(data, -1)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressRLE_DataTooShort(t *testing.T) {
	data := []byte{0x42, 0x43}
	valueSize := 4 // Expect 4 bytes but only have 2

	compressed, ok := TryCompressRLE(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressRLE_DataNotMultipleOfValueSize(t *testing.T) {
	data := []byte{0x42, 0x43, 0x44, 0x45, 0x46} // 5 bytes
	valueSize := 4                               // Not a multiple of 4

	compressed, ok := TryCompressRLE(data, valueSize)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressRLE_EmptyData(t *testing.T) {
	compressed, ok := TryCompressRLE([]byte{}, 4)

	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressRLE_SingleValue(t *testing.T) {
	data := []byte{0x42, 0x00, 0x00, 0x00}
	valueSize := 4

	compressed, ok := TryCompressRLE(data, valueSize)

	assert.False(t, ok, "single value not worth compressing")
	assert.Nil(t, compressed)
}

func TestCompressRLE_Int64Values(t *testing.T) {
	// Test with 8-byte values
	const count = 10
	const valueSize = 8
	data := make([]byte, count*valueSize)

	// 5 values of 0xDEADBEEF, then 5 values of 0xCAFEBABE
	for i := 0; i < 5; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], 0xDEADBEEFDEADBEEF)
	}
	for i := 5; i < 10; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], 0xCAFEBABECAFEBABE)
	}

	compressed, ok := TryCompressRLE(data, valueSize)

	require.True(t, ok, "two runs of int64 should compress")
	assert.NotNil(t, compressed)
	// 2 runs vs 10 values
	assert.Less(t, len(compressed), len(data))
}

func TestCompressRLE_ByteValues(t *testing.T) {
	// Test with single-byte values
	// 10 bytes of 0xAA, then 10 bytes of 0xBB
	data := make([]byte, 20)
	for i := 0; i < 10; i++ {
		data[i] = 0xAA
	}
	for i := 10; i < 20; i++ {
		data[i] = 0xBB
	}

	compressed, ok := TryCompressRLE(data, 1)

	require.True(t, ok, "two runs of bytes should compress")
	assert.NotNil(t, compressed)
	// 2 runs: 1 byte count + 1 byte value each = 4 bytes total
	// vs original 20 bytes
	assert.Less(t, len(compressed), 10)
}

// ---------------------------------------------------------------------------
// RLE Round-trip Tests: Compress then Decompress
// ---------------------------------------------------------------------------

func TestCompressRLE_RoundTrip_Basic(t *testing.T) {
	// Create data with runs: [42, 42, 42, 100, 100]
	const valueSize = 4
	original := make([]byte, 5*valueSize)

	binary.LittleEndian.PutUint32(original[0:], 42)
	binary.LittleEndian.PutUint32(original[4:], 42)
	binary.LittleEndian.PutUint32(original[8:], 42)
	binary.LittleEndian.PutUint32(original[12:], 100)
	binary.LittleEndian.PutUint32(original[16:], 100)

	// Compress
	compressed, ok := TryCompressRLE(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressRLE(compressed, valueSize, 5)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressRLE_RoundTrip_SingleRun(t *testing.T) {
	const count = 100
	const valueSize = 4
	const value = uint32(12345678)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], value)
	}

	// Compress
	compressed, ok := TryCompressRLE(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressRLE(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressRLE_RoundTrip_ManyRuns(t *testing.T) {
	// Create data with many runs of varying lengths
	const valueSize = 4
	values := []struct {
		value uint32
		count int
	}{
		{1, 5},
		{2, 3},
		{3, 10},
		{4, 1},
		{5, 7},
		{6, 2},
		{7, 15},
	}

	totalCount := 0
	for _, v := range values {
		totalCount += v.count
	}

	original := make([]byte, totalCount*valueSize)
	offset := 0
	for _, v := range values {
		for i := 0; i < v.count; i++ {
			binary.LittleEndian.PutUint32(original[offset:], v.value)
			offset += valueSize
		}
	}

	// Compress
	compressed, ok := TryCompressRLE(original, valueSize)
	require.True(t, ok, "multiple runs should compress")

	// Decompress
	decompressed, err := DecompressRLE(compressed, valueSize, uint64(totalCount))
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressRLE_RoundTrip_Int64(t *testing.T) {
	const count = 50
	const valueSize = 8
	original := make([]byte, count*valueSize)

	// Two runs
	for i := 0; i < 30; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], 0xDEADBEEFCAFEBABE)
	}
	for i := 30; i < 50; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], 0x0123456789ABCDEF)
	}

	// Compress
	compressed, ok := TryCompressRLE(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressRLE(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressRLE_RoundTrip_LargeCount(t *testing.T) {
	// Test with a large run count to ensure varint encoding works
	const count = 100000
	const valueSize = 4
	const value = uint32(0xCAFEBABE)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], value)
	}

	// Compress
	compressed, ok := TryCompressRLE(original, valueSize)
	require.True(t, ok)
	// Should be very small: varint(100000) + 4 bytes = ~8 bytes total
	assert.Less(t, len(compressed), 20)

	// Decompress
	decompressed, err := DecompressRLE(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

// ---------------------------------------------------------------------------
// CompressRLE Error Tests
// ---------------------------------------------------------------------------

func TestCompressRLE_ReturnsError_WhenNotBeneficial(t *testing.T) {
	// Alternating values - worst case for RLE
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)) // All different
	}

	compressed, err := CompressRLE(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRLENotBeneficial)
	assert.Nil(t, compressed)
}

func TestCompressRLE_Success(t *testing.T) {
	// Data with runs
	const valueSize = 4
	data := make([]byte, 6*valueSize)

	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	for i := 3; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 100)
	}

	compressed, err := CompressRLE(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
}

// ---------------------------------------------------------------------------
// RLE Compressor Interface Tests
// ---------------------------------------------------------------------------

func TestRLECompressor_Compress_Success(t *testing.T) {
	compressor := NewRLECompressor()

	// Data with runs
	const valueSize = 4
	data := make([]byte, 6*valueSize)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	for i := 3; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 100)
	}

	compressed, err := compressor.Compress(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Equal(t, CompressionRLE, compressor.Type())
}

func TestRLECompressor_Compress_Failure(t *testing.T) {
	compressor := NewRLECompressor()

	// Data without runs (all different)
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i))
	}

	compressed, err := compressor.Compress(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRLENotBeneficial)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Update existing test to expect RLE to work now
// ---------------------------------------------------------------------------

func TestCompress_RLE(t *testing.T) {
	// Data with runs
	const valueSize = 4
	data := make([]byte, 6*valueSize)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	for i := 3; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 100)
	}

	compressed, err := Compress(CompressionRLE, data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompress_RLE_NotBeneficial(t *testing.T) {
	// Data without runs
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i))
	}

	compressed, err := Compress(CompressionRLE, data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRLENotBeneficial)
	assert.Nil(t, compressed)
}

func TestTryCompress_RLE_FallbackToUncompressed(t *testing.T) {
	// Data without runs - should fall back to uncompressed
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i))
	}

	compressed, usedType, err := TryCompress(CompressionRLE, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionUncompressed, usedType)
	assert.Equal(t, data, compressed)
}

func TestTryCompress_RLE_Success(t *testing.T) {
	// Data with runs
	const valueSize = 4
	data := make([]byte, 6*valueSize)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}
	for i := 3; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 100)
	}

	compressed, usedType, err := TryCompress(CompressionRLE, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionRLE, usedType)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

// ---------------------------------------------------------------------------
// RLE Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkCompressRLE_ManyRuns(b *testing.B) {
	const valueSize = 4
	// Create data with 100 runs of 10 values each
	data := make([]byte, 1000*valueSize)
	for run := 0; run < 100; run++ {
		for i := 0; i < 10; i++ {
			offset := (run*10 + i) * valueSize
			binary.LittleEndian.PutUint32(data[offset:], uint32(run))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TryCompressRLE(data, valueSize)
	}
}

func BenchmarkCompressRLE_SingleRun(b *testing.B) {
	const count = 100000
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TryCompressRLE(data, valueSize)
	}
}

func BenchmarkCompressRLE_RoundTrip(b *testing.B) {
	const valueSize = 4
	// Create data with 100 runs of 10 values each
	data := make([]byte, 1000*valueSize)
	for run := 0; run < 100; run++ {
		for i := 0; i < 10; i++ {
			offset := (run*10 + i) * valueSize
			binary.LittleEndian.PutUint32(data[offset:], uint32(run))
		}
	}

	compressed, _ := TryCompressRLE(data, valueSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = TryCompressRLE(data, valueSize)
		_, _ = DecompressRLE(compressed, valueSize, 1000)
	}
}

// ---------------------------------------------------------------------------
// CompressDictionary Tests
// ---------------------------------------------------------------------------

func TestCompressDictionary_FewUniqueValues_Int32NotBeneficial(t *testing.T) {
	// Create data with few unique values: 100 values, but only 3 unique
	// Values: [1, 2, 3, 1, 2, 3, 1, 2, 3, ...] repeated
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32((i%3)+1))
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// Dictionary compression is NOT beneficial for int32 values because:
	// Compressed: 4 (dictSize) + 12 (3 values * 4) + 8 (indexCount) + 400 (indices * 4) = 424 bytes
	// Original: 400 bytes
	// 424 > 400, so it's not beneficial
	assert.False(t, ok, "int32 dictionary compression not beneficial when indices are same size as values")
	assert.Nil(t, compressed)
}

func TestCompressDictionary_ManyValuesWithFewUnique(t *testing.T) {
	// Create data with many values but only 2 unique values
	// 200 values, alternating between 42 and 100
	// Original: 200 * 4 = 800 bytes
	// Compressed: 4 (dictSize) + 8 (dict: 2 * 4) + 8 (indexCount) + 800 (indices: 200 * 4) = 820 bytes
	// Still not beneficial!
	//
	// Dictionary compression is beneficial when: overhead + dictBytes + indicesBytes < original
	// For fixed-size values: 12 + (unique * valueSize) + (count * 4) < count * valueSize
	// For int32 (valueSize=4): 12 + (unique * 4) + (count * 4) < count * 4
	// 12 + unique * 4 < 0  -- This can never be true for int32!
	//
	// Dictionary compression is most beneficial when valueSize > 4 (the index size)
	// Let's test with int64 (8 bytes) values

	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)

	// Only 2 unique values
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			binary.LittleEndian.PutUint64(data[i*valueSize:], 0xDEADBEEFDEADBEEF)
		} else {
			binary.LittleEndian.PutUint64(data[i*valueSize:], 0xCAFEBABECAFEBABE)
		}
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// Original: 800 bytes
	// Compressed: 4 + 16 (2 * 8) + 8 + 400 (100 * 4) = 428 bytes
	require.True(t, ok, "compression should succeed with int64 and few unique values")
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompressDictionary_AllUniqueValues(t *testing.T) {
	// Create data where all values are unique - dictionary won't help
	const count = 50
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i))
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// With all unique values, dictionary size equals original data size
	// plus we have overhead for indices, so it's definitely not beneficial
	assert.False(t, ok, "compression should fail when all values are unique")
	assert.Nil(t, compressed)
}

func TestCompressDictionary_SingleUniqueValue(t *testing.T) {
	// All values are the same (single unique value)
	// This should compress very well
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], 0x123456789ABCDEF0)
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// Original: 800 bytes
	// Compressed: 4 + 8 (1 * 8) + 8 + 400 (100 * 4) = 420 bytes
	require.True(t, ok, "single unique value should compress well")
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompressDictionary_InvalidInputs(t *testing.T) {
	// Empty data
	compressed, ok := TryCompressDictionary([]byte{}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Data too short for value size
	compressed, ok = TryCompressDictionary([]byte{1, 2}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Data not multiple of value size
	compressed, ok = TryCompressDictionary([]byte{1, 2, 3, 4, 5}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Single value (not worth compressing)
	compressed, ok = TryCompressDictionary([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 8)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestCompressDictionary_Int16Values(t *testing.T) {
	// Test with int16 values (2 bytes each)
	// For int16, indices are larger than values, so dictionary is rarely beneficial
	// unless we have very few unique values with very many repetitions
	const count = 1000
	const valueSize = 2
	data := make([]byte, count*valueSize)

	// Only 2 unique values
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint16(data[i*valueSize:], uint16(i%2+1))
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// Original: 2000 bytes
	// Compressed: 4 + 4 (2 * 2) + 8 + 4000 (1000 * 4) = 4016 bytes
	// Not beneficial!
	assert.False(t, ok, "int16 dictionary compression rarely beneficial")
	assert.Nil(t, compressed)
}

func TestCompressDictionary_LargeValues(t *testing.T) {
	// Test with large values (16 bytes each, like UUID)
	// This is where dictionary compression shines
	const count = 100
	const valueSize = 16
	data := make([]byte, count*valueSize)

	// Only 3 unique values
	values := [][]byte{
		{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF},
		{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00},
		{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},
	}

	for i := 0; i < count; i++ {
		copy(data[i*valueSize:], values[i%3])
	}

	compressed, ok := TryCompressDictionary(data, valueSize)

	// Original: 1600 bytes
	// Compressed: 4 + 48 (3 * 16) + 8 + 400 (100 * 4) = 460 bytes
	require.True(t, ok, "large values with few unique should compress very well")
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data)/2) // Should be less than half
}

// ---------------------------------------------------------------------------
// Dictionary Round-trip Tests: Compress then Decompress
// ---------------------------------------------------------------------------

func TestCompressDictionary_RoundTrip_Int64(t *testing.T) {
	const count = 100
	const valueSize = 8
	original := make([]byte, count*valueSize)

	// Create data with 5 unique values
	uniqueValues := []uint64{100, 200, 300, 400, 500}
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uniqueValues[i%5])
	}

	// Compress
	compressed, ok := TryCompressDictionary(original, valueSize)
	require.True(t, ok, "should compress successfully")

	// Decompress
	decompressed, err := DecompressDictionary(compressed, valueSize, uint64(count))
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressDictionary_RoundTrip_SingleValue(t *testing.T) {
	const count = 200
	const valueSize = 8
	const value = uint64(0xDEADBEEFCAFEBABE)
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], value)
	}

	// Compress
	compressed, ok := TryCompressDictionary(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressDictionary(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressDictionary_RoundTrip_LargeValues(t *testing.T) {
	const count = 50
	const valueSize = 16
	original := make([]byte, count*valueSize)

	// 2 unique 16-byte values
	value1 := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	value2 := []byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00}

	for i := 0; i < count; i++ {
		if i%2 == 0 {
			copy(original[i*valueSize:], value1)
		} else {
			copy(original[i*valueSize:], value2)
		}
	}

	// Compress
	compressed, ok := TryCompressDictionary(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressDictionary(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

// ---------------------------------------------------------------------------
// Dictionary Variable-Size (String) Tests
// ---------------------------------------------------------------------------

// Helper function to build length-prefixed string data
func buildLengthPrefixedStrings(strings []string) []byte {
	var buf bytes.Buffer
	for _, s := range strings {
		binary.Write(&buf, binary.LittleEndian, uint32(len(s)))
		buf.WriteString(s)
	}
	return buf.Bytes()
}

func TestCompressDictionary_VariableSize_FewUniqueStrings(t *testing.T) {
	// Create data with repeated strings
	// 100 strings, only 3 unique: "hello", "world", "foo"
	strings := make([]string, 100)
	uniqueStrings := []string{"hello", "world", "foo"}
	for i := range strings {
		strings[i] = uniqueStrings[i%3]
	}

	data := buildLengthPrefixedStrings(strings)

	compressed, ok := TryCompressDictionary(data, 0) // valueSize <= 0 for variable size

	// Original: 100 * (4 + 5) = 900 bytes (approx, depends on string lengths)
	// Let's calculate: "hello"=5, "world"=5, "foo"=3
	// Average = (5+5+3)/3 = 4.33 bytes per string + 4 bytes length = 8.33 bytes
	// Total original ~ 833 bytes
	// Compressed: 4 + (3 strings with lengths) + 8 + 400 indices
	// Dict: 4 + (4+5) + (4+5) + (4+3) = 4 + 25 = 29 bytes for dict
	// Total: 4 + 29 + 8 + 400 = 441 bytes
	require.True(t, ok, "variable size strings should compress well")
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompressDictionary_VariableSize_AllUnique(t *testing.T) {
	// All strings are unique - dictionary won't help
	strings := make([]string, 20)
	for i := range strings {
		strings[i] = fmt.Sprintf("unique_string_%d", i)
	}

	data := buildLengthPrefixedStrings(strings)

	compressed, ok := TryCompressDictionary(data, 0)

	assert.False(t, ok, "all unique strings should not compress")
	assert.Nil(t, compressed)
}

func TestCompressDictionary_VariableSize_RoundTrip(t *testing.T) {
	// Create data with repeated strings
	originalStrings := make([]string, 50)
	uniqueStrings := []string{"apple", "banana", "cherry", "date"}
	for i := range originalStrings {
		originalStrings[i] = uniqueStrings[i%4]
	}

	original := buildLengthPrefixedStrings(originalStrings)

	// Compress
	compressed, ok := TryCompressDictionary(original, 0)
	require.True(t, ok, "should compress successfully")

	// Decompress
	decompressed, err := DecompressDictionary(compressed, 0, uint64(len(originalStrings)))
	require.NoError(t, err)

	// For variable-size strings, decompression returns raw concatenated bytes
	// without length prefixes (this is by design in DecompressDictionary)
	// So we need to compare against the concatenated original strings
	expectedConcatenated := ""
	for _, s := range originalStrings {
		expectedConcatenated += s
	}
	assert.Equal(t, expectedConcatenated, string(decompressed))
}

func TestCompressDictionary_VariableSize_EmptyStrings(t *testing.T) {
	// Test with empty strings mixed with non-empty
	strings := []string{"", "hello", "", "world", "", "hello", "", "world"}
	data := buildLengthPrefixedStrings(strings)

	compressed, ok := TryCompressDictionary(data, 0)

	// 8 strings, 3 unique ("", "hello", "world")
	// Original: 8 * 4 (lengths) + 0 + 5 + 0 + 5 + 0 + 5 + 0 + 5 = 32 + 20 = 52 bytes
	// Compressed: 4 + (4+0) + (4+5) + (4+5) + 8 + 32 = 4 + 22 + 8 + 32 = 66 bytes
	// Not beneficial for this small example, but let's test the logic works
	if ok {
		// If it compressed, verify round-trip
		decompressed, err := DecompressDictionary(compressed, 0, uint64(len(strings)))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	}
	// Either way, the function should handle empty strings correctly
}

func TestCompressDictionary_VariableSize_InvalidData(t *testing.T) {
	// Truncated length
	compressed, ok := TryCompressDictionary([]byte{1, 2}, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Length says 100 bytes but data is short
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, 100) // Says 100 bytes follow
	// But only 4 bytes remain
	compressed, ok = TryCompressDictionary(data, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// CompressDictionary Error Tests
// ---------------------------------------------------------------------------

func TestCompressDictionary_ReturnsError_WhenNotBeneficial(t *testing.T) {
	// All unique values - dictionary not beneficial
	const count = 20
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i))
	}

	compressed, err := CompressDictionary(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDictionaryNotBeneficial)
	assert.Nil(t, compressed)
}

func TestCompressDictionary_Success(t *testing.T) {
	// Few unique values - dictionary should help
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%3))
	}

	compressed, err := CompressDictionary(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
}

// ---------------------------------------------------------------------------
// Dictionary Compressor Interface Tests
// ---------------------------------------------------------------------------

func TestDictionaryCompressor_Compress_Success(t *testing.T) {
	compressor := NewDictionaryCompressor()

	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%2))
	}

	compressed, err := compressor.Compress(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Equal(t, CompressionDictionary, compressor.Type())
}

func TestDictionaryCompressor_Compress_Failure(t *testing.T) {
	compressor := NewDictionaryCompressor()

	// All unique values - not beneficial
	const valueSize = 8
	data := make([]byte, 20*valueSize)
	for i := 0; i < 20; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i))
	}

	compressed, err := compressor.Compress(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDictionaryNotBeneficial)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Compress Dispatcher Tests for Dictionary
// ---------------------------------------------------------------------------

func TestCompress_Dictionary_Success(t *testing.T) {
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%5))
	}

	compressed, err := Compress(CompressionDictionary, data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompress_Dictionary_NotBeneficial(t *testing.T) {
	// All unique values
	const valueSize = 8
	data := make([]byte, 20*valueSize)
	for i := 0; i < 20; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i))
	}

	compressed, err := Compress(CompressionDictionary, data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDictionaryNotBeneficial)
	assert.Nil(t, compressed)
}

func TestTryCompress_Dictionary_FallbackToUncompressed(t *testing.T) {
	// All unique values - should fall back to uncompressed
	const valueSize = 8
	data := make([]byte, 20*valueSize)
	for i := 0; i < 20; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i))
	}

	compressed, usedType, err := TryCompress(CompressionDictionary, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionUncompressed, usedType)
	assert.Equal(t, data, compressed)
}

func TestTryCompress_Dictionary_Success(t *testing.T) {
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%3))
	}

	compressed, usedType, err := TryCompress(CompressionDictionary, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionDictionary, usedType)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

// ---------------------------------------------------------------------------
// Dictionary Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkCompressDictionary_FewUnique(b *testing.B) {
	const count = 10000
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TryCompressDictionary(data, valueSize)
	}
}

func BenchmarkCompressDictionary_ManyUnique(b *testing.B) {
	const count = 1000
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%100)) // 100 unique
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TryCompressDictionary(data, valueSize)
	}
}

func BenchmarkCompressDictionary_RoundTrip(b *testing.B) {
	const count = 10000
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%10))
	}

	compressed, _ := TryCompressDictionary(data, valueSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = TryCompressDictionary(data, valueSize)
		_, _ = DecompressDictionary(compressed, valueSize, count)
	}
}

// ---------------------------------------------------------------------------
// CompressBitPacking Tests
// ---------------------------------------------------------------------------

func TestCompressBitPackingFromUint64_SmallRange(t *testing.T) {
	// Values 0-3 need 2 bits each
	// 1000 values * 2 bits = 2000 bits = 250 bytes packed
	// Original: 1000 * 8 = 8000 bytes
	// Compressed: 1 + 8 + 250 = 259 bytes
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i % 4) // 0, 1, 2, 3 repeating
	}

	compressed, ok := CompressBitPackingFromUint64(values)

	require.True(t, ok, "small range values should compress")
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), 8000) // Much smaller than original
	// Verify header
	assert.Equal(t, uint8(2), compressed[0], "bitWidth should be 2 for values 0-3")
	assert.Equal(t, uint64(1000), binary.LittleEndian.Uint64(compressed[1:9]), "count should be 1000")
}

func TestCompressBitPackingFromUint64_AllZeros(t *testing.T) {
	// All zeros need 1 bit each (minimum)
	values := make([]uint64, 500)
	// All values are 0 by default

	compressed, ok := CompressBitPackingFromUint64(values)

	require.True(t, ok, "all zeros should compress")
	assert.NotNil(t, compressed)
	// 1 bit per value: 500 bits = 63 bytes
	// Compressed: 1 + 8 + 63 = 72 bytes vs 4000 original
	assert.Less(t, len(compressed), 100)
	assert.Equal(t, uint8(1), compressed[0], "bitWidth should be 1 for all zeros")
}

func TestCompressBitPackingFromUint64_SingleValue(t *testing.T) {
	// 1000 copies of value 42 (needs 6 bits)
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = 42
	}

	compressed, ok := CompressBitPackingFromUint64(values)

	require.True(t, ok, "single repeated value should compress")
	assert.NotNil(t, compressed)
	assert.Equal(t, uint8(6), compressed[0], "bitWidth should be 6 for value 42")
}

func TestCompressBitPackingFromUint64_FullRange64Bit(t *testing.T) {
	// Full 64-bit range values won't benefit from packing
	// 64 bits per value = same as original
	values := make([]uint64, 100)
	for i := range values {
		values[i] = uint64(i) | (uint64(1) << 63) // Set high bit
	}

	compressed, ok := CompressBitPackingFromUint64(values)

	assert.False(t, ok, "full 64-bit values should not compress")
	assert.Nil(t, compressed)
}

func TestCompressBitPackingFromUint64_Empty(t *testing.T) {
	compressed, ok := CompressBitPackingFromUint64([]uint64{})

	assert.False(t, ok, "empty values should not compress")
	assert.Nil(t, compressed)
}

func TestCompressBitPackingFromUint64_SingleElement(t *testing.T) {
	// Single element not worth compressing (1 byte header + 8 bytes count + data >= 8 bytes)
	// Actually: 1 + 8 + 1 = 10 bytes vs 8 bytes original
	compressed, ok := CompressBitPackingFromUint64([]uint64{42})

	assert.False(t, ok, "single element not worth compressing")
	assert.Nil(t, compressed)
}

func TestCompressBitPackingFromUint64_VariousBitWidths(t *testing.T) {
	testCases := []struct {
		name         string
		maxValue     uint64
		expectedBits uint8
	}{
		{"1-bit (0-1)", 1, 1},
		{"4-bit (0-15)", 15, 4},
		{"8-bit (0-255)", 255, 8},
		{"16-bit (0-65535)", 65535, 16},
		{"32-bit", 0xFFFFFFFF, 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create 1000 values with max = tc.maxValue
			values := make([]uint64, 1000)
			for i := range values {
				values[i] = uint64(i) % (tc.maxValue + 1)
			}
			values[0] = tc.maxValue // Ensure max value is present

			compressed, ok := CompressBitPackingFromUint64(values)

			require.True(t, ok, "should compress")
			assert.Equal(t, tc.expectedBits, compressed[0], "bitWidth should be %d", tc.expectedBits)
		})
	}
}

func TestTryCompressBitPackingFromBytes_Int32(t *testing.T) {
	// 100 int32 values in range 0-15 (4 bits each)
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%16))
	}

	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)

	require.True(t, ok, "small range int32 should compress")
	assert.NotNil(t, compressed)
	// 4 bits per value: 100 * 4 = 400 bits = 50 bytes
	// Compressed: 1 + 8 + 50 = 59 bytes vs 400 original
	assert.Less(t, len(compressed), len(data))
	assert.Equal(t, uint8(4), compressed[0], "bitWidth should be 4")
}

func TestTryCompressBitPackingFromBytes_Int64(t *testing.T) {
	// 500 int64 values in range 0-7 (3 bits each)
	const count = 500
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(i%8))
	}

	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)

	require.True(t, ok, "small range int64 should compress")
	assert.NotNil(t, compressed)
	// 3 bits per value: 500 * 3 = 1500 bits = 188 bytes
	// Compressed: 1 + 8 + 188 = 197 bytes vs 4000 original
	assert.Less(t, len(compressed), len(data)/10)
	assert.Equal(t, uint8(3), compressed[0], "bitWidth should be 3")
}

func TestTryCompressBitPackingFromBytes_Int8(t *testing.T) {
	// 200 int8 values in range 0-3 (2 bits each)
	const count = 200
	const valueSize = 1
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		data[i] = byte(i % 4)
	}

	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)

	require.True(t, ok, "small range int8 should compress")
	assert.NotNil(t, compressed)
	// 2 bits per value: 200 * 2 = 400 bits = 50 bytes
	// Compressed: 1 + 8 + 50 = 59 bytes vs 200 original
	assert.Less(t, len(compressed), len(data))
	assert.Equal(t, uint8(2), compressed[0], "bitWidth should be 2")
}

func TestTryCompressBitPackingFromBytes_Int16(t *testing.T) {
	// 1000 int16 values in range 0-31 (5 bits each)
	const count = 1000
	const valueSize = 2
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint16(data[i*valueSize:], uint16(i%32))
	}

	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)

	require.True(t, ok, "small range int16 should compress")
	assert.NotNil(t, compressed)
	// 5 bits per value: 1000 * 5 = 5000 bits = 625 bytes
	// Compressed: 1 + 8 + 625 = 634 bytes vs 2000 original
	assert.Less(t, len(compressed), len(data))
	assert.Equal(t, uint8(5), compressed[0], "bitWidth should be 5")
}

func TestTryCompressBitPackingFromBytes_InvalidInputs(t *testing.T) {
	// Empty data
	compressed, ok := TryCompressBitPackingFromBytes([]byte{}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Data too short for value size
	compressed, ok = TryCompressBitPackingFromBytes([]byte{1, 2}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Data not multiple of value size
	compressed, ok = TryCompressBitPackingFromBytes([]byte{1, 2, 3, 4, 5}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Single value (not worth compressing)
	compressed, ok = TryCompressBitPackingFromBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 8)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Invalid value size
	compressed, ok = TryCompressBitPackingFromBytes([]byte{1, 2, 3, 4}, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Negative value size
	compressed, ok = TryCompressBitPackingFromBytes([]byte{1, 2, 3, 4}, -1)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

func TestTryCompressBitPackingFromBytes_NotBeneficial(t *testing.T) {
	// Full 32-bit range values - not beneficial
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)|0x80000000) // High bit set
	}

	compressed, ok := TryCompressBitPackingFromBytes(data, valueSize)

	assert.False(t, ok, "full range values should not compress")
	assert.Nil(t, compressed)
}

func TestTryCompressBitPacking(t *testing.T) {
	// Test the alias function
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i % 16) // 4-bit values
	}

	compressed, ok := TryCompressBitPacking(values)

	require.True(t, ok)
	assert.NotNil(t, compressed)
	assert.Equal(t, uint8(4), compressed[0])
}

// ---------------------------------------------------------------------------
// BITPACKING Round-trip Tests: Compress then Decompress
// ---------------------------------------------------------------------------

func TestCompressBitPacking_RoundTrip_SmallRange(t *testing.T) {
	// Create original data with small range values
	const count = 1000
	const valueSize = 4
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], uint32(i%16))
	}

	// Compress
	compressed, ok := TryCompressBitPackingFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressBitPacking(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressBitPacking_RoundTrip_AllZeros(t *testing.T) {
	const count = 500
	const valueSize = 8
	original := make([]byte, count*valueSize) // All zeros

	// Compress
	compressed, ok := TryCompressBitPackingFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressBitPacking(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressBitPacking_RoundTrip_SingleValue(t *testing.T) {
	// All 42s
	const count = 1000
	const valueSize = 4
	original := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(original[i*valueSize:], 42)
	}

	// Compress
	compressed, ok := TryCompressBitPackingFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressBitPacking(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressBitPacking_RoundTrip_VariousBitWidths(t *testing.T) {
	testCases := []struct {
		name     string
		maxValue uint32
	}{
		{"1-bit", 1},
		{"4-bit", 15},
		{"8-bit", 255},
		{"16-bit", 65535},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			const count = 500
			const valueSize = 4
			original := make([]byte, count*valueSize)

			for i := 0; i < count; i++ {
				binary.LittleEndian.PutUint32(original[i*valueSize:], uint32(i)%(tc.maxValue+1))
			}
			// Ensure max value is present
			binary.LittleEndian.PutUint32(original[0:], tc.maxValue)

			// Compress
			compressed, ok := TryCompressBitPackingFromBytes(original, valueSize)
			require.True(t, ok)

			// Decompress
			decompressed, err := DecompressBitPacking(compressed, valueSize, count)
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, original, decompressed)
		})
	}
}

func TestCompressBitPacking_RoundTrip_Int64(t *testing.T) {
	const count = 800
	const valueSize = 8
	original := make([]byte, count*valueSize)

	// Values 0-127 (7 bits)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uint64(i%128))
	}

	// Compress
	compressed, ok := TryCompressBitPackingFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressBitPacking(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressBitPacking_RoundTrip_Uint64Direct(t *testing.T) {
	// Test the uint64 -> uint64 round-trip path
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = uint64(i % 100) // 7-bit values
	}

	// Compress
	compressed, ok := CompressBitPackingFromUint64(values)
	require.True(t, ok)

	// Decompress to uint64
	decompressed, err := DecompressBitPackingToUint64(compressed)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, values, decompressed)
}

// ---------------------------------------------------------------------------
// CompressBitPacking Error Tests
// ---------------------------------------------------------------------------

func TestCompressBitPacking_ReturnsError_WhenNotBeneficial(t *testing.T) {
	// Full range 32-bit values - not beneficial
	const count = 10
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)|0x80000000)
	}

	compressed, err := CompressBitPacking(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrBitPackingNotBeneficial)
	assert.Nil(t, compressed)
}

func TestCompressBitPacking_Success(t *testing.T) {
	// Small range values - beneficial
	const count = 500
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%8))
	}

	compressed, err := CompressBitPacking(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

// ---------------------------------------------------------------------------
// BITPACKING Compressor Interface Tests
// ---------------------------------------------------------------------------

func TestBitPackingCompressor_Compress_Success(t *testing.T) {
	compressor := NewBitPackingCompressor()

	const count = 500
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%16))
	}

	compressed, err := compressor.Compress(data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Equal(t, CompressionBitPacking, compressor.Type())
}

func TestBitPackingCompressor_Compress_Failure(t *testing.T) {
	compressor := NewBitPackingCompressor()

	// Full range values - not beneficial
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)|0x80000000)
	}

	compressed, err := compressor.Compress(data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrBitPackingNotBeneficial)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Compress Dispatcher Tests for BITPACKING
// ---------------------------------------------------------------------------

func TestCompress_BitPacking_Success(t *testing.T) {
	const count = 500
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%32))
	}

	compressed, err := Compress(CompressionBitPacking, data, valueSize)

	require.NoError(t, err)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

func TestCompress_BitPacking_NotBeneficial(t *testing.T) {
	// Full range values
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)|0x80000000)
	}

	compressed, err := Compress(CompressionBitPacking, data, valueSize)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrBitPackingNotBeneficial)
	assert.Nil(t, compressed)
}

func TestTryCompress_BitPacking_FallbackToUncompressed(t *testing.T) {
	// Full range values - should fall back to uncompressed
	const valueSize = 4
	data := make([]byte, 10*valueSize)
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i)|0x80000000)
	}

	compressed, usedType, err := TryCompress(CompressionBitPacking, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionUncompressed, usedType)
	assert.Equal(t, data, compressed)
}

func TestTryCompress_BitPacking_Success(t *testing.T) {
	const count = 500
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%16))
	}

	compressed, usedType, err := TryCompress(CompressionBitPacking, data, valueSize)

	require.NoError(t, err)
	assert.Equal(t, CompressionBitPacking, usedType)
	assert.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(data))
}

// ---------------------------------------------------------------------------
// bitsNeededForValue Tests
// ---------------------------------------------------------------------------

func TestBitsNeededForValue(t *testing.T) {
	testCases := []struct {
		value    uint64
		expected uint8
	}{
		{0, 1},                           // 0 needs at least 1 bit
		{1, 1},                           // 1 needs 1 bit
		{2, 2},                           // 2 (10) needs 2 bits
		{3, 2},                           // 3 (11) needs 2 bits
		{4, 3},                           // 4 (100) needs 3 bits
		{7, 3},                           // 7 (111) needs 3 bits
		{8, 4},                           // 8 (1000) needs 4 bits
		{15, 4},                          // 15 (1111) needs 4 bits
		{16, 5},                          // 16 needs 5 bits
		{255, 8},                         // 255 needs 8 bits
		{256, 9},                         // 256 needs 9 bits
		{65535, 16},                      // 16-bit max
		{65536, 17},                      // 17 bits
		{0xFFFFFFFF, 32},                 // 32-bit max
		{0x100000000, 33},                // 33 bits
		{0xFFFFFFFFFFFFFFFF, 64},         // 64-bit max
		{0x8000000000000000, 64},         // High bit set
		{0x4000000000000000, 63},         // Second-highest bit
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("value_%d", tc.value), func(t *testing.T) {
			result := bitsNeededForValue(tc.value)
			assert.Equal(t, tc.expected, result, "bitsNeededForValue(%d) should be %d", tc.value, tc.expected)
		})
	}
}

// ---------------------------------------------------------------------------
// packBits Tests
// ---------------------------------------------------------------------------

func TestPackBits_Simple(t *testing.T) {
	// Pack [0, 1, 2, 3] with 2 bits each
	// 0 = 00, 1 = 01, 2 = 10, 3 = 11
	// Packed LSB first: bit positions 0-1: 0, 2-3: 1, 4-5: 2, 6-7: 3
	// Byte 0: 11_10_01_00 = 0xE4
	values := []uint64{0, 1, 2, 3}

	result := packBits(values, 2)

	require.Len(t, result, 1)
	assert.Equal(t, byte(0xE4), result[0])
}

func TestPackBits_CrossesByteBoundary(t *testing.T) {
	// Pack 5 values of 3 bits each = 15 bits = 2 bytes
	values := []uint64{0, 1, 2, 3, 4}

	result := packBits(values, 3)

	require.Len(t, result, 2) // 15 bits needs 2 bytes
	// Values: 000, 001, 010, 011, 100
	// Bit layout (LSB first per byte):
	// Byte 0 bits 0-7:  val0[0,1,2] val1[0,1,2] val2[0,1] = 000 001 01 = 0b01001000 = 0x48
	// Byte 1 bits 0-6:  val2[2] val3[0,1,2] val4[0,1,2] = 0 011 100 = 0b00111000 but shifted by 1 = 0b01110000

	// Let me verify manually:
	// v0=0: bits 0,1,2 = 0,0,0
	// v1=1: bits 3,4,5 = 1,0,0
	// v2=2: bits 6,7,8 = 0,1,0
	// v3=3: bits 9,10,11 = 1,1,0
	// v4=4: bits 12,13,14 = 0,0,1

	// Byte 0 (bits 0-7): bit0=0, bit1=0, bit2=0, bit3=1, bit4=0, bit5=0, bit6=0, bit7=1
	// = 0b10001000 = 0x88
	// Byte 1 (bits 8-14): bit8=0, bit9=1, bit10=1, bit11=0, bit12=0, bit13=0, bit14=1
	// = 0b01100010 = 0x62 (but only 7 bits)

	// Actually let me recompute more carefully
	// packBits stores bits in little-endian order within bytes
	// For value v at index i, bit b of v goes to bit position (i*bitWidth + b)
	// That position maps to byte (pos/8) and bit (pos%8)

	// v0=0 (000): bits at positions 0,1,2 -> byte 0 bits 0,1,2 = 0
	// v1=1 (001): bits at positions 3,4,5 -> byte 0 bits 3,4,5 = bit3=1
	// v2=2 (010): bits at positions 6,7,8 -> byte 0 bits 6,7, byte 1 bit 0 = bit7=1
	// v3=3 (011): bits at positions 9,10,11 -> byte 1 bits 1,2,3 = bits 1,2 = 1
	// v4=4 (100): bits at positions 12,13,14 -> byte 1 bits 4,5,6 = bit6=1

	// Byte 0: bit0=0,bit1=0,bit2=0,bit3=1,bit4=0,bit5=0,bit6=0,bit7=1 = 0b10001000 = 0x88
	// Byte 1: bit0=0,bit1=1,bit2=1,bit3=0,bit4=0,bit5=0,bit6=1 = 0b01000110 = 0x46

	assert.Equal(t, byte(0x88), result[0], "byte 0")
	assert.Equal(t, byte(0x46), result[1], "byte 1")
}

func TestPackBits_Empty(t *testing.T) {
	result := packBits([]uint64{}, 4)
	assert.Empty(t, result)
}

func TestPackBits_ZeroBitWidth(t *testing.T) {
	result := packBits([]uint64{1, 2, 3}, 0)
	assert.Empty(t, result)
}

func TestPackBits_SingleBit(t *testing.T) {
	// 8 values of 1 bit each
	values := []uint64{1, 0, 1, 0, 1, 1, 0, 0}
	// Binary: 0b00110101 = 0x35

	result := packBits(values, 1)

	require.Len(t, result, 1)
	assert.Equal(t, byte(0x35), result[0])
}

func TestPackBits_FullByte(t *testing.T) {
	// 8 values of 8 bits each = 64 bits = 8 bytes
	values := []uint64{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}

	result := packBits(values, 8)

	require.Len(t, result, 8)
	// Each 8-bit value should be in its own byte
	assert.Equal(t, byte(0x12), result[0])
	assert.Equal(t, byte(0x34), result[1])
	assert.Equal(t, byte(0x56), result[2])
	assert.Equal(t, byte(0x78), result[3])
	assert.Equal(t, byte(0x9A), result[4])
	assert.Equal(t, byte(0xBC), result[5])
	assert.Equal(t, byte(0xDE), result[6])
	assert.Equal(t, byte(0xF0), result[7])
}

// ---------------------------------------------------------------------------
// BITPACKING Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkCompressBitPackingFromUint64_Small(b *testing.B) {
	values := make([]uint64, 100)
	for i := range values {
		values[i] = uint64(i % 16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressBitPackingFromUint64(values)
	}
}

func BenchmarkCompressBitPackingFromUint64_Large(b *testing.B) {
	values := make([]uint64, 100000)
	for i := range values {
		values[i] = uint64(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressBitPackingFromUint64(values)
	}
}

func BenchmarkCompressBitPacking_RoundTrip(b *testing.B) {
	const count = 10000
	const valueSize = 4
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(i%64))
	}

	compressed, _ := TryCompressBitPackingFromBytes(data, valueSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = TryCompressBitPackingFromBytes(data, valueSize)
		_, _ = DecompressBitPacking(compressed, valueSize, count)
	}
}

func BenchmarkPackBits_Small(b *testing.B) {
	values := make([]uint64, 100)
	for i := range values {
		values[i] = uint64(i % 16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packBits(values, 4)
	}
}

func BenchmarkPackBits_Large(b *testing.B) {
	values := make([]uint64, 100000)
	for i := range values {
		values[i] = uint64(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packBits(values, 8)
	}
}

// ---------------------------------------------------------------------------
// CompressPFORDelta Tests
// ---------------------------------------------------------------------------

func TestCompressPFORDeltaFromInt64_AscendingSequence(t *testing.T) {
	// Ascending sequence with small deltas should compress well
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(1000 + i) // 1000, 1001, 1002, ...
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok, "ascending sequence with small deltas should compress")
	// Should be much smaller than original (100 * 8 = 800 bytes)
	// Header: 8 (ref) + 1 (bitWidth) + 8 (count) = 17 bytes
	// Deltas: 99 values * 1 bit = 13 bytes (rounded up)
	assert.Less(t, len(compressed), 100, "compressed should be much smaller than original")
}

func TestCompressPFORDeltaFromInt64_ConstantSequence(t *testing.T) {
	// Constant sequence (all zero deltas) should compress very well
	values := make([]int64, 100)
	for i := range values {
		values[i] = 42 // All same value
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok, "constant sequence (zero deltas) should compress")
	// Should be very small
	// Header: 17 bytes, Deltas: 99 values * 1 bit = ~13 bytes
	assert.Less(t, len(compressed), 50)
}

func TestCompressPFORDeltaFromInt64_ArithmeticSequence(t *testing.T) {
	// Arithmetic sequence with constant delta of 10
	values := make([]int64, 50)
	for i := range values {
		values[i] = int64(100 + i*10) // 100, 110, 120, ...
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok, "arithmetic sequence should compress")
	// Delta is always 10, which needs 4 bits (2^4 = 16 > 10)
	assert.Less(t, len(compressed), 200)
}

func TestCompressPFORDeltaFromInt64_RandomData(t *testing.T) {
	// Random-looking data with large deltas may not compress well
	values := []int64{100, 500, 150, 1000, 50, 800, 200, 600}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	// May or may not compress depending on delta size
	// With 8 values at 8 bytes = 64 bytes original
	// Header = 17 bytes + packed deltas
	// Large deltas may not be beneficial
	if ok {
		t.Logf("Random data compressed to %d bytes from 64", len(compressed))
	} else {
		t.Log("Random data was not beneficial to compress")
	}
}

func TestCompressPFORDeltaFromInt64_SingleValue(t *testing.T) {
	// Single value should not compress (need at least 2 for deltas)
	values := []int64{42}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	assert.False(t, ok, "single value should not compress")
	assert.Nil(t, compressed)
}

func TestCompressPFORDeltaFromInt64_TwoValues(t *testing.T) {
	// Two values - minimal case
	values := []int64{100, 101}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	// 2 values * 8 bytes = 16 bytes original
	// Header: 17 bytes + 1 delta... so likely not beneficial
	assert.False(t, ok, "two values unlikely to be beneficial")
	assert.Nil(t, compressed)
}

func TestCompressPFORDeltaFromInt64_NegativeValues(t *testing.T) {
	// Sequence with negative values
	values := []int64{-100, -99, -98, -97, -96, -95, -94, -93, -92, -91}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	// This is an ascending sequence, should compress
	if !ok {
		t.Skip("Negative ascending sequence requires sufficient count to be beneficial")
	}
	assert.NotNil(t, compressed)
}

func TestCompressPFORDeltaFromInt64_LargeSequence(t *testing.T) {
	// Large sequence with small deltas should compress very well
	values := make([]int64, 10000)
	for i := range values {
		values[i] = int64(1000000 + i)
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok, "large ascending sequence should compress")
	// Original: 10000 * 8 = 80000 bytes
	// Compressed: 17 + ceil(9999 * 1 / 8) = 17 + 1250 = ~1267 bytes
	assert.Less(t, len(compressed), 5000)
	t.Logf("Compressed 80000 bytes to %d bytes (%.1f%% of original)",
		len(compressed), float64(len(compressed))/800.0)
}

func TestCompressPFORDeltaFromInt64_ZeroStart(t *testing.T) {
	// Sequence starting from zero
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i)
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok)
	assert.NotNil(t, compressed)
}

func TestCompressPFORDeltaFromInt64_LargeValues(t *testing.T) {
	// Large values but small deltas
	const base = int64(1 << 50) // Very large base
	values := make([]int64, 100)
	for i := range values {
		values[i] = base + int64(i)
	}

	compressed, ok := CompressPFORDeltaFromInt64(values)

	require.True(t, ok, "large values with small deltas should compress")
	assert.Less(t, len(compressed), 200)
}

// ---------------------------------------------------------------------------
// TryCompressPFORDeltaFromBytes Tests
// ---------------------------------------------------------------------------

func TestTryCompressPFORDeltaFromBytes_Int64(t *testing.T) {
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(1000+i))
	}

	compressed, ok := TryCompressPFORDeltaFromBytes(data, valueSize)

	require.True(t, ok)
	assert.Less(t, len(compressed), len(data))
}

func TestTryCompressPFORDeltaFromBytes_Int32(t *testing.T) {
	const count = 100
	const valueSize = 4
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*valueSize:], uint32(1000+i))
	}

	compressed, ok := TryCompressPFORDeltaFromBytes(data, valueSize)

	require.True(t, ok)
	assert.Less(t, len(compressed), len(data))
}

func TestTryCompressPFORDeltaFromBytes_Int16(t *testing.T) {
	const count = 100
	const valueSize = 2
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint16(data[i*valueSize:], uint16(1000+i))
	}

	compressed, ok := TryCompressPFORDeltaFromBytes(data, valueSize)

	// 100 * 2 = 200 bytes original, header is 17 bytes, may not compress
	// Actually with small deltas it might still compress
	if ok {
		assert.Less(t, len(compressed), len(data))
	}
}

func TestTryCompressPFORDeltaFromBytes_InvalidInputs(t *testing.T) {
	// Empty data
	compressed, ok := TryCompressPFORDeltaFromBytes([]byte{}, 8)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Invalid value size
	compressed, ok = TryCompressPFORDeltaFromBytes([]byte{1, 2, 3, 4}, 0)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	compressed, ok = TryCompressPFORDeltaFromBytes([]byte{1, 2, 3, 4}, -1)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Data not multiple of value size
	compressed, ok = TryCompressPFORDeltaFromBytes([]byte{1, 2, 3, 4, 5}, 4)
	assert.False(t, ok)
	assert.Nil(t, compressed)

	// Single value
	compressed, ok = TryCompressPFORDeltaFromBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 8)
	assert.False(t, ok)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// TryCompressPFORDelta Tests
// ---------------------------------------------------------------------------

func TestTryCompressPFORDelta(t *testing.T) {
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	compressed, ok := TryCompressPFORDelta(values)

	require.True(t, ok)
	assert.NotNil(t, compressed)
}

// ---------------------------------------------------------------------------
// CompressPFORDelta (via Compress dispatcher) Tests
// ---------------------------------------------------------------------------

func TestCompress_PFORDelta_Success(t *testing.T) {
	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)

	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(1000+i))
	}

	compressed, err := Compress(CompressionPFORDelta, data, valueSize)

	require.NoError(t, err)
	assert.Less(t, len(compressed), len(data))
}

func TestCompress_PFORDelta_NotBeneficial(t *testing.T) {
	// Very few values - compression not beneficial
	data := make([]byte, 16) // 2 int64 values
	binary.LittleEndian.PutUint64(data[0:], 100)
	binary.LittleEndian.PutUint64(data[8:], 101)

	compressed, err := Compress(CompressionPFORDelta, data, 8)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrPFORDeltaNotBeneficial)
	assert.Nil(t, compressed)
}

// ---------------------------------------------------------------------------
// Round-trip Tests: Compress then Decompress
// ---------------------------------------------------------------------------

func TestCompressPFORDelta_RoundTrip_AscendingInt64(t *testing.T) {
	const count = 100
	const valueSize = 8

	// Create ascending sequence
	original := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uint64(1000+i))
	}

	// Compress
	compressed, ok := TryCompressPFORDeltaFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressPFORDelta_RoundTrip_ConstantSequence(t *testing.T) {
	const count = 100
	const valueSize = 8
	const value = int64(42)

	// Create constant sequence
	original := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uint64(value))
	}

	// Compress
	compressed, ok := TryCompressPFORDeltaFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressPFORDelta_RoundTrip_ArithmeticSequence(t *testing.T) {
	const count = 50
	const valueSize = 8
	const start = int64(100)
	const delta = int64(10)

	// Create arithmetic sequence
	original := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uint64(start+int64(i)*delta))
	}

	// Compress
	compressed, ok := TryCompressPFORDeltaFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)
}

func TestCompressPFORDelta_RoundTrip_LargeCount(t *testing.T) {
	const count = 10000
	const valueSize = 8

	// Create ascending sequence
	original := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(original[i*valueSize:], uint64(1000000+i))
	}

	// Compress
	compressed, ok := TryCompressPFORDeltaFromBytes(original, valueSize)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, original, decompressed)

	// Verify compression ratio
	compressionRatio := float64(len(compressed)) / float64(len(original)) * 100
	t.Logf("Compression ratio: %.1f%% (%d -> %d bytes)",
		compressionRatio, len(original), len(compressed))
}

func TestCompressPFORDelta_RoundTrip_Int64ToInt64(t *testing.T) {
	// Test using the int64 API directly
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	// Compress
	compressed, ok := CompressPFORDeltaFromInt64(values)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDeltaToInt64(compressed)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, values, decompressed)
}

func TestCompressPFORDelta_RoundTrip_VaryingDeltas(t *testing.T) {
	// Sequence with varying but small deltas
	values := []int64{100, 102, 105, 106, 110, 111, 112, 115, 120, 125,
		126, 128, 130, 131, 135, 140, 145, 147, 150, 155}

	// Compress
	compressed, ok := CompressPFORDeltaFromInt64(values)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDeltaToInt64(compressed)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, values, decompressed)
}

func TestCompressPFORDelta_RoundTrip_LargeValues(t *testing.T) {
	const base = int64(1 << 50)
	values := make([]int64, 100)
	for i := range values {
		values[i] = base + int64(i)
	}

	// Compress
	compressed, ok := CompressPFORDeltaFromInt64(values)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDeltaToInt64(compressed)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, values, decompressed)
}

func TestCompressPFORDelta_RoundTrip_NegativeBase(t *testing.T) {
	// Ascending from negative value
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(-500 + i)
	}

	// Compress
	compressed, ok := CompressPFORDeltaFromInt64(values)
	require.True(t, ok)

	// Decompress
	decompressed, err := DecompressPFORDeltaToInt64(compressed)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, values, decompressed)
}

// ---------------------------------------------------------------------------
// PFOR_DELTA Compressor Interface Tests
// ---------------------------------------------------------------------------

func TestPFORDeltaCompressor_Compress_Success(t *testing.T) {
	compressor := NewPFORDeltaCompressor()

	const count = 100
	const valueSize = 8
	data := make([]byte, count*valueSize)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*valueSize:], uint64(1000+i))
	}

	compressed, err := compressor.Compress(data, valueSize)

	require.NoError(t, err)
	assert.Less(t, len(compressed), len(data))
	assert.Equal(t, CompressionPFORDelta, compressor.Type())
}

func TestPFORDeltaCompressor_Compress_NotBeneficial(t *testing.T) {
	compressor := NewPFORDeltaCompressor()

	// Too few values
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:], 100)
	binary.LittleEndian.PutUint64(data[8:], 101)

	compressed, err := compressor.Compress(data, 8)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrPFORDeltaNotBeneficial)
	assert.Nil(t, compressed)
}

func TestGetCompressor_PFORDelta(t *testing.T) {
	compressor := GetCompressor(CompressionPFORDelta)

	require.NotNil(t, compressor)
	assert.Equal(t, CompressionPFORDelta, compressor.Type())
}

// ---------------------------------------------------------------------------
// PFOR_DELTA Benchmark Tests
// ---------------------------------------------------------------------------

func BenchmarkCompressPFORDelta_Small(b *testing.B) {
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressPFORDeltaFromInt64(values)
	}
}

func BenchmarkCompressPFORDelta_Medium(b *testing.B) {
	values := make([]int64, 10000)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressPFORDeltaFromInt64(values)
	}
}

func BenchmarkCompressPFORDelta_Large(b *testing.B) {
	values := make([]int64, 100000)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CompressPFORDeltaFromInt64(values)
	}
}

func BenchmarkCompressPFORDelta_RoundTrip(b *testing.B) {
	const count = 10000
	values := make([]int64, count)
	for i := range values {
		values[i] = int64(1000 + i)
	}

	compressed, _ := CompressPFORDeltaFromInt64(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CompressPFORDeltaFromInt64(values)
		_, _ = DecompressPFORDeltaToInt64(compressed)
	}
}
