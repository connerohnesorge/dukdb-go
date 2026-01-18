package duckdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompressionRoundTrip_Constant verifies CONSTANT compression round-trip
// compatibility with DuckDB format.
//
// CONSTANT compression format:
// - Fixed-size types: Just the constant value (valueSize bytes)
// - VARCHAR: String length (uint32) + string data
func TestCompressionRoundTrip_Constant(t *testing.T) {
	t.Run("Int32_Constant", func(t *testing.T) {
		// Create constant int32 data: 100 copies of value 42
		const count = 100
		const value = int32(42)
		const valueSize = 4

		// Build original data (100 * 4 bytes)
		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], uint32(value))
		}

		// Compress using dukdb-go
		compressed, ok := CompressConstant(originalData, valueSize)
		require.True(t, ok, "compression should succeed")
		require.NotNil(t, compressed)

		// Verify compressed size (should be just valueSize bytes)
		assert.Equal(t, valueSize, len(compressed), "compressed data should be %d bytes", valueSize)

		// Verify compressed value matches original
		compressedValue := int32(binary.LittleEndian.Uint32(compressed))
		assert.Equal(t, value, compressedValue, "compressed value should match original")

		// Decompress and verify
		decompressed, err := DecompressConstant(compressed, valueSize, count)
		require.NoError(t, err, "decompression should succeed")

		// Verify decompressed data matches original
		assert.Equal(
			t,
			len(originalData),
			len(decompressed),
			"decompressed size should match original",
		)
		assert.Equal(t, originalData, decompressed, "decompressed data should match original")

		// Verify all values are correct
		for i := 0; i < count; i++ {
			offset := i * valueSize
			decompVal := int32(binary.LittleEndian.Uint32(decompressed[offset : offset+valueSize]))
			assert.Equal(t, value, decompVal, "value at index %d should match", i)
		}
	})

	t.Run("Int64_Constant", func(t *testing.T) {
		// Create constant int64 data: 50 copies of value 9999
		const count = 50
		const value = int64(9999)
		const valueSize = 8

		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(originalData[i*valueSize:], uint64(value))
		}

		// Compress
		compressed, ok := CompressConstant(originalData, valueSize)
		require.True(t, ok, "compression should succeed")
		assert.Equal(t, valueSize, len(compressed))

		// Decompress and verify
		decompressed, err := DecompressConstant(compressed, valueSize, count)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("SingleByte_Constant", func(t *testing.T) {
		// Create constant byte data: 200 copies of value 0xFF
		const count = 200
		const value = byte(0xFF)
		const valueSize = 1

		originalData := make([]byte, count)
		for i := 0; i < count; i++ {
			originalData[i] = value
		}

		// Compress
		compressed, ok := CompressConstant(originalData, valueSize)
		require.True(t, ok)
		assert.Equal(t, 1, len(compressed))
		assert.Equal(t, value, compressed[0])

		// Decompress
		decompressed, err := DecompressConstant(compressed, valueSize, count)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})
}

// TestCompressionRoundTrip_RLE verifies RLE compression round-trip
// compatibility with DuckDB format.
//
// DuckDB RLE format:
// - Bytes 0-7: metadata_offset (uint64) - offset to metadata
// - Bytes 8+: unique values (type-sized)
// - Metadata at metadata_offset: run lengths (uint16 each)
func TestCompressionRoundTrip_RLE(t *testing.T) {
	t.Run("Int32_SimpleRuns", func(t *testing.T) {
		// Create data with runs: [42, 42, 42, 100, 100, 999, 999, 999, 999]
		const valueSize = 4
		originalData := make([]byte, 9*valueSize)

		// 3x value 42
		for i := 0; i < 3; i++ {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], 42)
		}
		// 2x value 100
		for i := 3; i < 5; i++ {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], 100)
		}
		// 4x value 999
		for i := 5; i < 9; i++ {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], 999)
		}

		// Compress
		compressed, ok := TryCompressRLE(originalData, valueSize)
		require.True(t, ok, "RLE compression should succeed")
		require.NotNil(t, compressed)

		// Verify format: should have metadata_offset header
		require.GreaterOrEqual(t, len(compressed), 8, "should have at least 8-byte header")
		metadataOffset := binary.LittleEndian.Uint64(compressed[0:8])

		// Metadata offset should be within the data
		assert.LessOrEqual(t, metadataOffset, uint64(len(compressed)))
		assert.GreaterOrEqual(
			t,
			metadataOffset,
			uint64(8),
			"metadata offset should be after header",
		)

		// Decompress
		decompressed, err := DecompressRLE(compressed, valueSize, 9)
		require.NoError(t, err, "decompression should succeed")

		// Verify decompressed data matches original
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("Int64_ManyRuns", func(t *testing.T) {
		// Create data with alternating values
		const valueSize = 8
		const numPairs = 20
		originalData := make([]byte, numPairs*2*valueSize)

		for i := 0; i < numPairs; i++ {
			// Two copies of value i*10
			binary.LittleEndian.PutUint64(originalData[(i*2)*valueSize:], uint64(i*10))
			binary.LittleEndian.PutUint64(originalData[(i*2+1)*valueSize:], uint64(i*10))
		}

		// Compress
		compressed, ok := TryCompressRLE(originalData, valueSize)
		require.True(t, ok)

		// Decompress
		decompressed, err := DecompressRLE(compressed, valueSize, numPairs*2)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("SingleByte_Runs", func(t *testing.T) {
		// Create data: [1, 1, 1, 0, 0, 1, 1, 1, 1] repeated to make compression beneficial
		pattern := []byte{1, 1, 1, 0, 0, 1, 1, 1, 1}
		// Repeat pattern 10 times to ensure RLE compression is beneficial
		originalData := make([]byte, len(pattern)*10)
		for i := 0; i < 10; i++ {
			copy(originalData[i*len(pattern):], pattern)
		}

		// Compress
		compressed, ok := TryCompressRLE(originalData, 1)
		require.True(t, ok, "RLE compression should succeed for repeated pattern")

		// Decompress
		decompressed, err := DecompressRLE(compressed, 1, uint64(len(originalData)))
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})
}

// TestCompressionRoundTrip_BitPacking verifies BITPACKING compression
// round-trip compatibility with DuckDB format.
//
// Simple format (legacy):
// - [uint8 bitWidth][uint64 count][packed bits...]
func TestCompressionRoundTrip_BitPacking(t *testing.T) {
	t.Run("Values_0_to_15_4Bits", func(t *testing.T) {
		// Create data with values 0-15 (requires 4 bits each)
		values := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
		const valueSize = 4

		originalData := make([]byte, len(values)*valueSize)
		for i, v := range values {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], uint32(v))
		}

		// Compress
		compressed, ok := TryCompressBitPackingFromBytes(originalData, valueSize)
		require.True(t, ok, "bitpacking compression should succeed")

		// Verify format has header
		require.GreaterOrEqual(t, len(compressed), 9, "should have at least 9-byte header")
		bitWidth := compressed[0]
		count := binary.LittleEndian.Uint64(compressed[1:9])

		assert.Equal(t, uint8(4), bitWidth, "bit width should be 4")
		assert.Equal(t, uint64(len(values)), count, "count should match")

		// Decompress using legacy format
		decompressedUint64, err := DecompressBitPackingToUint64(compressed)
		require.NoError(t, err)
		require.Equal(t, len(values), len(decompressedUint64))

		// Convert back to bytes
		decompressed := BitPackedToBytes(decompressedUint64, valueSize)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("Values_0_to_255_8Bits", func(t *testing.T) {
		// Create data with values 0-255 (requires 8 bits each)
		const count = 256
		const valueSize = 4

		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], uint32(i))
		}

		// Compress
		compressed, ok := TryCompressBitPackingFromBytes(originalData, valueSize)
		require.True(t, ok)

		// Decompress
		decompressedUint64, err := DecompressBitPackingToUint64(compressed)
		require.NoError(t, err)
		decompressed := BitPackedToBytes(decompressedUint64, valueSize)

		assert.Equal(t, originalData, decompressed)
	})

	t.Run("SmallValues_2Bits", func(t *testing.T) {
		// Create data with values 0-3 (requires 2 bits each)
		values := []int32{0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3}
		const valueSize = 4

		originalData := make([]byte, len(values)*valueSize)
		for i, v := range values {
			binary.LittleEndian.PutUint32(originalData[i*valueSize:], uint32(v))
		}

		// Compress
		compressed, ok := TryCompressBitPackingFromBytes(originalData, valueSize)
		require.True(t, ok)

		bitWidth := compressed[0]
		assert.Equal(t, uint8(2), bitWidth, "bit width should be 2 for values 0-3")

		// Decompress
		decompressedUint64, err := DecompressBitPackingToUint64(compressed)
		require.NoError(t, err)
		decompressed := BitPackedToBytes(decompressedUint64, valueSize)

		assert.Equal(t, originalData, decompressed)
	})
}

// TestCompressionRoundTrip_Uncompressed verifies UNCOMPRESSED format
// round-trip (data passes through unchanged).
func TestCompressionRoundTrip_Uncompressed(t *testing.T) {
	t.Run("Int32_NoCompression", func(t *testing.T) {
		// Create data that won't compress well (all different values)
		const count = 100
		const valueSize = 4

		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint32(
				originalData[i*valueSize:],
				uint32(i*7+13),
			) // Pseudo-random
		}

		// Compress (should pass through)
		compressed, err := Compress(CompressionUncompressed, originalData, valueSize)
		require.NoError(t, err)

		// Uncompressed should be identical to original
		assert.Equal(t, originalData, compressed)

		// Decompress (should also pass through)
		decompressed, err := Decompress(CompressionUncompressed, compressed, valueSize, count)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})
}

// TestCompressionRoundTrip_PFORDelta verifies PFOR_DELTA compression
// round-trip compatibility with DuckDB format.
//
// PFOR_DELTA format:
// - [int64 reference][uint8 bitWidth][uint64 count][bit-packed deltas...]
func TestCompressionRoundTrip_PFORDelta(t *testing.T) {
	t.Run("Sequential_SmallDeltas", func(t *testing.T) {
		// Create sequential data: [100, 101, 102, 103, 104, 105, ...]
		const count = 50
		const startValue = int64(100)
		const valueSize = 8

		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(originalData[i*valueSize:], uint64(startValue+int64(i)))
		}

		// Compress
		compressed, ok := TryCompressPFORDeltaFromBytes(originalData, valueSize)
		require.True(t, ok, "PFOR_DELTA compression should succeed for sequential data")

		// Verify format
		require.GreaterOrEqual(t, len(compressed), 17, "should have at least 17-byte header")
		reference := int64(binary.LittleEndian.Uint64(compressed[0:8]))
		bitWidth := compressed[8]
		valueCount := binary.LittleEndian.Uint64(compressed[9:17])

		assert.Equal(t, startValue, reference, "reference should be first value")
		assert.LessOrEqual(t, bitWidth, uint8(4), "bit width should be small for delta=1")
		assert.Equal(t, uint64(count), valueCount, "count should match")

		// Decompress
		decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("VaryingGaps", func(t *testing.T) {
		// Create data with varying gaps: [10, 15, 17, 20, 30, 31, 35, 45]
		values := []int64{10, 15, 17, 20, 30, 31, 35, 45}
		const valueSize = 8

		originalData := make([]byte, len(values)*valueSize)
		for i, v := range values {
			binary.LittleEndian.PutUint64(originalData[i*valueSize:], uint64(v))
		}

		// Compress
		compressed, ok := TryCompressPFORDeltaFromBytes(originalData, valueSize)
		require.True(t, ok)

		// Decompress
		decompressed, err := DecompressPFORDelta(compressed, valueSize, uint64(len(values)))
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})

	t.Run("LargeSequence", func(t *testing.T) {
		// Create large sequential data
		const count = 1000
		const valueSize = 8

		originalData := make([]byte, count*valueSize)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint64(originalData[i*valueSize:], uint64(i))
		}

		// Compress
		compressed, ok := TryCompressPFORDeltaFromBytes(originalData, valueSize)
		require.True(t, ok)

		// Verify compression is beneficial
		assert.Less(
			t,
			len(compressed),
			len(originalData),
			"compressed should be smaller than original",
		)

		// Decompress
		decompressed, err := DecompressPFORDelta(compressed, valueSize, count)
		require.NoError(t, err)
		assert.Equal(t, originalData, decompressed)
	})
}

// TestCompressionRoundTrip_AllTypes tests all compression types with
// the same dataset to verify correct behavior.
func TestCompressionRoundTrip_AllTypes(t *testing.T) {
	// Create test data: 100 int32 values with pattern that suits different compressions
	const count = 100
	const valueSize = 4

	testCases := []struct {
		name        string
		createData  func() []byte
		compression CompressionType
		shouldWork  bool
	}{
		{
			name: "Constant_AllSame",
			createData: func() []byte {
				data := make([]byte, count*valueSize)
				for i := 0; i < count; i++ {
					binary.LittleEndian.PutUint32(data[i*valueSize:], 42)
				}
				return data
			},
			compression: CompressionConstant,
			shouldWork:  true,
		},
		{
			name: "RLE_Runs",
			createData: func() []byte {
				data := make([]byte, count*valueSize)
				for i := 0; i < count; i++ {
					val := uint32(i / 10) // 10 copies of each value
					binary.LittleEndian.PutUint32(data[i*valueSize:], val)
				}
				return data
			},
			compression: CompressionRLE,
			shouldWork:  true,
		},
		// BitPacking is skipped here because CompressBitPacking produces legacy format
		// while Decompress expects DuckDB format. See TestCompressionRoundTrip_BitPacking
		// for dedicated BitPacking tests using the legacy format.
		{
			name: "PFORDelta_Sequential",
			createData: func() []byte {
				data := make([]byte, count*8) // Use 8-byte values for PFOR_DELTA
				for i := 0; i < count; i++ {
					binary.LittleEndian.PutUint64(data[i*8:], uint64(i))
				}
				return data
			},
			compression: CompressionPFORDelta,
			shouldWork:  true,
		},
		{
			name: "Uncompressed_Random",
			createData: func() []byte {
				data := make([]byte, count*valueSize)
				for i := 0; i < count; i++ {
					val := uint32(i*7 + 13) // Pseudo-random
					binary.LittleEndian.PutUint32(data[i*valueSize:], val)
				}
				return data
			},
			compression: CompressionUncompressed,
			shouldWork:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalData := tc.createData()

			// Determine value size and count based on data
			actualValueSize := valueSize
			if tc.compression == CompressionPFORDelta {
				actualValueSize = 8 // PFOR_DELTA uses 8-byte values
			}
			actualCount := uint64(len(originalData) / actualValueSize)

			// Compress
			compressed, err := Compress(tc.compression, originalData, actualValueSize)
			if tc.shouldWork {
				require.NoError(t, err, "compression should succeed")
			} else {
				if err != nil {
					t.Skip("compression not supported or not beneficial")
				}
			}

			// Decompress
			decompressed, err := Decompress(
				tc.compression,
				compressed,
				actualValueSize,
				actualCount,
			)
			require.NoError(t, err, "decompression should succeed")

			// Verify round-trip
			assert.Equal(t, originalData, decompressed, "round-trip should preserve data")
		})
	}
}

// TestCompressionRoundTrip_EdgeCases tests edge cases for all compression types.
func TestCompressionRoundTrip_EdgeCases(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		// Empty data should work with EMPTY compression
		data := []byte{}
		compressed, err := Compress(CompressionEmpty, data, 4)
		require.NoError(t, err)
		assert.Empty(t, compressed)

		decompressed, err := Decompress(CompressionEmpty, compressed, 4, 0)
		require.NoError(t, err)
		assert.Empty(t, decompressed)
	})

	t.Run("SingleValue_Constant", func(t *testing.T) {
		// Single value (not beneficial for CONSTANT, needs at least 2)
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, 42)

		_, ok := CompressConstant(data, 4)
		assert.False(t, ok, "single value should not compress with CONSTANT")
	})

	t.Run("AllZeros", func(t *testing.T) {
		// All zeros with CONSTANT compression
		const count = 100
		data := make([]byte, count*4)

		compressed, ok := CompressConstant(data, 4)
		require.True(t, ok)
		assert.Equal(t, 4, len(compressed))

		decompressed, err := DecompressConstant(compressed, 4, count)
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("AllOnes", func(t *testing.T) {
		// All 0xFFFFFFFF with CONSTANT compression
		const count = 100
		data := make([]byte, count*4)
		for i := 0; i < count; i++ {
			binary.LittleEndian.PutUint32(data[i*4:], 0xFFFFFFFF)
		}

		compressed, ok := CompressConstant(data, 4)
		require.True(t, ok)

		decompressed, err := DecompressConstant(compressed, 4, count)
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})
}
