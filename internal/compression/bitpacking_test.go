package compression

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBitPackCodec_Type(t *testing.T) {
	codec := NewBitPackCodec(4)
	assert.Equal(t, CompressionBitPack, codec.Type())
}

func TestBitPackCodec_NewBitPackCodec(t *testing.T) {
	tests := []struct {
		name      string
		bitWidth  int
		shouldPanic bool
	}{
		{"valid 1 bit", 1, false},
		{"valid 4 bits", 4, false},
		{"valid 8 bits", 8, false},
		{"valid 16 bits", 16, false},
		{"valid 32 bits", 32, false},
		{"valid 64 bits", 64, false},
		{"invalid 0 bits", 0, true},
		{"invalid 65 bits", 65, true},
		{"invalid negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					NewBitPackCodec(tt.bitWidth)
				})
			} else {
				codec := NewBitPackCodec(tt.bitWidth)
				assert.NotNil(t, codec)
				assert.Equal(t, tt.bitWidth, codec.bitWidth)
			}
		})
	}
}

func TestBitPackCodec_Compress_Empty(t *testing.T) {
	codec := NewBitPackCodec(4)
	compressed, err := codec.Compress([]byte{})
	require.NoError(t, err)
	assert.Equal(t, []byte{4}, compressed) // Just the bit width header
}

func TestBitPackCodec_RoundTrip_1Bit(t *testing.T) {
	codec := NewBitPackCodec(1)

	// Test data: 8 values that fit in 1 bit each (0 or 1)
	data := []byte{0, 1, 0, 1, 1, 0, 1, 0}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Verify compression occurred
	assert.Less(t, len(compressed), len(data))

	// Decompress and verify
	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestBitPackCodec_RoundTrip_4Bit(t *testing.T) {
	codec := NewBitPackCodec(4)

	// Test data: 8 values that fit in 4 bits each (0-15)
	data := []byte{0, 1, 2, 3, 4, 5, 15, 14}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// With 4 bits per value, 8 values = 32 bits = 4 bytes + 1 byte header = 5 bytes
	// Original: 8 bytes, Compressed: 5 bytes (37.5% savings)
	assert.Less(t, len(compressed), len(data))

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestBitPackCodec_RoundTrip_8Bit(t *testing.T) {
	codec := NewBitPackCodec(8)

	// Test data: values using full 8 bits
	data := []byte{0, 1, 127, 128, 255, 200, 42, 99}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestBitPackCodec_RoundTrip_16Bit(t *testing.T) {
	codec := NewBitPackCodec(16)

	// Test data: 4 uint16 values
	data := make([]byte, 8)
	binary.LittleEndian.PutUint16(data[0:2], 0)
	binary.LittleEndian.PutUint16(data[2:4], 1000)
	binary.LittleEndian.PutUint16(data[4:6], 32767)
	binary.LittleEndian.PutUint16(data[6:8], 65535)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)

	// Verify values
	assert.Equal(t, uint16(0), binary.LittleEndian.Uint16(decompressed[0:2]))
	assert.Equal(t, uint16(1000), binary.LittleEndian.Uint16(decompressed[2:4]))
	assert.Equal(t, uint16(32767), binary.LittleEndian.Uint16(decompressed[4:6]))
	assert.Equal(t, uint16(65535), binary.LittleEndian.Uint16(decompressed[6:8]))
}

func TestBitPackCodec_RoundTrip_32Bit(t *testing.T) {
	codec := NewBitPackCodec(32)

	// Test data: 4 uint32 values
	data := make([]byte, 16)
	binary.LittleEndian.PutUint32(data[0:4], 0)
	binary.LittleEndian.PutUint32(data[4:8], 1000000)
	binary.LittleEndian.PutUint32(data[8:12], 2147483647)
	binary.LittleEndian.PutUint32(data[12:16], 4294967295)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)

	// Verify values
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(decompressed[0:4]))
	assert.Equal(t, uint32(1000000), binary.LittleEndian.Uint32(decompressed[4:8]))
	assert.Equal(t, uint32(2147483647), binary.LittleEndian.Uint32(decompressed[8:12]))
	assert.Equal(t, uint32(4294967295), binary.LittleEndian.Uint32(decompressed[12:16]))
}

func TestBitPackCodec_RoundTrip_64Bit(t *testing.T) {
	codec := NewBitPackCodec(64)

	// Test data: 4 uint64 values
	data := make([]byte, 32)
	binary.LittleEndian.PutUint64(data[0:8], 0)
	binary.LittleEndian.PutUint64(data[8:16], 1000000000000)
	binary.LittleEndian.PutUint64(data[16:24], 9223372036854775807)
	binary.LittleEndian.PutUint64(data[24:32], 18446744073709551615)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)

	// Verify values
	assert.Equal(t, uint64(0), binary.LittleEndian.Uint64(decompressed[0:8]))
	assert.Equal(t, uint64(1000000000000), binary.LittleEndian.Uint64(decompressed[8:16]))
	assert.Equal(t, uint64(9223372036854775807), binary.LittleEndian.Uint64(decompressed[16:24]))
	assert.Equal(t, uint64(18446744073709551615), binary.LittleEndian.Uint64(decompressed[24:32]))
}

func TestBitPackCodec_EdgeCases(t *testing.T) {
	t.Run("all zeros with 1 bit", func(t *testing.T) {
		codec := NewBitPackCodec(1)
		data := []byte{0, 0, 0, 0, 0, 0, 0, 0}

		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("all ones with 1 bit", func(t *testing.T) {
		codec := NewBitPackCodec(1)
		data := []byte{1, 1, 1, 1, 1, 1, 1, 1}

		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("max value for 4 bits", func(t *testing.T) {
		codec := NewBitPackCodec(4)
		data := []byte{15, 15, 15, 15} // Max value for 4 bits

		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("single value", func(t *testing.T) {
		codec := NewBitPackCodec(4)
		data := []byte{7}

		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("odd number of values", func(t *testing.T) {
		codec := NewBitPackCodec(4)
		data := []byte{1, 2, 3, 4, 5, 6, 7} // 7 values

		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})
}

func TestBitPackCodec_InvalidInput(t *testing.T) {
	t.Run("decompress empty data", func(t *testing.T) {
		codec := NewBitPackCodec(4)
		_, err := codec.Decompress([]byte{}, 8)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("decompress with invalid bit width", func(t *testing.T) {
		codec := NewBitPackCodec(4)
		invalidData := []byte{65, 0, 0, 0} // bit width 65 is invalid
		_, err := codec.Decompress(invalidData, 4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bit width")
	})

	t.Run("decompress with insufficient data", func(t *testing.T) {
		codec := NewBitPackCodec(8)
		// Header says 8-bit width, but only 1 byte of data (need at least 2 for destSize=1)
		insufficientData := []byte{8}
		_, err := codec.Decompress(insufficientData, 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient data")
	})
}

func TestBitPackCodec_Auto(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		valueSize int
		wantWidth int
	}{
		{
			name:      "1-bit values",
			data:      []byte{0, 1, 0, 1},
			valueSize: 1,
			wantWidth: 1,
		},
		{
			name:      "4-bit values",
			data:      []byte{0, 1, 2, 15, 14, 3},
			valueSize: 1,
			wantWidth: 4,
		},
		{
			name:      "8-bit values",
			data:      []byte{0, 1, 127, 128, 255},
			valueSize: 1,
			wantWidth: 8,
		},
		{
			name: "16-bit values needing 12 bits",
			data: func() []byte {
				d := make([]byte, 8)
				binary.LittleEndian.PutUint16(d[0:2], 0)
				binary.LittleEndian.PutUint16(d[2:4], 1000)
				binary.LittleEndian.PutUint16(d[4:6], 2000)
				binary.LittleEndian.PutUint16(d[6:8], 4095) // Max value for 12 bits
				return d
			}(),
			valueSize: 2,
			wantWidth: 12,
		},
		{
			name:      "all zeros",
			data:      []byte{0, 0, 0, 0},
			valueSize: 1,
			wantWidth: 1, // At least 1 bit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewBitPackCodecAuto(tt.data, tt.valueSize)
			assert.Equal(t, tt.wantWidth, codec.bitWidth)

			// Verify round-trip works
			compressed, err := codec.Compress(tt.data)
			require.NoError(t, err)

			decompressed, err := codec.Decompress(compressed, len(tt.data))
			require.NoError(t, err)
			assert.Equal(t, tt.data, decompressed)
		})
	}
}

func TestBitPackCodec_CompressionRatio(t *testing.T) {
	tests := []struct {
		name           string
		bitWidth       int
		data           []byte
		minCompression float64 // minimum expected compression ratio
	}{
		{
			name:           "1-bit should compress 87.5%",
			bitWidth:       1,
			data:           []byte{0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0},
			minCompression: 0.75, // At least 75% compression
		},
		{
			name:           "4-bit should compress 50%",
			bitWidth:       4,
			data:           []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			minCompression: 0.40, // At least 40% compression
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewBitPackCodec(tt.bitWidth)

			compressed, err := codec.Compress(tt.data)
			require.NoError(t, err)

			ratio := 1.0 - (float64(len(compressed)) / float64(len(tt.data)))
			assert.GreaterOrEqual(t, ratio, tt.minCompression,
				"expected at least %.0f%% compression, got %.0f%%",
				tt.minCompression*100, ratio*100)
		})
	}
}

func TestBitPackCodec_LargeData(t *testing.T) {
	codec := NewBitPackCodec(4)

	// Create 1000 values that fit in 4 bits
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 16) // Values 0-15
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Should compress to about half the size (plus header)
	assert.Less(t, len(compressed), len(data)/2+10)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func BenchmarkBitPackCodec_Compress_1Bit(b *testing.B) {
	codec := NewBitPackCodec(1)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 2)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Compress(data)
	}
}

func BenchmarkBitPackCodec_Compress_4Bit(b *testing.B) {
	codec := NewBitPackCodec(4)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Compress(data)
	}
}

func BenchmarkBitPackCodec_Compress_8Bit(b *testing.B) {
	codec := NewBitPackCodec(8)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Compress(data)
	}
}

func BenchmarkBitPackCodec_Decompress_4Bit(b *testing.B) {
	codec := NewBitPackCodec(4)
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 16)
	}

	compressed, _ := codec.Compress(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Decompress(compressed, len(data))
	}
}

func BenchmarkBitPackCodec_Auto(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewBitPackCodecAuto(data, 1)
	}
}
