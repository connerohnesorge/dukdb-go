package compression

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRLECodec(t *testing.T) {
	tests := []struct {
		name      string
		valueSize int
		wantPanic bool
	}{
		{"1-byte values", 1, false},
		{"2-byte values", 2, false},
		{"4-byte values", 4, false},
		{"8-byte values", 8, false},
		{"invalid zero", 0, true},
		{"invalid negative", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				assert.Panics(t, func() { NewRLECodec(tt.valueSize) })
				return
			}

			codec := NewRLECodec(tt.valueSize)
			require.NotNil(t, codec)
			assert.Equal(t, tt.valueSize, codec.valueSize)
		})
	}
}

func TestRLECodec_Type(t *testing.T) {
	codec := NewRLECodec(4)
	assert.Equal(t, CompressionRLE, codec.Type())
}

func TestRLECodec_Compress_AllSame(t *testing.T) {
	// Test with all identical values - best case for RLE
	tests := []struct {
		name      string
		valueSize int
		count     int
	}{
		{"1-byte, 10 values", 1, 10},
		{"2-byte, 20 values", 2, 20},
		{"4-byte, 100 values", 4, 100},
		{"8-byte, 50 values", 8, 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewRLECodec(tt.valueSize)

			// Create data with all same values
			data := make([]byte, tt.count*tt.valueSize)
			value := make([]byte, tt.valueSize)
			for i := range value {
				value[i] = 0xAA // pattern
			}
			for i := 0; i < tt.count; i++ {
				copy(data[i*tt.valueSize:], value)
			}

			compressed, err := codec.Compress(data)
			require.NoError(t, err)

			// Compressed should be: varint(count) + value
			// This should be much smaller than the original
			assert.Less(t, len(compressed), len(data),
				"RLE should compress repeated values")
		})
	}
}

func TestRLECodec_Compress_Alternating(t *testing.T) {
	// Test with alternating values - worst case for RLE
	codec := NewRLECodec(4)

	// Create alternating int32 values: 1, 2, 1, 2, 1, 2, ...
	count := 50
	data := make([]byte, count*4)
	for i := 0; i < count; i++ {
		val := int32(1 + (i % 2))
		binary.LittleEndian.PutUint32(data[i*4:], uint32(val))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// With alternating values, each run has length 1
	// Compressed should be: [varint(1), value1] + [varint(1), value2] + ...
	// This will be larger than the original (each value needs a run length prefix)
	assert.NotEmpty(t, compressed)
}

func TestRLECodec_Compress_Mixed(t *testing.T) {
	// Test with mixed runs of different values
	codec := NewRLECodec(4)

	// Create data: 5x(0), 10x(1), 3x(2), 7x(3)
	var buf bytes.Buffer
	writeInt32Run := func(value int32, count int) {
		for i := 0; i < count; i++ {
			err := binary.Write(&buf, binary.LittleEndian, value)
			require.NoError(t, err)
		}
	}

	writeInt32Run(0, 5)
	writeInt32Run(1, 10)
	writeInt32Run(2, 3)
	writeInt32Run(3, 7)

	data := buf.Bytes()
	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Should compress to: [5, 0] + [10, 1] + [3, 2] + [7, 3]
	// Each varint(count) takes 1 byte for small counts, value takes 4 bytes
	// So approximately 4 * (1 + 4) = 20 bytes
	assert.NotEmpty(t, compressed)
	assert.Less(t, len(compressed), len(data),
		"RLE should compress data with runs")
}

func TestRLECodec_Compress_Empty(t *testing.T) {
	codec := NewRLECodec(4)

	compressed, err := codec.Compress([]byte{})
	require.NoError(t, err)
	assert.Empty(t, compressed)
}

func TestRLECodec_Compress_InvalidSize(t *testing.T) {
	codec := NewRLECodec(4)

	// Data length not a multiple of value size
	data := []byte{1, 2, 3, 4, 5, 6, 7} // 7 bytes, not multiple of 4
	_, err := codec.Compress(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a multiple of value size")
}

func TestRLECodec_Decompress_Empty(t *testing.T) {
	codec := NewRLECodec(4)

	decompressed, err := codec.Decompress([]byte{}, 0)
	require.NoError(t, err)
	assert.Empty(t, decompressed)
}

func TestRLECodec_Decompress_InvalidDestSize(t *testing.T) {
	codec := NewRLECodec(4)

	// Create some compressed data
	data := []byte{0x05, 0xAA, 0xBB, 0xCC, 0xDD} // run_length=5, value=0xAABBCCDD

	// Try to decompress to size not multiple of value size
	_, err := codec.Decompress(data, 7)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a multiple of value size")
}

func TestRLECodec_Decompress_SizeMismatch(t *testing.T) {
	codec := NewRLECodec(4)

	// Create compressed data for 5 values
	var buf bytes.Buffer
	err := writeVarint(&buf, 5) // run length
	require.NoError(t, err)
	_, err = buf.Write([]byte{0xAA, 0xBB, 0xCC, 0xDD})
	require.NoError(t, err)

	// Try to decompress expecting different size
	_, err = codec.Decompress(buf.Bytes(), 8*4) // expecting 8 values, but only 5 encoded
	require.Error(t, err, "Should fail when compressed data doesn't match expected size")
}

func TestRLECodec_RoundTrip_1Byte(t *testing.T) {
	codec := NewRLECodec(1)

	tests := []struct {
		name string
		data []byte
	}{
		{"all same", []byte{42, 42, 42, 42, 42, 42, 42, 42}},
		{"alternating", []byte{1, 2, 1, 2, 1, 2, 1, 2}},
		{"sequential", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"runs", []byte{1, 1, 1, 2, 2, 3, 3, 3, 3, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := codec.Compress(tt.data)
			require.NoError(t, err)

			decompressed, err := codec.Decompress(compressed, len(tt.data))
			require.NoError(t, err)

			assert.Equal(t, tt.data, decompressed,
				"Round-trip should preserve data")
		})
	}
}

func TestRLECodec_RoundTrip_2Byte(t *testing.T) {
	codec := NewRLECodec(2)

	// Create int16 data
	values := []int16{100, 100, 100, 200, 200, 300, 400, 400, 400, 400}
	data := make([]byte, len(values)*2)
	for i, v := range values {
		binary.LittleEndian.PutUint16(data[i*2:], uint16(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	// Verify decompressed values
	for i := range values {
		val := int16(binary.LittleEndian.Uint16(decompressed[i*2:]))
		assert.Equal(t, values[i], val)
	}
}

func TestRLECodec_RoundTrip_4Byte(t *testing.T) {
	codec := NewRLECodec(4)

	// Create int32 data with various run lengths
	values := []int32{
		1000, 1000, 1000, 1000, 1000, // run of 5
		2000, 2000,                   // run of 2
		3000,                         // run of 1
		4000, 4000, 4000, 4000, 4000, 4000, 4000, // run of 7
	}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	// Verify decompressed values
	for i := range values {
		val := int32(binary.LittleEndian.Uint32(decompressed[i*4:]))
		assert.Equal(t, values[i], val)
	}
}

func TestRLECodec_RoundTrip_8Byte(t *testing.T) {
	codec := NewRLECodec(8)

	// Create int64 data (timestamps, dates, etc.)
	values := []int64{
		1234567890, 1234567890, 1234567890, // run of 3
		9876543210, 9876543210,             // run of 2
		1111111111,                         // run of 1
		2222222222, 2222222222, 2222222222, 2222222222, // run of 4
	}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	// Verify decompressed values
	for i := range values {
		val := int64(binary.LittleEndian.Uint64(decompressed[i*8:]))
		assert.Equal(t, values[i], val)
	}
}

func TestRLECodec_CompressionRatio(t *testing.T) {
	// Test that RLE achieves good compression for highly repetitive data
	codec := NewRLECodec(4)

	// Create data with 1000 identical int32 values
	count := 1000
	data := make([]byte, count*4)
	value := int32(42)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Compressed should be: varint(1000) + 4 bytes
	// varint(1000) = 2 bytes (0xE8 0x07)
	// So total should be ~6 bytes vs 4000 bytes original
	compressionRatio := float64(len(data)) / float64(len(compressed))
	assert.Greater(t, compressionRatio, 100.0,
		"RLE should achieve >100x compression for 1000 identical values")

	// Verify round-trip
	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestRLECodec_LargeRun(t *testing.T) {
	// Test with a very large run to ensure varint encoding works correctly
	codec := NewRLECodec(4)

	count := 100000 // 100k values
	data := make([]byte, count*4)
	value := uint32(0xDEADBEEF)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], value)
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Should handle large runs correctly")
}

func TestRLECodec_Interface(_ *testing.T) {
	codec := NewRLECodec(4)

	// Verify RLECodec implements all required interfaces
	var _ Codec = codec
	var _ Compressor = codec
	var _ Decompressor = codec
}

func TestRLECodec_CorruptedData(t *testing.T) {
	codec := NewRLECodec(4)

	tests := []struct {
		name string
		data []byte
	}{
		{"truncated run length", []byte{0x80}}, // incomplete varint
		{"missing value", []byte{0x05}},        // run length but no value
		{"partial value", []byte{0x05, 0xAA, 0xBB}}, // run length + incomplete value
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Decompress(tt.data, 20) // arbitrary dest size
			require.Error(t, err, "Should fail on corrupted data")
		})
	}
}

func BenchmarkRLECodec_Compress_AllSame(b *testing.B) {
	codec := NewRLECodec(4)
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size*4)
		value := int32(42)
		for i := 0; i < size; i++ {
			binary.LittleEndian.PutUint32(data[i*4:], uint32(value))
		}

		b.Run(formatSize(size*4), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				_, err := codec.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRLECodec_Compress_Alternating(b *testing.B) {
	codec := NewRLECodec(4)
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size*4)
		for i := 0; i < size; i++ {
			val := int32(i % 2)
			binary.LittleEndian.PutUint32(data[i*4:], uint32(val))
		}

		b.Run(formatSize(size*4), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				_, err := codec.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRLECodec_Decompress_AllSame(b *testing.B) {
	codec := NewRLECodec(4)
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		data := make([]byte, size*4)
		value := int32(42)
		for i := 0; i < size; i++ {
			binary.LittleEndian.PutUint32(data[i*4:], uint32(value))
		}

		compressed, _ := codec.Compress(data)

		b.Run(formatSize(size*4), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				_, err := codec.Decompress(compressed, len(data))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRLECodec_RoundTrip(b *testing.B) {
	codec := NewRLECodec(4)
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		data := make([]byte, size*4)
		// Mix of runs: 70% same value, 30% different
		for i := 0; i < size; i++ {
			var value int32
			if i%10 < 7 {
				value = 42
			} else {
				value = int32(i)
			}
			binary.LittleEndian.PutUint32(data[i*4:], uint32(value))
		}

		b.Run(formatSize(size*4), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for range b.N {
				compressed, err := codec.Compress(data)
				if err != nil {
					b.Fatal(err)
				}
				_, err = codec.Decompress(compressed, len(data))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
