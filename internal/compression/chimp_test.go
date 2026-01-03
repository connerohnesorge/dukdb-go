package compression

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewChimpCodec(t *testing.T) {
	tests := []struct {
		name     string
		isDouble bool
	}{
		{"float32 codec", false},
		{"float64 codec", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewChimpCodec(tt.isDouble)
			require.NotNil(t, codec)
			assert.Equal(t, tt.isDouble, codec.isDouble)
		})
	}
}

func TestChimpCodec_Type(t *testing.T) {
	codec := NewChimpCodec(true)
	assert.Equal(t, CompressionChimp, codec.Type())
}

func TestChimpCodec_Compress_Float64_AllSame(t *testing.T) {
	codec := NewChimpCodec(true)

	// Create data with all identical float64 values
	count := 100
	data := make([]byte, count*8)
	value := math.Pi
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Chimp should compress identical values extremely well
	// First value: 8 bytes, subsequent values: 1 bit each (rounded up)
	assert.Less(t, len(compressed), len(data)/2,
		"Chimp should compress identical values well")
}

func TestChimpCodec_Compress_Float32_AllSame(t *testing.T) {
	codec := NewChimpCodec(false)

	// Create data with all identical float32 values
	count := 100
	data := make([]byte, count*4)
	value := float32(3.14159)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Should compress very well for identical values
	assert.Less(t, len(compressed), len(data)/2,
		"Chimp should compress identical values well")
}

func TestChimpCodec_Compress_Float64_TimeSeries(t *testing.T) {
	codec := NewChimpCodec(true)

	// Simulate time-series data with small increments (like temperatures)
	count := 100
	data := make([]byte, count*8)
	baseTemp := 72.5 // Base temperature in Fahrenheit

	for i := 0; i < count; i++ {
		// Small variations: 72.5 ± 0.5
		temp := baseTemp + 0.5*math.Sin(float64(i)*0.1)
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(temp))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Chimp may not always achieve compression for small datasets
	// but it should work correctly
	assert.NotEmpty(t, compressed, "Should produce compressed output")
}

func TestChimpCodec_Compress_Float32_TimeSeries(t *testing.T) {
	codec := NewChimpCodec(false)

	// Simulate stock price data
	count := 100
	data := make([]byte, count*4)
	basePrice := float32(150.25)

	for i := 0; i < count; i++ {
		// Small price fluctuations
		price := basePrice + float32(math.Sin(float64(i)*0.2))
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(price))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Chimp may not always achieve compression for small datasets
	// but it should work correctly
	assert.NotEmpty(t, compressed, "Should produce compressed output")
}

func TestChimpCodec_Compress_Empty(t *testing.T) {
	tests := []struct {
		name     string
		isDouble bool
	}{
		{"float32", false},
		{"float64", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewChimpCodec(tt.isDouble)
			compressed, err := codec.Compress([]byte{})
			require.NoError(t, err)
			assert.Empty(t, compressed)
		})
	}
}

func TestChimpCodec_Compress_InvalidSize(t *testing.T) {
	tests := []struct {
		name     string
		isDouble bool
		dataLen  int
	}{
		{"float32 - 3 bytes", false, 3},
		{"float32 - 7 bytes", false, 7},
		{"float64 - 7 bytes", true, 7},
		{"float64 - 15 bytes", true, 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewChimpCodec(tt.isDouble)
			data := make([]byte, tt.dataLen)
			_, err := codec.Compress(data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a multiple of value size")
		})
	}
}

func TestChimpCodec_Decompress_Empty(t *testing.T) {
	tests := []struct {
		name     string
		isDouble bool
	}{
		{"float32", false},
		{"float64", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewChimpCodec(tt.isDouble)
			decompressed, err := codec.Decompress([]byte{}, 0)
			require.NoError(t, err)
			assert.Empty(t, decompressed)
		})
	}
}

func TestChimpCodec_Decompress_InvalidDestSize(t *testing.T) {
	tests := []struct {
		name     string
		isDouble bool
		destSize int
	}{
		{"float32 - 3 bytes", false, 3},
		{"float32 - 7 bytes", false, 7},
		{"float64 - 7 bytes", true, 7},
		{"float64 - 15 bytes", true, 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewChimpCodec(tt.isDouble)
			// Create some dummy compressed data
			data := make([]byte, 16)
			_, err := codec.Decompress(data, tt.destSize)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a multiple of value size")
		})
	}
}

func TestChimpCodec_RoundTrip_Float64_AllSame(t *testing.T) {
	codec := NewChimpCodec(true)

	// Test with identical values
	values := []float64{42.42, 42.42, 42.42, 42.42, 42.42}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	// Verify values
	for i := range values {
		bits := binary.LittleEndian.Uint64(decompressed[i*8:])
		val := math.Float64frombits(bits)
		assert.Equal(t, values[i], val)
	}
}

func TestChimpCodec_RoundTrip_Float32_AllSame(t *testing.T) {
	codec := NewChimpCodec(false)

	values := []float32{3.14, 3.14, 3.14, 3.14, 3.14}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	// Verify values
	for i := range values {
		bits := binary.LittleEndian.Uint32(decompressed[i*4:])
		val := math.Float32frombits(bits)
		assert.Equal(t, values[i], val)
	}
}

func TestChimpCodec_RoundTrip_Float64_Sequential(t *testing.T) {
	codec := NewChimpCodec(true)

	// Sequential values
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	for i := range values {
		bits := binary.LittleEndian.Uint64(decompressed[i*8:])
		val := math.Float64frombits(bits)
		assert.Equal(t, values[i], val)
	}
}

func TestChimpCodec_RoundTrip_Float32_Sequential(t *testing.T) {
	codec := NewChimpCodec(false)

	values := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")

	for i := range values {
		bits := binary.LittleEndian.Uint32(decompressed[i*4:])
		val := math.Float32frombits(bits)
		assert.Equal(t, values[i], val)
	}
}

func TestChimpCodec_RoundTrip_Float64_TimeSeries(t *testing.T) {
	codec := NewChimpCodec(true)

	// Simulate sensor readings (temperature sensor)
	count := 50
	data := make([]byte, count*8)
	baseTemp := 20.5

	for i := 0; i < count; i++ {
		// Temperature varying between 19.5 and 21.5
		temp := baseTemp + math.Sin(float64(i)*0.2)
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(temp))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")
}

func TestChimpCodec_RoundTrip_Float32_TimeSeries(t *testing.T) {
	codec := NewChimpCodec(false)

	// Simulate stock prices
	count := 50
	data := make([]byte, count*4)
	basePrice := float32(100.0)

	for i := 0; i < count; i++ {
		// Price varying around 100
		price := basePrice + float32(5.0*math.Sin(float64(i)*0.3))
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(price))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")
}

func TestChimpCodec_RoundTrip_Float64_SpecialValues(t *testing.T) {
	codec := NewChimpCodec(true)

	// Test special floating-point values
	values := []float64{
		0.0,
		-0.0,
		1.0,
		-1.0,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		math.Inf(1),
		math.Inf(-1),
		math.NaN(),
	}

	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	// For NaN, we need special comparison
	for i := range values {
		bits := binary.LittleEndian.Uint64(decompressed[i*8:])
		val := math.Float64frombits(bits)

		if math.IsNaN(values[i]) {
			assert.True(t, math.IsNaN(val), "NaN should be preserved")
		} else {
			assert.Equal(t, values[i], val, "Value %d should match", i)
		}
	}
}

func TestChimpCodec_RoundTrip_Float32_SpecialValues(t *testing.T) {
	codec := NewChimpCodec(false)

	values := []float32{
		0.0,
		-0.0,
		1.0,
		-1.0,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		float32(math.Inf(1)),
		float32(math.Inf(-1)),
		float32(math.NaN()),
	}

	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	for i := range values {
		bits := binary.LittleEndian.Uint32(decompressed[i*4:])
		val := math.Float32frombits(bits)

		if math.IsNaN(float64(values[i])) {
			assert.True(t, math.IsNaN(float64(val)), "NaN should be preserved")
		} else {
			assert.Equal(t, values[i], val, "Value %d should match", i)
		}
	}
}

func TestChimpCodec_RoundTrip_Float64_Large(t *testing.T) {
	codec := NewChimpCodec(true)

	// Test with larger dataset
	count := 1000
	data := make([]byte, count*8)
	for i := 0; i < count; i++ {
		value := 100.0 + math.Sin(float64(i)*0.1)
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")
}

func TestChimpCodec_RoundTrip_Float32_Large(t *testing.T) {
	codec := NewChimpCodec(false)

	count := 1000
	data := make([]byte, count*4)
	for i := 0; i < count; i++ {
		value := float32(50.0 + 10.0*math.Sin(float64(i)*0.05))
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)

	assert.Equal(t, data, decompressed, "Round-trip should preserve data")
}

func TestChimpCodec_Interface(_ *testing.T) {
	codec := NewChimpCodec(true)

	// Verify ChimpCodec implements all required interfaces
	var _ Codec = codec
	var _ Compressor = codec
	var _ Decompressor = codec
}

func TestChimpCodec_CompressionRatio_Float64(t *testing.T) {
	codec := NewChimpCodec(true)

	// Create highly compressible data (all same values)
	count := 1000
	data := make([]byte, count*8)
	value := 42.42
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	compressionRatio := float64(len(data)) / float64(len(compressed))
	assert.Greater(t, compressionRatio, 10.0,
		"Chimp should achieve >10x compression for identical values")

	// Verify round-trip
	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestChimpCodec_CompressionRatio_Float32(t *testing.T) {
	codec := NewChimpCodec(false)

	count := 1000
	data := make([]byte, count*4)
	value := float32(3.14159)
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	compressionRatio := float64(len(data)) / float64(len(compressed))
	assert.Greater(t, compressionRatio, 10.0,
		"Chimp should achieve >10x compression for identical values")

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

// Test helper functions

func TestCountLeadingZeros64(t *testing.T) {
	tests := []struct {
		value    uint64
		expected int
	}{
		{0, 64},
		{1, 63},
		{0xFF, 56},
		{0xFFFF, 48},
		{0xFFFFFFFF, 32},
		{0xFFFFFFFFFFFFFFFF, 0},
		{0x8000000000000000, 0},
		{0x0000000000000001, 63},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := countLeadingZeros64(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCountLeadingZeros32(t *testing.T) {
	tests := []struct {
		value    uint32
		expected int
	}{
		{0, 32},
		{1, 31},
		{0xFF, 24},
		{0xFFFF, 16},
		{0xFFFFFFFF, 0},
		{0x80000000, 0},
		{0x00000001, 31},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := countLeadingZeros32(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkChimpCodec_Compress_Float64_AllSame(b *testing.B) {
	codec := NewChimpCodec(true)
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		data := make([]byte, size*8)
		value := math.Pi
		for i := 0; i < size; i++ {
			binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
		}

		b.Run(formatSize(size*8), func(b *testing.B) {
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

func BenchmarkChimpCodec_Compress_Float64_TimeSeries(b *testing.B) {
	codec := NewChimpCodec(true)
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		data := make([]byte, size*8)
		for i := 0; i < size; i++ {
			value := 100.0 + math.Sin(float64(i)*0.1)
			binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
		}

		b.Run(formatSize(size*8), func(b *testing.B) {
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

func BenchmarkChimpCodec_Decompress_Float64(b *testing.B) {
	codec := NewChimpCodec(true)
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		data := make([]byte, size*8)
		for i := 0; i < size; i++ {
			value := 100.0 + math.Sin(float64(i)*0.1)
			binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
		}

		compressed, _ := codec.Compress(data)

		b.Run(formatSize(size*8), func(b *testing.B) {
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

func BenchmarkChimpCodec_RoundTrip_Float64(b *testing.B) {
	codec := NewChimpCodec(true)
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		data := make([]byte, size*8)
		for i := 0; i < size; i++ {
			value := 100.0 + math.Sin(float64(i)*0.1)
			binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(value))
		}

		b.Run(formatSize(size*8), func(b *testing.B) {
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
