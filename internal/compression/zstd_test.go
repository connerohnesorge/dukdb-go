package compression

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewZstdCodec(t *testing.T) {
	tests := []struct {
		name  string
		level int
	}{
		{"fastest level", 1},
		{"default level", 3},
		{"better compression", 5},
		{"best compression", 10},
		{"very high level", 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewZstdCodec(tt.level)
			require.NotNil(t, codec)
			assert.NotNil(t, codec)
		})
	}
}

func TestZstdCodec_Type(t *testing.T) {
	codec := NewZstdCodec(3)
	assert.Equal(t, CompressionZstd, codec.Type())
}

func TestZstdCodec_Compress_Empty(t *testing.T) {
	codec := NewZstdCodec(3)
	compressed, err := codec.Compress([]byte{})
	require.NoError(t, err)
	assert.Empty(t, compressed)
}

func TestZstdCodec_Decompress_Empty(t *testing.T) {
	codec := NewZstdCodec(3)
	decompressed, err := codec.Decompress([]byte{}, 0)
	require.NoError(t, err)
	assert.Empty(t, decompressed)
}

func TestZstdCodec_Decompress_EmptyDataNonZeroSize(t *testing.T) {
	codec := NewZstdCodec(3)
	_, err := codec.Decompress([]byte{}, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot decompress empty data")
}

func TestZstdCodec_Decompress_SizeMismatch(t *testing.T) {
	codec := NewZstdCodec(3)

	// Compress some data
	original := []byte("test data")
	compressed, err := codec.Compress(original)
	require.NoError(t, err)

	// Try to decompress with wrong size
	_, err = codec.Decompress(compressed, len(original)+10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decompressed size mismatch")
}

func TestZstdCodec_RoundTrip_SmallData(t *testing.T) {
	tests := []struct {
		name  string
		level int
		data  []byte
	}{
		{"fastest - hello world", 1, []byte("Hello, World!")},
		{"default - sentence", 3, []byte("The quick brown fox jumps over the lazy dog.")},
		{"better - paragraph", 5, []byte(strings.Repeat("Lorem ipsum dolor sit amet. ", 10))},
		{"best - repeated", 10, []byte(strings.Repeat("A", 1000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewZstdCodec(tt.level)

			compressed, err := codec.Compress(tt.data)
			require.NoError(t, err)
			assert.NotEmpty(t, compressed)

			decompressed, err := codec.Decompress(compressed, len(tt.data))
			require.NoError(t, err)
			assert.Equal(t, tt.data, decompressed, "Round-trip should preserve data")
		})
	}
}

func TestZstdCodec_RoundTrip_RepeatedData(t *testing.T) {
	// Test with highly compressible data (repeated bytes)
	codec := NewZstdCodec(3)

	data := bytes.Repeat([]byte("AAAA"), 1000)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Zstd should compress repeated data very well
	compressionRatio := float64(len(data)) / float64(len(compressed))
	assert.Greater(t, compressionRatio, 10.0,
		"Zstd should achieve >10x compression for repeated data")

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestZstdCodec_RoundTrip_RandomData(t *testing.T) {
	// Test with incompressible data (random bytes)
	codec := NewZstdCodec(3)

	data := make([]byte, 10000)
	_, err := rand.Read(data)
	require.NoError(t, err)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Random data won't compress well, might even expand slightly
	// Just verify it works correctly
	assert.NotEmpty(t, compressed)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed, "Round-trip should preserve random data")
}

func TestZstdCodec_RoundTrip_StructuredData(t *testing.T) {
	// Test with JSON-like structured data
	codec := NewZstdCodec(3)

	jsonData := []byte(`{
		"users": [
			{"id": 1, "name": "Alice", "email": "alice@example.com"},
			{"id": 2, "name": "Bob", "email": "bob@example.com"},
			{"id": 3, "name": "Charlie", "email": "charlie@example.com"}
		],
		"count": 3,
		"success": true
	}`)

	// Repeat to make it more compressible
	data := bytes.Repeat(jsonData, 100)

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Structured data with repetition should compress well
	compressionRatio := float64(len(data)) / float64(len(compressed))
	assert.Greater(t, compressionRatio, 2.0,
		"Zstd should compress structured data reasonably well")

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestZstdCodec_RoundTrip_BinaryData(t *testing.T) {
	// Test with binary data (integers)
	codec := NewZstdCodec(3)

	data := make([]byte, 0, 10000)
	for i := 0; i < 1000; i++ {
		// Repeating pattern of integers
		for j := 0; j < 10; j++ {
			data = append(data, byte(i%256))
		}
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestZstdCodec_RoundTrip_LargeData(t *testing.T) {
	// Test with larger dataset
	codec := NewZstdCodec(3)

	// Create 1MB of semi-compressible data
	data := make([]byte, 1024*1024)
	for i := range data {
		// Mix of repeated and varied data
		data[i] = byte((i / 100) % 256)
	}

	compressed, err := codec.Compress(data)
	require.NoError(t, err)

	// Should achieve some compression
	assert.Less(t, len(compressed), len(data),
		"Should compress large dataset")

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestZstdCodec_DifferentLevels(t *testing.T) {
	// Compare compression ratios at different levels
	data := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 1000)

	levels := []int{1, 3, 5, 10}
	prevSize := len(data)

	for _, level := range levels {
		t.Run(string(rune('0'+level)), func(t *testing.T) {
			codec := NewZstdCodec(level)

			compressed, err := codec.Compress(data)
			require.NoError(t, err)

			// Higher levels should generally produce smaller output
			// (though not guaranteed for all data)
			assert.LessOrEqual(t, len(compressed), prevSize,
				"Higher compression level should not increase size significantly")

			// Verify decompression works
			decompressed, err := codec.Decompress(compressed, len(data))
			require.NoError(t, err)
			assert.Equal(t, data, decompressed)
		})
	}
}

func TestZstdCodec_PoolingBehavior(t *testing.T) {
	// Test that the encoder/decoder pooling works correctly
	codec := NewZstdCodec(3)

	data := []byte("test data for pooling")

	// Perform multiple compressions/decompressions
	for i := 0; i < 10; i++ {
		compressed, err := codec.Compress(data)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(data))
		require.NoError(t, err)
		assert.Equal(t, data, decompressed)
	}
}

func TestZstdCodec_ConcurrentAccess(t *testing.T) {
	// Test that the codec is safe for concurrent use
	codec := NewZstdCodec(3)

	data := []byte("concurrent test data")

	// Run multiple goroutines
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				compressed, err := codec.Compress(data)
				if err != nil {
					t.Errorf("compression failed: %v", err)
					done <- false
					return
				}

				decompressed, err := codec.Decompress(compressed, len(data))
				if err != nil {
					t.Errorf("decompression failed: %v", err)
					done <- false
					return
				}

				if !bytes.Equal(data, decompressed) {
					t.Errorf("data mismatch")
					done <- false
					return
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		assert.True(t, <-done, "Goroutine should succeed")
	}
}

func TestZstdCodec_Interface(_ *testing.T) {
	codec := NewZstdCodec(3)

	// Verify ZstdCodec implements all required interfaces
	var _ Codec = codec
	var _ Compressor = codec
	var _ Decompressor = codec
}

// Benchmarks

func BenchmarkZstdCodec_Compress_SmallData(b *testing.B) {
	codec := NewZstdCodec(3)
	data := []byte("The quick brown fox jumps over the lazy dog.")

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for range b.N {
		_, err := codec.Compress(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZstdCodec_Compress_RepeatedData(b *testing.B) {
	codec := NewZstdCodec(3)
	sizes := []int{1024, 10240, 102400}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("AAAA"), size/4)

		b.Run(formatSize(size), func(b *testing.B) {
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

func BenchmarkZstdCodec_Compress_RandomData(b *testing.B) {
	codec := NewZstdCodec(3)
	sizes := []int{1024, 10240, 102400}

	for _, size := range sizes {
		data := make([]byte, size)
		_, _ = rand.Read(data)

		b.Run(formatSize(size), func(b *testing.B) {
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

func BenchmarkZstdCodec_Decompress(b *testing.B) {
	codec := NewZstdCodec(3)
	sizes := []int{1024, 10240, 102400}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("test data "), size/10)
		compressed, _ := codec.Compress(data)

		b.Run(formatSize(size), func(b *testing.B) {
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

func BenchmarkZstdCodec_RoundTrip(b *testing.B) {
	codec := NewZstdCodec(3)
	sizes := []int{1024, 10240, 102400}

	for _, size := range sizes {
		data := bytes.Repeat([]byte("test data "), size/10)

		b.Run(formatSize(size), func(b *testing.B) {
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

func BenchmarkZstdCodec_Levels(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data "), 1000)
	levels := []int{1, 3, 5, 10}

	for _, level := range levels {
		codec := NewZstdCodec(level)

		b.Run(string(rune('0'+level)), func(b *testing.B) {
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
