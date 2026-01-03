package compression

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSSTCodec_Train(t *testing.T) {
	tests := []struct {
		name        string
		samples     [][]byte
		expectError bool
	}{
		{
			name: "simple repeated pattern",
			samples: [][]byte{
				[]byte("hello world hello world"),
				[]byte("hello there hello again"),
			},
			expectError: false,
		},
		{
			name: "single sample",
			samples: [][]byte{
				[]byte("the quick brown fox jumps over the lazy dog"),
			},
			expectError: false,
		},
		{
			name:        "empty samples",
			samples:     [][]byte{},
			expectError: true,
		},
		{
			name:        "empty data",
			samples:     [][]byte{[]byte("")},
			expectError: true,
		},
		{
			name: "binary data",
			samples: [][]byte{
				{0x01, 0x02, 0x03, 0x01, 0x02, 0x03, 0x01, 0x02, 0x03},
				{0x01, 0x02, 0x03, 0x04, 0x05},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewFSSTCodec()
			err := codec.Train(tt.samples)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.True(t, codec.trained)
				// Verify symbol table was created
				assert.NotEmpty(t, codec.codeTable)
			}
		})
	}
}

func TestFSSTCodec_CompressDecompress_RoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		samples  [][]byte
		destSize int
	}{
		{
			name:  "simple text",
			input: []byte("hello world"),
			samples: [][]byte{
				[]byte("hello world"),
				[]byte("hello there"),
			},
			destSize: 11,
		},
		{
			name:  "repeated pattern",
			input: []byte("abcabcabcabcabc"),
			samples: [][]byte{
				[]byte("abcabcabcabcabc"),
			},
			destSize: 15,
		},
		{
			name:  "long repeated sequence",
			input: []byte("the quick brown fox jumps over the lazy dog the quick brown fox"),
			samples: [][]byte{
				[]byte("the quick brown fox jumps over the lazy dog the quick brown fox"),
			},
			destSize: 63, // actual length of the string
		},
		{
			name:     "empty data",
			input:    []byte(""),
			samples:  [][]byte{[]byte("x")}, // Non-empty sample to avoid training error
			destSize: 0,
		},
		{
			name:  "single byte",
			input: []byte("x"),
			samples: [][]byte{
				[]byte("xyz"),
			},
			destSize: 1,
		},
		{
			name:  "binary data",
			input: []byte{0x01, 0x02, 0x03, 0x01, 0x02, 0x03},
			samples: [][]byte{
				{0x01, 0x02, 0x03, 0x01, 0x02, 0x03},
			},
			destSize: 6,
		},
		{
			name:  "no common patterns",
			input: []byte("abcdefghijklmnop"),
			samples: [][]byte{
				[]byte("abcdefghijklmnop"),
			},
			destSize: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewFSSTCodec()

			// Skip empty input tests for training
			if len(tt.input) == 0 {
				err := codec.Train(tt.samples)
				require.NoError(t, err)
			} else {
				err := codec.Train(tt.samples)
				require.NoError(t, err)
			}

			// Compress
			compressed, err := codec.Compress(tt.input)
			require.NoError(t, err)
			assert.NotNil(t, compressed)

			// Decompress
			decompressed, err := codec.Decompress(compressed, tt.destSize)
			require.NoError(t, err)

			// Verify round-trip
			assert.Equal(t, tt.input, decompressed)
		})
	}
}

func TestFSSTCodec_AutoTrain(t *testing.T) {
	// Test that compression works without explicit training
	codec := NewFSSTCodec()

	input := []byte("the quick brown fox jumps over the lazy dog")

	// Compress without training (should auto-train)
	compressed, err := codec.Compress(input)
	require.NoError(t, err)
	assert.NotNil(t, compressed)

	// Should now be trained
	assert.True(t, codec.trained)

	// Decompress
	decompressed, err := codec.Decompress(compressed, len(input))
	require.NoError(t, err)
	assert.Equal(t, input, decompressed)
}

func TestFSSTCodec_LargeData(t *testing.T) {
	// Test with larger dataset
	var builder strings.Builder
	pattern := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
	for i := 0; i < 100; i++ {
		builder.WriteString(pattern)
	}
	input := []byte(builder.String())

	codec := NewFSSTCodec()
	err := codec.Train([][]byte{input})
	require.NoError(t, err)

	// Compress
	compressed, err := codec.Compress(input)
	require.NoError(t, err)

	// Should achieve some compression due to repeated pattern
	t.Logf("Original size: %d, Compressed size: %d, Ratio: %.2f",
		len(input), len(compressed), float64(len(compressed))/float64(len(input)))

	// Decompress
	decompressed, err := codec.Decompress(compressed, len(input))
	require.NoError(t, err)
	assert.Equal(t, input, decompressed)
}

func TestFSSTCodec_Type(t *testing.T) {
	codec := NewFSSTCodec()
	assert.Equal(t, CompressionFSST, codec.Type())
}

func TestFSSTCodec_CompressWithTraining(t *testing.T) {
	codec := NewFSSTCodec()

	data := []byte("hello world hello again")
	samples := [][]byte{
		[]byte("hello world"),
		[]byte("hello there"),
	}

	compressed, err := codec.CompressWithTraining(data, samples)
	require.NoError(t, err)
	assert.NotNil(t, compressed)

	decompressed, err := codec.Decompress(compressed, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, decompressed)
}

func TestFSSTCodec_GetSymbolTableSize(t *testing.T) {
	codec := NewFSSTCodec()

	samples := [][]byte{
		[]byte("the quick brown fox jumps over the lazy dog"),
	}
	err := codec.Train(samples)
	require.NoError(t, err)

	size := codec.GetSymbolTableSize()
	assert.Greater(t, size, 0)

	t.Logf("Symbol table size: %d bytes, %d symbols", size, len(codec.codeTable))
}

func TestFSSTCodec_GetCompressionRatio(t *testing.T) {
	codec := NewFSSTCodec()

	input := []byte("the quick brown fox the quick brown fox")
	err := codec.Train([][]byte{input})
	require.NoError(t, err)

	ratio, err := codec.GetCompressionRatio(input)
	require.NoError(t, err)

	// Ratio should be less than 1.0 for compressible data
	t.Logf("Compression ratio: %.2f", ratio)
	assert.Greater(t, ratio, 0.0)
}

func TestFSSTCodec_MarshalUnmarshalSymbolTable(t *testing.T) {
	codec1 := NewFSSTCodec()

	samples := [][]byte{
		[]byte("the quick brown fox jumps over the lazy dog"),
		[]byte("the quick brown cat"),
	}
	err := codec1.Train(samples)
	require.NoError(t, err)

	// Marshal symbol table
	tableData, err := codec1.MarshalSymbolTable()
	require.NoError(t, err)
	assert.NotNil(t, tableData)

	// Create new codec and unmarshal
	codec2 := NewFSSTCodec()
	err = codec2.UnmarshalSymbolTable(tableData)
	require.NoError(t, err)

	// Verify symbol tables match
	assert.Equal(t, len(codec1.codeTable), len(codec2.codeTable))
	assert.Equal(t, len(codec1.symbolTable), len(codec2.symbolTable))

	for symbol, code := range codec1.symbolTable {
		assert.Equal(t, code, codec2.symbolTable[symbol])
	}

	// Verify compression/decompression works with unmarshaled table
	input := []byte("the quick brown fox")
	compressed, err := codec2.Compress(input)
	require.NoError(t, err)

	decompressed, err := codec2.Decompress(compressed, len(input))
	require.NoError(t, err)
	assert.Equal(t, input, decompressed)
}

func TestFSSTCodec_SpecialCases(t *testing.T) {
	t.Run("single character repeated", func(t *testing.T) {
		codec := NewFSSTCodec()
		input := bytes.Repeat([]byte("a"), 100)

		err := codec.Train([][]byte{input})
		require.NoError(t, err)

		compressed, err := codec.Compress(input)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(input))
		require.NoError(t, err)
		assert.Equal(t, input, decompressed)
	})

	t.Run("alternating pattern", func(t *testing.T) {
		codec := NewFSSTCodec()
		input := []byte("ababababababab")

		err := codec.Train([][]byte{input})
		require.NoError(t, err)

		compressed, err := codec.Compress(input)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(input))
		require.NoError(t, err)
		assert.Equal(t, input, decompressed)
	})

	t.Run("all unique characters", func(t *testing.T) {
		codec := NewFSSTCodec()
		input := []byte("abcdefghijklmnopqrstuvwxyz")

		err := codec.Train([][]byte{input})
		require.NoError(t, err)

		compressed, err := codec.Compress(input)
		require.NoError(t, err)

		// Compression may not be effective, but should still work
		decompressed, err := codec.Decompress(compressed, len(input))
		require.NoError(t, err)
		assert.Equal(t, input, decompressed)
	})

	t.Run("unicode text", func(t *testing.T) {
		codec := NewFSSTCodec()
		input := []byte("Hello 世界! Hello 世界!")

		err := codec.Train([][]byte{input})
		require.NoError(t, err)

		compressed, err := codec.Compress(input)
		require.NoError(t, err)

		decompressed, err := codec.Decompress(compressed, len(input))
		require.NoError(t, err)
		assert.Equal(t, input, decompressed)
	})
}

func TestFSSTCodec_ErrorCases(t *testing.T) {
	t.Run("decompress empty data", func(t *testing.T) {
		codec := NewFSSTCodec()
		_, err := codec.Decompress([]byte{}, 10)
		assert.Error(t, err)
	})

	t.Run("decompress with invalid symbol code", func(t *testing.T) {
		codec := NewFSSTCodec()

		// Create invalid compressed data: [0 symbols][invalid code 99]
		invalidData := []byte{0, 99}
		_, err := codec.Decompress(invalidData, 10)
		assert.Error(t, err)
	})

	t.Run("size mismatch", func(t *testing.T) {
		codec := NewFSSTCodec()
		input := []byte("hello world")

		err := codec.Train([][]byte{input})
		require.NoError(t, err)

		compressed, err := codec.Compress(input)
		require.NoError(t, err)

		// Request wrong destination size
		_, err = codec.Decompress(compressed, 999)
		assert.Error(t, err)
	})
}

func BenchmarkFSSTCodec_Compress(b *testing.B) {
	codec := NewFSSTCodec()

	// Create sample data with repeated patterns
	var builder strings.Builder
	pattern := "the quick brown fox jumps over the lazy dog "
	for i := 0; i < 100; i++ {
		builder.WriteString(pattern)
	}
	input := []byte(builder.String())

	// Train
	err := codec.Train([][]byte{input})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Compress(input)
	}
}

func BenchmarkFSSTCodec_Decompress(b *testing.B) {
	codec := NewFSSTCodec()

	// Create sample data with repeated patterns
	var builder strings.Builder
	pattern := "the quick brown fox jumps over the lazy dog "
	for i := 0; i < 100; i++ {
		builder.WriteString(pattern)
	}
	input := []byte(builder.String())

	// Train and compress
	err := codec.Train([][]byte{input})
	require.NoError(b, err)

	compressed, err := codec.Compress(input)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = codec.Decompress(compressed, len(input))
	}
}

func BenchmarkFSSTCodec_Train(b *testing.B) {
	// Create sample data
	samples := [][]byte{
		[]byte("the quick brown fox jumps over the lazy dog"),
		[]byte("the lazy cat sleeps all day long"),
		[]byte("quick brown fox and lazy dog play together"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec := NewFSSTCodec()
		_ = codec.Train(samples)
	}
}
