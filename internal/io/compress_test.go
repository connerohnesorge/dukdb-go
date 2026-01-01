package io

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closeReader is a helper to close a reader and check for errors in tests.
func closeReader(t *testing.T, r io.ReadCloser) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Errorf("failed to close reader: %v", err)
	}
}

func TestDetectCompressionFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected Compression
	}{
		// GZIP extensions
		{"data.csv.gz", CompressionGZIP},
		{"data.gzip", CompressionGZIP},
		{"DATA.CSV.GZ", CompressionGZIP},
		{"file.GZIP", CompressionGZIP},

		// ZSTD extensions
		{"data.zst", CompressionZSTD},
		{"data.zstd", CompressionZSTD},
		{"FILE.ZST", CompressionZSTD},

		// Snappy extensions
		{"data.snappy", CompressionSnappy},
		{"FILE.SNAPPY", CompressionSnappy},

		// LZ4 extensions
		{"data.lz4", CompressionLZ4},
		{"FILE.LZ4", CompressionLZ4},

		// Brotli extensions
		{"data.br", CompressionBrotli},
		{"data.brotli", CompressionBrotli},
		{"FILE.BR", CompressionBrotli},

		// No compression
		{"data.csv", CompressionNone},
		{"data.parquet", CompressionNone},
		{"data.json", CompressionNone},
		{"data", CompressionNone},
		{"", CompressionNone},

		// Full paths
		{"/path/to/data.csv.gz", CompressionGZIP},
		{"./relative/path/file.zst", CompressionZSTD},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := DetectCompressionFromPath(tt.path)
			assert.Equal(t, tt.expected, result, "path: %s", tt.path)
		})
	}
}

func TestDetectCompressionFromMagicBytes(t *testing.T) {
	tests := []struct {
		name     string
		header   []byte
		expected Compression
	}{
		{
			name:     "GZIP magic bytes",
			header:   []byte{0x1f, 0x8b, 0x08, 0x00},
			expected: CompressionGZIP,
		},
		{
			name:     "ZSTD magic bytes",
			header:   []byte{0x28, 0xb5, 0x2f, 0xfd},
			expected: CompressionZSTD,
		},
		{
			name:     "LZ4 magic bytes",
			header:   []byte{0x04, 0x22, 0x4d, 0x18},
			expected: CompressionLZ4,
		},
		{
			name:     "Plain text (no compression)",
			header:   []byte("hello world"),
			expected: CompressionNone,
		},
		{
			name:     "Empty header",
			header:   make([]byte, 0),
			expected: CompressionNone,
		},
		{
			name:     "Short header",
			header:   []byte{0x1f},
			expected: CompressionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.header)
			compression, newReader, err := DetectCompression(reader)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, compression)

			// Verify the returned reader still has all the original data
			readBack, err := io.ReadAll(newReader)
			require.NoError(t, err)
			assert.Equal(t, tt.header, readBack)
		})
	}
}

func TestGZIPRoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for GZIP compression!")

	// Compress
	var compressed bytes.Buffer
	writer, err := NewCompressWriter(&compressed, CompressionGZIP)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify magic bytes
	assert.True(t, bytes.HasPrefix(compressed.Bytes(), gzipMagic))

	// Decompress
	reader, err := NewDecompressReader(&compressed, CompressionGZIP)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestZSTDRoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for ZSTD compression!")

	// Compress
	var compressed bytes.Buffer
	writer, err := NewCompressWriter(&compressed, CompressionZSTD)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify magic bytes
	assert.True(t, bytes.HasPrefix(compressed.Bytes(), zstdMagic))

	// Decompress
	reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), CompressionZSTD)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestSnappyRoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for Snappy compression!")

	// Compress
	var compressed bytes.Buffer
	writer, err := NewCompressWriter(&compressed, CompressionSnappy)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Decompress
	reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), CompressionSnappy)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestLZ4RoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for LZ4 compression!")

	// Compress
	var compressed bytes.Buffer
	writer, err := NewCompressWriter(&compressed, CompressionLZ4)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Verify magic bytes
	assert.True(t, bytes.HasPrefix(compressed.Bytes(), lz4Magic))

	// Decompress
	reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), CompressionLZ4)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestBrotliRoundTrip(t *testing.T) {
	original := []byte("Hello, this is test data for Brotli compression!")

	// Compress
	var compressed bytes.Buffer
	writer, err := NewCompressWriter(&compressed, CompressionBrotli)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Decompress
	reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), CompressionBrotli)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestNoCompressionRoundTrip(t *testing.T) {
	original := []byte("Hello, this is uncompressed data!")

	// Write with no compression (no-op passthrough)
	var output bytes.Buffer
	writer, err := NewCompressWriter(&output, CompressionNone)
	require.NoError(t, err)

	_, err = writer.Write(original)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Should be unchanged
	assert.Equal(t, original, output.Bytes())

	// Read with no decompression (no-op passthrough)
	reader, err := NewDecompressReader(&output, CompressionNone)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestDetectAndDecompress(t *testing.T) {
	original := []byte("Test data for auto-detection of compression format!")

	// Test GZIP auto-detection
	t.Run("GZIP auto-detect", func(t *testing.T) {
		var compressed bytes.Buffer
		gzWriter := gzip.NewWriter(&compressed)
		_, err := gzWriter.Write(original)
		require.NoError(t, err)
		require.NoError(t, gzWriter.Close())

		// Detect and decompress
		compression, reader, err := DetectCompression(bytes.NewReader(compressed.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, CompressionGZIP, compression)

		decompReader, err := NewDecompressReader(reader, compression)
		require.NoError(t, err)
		defer closeReader(t, decompReader)

		decompressed, err := io.ReadAll(decompReader)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	// Test ZSTD auto-detection
	t.Run("ZSTD auto-detect", func(t *testing.T) {
		var compressed bytes.Buffer
		zstdWriter, err := zstd.NewWriter(&compressed)
		require.NoError(t, err)
		_, err = zstdWriter.Write(original)
		require.NoError(t, err)
		require.NoError(t, zstdWriter.Close())

		// Detect and decompress
		compression, reader, err := DetectCompression(bytes.NewReader(compressed.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, CompressionZSTD, compression)

		decompReader, err := NewDecompressReader(reader, compression)
		require.NoError(t, err)
		defer closeReader(t, decompReader)

		decompressed, err := io.ReadAll(decompReader)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})

	// Test LZ4 auto-detection
	t.Run("LZ4 auto-detect", func(t *testing.T) {
		var compressed bytes.Buffer
		lz4Writer := lz4.NewWriter(&compressed)
		_, err := lz4Writer.Write(original)
		require.NoError(t, err)
		require.NoError(t, lz4Writer.Close())

		// Detect and decompress
		compression, reader, err := DetectCompression(bytes.NewReader(compressed.Bytes()))
		require.NoError(t, err)
		assert.Equal(t, CompressionLZ4, compression)

		decompReader, err := NewDecompressReader(reader, compression)
		require.NoError(t, err)
		defer closeReader(t, decompReader)

		decompressed, err := io.ReadAll(decompReader)
		require.NoError(t, err)
		assert.Equal(t, original, decompressed)
	})
}

func TestUnsupportedCompression(t *testing.T) {
	// Use a compression value that's not in the switch
	unsupported := Compression(999)

	_, err := NewDecompressReader(bytes.NewReader(make([]byte, 0)), unsupported)
	assert.ErrorIs(t, err, ErrUnsupportedCompression)

	_, err = NewCompressWriter(&bytes.Buffer{}, unsupported)
	assert.ErrorIs(t, err, ErrUnsupportedCompression)
}

func TestLargeDataRoundTrip(t *testing.T) {
	// Create a large data set (1MB)
	original := make([]byte, 1024*1024)
	for i := range original {
		original[i] = byte(i % 256)
	}

	compressions := []Compression{
		CompressionGZIP,
		CompressionZSTD,
		CompressionSnappy,
		CompressionLZ4,
		CompressionBrotli,
	}

	for _, c := range compressions {
		t.Run(c.String(), func(t *testing.T) {
			// Compress
			var compressed bytes.Buffer
			writer, err := NewCompressWriter(&compressed, c)
			require.NoError(t, err)

			_, err = writer.Write(original)
			require.NoError(t, err)
			err = writer.Close()
			require.NoError(t, err)

			// Verify compression actually reduced size (or at least works)
			t.Logf("%s: %d -> %d bytes (%.1f%%)",
				c.String(), len(original), compressed.Len(),
				float64(compressed.Len())/float64(len(original))*100)

			// Decompress
			reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), c)
			require.NoError(t, err)
			defer closeReader(t, reader)

			decompressed, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, original, decompressed)
		})
	}
}

func TestCompressionString(t *testing.T) {
	tests := []struct {
		compression Compression
		expected    string
	}{
		{CompressionNone, "none"},
		{CompressionGZIP, "gzip"},
		{CompressionZSTD, "zstd"},
		{CompressionSnappy, "snappy"},
		{CompressionLZ4, "lz4"},
		{CompressionBrotli, "brotli"},
		{Compression(999), unknownStr},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.compression.String())
		})
	}
}

func TestSnappyFramingFormat(t *testing.T) {
	// Test with data compressed using snappy's framing format
	original := []byte("Test data for snappy framing format compression!")

	var compressed bytes.Buffer
	sw := snappy.NewBufferedWriter(&compressed)
	_, err := sw.Write(original)
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	// Snappy framing format starts with stream identifier
	// The first byte should be 0xff for the stream identifier chunk
	if compressed.Len() > 0 {
		t.Logf("Snappy first byte: 0x%02x", compressed.Bytes()[0])
	}

	// Decompress
	reader, err := NewDecompressReader(bytes.NewReader(compressed.Bytes()), CompressionSnappy)
	require.NoError(t, err)
	defer closeReader(t, reader)

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestBrotliNoMagicBytes(t *testing.T) {
	// Brotli has no reliable magic bytes, so we rely on extension detection
	original := []byte("Test data for brotli compression!")

	var compressed bytes.Buffer
	bw := brotli.NewWriter(&compressed)
	_, err := bw.Write(original)
	require.NoError(t, err)
	require.NoError(t, bw.Close())

	// Detection from magic bytes should return CompressionNone for brotli
	compression, _, err := DetectCompression(bytes.NewReader(compressed.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, CompressionNone, compression,
		"Brotli should not be detected from magic bytes")

	// But extension detection should work
	assert.Equal(t, CompressionBrotli, DetectCompressionFromPath("data.br"))
	assert.Equal(t, CompressionBrotli, DetectCompressionFromPath("data.brotli"))
}
