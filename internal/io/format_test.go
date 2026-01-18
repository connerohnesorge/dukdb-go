package io

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectFormatFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected Format
	}{
		// CSV extensions
		{"data.csv", FormatCSV},
		{"data.CSV", FormatCSV},
		{"data.tsv", FormatCSV},
		{"data.TSV", FormatCSV},

		// Parquet extensions
		{"data.parquet", FormatParquet},
		{"data.PARQUET", FormatParquet},
		{"data.pq", FormatParquet},
		{"data.PQ", FormatParquet},

		// JSON extensions
		{"data.json", FormatJSON},
		{"data.JSON", FormatJSON},

		// NDJSON extensions
		{"data.ndjson", FormatNDJSON},
		{"data.NDJSON", FormatNDJSON},
		{"data.jsonl", FormatNDJSON},
		{"data.JSONL", FormatNDJSON},

		// XLSX extensions
		{"data.xlsx", FormatXLSX},
		{"data.XLSX", FormatXLSX},
		{"/path/to/workbook.xlsx", FormatXLSX},

		// With compression extensions
		{"data.csv.gz", FormatCSV},
		{"data.csv.gzip", FormatCSV},
		{"data.csv.zst", FormatCSV},
		{"data.csv.zstd", FormatCSV},
		{"data.csv.snappy", FormatCSV},
		{"data.csv.lz4", FormatCSV},
		{"data.csv.br", FormatCSV},
		{"data.csv.brotli", FormatCSV},
		{"data.parquet.gz", FormatParquet},
		{"data.json.zst", FormatJSON},
		{"data.ndjson.gz", FormatNDJSON},

		// Full paths
		{"/path/to/data.csv", FormatCSV},
		{"./relative/path/file.parquet", FormatParquet},
		{"C:\\Windows\\data.json", FormatJSON},

		// Unknown extensions
		{"data.txt", FormatUnknown},
		{"data.dat", FormatUnknown},
		{"data", FormatUnknown},
		{"", FormatUnknown},
		{"noextension", FormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := DetectFormatFromPath(tt.path)
			assert.Equal(t, tt.expected, result, "path: %s", tt.path)
		})
	}
}

func TestDetectFormatFromMagicBytes(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		expected Format
	}{
		{
			name:     "Parquet magic bytes",
			content:  []byte("PAR1" + "more content here..."),
			expected: FormatParquet,
		},
		{
			name:     "Parquet magic bytes (hex)",
			content:  []byte{0x50, 0x41, 0x52, 0x31, 0x00, 0x00, 0x00, 0x00},
			expected: FormatParquet,
		},
		{
			name:     "JSON array",
			content:  []byte(`[{"id": 1}, {"id": 2}]`),
			expected: FormatJSON,
		},
		{
			name:     "JSON array with leading whitespace",
			content:  []byte("  \n\t[{\"id\": 1}, {\"id\": 2}]"),
			expected: FormatJSON,
		},
		{
			name:     "JSON object",
			content:  []byte(`{"id": 1, "name": "test"}`),
			expected: FormatJSON,
		},
		{
			name:     "JSON object with leading whitespace",
			content:  []byte("  \n{\"id\": 1}"),
			expected: FormatJSON,
		},
		{
			name:     "NDJSON - multiple objects on separate lines",
			content:  []byte("{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}"),
			expected: FormatNDJSON,
		},
		{
			name:     "NDJSON with CRLF",
			content:  []byte("{\"id\": 1}\r\n{\"id\": 2}"),
			expected: FormatNDJSON,
		},
		{
			name:     "CSV content",
			content:  []byte("id,name,value\n1,alice,100\n2,bob,200"),
			expected: FormatCSV,
		},
		{
			name:     "CSV with header only",
			content:  []byte("id,name,value"),
			expected: FormatCSV,
		},
		{
			name:     "Empty content",
			content:  make([]byte, 0),
			expected: FormatCSV,
		},
		{
			name:     "Whitespace only",
			content:  []byte("   \n\t  "),
			expected: FormatCSV,
		},
		{
			name:     "Plain text (fallback to CSV)",
			content:  []byte("hello world this is some text"),
			expected: FormatCSV,
		},
		{
			name:     "ZIP/XLSX magic bytes (PK..)",
			content:  []byte{0x50, 0x4B, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00},
			expected: FormatXLSX,
		},
		{
			name:     "XLSX magic bytes with more content",
			content:  append([]byte{0x50, 0x4B, 0x03, 0x04}, []byte("more zip content here...")...),
			expected: FormatXLSX,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.content)
			format, newReader, err := DetectFormat(reader)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, format)

			// Verify the returned reader still has all the original data
			readBack, err := io.ReadAll(newReader)
			require.NoError(t, err)
			assert.Equal(t, tt.content, readBack)
		})
	}
}

func TestDetectFormatPreservesReaderContent(t *testing.T) {
	// Verify that after format detection, the reader can still be used
	// to read all the original content
	testCases := [][]byte{
		[]byte("PAR1" + strings.Repeat("x", 1000)), // Parquet
		[]byte(`[{"a":1},{"a":2}]`),                // JSON array
		[]byte("{\"a\":1}\n{\"a\":2}\n"),           // NDJSON
		[]byte("a,b,c\n1,2,3\n4,5,6"),              // CSV
	}

	for i, content := range testCases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			reader := bytes.NewReader(content)
			_, newReader, err := DetectFormat(reader)
			require.NoError(t, err)

			// Read all content from the returned reader
			readBack, err := io.ReadAll(newReader)
			require.NoError(t, err)
			assert.Equal(t, content, readBack)
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected Format
	}{
		{"csv", FormatCSV},
		{"CSV", FormatCSV},
		{"  csv  ", FormatCSV},
		{"parquet", FormatParquet},
		{"PARQUET", FormatParquet},
		{"json", FormatJSON},
		{"JSON", FormatJSON},
		{"ndjson", FormatNDJSON},
		{"NDJSON", FormatNDJSON},
		{"jsonl", FormatNDJSON},
		{"JSONL", FormatNDJSON},
		{"newline_delimited", FormatNDJSON},
		{"xlsx", FormatXLSX},
		{"XLSX", FormatXLSX},
		{"excel", FormatXLSX},
		{"EXCEL", FormatXLSX},
		{"unknown", FormatUnknown},
		{"", FormatUnknown},
		{"xml", FormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseFormat(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatString(t *testing.T) {
	// Test the String() method of Format
	tests := []struct {
		format   Format
		expected string
	}{
		{FormatUnknown, "unknown"},
		{FormatCSV, "csv"},
		{FormatJSON, "json"},
		{FormatNDJSON, "ndjson"},
		{FormatParquet, "parquet"},
		{FormatXLSX, "xlsx"},
		{Format(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.format.String())
		})
	}
}

func TestRemoveCompressionExtension(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"data.csv.gz", "data.csv"},
		{"data.csv.gzip", "data.csv"},
		{"data.csv.zst", "data.csv"},
		{"data.csv.zstd", "data.csv"},
		{"data.csv.snappy", "data.csv"},
		{"data.csv.lz4", "data.csv"},
		{"data.csv.br", "data.csv"},
		{"data.csv.brotli", "data.csv"},
		{"data.csv.GZ", "data.csv"}, // Case insensitive check in suffix
		{"data.csv", "data.csv"},    // No compression extension
		{"data.parquet", "data.parquet"},
		{"data", "data"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := removeCompressionExtension(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLikelyNDJSON(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		expected bool
	}{
		{
			name:     "Two JSON objects on separate lines",
			content:  []byte("{\"a\":1}\n{\"b\":2}"),
			expected: true,
		},
		{
			name:     "Single JSON object with newline and another object",
			content:  []byte("{\"id\":1}\n{\"id\":2}\n"),
			expected: true,
		},
		{
			name:     "Single JSON object without trailing content",
			content:  []byte("{\"a\":1}\n"),
			expected: true,
		},
		{
			name:     "JSON array element with comma (not NDJSON)",
			content:  []byte("{\"a\":1},"),
			expected: false,
		},
		{
			name:     "No newline at all",
			content:  []byte("{\"a\":1}"),
			expected: false,
		},
		{
			name:     "Empty content",
			content:  make([]byte, 0),
			expected: false,
		},
		{
			name:     "Whitespace only",
			content:  []byte("   \n  "),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLikelyNDJSON(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectFormatPriority(t *testing.T) {
	// Test that Parquet magic bytes take precedence over JSON-like content
	// This tests the case where content might look like JSON but has Parquet magic
	t.Run("Parquet takes precedence", func(t *testing.T) {
		// PAR1 followed by what looks like JSON (unlikely in real files but tests priority)
		content := []byte("PAR1{\"should\":\"not matter\"}")
		reader := bytes.NewReader(content)
		format, _, err := DetectFormat(reader)
		require.NoError(t, err)
		assert.Equal(t, FormatParquet, format)
	})
}
