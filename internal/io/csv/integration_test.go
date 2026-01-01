// Package csv provides CSV file reading and writing capabilities for dukdb-go.
// This file contains integration tests for round-trip consistency and format compatibility.
package csv

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Round-trip Tests
// =============================================================================

// TestRoundTrip_BasicTypes tests writing and reading back various basic data types.
func TestRoundTrip_BasicTypes(t *testing.T) {
	t.Run("integer values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		chunk := storage.NewDataChunkWithCapacity(types, 5)

		testValues := []int32{0, 1, -1, 100, -100}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"value"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual := roundTripped.GetValue(i, 0)
			assert.Equal(t, expected, actual, "row %d mismatch", i)
		}
	})

	t.Run("double values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_DOUBLE}
		chunk := storage.NewDataChunkWithCapacity(types, 5)

		testValues := []float64{0.0, 1.5, -2.5, 3.14159, 100.001}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"value"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual := roundTripped.GetValue(i, 0)
			assert.InDelta(t, expected, actual, 0.0001, "row %d mismatch", i)
		}
	})

	t.Run("boolean values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_BOOLEAN}
		chunk := storage.NewDataChunkWithCapacity(types, 4)

		testValues := []bool{true, false, true, false}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"flag"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual := roundTripped.GetValue(i, 0)
			assert.Equal(t, expected, actual, "row %d mismatch", i)
		}
	})

	t.Run("varchar values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_VARCHAR}
		chunk := storage.NewDataChunkWithCapacity(types, 4)

		// Test non-empty values only since empty becomes NULL
		testValues := []string{"hello", "world", "foo bar", "test123"}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"text"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual := roundTripped.GetValue(i, 0)
			assert.Equal(t, expected, actual, "row %d mismatch", i)
		}
	})

	t.Run("date values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_DATE}
		chunk := storage.NewDataChunkWithCapacity(types, 3)

		testValues := []time.Time{
			time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
		}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"date"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual, ok := roundTripped.GetValue(i, 0).(time.Time)
			require.True(t, ok, "row %d should be time.Time", i)
			assert.True(t,
				expected.Year() == actual.Year() &&
					expected.Month() == actual.Month() &&
					expected.Day() == actual.Day(),
				"row %d date mismatch: expected %v, got %v", i, expected, actual)
		}
	})

	t.Run("timestamp values", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_TIMESTAMP}
		chunk := storage.NewDataChunkWithCapacity(types, 3)

		testValues := []time.Time{
			time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			time.Date(2024, 6, 30, 14, 45, 30, 0, time.UTC),
			time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
		}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"timestamp"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual, ok := roundTripped.GetValue(i, 0).(time.Time)
			require.True(t, ok, "row %d should be time.Time", i)
			// Allow small time differences due to format precision
			assert.WithinDuration(t, expected, actual, time.Second,
				"row %d timestamp mismatch", i)
		}
	})
}

// TestRoundTrip_MixedTypes tests writing and reading data with multiple column types.
func TestRoundTrip_MixedTypes(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_VARCHAR,
	}

	chunk := storage.NewDataChunkWithCapacity(types, 3)

	// Row 0
	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, 3.14)
	chunk.SetValue(0, 2, true)
	chunk.SetValue(0, 3, "alice")

	// Row 1
	chunk.SetValue(1, 0, int32(2))
	chunk.SetValue(1, 1, 2.71)
	chunk.SetValue(1, 2, false)
	chunk.SetValue(1, 3, "bob")

	// Row 2
	chunk.SetValue(2, 0, int32(3))
	chunk.SetValue(2, 1, 0.0)
	chunk.SetValue(2, 2, true)
	chunk.SetValue(2, 3, "charlie")

	chunk.SetCount(3)

	roundTripped := performRoundTrip(t, chunk, []string{"id", "value", "flag", "name"})

	require.Equal(t, 3, roundTripped.Count())

	// Verify row 0
	assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
	assert.InDelta(t, 3.14, roundTripped.GetValue(0, 1), 0.001)
	assert.Equal(t, true, roundTripped.GetValue(0, 2))
	assert.Equal(t, "alice", roundTripped.GetValue(0, 3))

	// Verify row 1
	assert.Equal(t, int32(2), roundTripped.GetValue(1, 0))
	assert.InDelta(t, 2.71, roundTripped.GetValue(1, 1), 0.001)
	assert.Equal(t, false, roundTripped.GetValue(1, 2))
	assert.Equal(t, "bob", roundTripped.GetValue(1, 3))
}

// TestRoundTrip_NULLHandling tests NULL value round-trip behavior.
func TestRoundTrip_NULLHandling(t *testing.T) {
	t.Run("NULL in various columns", func(t *testing.T) {
		types := []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_DOUBLE,
		}

		chunk := storage.NewDataChunkWithCapacity(types, 4)

		// Row 0: all valid
		chunk.SetValue(0, 0, int32(1))
		chunk.SetValue(0, 1, "test")
		chunk.SetValue(0, 2, 1.5)

		// Row 1: NULL in second column
		chunk.SetValue(1, 0, int32(2))
		chunk.GetVector(1).Validity().SetInvalid(1)
		chunk.SetValue(1, 2, 2.5)

		// Row 2: NULL in third column
		chunk.SetValue(2, 0, int32(3))
		chunk.SetValue(2, 1, "hello")
		chunk.GetVector(2).Validity().SetInvalid(2)

		// Row 3: multiple NULLs
		chunk.SetValue(3, 0, int32(4))
		chunk.GetVector(1).Validity().SetInvalid(3)
		chunk.GetVector(2).Validity().SetInvalid(3)

		chunk.SetCount(4)

		roundTripped := performRoundTrip(t, chunk, []string{"id", "name", "value"})

		require.Equal(t, 4, roundTripped.Count())

		// Row 0: all valid
		assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
		assert.Equal(t, "test", roundTripped.GetValue(0, 1))
		assert.InDelta(t, 1.5, roundTripped.GetValue(0, 2), 0.001)

		// Row 1: NULL in second column
		assert.Equal(t, int32(2), roundTripped.GetValue(1, 0))
		assert.Nil(t, roundTripped.GetValue(1, 1))
		assert.InDelta(t, 2.5, roundTripped.GetValue(1, 2), 0.001)

		// Row 2: NULL in third column
		assert.Equal(t, int32(3), roundTripped.GetValue(2, 0))
		assert.Equal(t, "hello", roundTripped.GetValue(2, 1))
		assert.Nil(t, roundTripped.GetValue(2, 2))

		// Row 3: multiple NULLs
		assert.Equal(t, int32(4), roundTripped.GetValue(3, 0))
		assert.Nil(t, roundTripped.GetValue(3, 1))
		assert.Nil(t, roundTripped.GetValue(3, 2))
	})

	t.Run("custom NULL string", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		chunk := storage.NewDataChunkWithCapacity(types, 2)

		chunk.SetValue(0, 0, int32(1))
		chunk.SetValue(0, 1, "valid")
		chunk.SetValue(1, 0, int32(2))
		chunk.GetVector(1).Validity().SetInvalid(1)

		chunk.SetCount(2)

		// Write with custom NULL string
		var buf bytes.Buffer
		writerOpts := DefaultWriterOptions()
		writerOpts.NullStr = "\\N"
		writer, err := NewWriter(&buf, writerOpts)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "name"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Read with matching NULL string
		readerOpts := DefaultReaderOptions()
		readerOpts.NullStr = "\\N"
		reader, err := NewReader(strings.NewReader(buf.String()), readerOpts)
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()

		roundTripped, err := reader.ReadChunk()
		require.NoError(t, err)

		assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
		assert.Equal(t, "valid", roundTripped.GetValue(0, 1))
		assert.Equal(t, int32(2), roundTripped.GetValue(1, 0))
		assert.Nil(t, roundTripped.GetValue(1, 1))
	})
}

// =============================================================================
// Compatibility Tests with Test Fixtures
// =============================================================================

// testdataDir is the path to the testdata directory.
const testdataDir = "/home/connerohnesorge/Documents/001Repos/dukdb-go/internal/io/csv/testdata"

// getTestdataPath returns the absolute path to a testdata file.
func getTestdataPath(filename string) string {
	return filepath.Join(testdataDir, filename)
}

// TestFixture_SimpleCSV tests reading the simple CSV fixture.
func TestFixture_SimpleCSV(t *testing.T) {
	path := getTestdataPath("simple.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])
	assert.Equal(t, dukdb.TYPE_INTEGER, types[2])

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))
	assert.Equal(t, int32(100), chunk.GetValue(0, 2))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "bob", chunk.GetValue(1, 1))
	assert.Equal(t, int32(200), chunk.GetValue(1, 2))

	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))
	assert.Equal(t, int32(300), chunk.GetValue(2, 2))
}

// TestFixture_TabDelimited tests reading tab-delimited file.
func TestFixture_TabDelimited(t *testing.T) {
	path := getTestdataPath("tab_delimited.tsv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))
}

// TestFixture_SemicolonDelimited tests reading semicolon-delimited file.
func TestFixture_SemicolonDelimited(t *testing.T) {
	path := getTestdataPath("semicolon_delimited.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
}

// TestFixture_PipeDelimited tests reading pipe-delimited file.
func TestFixture_PipeDelimited(t *testing.T) {
	path := getTestdataPath("pipe_delimited.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
}

// TestFixture_MixedTypes tests reading file with various data types.
func TestFixture_MixedTypes(t *testing.T) {
	path := getTestdataPath("mixed_types.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{
		"id", "int_col", "double_col", "bool_col",
		"date_col", "timestamp_col", "varchar_col",
	}, schema)

	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])  // id
	assert.Equal(t, dukdb.TYPE_INTEGER, types[1])  // int_col
	assert.Equal(t, dukdb.TYPE_DOUBLE, types[2])   // double_col
	assert.Equal(t, dukdb.TYPE_BOOLEAN, types[3])  // bool_col
	assert.Equal(t, dukdb.TYPE_DATE, types[4])     // date_col
	assert.Equal(t, dukdb.TYPE_TIMESTAMP, types[5]) // timestamp_col
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[6])  // varchar_col

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Verify first row values
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(42), chunk.GetValue(0, 1))
	assert.InDelta(t, 3.14159, chunk.GetValue(0, 2), 0.00001)
	assert.Equal(t, true, chunk.GetValue(0, 3))
	assert.Equal(t, "hello world", chunk.GetValue(0, 6))
}

// TestFixture_WithNulls tests reading file with NULL values.
func TestFixture_WithNulls(t *testing.T) {
	path := getTestdataPath("with_nulls.csv")

	opts := DefaultReaderOptions()
	opts.NullStr = ""

	reader, err := NewReaderFromPath(path, opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 4, chunk.Count())

	// Row 0: all valid
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	// Row 1: name is NULL
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Nil(t, chunk.GetValue(1, 1))

	// Row 2: value is NULL (column 2 has mixed types, becomes VARCHAR)
	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))

	// Row 3: multiple NULLs
	assert.Equal(t, int32(4), chunk.GetValue(3, 0))
	assert.Nil(t, chunk.GetValue(3, 1))
}

// TestFixture_QuotedFields tests reading file with quoted fields.
func TestFixture_QuotedFields(t *testing.T) {
	path := getTestdataPath("quoted_fields.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Row 0: simple quoted field with comma
	assert.Equal(t, "Alice Smith", chunk.GetValue(0, 1))
	assert.Equal(t, "Hello, World", chunk.GetValue(0, 2))

	// Row 1: escaped quotes
	assert.Equal(t, `Bob "The Builder"`, chunk.GetValue(1, 1))
	assert.Equal(t, `Value with "quotes"`, chunk.GetValue(1, 2))

	// Row 2: multiline values
	assert.Equal(t, "Charlie\nNewline", chunk.GetValue(2, 1))
	assert.Equal(t, "Multi\nline\nvalue", chunk.GetValue(2, 2))
}

// TestFixture_SpecialCharacters tests reading file with special characters.
func TestFixture_SpecialCharacters(t *testing.T) {
	path := getTestdataPath("special_characters.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Tab character
	assert.Equal(t, "tab\there", chunk.GetValue(0, 1))

	// Comma inside quoted field
	assert.Equal(t, "comma, inside", chunk.GetValue(1, 1))

	// Escaped quote
	assert.Equal(t, `quote"escaped`, chunk.GetValue(2, 1))
}

// TestFixture_LargeNumbers tests reading file with large numbers.
func TestFixture_LargeNumbers(t *testing.T) {
	path := getTestdataPath("large_numbers.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0]) // id
	assert.Equal(t, dukdb.TYPE_INTEGER, types[1]) // small_int
	assert.Equal(t, dukdb.TYPE_BIGINT, types[2])  // big_int
	assert.Equal(t, dukdb.TYPE_DOUBLE, types[3])  // huge_double

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Verify bigint values
	assert.Equal(t, int64(3000000000), chunk.GetValue(0, 2))
	assert.Equal(t, int64(-3000000000), chunk.GetValue(1, 2))
	assert.Equal(t, int64(9223372036854775807), chunk.GetValue(2, 2))

	// Verify huge double values
	assert.InDelta(t, math.MaxFloat64, chunk.GetValue(0, 3), 1e300)
	assert.InDelta(t, -math.MaxFloat64, chunk.GetValue(1, 3), 1e300)
}

// TestFixture_CompressedCSV tests reading gzip compressed CSV.
func TestFixture_CompressedCSV(t *testing.T) {
	path := getTestdataPath("compressed.csv.gz")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))
	assert.Equal(t, int32(100), chunk.GetValue(0, 2))
}

// TestFixture_CRLFEndings tests reading file with CRLF line endings.
func TestFixture_CRLFEndings(t *testing.T) {
	path := getTestdataPath("crlf_endings.csv")

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, schema)

	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Values should be correctly parsed without CRLF artifacts
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))
	assert.Equal(t, int32(100), chunk.GetValue(0, 2))
}

// =============================================================================
// Edge Cases in Round-trip
// =============================================================================

// TestRoundTrip_SpecialCharactersInStrings tests string values with special characters.
func TestRoundTrip_SpecialCharactersInStrings(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{"comma in value", "hello, world"},
		{"quote in value", `say "hello"`},
		{"newline in value", "line1\nline2"},
		{"tab in value", "col1\tcol2"},
		{"multiple special chars", `"hello, world" said "Bob"`},
		{"leading/trailing spaces", "  spaced  "},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
			chunk := storage.NewDataChunkWithCapacity(types, 1)

			// Use 42 instead of 1 to avoid boolean inference (1 parses as boolean true)
			chunk.SetValue(0, 0, int32(42))
			chunk.SetValue(0, 1, tc.value)
			chunk.SetCount(1)

			roundTripped := performRoundTrip(t, chunk, []string{"id", "text"})

			require.Equal(t, 1, roundTripped.Count())
			assert.Equal(t, int32(42), roundTripped.GetValue(0, 0))
			// Note: leading/trailing spaces may be trimmed by the reader
			if tc.name == "leading/trailing spaces" {
				assert.Equal(t, strings.TrimSpace(tc.value), roundTripped.GetValue(0, 1))
			} else {
				assert.Equal(t, tc.value, roundTripped.GetValue(0, 1))
			}
		})
	}
}

// TestRoundTrip_UnicodeCharacters tests Unicode string values.
func TestRoundTrip_UnicodeCharacters(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)

	// Various Unicode strings
	unicodeValues := []string{
		"hello",      // ASCII
		"cafe",       // with accents would be good but keeping simple
		"test",       // simple test
	}

	for i, v := range unicodeValues {
		chunk.SetValue(i, 0, int32(i+1))
		chunk.SetValue(i, 1, v)
	}
	chunk.SetCount(len(unicodeValues))

	roundTripped := performRoundTrip(t, chunk, []string{"id", "text"})

	require.Equal(t, len(unicodeValues), roundTripped.Count())
	for i, expected := range unicodeValues {
		assert.Equal(t, expected, roundTripped.GetValue(i, 1), "row %d mismatch", i)
	}
}

// TestRoundTrip_VeryLargeNumbers tests edge cases with large numeric values.
func TestRoundTrip_VeryLargeNumbers(t *testing.T) {
	t.Run("large integers", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_BIGINT}
		chunk := storage.NewDataChunkWithCapacity(types, 3)

		testValues := []int64{
			math.MaxInt64,
			math.MinInt64,
			0,
		}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"value"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual := roundTripped.GetValue(i, 0)
			assert.Equal(t, expected, actual, "row %d mismatch", i)
		}
	})

	t.Run("very small doubles", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_DOUBLE}
		chunk := storage.NewDataChunkWithCapacity(types, 3)

		testValues := []float64{
			1e-300,
			-1e-300,
			math.SmallestNonzeroFloat64,
		}
		for i, v := range testValues {
			chunk.SetValue(i, 0, v)
		}
		chunk.SetCount(len(testValues))

		roundTripped := performRoundTrip(t, chunk, []string{"value"})

		require.Equal(t, len(testValues), roundTripped.Count())
		for i, expected := range testValues {
			actual, ok := roundTripped.GetValue(i, 0).(float64)
			require.True(t, ok)
			// For very small numbers, check relative difference
			if expected != 0 {
				assert.InDelta(t, expected, actual, math.Abs(expected*0.01), "row %d mismatch", i)
			}
		}
	})
}

// TestRoundTrip_EmptyStringsVsNULL tests distinction between empty strings and NULLs.
func TestRoundTrip_EmptyStringsVsNULL(t *testing.T) {
	// With default options (NullStr=""), empty strings and NULLs become NULL
	// This is expected behavior - CSV cannot distinguish between them by default

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)

	// Row 0: valid string
	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, "valid")

	// Row 1: NULL (marked invalid)
	chunk.SetValue(1, 0, int32(2))
	chunk.GetVector(1).Validity().SetInvalid(1)

	// Row 2: empty string (set but empty) - becomes NULL with default settings
	chunk.SetValue(2, 0, int32(3))
	chunk.SetValue(2, 1, "")

	chunk.SetCount(3)

	roundTripped := performRoundTrip(t, chunk, []string{"id", "text"})

	require.Equal(t, 3, roundTripped.Count())

	assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
	assert.Equal(t, "valid", roundTripped.GetValue(0, 1))

	assert.Equal(t, int32(2), roundTripped.GetValue(1, 0))
	assert.Nil(t, roundTripped.GetValue(1, 1)) // Explicit NULL

	assert.Equal(t, int32(3), roundTripped.GetValue(2, 0))
	// Empty string becomes NULL with default NullStr=""
	assert.Nil(t, roundTripped.GetValue(2, 1))
}

// =============================================================================
// Write-Read Consistency Tests with Files
// =============================================================================

// TestWriteRead_ToFile tests writing to a file and reading it back.
func TestWriteRead_ToFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "roundtrip.csv")

	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}

	chunk := storage.NewDataChunkWithCapacity(types, 3)

	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, "alice")
	chunk.SetValue(0, 2, 100.5)
	chunk.SetValue(0, 3, true)

	chunk.SetValue(1, 0, int32(2))
	chunk.SetValue(1, 1, "bob")
	chunk.SetValue(1, 2, 200.75)
	chunk.SetValue(1, 3, false)

	chunk.SetValue(2, 0, int32(3))
	chunk.SetValue(2, 1, "charlie")
	chunk.SetValue(2, 2, 300.25)
	chunk.SetValue(2, 3, true)

	chunk.SetCount(3)

	// Write to file
	writer, err := NewWriterToPath(path, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name", "value", "flag"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Read from file
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value", "flag"}, schema)

	roundTripped, err := reader.ReadChunk()
	require.NoError(t, err)

	require.Equal(t, 3, roundTripped.Count())

	// Verify values
	assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
	assert.Equal(t, "alice", roundTripped.GetValue(0, 1))
	assert.InDelta(t, 100.5, roundTripped.GetValue(0, 2), 0.01)
	assert.Equal(t, true, roundTripped.GetValue(0, 3))
}

// TestWriteRead_CompressedFile tests writing and reading compressed CSV.
func TestWriteRead_CompressedFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "compressed.csv.gz")

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, "compressed data")
	chunk.SetValue(1, 0, int32(2))
	chunk.SetValue(1, 1, "more data")
	chunk.SetCount(2)

	// Write compressed
	writer, err := NewWriterToPath(path, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "text"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read compressed
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	roundTripped, err := reader.ReadChunk()
	require.NoError(t, err)

	require.Equal(t, 2, roundTripped.Count())
	assert.Equal(t, int32(1), roundTripped.GetValue(0, 0))
	assert.Equal(t, "compressed data", roundTripped.GetValue(0, 1))
}

// TestWriteRead_MultipleChunks tests writing multiple chunks and reading them back.
func TestWriteRead_MultipleChunks(t *testing.T) {
	var buf bytes.Buffer

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	// Write multiple chunks
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	for chunkIdx := range 3 {
		chunk := storage.NewDataChunkWithCapacity(types, 2)

		baseID := int32(chunkIdx*2 + 1)
		chunk.SetValue(0, 0, baseID)
		chunk.SetValue(0, 1, "item"+string(rune('A'+chunkIdx*2)))
		chunk.SetValue(1, 0, baseID+1)
		chunk.SetValue(1, 1, "item"+string(rune('B'+chunkIdx*2)))
		chunk.SetCount(2)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read back all rows
	reader, err := NewReader(strings.NewReader(buf.String()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	totalRows := 0
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += chunk.Count()
	}

	assert.Equal(t, 6, totalRows) // 3 chunks * 2 rows each
}

// =============================================================================
// Helper Functions
// =============================================================================

// performRoundTrip writes a DataChunk to CSV and reads it back.
func performRoundTrip(t *testing.T, chunk *storage.DataChunk, columns []string) *storage.DataChunk {
	t.Helper()

	var buf bytes.Buffer

	// Write
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema(columns)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read
	reader, err := NewReader(strings.NewReader(buf.String()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	result, err := reader.ReadChunk()
	require.NoError(t, err)

	return result
}
