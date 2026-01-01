package json

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestChunk creates a DataChunk with the given types and data for testing.
func createTestChunk(t *testing.T, types []dukdb.Type, data [][]any) *storage.DataChunk {
	t.Helper()

	chunk := storage.NewDataChunkWithCapacity(types, len(data))
	for rowIdx, row := range data {
		for colIdx, val := range row {
			chunk.SetValue(rowIdx, colIdx, val)
		}
	}

	chunk.SetCount(len(data))

	return chunk
}

func TestWriter_BasicJSONArrayWriting(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}

	data := [][]any{
		{int32(1), "alice", 100.5},
		{int32(2), "bob", 200.75},
	}

	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name", "value"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()

	// Parse as JSON array
	var result []map[string]any
	err = json.Unmarshal([]byte(output), &result)
	require.NoError(t, err)

	assert.Len(t, result, 2)
	assert.Equal(t, float64(1), result[0]["id"])
	assert.Equal(t, "alice", result[0]["name"])
	assert.Equal(t, 100.5, result[0]["value"])
	assert.Equal(t, float64(2), result[1]["id"])
	assert.Equal(t, "bob", result[1]["name"])
	assert.Equal(t, 200.75, result[1]["value"])
}

func TestWriter_NDJSONWriting(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}

	data := [][]any{
		{int32(1), "alice"},
		{int32(2), "bob"},
		{int32(3), "charlie"},
	}

	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{Format: FormatNDJSON})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Len(t, lines, 3)

	// Parse each line as JSON
	var obj1, obj2, obj3 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &obj1))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &obj2))
	require.NoError(t, json.Unmarshal([]byte(lines[2]), &obj3))

	assert.Equal(t, float64(1), obj1["id"])
	assert.Equal(t, "alice", obj1["name"])
	assert.Equal(t, float64(2), obj2["id"])
	assert.Equal(t, "bob", obj2["name"])
	assert.Equal(t, float64(3), obj3["id"])
	assert.Equal(t, "charlie", obj3["name"])
}

func TestWriter_NDJSONShortFormat(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	data := [][]any{{int32(1)}, {int32(2)}}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{Format: FormatNDJSONShort})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Len(t, lines, 2)
}

func TestWriter_PrettyPrintArray(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}

	data := [][]any{
		{int32(1), "test"},
	}

	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{
		Format: FormatArray,
		Pretty: true,
		Indent: "  ",
	})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()

	// Should be valid JSON
	var result []map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &result))

	// Should contain newlines and indentation
	assert.Contains(t, output, "\n")
	assert.Contains(t, output, "  ")
}

func TestWriter_PrettyPrintNDJSON(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	data := [][]any{{int32(1), "test"}}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{
		Format: FormatNDJSON,
		Pretty: true,
	})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Pretty NDJSON still has one object per logical entry (may span lines)
	// But each "row" should still be valid JSON
	assert.NotEmpty(t, lines)
}

func TestWriter_NULLHandling(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 2)

	// Row 0: valid values
	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, "test")

	// Row 1: NULL in second column
	chunk.SetValue(1, 0, int32(2))
	chunk.GetVector(1).Validity().SetInvalid(1)
	chunk.SetCount(2)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()

	var result []map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &result))

	assert.Len(t, result, 2)
	assert.Equal(t, "test", result[0]["name"])
	assert.Nil(t, result[1]["name"]) // NULL becomes JSON null
}

func TestWriter_BooleanFormatting(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BOOLEAN}
	data := [][]any{
		{int32(1), true},
		{int32(2), false},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "flag"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Equal(t, true, result[0]["flag"])
	assert.Equal(t, false, result[1]["flag"])
}

func TestWriter_IntegerFormatting(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
	}
	data := [][]any{
		{int8(10), int16(100), int32(1000), int64(10000)},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"tiny", "small", "int", "big"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Equal(t, float64(10), result[0]["tiny"])
	assert.Equal(t, float64(100), result[0]["small"])
	assert.Equal(t, float64(1000), result[0]["int"])
	assert.Equal(t, float64(10000), result[0]["big"])
}

func TestWriter_DoubleSpecialValues(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	data := [][]any{
		{"positive_inf", math.Inf(1)},
		{"negative_inf", math.Inf(-1)},
		{"nan", math.NaN()},
		{"normal", 3.14},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"name", "value"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Equal(t, "Infinity", result[0]["value"])
	assert.Equal(t, "-Infinity", result[1]["value"])
	assert.Equal(t, "NaN", result[2]["value"])
	assert.Equal(t, 3.14, result[3]["value"])
}

func TestWriter_DateFormatting(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DATE}
	testDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	data := [][]any{{int32(1), testDate}}
	chunk := createTestChunk(t, types, data)

	t.Run("default ISO format", func(t *testing.T) {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "date"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		var result []map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

		assert.Equal(t, "2024-06-15", result[0]["date"])
	})

	t.Run("custom date format", func(t *testing.T) {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			DateFormat: "01/02/2006",
		})
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "date"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		var result []map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

		assert.Equal(t, "06/15/2024", result[0]["date"])
	})
}

func TestWriter_TimestampFormatting(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_TIMESTAMP}
	testTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
	data := [][]any{{int32(1), testTime}}
	chunk := createTestChunk(t, types, data)

	t.Run("default RFC3339 format", func(t *testing.T) {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "timestamp"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		var result []map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

		assert.Equal(t, "2024-06-15T14:30:45Z", result[0]["timestamp"])
	})

	t.Run("custom timestamp format", func(t *testing.T) {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			TimestampFormat: "2006-01-02 15:04:05",
		})
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "timestamp"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		var result []map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

		assert.Equal(t, "2024-06-15 14:30:45", result[0]["timestamp"])
	})
}

func TestWriter_MultipleChunks(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	chunk1 := createTestChunk(t, types, [][]any{
		{int32(1), "first"},
		{int32(2), "second"},
	})

	chunk2 := createTestChunk(t, types, [][]any{
		{int32(3), "third"},
		{int32(4), "fourth"},
	})

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk1)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk2)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Len(t, result, 4)
	assert.Equal(t, float64(1), result[0]["id"])
	assert.Equal(t, float64(4), result[3]["id"])
}

func TestWriter_CompressionOutput(t *testing.T) {
	t.Run("gzip compression to file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.json.gz")

		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}, {int32(2), "data"}}
		chunk := createTestChunk(t, types, data)

		writer, err := NewWriterToPath(path, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "name"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Verify the file exists and is valid gzip
		file, err := os.Open(path)
		require.NoError(t, err)

		defer func() { _ = file.Close() }()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)

		defer func() { _ = gzReader.Close() }()

		content, err := io.ReadAll(gzReader)
		require.NoError(t, err)

		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Len(t, result, 2)
	})

	t.Run("explicit compression option", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.json") // No .gz extension

		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		writer, err := NewWriterToPath(path, &WriterOptions{
			Compression: fileio.CompressionGZIP,
		})
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "name"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Verify the file is gzip compressed
		file, err := os.Open(path)
		require.NoError(t, err)

		defer func() { _ = file.Close() }()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)

		defer func() { _ = gzReader.Close() }()

		content, err := io.ReadAll(gzReader)
		require.NoError(t, err)
		assert.Contains(t, string(content), `"id"`)
	})
}

func TestWriter_EmptyChunk(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.SetCount(0) // Empty chunk

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Should be empty array
	output := buf.String()
	assert.Equal(t, "[]", output)
}

func TestWriter_NilChunk(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	// Writing nil chunk should not error
	err = writer.WriteChunk(nil)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Should be empty array
	output := buf.String()
	assert.Equal(t, "[]", output)
}

func TestWriter_FileOutput(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "output.json")

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	data := [][]any{{int32(1), "test"}, {int32(2), "data"}}
	chunk := createTestChunk(t, types, data)

	writer, err := NewWriterToPath(path, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read the file back and verify
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(content, &result))

	assert.Len(t, result, 2)
}

func TestWriter_DefaultOptions(t *testing.T) {
	opts := DefaultWriterOptions()

	assert.Equal(t, DefaultWriterFormat, opts.Format)
	assert.False(t, opts.Pretty)
	assert.Equal(t, DefaultIndent, opts.Indent)
	assert.Equal(t, DefaultDateFormat, opts.DateFormat)
	assert.Equal(t, DefaultTimestampWriteFormat, opts.TimestampFormat)
	assert.Equal(t, fileio.CompressionNone, opts.Compression)
}

func TestWriter_ApplyDefaults(t *testing.T) {
	opts := &WriterOptions{}
	opts.applyDefaults()

	assert.Equal(t, DefaultWriterFormat, opts.Format)
	assert.Equal(t, DefaultIndent, opts.Indent)
	assert.Equal(t, DefaultDateFormat, opts.DateFormat)
	assert.Equal(t, DefaultTimestampWriteFormat, opts.TimestampFormat)
}

func TestWriter_AutoGeneratedColumnNames(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	data := [][]any{{int32(1), "test"}}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Don't call SetSchema - should auto-generate column names
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Len(t, result, 1)
	_, hasColumn0 := result[0]["column0"]
	_, hasColumn1 := result[0]["column1"]
	assert.True(t, hasColumn0)
	assert.True(t, hasColumn1)
}

func TestWriter_StringEscaping(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_VARCHAR}
	data := [][]any{
		{`string with "quotes"`},
		{"string with\nnewline"},
		{"string with\ttab"},
		{`string with \backslash`},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"text"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Should be valid JSON with proper escaping
	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Len(t, result, 4)
	assert.Equal(t, `string with "quotes"`, result[0]["text"])
	assert.Equal(t, "string with\nnewline", result[1]["text"])
	assert.Equal(t, "string with\ttab", result[2]["text"])
	assert.Equal(t, `string with \backslash`, result[3]["text"])
}

func TestWriter_UUIDFormatting(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_UUID}

	uuid := dukdb.UUID{}
	// Set some bytes for the UUID
	uuid[0] = 0x12
	uuid[1] = 0x34
	uuid[2] = 0x56
	uuid[3] = 0x78
	uuid[4] = 0x9a
	uuid[5] = 0xbc
	uuid[6] = 0xde
	uuid[7] = 0xf0
	uuid[8] = 0x12
	uuid[9] = 0x34
	uuid[10] = 0x56
	uuid[11] = 0x78
	uuid[12] = 0x9a
	uuid[13] = 0xbc
	uuid[14] = 0xde
	uuid[15] = 0xf0

	data := [][]any{{int32(1), uuid}}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "uuid"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	// UUID should be formatted as string
	uuidStr, ok := result[0]["uuid"].(string)
	assert.True(t, ok)
	assert.Equal(t, "12345678-9abc-def0-1234-56789abcdef0", uuidStr)
}

func TestWriter_IntervalFormatting(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTERVAL}

	data := [][]any{
		{"years_months", dukdb.Interval{Months: 14, Days: 0, Micros: 0}},
		{"days", dukdb.Interval{Months: 0, Days: 5, Micros: 0}},
		{"time", dukdb.Interval{Months: 0, Days: 0, Micros: 3661000000}}, // 1:01:01
		{"zero", dukdb.Interval{Months: 0, Days: 0, Micros: 0}},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"name", "interval"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Equal(t, "1 years 2 months", result[0]["interval"])
	assert.Equal(t, "5 days", result[1]["interval"])
	assert.Equal(t, "01:01:01", result[2]["interval"])
	assert.Equal(t, "00:00:00", result[3]["interval"])
}

func TestWriter_NDJSONWithMultipleChunks(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	chunk1 := createTestChunk(t, types, [][]any{{int32(1)}, {int32(2)}})
	chunk2 := createTestChunk(t, types, [][]any{{int32(3)}, {int32(4)}})

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{Format: FormatNDJSON})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk1)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk2)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 4)

	for i, line := range lines {
		var obj map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &obj))
		assert.Equal(t, float64(i+1), obj["id"])
	}
}

func TestWriter_EmptyNDJSON(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.SetCount(0)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{Format: FormatNDJSON})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Empty NDJSON should be empty string
	assert.Equal(t, "", buf.String())
}

func TestWriter_VarcharWithSpecialCharacters(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_VARCHAR}
	data := [][]any{
		{"unicode: \u4e2d\u6587"},
		{"emoji: \U0001F600"},
		{"control: \x00\x01\x02"},
	}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"text"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Should be valid JSON
	var result []map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &result))

	assert.Len(t, result, 3)
}

func TestGenerateColumnNames(t *testing.T) {
	names := generateColumnNames(3)
	assert.Equal(t, []string{"column0", "column1", "column2"}, names)

	names = generateColumnNames(0)
	assert.Empty(t, names)
}
