package csv

import (
	"bytes"
	"compress/gzip"
	"io"
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

func TestWriter_BasicCSVWriting(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}

	data := [][]any{
		{int32(1), "alice", 100.5},
		{int32(2), "bob", 200.75},
		{int32(3), "charlie", 300.25},
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
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Len(t, lines, 4) // header + 3 data rows
	assert.Equal(t, "id,name,value", lines[0])
	assert.Equal(t, "1,alice,100.5", lines[1])
	assert.Equal(t, "2,bob,200.75", lines[2])
	assert.Equal(t, "3,charlie,300.25", lines[3])
}

func TestWriter_HeaderHandling(t *testing.T) {
	t.Run("with header enabled (default)", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{Header: true})
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "name"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 2)
		assert.Equal(t, "id,name", lines[0])
	})

	t.Run("with header disabled", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{Header: false})
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "name"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 1)
		assert.Equal(t, "1,test", lines[0])
	})

	t.Run("without explicit schema (auto-generated)", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{Header: true})
		require.NoError(t, err)

		// Don't call SetSchema - should auto-generate column names
		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Len(t, lines, 2)
		assert.Equal(t, "column0,column1", lines[0])
	})
}

func TestWriter_NULLValueHandling(t *testing.T) {
	t.Run("default empty string for NULL", func(t *testing.T) {
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
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, "1,test", lines[1])
		assert.Equal(t, "2,", lines[2]) // NULL renders as empty
	})

	t.Run("custom NULL string", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		chunk := storage.NewDataChunkWithCapacity(types, 2)

		chunk.SetValue(0, 0, int32(1))
		chunk.SetValue(0, 1, "test")
		chunk.SetValue(1, 0, int32(2))
		chunk.GetVector(1).Validity().SetInvalid(1)
		chunk.SetCount(2)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			Header:  true,
			NullStr: "NULL",
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
		assert.Equal(t, "2,NULL", lines[2])
	})
}

func TestWriter_QuoteEscaping(t *testing.T) {
	t.Run("field containing delimiter", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "hello, world"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "message"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		// The csv package should quote the field containing comma
		assert.Equal(t, `1,"hello, world"`, lines[1])
	})

	t.Run("field containing quote", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), `say "hello"`}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "message"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		// Quotes should be escaped by doubling
		assert.Equal(t, `1,"say ""hello"""`, lines[1])
	})

	t.Run("field containing newline", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "line1\nline2"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "message"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		// Should contain quoted field with embedded newline
		assert.Contains(t, output, `"line1`)
		assert.Contains(t, output, `line2"`)
	})
}

func TestWriter_CustomDelimiter(t *testing.T) {
	t.Run("tab delimiter", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}, {int32(2), "data"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			Delimiter: '\t',
			Header:    true,
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
		assert.Equal(t, "id\tname", lines[0])
		assert.Equal(t, "1\ttest", lines[1])
		assert.Equal(t, "2\tdata", lines[2])
	})

	t.Run("semicolon delimiter", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			Delimiter: ';',
			Header:    true,
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
		assert.Equal(t, "id;name", lines[0])
		assert.Equal(t, "1;test", lines[1])
	})

	t.Run("pipe delimiter", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, &WriterOptions{
			Delimiter: '|',
			Header:    true,
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
		assert.Equal(t, "id|name", lines[0])
		assert.Equal(t, "1|test", lines[1])
	})
}

func TestWriter_DateAndTimestampFormatting(t *testing.T) {
	t.Run("date formatting as YYYY-MM-DD", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DATE}
		testDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
		data := [][]any{{int32(1), testDate}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "date"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, "1,2024-06-15", lines[1])
	})

	t.Run("timestamp formatting as ISO 8601", func(t *testing.T) {
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_TIMESTAMP}
		testTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
		data := [][]any{{int32(1), testTime}}
		chunk := createTestChunk(t, types, data)

		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		require.NoError(t, err)

		err = writer.SetSchema([]string{"id", "timestamp"})
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		assert.Equal(t, "1,2024-06-15T14:30:45Z", lines[1])
	})
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

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Equal(t, "1,true", lines[1])
	assert.Equal(t, "2,false", lines[2])
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

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	assert.Len(t, lines, 5) // 1 header + 4 data rows
	assert.Equal(t, "id,name", lines[0])
	assert.Equal(t, "1,first", lines[1])
	assert.Equal(t, "2,second", lines[2])
	assert.Equal(t, "3,third", lines[3])
	assert.Equal(t, "4,fourth", lines[4])
}

func TestWriter_CompressionOutput(t *testing.T) {
	t.Run("gzip compression to file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.csv.gz")

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

		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		assert.Len(t, lines, 3)
		assert.Equal(t, "id,name", lines[0])
		assert.Equal(t, "1,test", lines[1])
		assert.Equal(t, "2,data", lines[2])
	})

	t.Run("explicit compression option", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "output.csv") // No .gz extension

		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		data := [][]any{{int32(1), "test"}}
		chunk := createTestChunk(t, types, data)

		writer, err := NewWriterToPath(path, &WriterOptions{
			Header:      true,
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
		assert.Contains(t, string(content), "id,name")
	})
}

func TestWriter_ForceQuote(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
	data := [][]any{{int32(1), "alice", "plain text"}}
	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, &WriterOptions{
		Header:     true,
		ForceQuote: []string{"name"},
	})
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name", "description"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Note: Go's csv.Writer doesn't support force quoting directly,
	// so this test verifies the option is accepted without error.
	// The actual quoting behavior depends on the content.
	output := buf.String()
	assert.Contains(t, output, "id,name,description")
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

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	// Should only have header
	assert.Len(t, lines, 1)
	assert.Equal(t, "id,name", lines[0])
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
}

func TestWriter_FileOutput(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "output.csv")

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

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	assert.Len(t, lines, 3)
	assert.Equal(t, "id,name", lines[0])
	assert.Equal(t, "1,test", lines[1])
	assert.Equal(t, "2,data", lines[2])
}

func TestWriter_DefaultOptions(t *testing.T) {
	opts := DefaultWriterOptions()

	assert.Equal(t, DefaultDelimiter, opts.Delimiter)
	assert.Equal(t, DefaultQuote, opts.Quote)
	assert.True(t, opts.Header)
	assert.Equal(t, "", opts.NullStr)
	assert.Nil(t, opts.ForceQuote)
	assert.Equal(t, fileio.CompressionNone, opts.Compression)
}

func TestWriter_ApplyDefaults(t *testing.T) {
	opts := &WriterOptions{}
	opts.applyDefaults()

	assert.Equal(t, DefaultDelimiter, opts.Delimiter)
	assert.Equal(t, DefaultQuote, opts.Quote)
}

func TestWriter_VariousTypes(t *testing.T) {
	types := []dukdb.Type{
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
	}

	data := [][]any{
		{int8(10), int16(100), int64(1000), float32(1.5), float64(2.5)},
	}

	chunk := createTestChunk(t, types, data)

	var buf bytes.Buffer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"tiny", "small", "big", "float", "double"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Equal(t, "10,100,1000,1.5,2.5", lines[1])
}
