package parquet

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Writer Creation Tests
// ============================================================================

func TestWriter_NewWriter(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_NewWriterWithOptions(t *testing.T) {
	buf := new(bytes.Buffer)
	opts := &WriterOptions{
		Codec:        "GZIP",
		RowGroupSize: 1000,
	}

	writer, err := NewWriter(buf, opts)
	require.NoError(t, err)
	require.NotNil(t, writer)

	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_NewWriterToPath(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.parquet")

	writer, err := NewWriterToPath(path, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	err = writer.Close()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestWriter_NewWriterToPath_OverwriteDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.parquet")

	// Create initial file
	f, err := os.Create(path)
	require.NoError(t, err)
	_ = f.Close()

	// Try to create writer without overwrite - should fail
	_, err = NewWriterToPath(path, &WriterOptions{Overwrite: false})
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")
}

func TestWriter_NewWriterToPath_OverwriteEnabled(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.parquet")

	// Create initial file
	f, err := os.Create(path)
	require.NoError(t, err)
	_, _ = f.WriteString("dummy content")
	_ = f.Close()

	// Create writer with overwrite enabled - should succeed
	writer, err := NewWriterToPath(path, &WriterOptions{Overwrite: true})
	require.NoError(t, err)
	require.NotNil(t, writer)

	err = writer.Close()
	require.NoError(t, err)
}

// ============================================================================
// Basic Writing Tests
// ============================================================================

func TestWriter_WriteChunk_Simple(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	// Create a simple chunk with integer and string columns
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	// Add data
	chunk.SetValue(0, 0, int64(1))
	chunk.SetValue(0, 1, "Alice")
	chunk.SetValue(1, 0, int64(2))
	chunk.SetValue(1, 1, "Bob")
	chunk.SetCount(2)

	// Set schema and write
	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify data was written
	require.Greater(t, buf.Len(), 0)
}

func TestWriter_WriteChunk_NilChunk(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	// Writing nil chunk should be a no-op
	err = writer.WriteChunk(nil)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_WriteChunk_InferSchema(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	// Create chunk without setting schema - should auto-generate column names
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, int32(42))
	chunk.SetValue(0, 1, float64(3.14))
	chunk.SetCount(1)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify data was written
	require.Greater(t, buf.Len(), 0)
}

// ============================================================================
// Type Conversion Tests
// ============================================================================

func TestWriter_WriteChunk_AllBasicTypes(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	types := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}

	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, true)
	chunk.SetValue(0, 1, int8(1))
	chunk.SetValue(0, 2, int16(2))
	chunk.SetValue(0, 3, int32(3))
	chunk.SetValue(0, 4, int64(4))
	chunk.SetValue(0, 5, float32(5.5))
	chunk.SetValue(0, 6, float64(6.6))
	chunk.SetValue(0, 7, "hello")
	chunk.SetCount(1)

	err = writer.SetSchema([]string{"bool", "tiny", "small", "int", "big", "flt", "dbl", "str"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	require.Greater(t, buf.Len(), 0)
}

func TestWriter_WriteChunk_NullValues(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	// Set some values as NULL
	chunk.SetValue(0, 0, int32(1))
	chunk.SetValue(0, 1, "valid")
	chunk.SetValue(1, 0, nil) // NULL
	chunk.SetValue(1, 1, "also valid")
	chunk.SetValue(2, 0, int32(3))
	chunk.SetValue(2, 1, nil) // NULL
	chunk.SetCount(3)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	require.Greater(t, buf.Len(), 0)
}

// ============================================================================
// Compression Tests
// ============================================================================

func TestWriter_Compression_Uncompressed(t *testing.T) {
	testWriterWithCodec(t, "UNCOMPRESSED")
}

func TestWriter_Compression_Snappy(t *testing.T) {
	testWriterWithCodec(t, "SNAPPY")
}

func TestWriter_Compression_Gzip(t *testing.T) {
	testWriterWithCodec(t, "GZIP")
}

func TestWriter_Compression_Zstd(t *testing.T) {
	testWriterWithCodec(t, "ZSTD")
}

func TestWriter_Compression_LZ4(t *testing.T) {
	testWriterWithCodec(t, "LZ4")
}

func TestWriter_Compression_Brotli(t *testing.T) {
	testWriterWithCodec(t, "BROTLI")
}

func testWriterWithCodec(t *testing.T, codec string) {
	t.Helper()

	buf := new(bytes.Buffer)
	opts := &WriterOptions{
		Codec: codec,
	}

	writer, err := NewWriter(buf, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, int64(1))
	chunk.SetValue(0, 1, "test data")
	chunk.SetCount(1)

	err = writer.SetSchema([]string{"id", "data"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	require.Greater(t, buf.Len(), 0, "codec %s should produce output", codec)
}

func TestWriter_Compression_UnsupportedCodec(t *testing.T) {
	buf := new(bytes.Buffer)
	opts := &WriterOptions{
		Codec: "INVALID_CODEC",
	}

	writer, err := NewWriter(buf, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.SetValue(0, 0, int32(1))
	chunk.SetCount(1)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	// Error should occur when trying to write (which initializes the writer)
	err = writer.WriteChunk(chunk)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported compression codec")
}

// ============================================================================
// Row Group Tests
// ============================================================================

func TestWriter_RowGroupSize(t *testing.T) {
	buf := new(bytes.Buffer)
	opts := &WriterOptions{
		Codec:        "UNCOMPRESSED",
		RowGroupSize: 5, // Small row group size for testing
	}

	writer, err := NewWriter(buf, opts)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 20)

	// Write 12 rows across multiple chunks
	for i := range 12 {
		chunk.SetValue(i, 0, int64(i))
	}
	chunk.SetCount(12)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify we can read the data back
	require.Greater(t, buf.Len(), 0)
}

// ============================================================================
// Round-trip Tests (Write then Read)
// ============================================================================

func TestWriter_RoundTrip_Simple(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, &WriterOptions{Codec: "SNAPPY"})
	require.NoError(t, err)

	// Write data
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE, dukdb.TYPE_BOOLEAN}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, int64(1))
	chunk.SetValue(0, 1, "Alice")
	chunk.SetValue(0, 2, float64(100.5))
	chunk.SetValue(0, 3, true)

	chunk.SetValue(1, 0, int64(2))
	chunk.SetValue(1, 1, "Bob")
	chunk.SetValue(1, 2, float64(200.75))
	chunk.SetValue(1, 3, false)

	chunk.SetCount(2)

	err = writer.SetSchema([]string{"id", "name", "value", "active"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read data back
	data := buf.Bytes()
	readerBuf := newBufferReaderAt(data)

	reader, err := NewReader(readerBuf, readerBuf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Verify schema
	schema, err := reader.Schema()
	require.NoError(t, err)
	require.Contains(t, schema, "id")
	require.Contains(t, schema, "name")
	require.Contains(t, schema, "value")
	require.Contains(t, schema, "active")

	// Read chunk
	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.Equal(t, 2, readChunk.Count())

	// Find column indices
	idIdx := findColumnIndex(schema, "id")
	nameIdx := findColumnIndex(schema, "name")
	valueIdx := findColumnIndex(schema, "value")
	activeIdx := findColumnIndex(schema, "active")

	// Verify first row
	require.Equal(t, int64(1), readChunk.GetValue(0, idIdx))
	require.Equal(t, "Alice", readChunk.GetValue(0, nameIdx))
	require.InDelta(t, 100.5, readChunk.GetValue(0, valueIdx), 0.01)
	require.Equal(t, true, readChunk.GetValue(0, activeIdx))

	// Verify second row
	require.Equal(t, int64(2), readChunk.GetValue(1, idIdx))
	require.Equal(t, "Bob", readChunk.GetValue(1, nameIdx))
	require.InDelta(t, 200.75, readChunk.GetValue(1, valueIdx), 0.01)
	require.Equal(t, false, readChunk.GetValue(1, activeIdx))
}

func TestWriter_RoundTrip_AllCodecs(t *testing.T) {
	codecs := []string{"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "BROTLI"}

	for _, codec := range codecs {
		t.Run(codec, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer, err := NewWriter(buf, &WriterOptions{Codec: codec})
			require.NoError(t, err)

			types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
			chunk := storage.NewDataChunkWithCapacity(types, 10)

			chunk.SetValue(0, 0, int64(42))
			chunk.SetValue(0, 1, "test value")
			chunk.SetCount(1)

			err = writer.SetSchema([]string{"id", "data"})
			require.NoError(t, err)

			err = writer.WriteChunk(chunk)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Read back
			data := buf.Bytes()
			readerBuf := newBufferReaderAt(data)

			reader, err := NewReader(readerBuf, readerBuf.Size(), nil)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			readChunk, err := reader.ReadChunk()
			require.NoError(t, err)
			require.Equal(t, 1, readChunk.Count())

			schema, _ := reader.Schema()
			idIdx := findColumnIndex(schema, "id")
			dataIdx := findColumnIndex(schema, "data")

			require.Equal(t, int64(42), readChunk.GetValue(0, idIdx))
			require.Equal(t, "test value", readChunk.GetValue(0, dataIdx))
		})
	}
}

func TestWriter_RoundTrip_LargeData(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, &WriterOptions{
		Codec:        "SNAPPY",
		RowGroupSize: 100,
	})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	numRows := 500

	chunk := storage.NewDataChunkWithCapacity(types, numRows)
	for i := range numRows {
		chunk.SetValue(i, 0, int64(i))
		chunk.SetValue(i, 1, "row data")
	}
	chunk.SetCount(numRows)

	err = writer.SetSchema([]string{"id", "data"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back and count rows
	data := buf.Bytes()
	readerBuf := newBufferReaderAt(data)

	reader, err := NewReader(readerBuf, readerBuf.Size(), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	totalRows := 0
	for {
		readChunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += readChunk.Count()
	}

	require.Equal(t, numRows, totalRows)
}

// ============================================================================
// Date/Time Type Tests
// ============================================================================

func TestWriter_DateTypes(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_DATE, dukdb.TYPE_TIMESTAMP}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	// Using time values
	testDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	testTimestamp := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	chunk.SetValue(0, 0, testDate)
	chunk.SetValue(0, 1, testTimestamp)
	chunk.SetCount(1)

	err = writer.SetSchema([]string{"date_col", "ts_col"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	require.Greater(t, buf.Len(), 0)
}

// ============================================================================
// Options Tests
// ============================================================================

func TestWriterOptions_DefaultValues(t *testing.T) {
	opts := DefaultWriterOptions()
	require.NotNil(t, opts)
	require.Equal(t, DefaultCodec, opts.Codec)
	require.Equal(t, DefaultRowGroupSize, opts.RowGroupSize)
	require.Equal(t, 0, opts.CompressionLevel)
	require.False(t, opts.Overwrite)
}

func TestWriterOptions_ApplyDefaults(t *testing.T) {
	opts := &WriterOptions{}
	opts.applyDefaults()
	require.Equal(t, DefaultCodec, opts.Codec)
	require.Equal(t, DefaultRowGroupSize, opts.RowGroupSize)
}

func TestWriterOptions_ApplyDefaultsPreservesValues(t *testing.T) {
	opts := &WriterOptions{
		Codec:        "GZIP",
		RowGroupSize: 5000,
	}
	opts.applyDefaults()
	require.Equal(t, "GZIP", opts.Codec)
	require.Equal(t, 5000, opts.RowGroupSize)
}

// ============================================================================
// Edge Case Tests
// ============================================================================

func TestWriter_EmptyChunk(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	chunk.SetCount(0) // Empty chunk

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_CloseWithoutWrite(t *testing.T) {
	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, nil)
	require.NoError(t, err)

	// Close without writing anything
	err = writer.Close()
	require.NoError(t, err)
}

func TestWriter_ToExportedName(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"column", "Column"},
		{"myField", "MyField"},
		{"my_field", "My_field"},
		{"123field", "_23field"}, // Starts with digit
		{"field-name", "Field_name"},
		{"", "Column"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := toExportedName(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

// ============================================================================
// File Path Tests
// ============================================================================

func TestWriter_WriteToFile_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.parquet")

	// Write to file
	writer, err := NewWriterToPath(path, &WriterOptions{Codec: "SNAPPY", Overwrite: true})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, int64(1))
	chunk.SetValue(0, 1, "file test")
	chunk.SetCount(1)

	err = writer.SetSchema([]string{"id", "data"})
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read from file
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	require.Equal(t, 1, readChunk.Count())

	schema, _ := reader.Schema()
	idIdx := findColumnIndex(schema, "id")
	dataIdx := findColumnIndex(schema, "data")

	require.Equal(t, int64(1), readChunk.GetValue(0, idIdx))
	require.Equal(t, "file test", readChunk.GetValue(0, dataIdx))
}

// ============================================================================
// Test with parquet-go Generic Reader for verification
// ============================================================================

func TestWriter_VerifyWithGenericReader(t *testing.T) {
	type TestRecord struct {
		Id     int64   `parquet:"id"`
		Name   string  `parquet:"name"`
		Value  float64 `parquet:"value"`
		Active bool    `parquet:"active"`
	}

	buf := new(bytes.Buffer)
	writer, err := NewWriter(buf, &WriterOptions{Codec: "SNAPPY"})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE, dukdb.TYPE_BOOLEAN}
	chunk := storage.NewDataChunkWithCapacity(types, 10)

	chunk.SetValue(0, 0, int64(1))
	chunk.SetValue(0, 1, "Alice")
	chunk.SetValue(0, 2, float64(100.5))
	chunk.SetValue(0, 3, true)

	chunk.SetValue(1, 0, int64(2))
	chunk.SetValue(1, 1, "Bob")
	chunk.SetValue(1, 2, float64(200.75))
	chunk.SetValue(1, 3, false)

	chunk.SetCount(2)

	err = writer.SetSchema([]string{"id", "name", "value", "active"})
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back using parquet-go's GenericReader
	data := buf.Bytes()
	readerBuf := newBufferReaderAt(data)

	genericReader := parquet.NewGenericReader[TestRecord](readerBuf)
	defer func() { _ = genericReader.Close() }()

	records := make([]TestRecord, 2)
	n, err := genericReader.Read(records)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 2, n)

	require.Equal(t, int64(1), records[0].Id)
	require.Equal(t, "Alice", records[0].Name)
	require.InDelta(t, 100.5, records[0].Value, 0.01)
	require.True(t, records[0].Active)

	require.Equal(t, int64(2), records[1].Id)
	require.Equal(t, "Bob", records[1].Name)
	require.InDelta(t, 200.75, records[1].Value, 0.01)
	require.False(t, records[1].Active)
}

// ============================================================================
// Helper Functions
// ============================================================================

func findColumnIndex(schema []string, name string) int {
	for i, col := range schema {
		if col == name {
			return i
		}
	}

	return -1
}
