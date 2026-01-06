package arrow

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestDataChunk creates a DataChunk with test data for writing.
func createTestDataChunk(t *testing.T) *storage.DataChunk {
	t.Helper()

	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}

	chunk := storage.NewDataChunkWithCapacity(types, 3)
	chunk.SetCount(3)

	// Set values
	chunk.GetVector(0).SetValue(0, int64(1))
	chunk.GetVector(0).SetValue(1, int64(2))
	chunk.GetVector(0).SetValue(2, int64(3))

	chunk.GetVector(1).SetValue(0, "Alice")
	chunk.GetVector(1).SetValue(1, "Bob")
	chunk.GetVector(1).SetValue(2, "Charlie")

	chunk.GetVector(2).SetValue(0, 1.5)
	chunk.GetVector(2).SetValue(1, 2.5)
	chunk.GetVector(2).SetValue(2, 3.5)

	return chunk
}

func TestWriterToBuffer(t *testing.T) {
	// Create a buffer to write to
	var buf bytes.Buffer

	// Create writer
	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Set schema
	err = writer.SetSchema([]string{"id", "name", "value"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE})
	require.NoError(t, err)

	// Create and write a chunk
	chunk := createTestDataChunk(t)
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	// Close writer
	err = writer.Close()
	require.NoError(t, err)

	// Verify the output is valid Arrow IPC
	assert.True(t, buf.Len() > 0)
	assert.True(t, IsArrowFile(buf.Bytes()))

	// Read back and verify
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, columns)

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, readChunk.Count())

	// Verify data
	assert.Equal(t, int64(1), readChunk.GetValue(0, 0))
	assert.Equal(t, int64(2), readChunk.GetValue(1, 0))
	assert.Equal(t, int64(3), readChunk.GetValue(2, 0))

	assert.Equal(t, "Alice", readChunk.GetValue(0, 1))
	assert.Equal(t, "Bob", readChunk.GetValue(1, 1))
	assert.Equal(t, "Charlie", readChunk.GetValue(2, 1))

	assert.Equal(t, 1.5, readChunk.GetValue(0, 2))
	assert.Equal(t, 2.5, readChunk.GetValue(1, 2))
	assert.Equal(t, 3.5, readChunk.GetValue(2, 2))
}

func TestWriterToPath(t *testing.T) {
	// Create temp file path
	tmpFile, err := os.CreateTemp("", "test_arrow_writer_*.arrow")
	require.NoError(t, err)
	path := tmpFile.Name()
	_ = tmpFile.Close()
	_ = os.Remove(path) // Remove so writer can create it
	defer func() { _ = os.Remove(path) }()

	// Create writer
	writer, err := NewWriterToPath(path, nil)
	require.NoError(t, err)

	// Set schema
	err = writer.SetSchema([]string{"id", "name", "value"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE})
	require.NoError(t, err)

	// Create and write a chunk
	chunk := createTestDataChunk(t)
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	// Close writer
	err = writer.Close()
	require.NoError(t, err)

	// Verify file exists
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.True(t, info.Size() > 0)

	// Read back and verify
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, readChunk.Count())
}

func TestWriterToPathOverwrite(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_arrow_writer_*.arrow")
	require.NoError(t, err)
	path := tmpFile.Name()
	_, _ = tmpFile.WriteString("existing content")
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(path) }()

	// Try to create writer without overwrite - should fail
	_, err = NewWriterToPath(path, nil)
	assert.Error(t, err)

	// Create writer with overwrite
	writer, err := NewWriterToPathOverwrite(path, nil)
	require.NoError(t, err)

	// Set schema and write
	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.SetCount(1)
	chunk.GetVector(0).SetValue(0, int64(42))

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify file was overwritten with valid Arrow data
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, readChunk.Count())
	assert.Equal(t, int64(42), readChunk.GetValue(0, 0))
}

func TestWriterWithCompression(t *testing.T) {
	tests := []struct {
		name        string
		compression Compression
	}{
		{"none", CompressionNone},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZSTD},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			opts := &WriterOptions{
				Compression: tt.compression,
			}
			writer, err := NewWriter(&buf, opts)
			require.NoError(t, err)

			err = writer.SetSchema([]string{"id"})
			require.NoError(t, err)

			err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT})
			require.NoError(t, err)

			types := []dukdb.Type{dukdb.TYPE_BIGINT}
			chunk := storage.NewDataChunkWithCapacity(types, 1)
			chunk.SetCount(1)
			chunk.GetVector(0).SetValue(0, int64(42))

			err = writer.WriteChunk(chunk)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify we can read it back
			reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			readChunk, err := reader.ReadChunk()
			require.NoError(t, err)
			assert.Equal(t, int64(42), readChunk.GetValue(0, 0))
		})
	}
}

func TestWriterMultipleChunks(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT})
	require.NoError(t, err)

	// Write multiple chunks
	for i := 0; i < 5; i++ {
		types := []dukdb.Type{dukdb.TYPE_BIGINT}
		chunk := storage.NewDataChunkWithCapacity(types, 10)
		chunk.SetCount(10)
		for j := 0; j < 10; j++ {
			chunk.GetVector(0).SetValue(j, int64(i*10+j))
		}
		err = writer.WriteChunk(chunk)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Read back and verify
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	assert.Equal(t, 5, reader.NumRecordBatches())

	// Read all data
	totalRows := 0
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += chunk.Count()
	}

	assert.Equal(t, 50, totalRows)
}

func TestWriterNilChunk(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Writing nil chunk should succeed (no-op)
	err = writer.WriteChunk(nil)
	require.NoError(t, err)

	// Set schema and write real data
	err = writer.SetSchema([]string{"id"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.SetCount(1)
	chunk.GetVector(0).SetValue(0, int64(42))

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestWriterAutoGeneratedColumnNames(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	// Don't set schema - should auto-generate column names
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.SetCount(1)
	chunk.GetVector(0).SetValue(0, int64(42))
	chunk.GetVector(1).SetValue(0, "test")

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back and verify auto-generated names
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"column0", "column1"}, columns)
}

func TestRecordBatchBuilder(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	alloc := memory.NewGoAllocator()
	builder := NewRecordBatchBuilder(schema, alloc)
	defer builder.Release()

	// Create a chunk
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 2)
	chunk.SetCount(2)
	chunk.GetVector(0).SetValue(0, int64(1))
	chunk.GetVector(0).SetValue(1, int64(2))
	chunk.GetVector(1).SetValue(0, "Alice")
	chunk.GetVector(1).SetValue(1, "Bob")

	// Build record batch
	record, err := builder.Build(chunk)
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())
}

func TestDuckDBTypeToArrow(t *testing.T) {
	tests := []struct {
		name     string
		duckType dukdb.Type
		wantErr  bool
	}{
		{"boolean", dukdb.TYPE_BOOLEAN, false},
		{"tinyint", dukdb.TYPE_TINYINT, false},
		{"smallint", dukdb.TYPE_SMALLINT, false},
		{"integer", dukdb.TYPE_INTEGER, false},
		{"bigint", dukdb.TYPE_BIGINT, false},
		{"utinyint", dukdb.TYPE_UTINYINT, false},
		{"usmallint", dukdb.TYPE_USMALLINT, false},
		{"uinteger", dukdb.TYPE_UINTEGER, false},
		{"ubigint", dukdb.TYPE_UBIGINT, false},
		{"float", dukdb.TYPE_FLOAT, false},
		{"double", dukdb.TYPE_DOUBLE, false},
		{"varchar", dukdb.TYPE_VARCHAR, false},
		{"blob", dukdb.TYPE_BLOB, false},
		{"date", dukdb.TYPE_DATE, false},
		{"time", dukdb.TYPE_TIME, false},
		{"timestamp", dukdb.TYPE_TIMESTAMP, false},
		{"timestamp_s", dukdb.TYPE_TIMESTAMP_S, false},
		{"timestamp_ms", dukdb.TYPE_TIMESTAMP_MS, false},
		{"timestamp_ns", dukdb.TYPE_TIMESTAMP_NS, false},
		{"interval", dukdb.TYPE_INTERVAL, false},
		{"uuid", dukdb.TYPE_UUID, false},
		{"hugeint", dukdb.TYPE_HUGEINT, false},
		{"decimal", dukdb.TYPE_DECIMAL, false},
		{"list", dukdb.TYPE_LIST, false},
		{"struct", dukdb.TYPE_STRUCT, false},
		{"map", dukdb.TYPE_MAP, false},
		{"invalid", dukdb.TYPE_INVALID, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType, err := DuckDBTypeToArrow(tt.duckType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, arrowType)
			}
		})
	}
}

func TestWriterWithNullValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 3)
	chunk.SetCount(3)

	// First row - all valid
	chunk.GetVector(0).SetValue(0, int64(1))
	chunk.GetVector(1).SetValue(0, "Alice")

	// Second row - null id
	chunk.GetVector(0).SetValue(1, nil)
	chunk.GetVector(1).SetValue(1, "Bob")

	// Third row - null name
	chunk.GetVector(0).SetValue(2, int64(3))
	chunk.GetVector(1).SetValue(2, nil)

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back and verify nulls
	reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Get the raw record batch to check nulls
	record, err := reader.RecordBatchAt(0)
	require.NoError(t, err)
	defer record.Release()

	// Check null at row 1, column 0
	assert.True(t, record.Column(0).IsNull(1))
	// Check null at row 2, column 1
	assert.True(t, record.Column(1).IsNull(2))
}

func TestWriterRoundTrip(t *testing.T) {
	// Create original Arrow file
	originalPath := createTestArrowFile(t)
	defer func() { _ = os.Remove(originalPath) }()

	// Read from original
	reader, err := NewReaderFromPath(originalPath, nil)
	require.NoError(t, err)

	columns, err := reader.Schema()
	require.NoError(t, err)

	types, err := reader.Types()
	require.NoError(t, err)

	// Create output buffer
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema(columns)
	require.NoError(t, err)

	err = writer.SetTypes(types)
	require.NoError(t, err)

	// Copy all chunks
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		err = writer.WriteChunk(chunk)
		require.NoError(t, err)
	}

	err = reader.Close()
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read the copy and verify
	reader2, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader2.Close() }()

	columns2, err := reader2.Schema()
	require.NoError(t, err)
	assert.Equal(t, columns, columns2)

	chunk2, err := reader2.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk2.Count())

	assert.Equal(t, int64(1), chunk2.GetValue(0, 0))
	assert.Equal(t, "Alice", chunk2.GetValue(0, 1))
	assert.Equal(t, 1.5, chunk2.GetValue(0, 2))
}

func TestWriterVerifyWithIPC(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR})
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 2)
	chunk.SetCount(2)
	chunk.GetVector(0).SetValue(0, int64(1))
	chunk.GetVector(0).SetValue(1, int64(2))
	chunk.GetVector(1).SetValue(0, "Alice")
	chunk.GetVector(1).SetValue(1, "Bob")

	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify using Arrow IPC directly
	alloc := memory.NewGoAllocator()
	ipcReader, err := ipc.NewFileReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer func() { _ = ipcReader.Close() }()

	assert.Equal(t, 1, ipcReader.NumRecords())

	record, err := ipcReader.Record(0)
	require.NoError(t, err)

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())
}
