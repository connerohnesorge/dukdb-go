package arrow

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamWriterToBuffer(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
	require.NoError(t, err)

	err = writer.SetSchema([]string{"id", "name", "value"})
	require.NoError(t, err)

	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE})
	require.NoError(t, err)

	// Create and write a chunk
	chunk := createTestDataChunk(t)
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify the output is valid Arrow IPC stream
	assert.True(t, buf.Len() > 0)
	assert.True(t, IsArrowStream(buf.Bytes()))

	// Read back using stream reader
	reader, err := NewStreamReader(&buf, nil)
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
	assert.Equal(t, "Alice", readChunk.GetValue(0, 1))
	assert.Equal(t, 1.5, readChunk.GetValue(0, 2))
}

func TestStreamWriterToPath(t *testing.T) {
	// Create temp file path
	tmpFile, err := os.CreateTemp("", "test_arrow_stream_*.arrows")
	require.NoError(t, err)
	path := tmpFile.Name()
	_ = tmpFile.Close()
	_ = os.Remove(path) // Remove so writer can create it
	defer func() { _ = os.Remove(path) }()

	writer, err := NewStreamWriterToPath(path, nil)
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

	// Verify file exists
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.True(t, info.Size() > 0)

	// Read back
	reader, err := NewStreamReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, readChunk.Count())
	assert.Equal(t, int64(42), readChunk.GetValue(0, 0))
}

func TestStreamWriterToPathOverwrite(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_arrow_stream_*.arrows")
	require.NoError(t, err)
	path := tmpFile.Name()
	_, _ = tmpFile.WriteString("existing content")
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(path) }()

	// Try to create writer without overwrite - should fail
	_, err = NewStreamWriterToPath(path, nil)
	assert.Error(t, err)

	// Create writer with overwrite
	writer, err := NewStreamWriterToPathOverwrite(path, nil)
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

	// Verify file was overwritten
	reader, err := NewStreamReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readChunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, int64(42), readChunk.GetValue(0, 0))
}

func TestStreamWriterWithCompression(t *testing.T) {
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
			writer, err := NewStreamWriter(&buf, opts)
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
			reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			readChunk, err := reader.ReadChunk()
			require.NoError(t, err)
			assert.Equal(t, int64(42), readChunk.GetValue(0, 0))
		})
	}
}

func TestStreamWriterMultipleChunks(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

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

func TestStreamWriterNilChunk(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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

func TestStreamWriterAutoGeneratedColumnNames(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"column0", "column1"}, columns)
}

func TestStreamWriterVerifyWithIPC(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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
	ipcReader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer ipcReader.Release()

	// Read first record
	assert.True(t, ipcReader.Next())
	record := ipcReader.Record()
	require.NotNil(t, record)

	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	// No more records
	assert.False(t, ipcReader.Next())
}

func TestStreamWriterWithNullValues(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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

	// Read back using IPC directly to check nulls
	alloc := memory.NewGoAllocator()
	ipcReader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer ipcReader.Release()

	assert.True(t, ipcReader.Next())
	record := ipcReader.Record()

	// Check null at row 1, column 0
	assert.True(t, record.Column(0).IsNull(1))
	// Check null at row 2, column 1
	assert.True(t, record.Column(1).IsNull(2))
}

func TestStreamWriterRoundTrip(t *testing.T) {
	// Create original Arrow file
	originalPath := createTestArrowFile(t)
	defer func() { _ = os.Remove(originalPath) }()

	// Read from original file
	reader, err := NewReaderFromPath(originalPath, nil)
	require.NoError(t, err)

	columns, err := reader.Schema()
	require.NoError(t, err)

	types, err := reader.Types()
	require.NoError(t, err)

	// Create output buffer for stream
	var buf bytes.Buffer

	writer, err := NewStreamWriter(&buf, nil)
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

	// Read the stream copy and verify
	reader2, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
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

func TestFileVsStreamFormat(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.SetCount(1)
	chunk.GetVector(0).SetValue(0, int64(42))

	// Write as file format
	var fileBuf bytes.Buffer
	fileWriter, err := NewWriter(&fileBuf, nil)
	require.NoError(t, err)

	err = fileWriter.SetSchema([]string{"id"})
	require.NoError(t, err)
	err = fileWriter.SetTypes(types)
	require.NoError(t, err)
	err = fileWriter.WriteChunk(chunk)
	require.NoError(t, err)
	err = fileWriter.Close()
	require.NoError(t, err)

	// Write as stream format
	var streamBuf bytes.Buffer
	streamWriter, err := NewStreamWriter(&streamBuf, nil)
	require.NoError(t, err)

	err = streamWriter.SetSchema([]string{"id"})
	require.NoError(t, err)
	err = streamWriter.SetTypes(types)
	require.NoError(t, err)
	err = streamWriter.WriteChunk(chunk)
	require.NoError(t, err)
	err = streamWriter.Close()
	require.NoError(t, err)

	// Verify file format has ARROW1 magic
	assert.True(t, IsArrowFile(fileBuf.Bytes()))
	assert.False(t, IsArrowStream(fileBuf.Bytes()))

	// Verify stream format has continuation indicator
	assert.True(t, IsArrowStream(streamBuf.Bytes()))
	assert.False(t, IsArrowFile(streamBuf.Bytes()))

	// Both should be readable and contain same data
	fileReader, err := NewReader(bytes.NewReader(fileBuf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = fileReader.Close() }()

	streamReader, err := NewStreamReader(bytes.NewReader(streamBuf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = streamReader.Close() }()

	fileChunk, err := fileReader.ReadChunk()
	require.NoError(t, err)

	streamChunk, err := streamReader.ReadChunk()
	require.NoError(t, err)

	assert.Equal(t, fileChunk.GetValue(0, 0), streamChunk.GetValue(0, 0))
}
