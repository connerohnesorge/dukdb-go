package arrow

import (
	"bytes"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestArrowStream creates an Arrow IPC stream with test data in a buffer.
func createTestArrowStream(t *testing.T) *bytes.Buffer {
	t.Helper()

	// Create a simple schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	// Create a buffer to write to
	buf := &bytes.Buffer{}

	// Create an IPC stream writer
	alloc := memory.NewGoAllocator()
	writer := ipc.NewWriter(buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	// Build a record batch
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	bldr.Field(2).(*array.Float64Builder).AppendValues([]float64{1.5, 2.5, 3.5}, nil)

	record := bldr.NewRecord()
	defer record.Release()

	// Write the record batch
	err := writer.Write(record)
	require.NoError(t, err)

	// Add another batch
	bldr2 := array.NewRecordBuilder(alloc, schema)
	defer bldr2.Release()

	bldr2.Field(0).(*array.Int64Builder).AppendValues([]int64{4, 5}, nil)
	bldr2.Field(1).(*array.StringBuilder).AppendValues([]string{"Dave", "Eve"}, nil)
	bldr2.Field(2).(*array.Float64Builder).AppendValues([]float64{4.5, 5.5}, nil)

	record2 := bldr2.NewRecord()
	defer record2.Release()

	err = writer.Write(record2)
	require.NoError(t, err)

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	return buf
}

func TestStreamReaderBasic(t *testing.T) {
	// Create test stream
	buf := createTestArrowStream(t)

	// Open stream reader
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Check schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, columns)

	// Check types
	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}, types)

	// Read all data - the stream reader will read both batches into a single chunk
	// since MaxRowsPerChunk (2048) is larger than the total rows (5)
	totalRows := 0
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += chunk.Count()
	}

	// Total rows should be 5 (3 from first batch + 2 from second)
	assert.Equal(t, 5, totalRows)
}

func TestStreamReaderIterator(t *testing.T) {
	// Create test stream
	buf := createTestArrowStream(t)

	// Open stream reader
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Initialize the reader
	_, err = reader.Schema()
	require.NoError(t, err)

	// Use iterator pattern
	batchCount := 0
	totalRows := int64(0)

	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record)
		totalRows += record.NumRows()
		batchCount++
	}

	require.NoError(t, reader.Err())
	assert.Equal(t, 2, batchCount)
	assert.Equal(t, int64(5), totalRows)
}

func TestStreamReaderWithColumnProjection(t *testing.T) {
	// Create test stream
	buf := createTestArrowStream(t)

	// Open stream reader with column projection
	opts := &ReaderOptions{
		Columns: []string{"name", "id"},
	}
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Check schema
	columns, err := reader.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"name", "id"}, columns)

	// Check types (reordered)
	types, err := reader.Types()
	require.NoError(t, err)
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}, types)

	// Read chunk - will contain all 5 rows since MaxRowsPerChunk (2048) > 5
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 5, chunk.Count()) // 3 + 2 rows from both batches
	assert.Equal(t, 2, chunk.ColumnCount())

	// Verify projected data
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
	assert.Equal(t, int64(1), chunk.GetValue(0, 1))
}

func TestStreamReaderInvalidColumn(t *testing.T) {
	// Create test stream
	buf := createTestArrowStream(t)

	// Open stream reader with invalid column
	opts := &ReaderOptions{
		Columns: []string{"nonexistent"},
	}
	reader, err := NewStreamReader(bytes.NewReader(buf.Bytes()), opts)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Schema should fail
	_, err = reader.Schema()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}
