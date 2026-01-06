package arrow

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestArrowFile creates a temporary Arrow IPC file with test data.
func createTestArrowFile(t *testing.T) string {
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

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test_arrow_*.arrow")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	// Create an IPC file writer
	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	// Build a record batch
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	bldr.Field(2).(*array.Float64Builder).AppendValues([]float64{1.5, 2.5, 3.5}, nil)

	record := bldr.NewRecord()
	defer record.Release()

	// Write the record batch
	err = writer.Write(record)
	require.NoError(t, err)

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	return tmpFile.Name()
}

func TestReaderFromPath(t *testing.T) {
	// Create test file
	path := createTestArrowFile(t)
	defer func() { _ = os.Remove(path) }()

	// Open reader
	reader, err := NewReaderFromPath(path, nil)
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

	// Read chunk
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, 3, chunk.ColumnCount())

	// Verify data
	assert.Equal(t, int64(1), chunk.GetValue(0, 0))
	assert.Equal(t, int64(2), chunk.GetValue(1, 0))
	assert.Equal(t, int64(3), chunk.GetValue(2, 0))

	assert.Equal(t, "Alice", chunk.GetValue(0, 1))
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))
	assert.Equal(t, "Charlie", chunk.GetValue(2, 1))

	assert.Equal(t, 1.5, chunk.GetValue(0, 2))
	assert.Equal(t, 2.5, chunk.GetValue(1, 2))
	assert.Equal(t, 3.5, chunk.GetValue(2, 2))

	// Read again should return EOF
	_, err = reader.ReadChunk()
	assert.ErrorIs(t, err, io.EOF)
}

func TestReaderWithColumnProjection(t *testing.T) {
	// Create test file
	path := createTestArrowFile(t)
	defer func() { _ = os.Remove(path) }()

	// Open reader with column projection
	opts := &ReaderOptions{
		Columns: []string{"name", "id"},
	}
	reader, err := NewReaderFromPath(path, opts)
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

	// Read chunk
	chunk, err := reader.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, 2, chunk.ColumnCount())

	// Verify projected data (name is now column 0, id is column 1)
	assert.Equal(t, "Alice", chunk.GetValue(0, 0))
	assert.Equal(t, int64(1), chunk.GetValue(0, 1))
}

func TestReaderNumRecordBatches(t *testing.T) {
	// Create test file
	path := createTestArrowFile(t)
	defer func() { _ = os.Remove(path) }()

	// Open reader
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Check number of record batches
	assert.Equal(t, 1, reader.NumRecordBatches())
}

func TestReaderRecordBatchAt(t *testing.T) {
	// Create test file
	path := createTestArrowFile(t)
	defer func() { _ = os.Remove(path) }()

	// Open reader
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Get record batch
	record, err := reader.RecordBatchAt(0)
	require.NoError(t, err)
	defer record.Release()

	assert.Equal(t, int64(3), record.NumRows())
	assert.Equal(t, int64(3), record.NumCols())

	// Invalid index
	_, err = reader.RecordBatchAt(1)
	assert.Error(t, err)

	_, err = reader.RecordBatchAt(-1)
	assert.Error(t, err)
}

// createTestArrowFileWithMultipleBatches creates a test Arrow file with multiple record batches.
func createTestArrowFileWithMultipleBatches(t *testing.T, numBatches int, rowsPerBatch int) string {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "batch_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	tmpFile, err := os.CreateTemp("", "test_multi_batch_*.arrow")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	for batch := 0; batch < numBatches; batch++ {
		bldr := array.NewRecordBuilder(alloc, schema)

		batchIDs := make([]int32, rowsPerBatch)
		rowIDs := make([]int64, rowsPerBatch)
		values := make([]float64, rowsPerBatch)

		for row := 0; row < rowsPerBatch; row++ {
			batchIDs[row] = int32(batch)
			rowIDs[row] = int64(batch*rowsPerBatch + row)
			values[row] = float64(batch*100) + float64(row)
		}

		bldr.Field(0).(*array.Int32Builder).AppendValues(batchIDs, nil)
		bldr.Field(1).(*array.Int64Builder).AppendValues(rowIDs, nil)
		bldr.Field(2).(*array.Float64Builder).AppendValues(values, nil)

		record := bldr.NewRecord()
		err = writer.Write(record)
		record.Release()
		bldr.Release()
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	return tmpFile.Name()
}

func TestReaderRandomAccessMultipleBatches(t *testing.T) {
	const numBatches = 5
	const rowsPerBatch = 10

	path := createTestArrowFileWithMultipleBatches(t, numBatches, rowsPerBatch)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Check number of batches
	assert.Equal(t, numBatches, reader.NumRecordBatches())

	// Test accessing batches in random order
	order := []int{3, 0, 4, 1, 2}
	for _, idx := range order {
		record, err := reader.RecordBatchAt(idx)
		require.NoError(t, err, "failed to read batch %d", idx)

		assert.Equal(t, int64(rowsPerBatch), record.NumRows())
		assert.Equal(t, int64(3), record.NumCols())

		// Verify first row's batch_id matches expected batch index
		batchIDCol := record.Column(0).(*array.Int32)
		assert.Equal(t, int32(idx), batchIDCol.Value(0), "batch_id mismatch for batch %d", idx)

		// Verify first row_id value
		rowIDCol := record.Column(1).(*array.Int64)
		expectedRowID := int64(idx * rowsPerBatch)
		assert.Equal(t, expectedRowID, rowIDCol.Value(0), "row_id mismatch for batch %d", idx)

		record.Release()
	}
}

func TestReaderRandomAccessSameBatchTwice(t *testing.T) {
	path := createTestArrowFileWithMultipleBatches(t, 3, 5)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Read batch 1 twice
	record1, err := reader.RecordBatchAt(1)
	require.NoError(t, err)

	record2, err := reader.RecordBatchAt(1)
	require.NoError(t, err)

	// Both should have the same data
	assert.Equal(t, record1.NumRows(), record2.NumRows())

	batchID1 := record1.Column(0).(*array.Int32).Value(0)
	batchID2 := record2.Column(0).(*array.Int32).Value(0)
	assert.Equal(t, batchID1, batchID2)

	record1.Release()
	record2.Release()
}

func TestReaderRecordBatchAtBoundaryConditions(t *testing.T) {
	path := createTestArrowFileWithMultipleBatches(t, 3, 5)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	numBatches := reader.NumRecordBatches()
	assert.Equal(t, 3, numBatches)

	// Test first batch
	record, err := reader.RecordBatchAt(0)
	require.NoError(t, err)
	record.Release()

	// Test last batch
	record, err = reader.RecordBatchAt(numBatches - 1)
	require.NoError(t, err)
	record.Release()

	// Test out of bounds
	_, err = reader.RecordBatchAt(numBatches)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = reader.RecordBatchAt(-1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestArrowTypeToDuckDB(t *testing.T) {
	tests := []struct {
		name     string
		arrowTyp arrow.DataType
		expected dukdb.Type
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, dukdb.TYPE_BOOLEAN},
		{"int8", arrow.PrimitiveTypes.Int8, dukdb.TYPE_TINYINT},
		{"int16", arrow.PrimitiveTypes.Int16, dukdb.TYPE_SMALLINT},
		{"int32", arrow.PrimitiveTypes.Int32, dukdb.TYPE_INTEGER},
		{"int64", arrow.PrimitiveTypes.Int64, dukdb.TYPE_BIGINT},
		{"uint8", arrow.PrimitiveTypes.Uint8, dukdb.TYPE_UTINYINT},
		{"uint16", arrow.PrimitiveTypes.Uint16, dukdb.TYPE_USMALLINT},
		{"uint32", arrow.PrimitiveTypes.Uint32, dukdb.TYPE_UINTEGER},
		{"uint64", arrow.PrimitiveTypes.Uint64, dukdb.TYPE_UBIGINT},
		{"float32", arrow.PrimitiveTypes.Float32, dukdb.TYPE_FLOAT},
		{"float64", arrow.PrimitiveTypes.Float64, dukdb.TYPE_DOUBLE},
		{"string", arrow.BinaryTypes.String, dukdb.TYPE_VARCHAR},
		{"binary", arrow.BinaryTypes.Binary, dukdb.TYPE_BLOB},
		{"date32", arrow.FixedWidthTypes.Date32, dukdb.TYPE_DATE},
		{"time64us", arrow.FixedWidthTypes.Time64us, dukdb.TYPE_TIME},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ArrowTypeToDuckDB(tt.arrowTyp)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name     string
		header   []byte
		expected ArrowFormat
	}{
		{
			name:     "arrow file magic",
			header:   []byte("ARROW1\x00\x00"),
			expected: FormatFile,
		},
		{
			name:     "stream continuation",
			header:   []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00},
			expected: FormatStream,
		},
		{
			name:     "unknown",
			header:   []byte("unknown data"),
			expected: FormatUnknown,
		},
		{
			name:     "empty",
			header:   []byte{},
			expected: FormatUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, _, err := DetectFormat(bytes.NewReader(tt.header))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, format)
		})
	}
}

func TestDetectFormatFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected ArrowFormat
	}{
		{"data.arrow", FormatFile},
		{"data.feather", FormatFile},
		{"data.ipc", FormatFile},
		{"data.arrows", FormatStream},
		{"data.ARROW", FormatFile},
		{"data.csv", FormatUnknown},
		{"data.parquet", FormatUnknown},
		{"noextension", FormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			format := DetectFormatFromPath(tt.path)
			assert.Equal(t, tt.expected, format, "path: %s", tt.path)
		})
	}
}

func TestIsArrowFile(t *testing.T) {
	assert.True(t, IsArrowFile([]byte("ARROW1\x00\x00")))
	assert.False(t, IsArrowFile([]byte("notarrow")))
	assert.False(t, IsArrowFile([]byte{}))
}

func TestIsArrowStream(t *testing.T) {
	assert.True(t, IsArrowStream([]byte{0xFF, 0xFF, 0xFF, 0xFF}))
	assert.False(t, IsArrowStream([]byte{0x00, 0x00, 0x00, 0x00}))
	assert.False(t, IsArrowStream([]byte{}))
}
