package arrow

import (
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestArrowFileMultipleBatches creates a test Arrow file with multiple batches.
func createTestArrowFileMultipleBatches(t *testing.T, numBatches int) string {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	tmpFile, err := os.CreateTemp("", "test_arrow_multi_*.arrow")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Write multiple batches
	for batch := 0; batch < numBatches; batch++ {
		// Reset builder for new batch
		bldr.Field(0).(*array.Int64Builder).AppendValues(
			[]int64{int64(batch*10 + 1), int64(batch*10 + 2), int64(batch*10 + 3)},
			nil,
		)
		bldr.Field(1).(*array.Float64Builder).AppendValues(
			[]float64{float64(batch) + 0.1, float64(batch) + 0.2, float64(batch) + 0.3},
			nil,
		)

		record := bldr.NewRecord()
		err = writer.Write(record)
		record.Release()
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	return tmpFile.Name()
}

func TestBatchIterator_Reader(t *testing.T) {
	path := createTestArrowFileMultipleBatches(t, 3)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter := reader.Iterator()
	defer func() {
		if ri, ok := iter.(*ReaderIterator); ok {
			ri.Release()
		}
	}()

	// Check schema
	schema := iter.Schema()
	require.NotNil(t, schema)
	assert.Equal(t, 2, schema.NumFields())
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "value", schema.Field(1).Name)

	// Iterate over batches
	batchCount := 0
	for iter.Next() {
		record := iter.Record()
		require.NotNil(t, record)
		assert.Equal(t, int64(3), record.NumRows())
		assert.Equal(t, int64(2), record.NumCols())

		// Verify first value in batch
		idArr := record.Column(0).(*array.Int64)
		expectedID := int64(batchCount*10 + 1)
		assert.Equal(t, expectedID, idArr.Value(0))

		batchCount++
	}

	assert.NoError(t, iter.Err())
	assert.Equal(t, 3, batchCount)
}

// createTestArrowStreamFile creates a test Arrow stream file with multiple batches.
func createTestArrowStreamFile(t *testing.T, numBatches int) string {
	t.Helper()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	tmpFile, err := os.CreateTemp("", "test_arrow_stream_*.arrows")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	alloc := memory.NewGoAllocator()
	// Use ipc.NewWriter for stream format instead of ipc.NewFileWriter
	writer := ipc.NewWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	// Write multiple batches
	for batch := 0; batch < numBatches; batch++ {
		bldr.Field(0).(*array.Int64Builder).AppendValues(
			[]int64{int64(batch*10 + 1), int64(batch*10 + 2), int64(batch*10 + 3)},
			nil,
		)
		bldr.Field(1).(*array.Float64Builder).AppendValues(
			[]float64{float64(batch) + 0.1, float64(batch) + 0.2, float64(batch) + 0.3},
			nil,
		)

		record := bldr.NewRecord()
		err = writer.Write(record)
		record.Release()
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	return tmpFile.Name()
}

func TestBatchIterator_StreamReader(t *testing.T) {
	path := createTestArrowStreamFile(t, 2)
	defer func() { _ = os.Remove(path) }()

	streamReader, err := NewStreamReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = streamReader.Close() }()

	iter := streamReader.Iterator()

	// Check schema
	schema := iter.Schema()
	require.NotNil(t, schema)
	assert.Equal(t, 2, schema.NumFields())

	// Iterate over batches
	batchCount := 0
	for iter.Next() {
		record := iter.Record()
		require.NotNil(t, record)
		assert.Equal(t, int64(3), record.NumRows())
		batchCount++
	}

	assert.NoError(t, iter.Err())
	assert.Equal(t, 2, batchCount)
}

func TestBatchIterator_EmptyFile(t *testing.T) {
	// Create empty Arrow file (just schema, no batches)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	tmpFile, err := os.CreateTemp("", "test_empty_*.arrow")
	require.NoError(t, err)
	path := tmpFile.Name()
	defer func() { _ = os.Remove(path) }()

	alloc := memory.NewGoAllocator()
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Open and iterate
	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter := reader.Iterator()
	defer func() {
		if ri, ok := iter.(*ReaderIterator); ok {
			ri.Release()
		}
	}()

	// Should not have any batches
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestBatchIterator_SingleBatch(t *testing.T) {
	path := createTestArrowFileMultipleBatches(t, 1)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter := reader.Iterator()
	defer func() {
		if ri, ok := iter.(*ReaderIterator); ok {
			ri.Release()
		}
	}()

	// First Next should succeed
	assert.True(t, iter.Next())
	record := iter.Record()
	require.NotNil(t, record)
	assert.Equal(t, int64(3), record.NumRows())

	// Second Next should fail
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestBatchIterator_InterfaceCompliance(t *testing.T) {
	path := createTestArrowFileMultipleBatches(t, 1)
	defer func() { _ = os.Remove(path) }()

	reader, err := NewReaderFromPath(path, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Test that Iterator() returns BatchIterator interface
	var iter BatchIterator = reader.Iterator()
	require.NotNil(t, iter)

	// Schema should work without calling Next
	schema := iter.Schema()
	require.NotNil(t, schema)

	// Should be able to iterate using interface
	for iter.Next() {
		record := iter.Record()
		assert.NotNil(t, record)
	}

	assert.NoError(t, iter.Err())
}
