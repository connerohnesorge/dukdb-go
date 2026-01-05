package duckdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRowGroupWriter tests creating a new RowGroupWriter.
func TestNewRowGroupWriter(t *testing.T) {
	t.Parallel()

	// Create temp file for block manager
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeVarchar, TypeDouble}
	mods := []*TypeModifiers{nil, nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)
	require.NotNil(t, writer)

	assert.Equal(t, uint64(1), writer.TableOID())
	assert.Equal(t, 3, writer.ColumnCount())
	assert.Equal(t, uint64(0), writer.RowCount())
	assert.False(t, writer.IsFull())
	assert.Equal(t, uint64(0), writer.CurrentRowStart())
}

// TestRowGroupWriterAppendRow tests appending individual rows.
func TestRowGroupWriterAppendRow(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeVarchar}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Append a valid row
	err = writer.AppendRow([]any{int32(42), "hello"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), writer.RowCount())

	// Append another row
	err = writer.AppendRow([]any{int32(100), "world"})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), writer.RowCount())

	// Append row with NULL
	err = writer.AppendRow([]any{nil, "null value"})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), writer.RowCount())
}

// TestRowGroupWriterAppendRowErrors tests error cases for AppendRow.
func TestRowGroupWriterAppendRowErrors(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeVarchar}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Wrong column count - too few
	err = writer.AppendRow([]any{int32(42)})
	assert.ErrorIs(t, err, ErrColumnCountMismatch)

	// Wrong column count - too many
	err = writer.AppendRow([]any{int32(42), "hello", "extra"})
	assert.ErrorIs(t, err, ErrColumnCountMismatch)
}

// TestRowGroupWriterAppendRows tests appending multiple rows at once.
func TestRowGroupWriterAppendRows(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeDouble}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	rows := [][]any{
		{int32(1), 1.1},
		{int32(2), 2.2},
		{int32(3), 3.3},
		{int32(4), 4.4},
		{int32(5), 5.5},
	}

	count, err := writer.AppendRows(rows)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
	assert.Equal(t, uint64(5), writer.RowCount())
}

// TestRowGroupWriterFlush tests flushing buffered data.
func TestRowGroupWriterFlush(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeVarchar}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 42, types, mods, 100)

	// Flush empty - should return nil
	rgp, err := writer.Flush()
	require.NoError(t, err)
	assert.Nil(t, rgp)

	// Add some rows
	for i := 0; i < 10; i++ {
		err := writer.AppendRow([]any{int32(i), "test"})
		require.NoError(t, err)
	}

	// Flush with data
	rgp, err = writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp)

	// Verify row group pointer
	assert.Equal(t, uint64(42), rgp.TableOID)
	assert.Equal(t, uint64(100), rgp.RowStart)
	assert.Equal(t, uint64(10), rgp.TupleCount)
	assert.Equal(t, 2, len(rgp.DataPointers))

	// Verify writer state after flush
	assert.Equal(t, uint64(0), writer.RowCount())
	assert.Equal(t, uint64(110), writer.CurrentRowStart()) // Advanced by 10

	// Second flush should be empty
	rgp2, err := writer.Flush()
	require.NoError(t, err)
	assert.Nil(t, rgp2)
}

// TestRowGroupWriterFlushWithNulls tests flushing data with NULL values.
func TestRowGroupWriterFlushWithNulls(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger, TypeVarchar}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Add rows with NULLs
	rows := [][]any{
		{int32(1), "hello"},
		{nil, "world"},
		{int32(3), nil},
		{nil, nil},
		{int32(5), "end"},
	}

	_, err = writer.AppendRows(rows)
	require.NoError(t, err)

	// Flush
	rgp, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp)

	assert.Equal(t, uint64(5), rgp.TupleCount)
}

// TestRowGroupWriterMultipleFlushes tests multiple flush operations.
func TestRowGroupWriterMultipleFlushes(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 50)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeBigInt}
	mods := []*TypeModifiers{nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// First batch
	for i := 0; i < 5; i++ {
		err := writer.AppendRow([]any{int64(i)})
		require.NoError(t, err)
	}

	rgp1, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp1)
	assert.Equal(t, uint64(0), rgp1.RowStart)
	assert.Equal(t, uint64(5), rgp1.TupleCount)

	// Second batch
	for i := 5; i < 12; i++ {
		err := writer.AppendRow([]any{int64(i)})
		require.NoError(t, err)
	}

	rgp2, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp2)
	assert.Equal(t, uint64(5), rgp2.RowStart)
	assert.Equal(t, uint64(7), rgp2.TupleCount)

	// Third batch
	for i := 12; i < 15; i++ {
		err := writer.AppendRow([]any{int64(i)})
		require.NoError(t, err)
	}

	rgp3, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp3)
	assert.Equal(t, uint64(12), rgp3.RowStart)
	assert.Equal(t, uint64(3), rgp3.TupleCount)
}

// TestRowGroupWriterClose tests the Close method.
func TestRowGroupWriterClose(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger}
	mods := []*TypeModifiers{nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Add some rows
	for i := 0; i < 5; i++ {
		err := writer.AppendRow([]any{int32(i)})
		require.NoError(t, err)
	}

	// Close should flush remaining data
	rgp, err := writer.Close()
	require.NoError(t, err)
	require.NotNil(t, rgp)
	assert.Equal(t, uint64(5), rgp.TupleCount)

	// Further operations should fail
	err = writer.AppendRow([]any{int32(99)})
	assert.ErrorIs(t, err, ErrRowGroupWriterClosed)

	_, err = writer.Flush()
	assert.ErrorIs(t, err, ErrRowGroupWriterClosed)
}

// TestRowGroupWriterCloseEmpty tests closing an empty writer.
func TestRowGroupWriterCloseEmpty(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 10)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger}
	mods := []*TypeModifiers{nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Close empty writer
	rgp, err := writer.Close()
	require.NoError(t, err)
	assert.Nil(t, rgp)

	// Double close should be safe
	rgp, err = writer.Close()
	require.NoError(t, err)
	assert.Nil(t, rgp)
}

// TestEncodeColumnDataSimple tests encoding simple column data.
func TestEncodeColumnDataSimple(t *testing.T) {
	t.Parallel()

	// Test integer column
	data := []any{int32(1), int32(2), int32(3)}
	encoded, validity, err := encodeColumnData(data, TypeInteger, nil)
	require.NoError(t, err)
	assert.NotNil(t, encoded)
	assert.NotNil(t, validity)
	assert.True(t, validity.AllValid())
	assert.Equal(t, 12, len(encoded)) // 3 * 4 bytes
}

// TestEncodeColumnDataWithNulls tests encoding column data with NULL values.
func TestEncodeColumnDataWithNulls(t *testing.T) {
	t.Parallel()

	data := []any{int32(1), nil, int32(3), nil}
	encoded, validity, err := encodeColumnData(data, TypeInteger, nil)
	require.NoError(t, err)
	assert.NotNil(t, encoded)
	assert.NotNil(t, validity)
	assert.False(t, validity.AllValid())
	assert.True(t, validity.IsValid(0))
	assert.False(t, validity.IsValid(1))
	assert.True(t, validity.IsValid(2))
	assert.False(t, validity.IsValid(3))
	assert.Equal(t, 16, len(encoded)) // 4 * 4 bytes (including placeholders)
}

// TestEncodeColumnDataVariableSize tests encoding variable-size data.
func TestEncodeColumnDataVariableSize(t *testing.T) {
	t.Parallel()

	data := []any{"hello", "world"}
	encoded, validity, err := encodeColumnData(data, TypeVarchar, nil)
	require.NoError(t, err)
	assert.NotNil(t, encoded)
	assert.NotNil(t, validity)
	assert.True(t, validity.AllValid())

	// Each string: 4 bytes length + string bytes
	// "hello": 4 + 5 = 9
	// "world": 4 + 5 = 9
	// Total: 18
	assert.Equal(t, 18, len(encoded))
}

// TestEncodeColumnDataEmpty tests encoding empty column data.
func TestEncodeColumnDataEmpty(t *testing.T) {
	t.Parallel()

	data := []any{}
	encoded, validity, err := encodeColumnData(data, TypeInteger, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, len(encoded))
	assert.Nil(t, validity)
}

// TestComputeStatisticsNumeric tests statistics computation for numeric data.
func TestComputeStatisticsNumeric(t *testing.T) {
	t.Parallel()

	data := []any{int32(5), int32(2), int32(8), int32(1), int32(9)}
	stats := computeStatistics(data, TypeInteger)

	assert.True(t, stats.HasStats)
	assert.False(t, stats.HasNull)
	assert.Equal(t, uint64(0), stats.NullCount)
	assert.Equal(t, uint64(5), stats.DistinctCount)
	assert.NotNil(t, stats.StatData)
}

// TestComputeStatisticsWithNulls tests statistics computation with NULL values.
func TestComputeStatisticsWithNulls(t *testing.T) {
	t.Parallel()

	data := []any{int32(1), nil, int32(3), nil, nil}
	stats := computeStatistics(data, TypeInteger)

	assert.True(t, stats.HasStats)
	assert.True(t, stats.HasNull)
	assert.Equal(t, uint64(3), stats.NullCount)
	assert.Equal(t, uint64(2), stats.DistinctCount) // 1 and 3
}

// TestComputeStatisticsString tests statistics computation for string data.
func TestComputeStatisticsString(t *testing.T) {
	t.Parallel()

	data := []any{"a", "bb", "ccc", "dddd", "eeeee"}
	stats := computeStatistics(data, TypeVarchar)

	assert.True(t, stats.HasStats)
	assert.False(t, stats.HasNull)
	assert.Equal(t, uint64(5), stats.DistinctCount)
	assert.NotNil(t, stats.StatData)
}

// TestComputeStatisticsEmpty tests statistics computation for empty data.
func TestComputeStatisticsEmpty(t *testing.T) {
	t.Parallel()

	data := []any{}
	stats := computeStatistics(data, TypeInteger)

	assert.True(t, stats.HasStats)
	assert.False(t, stats.HasNull)
	assert.Equal(t, uint64(0), stats.NullCount)
	assert.Equal(t, uint64(0), stats.DistinctCount)
}

// TestRowGroupWriterDifferentTypes tests writing different data types.
func TestRowGroupWriterDifferentTypes(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 50)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{
		TypeBoolean,
		TypeTinyInt,
		TypeSmallInt,
		TypeInteger,
		TypeBigInt,
		TypeFloat,
		TypeDouble,
		TypeVarchar,
	}
	mods := make([]*TypeModifiers, len(types))

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Add rows with different types
	rows := [][]any{
		{true, int8(1), int16(100), int32(1000), int64(10000), float32(1.1), float64(1.11), "row1"},
		{false, int8(-1), int16(-100), int32(-1000), int64(-10000), float32(-1.1), float64(-1.11), "row2"},
		{nil, nil, nil, nil, nil, nil, nil, nil}, // All NULLs
	}

	_, err = writer.AppendRows(rows)
	require.NoError(t, err)

	rgp, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp)

	assert.Equal(t, uint64(3), rgp.TupleCount)
	assert.Equal(t, 8, len(rgp.DataPointers))
}

// TestRowGroupWriterCompressionSelection tests that compression is selected appropriately.
func TestRowGroupWriterCompressionSelection(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 50)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger}
	mods := []*TypeModifiers{nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Add constant data (should select CONSTANT compression)
	for i := 0; i < 100; i++ {
		err := writer.AppendRow([]any{int32(42)})
		require.NoError(t, err)
	}

	rgp, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp)
	assert.Equal(t, uint64(100), rgp.TupleCount)
}

// TestRowGroupWriterLargeDataset tests writing a larger dataset.
func TestRowGroupWriterLargeDataset(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 100)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeBigInt, TypeDouble}
	mods := []*TypeModifiers{nil, nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Write 1000 rows
	for i := 0; i < 1000; i++ {
		err := writer.AppendRow([]any{int64(i), float64(i) * 1.5})
		require.NoError(t, err)
	}

	rgp, err := writer.Flush()
	require.NoError(t, err)
	require.NotNil(t, rgp)
	assert.Equal(t, uint64(1000), rgp.TupleCount)
}

// TestRowGroupWriterConcurrentAccess tests thread safety.
func TestRowGroupWriterConcurrentAccess(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.duckdb")
	file, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	bm := NewBlockManager(file, DefaultBlockSize, 100)
	defer func() { _ = bm.Close() }()

	types := []LogicalTypeID{TypeInteger}
	mods := []*TypeModifiers{nil}

	writer := NewRowGroupWriter(bm, 1, types, mods, 0)

	// Concurrent reads of row count
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = writer.RowCount()
				_ = writer.IsFull()
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
