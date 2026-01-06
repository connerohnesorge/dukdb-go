package parallel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// RowGroupMeta Tests
// ============================================================================

func TestRowGroupMetaEndRow(t *testing.T) {
	rg := RowGroupMeta{
		ID:       0,
		StartRow: 100,
		RowCount: 500,
	}

	assert.Equal(t, uint64(600), rg.EndRow())
}

func TestRowGroupMetaZeroRows(t *testing.T) {
	rg := RowGroupMeta{
		ID:       0,
		StartRow: 0,
		RowCount: 0,
	}

	assert.Equal(t, uint64(0), rg.EndRow())
}

// ============================================================================
// InMemoryTableReader Tests
// ============================================================================

func TestInMemoryTableReaderBasic(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	require.NotNil(t, reader)

	// Register a table
	columnNames := []string{"id", "name", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, columnNames, columnTypes)

	// Get column info
	names, err := reader.GetColumnNames(1)
	require.NoError(t, err)
	assert.Equal(t, columnNames, names)

	types, err := reader.GetColumnTypes(1)
	require.NoError(t, err)
	assert.Equal(t, columnTypes, types)
}

func TestInMemoryTableReaderNotFound(t *testing.T) {
	reader := NewInMemoryTableReader(0)

	_, err := reader.GetColumnNames(999)
	assert.ErrorIs(t, err, ErrTableNotFound)

	_, err = reader.GetColumnTypes(999)
	assert.ErrorIs(t, err, ErrTableNotFound)

	_, err = reader.GetRowGroupMeta(999)
	assert.ErrorIs(t, err, ErrTableNotFound)

	_, err = reader.ReadRowGroup(999, 0, nil)
	assert.ErrorIs(t, err, ErrTableNotFound)
}

func TestInMemoryTableReaderAddChunk(t *testing.T) {
	reader := NewInMemoryTableReader(0)

	columnNames := []string{"id", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, columnNames, columnTypes)

	// Add a chunk
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), float64(i) * 1.5})
	}
	reader.AddChunk(1, chunk)

	// Get row group meta
	meta, err := reader.GetRowGroupMeta(1)
	require.NoError(t, err)
	require.Len(t, meta, 1)
	assert.Equal(t, 0, meta[0].ID)
	assert.Equal(t, uint64(0), meta[0].StartRow)
	assert.Equal(t, uint64(100), meta[0].RowCount)
}

func TestInMemoryTableReaderReadRowGroup(t *testing.T) {
	reader := NewInMemoryTableReader(0)

	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	reader.RegisterTable(1, []string{"id", "name"}, columnTypes)

	// Add chunks
	chunk1 := storage.NewDataChunkWithCapacity(columnTypes, 50)
	for i := 0; i < 50; i++ {
		chunk1.AppendRow([]any{int32(i), "name" + string(rune('A'+i%26))})
	}
	reader.AddChunk(1, chunk1)

	chunk2 := storage.NewDataChunkWithCapacity(columnTypes, 30)
	for i := 0; i < 30; i++ {
		chunk2.AppendRow([]any{int32(i + 50), "name" + string(rune('A'+(i+50)%26))})
	}
	reader.AddChunk(1, chunk2)

	// Read first row group
	result, err := reader.ReadRowGroup(1, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, 50, result.Count())

	// Read second row group
	result, err = reader.ReadRowGroup(1, 1, nil)
	require.NoError(t, err)
	assert.Equal(t, 30, result.Count())

	// Invalid row group
	_, err = reader.ReadRowGroup(1, 5, nil)
	assert.ErrorIs(t, err, ErrRowGroupNotFound)
}

func TestInMemoryTableReaderReadWithProjection(t *testing.T) {
	reader := NewInMemoryTableReader(0)

	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "name", "value"}, columnTypes)

	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i), "name", float64(i)})
	}
	reader.AddChunk(1, chunk)

	// Read with projection (only id and value)
	result, err := reader.ReadRowGroup(1, 0, []int{0, 2})
	require.NoError(t, err)
	assert.Equal(t, 10, result.Count())
	assert.Equal(t, 2, result.ColumnCount())

	// Check values
	assert.Equal(t, int32(0), result.GetValue(0, 0))
	assert.Equal(t, float64(0), result.GetValue(0, 1))
}

// ============================================================================
// ParallelTableScan Tests
// ============================================================================

func TestParallelTableScanCreation(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnNames := []string{"id", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}

	scan := NewParallelTableScan(1, "test_table", columnNames, columnTypes, reader)
	require.NotNil(t, scan)

	assert.Equal(t, uint64(1), scan.TableOID)
	assert.Equal(t, "test_table", scan.TableName)
	assert.Equal(t, columnNames, scan.Columns)
	assert.Equal(t, columnTypes, scan.ColumnTypes)
	assert.True(t, scan.Config.EnableFilterPushdown)
	assert.True(t, scan.Config.EnableProjectionPushdown)
}

func TestParallelTableScanWithConfig(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	config := ScanConfig{
		EnableFilterPushdown:     false,
		EnableProjectionPushdown: false,
		MorselConfig: MorselConfig{
			MinSize:    500,
			MaxSize:    5000,
			TargetSize: 2000,
		},
	}

	scan := NewParallelTableScanWithConfig(
		1, "test", []string{"col"}, []dukdb.Type{dukdb.TYPE_INTEGER},
		reader, config,
	)

	assert.False(t, scan.Config.EnableFilterPushdown)
	assert.False(t, scan.Config.EnableProjectionPushdown)
}

func TestParallelTableScanSetProjections(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnNames := []string{"id", "name", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}

	scan := NewParallelTableScan(1, "test", columnNames, columnTypes, reader)

	// Set by index
	scan.SetProjections([]int{0, 2})
	assert.Equal(t, []int{0, 2}, scan.Projections)

	// Set by name
	scan.SetProjectionsByName([]string{"name", "id"})
	assert.Equal(t, []int{1, 0}, scan.Projections)
}

func TestParallelTableScanProjectedColumns(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnNames := []string{"id", "name", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}

	scan := NewParallelTableScan(1, "test", columnNames, columnTypes, reader)

	// Without projection
	assert.Equal(t, columnTypes, scan.ProjectedColumnTypes())
	assert.Equal(t, columnNames, scan.ProjectedColumnNames())

	// With projection
	scan.SetProjections([]int{2, 0})
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_DOUBLE, dukdb.TYPE_INTEGER}, scan.ProjectedColumnTypes())
	assert.Equal(t, []string{"value", "id"}, scan.ProjectedColumnNames())
}

func TestParallelTableScanGenerateMorsels(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	reader.RegisterTable(1, []string{"id"}, columnTypes)

	// Add multiple chunks
	for i := 0; i < 5; i++ {
		chunk := storage.NewDataChunkWithCapacity(columnTypes, 1000)
		for j := 0; j < 1000; j++ {
			chunk.AppendRow([]any{int32(i*1000 + j)})
		}
		reader.AddChunk(1, chunk)
	}

	scan := NewParallelTableScan(1, "test", []string{"id"}, columnTypes, reader)

	morsels := scan.GenerateMorsels()
	require.NotEmpty(t, morsels)

	// Verify all rows are covered
	var totalRows uint64
	for _, m := range morsels {
		totalRows += m.RowCount()
		assert.Equal(t, uint64(1), m.TableID)
	}
	assert.Equal(t, uint64(5000), totalRows)
}

func TestParallelTableScanGenerateMorselsWithExplicitRowGroups(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}

	scan := NewParallelTableScan(1, "test", []string{"id"}, columnTypes, reader)

	// Set explicit row groups
	scan.SetRowGroups([]RowGroupMeta{
		{ID: 0, StartRow: 0, RowCount: 2000},
		{ID: 1, StartRow: 2000, RowCount: 3000},
		{ID: 2, StartRow: 5000, RowCount: 1500},
	})

	morsels := scan.GenerateMorsels()
	require.NotEmpty(t, morsels)

	assert.Equal(t, uint64(6500), scan.TotalRowCount())
	assert.Equal(t, 3, scan.RowGroupCount())
}

func TestParallelTableScanScanNoReader(t *testing.T) {
	scan := NewParallelTableScan(1, "test", []string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}, nil)

	_, err := scan.Scan(Morsel{TableID: 1, RowGroup: 0})
	assert.ErrorIs(t, err, ErrNoDataReader)
}

func TestParallelTableScanScan(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "value"}, columnTypes)

	// Add test data
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), float64(i) * 2.0})
	}
	reader.AddChunk(1, chunk)

	scan := NewParallelTableScan(1, "test", []string{"id", "value"}, columnTypes, reader)

	result, err := scan.Scan(Morsel{TableID: 1, RowGroup: 0})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 100, result.Count())
	assert.Equal(t, 2, result.ColumnCount())
}

func TestParallelTableScanWithFilter(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "value"}, columnTypes)

	chunk := storage.NewDataChunkWithCapacity(columnTypes, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), float64(i)})
	}
	reader.AddChunk(1, chunk)

	scan := NewParallelTableScan(1, "test", []string{"id", "value"}, columnTypes, reader)

	// Filter: id >= 50
	scan.SetFilter(&SimpleCompareFilter{
		ColumnIdx: 0,
		Op:        ">=",
		Value:     int32(50),
	})

	result, err := scan.Scan(Morsel{TableID: 1, RowGroup: 0})
	require.NoError(t, err)
	assert.Equal(t, 50, result.Count())

	// Verify first value is 50
	assert.Equal(t, int32(50), result.GetValue(0, 0))
}

func TestParallelTableScanWithProjection(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "name", "value"}, columnTypes)

	chunk := storage.NewDataChunkWithCapacity(columnTypes, 50)
	for i := 0; i < 50; i++ {
		chunk.AppendRow([]any{int32(i), "test", float64(i)})
	}
	reader.AddChunk(1, chunk)

	scan := NewParallelTableScan(1, "test", []string{"id", "name", "value"}, columnTypes, reader)
	scan.SetProjections([]int{0, 2}) // Only id and value

	result, err := scan.Scan(Morsel{TableID: 1, RowGroup: 0})
	require.NoError(t, err)
	assert.Equal(t, 50, result.Count())
	assert.Equal(t, 2, result.ColumnCount())
}

// ============================================================================
// Filter Tests
// ============================================================================

func TestSimpleCompareFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i), "name"})
	}

	tests := []struct {
		name     string
		filter   *SimpleCompareFilter
		expected []int // Expected row indices that pass
	}{
		{
			name:     "equals",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: "=", Value: int32(5)},
			expected: []int{5},
		},
		{
			name:     "not equals",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: "!=", Value: int32(5)},
			expected: []int{0, 1, 2, 3, 4, 6, 7, 8, 9},
		},
		{
			name:     "greater than",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: ">", Value: int32(7)},
			expected: []int{8, 9},
		},
		{
			name:     "greater or equal",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(8)},
			expected: []int{8, 9},
		},
		{
			name:     "less than",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: "<", Value: int32(3)},
			expected: []int{0, 1, 2},
		},
		{
			name:     "less or equal",
			filter:   &SimpleCompareFilter{ColumnIdx: 0, Op: "<=", Value: int32(2)},
			expected: []int{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var passing []int
			for i := 0; i < chunk.Count(); i++ {
				if tt.filter.Evaluate(chunk, i) {
					passing = append(passing, i)
				}
			}
			assert.Equal(t, tt.expected, passing)
		})
	}
}

func TestAndFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Filter: id >= 3 AND id < 7
	andFilter := &AndFilter{
		Filters: []FilterExpr{
			&SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(3)},
			&SimpleCompareFilter{ColumnIdx: 0, Op: "<", Value: int32(7)},
		},
	}

	var passing []int
	for i := 0; i < chunk.Count(); i++ {
		if andFilter.Evaluate(chunk, i) {
			passing = append(passing, i)
		}
	}

	assert.Equal(t, []int{3, 4, 5, 6}, passing)
}

func TestOrFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Filter: id < 2 OR id > 7
	orFilter := &OrFilter{
		Filters: []FilterExpr{
			&SimpleCompareFilter{ColumnIdx: 0, Op: "<", Value: int32(2)},
			&SimpleCompareFilter{ColumnIdx: 0, Op: ">", Value: int32(7)},
		},
	}

	var passing []int
	for i := 0; i < chunk.Count(); i++ {
		if orFilter.Evaluate(chunk, i) {
			passing = append(passing, i)
		}
	}

	assert.Equal(t, []int{0, 1, 8, 9}, passing)
}

func TestEmptyOrFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Empty OR filter should pass all
	orFilter := &OrFilter{Filters: []FilterExpr{}}

	for i := 0; i < chunk.Count(); i++ {
		assert.True(t, orFilter.Evaluate(chunk, i))
	}
}

func TestStringFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	chunk.AppendRow([]any{"apple"})
	chunk.AppendRow([]any{"banana"})
	chunk.AppendRow([]any{"cherry"})
	chunk.AppendRow([]any{"date"})
	chunk.AppendRow([]any{"elderberry"})

	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: "cherry"}

	var passing []int
	for i := 0; i < chunk.Count(); i++ {
		if filter.Evaluate(chunk, i) {
			passing = append(passing, i)
		}
	}

	assert.Equal(t, []int{2, 3, 4}, passing) // cherry, date, elderberry
}

// ============================================================================
// ApplyFilter Tests
// ============================================================================

func TestApplyFilterNil(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Nil filter returns original chunk
	result := ApplyFilter(chunk, nil)
	assert.Equal(t, chunk, result)

	// Nil chunk returns nil
	result = ApplyFilter(nil, &SimpleCompareFilter{})
	assert.Nil(t, result)
}

func TestApplyFilterAllMatch(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Filter that matches all
	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(0)}
	result := ApplyFilter(chunk, filter)

	assert.Equal(t, chunk, result) // Should return original chunk
}

func TestApplyFilterNoneMatch(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Filter that matches none
	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: "<", Value: int32(0)}
	result := ApplyFilter(chunk, filter)

	assert.Equal(t, 0, result.Count())
}

func TestApplyFilterSomeMatch(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i), "name"})
	}

	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(7)}
	result := ApplyFilter(chunk, filter)

	assert.Equal(t, 3, result.Count())
	assert.Equal(t, int32(7), result.GetValue(0, 0))
	assert.Equal(t, int32(8), result.GetValue(1, 0))
	assert.Equal(t, int32(9), result.GetValue(2, 0))
}

func TestApplyFilterFunc(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Filter function: even numbers only
	result := ApplyFilterFunc(chunk, func(c *storage.DataChunk, idx int) bool {
		val := c.GetValue(idx, 0)
		if val == nil {
			return false
		}
		return val.(int32)%2 == 0
	})

	assert.Equal(t, 5, result.Count())
}

// ============================================================================
// ApplyProjection Tests
// ============================================================================

func TestApplyProjectionNil(t *testing.T) {
	result := ApplyProjection(nil, []int{0, 1})
	assert.Nil(t, result)
}

func TestApplyProjectionEmpty(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i), "name"})
	}

	// Empty projections returns original
	result := ApplyProjection(chunk, nil)
	assert.Equal(t, chunk, result)

	result = ApplyProjection(chunk, []int{})
	assert.Equal(t, chunk, result)
}

func TestApplyProjection(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i), "name", float64(i) * 2.0})
	}

	// Project only first and last columns
	result := ApplyProjection(chunk, []int{0, 2})

	assert.Equal(t, 5, result.Count())
	assert.Equal(t, 2, result.ColumnCount())

	// Verify data
	for i := 0; i < 5; i++ {
		assert.Equal(t, int32(i), result.GetValue(i, 0))
		assert.Equal(t, float64(i)*2.0, result.GetValue(i, 1))
	}
}

func TestApplyProjectionReorder(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 3)
	chunk.AppendRow([]any{int32(1), "a", float64(1.0)})
	chunk.AppendRow([]any{int32(2), "b", float64(2.0)})
	chunk.AppendRow([]any{int32(3), "c", float64(3.0)})

	// Reorder columns: double, varchar, integer
	result := ApplyProjection(chunk, []int{2, 1, 0})

	assert.Equal(t, 3, result.Count())
	assert.Equal(t, 3, result.ColumnCount())

	// Verify reordered data
	assert.Equal(t, float64(1.0), result.GetValue(0, 0))
	assert.Equal(t, "a", result.GetValue(0, 1))
	assert.Equal(t, int32(1), result.GetValue(0, 2))
}

// ============================================================================
// ApplyFilterAndProjection Tests
// ============================================================================

func TestApplyFilterAndProjection(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i), "name", float64(i)})
	}

	// Filter: id >= 5
	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(5)}
	// Project: id and value
	projections := []int{0, 2}

	result := ApplyFilterAndProjection(chunk, filter, projections)

	assert.Equal(t, 5, result.Count())
	assert.Equal(t, 2, result.ColumnCount())

	// Verify first row
	assert.Equal(t, int32(5), result.GetValue(0, 0))
	assert.Equal(t, float64(5), result.GetValue(0, 1))
}

func TestApplyFilterAndProjectionNoFilter(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 5)
	for i := 0; i < 5; i++ {
		chunk.AppendRow([]any{int32(i), "name"})
	}

	result := ApplyFilterAndProjection(chunk, nil, []int{1})

	assert.Equal(t, 5, result.Count())
	assert.Equal(t, 1, result.ColumnCount())
}

func TestApplyFilterAndProjectionNoProjection(t *testing.T) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, 10)
	for i := 0; i < 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: "<", Value: int32(5)}
	result := ApplyFilterAndProjection(chunk, filter, nil)

	assert.Equal(t, 5, result.Count())
	assert.Equal(t, 1, result.ColumnCount())
}

// ============================================================================
// Integration Tests: Parallel Scan with Thread Pool
// ============================================================================

func TestParallelScanWithThreadPool(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "value"}, columnTypes)

	// Add multiple chunks
	numChunks := 10
	rowsPerChunk := 100
	for i := 0; i < numChunks; i++ {
		chunk := storage.NewDataChunkWithCapacity(columnTypes, rowsPerChunk)
		for j := 0; j < rowsPerChunk; j++ {
			chunk.AppendRow([]any{int32(i*rowsPerChunk + j), float64(j)})
		}
		reader.AddChunk(1, chunk)
	}

	scan := NewParallelTableScan(1, "test", []string{"id", "value"}, columnTypes, reader)

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	sink := &testSink{}
	pipeline := &ParallelPipeline{
		Source:      scan,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 4,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Verify total row count
	totalRows := sink.totalRows()
	assert.Equal(t, numChunks*rowsPerChunk, totalRows)
}

func TestParallelScanWithFilterAndProjection(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "name", "value"}, columnTypes)

	// Add data
	numChunks := 5
	for i := 0; i < numChunks; i++ {
		chunk := storage.NewDataChunkWithCapacity(columnTypes, 200)
		for j := 0; j < 200; j++ {
			chunk.AppendRow([]any{int32(i*200 + j), "name", float64(j)})
		}
		reader.AddChunk(1, chunk)
	}

	scan := NewParallelTableScan(1, "test", []string{"id", "name", "value"}, columnTypes, reader)

	// Set filter: id >= 500
	scan.SetFilter(&SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(500)})
	// Set projection: only id and value
	scan.SetProjections([]int{0, 2})

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	sink := &testSink{}
	pipeline := &ParallelPipeline{
		Source:      scan,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 2,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Should have 500 rows (ids 500-999)
	assert.Equal(t, 500, sink.totalRows())

	// All chunks should have 2 columns
	for _, chunk := range sink.chunks {
		assert.Equal(t, 2, chunk.ColumnCount())
	}
}

func TestParallelScanConcurrency(t *testing.T) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	reader.RegisterTable(1, []string{"id"}, columnTypes)

	// Add many small chunks
	numChunks := 100
	for i := 0; i < numChunks; i++ {
		chunk := storage.NewDataChunkWithCapacity(columnTypes, 50)
		for j := 0; j < 50; j++ {
			chunk.AppendRow([]any{int32(i*50 + j)})
		}
		reader.AddChunk(1, chunk)
	}

	scan := NewParallelTableScan(1, "test", []string{"id"}, columnTypes, reader)

	// Run with different worker counts
	for _, numWorkers := range []int{1, 2, 4, 8} {
		t.Run("workers_"+string(rune('0'+numWorkers)), func(t *testing.T) {
			pool := NewThreadPool(numWorkers)
			defer pool.Shutdown()

			sink := &testSink{}
			pipeline := &ParallelPipeline{
				Source:      scan,
				Operators:   nil,
				Sink:        sink,
				Parallelism: numWorkers,
			}

			ctx := context.Background()
			err := pool.Execute(ctx, pipeline)
			require.NoError(t, err)

			// All rows should be processed
			assert.Equal(t, numChunks*50, sink.totalRows())
		})
	}
}

// testSink is a thread-safe sink for testing.
type testSink struct {
	mu     sync.Mutex
	chunks []*storage.DataChunk
	count  atomic.Int32
}

func (s *testSink) Combine(chunk *storage.DataChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, chunk.Clone())
	s.count.Add(1)
	return nil
}

func (s *testSink) totalRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, c := range s.chunks {
		total += c.Count()
	}
	return total
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkApplyFilter(b *testing.B) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, storage.StandardVectorSize)
	for i := 0; i < storage.StandardVectorSize; i++ {
		chunk.AppendRow([]any{int32(i), float64(i)})
	}

	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(1000)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ApplyFilter(chunk, filter)
	}
}

func BenchmarkApplyProjection(b *testing.B) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE, dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, storage.StandardVectorSize)
	for i := 0; i < storage.StandardVectorSize; i++ {
		chunk.AppendRow([]any{int32(i), "test", float64(i), int64(i)})
	}

	projections := []int{0, 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ApplyProjection(chunk, projections)
	}
}

func BenchmarkApplyFilterAndProjection(b *testing.B) {
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}
	chunk := storage.NewDataChunkWithCapacity(columnTypes, storage.StandardVectorSize)
	for i := 0; i < storage.StandardVectorSize; i++ {
		chunk.AppendRow([]any{int32(i), "test", float64(i)})
	}

	filter := &SimpleCompareFilter{ColumnIdx: 0, Op: ">=", Value: int32(1000)}
	projections := []int{0, 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ApplyFilterAndProjection(chunk, filter, projections)
	}
}

func BenchmarkParallelScan(b *testing.B) {
	reader := NewInMemoryTableReader(0)
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	reader.RegisterTable(1, []string{"id", "value"}, columnTypes)

	// Add chunks
	for i := 0; i < 10; i++ {
		chunk := storage.NewDataChunkWithCapacity(columnTypes, storage.StandardVectorSize)
		for j := 0; j < storage.StandardVectorSize; j++ {
			chunk.AppendRow([]any{int32(j), float64(j)})
		}
		reader.AddChunk(1, chunk)
	}

	scan := NewParallelTableScan(1, "test", []string{"id", "value"}, columnTypes, reader)

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink := &testSink{}
		pipeline := &ParallelPipeline{
			Source:      scan,
			Sink:        sink,
			Parallelism: 4,
		}
		_ = pool.Execute(context.Background(), pipeline)
	}
}
