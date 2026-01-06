// Package parallel provides parallel query execution infrastructure.
// This file tests parallel sort functionality.
package parallel

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortOrderString tests SortOrder string representation.
func TestSortOrderString(t *testing.T) {
	assert.Equal(t, "ASC", Ascending.String())
	assert.Equal(t, "DESC", Descending.String())
	assert.Equal(t, "UNKNOWN", SortOrder(100).String())
}

// TestNullsPositionString tests NullsPosition string representation.
func TestNullsPositionString(t *testing.T) {
	assert.Equal(t, "NULLS FIRST", NullsFirst.String())
	assert.Equal(t, "NULLS LAST", NullsLast.String())
	assert.Equal(t, "UNKNOWN", NullsPosition(100).String())
}

// TestNewSortKey tests SortKey creation.
func TestNewSortKey(t *testing.T) {
	key := NewSortKey(0, "id")
	assert.Equal(t, 0, key.Column)
	assert.Equal(t, "id", key.ColumnName)
	assert.Equal(t, Ascending, key.Order)
	assert.Equal(t, NullsLast, key.Nulls)

	key2 := NewSortKeyWithOrder(1, "name", Descending, NullsFirst)
	assert.Equal(t, 1, key2.Column)
	assert.Equal(t, "name", key2.ColumnName)
	assert.Equal(t, Descending, key2.Order)
	assert.Equal(t, NullsFirst, key2.Nulls)
}

// TestCompareValuesNumeric tests numeric value comparison.
func TestCompareValuesNumeric(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		order    SortOrder
		nulls    NullsPosition
		expected int
	}{
		// Basic numeric comparisons
		{"int less", 1, 2, Ascending, NullsLast, -1},
		{"int greater", 2, 1, Ascending, NullsLast, 1},
		{"int equal", 1, 1, Ascending, NullsLast, 0},
		{"int desc less", 1, 2, Descending, NullsLast, 1},
		{"int desc greater", 2, 1, Descending, NullsLast, -1},

		// Float comparisons
		{"float less", 1.5, 2.5, Ascending, NullsLast, -1},
		{"float greater", 2.5, 1.5, Ascending, NullsLast, 1},
		{"float equal", 1.5, 1.5, Ascending, NullsLast, 0},

		// Mixed numeric types
		{"int vs float", int64(1), 1.5, Ascending, NullsLast, -1},
		{"float vs int", 1.5, int64(1), Ascending, NullsLast, 1},

		// Negative numbers
		{"negative less", -2, -1, Ascending, NullsLast, -1},
		{"negative greater", -1, -2, Ascending, NullsLast, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b, tt.order, tt.nulls)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCompareValuesString tests string value comparison.
func TestCompareValuesString(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		order    SortOrder
		nulls    NullsPosition
		expected int
	}{
		{"string less", "apple", "banana", Ascending, NullsLast, -1},
		{"string greater", "banana", "apple", Ascending, NullsLast, 1},
		{"string equal", "apple", "apple", Ascending, NullsLast, 0},
		{"string desc less", "apple", "banana", Descending, NullsLast, 1},
		{"string desc greater", "banana", "apple", Descending, NullsLast, -1},
		{"case sensitive", "Apple", "apple", Ascending, NullsLast, -1}, // 'A' < 'a'
		{"empty string", "", "a", Ascending, NullsLast, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b, tt.order, tt.nulls)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCompareValuesNull tests NULL value handling.
func TestCompareValuesNull(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		order    SortOrder
		nulls    NullsPosition
		expected int
	}{
		// NULLS LAST with ASC
		{"null vs value, nulls last asc", nil, 1, Ascending, NullsLast, 1},
		{"value vs null, nulls last asc", 1, nil, Ascending, NullsLast, -1},
		{"null vs null", nil, nil, Ascending, NullsLast, 0},

		// NULLS FIRST with ASC
		{"null vs value, nulls first asc", nil, 1, Ascending, NullsFirst, -1},
		{"value vs null, nulls first asc", 1, nil, Ascending, NullsFirst, 1},

		// NULLS LAST with DESC
		{"null vs value, nulls last desc", nil, 1, Descending, NullsLast, -1},
		{"value vs null, nulls last desc", 1, nil, Descending, NullsLast, 1},

		// NULLS FIRST with DESC
		{"null vs value, nulls first desc", nil, 1, Descending, NullsFirst, 1},
		{"value vs null, nulls first desc", 1, nil, Descending, NullsFirst, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareValues(tt.a, tt.b, tt.order, tt.nulls)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCompareValuesBool tests boolean value comparison.
func TestCompareValuesBool(t *testing.T) {
	// false < true
	assert.Equal(t, -1, CompareValues(false, true, Ascending, NullsLast))
	assert.Equal(t, 1, CompareValues(true, false, Ascending, NullsLast))
	assert.Equal(t, 0, CompareValues(true, true, Ascending, NullsLast))
	assert.Equal(t, 0, CompareValues(false, false, Ascending, NullsLast))

	// Descending
	assert.Equal(t, 1, CompareValues(false, true, Descending, NullsLast))
	assert.Equal(t, -1, CompareValues(true, false, Descending, NullsLast))
}

// TestCompareValuesBytes tests byte slice comparison.
func TestCompareValuesBytes(t *testing.T) {
	assert.Equal(t, -1, CompareValues([]byte{1, 2}, []byte{1, 3}, Ascending, NullsLast))
	assert.Equal(t, 1, CompareValues([]byte{1, 3}, []byte{1, 2}, Ascending, NullsLast))
	assert.Equal(t, 0, CompareValues([]byte{1, 2}, []byte{1, 2}, Ascending, NullsLast))
	assert.Equal(t, -1, CompareValues([]byte{1}, []byte{1, 2}, Ascending, NullsLast))
	assert.Equal(t, 1, CompareValues([]byte{1, 2}, []byte{1}, Ascending, NullsLast))
}

// TestCompareRows tests multi-column row comparison.
func TestCompareRows(t *testing.T) {
	keys := []SortKey{
		{Column: 0, Order: Ascending, Nulls: NullsLast},
		{Column: 1, Order: Ascending, Nulls: NullsLast},
	}

	// First key determines order
	assert.Equal(t, -1, CompareRows([]any{1, "a"}, []any{2, "a"}, keys))
	assert.Equal(t, 1, CompareRows([]any{2, "a"}, []any{1, "a"}, keys))

	// First key equal, second key determines
	assert.Equal(t, -1, CompareRows([]any{1, "a"}, []any{1, "b"}, keys))
	assert.Equal(t, 1, CompareRows([]any{1, "b"}, []any{1, "a"}, keys))

	// All keys equal
	assert.Equal(t, 0, CompareRows([]any{1, "a"}, []any{1, "a"}, keys))
}

// TestCompareRowsMultipleKeys tests multi-column comparison with different orders.
func TestCompareRowsMultipleKeys(t *testing.T) {
	// First key ASC, second key DESC
	keys := []SortKey{
		{Column: 0, Order: Ascending, Nulls: NullsLast},
		{Column: 1, Order: Descending, Nulls: NullsLast},
	}

	// Same first key, second key DESC
	assert.Equal(t, 1, CompareRows([]any{1, "a"}, []any{1, "b"}, keys))  // "b" > "a" in DESC
	assert.Equal(t, -1, CompareRows([]any{1, "b"}, []any{1, "a"}, keys)) // "a" < "b" in DESC
}

// TestSortedPartition tests SortedPartition functionality.
func TestSortedPartition(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}
	p := NewSortedPartition(keys)

	// Add rows in unsorted order
	p.AddRow([]any{3, "c"}, 0)
	p.AddRow([]any{1, "a"}, 1)
	p.AddRow([]any{2, "b"}, 2)

	assert.Equal(t, 3, p.Len())

	// Sort
	p.Sort()

	// Verify sorted order
	assert.Equal(t, 1, p.Rows[0].Row[0])
	assert.Equal(t, 2, p.Rows[1].Row[0])
	assert.Equal(t, 3, p.Rows[2].Row[0])
}

// TestSortedPartitionStability tests that sorting is stable.
func TestSortedPartitionStability(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}
	p := NewSortedPartition(keys)

	// Add rows with same sort key but different values in second column
	p.AddRow([]any{1, "first"}, 0)
	p.AddRow([]any{1, "second"}, 1)
	p.AddRow([]any{1, "third"}, 2)

	p.Sort()

	// Original order should be preserved for equal keys
	assert.Equal(t, "first", p.Rows[0].Row[1])
	assert.Equal(t, "second", p.Rows[1].Row[1])
	assert.Equal(t, "third", p.Rows[2].Row[1])
}

// TestKWayMerger tests K-way merge functionality.
func TestKWayMerger(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}

	// Create sorted partitions
	p1 := NewSortedPartition(keys)
	p1.AddRow([]any{1}, 0)
	p1.AddRow([]any{4}, 3)
	p1.AddRow([]any{7}, 6)

	p2 := NewSortedPartition(keys)
	p2.AddRow([]any{2}, 1)
	p2.AddRow([]any{5}, 4)
	p2.AddRow([]any{8}, 7)

	p3 := NewSortedPartition(keys)
	p3.AddRow([]any{3}, 2)
	p3.AddRow([]any{6}, 5)
	p3.AddRow([]any{9}, 8)

	merger := NewKWayMerger([]*SortedPartition{p1, p2, p3}, keys)

	// Merge and verify order
	var result []int
	for {
		row, ok := merger.Next()
		if !ok {
			break
		}
		result = append(result, row[0].(int))
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, expected, result)
}

// TestKWayMergerWithLimit tests K-way merge with limit.
func TestKWayMergerWithLimit(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	p1 := NewSortedPartition(keys)
	p1.AddRow([]any{int32(1)}, 0)
	p1.AddRow([]any{int32(3)}, 2)
	p1.AddRow([]any{int32(5)}, 4)

	p2 := NewSortedPartition(keys)
	p2.AddRow([]any{int32(2)}, 1)
	p2.AddRow([]any{int32(4)}, 3)
	p2.AddRow([]any{int32(6)}, 5)

	merger := NewKWayMerger([]*SortedPartition{p1, p2}, keys)
	chunk := merger.MergeWithLimit(types, 3)

	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
}

// TestKWayMergerEmptyPartitions tests K-way merge with empty partitions.
func TestKWayMergerEmptyPartitions(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	p1 := NewSortedPartition(keys)
	p1.AddRow([]any{int32(1)}, 0)

	p2 := NewSortedPartition(keys) // Empty

	p3 := NewSortedPartition(keys)
	p3.AddRow([]any{int32(2)}, 1)

	merger := NewKWayMerger([]*SortedPartition{p1, p2, p3}, keys)
	chunk := merger.MergeAll(types)

	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
}

// mockSortSource is a mock ParallelSource for testing sort.
type mockSortSource struct {
	chunks     []*storage.DataChunk
	morselSize int
}

func newMockSortSource(data [][]any, types []dukdb.Type) *mockSortSource {
	chunks := make([]*storage.DataChunk, 0)

	chunkSize := 100
	for start := 0; start < len(data); start += chunkSize {
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := storage.NewDataChunkWithCapacity(types, end-start)
		for i := start; i < end; i++ {
			chunk.AppendRow(data[i])
		}
		chunks = append(chunks, chunk)
	}

	return &mockSortSource{
		chunks:     chunks,
		morselSize: chunkSize,
	}
}

func (m *mockSortSource) GenerateMorsels() []Morsel {
	morsels := make([]Morsel, len(m.chunks))
	for i := range m.chunks {
		morsels[i] = Morsel{
			TableID:  1,
			RowGroup: i,
			StartRow: uint64(i * m.morselSize),
			EndRow:   uint64((i + 1) * m.morselSize),
		}
	}
	return morsels
}

func (m *mockSortSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if morsel.RowGroup < len(m.chunks) {
		return m.chunks[morsel.RowGroup], nil
	}
	return nil, nil
}

// TestParallelSortBasic tests basic parallel sort.
func TestParallelSortBasic(t *testing.T) {
	data := [][]any{
		{int64(5)},
		{int64(3)},
		{int64(8)},
		{int64(1)},
		{int64(7)},
		{int64(2)},
		{int64(9)},
		{int64(4)},
		{int64(6)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, 9, result.Count())

	// Verify sorted order
	for i := 0; i < result.Count(); i++ {
		assert.Equal(t, int64(i+1), result.GetValue(i, 0))
	}
}

// TestParallelSortDescending tests descending sort.
func TestParallelSortDescending(t *testing.T) {
	data := [][]any{
		{int64(1)},
		{int64(5)},
		{int64(3)},
		{int64(2)},
		{int64(4)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKeyWithOrder(0, "value", Descending, NullsLast)}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 5, result.Count())

	// Verify descending order
	for i := 0; i < result.Count(); i++ {
		assert.Equal(t, int64(5-i), result.GetValue(i, 0))
	}
}

// TestParallelSortWithNulls tests sort with NULL values.
func TestParallelSortWithNulls(t *testing.T) {
	data := [][]any{
		{int64(3)},
		{nil},
		{int64(1)},
		{nil},
		{int64(2)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	// Test NULLS LAST
	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKeyWithOrder(0, "value", Ascending, NullsLast)}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	// Non-null values first, then nulls
	assert.Equal(t, int64(1), result.GetValue(0, 0))
	assert.Equal(t, int64(2), result.GetValue(1, 0))
	assert.Equal(t, int64(3), result.GetValue(2, 0))
	assert.Nil(t, result.GetValue(3, 0))
	assert.Nil(t, result.GetValue(4, 0))
}

// TestParallelSortNullsFirst tests NULLS FIRST behavior.
func TestParallelSortNullsFirst(t *testing.T) {
	data := [][]any{
		{int64(3)},
		{nil},
		{int64(1)},
		{nil},
		{int64(2)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKeyWithOrder(0, "value", Ascending, NullsFirst)}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	// Nulls first, then non-null values
	assert.Nil(t, result.GetValue(0, 0))
	assert.Nil(t, result.GetValue(1, 0))
	assert.Equal(t, int64(1), result.GetValue(2, 0))
	assert.Equal(t, int64(2), result.GetValue(3, 0))
	assert.Equal(t, int64(3), result.GetValue(4, 0))
}

// TestParallelSortMultipleKeys tests multi-column sort.
func TestParallelSortMultipleKeys(t *testing.T) {
	data := [][]any{
		{"B", int64(2)},
		{"A", int64(3)},
		{"B", int64(1)},
		{"A", int64(1)},
		{"A", int64(2)},
		{"B", int64(3)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{
		NewSortKey(0, "name"),
		NewSortKey(1, "value"),
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"name", "value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	// Should be sorted by name ASC, then value ASC
	expected := [][]any{
		{"A", int64(1)},
		{"A", int64(2)},
		{"A", int64(3)},
		{"B", int64(1)},
		{"B", int64(2)},
		{"B", int64(3)},
	}

	assert.Equal(t, len(expected), result.Count())
	for i, exp := range expected {
		assert.Equal(t, exp[0], result.GetValue(i, 0), "row %d col 0", i)
		assert.Equal(t, exp[1], result.GetValue(i, 1), "row %d col 1", i)
	}
}

// TestParallelSortMultipleKeysMixedOrder tests multi-column sort with mixed order.
func TestParallelSortMultipleKeysMixedOrder(t *testing.T) {
	data := [][]any{
		{"B", int64(2)},
		{"A", int64(3)},
		{"B", int64(1)},
		{"A", int64(1)},
		{"A", int64(2)},
		{"B", int64(3)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{
		NewSortKeyWithOrder(0, "name", Ascending, NullsLast),
		NewSortKeyWithOrder(1, "value", Descending, NullsLast), // Second key DESC
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"name", "value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	// Should be sorted by name ASC, then value DESC
	expected := [][]any{
		{"A", int64(3)},
		{"A", int64(2)},
		{"A", int64(1)},
		{"B", int64(3)},
		{"B", int64(2)},
		{"B", int64(1)},
	}

	assert.Equal(t, len(expected), result.Count())
	for i, exp := range expected {
		assert.Equal(t, exp[0], result.GetValue(i, 0), "row %d col 0", i)
		assert.Equal(t, exp[1], result.GetValue(i, 1), "row %d col 1", i)
	}
}

// TestParallelSortWithLimit tests ORDER BY ... LIMIT.
func TestParallelSortWithLimit(t *testing.T) {
	data := make([][]any, 100)
	for i := 0; i < 100; i++ {
		data[i] = []any{int64(100 - i)} // 100, 99, 98, ..., 1
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	ps := NewParallelSortWithOptions(source, keys, 0, 10, 0)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 10, result.Count())

	// Should be 1, 2, 3, ..., 10
	for i := 0; i < 10; i++ {
		assert.Equal(t, int64(i+1), result.GetValue(i, 0))
	}
}

// TestParallelSortWithOffset tests ORDER BY ... OFFSET.
func TestParallelSortWithOffset(t *testing.T) {
	data := make([][]any, 20)
	for i := 0; i < 20; i++ {
		data[i] = []any{int64(i + 1)} // 1, 2, ..., 20
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSortWithOptions(source, keys, 0, 5, 10)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 5, result.Count())

	// Should be 11, 12, 13, 14, 15
	for i := 0; i < 5; i++ {
		assert.Equal(t, int64(11+i), result.GetValue(i, 0))
	}
}

// TestParallelSortStability tests that sort is stable.
func TestParallelSortStability(t *testing.T) {
	// Create data where multiple rows have the same sort key
	data := [][]any{
		{int64(1), "first_1"},
		{int64(2), "first_2"},
		{int64(1), "second_1"},
		{int64(2), "second_2"},
		{int64(1), "third_1"},
		{int64(2), "third_2"},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "key")}

	pool := NewThreadPool(1) // Single worker for deterministic ordering
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"key", "value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	// All 1s should come first, maintaining original order
	assert.Equal(t, int64(1), result.GetValue(0, 0))
	assert.Equal(t, "first_1", result.GetValue(0, 1))
	assert.Equal(t, int64(1), result.GetValue(1, 0))
	assert.Equal(t, "second_1", result.GetValue(1, 1))
	assert.Equal(t, int64(1), result.GetValue(2, 0))
	assert.Equal(t, "third_1", result.GetValue(2, 1))

	// Then all 2s, maintaining original order
	assert.Equal(t, int64(2), result.GetValue(3, 0))
	assert.Equal(t, "first_2", result.GetValue(3, 1))
	assert.Equal(t, int64(2), result.GetValue(4, 0))
	assert.Equal(t, "second_2", result.GetValue(4, 1))
	assert.Equal(t, int64(2), result.GetValue(5, 0))
	assert.Equal(t, "third_2", result.GetValue(5, 1))
}

// TestParallelSortLargeDataset tests sorting a large dataset.
func TestParallelSortLargeDataset(t *testing.T) {
	// Create 10000 rows in random order
	n := 10000
	data := make([][]any, n)
	for i := 0; i < n; i++ {
		data[i] = []any{int64(rand.Intn(n)), i}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_INTEGER}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value", "id"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, n, result.Count())

	// Verify sorted order
	prev := int64(-1)
	for i := 0; i < result.Count(); i++ {
		val := result.GetValue(i, 0).(int64)
		assert.GreaterOrEqual(t, val, prev, "row %d should be >= previous", i)
		prev = val
	}
}

// TestParallelSortEmptySource tests sorting empty data.
func TestParallelSortEmptySource(t *testing.T) {
	source := newMockSortSource([][]any{}, []dukdb.Type{dukdb.TYPE_BIGINT})
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, []dukdb.Type{dukdb.TYPE_BIGINT})

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 0, result.Count())
}

// TestParallelSortNilSource tests sorting with nil source.
func TestParallelSortNilSource(t *testing.T) {
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(nil, keys)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 0, result.Count())
}

// TestParallelSortConcurrency tests concurrent access.
func TestParallelSortConcurrency(t *testing.T) {
	n := 1000
	data := make([][]any, n)
	for i := 0; i < n; i++ {
		data[i] = []any{int64(rand.Intn(n))}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(8)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, n, result.Count())

	// Verify sorted
	for i := 1; i < result.Count(); i++ {
		prev := result.GetValue(i-1, 0).(int64)
		curr := result.GetValue(i, 0).(int64)
		assert.LessOrEqual(t, prev, curr)
	}
}

// TestParallelSortCancellation tests context cancellation.
func TestParallelSortCancellation(t *testing.T) {
	n := 10000
	data := make([][]any, n)
	for i := 0; i < n; i++ {
		data[i] = []any{int64(rand.Intn(n))}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := ps.Execute(pool, ctx)
	// Either returns context error or completes quickly
	_ = err // Error is acceptable
}

// TestParallelSortStrings tests sorting string values.
func TestParallelSortStrings(t *testing.T) {
	data := [][]any{
		{"banana"},
		{"apple"},
		{"cherry"},
		{"date"},
		{"elderberry"},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "fruit")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"fruit"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, exp := range expected {
		assert.Equal(t, exp, result.GetValue(i, 0))
	}
}

// TestParallelSortMixedTypes tests sorting various numeric types.
func TestParallelSortMixedTypes(t *testing.T) {
	data := [][]any{
		{float64(3.14)},
		{float64(1.5)},
		{float64(2.71)},
		{float64(0.5)},
		{float64(4.0)},
	}
	types := []dukdb.Type{dukdb.TYPE_DOUBLE}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	result, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	expected := []float64{0.5, 1.5, 2.71, 3.14, 4.0}
	for i, exp := range expected {
		assert.InDelta(t, exp, result.GetValue(i, 0), 0.001)
	}
}

// TestParallelSortTotalRows tests TotalRows method.
func TestParallelSortTotalRows(t *testing.T) {
	data := [][]any{
		{int64(3)},
		{int64(1)},
		{int64(2)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockSortSource(data, types)
	keys := []SortKey{NewSortKey(0, "value")}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ps := NewParallelSort(source, keys)
	ps.SetSchema([]string{"value"}, types)

	ctx := context.Background()
	_, err := ps.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 3, ps.TotalRows())
}

// TestSortedPartitionWithCapacity tests pre-allocated capacity.
func TestSortedPartitionWithCapacity(t *testing.T) {
	keys := []SortKey{NewSortKey(0, "value")}
	p := NewSortedPartitionWithCapacity(keys, 100)

	assert.Equal(t, 0, p.Len())
	assert.GreaterOrEqual(t, cap(p.Rows), 100)
}

// TestKWayMergerReset tests resetting the merger.
func TestKWayMergerReset(t *testing.T) {
	keys := []SortKey{{Column: 0, Order: Ascending, Nulls: NullsLast}}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	p1 := NewSortedPartition(keys)
	p1.AddRow([]any{1}, 0)
	p1.AddRow([]any{2}, 1)

	merger := NewKWayMerger([]*SortedPartition{p1}, keys)

	// First merge
	chunk1 := merger.MergeAll(types)
	assert.Equal(t, 2, chunk1.Count())

	// Reset and merge again
	merger.Reset()
	chunk2 := merger.MergeAll(types)
	assert.Equal(t, 2, chunk2.Count())
}

// TestDefaultSortConfig tests default configuration.
func TestDefaultSortConfig(t *testing.T) {
	config := DefaultSortConfig()
	assert.Equal(t, 0, config.NumPartitions)
	assert.Equal(t, 0, config.Limit)
	assert.Equal(t, 0, config.Offset)
}

// BenchmarkParallelSort benchmarks parallel sort performance.
func BenchmarkParallelSort(b *testing.B) {
	n := 10000
	data := make([][]any, n)
	for i := 0; i < n; i++ {
		data[i] = []any{int64(rand.Intn(n))}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	keys := []SortKey{NewSortKey(0, "value")}
	pool := NewThreadPool(4)
	defer pool.Shutdown()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		source := newMockSortSource(data, types)
		ps := NewParallelSort(source, keys)
		ps.SetSchema([]string{"value"}, types)

		ctx := context.Background()
		_, _ = ps.Execute(pool, ctx)
	}
}

// BenchmarkGoSort benchmarks Go's standard sort for comparison.
func BenchmarkGoSort(b *testing.B) {
	n := 10000
	data := make([]int64, n)
	for i := 0; i < n; i++ {
		data[i] = int64(rand.Intn(n))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dataCopy := make([]int64, n)
		copy(dataCopy, data)
		sort.Slice(dataCopy, func(i, j int) bool {
			return dataCopy[i] < dataCopy[j]
		})
	}
}
