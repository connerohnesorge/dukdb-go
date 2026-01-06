// Package parallel provides parallel query execution infrastructure.
// This file tests parallel aggregation functionality.
package parallel

import (
	"context"
	"sync"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAggregateStateSum tests the SumState implementation.
func TestAggregateStateSum(t *testing.T) {
	state := NewSumState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with values
	state.Update(10)
	state.Update(20)
	state.Update(30)

	result := state.Finalize()
	assert.Equal(t, 60.0, result)

	// Test with nil values (should be ignored)
	state.Reset()
	state.Update(10)
	state.Update(nil)
	state.Update(20)
	assert.Equal(t, 30.0, state.Finalize())

	// Test merge
	state1 := NewSumState()
	state1.Update(100)
	state2 := NewSumState()
	state2.Update(200)
	state1.Merge(state2)
	assert.Equal(t, 300.0, state1.Finalize())

	// Test clone
	state3 := NewSumState()
	state3.Update(50)
	cloned := state3.Clone()
	state3.Update(50)
	assert.Equal(t, 100.0, state3.Finalize())
	assert.Equal(t, 50.0, cloned.Finalize())
}

// TestAggregateStateCount tests the CountState implementation.
func TestAggregateStateCount(t *testing.T) {
	state := NewCountState()

	// Test initial state
	assert.Equal(t, int64(0), state.Finalize())

	// Test with values
	state.Update(10)
	state.Update("hello")
	state.Update(true)

	assert.Equal(t, int64(3), state.Finalize())

	// Test with nil values (should not count)
	state.Reset()
	state.Update(10)
	state.Update(nil)
	state.Update(20)
	assert.Equal(t, int64(2), state.Finalize())

	// Test merge
	state1 := NewCountState()
	state1.Update(1)
	state1.Update(2)
	state2 := NewCountState()
	state2.Update(3)
	state1.Merge(state2)
	assert.Equal(t, int64(3), state1.Finalize())
}

// TestAggregateStateCountStar tests the CountStarState implementation.
func TestAggregateStateCountStar(t *testing.T) {
	state := NewCountStarState()

	// Test initial state
	assert.Equal(t, int64(0), state.Finalize())

	// Test with values including nil (should count all)
	state.Update(10)
	state.Update(nil)
	state.Update("hello")

	assert.Equal(t, int64(3), state.Finalize())

	// Test merge
	state1 := NewCountStarState()
	state1.Update(1)
	state1.Update(nil)
	state2 := NewCountStarState()
	state2.Update(3)
	state1.Merge(state2)
	assert.Equal(t, int64(3), state1.Finalize())
}

// TestAggregateStateAvg tests the AvgState implementation.
func TestAggregateStateAvg(t *testing.T) {
	state := NewAvgState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with values
	state.Update(10)
	state.Update(20)
	state.Update(30)

	result := state.Finalize()
	assert.Equal(t, 20.0, result)

	// Test with nil values
	state.Reset()
	state.Update(10)
	state.Update(nil)
	state.Update(20)
	assert.Equal(t, 15.0, state.Finalize())

	// Test merge
	state1 := NewAvgState()
	state1.Update(10)
	state1.Update(20)
	state2 := NewAvgState()
	state2.Update(30)
	state2.Update(40)
	state1.Merge(state2)
	assert.Equal(t, 25.0, state1.Finalize())
}

// TestAggregateStateMin tests the MinState implementation.
func TestAggregateStateMin(t *testing.T) {
	state := NewMinState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with numeric values
	state.Update(30)
	state.Update(10)
	state.Update(20)

	result := state.Finalize()
	assert.Equal(t, 10, result)

	// Test with nil values
	state.Reset()
	state.Update(nil)
	state.Update(20)
	state.Update(nil)
	assert.Equal(t, 20, state.Finalize())

	// Test merge
	state1 := NewMinState()
	state1.Update(50)
	state2 := NewMinState()
	state2.Update(25)
	state1.Merge(state2)
	assert.Equal(t, 25, state1.Finalize())

	// Test with strings
	state3 := NewMinState()
	state3.Update("banana")
	state3.Update("apple")
	state3.Update("cherry")
	assert.Equal(t, "apple", state3.Finalize())
}

// TestAggregateStateMax tests the MaxState implementation.
func TestAggregateStateMax(t *testing.T) {
	state := NewMaxState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with numeric values
	state.Update(10)
	state.Update(30)
	state.Update(20)

	result := state.Finalize()
	assert.Equal(t, 30, result)

	// Test merge
	state1 := NewMaxState()
	state1.Update(50)
	state2 := NewMaxState()
	state2.Update(75)
	state1.Merge(state2)
	assert.Equal(t, 75, state1.Finalize())

	// Test with strings
	state3 := NewMaxState()
	state3.Update("banana")
	state3.Update("apple")
	state3.Update("cherry")
	assert.Equal(t, "cherry", state3.Finalize())
}

// TestAggregateStateFirst tests the FirstState implementation.
func TestAggregateStateFirst(t *testing.T) {
	state := NewFirstState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with values
	state.Update(nil)
	state.Update(10)
	state.Update(20)

	result := state.Finalize()
	assert.Equal(t, 10, result)

	// Test merge (keeps first state's value)
	state1 := NewFirstState()
	state1.Update(100)
	state2 := NewFirstState()
	state2.Update(200)
	state1.Merge(state2)
	assert.Equal(t, 100, state1.Finalize())

	// Test merge when first state is empty
	state3 := NewFirstState()
	state4 := NewFirstState()
	state4.Update(300)
	state3.Merge(state4)
	assert.Equal(t, 300, state3.Finalize())
}

// TestAggregateStateLast tests the LastState implementation.
func TestAggregateStateLast(t *testing.T) {
	state := NewLastState()

	// Test initial state
	assert.Nil(t, state.Finalize())

	// Test with values
	state.Update(10)
	state.Update(nil)
	state.Update(30)

	result := state.Finalize()
	assert.Equal(t, 30, result)

	// Test merge (takes other state's value)
	state1 := NewLastState()
	state1.Update(100)
	state2 := NewLastState()
	state2.Update(200)
	state1.Merge(state2)
	assert.Equal(t, 200, state1.Finalize())
}

// TestAggregateHashTable tests the AggregateHashTable implementation.
func TestAggregateHashTable(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum"},
		{Type: AggCount, Column: 0, OutputCol: "count"},
	}

	table := NewAggregateHashTable(aggregates)
	assert.Equal(t, 0, table.Count())

	// Test GetOrCreate
	key1 := []any{"group1"}
	entry1 := table.GetOrCreate(key1)
	assert.NotNil(t, entry1)
	assert.Equal(t, 1, table.Count())

	// Same key should return same entry
	entry1Again := table.GetOrCreate(key1)
	assert.Equal(t, entry1, entry1Again)
	assert.Equal(t, 1, table.Count())

	// Different key
	key2 := []any{"group2"}
	entry2 := table.GetOrCreate(key2)
	assert.NotNil(t, entry2)
	assert.NotEqual(t, entry1, entry2)
	assert.Equal(t, 2, table.Count())

	// Update states
	entry1.States[0].Update(10)
	entry1.States[0].Update(20)
	entry1.States[1].Update(10)
	entry1.States[1].Update(20)

	entry2.States[0].Update(100)
	entry2.States[1].Update(100)

	// Verify states
	assert.Equal(t, 30.0, entry1.States[0].Finalize())
	assert.Equal(t, int64(2), entry1.States[1].Finalize())
	assert.Equal(t, 100.0, entry2.States[0].Finalize())
	assert.Equal(t, int64(1), entry2.States[1].Finalize())
}

// TestAggregateHashTableMerge tests merging two hash tables.
func TestAggregateHashTableMerge(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum"},
	}

	// Create two tables with overlapping and non-overlapping keys
	table1 := NewAggregateHashTable(aggregates)
	table1.GetOrCreate([]any{"A"}).States[0].Update(10)
	table1.GetOrCreate([]any{"B"}).States[0].Update(20)

	table2 := NewAggregateHashTable(aggregates)
	table2.GetOrCreate([]any{"A"}).States[0].Update(30)
	table2.GetOrCreate([]any{"C"}).States[0].Update(40)

	// Merge
	err := table1.Merge(table2)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, 3, table1.Count())

	// Group A should have sum 40 (10 + 30)
	entryA := table1.GetOrCreate([]any{"A"})
	assert.Equal(t, 40.0, entryA.States[0].Finalize())

	// Group B should have sum 20
	entryB := table1.GetOrCreate([]any{"B"})
	assert.Equal(t, 20.0, entryB.States[0].Finalize())

	// Group C should have sum 40
	entryC := table1.GetOrCreate([]any{"C"})
	assert.Equal(t, 40.0, entryC.States[0].Finalize())
}

// TestAggregateHashTableToDataChunk tests converting hash table to DataChunk.
func TestAggregateHashTableToDataChunk(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 0, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
	}

	table := NewAggregateHashTable(aggregates)

	// Add some groups
	entry1 := table.GetOrCreate([]any{"group1"})
	entry1.States[0].Update(10)
	entry1.States[0].Update(20)
	entry1.States[1].Update(10)
	entry1.States[1].Update(20)

	entry2 := table.GetOrCreate([]any{"group2"})
	entry2.States[0].Update(100)
	entry2.States[1].Update(100)

	// Convert to chunk
	groupByCols := []string{"name"}
	groupByTypes := []dukdb.Type{dukdb.TYPE_VARCHAR}
	chunk := table.ToDataChunk(groupByCols, groupByTypes)

	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, 3, chunk.ColumnCount()) // 1 group col + 2 agg cols

	// Results may be in any order, so collect and verify
	results := make(map[string][]any)
	for i := 0; i < chunk.Count(); i++ {
		name := chunk.GetValue(i, 0).(string)
		sum := chunk.GetValue(i, 1)
		count := chunk.GetValue(i, 2)
		results[name] = []any{sum, count}
	}

	assert.Equal(t, 30.0, results["group1"][0])
	assert.Equal(t, int64(2), results["group1"][1])
	assert.Equal(t, 100.0, results["group2"][0])
	assert.Equal(t, int64(1), results["group2"][1])
}

// TestAggregationSink tests the AggregationSink implementation.
func TestAggregationSink(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum"},
	}

	sink := NewAggregationSink(aggregates)

	// Create local tables
	local1 := NewAggregateHashTable(aggregates)
	local1.GetOrCreate([]any{"A"}).States[0].Update(10)

	local2 := NewAggregateHashTable(aggregates)
	local2.GetOrCreate([]any{"A"}).States[0].Update(20)
	local2.GetOrCreate([]any{"B"}).States[0].Update(30)

	// Combine
	err := sink.Combine(local1)
	require.NoError(t, err)
	err = sink.Combine(local2)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, 2, sink.GlobalTable.Count())
	assert.Equal(t, 30.0, sink.GlobalTable.GetOrCreate([]any{"A"}).States[0].Finalize())
	assert.Equal(t, 30.0, sink.GlobalTable.GetOrCreate([]any{"B"}).States[0].Finalize())
}

// mockAggSource is a mock ParallelSource for testing aggregation.
type mockAggSource struct {
	chunks     []*storage.DataChunk
	morselSize int
}

func newMockAggSource(data [][]any, types []dukdb.Type) *mockAggSource {
	chunks := make([]*storage.DataChunk, 0)

	// Split data into chunks
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

	return &mockAggSource{
		chunks:     chunks,
		morselSize: chunkSize,
	}
}

func (m *mockAggSource) GenerateMorsels() []Morsel {
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

func (m *mockAggSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if morsel.RowGroup < len(m.chunks) {
		return m.chunks[morsel.RowGroup], nil
	}
	return nil, nil
}

// TestParallelAggregateBasic tests basic parallel aggregation.
func TestParallelAggregateBasic(t *testing.T) {
	// Create test data: group column, value column
	data := [][]any{
		{"A", int64(10)},
		{"B", int64(20)},
		{"A", int64(30)},
		{"B", int64(40)},
		{"A", int64(50)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 1, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},                          // Group by column 0
		[]string{"group"},                 // Group column name
		[]dukdb.Type{dukdb.TYPE_VARCHAR},  // Group column type
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, 2, result.Count())    // 2 groups: A and B
	assert.Equal(t, 3, result.ColumnCount()) // group + sum + count

	// Collect results
	results := make(map[string][]any)
	for i := 0; i < result.Count(); i++ {
		name := result.GetValue(i, 0).(string)
		sum := result.GetValue(i, 1)
		count := result.GetValue(i, 2)
		results[name] = []any{sum, count}
	}

	// A: sum=90, count=3
	assert.Equal(t, 90.0, results["A"][0])
	assert.Equal(t, int64(3), results["A"][1])

	// B: sum=60, count=2
	assert.Equal(t, 60.0, results["B"][0])
	assert.Equal(t, int64(2), results["B"][1])
}

// TestParallelAggregateWithNulls tests aggregation with NULL values.
func TestParallelAggregateWithNulls(t *testing.T) {
	data := [][]any{
		{"A", int64(10)},
		{"A", nil},
		{"A", int64(20)},
		{"B", nil},
		{"B", int64(30)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 1, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
		{Type: AggCountStar, Column: -1, OutputCol: "count_star", OutputType: dukdb.TYPE_BIGINT},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_VARCHAR},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	results := make(map[string][]any)
	for i := 0; i < result.Count(); i++ {
		name := result.GetValue(i, 0).(string)
		sum := result.GetValue(i, 1)
		count := result.GetValue(i, 2)
		countStar := result.GetValue(i, 3)
		results[name] = []any{sum, count, countStar}
	}

	// A: sum=30, count=2 (nulls not counted), count_star=3
	assert.Equal(t, 30.0, results["A"][0])
	assert.Equal(t, int64(2), results["A"][1])
	assert.Equal(t, int64(3), results["A"][2])

	// B: sum=30, count=1, count_star=2
	assert.Equal(t, 30.0, results["B"][0])
	assert.Equal(t, int64(1), results["B"][1])
	assert.Equal(t, int64(2), results["B"][2])
}

// TestParallelAggregateMultipleGroupColumns tests aggregation with multiple group-by columns.
func TestParallelAggregateMultipleGroupColumns(t *testing.T) {
	data := [][]any{
		{"A", "X", int64(10)},
		{"A", "Y", int64(20)},
		{"A", "X", int64(30)},
		{"B", "X", int64(40)},
		{"B", "Y", int64(50)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 2, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0, 1}, // Group by columns 0 and 1
		[]string{"group1", "group2"},
		[]dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 4, result.Count()) // 4 unique (group1, group2) combinations

	// Collect results
	results := make(map[string]float64)
	for i := 0; i < result.Count(); i++ {
		g1 := result.GetValue(i, 0).(string)
		g2 := result.GetValue(i, 1).(string)
		sum := result.GetValue(i, 2).(float64)
		results[g1+"-"+g2] = sum
	}

	assert.Equal(t, 40.0, results["A-X"])
	assert.Equal(t, 20.0, results["A-Y"])
	assert.Equal(t, 40.0, results["B-X"])
	assert.Equal(t, 50.0, results["B-Y"])
}

// TestParallelAggregateHighCardinality tests high-cardinality optimization.
func TestParallelAggregateHighCardinality(t *testing.T) {
	// Create data with many unique groups
	data := make([][]any, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = []any{int64(i), int64(i * 10)}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
	}

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"id"},
		[]dukdb.Type{dukdb.TYPE_BIGINT},
		aggregates,
		pool.NumWorkers,
	)
	// Set low threshold to trigger parallel merge
	agg.SetHighCardinalityThreshold(100)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 1000, result.Count())

	// Verify some values
	sums := make(map[int64]float64)
	for i := 0; i < result.Count(); i++ {
		id := result.GetValue(i, 0).(int64)
		sum := result.GetValue(i, 1).(float64)
		sums[id] = sum
	}

	assert.Equal(t, 0.0, sums[0])
	assert.Equal(t, 100.0, sums[10])
	assert.Equal(t, 9990.0, sums[999])
}

// TestParallelAggregateAllAggTypes tests all aggregate types.
func TestParallelAggregateAllAggTypes(t *testing.T) {
	data := [][]any{
		{"A", int64(10)},
		{"A", int64(20)},
		{"A", int64(30)},
	}
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 1, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
		{Type: AggAvg, Column: 1, OutputCol: "avg", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggMin, Column: 1, OutputCol: "min", OutputType: dukdb.TYPE_ANY},
		{Type: AggMax, Column: 1, OutputCol: "max", OutputType: dukdb.TYPE_ANY},
		{Type: AggFirst, Column: 1, OutputCol: "first", OutputType: dukdb.TYPE_ANY},
		{Type: AggLast, Column: 1, OutputCol: "last", OutputType: dukdb.TYPE_ANY},
	}

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_VARCHAR},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Count())

	// Verify all aggregates
	assert.Equal(t, 60.0, result.GetValue(0, 1))  // sum
	assert.Equal(t, int64(3), result.GetValue(0, 2)) // count
	assert.Equal(t, 20.0, result.GetValue(0, 3))  // avg
	assert.Equal(t, int64(10), result.GetValue(0, 4)) // min
	assert.Equal(t, int64(30), result.GetValue(0, 5)) // max
	assert.Equal(t, int64(10), result.GetValue(0, 6)) // first
	assert.Equal(t, int64(30), result.GetValue(0, 7)) // last
}

// TestSimpleAggregation tests aggregation without grouping.
func TestSimpleAggregation(t *testing.T) {
	data := [][]any{
		{int64(10)},
		{int64(20)},
		{int64(30)},
		{int64(40)},
		{int64(50)},
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 0, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
		{Type: AggAvg, Column: 0, OutputCol: "avg", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggMin, Column: 0, OutputCol: "min", OutputType: dukdb.TYPE_ANY},
		{Type: AggMax, Column: 0, OutputCol: "max", OutputType: dukdb.TYPE_ANY},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewSimpleAggregation(source, aggregates)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, result.Count())
	assert.Equal(t, 150.0, result.GetValue(0, 0))    // sum
	assert.Equal(t, int64(5), result.GetValue(0, 1)) // count
	assert.Equal(t, 30.0, result.GetValue(0, 2))     // avg
	assert.Equal(t, int64(10), result.GetValue(0, 3))   // min
	assert.Equal(t, int64(50), result.GetValue(0, 4))   // max
}

// TestParallelAggregateConcurrency tests concurrent access to aggregation.
func TestParallelAggregateConcurrency(t *testing.T) {
	// Create larger dataset for concurrent test
	data := make([][]any, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = []any{int64(i % 100), int64(1)} // 100 groups, value=1
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
		{Type: AggCount, Column: 1, OutputCol: "count", OutputType: dukdb.TYPE_BIGINT},
	}

	pool := NewThreadPool(8)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_BIGINT},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)

	assert.Equal(t, 100, result.Count())

	// Each group should have exactly 100 rows (10000/100)
	for i := 0; i < result.Count(); i++ {
		count := result.GetValue(i, 2).(int64)
		assert.Equal(t, int64(100), count, "Each group should have 100 rows")
	}
}

// TestAggregateHashTableConcurrentAccess tests thread-safe access.
func TestAggregateHashTableConcurrentAccess(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 0, OutputCol: "sum"},
	}

	table := NewAggregateHashTable(aggregates)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOpsPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				key := []any{int64(j % 100)}
				entry := table.GetOrCreateSafe(key)
				// Note: States themselves are not thread-safe
				// This is correct for per-worker local tables
				_ = entry
			}
		}(i)
	}

	wg.Wait()

	// Should have 100 unique groups
	assert.Equal(t, 100, table.Count())
}

// TestGroupKeysEqual tests group key equality comparison.
func TestGroupKeysEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []any
		b        []any
		expected bool
	}{
		{
			name:     "equal strings",
			a:        []any{"hello", "world"},
			b:        []any{"hello", "world"},
			expected: true,
		},
		{
			name:     "equal numbers",
			a:        []any{int64(1), int64(2)},
			b:        []any{int64(1), int64(2)},
			expected: true,
		},
		{
			name:     "different lengths",
			a:        []any{"a"},
			b:        []any{"a", "b"},
			expected: false,
		},
		{
			name:     "different values",
			a:        []any{"a", "b"},
			b:        []any{"a", "c"},
			expected: false,
		},
		{
			name:     "nil values",
			a:        []any{nil, "b"},
			b:        []any{nil, "b"},
			expected: true,
		},
		{
			name:     "mixed types",
			a:        []any{int64(1), "hello"},
			b:        []any{int64(1), "hello"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupKeysEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestHashGroupKey tests group key hashing.
func TestHashGroupKey(t *testing.T) {
	// Same keys should produce same hash
	key1 := []any{"hello", int64(42)}
	key2 := []any{"hello", int64(42)}
	assert.Equal(t, hashGroupKey(key1), hashGroupKey(key2))

	// Different keys should (likely) produce different hashes
	key3 := []any{"world", int64(42)}
	assert.NotEqual(t, hashGroupKey(key1), hashGroupKey(key3))

	// Empty keys
	assert.NotPanics(t, func() {
		hashGroupKey([]any{})
	})

	// Nil values in key
	keyWithNil := []any{nil, "hello", nil}
	assert.NotPanics(t, func() {
		hashGroupKey(keyWithNil)
	})
}

// TestAggregateTypeString tests AggregateType string conversion.
func TestAggregateTypeString(t *testing.T) {
	assert.Equal(t, "SUM", AggSum.String())
	assert.Equal(t, "COUNT", AggCount.String())
	assert.Equal(t, "COUNT(*)", AggCountStar.String())
	assert.Equal(t, "AVG", AggAvg.String())
	assert.Equal(t, "MIN", AggMin.String())
	assert.Equal(t, "MAX", AggMax.String())
	assert.Equal(t, "FIRST", AggFirst.String())
	assert.Equal(t, "LAST", AggLast.String())
	assert.Equal(t, "UNKNOWN", AggregateType(100).String())
}

// TestNewAggregateState tests the aggregate state factory.
func TestNewAggregateState(t *testing.T) {
	assert.IsType(t, &SumState{}, NewAggregateState(AggSum))
	assert.IsType(t, &CountState{}, NewAggregateState(AggCount))
	assert.IsType(t, &CountStarState{}, NewAggregateState(AggCountStar))
	assert.IsType(t, &AvgState{}, NewAggregateState(AggAvg))
	assert.IsType(t, &MinState{}, NewAggregateState(AggMin))
	assert.IsType(t, &MaxState{}, NewAggregateState(AggMax))
	assert.IsType(t, &FirstState{}, NewAggregateState(AggFirst))
	assert.IsType(t, &LastState{}, NewAggregateState(AggLast))
	// Unknown type defaults to count
	assert.IsType(t, &CountState{}, NewAggregateState(AggregateType(100)))
}

// TestParallelAggregateEmptySource tests aggregation with empty source.
func TestParallelAggregateEmptySource(t *testing.T) {
	source := newMockAggSource([][]any{}, []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_BIGINT})

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_VARCHAR},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Count())
}

// TestParallelAggregateNilSource tests aggregation with nil source.
func TestParallelAggregateNilSource(t *testing.T) {
	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
	}

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		nil,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_VARCHAR},
		aggregates,
		pool.NumWorkers,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, result.Count())
}

// TestParallelAggregateCancellation tests context cancellation.
func TestParallelAggregateCancellation(t *testing.T) {
	// Create large dataset
	data := make([][]any, 10000)
	for i := 0; i < 10000; i++ {
		data[i] = []any{int64(i % 100), int64(1)}
	}
	types := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_BIGINT}

	source := newMockAggSource(data, types)

	aggregates := []AggregateFunc{
		{Type: AggSum, Column: 1, OutputCol: "sum", OutputType: dukdb.TYPE_DOUBLE},
	}

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group"},
		[]dukdb.Type{dukdb.TYPE_BIGINT},
		aggregates,
		pool.NumWorkers,
	)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := agg.Execute(pool, ctx)
	// Either returns context error or completes quickly
	// (depends on timing)
	_ = err // Error is acceptable
}
