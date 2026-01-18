// Package parallel provides parallel query execution infrastructure.
// This file contains tests to verify that sequential (single-threaded) execution
// still works correctly, ensuring no regressions from parallel implementation.
package parallel

import (
	"context"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Sequential Fallback Tests
// ============================================================================

// TestSequentialFallback tests that execution works correctly with a single worker.
func TestSequentialFallback(t *testing.T) {
	chunks := createTestDataChunks(5000, 500)
	source := newTestTableSource(chunks)

	// Single worker pool
	pool := NewThreadPool(1)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Verify all rows processed
	totalRows := 0
	for _, chunk := range sink.chunks {
		totalRows += chunk.Count()
	}
	assert.Equal(t, 5000, totalRows, "all rows should be processed in sequential mode")

	// Verify data integrity
	var values []int32
	for _, chunk := range sink.chunks {
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			if v, ok := val.(int32); ok {
				values = append(values, v)
			}
		}
	}
	assert.Len(t, values, 5000)
}

// TestSmallQueryNoParallel tests that small queries run efficiently without
// unnecessary parallel overhead.
func TestSmallQueryNoParallel(t *testing.T) {
	// Very small dataset - less than MinRowsForParallel
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), "small_" + itoa(i)})
	}

	source := newTestTableSource([]*storage.DataChunk{chunk})

	// Even with multiple workers, small queries should work correctly
	pool := NewThreadPool(4)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 4,
	}

	start := time.Now()
	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	elapsed := time.Since(start)
	require.NoError(t, err)

	// Verify results
	totalRows := 0
	for _, c := range sink.chunks {
		totalRows += c.Count()
	}
	assert.Equal(t, 100, totalRows)

	// Small queries should complete quickly
	assert.Less(t, elapsed, 1*time.Second, "small query should complete quickly")
}

// TestDisabledParallelism tests execution with parallelism explicitly disabled.
func TestDisabledParallelism(t *testing.T) {
	chunks := createTestDataChunks(2000, 200)
	source := newTestTableSource(chunks)

	// Create pool with 1 worker (effectively sequential)
	pool := NewThreadPool(1)
	defer pool.Shutdown()

	assert.Equal(t, 1, pool.NumWorkers, "pool should have single worker")

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// All rows should be processed
	totalRows := 0
	for _, c := range sink.chunks {
		totalRows += c.Count()
	}
	assert.Equal(t, 2000, totalRows)
}

// TestSequentialHashJoin tests hash join with single worker.
func TestSequentialHashJoin(t *testing.T) {
	// Build side
	buildChunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}, 500)
	for i := 0; i < 500; i++ {
		buildChunk.AppendRow([]any{int32(i), int64(i * 100)})
	}

	// Probe side
	probeChunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 200)
	for i := 0; i < 200; i++ {
		probeChunk.AppendRow([]any{int32(i * 2), "name_" + itoa(i)})
	}

	buildSource := newTestTableSource([]*storage.DataChunk{buildChunk})
	probeSource := newTestTableSource([]*storage.DataChunk{probeChunk})

	pool := NewThreadPool(1) // Single worker
	defer pool.Shutdown()

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		4, // Still use partitions for correctness
	)
	join.SetBuildSchema(
		[]string{"id", "value"},
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
	)
	join.SetProbeSchema(
		[]string{"id", "name"},
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
	)

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	// Collect results
	var results [][]any
	for chunk := range resultChan {
		results = append(results, collectChunkValues(chunk)...)
	}

	// Should have matches for keys 0, 2, 4, ..., 398 (all even numbers < 400 that exist in build)
	// Probe keys are 0, 2, 4, ..., 398 (200 rows with i*2)
	// All 200 probe rows should match (since build has 0-499)
	assert.Equal(t, 200, len(results), "should have correct number of join results")
}

// TestSequentialAggregate tests aggregation with single worker.
func TestSequentialAggregate(t *testing.T) {
	// Create data with 10 groups
	chunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}, 1000)
	for i := 0; i < 1000; i++ {
		groupID := int32(i % 10)
		value := int64(i)
		chunk.AppendRow([]any{groupID, value})
	}

	source := newTestTableSource([]*storage.DataChunk{chunk})

	pool := NewThreadPool(1) // Single worker
	defer pool.Shutdown()

	aggregates := []AggregateFunc{
		NewAggregateFunc(AggSum, 1, "sum_value"),
		NewAggregateFunc(AggCount, 1, "count_value"),
	}

	agg := NewParallelAggregate(
		source,
		[]int{0},
		[]string{"group_id"},
		[]dukdb.Type{dukdb.TYPE_INTEGER},
		aggregates,
		1, // Single worker
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have 10 groups
	assert.Equal(t, 10, result.Count(), "should have 10 groups")

	// Verify count for each group (should be 100)
	for i := 0; i < result.Count(); i++ {
		countVal := result.GetValue(i, 2) // count is third column (after group key and sum)
		if count, ok := countVal.(int64); ok {
			assert.Equal(t, int64(100), count, "each group should have 100 rows")
		}
	}
}

// TestSequentialSort tests sorting with single worker.
func TestSequentialSort(t *testing.T) {
	// Create unsorted data
	chunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 500)
	for i := 0; i < 500; i++ {
		// Insert in reverse order
		value := int32(499 - i)
		chunk.AppendRow([]any{value, "name_" + itoa(int(value))})
	}

	source := newTestTableSource([]*storage.DataChunk{chunk})

	pool := NewThreadPool(1) // Single worker
	defer pool.Shutdown()

	sortOp := NewParallelSort(source, []SortKey{NewSortKey(0, "value")})
	sortOp.SetSchema([]string{"value", "name"},
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	ctx := context.Background()
	result, err := sortOp.Execute(pool, ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, 500, result.Count())

	// Verify sorted order
	for i := 0; i < result.Count(); i++ {
		val := result.GetValue(i, 0)
		if v, ok := val.(int32); ok {
			assert.Equal(t, int32(i), v, "value at position %d should be %d", i, i)
		}
	}
}

// TestSequentialPipeline tests pipeline execution with single worker.
func TestSequentialPipeline(t *testing.T) {
	chunks := createTestDataChunks(1000, 100)
	source := &testPipelineSource{chunks: chunks}

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	pipe := NewPipeline(1, "test_sequential")
	pipe.SetSource(source)
	pipe.Parallel = false // Explicitly disable parallel

	sink := NewChunkSink()
	pipe.SetSink(sink)

	ctx := context.Background()
	err := pipe.Execute(pool, ctx)
	require.NoError(t, err)

	result, err := pipe.GetResult()
	require.NoError(t, err)

	if result != nil {
		assert.Equal(t, 1000, result.Count())
	}
}

// testPipelineSource implements PipelineSource for testing.
type testPipelineSource struct {
	chunks []*storage.DataChunk
}

func (s *testPipelineSource) GenerateMorsels() []Morsel {
	morsels := make([]Morsel, 0, len(s.chunks))
	var startRow uint64
	for i, chunk := range s.chunks {
		morsels = append(morsels, Morsel{
			TableID:  1,
			StartRow: startRow,
			EndRow:   startRow + uint64(chunk.Count()),
			RowGroup: i,
		})
		startRow += uint64(chunk.Count())
	}
	return morsels
}

func (s *testPipelineSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if morsel.RowGroup < 0 || morsel.RowGroup >= len(s.chunks) {
		return nil, nil
	}
	return s.chunks[morsel.RowGroup].Clone(), nil
}

func (s *testPipelineSource) Schema() []ColumnDef {
	if len(s.chunks) == 0 {
		return nil
	}
	types := s.chunks[0].Types()
	cols := make([]ColumnDef, len(types))
	for i, t := range types {
		cols[i] = ColumnDef{Name: "col" + itoa(i), Type: t}
	}
	return cols
}

// ============================================================================
// Edge Cases with Sequential Execution
// ============================================================================

// TestSequentialEmptyInput tests sequential execution with empty input.
func TestSequentialEmptyInput(t *testing.T) {
	source := newTestTableSource([]*storage.DataChunk{})

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	assert.Equal(t, 0, sink.ChunkCount())
}

// TestSequentialSingleRow tests sequential execution with single row.
func TestSequentialSingleRow(t *testing.T) {
	chunk := storage.NewDataChunkWithCapacity([]dukdb.Type{dukdb.TYPE_INTEGER}, 1)
	chunk.AppendRow([]any{int32(42)})

	source := newTestTableSource([]*storage.DataChunk{chunk})

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	assert.Equal(t, 1, sink.ChunkCount())
	if len(sink.chunks) > 0 {
		assert.Equal(t, 1, sink.chunks[0].Count())
		val := sink.chunks[0].GetValue(0, 0)
		assert.Equal(t, int32(42), val)
	}
}

// TestSequentialWithOperators tests sequential execution with pipeline operators.
func TestSequentialWithOperators(t *testing.T) {
	// Create data
	chunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), int64(i * 10)})
	}

	source := newTestTableSource([]*storage.DataChunk{chunk})

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	// Add a filter operator (keep only even numbers)
	filterOp := NewFilterOp(&SimpleCompareFilter{
		ColumnIdx: 0,
		Op:        ">=",
		Value:     int32(50),
	})

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   []PipelineOp{filterOp},
		Sink:        sink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Should have 50 rows (values 50-99)
	totalRows := 0
	for _, c := range sink.chunks {
		totalRows += c.Count()
	}
	assert.Equal(t, 50, totalRows)
}

// TestSequentialCancellation tests that cancellation works in sequential mode.
func TestSequentialCancellation(t *testing.T) {
	chunks := createTestDataChunks(100000, 1000)
	source := newTestTableSource(chunks)

	pool := NewThreadPool(1)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 1,
	}

	// Cancel immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should complete quickly due to cancellation
	done := make(chan struct{})
	go func() {
		_ = pool.Execute(ctx, pipeline)
		close(done)
	}()

	select {
	case <-done:
		// Good, completed
	case <-time.After(5 * time.Second):
		t.Fatal("sequential execution did not stop on cancellation")
	}
}

// TestSequentialPoolReuse tests that a single-worker pool can be reused.
func TestSequentialPoolReuse(t *testing.T) {
	pool := NewThreadPool(1)
	defer pool.Shutdown()

	for i := 0; i < 5; i++ {
		chunks := createTestDataChunks(500, 50)
		source := newTestTableSource(chunks)

		sink := newMockSink()
		pipeline := &ParallelPipeline{
			Source:      source,
			Operators:   nil,
			Sink:        sink,
			Parallelism: 1,
		}

		ctx := context.Background()
		err := pool.Execute(ctx, pipeline)
		require.NoError(t, err)

		totalRows := 0
		for _, c := range sink.chunks {
			totalRows += c.Count()
		}
		assert.Equal(t, 500, totalRows, "iteration %d should process all rows", i)

		pool.Reset()
	}
}

// ============================================================================
// Comparison: Sequential vs Parallel Results
// ============================================================================

// TestSequentialParallelEquivalence ensures sequential and parallel produce same results.
func TestSequentialParallelEquivalence(t *testing.T) {
	chunks := createTestDataChunks(5000, 500)
	source := newTestTableSource(chunks)

	// Sequential execution
	seqPool := NewThreadPool(1)
	seqSink := newMockSink()
	seqPipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        seqSink,
		Parallelism: 1,
	}

	ctx := context.Background()
	err := seqPool.Execute(ctx, seqPipeline)
	require.NoError(t, err)
	seqPool.Shutdown()

	// Parallel execution
	parPool := NewThreadPool(4)
	parSink := newMockSink()
	parPipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        parSink,
		Parallelism: 4,
	}

	err = parPool.Execute(ctx, parPipeline)
	require.NoError(t, err)
	parPool.Shutdown()

	// Collect and compare results
	var seqRows [][]any
	for _, chunk := range seqSink.chunks {
		seqRows = append(seqRows, collectChunkValues(chunk)...)
	}

	var parRows [][]any
	for _, chunk := range parSink.chunks {
		parRows = append(parRows, collectChunkValues(chunk)...)
	}

	// Sort both by first column for comparison
	sortRowsByFirstColumn(seqRows)
	sortRowsByFirstColumn(parRows)

	assert.Equal(t, len(seqRows), len(parRows), "row counts should match")

	for i := range seqRows {
		for j := range seqRows[i] {
			seqFloat, seqOk := toSortFloat64(seqRows[i][j])
			parFloat, parOk := toSortFloat64(parRows[i][j])
			if seqOk && parOk {
				assert.Equal(t, seqFloat, parFloat, "value mismatch at row %d, col %d", i, j)
			} else {
				assert.Equal(t, seqRows[i][j], parRows[i][j], "value mismatch at row %d, col %d", i, j)
			}
		}
	}
}
