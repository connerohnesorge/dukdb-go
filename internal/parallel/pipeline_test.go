package parallel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipelineEvent tests the PipelineEvent synchronization primitive.
func TestPipelineEvent(t *testing.T) {
	t.Run("basic completion", func(t *testing.T) {
		event := NewPipelineEvent("test")
		assert.False(t, event.IsComplete())

		event.Complete()
		assert.True(t, event.IsComplete())
	})

	t.Run("idempotent completion", func(t *testing.T) {
		event := NewPipelineEvent("test")
		event.Complete()
		event.Complete() // Should not panic
		assert.True(t, event.IsComplete())
	})

	t.Run("wait for dependencies", func(t *testing.T) {
		dep1 := NewPipelineEvent("dep1")
		dep2 := NewPipelineEvent("dep2")
		event := NewPipelineEvent("main", dep1, dep2)

		var done int32

		go func() {
			event.Wait()
			atomic.StoreInt32(&done, 1)
		}()

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, int32(0), atomic.LoadInt32(&done))

		dep1.Complete()
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, int32(0), atomic.LoadInt32(&done))

		dep2.Complete()
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, int32(1), atomic.LoadInt32(&done))
	})

	t.Run("wait with context cancellation", func(t *testing.T) {
		dep := NewPipelineEvent("dep")
		event := NewPipelineEvent("main", dep)

		ctx, cancel := context.WithCancel(context.Background())

		var err error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = event.WaitContext(ctx)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()

		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

// TestPipeline tests basic pipeline functionality.
func TestPipeline(t *testing.T) {
	t.Run("new pipeline", func(t *testing.T) {
		pipe := NewPipeline(1, "test")
		assert.Equal(t, 1, pipe.ID)
		assert.Equal(t, "test", pipe.Name)
		assert.True(t, pipe.Parallel)
		assert.Empty(t, pipe.Operators)
	})

	t.Run("add operator", func(t *testing.T) {
		pipe := NewPipeline(1, "test")
		pipe.AddOperator(NewFilterOp(nil))
		pipe.AddOperator(NewProjectOp([]int{0, 1}))

		assert.Len(t, pipe.Operators, 2)
	})

	t.Run("add dependency", func(t *testing.T) {
		dep := NewPipelineEvent("dep")
		pipe := NewPipeline(1, "test")
		pipe.AddDependency(dep)

		assert.Len(t, pipe.Dependencies, 1)
	})
}

// mockPipelineSource is a test source for pipelines.
type mockPipelineSource struct {
	morsels []Morsel
	chunks  []*storage.DataChunk
	schema  []ColumnDef
	scanIdx int32
	mu      sync.Mutex
}

func newMockPipelineSource(rows int, cols int) *mockPipelineSource {
	// Create column types and schema
	types := make([]dukdb.Type, cols)
	schema := make([]ColumnDef, cols)
	for i := 0; i < cols; i++ {
		types[i] = dukdb.TYPE_INTEGER
		schema[i] = ColumnDef{Name: "col" + string(rune('a'+i)), Type: dukdb.TYPE_INTEGER}
	}

	// Create chunks
	chunkSize := storage.StandardVectorSize
	numChunks := (rows + chunkSize - 1) / chunkSize
	chunks := make([]*storage.DataChunk, numChunks)
	morsels := make([]Morsel, numChunks)

	rowIdx := 0
	for i := 0; i < numChunks; i++ {
		chunkRows := chunkSize
		if rowIdx+chunkRows > rows {
			chunkRows = rows - rowIdx
		}

		chunk := storage.NewDataChunkWithCapacity(types, chunkRows)
		for r := 0; r < chunkRows; r++ {
			rowVals := make([]any, cols)
			for c := 0; c < cols; c++ {
				rowVals[c] = rowIdx + r + c
			}
			chunk.AppendRow(rowVals)
		}
		chunks[i] = chunk

		morsels[i] = Morsel{
			TableID:  1,
			StartRow: uint64(rowIdx),
			EndRow:   uint64(rowIdx + chunkRows),
			RowGroup: i,
		}

		rowIdx += chunkRows
	}

	return &mockPipelineSource{
		morsels: morsels,
		chunks:  chunks,
		schema:  schema,
	}
}

func (m *mockPipelineSource) GenerateMorsels() []Morsel {
	return m.morsels
}

func (m *mockPipelineSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if morsel.RowGroup < 0 || morsel.RowGroup >= len(m.chunks) {
		return nil, nil
	}
	atomic.AddInt32(&m.scanIdx, 1)
	return m.chunks[morsel.RowGroup].Clone(), nil
}

func (m *mockPipelineSource) Schema() []ColumnDef {
	return m.schema
}

func TestPipelineExecution(t *testing.T) {
	t.Run("execute empty pipeline", func(t *testing.T) {
		pipe := NewPipeline(1, "empty")
		pool := NewThreadPool(2)
		defer pool.Shutdown()

		err := pipe.Execute(pool, context.Background())
		require.NoError(t, err)
	})

	t.Run("execute with source only", func(t *testing.T) {
		source := newMockPipelineSource(100, 3)
		pipe := NewPipeline(1, "source_only")
		pipe.SetSource(source)

		sink := NewChunkSink()
		pipe.SetSink(sink)

		pool := NewThreadPool(2)
		defer pool.Shutdown()

		err := pipe.Execute(pool, context.Background())
		require.NoError(t, err)

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 100, result.Count())
	})

	t.Run("execute with filter", func(t *testing.T) {
		source := newMockPipelineSource(100, 3)
		pipe := NewPipeline(1, "with_filter")
		pipe.SetSource(source)

		// Filter where first column > 50
		filter := &SimpleCompareFilter{
			ColumnIdx: 0,
			Op:        ">",
			Value:     50,
		}
		pipe.AddOperator(NewFilterOp(filter))

		sink := NewChunkSink()
		pipe.SetSink(sink)

		pool := NewThreadPool(2)
		defer pool.Shutdown()

		err := pipe.Execute(pool, context.Background())
		require.NoError(t, err)

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Less(t, result.Count(), 100)
	})

	t.Run("execute with projection", func(t *testing.T) {
		source := newMockPipelineSource(50, 5)
		pipe := NewPipeline(1, "with_project")
		pipe.SetSource(source)

		pipe.AddOperator(NewProjectOp([]int{0, 2, 4}))

		sink := NewChunkSink()
		pipe.SetSink(sink)

		pool := NewThreadPool(2)
		defer pool.Shutdown()

		err := pipe.Execute(pool, context.Background())
		require.NoError(t, err)

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 50, result.Count())
		assert.Equal(t, 3, result.ColumnCount())
	})

	t.Run("execute sequential", func(t *testing.T) {
		source := newMockPipelineSource(100, 2)
		pipe := NewPipeline(1, "sequential")
		pipe.SetSource(source)
		pipe.Parallel = false // Force sequential execution

		sink := NewChunkSink()
		pipe.SetSink(sink)

		pool := NewThreadPool(4)
		defer pool.Shutdown()

		err := pipe.Execute(pool, context.Background())
		require.NoError(t, err)

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 100, result.Count())
	})

	t.Run("execute with dependencies", func(t *testing.T) {
		depEvent := NewPipelineEvent("dependency")

		pipe := NewPipeline(1, "with_dep")
		pipe.AddDependency(depEvent)
		pipe.SetSource(newMockPipelineSource(10, 2))
		pipe.SetSink(NewChunkSink())

		pool := NewThreadPool(2)
		defer pool.Shutdown()

		// Start execution in goroutine
		var execErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			execErr = pipe.Execute(pool, context.Background())
		}()

		// Give time for pipeline to wait
		time.Sleep(20 * time.Millisecond)

		// Complete the dependency
		depEvent.Complete()

		// Wait for execution
		wg.Wait()
		require.NoError(t, execErr)
	})

	t.Run("context cancellation", func(t *testing.T) {
		source := newMockPipelineSource(10000, 3)
		pipe := NewPipeline(1, "cancellable")
		pipe.SetSource(source)
		pipe.SetSink(NewChunkSink())

		pool := NewThreadPool(4)
		defer pool.Shutdown()

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel quickly
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		err := pipe.Execute(pool, ctx)
		// May or may not error depending on timing
		_ = err
	})
}

func TestExchangeOp(t *testing.T) {
	t.Run("gather exchange", func(t *testing.T) {
		exchange := NewExchangeOp(ExchangeGather, 4, nil)
		assert.Equal(t, "Exchange(GATHER)", exchange.Name())

		chunk := createPipelineTestChunk(10, 2)
		result, err := exchange.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Equal(t, chunk.Count(), result.Count())
	})

	t.Run("repartition exchange", func(t *testing.T) {
		exchange := NewExchangeOp(ExchangeRepartition, 4, []int{0})
		assert.Equal(t, "Exchange(REPARTITION)", exchange.Name())

		chunk := createPipelineTestChunk(100, 2)
		result, err := exchange.Execute(nil, chunk)
		require.NoError(t, err)
		// Result may have fewer rows since some went to other partitions
		assert.LessOrEqual(t, result.Count(), chunk.Count())
	})

	t.Run("broadcast exchange", func(t *testing.T) {
		exchange := NewExchangeOp(ExchangeBroadcast, 4, nil)
		assert.Equal(t, "Exchange(BROADCAST)", exchange.Name())

		chunk := createPipelineTestChunk(10, 2)
		result, err := exchange.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Equal(t, chunk.Count(), result.Count())
	})
}

func TestFilterOp(t *testing.T) {
	t.Run("filter with nil", func(t *testing.T) {
		filter := NewFilterOp(nil)
		chunk := createPipelineTestChunk(10, 2)

		result, err := filter.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Equal(t, chunk.Count(), result.Count())
	})

	t.Run("filter with predicate", func(t *testing.T) {
		filter := NewFilterOp(&SimpleCompareFilter{
			ColumnIdx: 0,
			Op:        ">=",
			Value:     5,
		})
		chunk := createPipelineTestChunk(10, 2)

		result, err := filter.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Less(t, result.Count(), chunk.Count())
	})
}

func TestProjectOp(t *testing.T) {
	t.Run("empty projection", func(t *testing.T) {
		project := NewProjectOp(nil)
		chunk := createPipelineTestChunk(10, 5)

		result, err := project.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Equal(t, 5, result.ColumnCount())
	})

	t.Run("select columns", func(t *testing.T) {
		project := NewProjectOp([]int{0, 2, 4})
		chunk := createPipelineTestChunk(10, 5)

		result, err := project.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Equal(t, 3, result.ColumnCount())
		assert.Equal(t, 10, result.Count())
	})
}

func TestLimitOp(t *testing.T) {
	t.Run("limit only", func(t *testing.T) {
		limit := NewLimitOp(5, 0)
		state := make(map[int]any)

		chunk := createPipelineTestChunk(10, 2)
		result, err := limit.Execute(state, chunk)
		require.NoError(t, err)
		assert.Equal(t, 5, result.Count())
	})

	t.Run("offset only", func(t *testing.T) {
		limit := NewLimitOp(0, 3)
		state := make(map[int]any)

		chunk := createPipelineTestChunk(10, 2)
		result, err := limit.Execute(state, chunk)
		require.NoError(t, err)
		assert.Equal(t, 7, result.Count())
	})

	t.Run("limit and offset", func(t *testing.T) {
		limit := NewLimitOp(5, 2)
		state := make(map[int]any)

		chunk := createPipelineTestChunk(10, 2)
		result, err := limit.Execute(state, chunk)
		require.NoError(t, err)
		assert.Equal(t, 5, result.Count())
	})

	t.Run("across multiple chunks", func(t *testing.T) {
		limit := NewLimitOp(15, 5)
		state := make(map[int]any)

		// First chunk of 10 rows
		chunk1 := createPipelineTestChunk(10, 2)
		result1, err := limit.Execute(state, chunk1)
		require.NoError(t, err)
		// Should skip 5, return 5
		assert.Equal(t, 5, result1.Count())

		// Second chunk of 10 rows
		chunk2 := createPipelineTestChunk(10, 2)
		result2, err := limit.Execute(state, chunk2)
		require.NoError(t, err)
		// Should return up to remaining limit (10)
		assert.Equal(t, 10, result2.Count())

		// Third chunk - should return empty (limit reached)
		chunk3 := createPipelineTestChunk(10, 2)
		result3, err := limit.Execute(state, chunk3)
		require.NoError(t, err)
		assert.Equal(t, 0, result3.Count())
	})
}

func TestChunkSink(t *testing.T) {
	t.Run("empty sink", func(t *testing.T) {
		sink := NewChunkSink()
		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("single chunk", func(t *testing.T) {
		sink := NewChunkSink()
		chunk := createPipelineTestChunk(10, 3)

		err := sink.Combine(chunk)
		require.NoError(t, err)

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 10, result.Count())
		assert.Equal(t, 3, result.ColumnCount())
	})

	t.Run("multiple chunks", func(t *testing.T) {
		sink := NewChunkSink()

		for i := 0; i < 5; i++ {
			chunk := createPipelineTestChunk(10, 2)
			err := sink.Combine(chunk)
			require.NoError(t, err)
		}

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 50, result.Count())
	})

	t.Run("concurrent access", func(t *testing.T) {
		sink := NewChunkSink()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunk := createPipelineTestChunk(10, 2)
				_ = sink.Combine(chunk)
			}()
		}
		wg.Wait()

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 100, result.Count())
	})
}

func TestSortBreakerOp(t *testing.T) {
	t.Run("sort single chunk", func(t *testing.T) {
		keys := []SortKey{NewSortKey(0, "col0")}
		sorter := NewSortBreakerOp(keys)

		assert.True(t, sorter.BreakPipeline())
		assert.Equal(t, "Sort", sorter.Name())

		// Create chunk with descending values
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		chunk := storage.NewDataChunkWithCapacity(types, 10)
		for i := 9; i >= 0; i-- {
			chunk.AppendRow([]any{int32(i), int32(i * 2)})
		}

		// Execute (should just collect)
		result, err := sorter.Execute(nil, chunk)
		require.NoError(t, err)
		assert.Nil(t, result)

		// Finalize should return sorted data
		sorted, err := sorter.Finalize()
		require.NoError(t, err)
		require.NotNil(t, sorted)
		assert.Equal(t, 10, sorted.Count())

		// Verify ascending order (use toSortFloat64 for comparison since types may vary)
		for i := 0; i < sorted.Count()-1; i++ {
			v1, _ := toSortFloat64(sorted.GetValue(i, 0))
			v2, _ := toSortFloat64(sorted.GetValue(i+1, 0))
			assert.LessOrEqual(t, v1, v2)
		}
	})

	t.Run("sort multiple chunks", func(t *testing.T) {
		keys := []SortKey{NewSortKeyWithOrder(0, "col0", Descending, NullsLast)}
		sorter := NewSortBreakerOp(keys)

		// Add multiple chunks
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		for batch := 0; batch < 3; batch++ {
			chunk := storage.NewDataChunkWithCapacity(types, 5)
			for i := 0; i < 5; i++ {
				chunk.AppendRow([]any{int32(batch*5 + i)})
			}
			_, _ = sorter.Execute(nil, chunk)
		}

		// Finalize
		sorted, err := sorter.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 15, sorted.Count())

		// Verify descending order
		for i := 0; i < sorted.Count()-1; i++ {
			v1, _ := toSortFloat64(sorted.GetValue(i, 0))
			v2, _ := toSortFloat64(sorted.GetValue(i+1, 0))
			assert.GreaterOrEqual(t, v1, v2)
		}
	})
}

func TestAggregateBreakerOp(t *testing.T) {
	t.Run("aggregate with group by", func(t *testing.T) {
		aggregates := []AggregateFunc{
			NewAggregateFunc(AggSum, 1, "sum_col1"),
			NewAggregateFunc(AggCount, 1, "count_col1"),
		}
		agg := NewAggregateBreakerOp(
			[]int{0},                           // group by column 0
			[]string{"group_col"},              // group by column names
			[]dukdb.Type{dukdb.TYPE_INTEGER},   // group by types
			aggregates,
		)

		assert.True(t, agg.BreakPipeline())
		assert.Equal(t, "Aggregate", agg.Name())

		// Create test data with 3 groups
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
		chunk := storage.NewDataChunkWithCapacity(types, 9)
		// Group 1: values 10, 20, 30
		chunk.AppendRow([]any{1, 10})
		chunk.AppendRow([]any{1, 20})
		chunk.AppendRow([]any{1, 30})
		// Group 2: values 5, 15
		chunk.AppendRow([]any{2, 5})
		chunk.AppendRow([]any{2, 15})
		// Group 3: value 100
		chunk.AppendRow([]any{3, 100})

		// Execute
		_, _ = agg.Execute(nil, chunk)

		// Finalize
		result, err := agg.Finalize()
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, 3, result.Count()) // 3 groups
		assert.Equal(t, 3, result.ColumnCount()) // group_col, sum, count
	})

	t.Run("aggregate without group by not breaker", func(t *testing.T) {
		aggregates := []AggregateFunc{
			NewAggregateFunc(AggSum, 0, "total"),
		}
		agg := NewAggregateBreakerOp(
			[]int{},         // no group by
			[]string{},
			[]dukdb.Type{},
			aggregates,
		)

		// Without GROUP BY, aggregate is not a pipeline breaker
		assert.False(t, agg.BreakPipeline())
	})
}

func TestHashBuildOp(t *testing.T) {
	t.Run("build hash table", func(t *testing.T) {
		build := NewHashBuildOp([]int{0}, 4)

		assert.True(t, build.BreakPipeline())
		assert.Equal(t, "HashBuild", build.Name())

		// Insert test data
		types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
		chunk := storage.NewDataChunkWithCapacity(types, 10)
		for i := 0; i < 10; i++ {
			chunk.AppendRow([]any{i, "value" + string(rune('0'+i))})
		}

		_, _ = build.Execute(nil, chunk)

		// Verify data is in hash tables
		totalEntries := 0
		for _, ht := range build.HashTables {
			totalEntries += ht.Count()
		}
		assert.Equal(t, 10, totalEntries)

		build.MarkBuilt()
		assert.True(t, build.Built)
	})
}

func TestWindowBreakerOp(t *testing.T) {
	t.Run("window function collection", func(t *testing.T) {
		funcs := []WindowFunc{
			{Type: "ROW_NUMBER", Column: -1, OutputCol: "rn", OutputType: dukdb.TYPE_BIGINT},
		}
		window := NewWindowBreakerOp([]int{0}, []SortKey{NewSortKey(1, "col1")}, funcs)

		assert.True(t, window.BreakPipeline())
		assert.Equal(t, "Window", window.Name())

		chunk := createPipelineTestChunk(10, 3)
		_, _ = window.Execute(nil, chunk)

		result, err := window.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 10, result.Count())
	})
}

func TestPipelineCompiler(t *testing.T) {
	t.Run("create compiler", func(t *testing.T) {
		compiler := NewPipelineCompiler(4)
		assert.Equal(t, 4, compiler.NumWorkers)
	})

	t.Run("compile nil plan", func(t *testing.T) {
		compiler := NewPipelineCompiler(4)
		pipelines, err := compiler.Compile(nil)
		require.NoError(t, err)
		// Should have at least the initial pipeline
		assert.NotNil(t, pipelines)
	})
}

func TestPipelineExecutor(t *testing.T) {
	t.Run("execute empty pipeline list", func(t *testing.T) {
		pool := NewThreadPool(2)
		defer pool.Shutdown()

		executor := NewPipelineExecutor(pool, []*Pipeline{})
		err := executor.Execute(context.Background())
		require.NoError(t, err)
	})

	t.Run("execute single pipeline", func(t *testing.T) {
		pool := NewThreadPool(2)
		defer pool.Shutdown()

		source := newMockPipelineSource(50, 2)
		pipe := NewPipeline(0, "single")
		pipe.SetSource(source)
		pipe.SetSink(NewChunkSink())

		executor := NewPipelineExecutor(pool, []*Pipeline{pipe})
		err := executor.Execute(context.Background())
		require.NoError(t, err)

		result, err := executor.GetFinalResult()
		require.NoError(t, err)
		assert.Equal(t, 50, result.Count())
	})

	t.Run("execute sequential", func(t *testing.T) {
		pool := NewThreadPool(2)
		defer pool.Shutdown()

		source := newMockPipelineSource(20, 2)
		pipe := NewPipeline(0, "seq")
		pipe.SetSource(source)
		pipe.SetSink(NewChunkSink())

		executor := NewPipelineExecutor(pool, []*Pipeline{pipe})
		err := executor.ExecuteSequential(context.Background())
		require.NoError(t, err)

		result, err := executor.GetFinalResult()
		require.NoError(t, err)
		assert.Equal(t, 20, result.Count())
	})

	t.Run("execute with dependencies", func(t *testing.T) {
		pool := NewThreadPool(2)
		defer pool.Shutdown()

		// First pipeline
		source1 := newMockPipelineSource(20, 2)
		pipe1 := NewPipeline(0, "first")
		pipe1.SetSource(source1)
		pipe1.SetSink(NewChunkSink())

		// Second pipeline depends on first
		source2 := newMockPipelineSource(20, 2)
		pipe2 := NewPipeline(1, "second")
		pipe2.SetSource(source2)
		pipe2.SetSink(NewChunkSink())
		pipe2.AddDependency(pipe1.CompletionEvent)

		executor := NewPipelineExecutor(pool, []*Pipeline{pipe1, pipe2})
		err := executor.Execute(context.Background())
		require.NoError(t, err)

		// Both should be complete
		assert.True(t, pipe1.CompletionEvent.IsComplete())
		assert.True(t, pipe2.CompletionEvent.IsComplete())
	})
}

func TestExchangeTypeString(t *testing.T) {
	assert.Equal(t, "GATHER", ExchangeGather.String())
	assert.Equal(t, "SCATTER", ExchangeScatter.String())
	assert.Equal(t, "REPARTITION", ExchangeRepartition.String())
	assert.Equal(t, "BROADCAST", ExchangeBroadcast.String())
	assert.Equal(t, "UNKNOWN", ExchangeType(99).String())
}

// createPipelineTestChunk creates a test DataChunk with integer data.
func createPipelineTestChunk(rows, cols int) *storage.DataChunk {
	types := make([]dukdb.Type, cols)
	for i := 0; i < cols; i++ {
		types[i] = dukdb.TYPE_INTEGER
	}

	chunk := storage.NewDataChunkWithCapacity(types, rows)
	for r := 0; r < rows; r++ {
		rowVals := make([]any, cols)
		for c := 0; c < cols; c++ {
			rowVals[c] = r + c
		}
		chunk.AppendRow(rowVals)
	}
	return chunk
}

// BenchmarkPipelineExecution benchmarks pipeline execution.
func BenchmarkPipelineExecution(b *testing.B) {
	b.Run("parallel_1000_rows", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			source := newMockPipelineSource(1000, 3)
			pipe := NewPipeline(0, "bench")
			pipe.SetSource(source)
			pipe.SetSink(NewChunkSink())

			pool := NewThreadPool(4)
			_ = pipe.Execute(pool, context.Background())
			pool.Shutdown()
		}
	})

	b.Run("parallel_10000_rows", func(b *testing.B) {
		pool := NewThreadPool(4)
		defer pool.Shutdown()

		for i := 0; i < b.N; i++ {
			source := newMockPipelineSource(10000, 3)
			pipe := NewPipeline(0, "bench")
			pipe.SetSource(source)
			pipe.SetSink(NewChunkSink())

			_ = pipe.Execute(pool, context.Background())
		}
	})
}

// TestConcurrentPipelineAccess tests race conditions.
func TestConcurrentPipelineAccess(t *testing.T) {
	t.Run("concurrent event completion", func(t *testing.T) {
		event := NewPipelineEvent("concurrent")

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				event.Complete()
			}()
		}
		wg.Wait()

		assert.True(t, event.IsComplete())
	})

	t.Run("concurrent sink combine", func(t *testing.T) {
		sink := NewChunkSink()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunk := createPipelineTestChunk(10, 2)
				_ = sink.Combine(chunk)
			}()
		}
		wg.Wait()

		result, err := sink.Finalize()
		require.NoError(t, err)
		assert.Equal(t, 1000, result.Count())
	})
}
