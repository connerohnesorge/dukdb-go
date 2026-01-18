// Package parallel provides parallel query execution infrastructure.
// This file contains scaling benchmarks that measure performance with
// varying worker counts (1, 2, 4, 8 workers).
package parallel

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ============================================================================
// Benchmark Helpers
// ============================================================================

// benchmarkDataSize is the number of rows to use in benchmarks.
const benchmarkDataSize = 100000

// benchmarkChunkSize is the chunk size for benchmark data.
const benchmarkChunkSize = 2048

// createBenchmarkData creates test data for benchmarks.
func createBenchmarkData(rowCount, chunkSize int) []*storage.DataChunk {
	if chunkSize <= 0 {
		chunkSize = benchmarkChunkSize
	}

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT, dukdb.TYPE_DOUBLE}
	var chunks []*storage.DataChunk

	for i := 0; i < rowCount; i += chunkSize {
		end := i + chunkSize
		if end > rowCount {
			end = rowCount
		}

		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			row := []any{
				int32(j),
				int64(j * 10),
				float64(j) * 1.5,
			}
			chunk.AppendRow(row)
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

// benchmarkSource is a ParallelSource for benchmarks.
type benchmarkSource struct {
	chunks    []*storage.DataChunk
	rowGroups []RowGroupMeta
}

func newBenchmarkSource(chunks []*storage.DataChunk) *benchmarkSource {
	source := &benchmarkSource{chunks: chunks}

	var startRow uint64
	for i, chunk := range chunks {
		source.rowGroups = append(source.rowGroups, RowGroupMeta{
			ID:       i,
			StartRow: startRow,
			RowCount: uint64(chunk.Count()),
		})
		startRow += uint64(chunk.Count())
	}

	return source
}

func (s *benchmarkSource) GenerateMorsels() []Morsel {
	morsels := make([]Morsel, 0, len(s.rowGroups))
	for i, rg := range s.rowGroups {
		morsels = append(morsels, Morsel{
			TableID:  1,
			StartRow: rg.StartRow,
			EndRow:   rg.StartRow + rg.RowCount,
			RowGroup: i,
		})
	}
	return morsels
}

func (s *benchmarkSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if morsel.RowGroup < 0 || morsel.RowGroup >= len(s.chunks) {
		return nil, nil
	}
	return s.chunks[morsel.RowGroup].Clone(), nil
}

// discardSink is a sink that discards all data (for pure scan benchmarks).
type discardSink struct {
	count int64
	mu    sync.Mutex
}

func (s *discardSink) Combine(chunk *storage.DataChunk) error {
	s.mu.Lock()
	s.count += int64(chunk.Count())
	s.mu.Unlock()
	return nil
}

// ============================================================================
// Parallel Scan Benchmarks
// ============================================================================

func benchmarkParallelScan(b *testing.B, numWorkers int) {
	chunks := createBenchmarkData(benchmarkDataSize, benchmarkChunkSize)
	source := newBenchmarkSource(chunks)

	pool := NewThreadPool(numWorkers)
	defer pool.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sink := &discardSink{}
		pipeline := &ParallelPipeline{
			Source:      source,
			Operators:   nil,
			Sink:        sink,
			Parallelism: numWorkers,
		}

		ctx := context.Background()
		if err := pool.Execute(ctx, pipeline); err != nil {
			b.Fatal(err)
		}
		pool.Reset()
	}
}

func BenchmarkParallelScan1Worker(b *testing.B) {
	benchmarkParallelScan(b, 1)
}

func BenchmarkParallelScan2Workers(b *testing.B) {
	benchmarkParallelScan(b, 2)
}

func BenchmarkParallelScan4Workers(b *testing.B) {
	benchmarkParallelScan(b, 4)
}

func BenchmarkParallelScan8Workers(b *testing.B) {
	benchmarkParallelScan(b, 8)
}

// ============================================================================
// Parallel Hash Join Benchmarks
// ============================================================================

func createJoinBenchmarkData(buildRows, probeRows int) (*benchmarkSource, *benchmarkSource) {
	// Build side: id, value
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}
	buildChunks := make([]*storage.DataChunk, 0)
	for i := 0; i < buildRows; i += benchmarkChunkSize {
		end := i + benchmarkChunkSize
		if end > buildRows {
			end = buildRows
		}
		chunk := storage.NewDataChunkWithCapacity(buildTypes, end-i)
		for j := i; j < end; j++ {
			chunk.AppendRow([]any{int32(j), int64(j * 100)})
		}
		buildChunks = append(buildChunks, chunk)
	}

	// Probe side: id, name
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeChunks := make([]*storage.DataChunk, 0)
	for i := 0; i < probeRows; i += benchmarkChunkSize {
		end := i + benchmarkChunkSize
		if end > probeRows {
			end = probeRows
		}
		chunk := storage.NewDataChunkWithCapacity(probeTypes, end-i)
		for j := i; j < end; j++ {
			// Match 50% of probe rows
			key := int32(j * 2 % buildRows)
			chunk.AppendRow([]any{key, "name"})
		}
		probeChunks = append(probeChunks, chunk)
	}

	return newBenchmarkSource(buildChunks), newBenchmarkSource(probeChunks)
}

func benchmarkParallelHashJoin(b *testing.B, numWorkers int) {
	buildSource, probeSource := createJoinBenchmarkData(50000, 50000)

	pool := NewThreadPool(numWorkers)
	defer pool.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		join := NewParallelHashJoin(
			buildSource, probeSource,
			[]int{0}, []int{0},
			InnerJoin,
			SelectPartitionCount(50000, numWorkers),
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
		if err != nil {
			b.Fatal(err)
		}

		// Drain results
		count := 0
		for chunk := range resultChan {
			count += chunk.Count()
		}

		join.Cleanup()
		pool.Reset()
	}
}

func BenchmarkParallelHashJoin1Worker(b *testing.B) {
	benchmarkParallelHashJoin(b, 1)
}

func BenchmarkParallelHashJoin2Workers(b *testing.B) {
	benchmarkParallelHashJoin(b, 2)
}

func BenchmarkParallelHashJoin4Workers(b *testing.B) {
	benchmarkParallelHashJoin(b, 4)
}

func BenchmarkParallelHashJoin8Workers(b *testing.B) {
	benchmarkParallelHashJoin(b, 8)
}

// ============================================================================
// Parallel Aggregate Benchmarks
// ============================================================================

func createAggregateBenchmarkData(rowCount, numGroups int) *benchmarkSource {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}
	var chunks []*storage.DataChunk

	for i := 0; i < rowCount; i += benchmarkChunkSize {
		end := i + benchmarkChunkSize
		if end > rowCount {
			end = rowCount
		}
		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			groupID := int32(j % numGroups)
			value := int64(j * 10)
			chunk.AppendRow([]any{groupID, value})
		}
		chunks = append(chunks, chunk)
	}

	return newBenchmarkSource(chunks)
}

func benchmarkParallelAggregate(b *testing.B, numWorkers int) {
	source := createAggregateBenchmarkData(benchmarkDataSize, 1000)

	pool := NewThreadPool(numWorkers)
	defer pool.Shutdown()

	aggregates := []AggregateFunc{
		NewAggregateFunc(AggSum, 1, "sum_value"),
		NewAggregateFunc(AggCount, 1, "count_value"),
		NewAggregateFunc(AggAvg, 1, "avg_value"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agg := NewParallelAggregate(
			source,
			[]int{0},
			[]string{"group_id"},
			[]dukdb.Type{dukdb.TYPE_INTEGER},
			aggregates,
			numWorkers,
		)

		ctx := context.Background()
		result, err := agg.Execute(pool, ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = result

		pool.Reset()
	}
}

func BenchmarkParallelAggregate1Worker(b *testing.B) {
	benchmarkParallelAggregate(b, 1)
}

func BenchmarkParallelAggregate2Workers(b *testing.B) {
	benchmarkParallelAggregate(b, 2)
}

func BenchmarkParallelAggregate4Workers(b *testing.B) {
	benchmarkParallelAggregate(b, 4)
}

func BenchmarkParallelAggregate8Workers(b *testing.B) {
	benchmarkParallelAggregate(b, 8)
}

// ============================================================================
// Parallel Sort Benchmarks
// ============================================================================

func createSortBenchmarkData(rowCount int) *benchmarkSource {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}
	var chunks []*storage.DataChunk

	// Create unsorted data
	for i := 0; i < rowCount; i += benchmarkChunkSize {
		end := i + benchmarkChunkSize
		if end > rowCount {
			end = rowCount
		}
		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			// Pseudo-random distribution
			value := int32((j*7 + 13) % rowCount)
			chunk.AppendRow([]any{value, int64(j)})
		}
		chunks = append(chunks, chunk)
	}

	return newBenchmarkSource(chunks)
}

func benchmarkParallelSort(b *testing.B, numWorkers int) {
	source := createSortBenchmarkData(benchmarkDataSize)

	pool := NewThreadPool(numWorkers)
	defer pool.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sortOp := NewParallelSort(source, []SortKey{NewSortKey(0, "value")})
		sortOp.SetSchema(
			[]string{"value", "id"},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
		)

		ctx := context.Background()
		result, err := sortOp.Execute(pool, ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = result

		pool.Reset()
	}
}

func BenchmarkParallelSort1Worker(b *testing.B) {
	benchmarkParallelSort(b, 1)
}

func BenchmarkParallelSort2Workers(b *testing.B) {
	benchmarkParallelSort(b, 2)
}

func BenchmarkParallelSort4Workers(b *testing.B) {
	benchmarkParallelSort(b, 4)
}

func BenchmarkParallelSort8Workers(b *testing.B) {
	benchmarkParallelSort(b, 8)
}

// ============================================================================
// Scaling Benchmark Summary
// ============================================================================

// BenchmarkScaling runs a comprehensive scaling test and reports speedup.
// This is not a standard Go benchmark but shows scaling characteristics.
func BenchmarkScaling(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping scaling benchmark in short mode")
	}

	// Test configurations
	type testConfig struct {
		name string
		run  func(workers int) time.Duration
	}

	// Create test data once
	chunks := createBenchmarkData(benchmarkDataSize, benchmarkChunkSize)
	source := newBenchmarkSource(chunks)

	buildSource, probeSource := createJoinBenchmarkData(50000, 50000)
	aggSource := createAggregateBenchmarkData(benchmarkDataSize, 1000)
	sortSource := createSortBenchmarkData(benchmarkDataSize)

	tests := []testConfig{
		{
			name: "Scan",
			run: func(workers int) time.Duration {
				pool := NewThreadPool(workers)
				defer pool.Shutdown()

				start := time.Now()
				sink := &discardSink{}
				pipeline := &ParallelPipeline{
					Source:      source,
					Sink:        sink,
					Parallelism: workers,
				}
				ctx := context.Background()
				_ = pool.Execute(ctx, pipeline)
				return time.Since(start)
			},
		},
		{
			name: "HashJoin",
			run: func(workers int) time.Duration {
				pool := NewThreadPool(workers)
				defer pool.Shutdown()

				start := time.Now()
				join := NewParallelHashJoin(
					buildSource, probeSource,
					[]int{0}, []int{0},
					InnerJoin,
					SelectPartitionCount(50000, workers),
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
				resultChan, _ := join.Execute(pool, ctx)
				for range resultChan {
				}
				return time.Since(start)
			},
		},
		{
			name: "Aggregate",
			run: func(workers int) time.Duration {
				pool := NewThreadPool(workers)
				defer pool.Shutdown()

				aggregates := []AggregateFunc{
					NewAggregateFunc(AggSum, 1, "sum_value"),
				}

				start := time.Now()
				agg := NewParallelAggregate(
					aggSource,
					[]int{0},
					[]string{"group_id"},
					[]dukdb.Type{dukdb.TYPE_INTEGER},
					aggregates,
					workers,
				)
				ctx := context.Background()
				_, _ = agg.Execute(pool, ctx)
				return time.Since(start)
			},
		},
		{
			name: "Sort",
			run: func(workers int) time.Duration {
				pool := NewThreadPool(workers)
				defer pool.Shutdown()

				start := time.Now()
				sortOp := NewParallelSort(sortSource, []SortKey{NewSortKey(0, "value")})
				sortOp.SetSchema(
					[]string{"value", "id"},
					[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
				)
				ctx := context.Background()
				_, _ = sortOp.Execute(pool, ctx)
				return time.Since(start)
			},
		},
	}

	workerCounts := []int{1, 2, 4, 8}
	maxWorkers := runtime.GOMAXPROCS(0)
	if maxWorkers < 8 {
		workerCounts = workerCounts[:len(workerCounts)-1]
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			// Run baseline (1 worker)
			baseline := test.run(1)

			for _, workers := range workerCounts {
				b.Run(fmt.Sprintf("%d_workers", workers), func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_ = test.run(workers)
					}
				})

				// Report speedup
				duration := test.run(workers)
				speedup := float64(baseline) / float64(duration)
				b.Logf("%s: %d workers -> %.2fx speedup (baseline=%v, actual=%v)",
					test.name, workers, speedup, baseline, duration)
			}
		})
	}
}

// ============================================================================
// Memory Arena Benchmarks
// ============================================================================

func BenchmarkMemoryArenaAllocate(b *testing.B) {
	arena := NewMemoryArena(DefaultBlockSize)
	defer arena.Clear()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = arena.Allocate(1024)
		if i%100 == 99 {
			arena.Reset()
		}
	}
}

func BenchmarkMemoryArenaAllocateParallel(b *testing.B) {
	// Each goroutine gets its own arena (simulating per-worker arenas)
	numWorkers := runtime.GOMAXPROCS(0)
	arenas := make([]*MemoryArena, numWorkers)
	for i := 0; i < numWorkers; i++ {
		arenas[i] = NewMemoryArena(DefaultBlockSize)
	}
	defer func() {
		for _, arena := range arenas {
			arena.Clear()
		}
	}()

	var workerCounter int64
	var mu sync.Mutex

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		workerID := int(workerCounter % int64(numWorkers))
		workerCounter++
		mu.Unlock()

		arena := arenas[workerID]
		count := 0

		for pb.Next() {
			_, _ = arena.Allocate(1024)
			count++
			if count%100 == 0 {
				arena.Reset()
			}
		}
	})
}

// ============================================================================
// Morsel Distribution Benchmarks
// ============================================================================

func BenchmarkMorselDistribution(b *testing.B) {
	numMorsels := 1000

	b.Run("without_work_stealing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			morsels := make([]Morsel, numMorsels)
			for j := range morsels {
				morsels[j] = Morsel{
					TableID:  1,
					StartRow: uint64(j * 100),
					EndRow:   uint64((j + 1) * 100),
				}
			}

			wd := NewWorkDistributorWithCapacity(4, false, numMorsels)
			wd.Distribute(morsels)

			// Drain all work
			for w := 0; w < 4; w++ {
				for {
					_, ok := wd.GetWork(w)
					if !ok {
						break
					}
				}
			}
		}
	})

	b.Run("with_work_stealing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			morsels := make([]Morsel, numMorsels)
			for j := range morsels {
				morsels[j] = Morsel{
					TableID:  1,
					StartRow: uint64(j * 100),
					EndRow:   uint64((j + 1) * 100),
				}
			}

			wd := NewWorkDistributorWithCapacity(4, true, numMorsels)
			wd.Distribute(morsels)

			// Drain all work
			for w := 0; w < 4; w++ {
				for {
					_, ok := wd.GetWork(w)
					if !ok {
						break
					}
				}
			}
		}
	})
}

// ============================================================================
// Thread Pool Benchmarks
// ============================================================================

func BenchmarkThreadPoolExecute(b *testing.B) {
	chunks := createBenchmarkData(10000, 1000)
	source := newBenchmarkSource(chunks)

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sink := &discardSink{}
		pipeline := &ParallelPipeline{
			Source:      source,
			Sink:        sink,
			Parallelism: 4,
		}

		ctx := context.Background()
		_ = pool.Execute(ctx, pipeline)
		pool.Reset()
	}
}

func BenchmarkThreadPoolWithLimit(b *testing.B) {
	chunks := createBenchmarkData(10000, 1000)
	source := newBenchmarkSource(chunks)

	pool := NewThreadPoolWithLimit(4, 1<<30) // 1GB limit
	defer pool.Shutdown()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sink := &discardSink{}
		pipeline := &ParallelPipeline{
			Source:      source,
			Sink:        sink,
			Parallelism: 4,
		}

		ctx := context.Background()
		_ = pool.Execute(ctx, pipeline)
		pool.Reset()
	}
}
