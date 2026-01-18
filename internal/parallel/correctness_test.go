// Package parallel provides parallel query execution infrastructure.
// This file contains correctness tests that verify parallel execution produces
// the same results as sequential execution.
package parallel

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

// createTestDataChunks creates test data chunks with the specified row count.
func createTestDataChunks(rowCount int, chunkSize int) []*storage.DataChunk {
	if chunkSize <= 0 {
		chunkSize = storage.StandardVectorSize
	}

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}
	var chunks []*storage.DataChunk

	for i := 0; i < rowCount; i += chunkSize {
		end := i + chunkSize
		if end > rowCount {
			end = rowCount
		}

		chunk := storage.NewDataChunkWithCapacity(types, end-i)
		for j := i; j < end; j++ {
			row := []any{
				int32(j),         // id
				int64(j * 10),    // value
				"row_" + itoa(j), // name
			}
			chunk.AppendRow(row)
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

// itoa is a simple integer to string conversion.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	if i < 0 {
		return "-" + itoa(-i)
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

// collectChunkValues extracts all values from a chunk into a slice of rows.
func collectChunkValues(chunk *storage.DataChunk) [][]any {
	if chunk == nil {
		return nil
	}

	var rows [][]any
	for i := 0; i < chunk.Count(); i++ {
		row := make([]any, chunk.ColumnCount())
		for j := 0; j < chunk.ColumnCount(); j++ {
			row[j] = chunk.GetValue(i, j)
		}
		rows = append(rows, row)
	}
	return rows
}

// sortRowsByFirstColumn sorts rows by the first column (integer).
func sortRowsByFirstColumn(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool {
		a, aok := toSortFloat64(rows[i][0])
		b, bok := toSortFloat64(rows[j][0])
		if aok && bok {
			return a < b
		}
		return false
	})
}

// compareRowSets checks if two sets of rows contain the same data (order-independent).
func compareRowSets(t *testing.T, expected, actual [][]any) {
	require.Equal(t, len(expected), len(actual), "row count mismatch")

	// Sort both by first column
	sortRowsByFirstColumn(expected)
	sortRowsByFirstColumn(actual)

	for i := range expected {
		require.Equal(t, len(expected[i]), len(actual[i]), "column count mismatch at row %d", i)
		for j := range expected[i] {
			// Use loose comparison for numeric values
			expFloat, expOk := toSortFloat64(expected[i][j])
			actFloat, actOk := toSortFloat64(actual[i][j])
			if expOk && actOk {
				assert.Equal(t, expFloat, actFloat, "value mismatch at row %d, col %d", i, j)
			} else {
				assert.Equal(t, expected[i][j], actual[i][j], "value mismatch at row %d, col %d", i, j)
			}
		}
	}
}

// testTableSource implements ParallelSource for testing.
type testTableSource struct {
	chunks    []*storage.DataChunk
	rowGroups []RowGroupMeta
	mu        sync.RWMutex
}

func newTestTableSource(chunks []*storage.DataChunk) *testTableSource {
	source := &testTableSource{
		chunks: chunks,
	}

	// Create row group metadata
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

func (s *testTableSource) GenerateMorsels() []Morsel {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

func (s *testTableSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if morsel.RowGroup < 0 || morsel.RowGroup >= len(s.chunks) {
		return nil, nil
	}
	return s.chunks[morsel.RowGroup].Clone(), nil
}

// ============================================================================
// Parallel Scan Correctness Tests
// ============================================================================

// TestParallelScanCorrectness verifies that parallel scan produces the same
// results as sequential scan.
func TestParallelScanCorrectness(t *testing.T) {
	// Create test data
	chunks := createTestDataChunks(10000, 1000)
	source := newTestTableSource(chunks)

	// Sequential scan
	var seqRows [][]any
	for _, morsel := range source.GenerateMorsels() {
		chunk, err := source.Scan(morsel)
		require.NoError(t, err)
		seqRows = append(seqRows, collectChunkValues(chunk)...)
	}

	// Parallel scan with different worker counts
	workerCounts := []int{1, 2, 4, 8}
	for _, numWorkers := range workerCounts {
		t.Run("workers="+itoa(numWorkers), func(t *testing.T) {
			pool := NewThreadPool(numWorkers)
			defer pool.Shutdown()

			sink := newMockSink()
			pipeline := &ParallelPipeline{
				Source:      source,
				Operators:   nil,
				Sink:        sink,
				Parallelism: numWorkers,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := pool.Execute(ctx, pipeline)
			require.NoError(t, err)

			// Collect parallel results
			var parRows [][]any
			for _, chunk := range sink.chunks {
				parRows = append(parRows, collectChunkValues(chunk)...)
			}

			// Compare results
			compareRowSets(t, seqRows, parRows)
		})
	}
}

// ============================================================================
// Parallel Hash Join Correctness Tests
// ============================================================================

// TestParallelHashJoinCorrectness verifies that parallel hash join produces
// correct results.
func TestParallelHashJoinCorrectness(t *testing.T) {
	// Create build side data: 1000 rows with id, value
	buildChunks := make([]*storage.DataChunk, 1)
	buildChunks[0] = storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}, 1000)
	for i := 0; i < 1000; i++ {
		buildChunks[0].AppendRow([]any{int32(i), int64(i * 100)})
	}

	// Create probe side data: 500 rows with id, name (some will match, some won't)
	probeChunks := make([]*storage.DataChunk, 1)
	probeChunks[0] = storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 500)
	for i := 0; i < 500; i++ {
		probeChunks[0].AppendRow([]any{int32(i * 2), "name_" + itoa(i)})
	}

	buildSource := newTestTableSource(buildChunks)
	probeSource := newTestTableSource(probeChunks)

	// Sequential hash join (reference implementation)
	seqResult := sequentialHashJoin(buildChunks[0], probeChunks[0], 0, 0)

	// Parallel hash join with different configurations
	testCases := []struct {
		name       string
		numWorkers int
		partitions int
	}{
		{"1 worker, 4 partitions", 1, 4},
		{"2 workers, 4 partitions", 2, 4},
		{"4 workers, 8 partitions", 4, 8},
		{"4 workers, 16 partitions", 4, 16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool := NewThreadPool(tc.numWorkers)
			defer pool.Shutdown()

			join := NewParallelHashJoin(
				buildSource, probeSource,
				[]int{0}, []int{0}, // Join on first column
				InnerJoin,
				tc.partitions,
			)
			join.SetBuildSchema(
				[]string{"id", "value"},
				[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
			)
			join.SetProbeSchema(
				[]string{"id", "name"},
				[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			resultChan, err := join.Execute(pool, ctx)
			require.NoError(t, err)

			// Collect parallel results
			var parRows [][]any
			for chunk := range resultChan {
				parRows = append(parRows, collectChunkValues(chunk)...)
			}

			// Compare results
			compareRowSets(t, seqResult, parRows)
		})
	}
}

// sequentialHashJoin performs a simple sequential hash join for comparison.
func sequentialHashJoin(build, probe *storage.DataChunk, buildKey, probeKey int) [][]any {
	// Build hash table
	hashTable := make(map[any][]any)
	for i := 0; i < build.Count(); i++ {
		key := build.GetValue(i, buildKey)
		row := make([]any, build.ColumnCount())
		for j := 0; j < build.ColumnCount(); j++ {
			row[j] = build.GetValue(i, j)
		}
		hashTable[key] = row
	}

	// Probe and join
	var results [][]any
	for i := 0; i < probe.Count(); i++ {
		key := probe.GetValue(i, probeKey)
		if buildRow, ok := hashTable[key]; ok {
			probeRow := make([]any, probe.ColumnCount())
			for j := 0; j < probe.ColumnCount(); j++ {
				probeRow[j] = probe.GetValue(i, j)
			}
			// Concatenate probe row + build row (matching join output order)
			result := make([]any, len(probeRow)+len(buildRow))
			copy(result, probeRow)
			copy(result[len(probeRow):], buildRow)
			results = append(results, result)
		}
	}

	return results
}

// ============================================================================
// Parallel Aggregate Correctness Tests
// ============================================================================

// TestParallelAggregateCorrectness verifies that parallel aggregation produces
// correct results.
func TestParallelAggregateCorrectness(t *testing.T) {
	// Create test data with groups
	chunks := make([]*storage.DataChunk, 4)
	for c := 0; c < 4; c++ {
		chunks[c] = storage.NewDataChunkWithCapacity(
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}, 250)
		for i := 0; i < 250; i++ {
			groupID := (c*250 + i) % 10 // 10 groups
			value := int64(c*250 + i)
			chunks[c].AppendRow([]any{int32(groupID), value})
		}
	}

	source := newTestTableSource(chunks)

	// Sequential aggregate (reference implementation)
	seqResult := sequentialAggregate(chunks, 0, AggSum, 1)

	// Parallel aggregate with different worker counts
	workerCounts := []int{1, 2, 4}
	for _, numWorkers := range workerCounts {
		t.Run("workers="+itoa(numWorkers), func(t *testing.T) {
			pool := NewThreadPool(numWorkers)
			defer pool.Shutdown()

			aggregates := []AggregateFunc{
				NewAggregateFunc(AggSum, 1, "sum_value"),
			}

			agg := NewParallelAggregate(
				source,
				[]int{0},                         // Group by first column
				[]string{"group_id"},             // Group column names
				[]dukdb.Type{dukdb.TYPE_INTEGER}, // Group column types
				aggregates,
				numWorkers,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := agg.Execute(pool, ctx)
			require.NoError(t, err)

			parRows := collectChunkValues(result)

			// Compare results
			compareRowSets(t, seqResult, parRows)
		})
	}
}

// sequentialAggregate performs a simple sequential aggregation for comparison.
func sequentialAggregate(
	chunks []*storage.DataChunk,
	groupByCol int,
	aggType AggregateType,
	valueCol int,
) [][]any {
	// Group values
	groups := make(map[any]float64)
	counts := make(map[any]int64)

	for _, chunk := range chunks {
		for i := 0; i < chunk.Count(); i++ {
			key := chunk.GetValue(i, groupByCol)
			value := chunk.GetValue(i, valueCol)
			if f, ok := toAggFloat64(value); ok {
				groups[key] += f
				counts[key]++
			}
		}
	}

	results := make([][]any, 0, len(groups))
	for key, sum := range groups {
		var aggValue any
		switch aggType {
		case AggSum:
			aggValue = sum
		case AggCount:
			aggValue = counts[key]
		case AggAvg:
			aggValue = sum / float64(counts[key])
		case AggCountStar, AggMin, AggMax, AggFirst, AggLast:
			// Not used in this test, but included for completeness
			aggValue = sum
		}
		results = append(results, []any{key, aggValue})
	}

	return results
}

// ============================================================================
// Parallel Sort Correctness Tests
// ============================================================================

// TestParallelSortCorrectness verifies that parallel sort produces correctly
// sorted results.
func TestParallelSortCorrectness(t *testing.T) {
	// Create unsorted test data
	chunks := make([]*storage.DataChunk, 4)
	for c := 0; c < 4; c++ {
		chunks[c] = storage.NewDataChunkWithCapacity(
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 250)
		for i := 0; i < 250; i++ {
			// Create values in random order
			value := int32((c*250 + i*7) % 1000) // Pseudo-random distribution
			chunks[c].AppendRow([]any{value, "name_" + itoa(int(value))})
		}
	}

	source := newTestTableSource(chunks)

	// Sequential sort (reference implementation)
	seqResult := sequentialSort(chunks, []SortKey{NewSortKey(0, "value")})

	// Parallel sort with different worker counts
	workerCounts := []int{1, 2, 4}
	for _, numWorkers := range workerCounts {
		t.Run("workers="+itoa(numWorkers), func(t *testing.T) {
			pool := NewThreadPool(numWorkers)
			defer pool.Shutdown()

			sortOp := NewParallelSort(source, []SortKey{NewSortKey(0, "value")})
			sortOp.SetSchema([]string{"value", "name"},
				[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := sortOp.Execute(pool, ctx)
			require.NoError(t, err)

			parRows := collectChunkValues(result)

			// For sort, order matters - compare element by element
			require.Equal(t, len(seqResult), len(parRows), "row count mismatch")
			for i := range seqResult {
				assert.Equal(t, seqResult[i][0], parRows[i][0],
					"value mismatch at position %d", i)
			}
		})
	}
}

// sequentialSort performs a simple sequential sort for comparison.
func sequentialSort(chunks []*storage.DataChunk, keys []SortKey) [][]any {
	var rows [][]any
	for _, chunk := range chunks {
		rows = append(rows, collectChunkValues(chunk)...)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, key := range keys {
			cmp := CompareValues(rows[i][key.Column], rows[j][key.Column], key.Order, key.Nulls)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	return rows
}

// ============================================================================
// Data Size Variation Tests
// ============================================================================

// TestCorrectnessSmallData tests parallel operations with small data sets.
func TestCorrectnessSmallData(t *testing.T) {
	// Small dataset: 10 rows
	chunks := createTestDataChunks(10, 10)
	source := newTestTableSource(chunks)

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 4,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Verify all rows processed
	totalRows := 0
	for _, chunk := range sink.chunks {
		totalRows += chunk.Count()
	}
	assert.Equal(t, 10, totalRows)
}

// TestCorrectnessLargeData tests parallel operations with larger data sets.
func TestCorrectnessLargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large data test in short mode")
	}

	// Large dataset: 100,000 rows
	chunks := createTestDataChunks(100000, 2048)
	source := newTestTableSource(chunks)

	// Collect expected rows
	var expected [][]any
	for _, chunk := range chunks {
		expected = append(expected, collectChunkValues(chunk)...)
	}

	pool := NewThreadPool(8)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 8,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Collect actual rows
	var actual [][]any
	for _, chunk := range sink.chunks {
		actual = append(actual, collectChunkValues(chunk)...)
	}

	// Compare results
	compareRowSets(t, expected, actual)
}

// ============================================================================
// Edge Case Tests
// ============================================================================

// TestCorrectnessNullHandling tests correct handling of NULL values.
func TestCorrectnessNullHandling(t *testing.T) {
	// Create data with NULL values
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT}
	chunk := storage.NewDataChunkWithCapacity(types, 10)
	for i := 0; i < 10; i++ {
		var value any
		if i%3 == 0 {
			value = nil // NULL every 3rd row
		} else {
			value = int64(i * 100)
		}
		chunk.AppendRow([]any{int32(i), value})
	}

	chunks := []*storage.DataChunk{chunk}
	source := newTestTableSource(chunks)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	// Test with aggregation
	aggregates := []AggregateFunc{
		NewAggregateFunc(AggSum, 1, "sum_value"),
		NewAggregateFunc(AggCount, 1, "count_value"),
	}

	agg := NewParallelAggregate(
		source,
		[]int{},        // No grouping
		[]string{},     // No group columns
		[]dukdb.Type{}, // No group types
		aggregates,
		2,
	)

	ctx := context.Background()
	result, err := agg.Execute(pool, ctx)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Non-null values are at indices 1, 2, 4, 5, 7, 8 -> values 100, 200, 400, 500, 700, 800
	// Sum = 2700, Count = 6
	if result.Count() > 0 {
		sumValue := result.GetValue(0, 0)
		countValue := result.GetValue(0, 1)

		if sumFloat, ok := toAggFloat64(sumValue); ok {
			assert.Equal(t, 2700.0, sumFloat, "sum mismatch with NULL handling")
		}
		if countInt, ok := countValue.(int64); ok {
			assert.Equal(t, int64(6), countInt, "count mismatch with NULL handling")
		}
	}
}

// TestCorrectnessDuplicateKeys tests correct handling of duplicate join keys.
func TestCorrectnessDuplicateKeys(t *testing.T) {
	// Build side with duplicate keys
	buildChunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 10)
	for i := 0; i < 10; i++ {
		key := int32(i % 3) // Only 3 unique keys
		buildChunk.AppendRow([]any{key, "build_" + itoa(i)})
	}

	// Probe side with duplicate keys
	probeChunk := storage.NewDataChunkWithCapacity(
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}, 6)
	for i := 0; i < 6; i++ {
		key := int32(i % 2) // Only 2 unique keys (0, 1)
		probeChunk.AppendRow([]any{key, "probe_" + itoa(i)})
	}

	buildSource := newTestTableSource([]*storage.DataChunk{buildChunk})
	probeSource := newTestTableSource([]*storage.DataChunk{probeChunk})

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		4,
	)
	join.SetBuildSchema(
		[]string{"key", "build_name"},
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
	)
	join.SetProbeSchema(
		[]string{"key", "probe_name"},
		[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
	)

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	// Count results
	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Expected:
	// Key 0: 4 build rows * 3 probe rows = 12 matches (build: 0,3,6,9; probe: 0,2,4)
	// Key 1: 3 build rows * 3 probe rows = 9 matches (build: 1,4,7; probe: 1,3,5)
	// Total = 21
	assert.Equal(t, 21, resultCount, "duplicate key join should produce correct number of results")
}

// TestCorrectnessEmptyInput tests handling of empty inputs.
func TestCorrectnessEmptyInput(t *testing.T) {
	// Empty source
	emptySource := newTestTableSource([]*storage.DataChunk{})

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      emptySource,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 2,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	assert.Equal(t, 0, sink.ChunkCount(), "empty input should produce no output")
}

// TestCorrectnessSingleRow tests handling of single row input.
func TestCorrectnessSingleRow(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunkWithCapacity(types, 1)
	chunk.AppendRow([]any{int32(42)})

	source := newTestTableSource([]*storage.DataChunk{chunk})

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	sink := newMockSink()
	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 4,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Verify the single row was processed
	totalRows := 0
	for _, c := range sink.chunks {
		totalRows += c.Count()
	}
	assert.Equal(t, 1, totalRows, "single row should be processed correctly")

	if len(sink.chunks) > 0 && sink.chunks[0].Count() > 0 {
		value := sink.chunks[0].GetValue(0, 0)
		assert.Equal(t, int32(42), value, "single row value should be preserved")
	}
}
