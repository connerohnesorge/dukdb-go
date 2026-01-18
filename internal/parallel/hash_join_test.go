package parallel

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockJoinSource is a mock ParallelSource for testing joins.
type mockJoinSource struct {
	chunks  []*storage.DataChunk
	columns []string
	types   []dukdb.Type
}

func newMockJoinSource(columns []string, types []dukdb.Type) *mockJoinSource {
	return &mockJoinSource{
		chunks:  make([]*storage.DataChunk, 0),
		columns: columns,
		types:   types,
	}
}

func (m *mockJoinSource) AddChunk(chunk *storage.DataChunk) {
	m.chunks = append(m.chunks, chunk)
}

func (m *mockJoinSource) GenerateMorsels() []Morsel {
	morsels := make([]Morsel, len(m.chunks))
	for i := range m.chunks {
		morsels[i] = Morsel{
			TableID:  1,
			RowGroup: i,
			StartRow: 0,
			EndRow:   uint64(m.chunks[i].Count()),
		}
	}
	return morsels
}

func (m *mockJoinSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if morsel.RowGroup < 0 || morsel.RowGroup >= len(m.chunks) {
		return nil, nil
	}
	return m.chunks[morsel.RowGroup].Clone(), nil
}

func createTestChunk(types []dukdb.Type, rows [][]any) *storage.DataChunk {
	chunk := storage.NewDataChunkWithCapacity(types, len(rows))
	for _, row := range rows {
		chunk.AppendRow(row)
	}
	return chunk
}

func TestHashTable_InsertAndProbe(t *testing.T) {
	ht := NewHashTable()

	// Insert some entries
	ht.Insert(100, []any{1, "Alice"})
	ht.Insert(100, []any{1, "Alice2"}) // Same hash, different row
	ht.Insert(200, []any{2, "Bob"})

	assert.Equal(t, 3, ht.Count())

	// Probe with no key matcher
	matches := ht.Probe(100, nil)
	assert.Equal(t, 2, len(matches))

	// Probe with key matcher
	matches = ht.Probe(100, func(row []any) bool {
		return row[1] == "Alice"
	})
	assert.Equal(t, 1, len(matches))
	assert.Equal(t, "Alice", matches[0].Row[1])

	// Probe non-existent hash
	matches = ht.Probe(999, nil)
	assert.Equal(t, 0, len(matches))
}

func TestHashTable_ConcurrentAccess(t *testing.T) {
	ht := NewHashTable()

	// Concurrent inserts
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ht.Insert(uint64(id%10), []any{id, "test"})
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 100, ht.Count())

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = ht.Probe(uint64(id%10), nil)
		}(i)
	}
	wg.Wait()
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{100, 128},
		{1000, 1024},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := nextPowerOf2(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSelectPartitionCount(t *testing.T) {
	tests := []struct {
		buildRows  int
		numWorkers int
		expected   int
	}{
		{0, 4, 4},              // Min partitions = workers
		{1000, 4, 4},           // Small dataset, use worker count
		{100000, 4, 4},         // Medium dataset
		{1000000, 4, 16},       // Large dataset, more partitions
		{10000000, 8, 256},     // Very large dataset
		{100000000, 16, 1024},  // Huge dataset, capped at 1024
		{1000000000, 16, 1024}, // Beyond cap
	}

	for _, tt := range tests {
		result := SelectPartitionCount(tt.buildRows, tt.numWorkers)
		// Verify it's a power of 2
		assert.True(t, result > 0 && (result&(result-1)) == 0, "result should be power of 2")
		assert.Equal(t, tt.expected, result)
	}
}

func TestParallelHashJoin_InnerJoin(t *testing.T) {
	// Create build source (employees)
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"dept_id", "name"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, "Alice"},
		{2, "Bob"},
		{1, "Charlie"},
		{3, "David"},
	}))

	// Create probe source (departments)
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeSource := newMockJoinSource([]string{"id", "dept_name"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, "Engineering"},
		{2, "Marketing"},
		{4, "Sales"}, // No match
	}))

	// Create join
	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0}, // Join on first column
		InnerJoin,
		4, // 4 partitions
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	// Execute with thread pool
	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	// Collect results
	var results [][]any
	for chunk := range resultChan {
		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have 3 matches:
	// Engineering + Alice, Engineering + Charlie, Marketing + Bob
	assert.Equal(t, 3, len(results))

	// Cleanup
	join.Cleanup()
}

func TestParallelHashJoin_LeftJoin(t *testing.T) {
	// Create build source (departments)
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "dept_name"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, "Engineering"},
		{2, "Marketing"},
	}))

	// Create probe source (employees)
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeSource := newMockJoinSource([]string{"dept_id", "name"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, "Alice"},
		{3, "Bob"}, // No matching department
	}))

	// Create left join
	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		LeftJoin,
		4,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	var results [][]any
	for chunk := range resultChan {
		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have 2 rows:
	// Alice + Engineering (match)
	// Bob + NULL (no match, left join includes unmatched probe rows)
	assert.Equal(t, 2, len(results))

	// Find the unmatched row (Bob)
	foundUnmatched := false
	for _, row := range results {
		if row[1] == "Bob" {
			foundUnmatched = true
			// Build columns should be nil
			assert.Nil(t, row[2], "dept_id should be NULL for unmatched row")
			assert.Nil(t, row[3], "dept_name should be NULL for unmatched row")
		}
	}
	assert.True(t, foundUnmatched, "should include unmatched probe row")

	join.Cleanup()
}

func TestParallelHashJoin_RightJoin(t *testing.T) {
	// Create build source (departments) - this becomes the "right" side
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "dept_name"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, "Engineering"},
		{2, "Marketing"},
		{3, "HR"}, // No matching employee
	}))

	// Create probe source (employees) - this is the "left" side
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeSource := newMockJoinSource([]string{"dept_id", "name"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, "Alice"},
		{2, "Bob"},
	}))

	// Create right join
	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		RightJoin,
		4,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	var results [][]any
	for chunk := range resultChan {
		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have 3 rows:
	// Alice + Engineering (match)
	// Bob + Marketing (match)
	// NULL + HR (unmatched build row)
	assert.Equal(t, 3, len(results))

	join.Cleanup()
}

func TestParallelHashJoin_EmptyBuild(t *testing.T) {
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	buildSource := newMockJoinSource([]string{"id"}, buildTypes)
	// No data added

	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeSource := newMockJoinSource([]string{"id", "name"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, "Alice"},
	}))

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		4,
	)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Inner join with empty build should produce no results
	assert.Equal(t, 0, resultCount)

	join.Cleanup()
}

func TestParallelHashJoin_EmptyProbe(t *testing.T) {
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "name"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, "Alice"},
	}))

	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"id"}, probeTypes)
	// No data added

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		4,
	)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Inner join with empty probe should produce no results
	assert.Equal(t, 0, resultCount)

	join.Cleanup()
}

func TestParallelHashJoin_MultipleKeyColumns(t *testing.T) {
	// Create build source with composite key
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"key1", "key2", "value"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, 1, "A"},
		{1, 2, "B"},
		{2, 1, "C"},
	}))

	// Create probe source
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"k1", "k2"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, 1}, // Match A
		{1, 2}, // Match B
		{1, 3}, // No match
		{2, 1}, // Match C
	}))

	// Create join with composite key
	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0, 1}, []int{0, 1}, // Join on first two columns
		InnerJoin,
		4,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	var results [][]any
	for chunk := range resultChan {
		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have 3 matches
	assert.Equal(t, 3, len(results))

	join.Cleanup()
}

func TestParallelHashJoin_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	// Create build source with many rows
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "name"}, buildTypes)

	buildRows := make([][]any, 10000)
	for i := 0; i < 10000; i++ {
		buildRows[i] = []any{int32(i % 1000), "name"}
	}
	buildSource.AddChunk(createTestChunk(buildTypes, buildRows))

	// Create probe source
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"id"}, probeTypes)

	probeRows := make([][]any, 1000)
	for i := 0; i < 1000; i++ {
		probeRows[i] = []any{int32(i)}
	}
	probeSource.AddChunk(createTestChunk(probeTypes, probeRows))

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		16,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(4)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Each probe row should match 10 build rows (10000 / 1000)
	assert.Equal(t, 10000, resultCount)

	join.Cleanup()
}

func TestParallelHashJoin_Cancellation(t *testing.T) {
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	buildSource := newMockJoinSource([]string{"id"}, buildTypes)

	// Add many chunks to give time for cancellation
	for i := 0; i < 100; i++ {
		buildSource.AddChunk(createTestChunk(buildTypes, [][]any{{int32(i)}}))
	}

	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"id"}, probeTypes)
	for i := 0; i < 100; i++ {
		probeSource.AddChunk(createTestChunk(probeTypes, [][]any{{int32(i)}}))
	}

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		4,
	)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	// Cancel immediately
	cancel()

	// Drain results (should stop early due to cancellation)
	for range resultChan {
		// Drain
	}

	join.Cleanup()
}

func TestParallelHashJoin_ValidationErrors(t *testing.T) {
	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()

	// Test no build source
	join1 := &ParallelHashJoin{
		ProbeSource:     newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
		BuildKeyIndices: []int{0},
		ProbeKeyIndices: []int{0},
	}
	_, err := join1.Execute(pool, ctx)
	assert.ErrorIs(t, err, ErrNoBuildSource)

	// Test no probe source
	join2 := &ParallelHashJoin{
		BuildSource:     newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
		BuildKeyIndices: []int{0},
		ProbeKeyIndices: []int{0},
	}
	_, err = join2.Execute(pool, ctx)
	assert.ErrorIs(t, err, ErrNoProbeSource)

	// Test no join keys
	join3 := &ParallelHashJoin{
		BuildSource: newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
		ProbeSource: newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
	}
	_, err = join3.Execute(pool, ctx)
	assert.ErrorIs(t, err, ErrNoJoinKeys)

	// Test key count mismatch
	join4 := &ParallelHashJoin{
		BuildSource:     newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
		ProbeSource:     newMockJoinSource([]string{"id"}, []dukdb.Type{dukdb.TYPE_INTEGER}),
		BuildKeyIndices: []int{0, 1},
		ProbeKeyIndices: []int{0},
	}
	_, err = join4.Execute(pool, ctx)
	assert.ErrorIs(t, err, ErrJoinKeyMismatch)
}

func TestSpillManager(t *testing.T) {
	// Create temp directory for spill files
	tempDir, err := os.MkdirTemp("", "spill_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tempDir) }()

	sm := NewSpillManager(tempDir, 10)

	// Should not spill small partitions
	assert.False(t, sm.ShouldSpill(0, 5))

	// Should spill large partitions
	assert.True(t, sm.ShouldSpill(0, 15))

	// Test spill and read
	entries := []PartitionEntry{
		{Hash: 100, Row: []any{int64(1), "Alice", true}},
		{Hash: 200, Row: []any{int64(2), "Bob", false}},
		{Hash: 100, Row: []any{int64(3), "Charlie", nil}},
	}

	filename, err := sm.Spill(0, entries)
	require.NoError(t, err)
	assert.NotEmpty(t, filename)
	assert.True(t, sm.IsSpilled(0))

	// Read back
	readEntries, err := sm.ReadSpilled(0)
	require.NoError(t, err)
	require.Equal(t, len(entries), len(readEntries))

	for i, entry := range readEntries {
		assert.Equal(t, entries[i].Hash, entry.Hash)
		assert.Equal(t, len(entries[i].Row), len(entry.Row))
	}

	// Cleanup
	err = sm.Cleanup()
	require.NoError(t, err)
	assert.False(t, sm.IsSpilled(0))
}

func TestParallelHashJoin_WithSpilling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping spill test in short mode")
	}

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "join_spill_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create build source with many rows to trigger spilling
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "name"}, buildTypes)

	buildRows := make([][]any, 1000)
	for i := 0; i < 1000; i++ {
		buildRows[i] = []any{int32(i), "name"}
	}
	buildSource.AddChunk(createTestChunk(buildTypes, buildRows))

	// Create probe source
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"id"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{int32(0)},
		{int32(500)},
		{int32(999)},
	}))

	// Create join with spill manager (low threshold to force spilling)
	config := HashJoinConfig{
		NumPartitions:  4,
		SpillThreshold: 50, // Very low to force spilling
		SpillDir:       filepath.Join(tempDir, "spill"),
	}

	join := NewParallelHashJoinWithConfig(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		config,
		2,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Should have 3 matches
	assert.Equal(t, 3, resultCount)

	join.Cleanup()
}

func TestJoinType_String(t *testing.T) {
	assert.Equal(t, "INNER", InnerJoin.String())
	assert.Equal(t, "LEFT", LeftJoin.String())
	assert.Equal(t, "RIGHT", RightJoin.String())
	assert.Equal(t, "FULL", FullJoin.String())
	assert.Equal(t, "UNKNOWN", JoinType(99).String())
}

func TestEqualValues(t *testing.T) {
	tests := []struct {
		a, b     any
		expected bool
	}{
		{nil, nil, true},
		{nil, 1, false},
		{1, nil, false},
		{1, 1, true},
		{1, 2, false},
		{int32(1), int64(1), true},
		{1.0, 1, true},
		{"hello", "hello", true},
		{"hello", "world", false},
		{true, true, true},
		{true, false, false},
	}

	for _, tt := range tests {
		result := equalValues(tt.a, tt.b)
		assert.Equal(t, tt.expected, result, "equalValues(%v, %v)", tt.a, tt.b)
	}
}

// TestParallelHashJoin_RaceDetector tests for race conditions.
// Run with: go test -race ./...
func TestParallelHashJoin_RaceDetector(t *testing.T) {
	// Create sources with enough data to trigger concurrent access
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "name"}, buildTypes)

	// Add multiple chunks to ensure concurrent processing
	for i := 0; i < 10; i++ {
		rows := make([][]any, 100)
		for j := 0; j < 100; j++ {
			rows[j] = []any{int32(i*100 + j), "name"}
		}
		buildSource.AddChunk(createTestChunk(buildTypes, rows))
	}

	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	probeSource := newMockJoinSource([]string{"id"}, probeTypes)

	for i := 0; i < 10; i++ {
		rows := make([][]any, 100)
		for j := 0; j < 100; j++ {
			rows[j] = []any{int32(i*100 + j)}
		}
		probeSource.AddChunk(createTestChunk(probeTypes, rows))
	}

	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		InnerJoin,
		8,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	// Use multiple workers to increase chance of detecting races
	pool := NewThreadPool(8)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	resultCount := 0
	for chunk := range resultChan {
		resultCount += chunk.Count()
	}

	// Should have 1000 matches (each probe matches exactly one build)
	assert.Equal(t, 1000, resultCount)

	join.Cleanup()
}

// TestParallelHashJoin_FullJoin tests full outer join.
func TestParallelHashJoin_FullJoin(t *testing.T) {
	// Create build source
	buildTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	buildSource := newMockJoinSource([]string{"id", "b_val"}, buildTypes)
	buildSource.AddChunk(createTestChunk(buildTypes, [][]any{
		{1, "Build1"},
		{2, "Build2"},
		{3, "Build3"}, // No probe match
	}))

	// Create probe source
	probeTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	probeSource := newMockJoinSource([]string{"id", "p_val"}, probeTypes)
	probeSource.AddChunk(createTestChunk(probeTypes, [][]any{
		{1, "Probe1"},
		{2, "Probe2"},
		{4, "Probe4"}, // No build match
	}))

	// Create full outer join
	join := NewParallelHashJoin(
		buildSource, probeSource,
		[]int{0}, []int{0},
		FullJoin,
		4,
	)
	join.SetBuildSchema(buildSource.columns, buildSource.types)
	join.SetProbeSchema(probeSource.columns, probeSource.types)

	pool := NewThreadPool(2)
	defer pool.Shutdown()

	ctx := context.Background()
	resultChan, err := join.Execute(pool, ctx)
	require.NoError(t, err)

	var results [][]any
	for chunk := range resultChan {
		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have 4 rows:
	// Probe1 + Build1 (match)
	// Probe2 + Build2 (match)
	// Probe4 + NULL (unmatched probe, from left side)
	// NULL + Build3 (unmatched build, from right side)
	assert.Equal(t, 4, len(results))

	join.Cleanup()
}
