package parallel

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryArenaBasic tests basic arena allocation.
func TestMemoryArenaBasic(t *testing.T) {
	arena := NewMemoryArena(1024)
	require.NotNil(t, arena)

	// Allocate some memory
	data, err := arena.Allocate(100)
	require.NoError(t, err)
	assert.Len(t, data, 100)

	// Allocate more
	data2, err := arena.Allocate(200)
	require.NoError(t, err)
	assert.Len(t, data2, 200)

	// Check size tracking
	assert.Greater(t, arena.CurrentSize(), int64(0))
	assert.Equal(t, 1, arena.BlockCount())
}

// TestMemoryArenaReset tests arena reset functionality.
func TestMemoryArenaReset(t *testing.T) {
	arena := NewMemoryArena(1024)

	// Allocate memory
	_, err := arena.Allocate(500)
	require.NoError(t, err)
	_, err = arena.Allocate(500)
	require.NoError(t, err)

	// Should have 1 or 2 blocks
	blocksBefore := arena.BlockCount()
	assert.GreaterOrEqual(t, blocksBefore, 1)

	// Reset
	arena.Reset()

	// Should still have first block
	assert.LessOrEqual(t, arena.BlockCount(), blocksBefore)

	// Should be able to allocate again
	data, err := arena.Allocate(100)
	require.NoError(t, err)
	assert.Len(t, data, 100)
}

// TestMemoryArenaClear tests complete arena clearing.
func TestMemoryArenaClear(t *testing.T) {
	arena := NewMemoryArena(1024)

	// Allocate memory
	_, err := arena.Allocate(500)
	require.NoError(t, err)

	// Clear
	arena.Clear()

	assert.Equal(t, 0, arena.BlockCount())
	assert.Equal(t, int64(0), arena.CurrentSize())
}

// TestMemoryArenaLargeAllocation tests allocation larger than block size.
func TestMemoryArenaLargeAllocation(t *testing.T) {
	arena := NewMemoryArena(1024)

	// Allocate more than block size
	data, err := arena.Allocate(2048)
	require.NoError(t, err)
	assert.Len(t, data, 2048)
}

// TestMemoryLimitBasic tests memory limit functionality.
func TestMemoryLimitBasic(t *testing.T) {
	limit := NewMemoryLimit(1000)

	// Reserve within limit
	assert.True(t, limit.Reserve(500))
	assert.Equal(t, int64(500), limit.CurrentUsage())

	// Reserve more within limit
	assert.True(t, limit.Reserve(400))
	assert.Equal(t, int64(900), limit.CurrentUsage())

	// Try to exceed limit
	assert.False(t, limit.Reserve(200))
	assert.Equal(t, int64(900), limit.CurrentUsage())

	// Release some
	limit.Release(300)
	assert.Equal(t, int64(600), limit.CurrentUsage())

	// Now can reserve more
	assert.True(t, limit.Reserve(200))
}

// TestMemoryArenaWithLimit tests arena with memory limit.
func TestMemoryArenaWithLimit(t *testing.T) {
	limit := NewMemoryLimit(10000)
	arena := NewMemoryArenaWithLimit(1024, limit, 1)

	// Allocate within limit
	data, err := arena.Allocate(500)
	require.NoError(t, err)
	assert.Len(t, data, 500)

	// Memory should be tracked
	assert.Greater(t, limit.CurrentUsage(), int64(0))
}

// TestWorkerCreation tests worker creation.
func TestWorkerCreation(t *testing.T) {
	worker := NewWorker(0, 1024)
	require.NotNil(t, worker)

	assert.Equal(t, 0, worker.ID)
	assert.NotNil(t, worker.Arena)
	assert.NotNil(t, worker.LocalState)
	assert.False(t, worker.IsRunning())
}

// TestWorkerWithLimit tests worker creation with memory limit.
func TestWorkerWithLimit(t *testing.T) {
	limit := NewMemoryLimit(10000)
	worker := NewWorkerWithLimit(1, 1024, limit, 4)
	require.NotNil(t, worker)

	assert.Equal(t, 1, worker.ID)
	assert.NotNil(t, worker.Arena)
}

// TestThreadPoolCreation tests thread pool creation with default workers.
func TestThreadPoolCreation(t *testing.T) {
	pool := NewThreadPool(0)
	require.NotNil(t, pool)

	assert.Equal(t, runtime.GOMAXPROCS(0), pool.NumWorkers)
	assert.Len(t, pool.Workers, pool.NumWorkers)
	assert.False(t, pool.IsShutdown())
}

// TestThreadPoolCreationWithCount tests thread pool with specific worker count.
func TestThreadPoolCreationWithCount(t *testing.T) {
	pool := NewThreadPool(4)
	require.NotNil(t, pool)

	assert.Equal(t, 4, pool.NumWorkers)
	assert.Len(t, pool.Workers, 4)
}

// TestThreadPoolWithLimit tests thread pool with memory limit.
func TestThreadPoolWithLimit(t *testing.T) {
	pool := NewThreadPoolWithLimit(2, 1<<20) // 1MB limit
	require.NotNil(t, pool)

	assert.Equal(t, 2, pool.NumWorkers)
	assert.NotNil(t, pool.memLimit)
}

// TestThreadPoolShutdown tests graceful shutdown.
func TestThreadPoolShutdown(t *testing.T) {
	pool := NewThreadPool(2)
	require.NotNil(t, pool)

	assert.False(t, pool.IsShutdown())

	pool.Shutdown()

	assert.True(t, pool.IsShutdown())
}

// mockSource implements ParallelSource for testing.
type mockSource struct {
	morsels []Morsel
	chunks  map[int]*storage.DataChunk
}

func newMockSource(numMorsels int) *mockSource {
	source := &mockSource{
		morsels: make([]Morsel, numMorsels),
		chunks:  make(map[int]*storage.DataChunk),
	}

	for i := 0; i < numMorsels; i++ {
		source.morsels[i] = Morsel{
			TableID:  1,
			StartRow: uint64(i * 100),
			EndRow:   uint64((i + 1) * 100),
			RowGroup: i,
		}

		// Create a simple chunk with integer data
		chunk := storage.NewDataChunkWithCapacity([]dukdb.Type{dukdb.TYPE_INTEGER}, 100)
		for j := 0; j < 100; j++ {
			chunk.SetValue(j, 0, int32(i*100+j))
		}
		chunk.SetCount(100)
		source.chunks[i] = chunk
	}

	return source
}

func (s *mockSource) GenerateMorsels() []Morsel {
	return s.morsels
}

func (s *mockSource) Scan(morsel Morsel) (*storage.DataChunk, error) {
	if chunk, ok := s.chunks[morsel.RowGroup]; ok {
		return chunk.Clone(), nil
	}
	return storage.NewDataChunk([]dukdb.Type{dukdb.TYPE_INTEGER}), nil
}

// mockSink implements PipelineSink for testing.
type mockSink struct {
	chunks []*storage.DataChunk
	count  atomic.Int64
	mu     sync.Mutex
}

func newMockSink() *mockSink {
	return &mockSink{
		chunks: make([]*storage.DataChunk, 0),
	}
}

func (s *mockSink) Combine(chunk *storage.DataChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, chunk.Clone())
	s.count.Add(1)
	return nil
}

func (s *mockSink) ChunkCount() int {
	return int(s.count.Load())
}

// TestThreadPoolExecute tests basic pipeline execution.
func TestThreadPoolExecute(t *testing.T) {
	pool := NewThreadPool(2)
	defer pool.Shutdown()

	source := newMockSource(4)
	sink := newMockSink()

	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// All morsels should be processed
	assert.Equal(t, 4, sink.ChunkCount())
}

// TestThreadPoolCancellation tests context cancellation.
func TestThreadPoolCancellation(t *testing.T) {
	pool := NewThreadPool(2)
	defer pool.Shutdown()

	source := newMockSource(100) // Many morsels
	sink := newMockSink()

	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 2,
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
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not stop on cancellation")
	}
}

// TestMorselRowCount tests morsel row count calculation.
func TestMorselRowCount(t *testing.T) {
	m := Morsel{
		TableID:  1,
		StartRow: 100,
		EndRow:   200,
		RowGroup: 0,
	}

	assert.Equal(t, uint64(100), m.RowCount())
}

// TestWorkerReset tests worker reset functionality.
func TestWorkerReset(t *testing.T) {
	worker := NewWorker(0, 1024)

	// Add some state
	worker.LocalState[0] = "test"
	_, _ = worker.Arena.Allocate(500)

	worker.Reset()

	assert.Empty(t, worker.LocalState)
}

// TestThreadPoolReset tests pool reset functionality.
func TestThreadPoolReset(t *testing.T) {
	pool := NewThreadPool(2)

	// Execute something first
	source := newMockSource(2)
	sink := newMockSink()

	pipeline := &ParallelPipeline{
		Source:      source,
		Operators:   nil,
		Sink:        sink,
		Parallelism: 2,
	}

	ctx := context.Background()
	err := pool.Execute(ctx, pipeline)
	require.NoError(t, err)

	// Reset for reuse
	pool.Reset()

	// Should be able to execute again
	source2 := newMockSource(2)
	sink2 := newMockSink()

	pipeline2 := &ParallelPipeline{
		Source:      source2,
		Operators:   nil,
		Sink:        sink2,
		Parallelism: 2,
	}

	err = pool.Execute(ctx, pipeline2)
	require.NoError(t, err)

	assert.Equal(t, 2, sink2.ChunkCount())
}

// TestThreadPoolMemoryUsage tests memory usage tracking.
func TestThreadPoolMemoryUsage(t *testing.T) {
	pool := NewThreadPoolWithLimit(2, 1<<20)

	// Initially should have low usage
	initial := pool.MemoryUsage()

	// Allocate some memory in workers
	for _, w := range pool.Workers {
		_, _ = w.Arena.Allocate(1000)
	}

	// Should have higher usage now
	assert.Greater(t, pool.MemoryUsage(), initial)

	pool.Shutdown()
}

// TestGetWorker tests getting worker by index.
func TestGetWorker(t *testing.T) {
	pool := NewThreadPool(4)

	// Valid indices
	for i := 0; i < 4; i++ {
		w := pool.GetWorker(i)
		assert.NotNil(t, w)
		assert.Equal(t, i, w.ID)
	}

	// Invalid indices
	assert.Nil(t, pool.GetWorker(-1))
	assert.Nil(t, pool.GetWorker(100))

	pool.Shutdown()
}

// TestAllocateAligned tests aligned allocation.
func TestAllocateAligned(t *testing.T) {
	arena := NewMemoryArena(4096)

	// Allocate aligned to 16 bytes
	data, err := arena.AllocateAligned(100, 16)
	require.NoError(t, err)
	assert.Len(t, data, 100)

	// Allocate with invalid alignment (should default to 8)
	data2, err := arena.AllocateAligned(50, 0)
	require.NoError(t, err)
	assert.Len(t, data2, 50)
}

// TestMemoryLimitConcurrency tests memory limit under concurrent access.
func TestMemoryLimitConcurrency(t *testing.T) {
	limit := NewMemoryLimit(10000)
	var wg sync.WaitGroup

	// Multiple goroutines trying to reserve and release
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if limit.Reserve(10) {
					limit.Release(10)
				}
			}
		}()
	}

	wg.Wait()

	// Should end up with 0 usage
	assert.Equal(t, int64(0), limit.CurrentUsage())
}

// ============================================================================
// Morsel Generator Tests
// ============================================================================

// TestMorselGeneratorBasic tests basic morsel generation.
func TestMorselGeneratorBasic(t *testing.T) {
	gen := NewMorselGenerator()
	require.NotNil(t, gen)

	// Generate morsels for 10000 rows
	morsels := gen.GenerateMorsels(1, 10000, 0)
	require.NotEmpty(t, morsels)

	// Verify all rows are covered
	var totalRows uint64
	for _, m := range morsels {
		assert.Equal(t, uint64(1), m.TableID)
		assert.LessOrEqual(t, m.StartRow, m.EndRow)
		totalRows += m.RowCount()
	}
	assert.Equal(t, uint64(10000), totalRows)
}

// TestMorselGeneratorEmptyTable tests morsel generation for empty table.
func TestMorselGeneratorEmptyTable(t *testing.T) {
	gen := NewMorselGenerator()

	morsels := gen.GenerateMorsels(1, 0, 0)
	assert.Empty(t, morsels)
}

// TestMorselGeneratorSmallTable tests morsel generation for small table.
func TestMorselGeneratorSmallTable(t *testing.T) {
	gen := NewMorselGenerator()

	// Table smaller than min morsel size
	morsels := gen.GenerateMorsels(1, 100, 0)
	require.Len(t, morsels, 1)

	assert.Equal(t, uint64(0), morsels[0].StartRow)
	assert.Equal(t, uint64(100), morsels[0].EndRow)
}

// TestMorselGeneratorCustomConfig tests morsel generation with custom config.
func TestMorselGeneratorCustomConfig(t *testing.T) {
	config := MorselConfig{
		MinSize:    100,
		MaxSize:    1000,
		TargetSize: 500,
	}
	gen := NewMorselGeneratorWithConfig(config)

	morsels := gen.GenerateMorsels(1, 2500, 0)
	require.NotEmpty(t, morsels)

	// Verify config is applied
	for _, m := range morsels {
		rowCount := m.RowCount()
		// Most morsels should be around target size, except last
		if m.EndRow < 2500 {
			assert.GreaterOrEqual(t, rowCount, uint64(100))
		}
	}
}

// TestMorselGeneratorFromRowGroups tests morsel generation from row groups.
func TestMorselGeneratorFromRowGroups(t *testing.T) {
	gen := NewMorselGenerator()

	rowGroups := []RowGroupInfo{
		{StartRow: 0, RowCount: 5000},
		{StartRow: 5000, RowCount: 3000},
		{StartRow: 8000, RowCount: 2000},
	}

	morsels := gen.GenerateMorselsFromRowGroups(1, rowGroups)
	require.NotEmpty(t, morsels)

	// Verify all rows are covered
	var totalRows uint64
	for _, m := range morsels {
		totalRows += m.RowCount()
	}
	assert.Equal(t, uint64(10000), totalRows)
}

// TestMorselGeneratorLargeRowGroup tests splitting of large row groups.
func TestMorselGeneratorLargeRowGroup(t *testing.T) {
	config := MorselConfig{
		MinSize:    1000,
		MaxSize:    5000,
		TargetSize: 2000,
	}
	gen := NewMorselGeneratorWithConfig(config)

	// Row group larger than max size
	rowGroups := []RowGroupInfo{
		{StartRow: 0, RowCount: 15000},
	}

	morsels := gen.GenerateMorselsFromRowGroups(1, rowGroups)
	require.NotEmpty(t, morsels)

	// Should be split into multiple morsels
	assert.Greater(t, len(morsels), 1)

	// Verify all rows are covered
	var totalRows uint64
	for _, m := range morsels {
		totalRows += m.RowCount()
	}
	assert.Equal(t, uint64(15000), totalRows)
}

// TestDefaultMorselConfig tests default configuration.
func TestDefaultMorselConfig(t *testing.T) {
	config := DefaultMorselConfig()

	assert.Equal(t, MinMorselSize, config.MinSize)
	assert.Equal(t, MaxMorselSize, config.MaxSize)
	assert.Greater(t, config.TargetSize, 0)
}

// TestMorselGeneratorConfigValidation tests config validation.
func TestMorselGeneratorConfigValidation(t *testing.T) {
	// Invalid config with negative values
	config := MorselConfig{
		MinSize:    -100,
		MaxSize:    -200,
		TargetSize: -50,
	}
	gen := NewMorselGeneratorWithConfig(config)

	// Should use defaults
	assert.Greater(t, gen.Config().MinSize, 0)
	assert.Greater(t, gen.Config().MaxSize, 0)
	assert.Greater(t, gen.Config().TargetSize, 0)
}

// ============================================================================
// Work Distributor Tests
// ============================================================================

// TestWorkDistributorCreation tests work distributor creation.
func TestWorkDistributorCreation(t *testing.T) {
	wd := NewWorkDistributor(4, false)
	require.NotNil(t, wd)

	assert.Equal(t, 4, wd.NumWorkers())
	assert.False(t, wd.IsDistributed())
	assert.Nil(t, wd.GlobalQueue()) // No work stealing
}

// TestWorkDistributorWithWorkStealing tests work distributor with work stealing.
func TestWorkDistributorWithWorkStealing(t *testing.T) {
	wd := NewWorkDistributor(4, true)
	require.NotNil(t, wd)

	assert.NotNil(t, wd.GlobalQueue())
}

// TestWorkDistributorDistribute tests morsel distribution.
func TestWorkDistributorDistribute(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 100, RowGroup: 0},
		{TableID: 1, StartRow: 100, EndRow: 200, RowGroup: 1},
		{TableID: 1, StartRow: 200, EndRow: 300, RowGroup: 2},
		{TableID: 1, StartRow: 300, EndRow: 400, RowGroup: 3},
	}

	wd.Distribute(morsels)

	assert.True(t, wd.IsDistributed())
	assert.Equal(t, 4, wd.MorselCount())
}

// TestWorkDistributorGetWork tests getting work from distributor.
func TestWorkDistributorGetWork(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 100, RowGroup: 0},
		{TableID: 1, StartRow: 100, EndRow: 200, RowGroup: 1},
	}

	wd.Distribute(morsels)

	// Get all work
	var received []Morsel
	for {
		m, ok := wd.GetWork(0)
		if !ok {
			break
		}
		received = append(received, m)
	}

	for {
		m, ok := wd.GetWork(1)
		if !ok {
			break
		}
		received = append(received, m)
	}

	// Should receive all morsels
	assert.Len(t, received, 2)
}

// TestWorkDistributorWorkStealing tests work stealing behavior.
func TestWorkDistributorWorkStealing(t *testing.T) {
	wd := NewWorkDistributor(2, true)

	// Create more morsels than queue can hold initially
	morsels := make([]Morsel, 20)
	for i := range morsels {
		morsels[i] = Morsel{
			TableID:  1,
			StartRow: uint64(i * 100),
			EndRow:   uint64((i + 1) * 100),
			RowGroup: i,
		}
	}

	wd.Distribute(morsels)

	// Workers should be able to steal work
	var received int
	for {
		_, ok := wd.GetWork(0)
		if !ok {
			break
		}
		received++
	}

	// Should eventually get work (either local or stolen)
	assert.Greater(t, received, 0)
}

// TestWorkDistributorEmptyMorsels tests distribution of empty morsel list.
func TestWorkDistributorEmptyMorsels(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	wd.Distribute(nil)

	assert.True(t, wd.IsDistributed())
	assert.Equal(t, 0, wd.MorselCount())

	// Should immediately return false
	_, ok := wd.GetWork(0)
	assert.False(t, ok)
}

// TestWorkDistributorInvalidWorkerID tests invalid worker ID handling.
func TestWorkDistributorInvalidWorkerID(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	_, ok := wd.GetWork(-1)
	assert.False(t, ok)

	_, ok = wd.GetWork(100)
	assert.False(t, ok)
}

// TestWorkDistributorWorkerQueue tests getting worker queue directly.
func TestWorkDistributorWorkerQueue(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	queue := wd.WorkerQueue(0)
	assert.NotNil(t, queue)

	queue = wd.WorkerQueue(-1)
	assert.Nil(t, queue)

	queue = wd.WorkerQueue(100)
	assert.Nil(t, queue)
}

// TestWorkDistributorConcurrency tests concurrent access to work distributor.
func TestWorkDistributorConcurrency(t *testing.T) {
	numMorsels := 100
	// Use capacity hint to avoid blocking during distribution
	wd := NewWorkDistributorWithCapacity(4, true, numMorsels)

	morsels := make([]Morsel, numMorsels)
	for i := range morsels {
		morsels[i] = Morsel{
			TableID:  1,
			StartRow: uint64(i * 100),
			EndRow:   uint64((i + 1) * 100),
			RowGroup: i,
		}
	}

	wd.Distribute(morsels)

	var wg sync.WaitGroup
	var count atomic.Int32

	// Multiple workers consuming concurrently
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				_, ok := wd.GetWork(workerID)
				if !ok {
					return
				}
				count.Add(1)
			}
		}(w)
	}

	wg.Wait()

	// All morsels should be consumed exactly once
	assert.Equal(t, int32(numMorsels), count.Load())
}

// TestTryGetWork tests non-blocking work retrieval.
func TestTryGetWork(t *testing.T) {
	wd := NewWorkDistributor(2, false)

	// Before distribution, should return false
	_, ok := wd.TryGetWork(0)
	assert.False(t, ok)

	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 100, RowGroup: 0},
	}

	wd.Distribute(morsels)

	// After distribution with work available
	m, ok := wd.TryGetWork(0)
	if ok {
		assert.Equal(t, uint64(1), m.TableID)
	}
}

// ============================================================================
// Morsel Priority Tests
// ============================================================================

// TestMorselPriority tests morsel priority setting.
func TestMorselPriority(t *testing.T) {
	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 100, Priority: 0},
		{TableID: 1, StartRow: 100, EndRow: 200, Priority: 0},
	}

	SetMorselPriority(morsels, 1, 10)
	assert.Equal(t, 10, morsels[1].Priority)

	// Invalid index should not panic
	SetMorselPriority(morsels, -1, 5)
	SetMorselPriority(morsels, 100, 5)
}

// TestPrioritizeLargeMorsels tests automatic priority assignment.
func TestPrioritizeLargeMorsels(t *testing.T) {
	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 1000, Priority: 0},
		{TableID: 1, StartRow: 1000, EndRow: 5000, Priority: 0},
		{TableID: 1, StartRow: 5000, EndRow: 5500, Priority: 0},
	}

	PrioritizeLargeMorsels(morsels)

	// Larger morsels should have higher priority
	assert.Equal(t, 1, morsels[0].Priority)  // 1000 rows / 1000 = 1
	assert.Equal(t, 4, morsels[1].Priority)  // 4000 rows / 1000 = 4
	assert.Equal(t, 0, morsels[2].Priority)  // 500 rows / 1000 = 0
}

// TestMorselPrioritySorting tests that high priority morsels are processed first.
func TestMorselPrioritySorting(t *testing.T) {
	wd := NewWorkDistributor(1, false)

	morsels := []Morsel{
		{TableID: 1, StartRow: 0, EndRow: 100, Priority: 1},
		{TableID: 1, StartRow: 100, EndRow: 200, Priority: 10},
		{TableID: 1, StartRow: 200, EndRow: 300, Priority: 5},
	}

	wd.Distribute(morsels)

	// First morsel should be highest priority
	m, ok := wd.GetWork(0)
	assert.True(t, ok)
	assert.Equal(t, 10, m.Priority)

	m, ok = wd.GetWork(0)
	assert.True(t, ok)
	assert.Equal(t, 5, m.Priority)

	m, ok = wd.GetWork(0)
	assert.True(t, ok)
	assert.Equal(t, 1, m.Priority)
}
