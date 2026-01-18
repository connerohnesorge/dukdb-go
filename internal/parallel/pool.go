package parallel

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/dukdb/dukdb-go/internal/storage"
)

// Configuration constants for parallel execution.
const (
	// DefaultParallelism uses GOMAXPROCS to determine worker count.
	DefaultParallelism = 0
	// MinMorselSize is the minimum number of rows per morsel.
	MinMorselSize = 1024
	// MaxMorselSize is the maximum number of rows per morsel (one row group).
	MaxMorselSize = 122880
	// DefaultWorkChannelMultiplier determines buffering for prefetch.
	DefaultWorkChannelMultiplier = 2
)

// Morsel represents a unit of parallel work.
// Each morsel defines a range of rows from a table that can be processed
// independently by a worker.
type Morsel struct {
	TableID  uint64
	StartRow uint64
	EndRow   uint64
	RowGroup int
	Priority int // For work stealing prioritization (higher = processed first)
}

// RowCount returns the number of rows in this morsel.
func (m Morsel) RowCount() uint64 {
	return m.EndRow - m.StartRow
}

// ParallelSource is an interface for sources that can generate morsels
// and scan data for parallel execution.
type ParallelSource interface {
	// GenerateMorsels returns all morsels that can be processed in parallel.
	GenerateMorsels() []Morsel
	// Scan reads data for the given morsel and returns a DataChunk.
	Scan(morsel Morsel) (*storage.DataChunk, error)
}

// PipelineOp represents a pipeline operator that transforms data.
type PipelineOp interface {
	// Execute processes a DataChunk using thread-local state.
	Execute(localState map[int]any, chunk *storage.DataChunk) (*storage.DataChunk, error)
}

// PipelineSink is the final destination for pipeline results.
type PipelineSink interface {
	// Combine adds processed data to the sink. Must be thread-safe.
	Combine(chunk *storage.DataChunk) error
}

// ParallelPipeline represents an executable parallel pipeline.
type ParallelPipeline struct {
	Source      ParallelSource
	Operators   []PipelineOp
	Sink        PipelineSink
	Parallelism int
}

// Worker processes morsels from the source.
// Each worker has its own memory arena and thread-local state
// to avoid contention during execution.
type Worker struct {
	ID         int
	Arena      *MemoryArena
	LocalState map[int]any
	running    atomic.Bool
}

// NewWorker creates a new worker with the given ID.
func NewWorker(id int, arenaBlockSize int) *Worker {
	return &Worker{
		ID:         id,
		Arena:      NewMemoryArena(arenaBlockSize),
		LocalState: make(map[int]any),
	}
}

// NewWorkerWithLimit creates a new worker with a shared memory limit.
func NewWorkerWithLimit(id int, arenaBlockSize int, memLimit *MemoryLimit, numWorkers int) *Worker {
	return &Worker{
		ID:         id,
		Arena:      NewMemoryArenaWithLimit(arenaBlockSize, memLimit, numWorkers),
		LocalState: make(map[int]any),
	}
}

// Run processes morsels from the source through the pipeline.
// It runs until the morsel channel is closed or the context is cancelled.
func (w *Worker) Run(
	ctx context.Context,
	morselChan <-chan Morsel,
	pipeline *ParallelPipeline,
	resultChan chan<- *storage.DataChunk,
	errChan chan<- error,
) {
	w.running.Store(true)
	defer w.running.Store(false)

	for {
		select {
		case <-ctx.Done():
			return
		case morsel, ok := <-morselChan:
			if !ok {
				return
			}

			// Scan the morsel to get a data chunk
			chunk, err := pipeline.Source.Scan(morsel)
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
				return
			}

			// Process through pipeline operators
			for _, op := range pipeline.Operators {
				chunk, err = op.Execute(w.LocalState, chunk)
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
				if chunk == nil || chunk.Count() == 0 {
					break
				}
			}

			// Send result to sink or result channel
			if chunk != nil && chunk.Count() > 0 {
				if pipeline.Sink != nil {
					if err := pipeline.Sink.Combine(chunk); err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}
				} else if resultChan != nil {
					select {
					case resultChan <- chunk:
					case <-ctx.Done():
						return
					}
				}
			}

			// Reset arena after processing each morsel
			w.Arena.Reset()
		}
	}
}

// IsRunning returns whether the worker is currently running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// Reset clears the worker's thread-local state and arena.
func (w *Worker) Reset() {
	w.LocalState = make(map[int]any)
	w.Arena.Reset()
}

// ThreadPool manages parallel execution workers.
type ThreadPool struct {
	NumWorkers int
	Workers    []*Worker
	memLimit   *MemoryLimit
	shutdown   atomic.Bool
	mu         sync.Mutex
}

// NewThreadPool creates a new thread pool with the specified number of workers.
// If numWorkers is 0 or negative, it defaults to GOMAXPROCS.
func NewThreadPool(numWorkers int) *ThreadPool {
	if numWorkers <= 0 {
		numWorkers = runtime.GOMAXPROCS(0)
	}

	pool := &ThreadPool{
		NumWorkers: numWorkers,
		Workers:    make([]*Worker, numWorkers),
	}

	// Create workers with per-worker memory arenas
	for i := 0; i < numWorkers; i++ {
		pool.Workers[i] = NewWorker(i, DefaultArenaMaxSize)
	}

	return pool
}

// NewThreadPoolWithLimit creates a thread pool with shared memory limiting.
func NewThreadPoolWithLimit(numWorkers int, maxMemory int64) *ThreadPool {
	if numWorkers <= 0 {
		numWorkers = runtime.GOMAXPROCS(0)
	}

	memLimit := NewMemoryLimit(maxMemory)

	pool := &ThreadPool{
		NumWorkers: numWorkers,
		Workers:    make([]*Worker, numWorkers),
		memLimit:   memLimit,
	}

	// Create workers with shared memory limit
	for i := 0; i < numWorkers; i++ {
		pool.Workers[i] = NewWorkerWithLimit(i, DefaultBlockSize, memLimit, numWorkers)
	}

	return pool
}

// Execute runs a parallel pipeline to completion.
// It distributes morsels to workers, collects results, and handles errors.
func (p *ThreadPool) Execute(ctx context.Context, pipeline *ParallelPipeline) error {
	p.mu.Lock()
	if p.shutdown.Load() {
		p.mu.Unlock()
		return errors.New("thread pool is shut down")
	}
	p.mu.Unlock()

	// Create channels for this execution
	workChan := make(chan Morsel, p.NumWorkers*DefaultWorkChannelMultiplier)
	resultChan := make(chan *storage.DataChunk, p.NumWorkers*DefaultWorkChannelMultiplier)
	errChan := make(chan error, p.NumWorkers)

	// Create a cancellable context for this execution
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// Start workers
	for _, worker := range p.Workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.Run(execCtx, workChan, pipeline, resultChan, errChan)
		}(worker)
	}

	// Distribute morsels in a separate goroutine
	go func() {
		defer close(workChan)
		if pipeline.Source == nil {
			return
		}
		morsels := pipeline.Source.GenerateMorsels()
		for _, morsel := range morsels {
			select {
			case <-execCtx.Done():
				return
			case workChan <- morsel:
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Close result channel after all workers are done
	close(resultChan)

	// Drain result channel to prevent goroutine leaks
	for range resultChan {
		// Discard results since we're using a sink
	}

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// ExecuteWithResults runs a pipeline and returns results through a channel.
// The returned channel will be closed when execution completes.
func (p *ThreadPool) ExecuteWithResults(
	ctx context.Context,
	pipeline *ParallelPipeline,
) (<-chan *storage.DataChunk, <-chan error) {
	resultChan := make(chan *storage.DataChunk, p.NumWorkers*DefaultWorkChannelMultiplier)
	errChanOut := make(chan error, 1)

	go func() {
		defer close(resultChan)
		defer close(errChanOut)

		p.mu.Lock()
		if p.shutdown.Load() {
			p.mu.Unlock()
			errChanOut <- errors.New("thread pool is shut down")
			return
		}
		p.mu.Unlock()

		// Create channels for this execution
		workChan := make(chan Morsel, p.NumWorkers*DefaultWorkChannelMultiplier)
		errChan := make(chan error, p.NumWorkers)

		// Create a cancellable context for this execution
		execCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup

		// Start workers
		for _, worker := range p.Workers {
			wg.Add(1)
			go func(w *Worker) {
				defer wg.Done()
				w.Run(execCtx, workChan, pipeline, resultChan, errChan)
			}(worker)
		}

		// Distribute morsels in a separate goroutine
		go func() {
			defer close(workChan)
			if pipeline.Source == nil {
				return
			}
			morsels := pipeline.Source.GenerateMorsels()
			for _, morsel := range morsels {
				select {
				case <-execCtx.Done():
					return
				case workChan <- morsel:
				}
			}
		}()

		// Wait for completion
		wg.Wait()

		// Check for errors
		select {
		case err := <-errChan:
			errChanOut <- err
		default:
		}
	}()

	return resultChan, errChanOut
}

// Shutdown gracefully shuts down the thread pool.
// It marks the pool as shut down so no new executions can start.
func (p *ThreadPool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown.Load() {
		return
	}
	p.shutdown.Store(true)

	// Clear worker arenas
	for _, w := range p.Workers {
		w.Arena.Clear()
	}
}

// IsShutdown returns whether the pool has been shut down.
func (p *ThreadPool) IsShutdown() bool {
	return p.shutdown.Load()
}

// Reset prepares the pool for reuse after execution.
func (p *ThreadPool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown.Load() {
		return
	}

	// Reset all workers
	for _, w := range p.Workers {
		w.Reset()
	}
}

// MemoryUsage returns the current memory usage across all workers.
func (p *ThreadPool) MemoryUsage() int64 {
	if p.memLimit != nil {
		return p.memLimit.CurrentUsage()
	}

	var total int64
	for _, w := range p.Workers {
		total += w.Arena.CurrentSize()
	}
	return total
}

// WorkerCount returns the number of workers in the pool.
func (p *ThreadPool) WorkerCount() int {
	return p.NumWorkers
}

// GetWorker returns the worker at the given index.
func (p *ThreadPool) GetWorker(idx int) *Worker {
	if idx < 0 || idx >= len(p.Workers) {
		return nil
	}
	return p.Workers[idx]
}
