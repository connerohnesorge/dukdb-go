// Package parallel provides morsel-driven parallel execution infrastructure.
// This file implements morsel generation and work distribution with work stealing.
package parallel

import (
	"sync"
	"sync/atomic"
)

// DefaultTargetMorselSize is the default target size for morsels (8K rows).
// This provides good cache efficiency for most workloads.
const DefaultTargetMorselSize = 8192

// DefaultQueueMultiplier is the multiplier for queue sizing.
const DefaultQueueMultiplier = 8

// MorselConfig holds configuration for morsel size limits.
type MorselConfig struct {
	// MinSize is the minimum number of rows per morsel.
	// Morsels smaller than this will be merged with adjacent morsels.
	MinSize int

	// MaxSize is the maximum number of rows per morsel.
	// Larger row ranges will be split into multiple morsels.
	MaxSize int

	// TargetSize is the preferred number of rows per morsel.
	// Used as a hint for optimal morsel sizing.
	TargetSize int
}

// DefaultMorselConfig returns the default morsel configuration.
func DefaultMorselConfig() MorselConfig {
	return MorselConfig{
		MinSize:    MinMorselSize,
		MaxSize:    MaxMorselSize,
		TargetSize: DefaultTargetMorselSize,
	}
}

// MorselGenerator creates morsels from table metadata.
// It partitions table data into work units that can be processed in parallel.
type MorselGenerator struct {
	config MorselConfig
}

// NewMorselGenerator creates a new morsel generator with default configuration.
func NewMorselGenerator() *MorselGenerator {
	return &MorselGenerator{
		config: DefaultMorselConfig(),
	}
}

// NewMorselGeneratorWithConfig creates a new morsel generator with custom configuration.
func NewMorselGeneratorWithConfig(config MorselConfig) *MorselGenerator {
	// Validate and fix configuration
	if config.MinSize <= 0 {
		config.MinSize = MinMorselSize
	}
	if config.MaxSize <= 0 {
		config.MaxSize = MaxMorselSize
	}
	if config.MaxSize < config.MinSize {
		config.MaxSize = config.MinSize
	}
	if config.TargetSize <= 0 {
		config.TargetSize = config.MinSize
	}
	if config.TargetSize > config.MaxSize {
		config.TargetSize = config.MaxSize
	}

	return &MorselGenerator{
		config: config,
	}
}

// GenerateMorsels creates morsels from table row count.
// It partitions the table into morsels based on row group boundaries and size limits.
func (g *MorselGenerator) GenerateMorsels(tableID uint64, totalRows uint64, rowGroupSize int) []Morsel {
	if totalRows == 0 {
		return nil
	}

	// Use target size for morsel creation, bounded by min/max
	morselSize := g.config.TargetSize
	if morselSize > int(totalRows) {
		morselSize = int(totalRows)
	}

	// Calculate row group size if not provided
	if rowGroupSize <= 0 {
		rowGroupSize = g.config.MaxSize
	}

	var morsels []Morsel
	var currentRow uint64
	rowGroup := 0

	for currentRow < totalRows {
		// Determine end row for this morsel
		endRow := currentRow + uint64(morselSize)
		if endRow > totalRows {
			endRow = totalRows
		}

		// Ensure we don't create tiny morsels at the end
		remaining := totalRows - endRow
		if remaining > 0 && remaining < uint64(g.config.MinSize) {
			endRow = totalRows
		}

		morsel := Morsel{
			TableID:  tableID,
			StartRow: currentRow,
			EndRow:   endRow,
			RowGroup: rowGroup,
			Priority: 0,
		}

		morsels = append(morsels, morsel)

		currentRow = endRow

		// Update row group when crossing row group boundaries
		if rowGroupSize > 0 && int(currentRow)%rowGroupSize == 0 {
			rowGroup++
		}
	}

	return morsels
}

// GenerateMorselsFromRowGroups creates morsels aligned to row group boundaries.
// This is more efficient when the table is already organized into row groups.
func (g *MorselGenerator) GenerateMorselsFromRowGroups(
	tableID uint64,
	rowGroups []RowGroupInfo,
) []Morsel {
	var morsels []Morsel

	for i, rg := range rowGroups {
		switch {
		case rg.RowCount > uint64(g.config.MaxSize):
			// Row group is larger than max morsel size, split it
			subMorsels := g.splitRowGroup(tableID, rg, i)
			morsels = append(morsels, subMorsels...)
		default:
			// Create single morsel for this row group (handles both normal and small row groups)
			morsels = append(morsels, Morsel{
				TableID:  tableID,
				StartRow: rg.StartRow,
				EndRow:   rg.StartRow + rg.RowCount,
				RowGroup: i,
				Priority: 0,
			})
		}
	}

	return morsels
}

// splitRowGroup splits a large row group into multiple morsels.
func (g *MorselGenerator) splitRowGroup(tableID uint64, rg RowGroupInfo, rgIndex int) []Morsel {
	var morsels []Morsel

	currentRow := rg.StartRow
	endRow := rg.StartRow + rg.RowCount

	for currentRow < endRow {
		morselEnd := currentRow + uint64(g.config.TargetSize)
		if morselEnd > endRow {
			morselEnd = endRow
		}

		// Avoid tiny trailing morsels
		remaining := endRow - morselEnd
		if remaining > 0 && remaining < uint64(g.config.MinSize) {
			morselEnd = endRow
		}

		morsels = append(morsels, Morsel{
			TableID:  tableID,
			StartRow: currentRow,
			EndRow:   morselEnd,
			RowGroup: rgIndex,
			Priority: 0,
		})

		currentRow = morselEnd
	}

	return morsels
}

// Config returns the current morsel configuration.
func (g *MorselGenerator) Config() MorselConfig {
	return g.config
}

// RowGroupInfo contains metadata about a row group for morsel generation.
type RowGroupInfo struct {
	StartRow uint64
	RowCount uint64
}

// WorkDistributor handles morsel distribution to workers with optional work stealing.
// It maintains per-worker queues and a global queue for work stealing.
type WorkDistributor struct {
	morsels           []Morsel
	queues            []chan Morsel // Per-worker queues
	globalQueue       chan Morsel   // For work stealing fallback
	enableWorkStealing bool
	numWorkers        int
	distributed       atomic.Bool
	closed            atomic.Bool
	mu                sync.RWMutex
}

// NewWorkDistributor creates a new work distributor.
// numWorkers specifies the number of worker queues to create.
// enableWorkStealing enables the work stealing mechanism.
func NewWorkDistributor(numWorkers int, enableWorkStealing bool) *WorkDistributor {
	return NewWorkDistributorWithCapacity(numWorkers, enableWorkStealing, 0)
}

// NewWorkDistributorWithCapacity creates a work distributor with a specified capacity hint.
// If capacity is 0, a default queue size is used.
// The capacity should be at least as large as the expected number of morsels.
func NewWorkDistributorWithCapacity(numWorkers int, enableWorkStealing bool, capacity int) *WorkDistributor {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Default queue size per worker
	queueSize := DefaultWorkChannelMultiplier * DefaultQueueMultiplier
	if capacity > 0 {
		// Size queues to hold all morsels distributed across workers
		queueSize = (capacity / numWorkers) + 1
	}

	wd := &WorkDistributor{
		morsels:            make([]Morsel, 0),
		queues:             make([]chan Morsel, numWorkers),
		enableWorkStealing: enableWorkStealing,
		numWorkers:         numWorkers,
	}

	// Create per-worker queues
	for i := 0; i < numWorkers; i++ {
		wd.queues[i] = make(chan Morsel, queueSize)
	}

	// Create global queue for work stealing
	if enableWorkStealing {
		globalSize := queueSize * numWorkers
		if capacity > 0 && capacity > globalSize {
			globalSize = capacity
		}
		wd.globalQueue = make(chan Morsel, globalSize)
	}

	return wd
}

// Distribute sends morsels to worker queues using round-robin distribution.
// Morsels are distributed evenly across workers initially.
// Extra morsels go to the global queue for work stealing.
func (d *WorkDistributor) Distribute(morsels []Morsel) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.distributed.Load() {
		return
	}

	d.morsels = morsels

	if len(morsels) == 0 {
		d.closeQueues()
		d.distributed.Store(true)
		return
	}

	// Sort morsels by priority (higher priority first)
	sortedMorsels := make([]Morsel, len(morsels))
	copy(sortedMorsels, morsels)
	sortMorselsByPriority(sortedMorsels)

	// Distribute morsels round-robin, blocking on queues to ensure delivery
	for i, morsel := range sortedMorsels {
		workerID := i % d.numWorkers

		// Try non-blocking first for better performance
		select {
		case d.queues[workerID] <- morsel:
			continue
		default:
			// Worker queue full
		}

		// If work stealing enabled, try global queue
		if d.enableWorkStealing && d.globalQueue != nil {
			select {
			case d.globalQueue <- morsel:
				continue
			default:
				// Global queue also full
			}
		}

		// Both queues full, need to block
		// Use select to try any available queue
		if d.enableWorkStealing && d.globalQueue != nil {
			select {
			case d.queues[workerID] <- morsel:
			case d.globalQueue <- morsel:
			}
		} else {
			// No work stealing, block on worker queue
			d.queues[workerID] <- morsel
		}
	}

	d.closeQueues()
	d.distributed.Store(true)
}

// DistributeAsync distributes morsels asynchronously without blocking.
// Returns immediately; morsels are distributed in a background goroutine.
func (d *WorkDistributor) DistributeAsync(morsels []Morsel) {
	go d.Distribute(morsels)
}

// closeQueues closes all worker queues and the global queue.
func (d *WorkDistributor) closeQueues() {
	if d.closed.Swap(true) {
		return
	}

	for i := range d.queues {
		close(d.queues[i])
	}

	if d.globalQueue != nil {
		close(d.globalQueue)
	}
}

// GetWork gets the next morsel for a worker.
// It first checks the worker's local queue, then attempts work stealing.
// Returns the morsel and true if one was available, or empty morsel and false if done.
func (d *WorkDistributor) GetWork(workerID int) (Morsel, bool) {
	if workerID < 0 || workerID >= d.numWorkers {
		return Morsel{}, false
	}

	// Try local queue first
	morsel, ok := <-d.queues[workerID]
	if ok {
		return morsel, true
	}

	// Local queue is closed, try work stealing
	if d.enableWorkStealing {
		return d.stealWork(workerID)
	}

	return Morsel{}, false
}

// TryGetWork attempts to get work without blocking.
// Returns immediately with the result.
func (d *WorkDistributor) TryGetWork(workerID int) (Morsel, bool) {
	if workerID < 0 || workerID >= d.numWorkers {
		return Morsel{}, false
	}

	// Try local queue first (non-blocking)
	select {
	case morsel, ok := <-d.queues[workerID]:
		if ok {
			return morsel, true
		}
	default:
		// Local queue empty, fall through to work stealing
	}

	// Try work stealing (non-blocking)
	if d.enableWorkStealing {
		return d.tryStealWork()
	}

	return Morsel{}, false
}

// stealWork attempts to steal work from the global queue or other workers.
func (d *WorkDistributor) stealWork(workerID int) (Morsel, bool) {
	// Try global queue first
	if d.globalQueue != nil {
		morsel, ok := <-d.globalQueue
		if ok {
			return morsel, true
		}
	}

	// Try stealing from other workers' queues
	for i := 0; i < d.numWorkers; i++ {
		victimID := (workerID + 1 + i) % d.numWorkers
		if victimID == workerID {
			continue
		}

		select {
		case morsel, ok := <-d.queues[victimID]:
			if ok {
				return morsel, true
			}
		default:
			// Queue empty or closed, try next
			continue
		}
	}

	return Morsel{}, false
}

// tryStealWork attempts to steal work without blocking.
func (d *WorkDistributor) tryStealWork() (Morsel, bool) {
	// Try global queue first
	if d.globalQueue != nil {
		select {
		case morsel, ok := <-d.globalQueue:
			if ok {
				return morsel, true
			}
		default:
			// Global queue empty
		}
	}

	return Morsel{}, false
}

// WorkerQueue returns the channel for a specific worker.
// Used for direct integration with Worker.Run.
func (d *WorkDistributor) WorkerQueue(workerID int) <-chan Morsel {
	if workerID < 0 || workerID >= d.numWorkers {
		return nil
	}
	return d.queues[workerID]
}

// GlobalQueue returns the global queue for work stealing.
// Returns nil if work stealing is disabled.
func (d *WorkDistributor) GlobalQueue() <-chan Morsel {
	return d.globalQueue
}

// NumWorkers returns the number of workers.
func (d *WorkDistributor) NumWorkers() int {
	return d.numWorkers
}

// IsDistributed returns whether morsels have been distributed.
func (d *WorkDistributor) IsDistributed() bool {
	return d.distributed.Load()
}

// MorselCount returns the total number of morsels.
func (d *WorkDistributor) MorselCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.morsels)
}

// sortMorselsByPriority sorts morsels in descending priority order.
// Higher priority morsels will be processed first.
func sortMorselsByPriority(morsels []Morsel) {
	// Simple insertion sort for small arrays; for larger arrays, consider using sort.Slice
	for i := 1; i < len(morsels); i++ {
		key := morsels[i]
		j := i - 1
		for j >= 0 && morsels[j].Priority < key.Priority {
			morsels[j+1] = morsels[j]
			j--
		}
		morsels[j+1] = key
	}
}

// SetMorselPriority sets the priority for a morsel at the given index.
// Higher priority morsels are processed first during distribution.
func SetMorselPriority(morsels []Morsel, index int, priority int) {
	if index >= 0 && index < len(morsels) {
		morsels[index].Priority = priority
	}
}

// PrioritizeLargeMorsels sets higher priority for morsels with more rows.
// This helps balance load by processing larger work units first.
func PrioritizeLargeMorsels(morsels []Morsel) {
	for i := range morsels {
		rowCount := morsels[i].EndRow - morsels[i].StartRow
		// Priority based on row count (scaled to reasonable range)
		morsels[i].Priority = int(rowCount / 1000)
	}
}
