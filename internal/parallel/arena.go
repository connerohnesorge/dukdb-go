// Package parallel provides parallel query execution infrastructure for dukdb-go.
// It implements a morsel-driven pipeline execution model using Go's native
// concurrency primitives.
package parallel

import (
	"errors"
	"runtime"
	"sync"
)

// ErrMemoryLimitExceeded is returned when a memory allocation would exceed
// the configured memory limit.
var ErrMemoryLimitExceeded = errors.New("memory limit exceeded")

// MemoryLimit tracks total memory usage across all workers.
// It provides global memory accounting to prevent OOM situations.
type MemoryLimit struct {
	maxTotal    int64
	currentUsed int64
	mu          sync.Mutex
}

// NewMemoryLimit creates a new memory limit tracker with the given maximum.
func NewMemoryLimit(maxTotal int64) *MemoryLimit {
	return &MemoryLimit{maxTotal: maxTotal}
}

// Reserve attempts to reserve the given amount of memory.
// Returns true if the reservation was successful, false if it would exceed the limit.
func (m *MemoryLimit) Reserve(size int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentUsed+size > m.maxTotal {
		return false
	}
	m.currentUsed += size
	return true
}

// Release returns the given amount of memory back to the pool.
func (m *MemoryLimit) Release(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentUsed -= size
	if m.currentUsed < 0 {
		m.currentUsed = 0
	}
}

// CurrentUsage returns the current memory usage.
func (m *MemoryLimit) CurrentUsage() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentUsed
}

// MaxTotal returns the maximum memory limit.
func (m *MemoryLimit) MaxTotal() int64 {
	return m.maxTotal
}

// MemoryArena provides fast allocation for a single worker.
// It uses a block-based allocation strategy to reduce GC pressure
// and provides contention-free allocation within a single goroutine.
type MemoryArena struct {
	blocks       [][]byte
	current      int
	offset       int
	blockSize    int
	maxSize      int64
	currentSize  int64
	overflowSize int64
	memLimit     *MemoryLimit
}

// DefaultBlockSize is the default size for arena blocks (64KB).
const DefaultBlockSize = 64 * 1024

// DefaultArenaMaxSize is the default maximum arena size (64MB per worker).
const DefaultArenaMaxSize = 64 * 1024 * 1024

// NewMemoryArena creates a new memory arena with the given block size.
// If blockSize is 0, DefaultBlockSize is used.
func NewMemoryArena(blockSize int) *MemoryArena {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	return &MemoryArena{
		blocks:    make([][]byte, 0),
		blockSize: blockSize,
		maxSize:   DefaultArenaMaxSize,
	}
}

// NewMemoryArenaWithLimit creates an arena with dynamic sizing based on available memory.
// It uses a shared MemoryLimit for global memory accounting across workers.
func NewMemoryArenaWithLimit(blockSize int, memLimit *MemoryLimit, numWorkers int) *MemoryArena {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}

	// Calculate per-worker limit based on available memory
	availableMem := getAvailableSystemMemory()
	perWorkerLimit := availableMem / int64(numWorkers)

	// Cap at reasonable maximum (e.g., 1GB per worker)
	const maxPerWorker int64 = 1 << 30
	if perWorkerLimit > maxPerWorker {
		perWorkerLimit = maxPerWorker
	}

	return &MemoryArena{
		blocks:    make([][]byte, 0),
		blockSize: blockSize,
		maxSize:   perWorkerLimit,
		memLimit:  memLimit,
	}
}

// getAvailableSystemMemory returns an estimate of available system memory.
// It uses Go's runtime memory statistics to determine a safe allocation limit.
func getAvailableSystemMemory() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Use 75% of total system memory, leaving room for Go runtime
	return int64(m.Sys) * 75 / 100
}

// Allocate allocates the given number of bytes from the arena.
// Returns the allocated byte slice or an error if the memory limit is exceeded.
func (a *MemoryArena) Allocate(size int) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}

	// Check if we can allocate within arena
	if a.currentSize+int64(size) <= a.maxSize {
		// Check if current block has space
		if len(a.blocks) > 0 && a.offset+size <= len(a.blocks[a.current]) {
			ptr := a.blocks[a.current][a.offset : a.offset+size]
			a.offset += size
			return ptr, nil
		}

		// Need a new block
		newBlockSize := a.blockSize
		if size > newBlockSize {
			newBlockSize = size
		}

		// Check memory limit if set
		if a.memLimit != nil && !a.memLimit.Reserve(int64(newBlockSize)) {
			return nil, ErrMemoryLimitExceeded
		}

		newBlock := make([]byte, newBlockSize)
		a.blocks = append(a.blocks, newBlock)
		a.current = len(a.blocks) - 1
		a.offset = size
		a.currentSize += int64(newBlockSize)

		return newBlock[:size], nil
	}

	// Arena overflow - allocate from heap with tracking
	if a.memLimit != nil && !a.memLimit.Reserve(int64(size)) {
		return nil, ErrMemoryLimitExceeded
	}
	a.overflowSize += int64(size)
	return make([]byte, size), nil
}

// AllocateAligned allocates memory aligned to the given boundary.
// alignment must be a power of 2.
func (a *MemoryArena) AllocateAligned(size, alignment int) ([]byte, error) {
	if alignment <= 0 || (alignment&(alignment-1)) != 0 {
		alignment = 8 // Default to 8-byte alignment
	}

	// Calculate padding needed for alignment
	if len(a.blocks) > 0 {
		currentAddr := a.offset
		padding := (alignment - (currentAddr % alignment)) % alignment
		if padding > 0 {
			_, err := a.Allocate(padding)
			if err != nil {
				return nil, err
			}
		}
	}

	return a.Allocate(size)
}

// Reset clears all data from the arena, releasing memory back to the limit.
// It keeps the first block allocated to avoid repeated allocations.
func (a *MemoryArena) Reset() {
	// Release memory limit reservations
	if a.memLimit != nil {
		a.memLimit.Release(a.currentSize + a.overflowSize)
	}

	// Keep first block, release others
	if len(a.blocks) > 1 {
		// Reserve memory for the first block we're keeping
		if a.memLimit != nil {
			a.memLimit.Reserve(int64(len(a.blocks[0])))
		}
		a.blocks = a.blocks[:1]
		a.currentSize = int64(len(a.blocks[0]))
	} else if len(a.blocks) == 1 {
		// Reserve memory for the first block we're keeping
		if a.memLimit != nil {
			a.memLimit.Reserve(int64(len(a.blocks[0])))
		}
		a.currentSize = int64(len(a.blocks[0]))
	} else {
		a.currentSize = 0
	}

	a.current = 0
	a.offset = 0
	a.overflowSize = 0
}

// Clear completely clears the arena, releasing all memory.
func (a *MemoryArena) Clear() {
	// Release memory limit reservations
	if a.memLimit != nil {
		a.memLimit.Release(a.currentSize + a.overflowSize)
	}

	a.blocks = make([][]byte, 0)
	a.current = 0
	a.offset = 0
	a.currentSize = 0
	a.overflowSize = 0
}

// CurrentSize returns the current allocated size in the arena.
func (a *MemoryArena) CurrentSize() int64 {
	return a.currentSize + a.overflowSize
}

// BlockCount returns the number of blocks allocated.
func (a *MemoryArena) BlockCount() int {
	return len(a.blocks)
}

// BlockSize returns the configured block size.
func (a *MemoryArena) BlockSize() int {
	return a.blockSize
}

// MaxSize returns the maximum arena size.
func (a *MemoryArena) MaxSize() int64 {
	return a.maxSize
}
