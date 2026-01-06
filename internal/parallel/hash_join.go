// Package parallel provides parallel query execution infrastructure.
// This file implements parallel hash join with radix partitioning.
package parallel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Error definitions for hash join operations.
var (
	// ErrNoJoinKeys is returned when no join key columns are specified.
	ErrNoJoinKeys = errors.New("no join key columns specified")

	// ErrJoinKeyMismatch is returned when build and probe have different key counts.
	ErrJoinKeyMismatch = errors.New("build and probe key column counts do not match")

	// ErrNoBuildSource is returned when no build source is configured.
	ErrNoBuildSource = errors.New("no build source configured")

	// ErrNoProbeSource is returned when no probe source is configured.
	ErrNoProbeSource = errors.New("no probe source configured")

	// ErrSpillFailed is returned when spilling to disk fails.
	ErrSpillFailed = errors.New("spill to disk failed")

	// ErrJoinCancelled is returned when the join is cancelled.
	ErrJoinCancelled = errors.New("join cancelled")
)

// JoinType represents the type of join operation.
type JoinType int

const (
	// InnerJoin returns only matching rows from both sides.
	InnerJoin JoinType = iota
	// LeftJoin returns all rows from the left (probe) side and matching rows from the right (build) side.
	LeftJoin
	// RightJoin returns all rows from the right (build) side and matching rows from the left (probe) side.
	RightJoin
	// FullJoin returns all rows from both sides, with NULLs where there is no match.
	FullJoin
)

// String returns the string representation of the join type.
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER"
	case LeftJoin:
		return "LEFT"
	case RightJoin:
		return "RIGHT"
	case FullJoin:
		return "FULL"
	default:
		return "UNKNOWN"
	}
}

// PartitionEntry holds a row destined for a specific partition.
type PartitionEntry struct {
	Hash uint64
	Row  []any
}

// HashTable is a hash table for join build side.
// It supports concurrent reads during probe phase but requires
// locking during the build phase.
type HashTable struct {
	entries map[uint64][]PartitionEntry
	mu      sync.RWMutex
}

// NewHashTable creates a new HashTable.
func NewHashTable() *HashTable {
	return &HashTable{
		entries: make(map[uint64][]PartitionEntry),
	}
}

// NewHashTableWithCapacity creates a new HashTable with pre-allocated capacity.
func NewHashTableWithCapacity(capacity int) *HashTable {
	return &HashTable{
		entries: make(map[uint64][]PartitionEntry, capacity),
	}
}

// Insert adds an entry to the hash table. Thread-safe with locking.
func (ht *HashTable) Insert(hash uint64, row []any) {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.entries[hash] = append(ht.entries[hash], PartitionEntry{Hash: hash, Row: row})
}

// InsertUnsafe adds an entry without locking. Use only when single-threaded access is guaranteed.
func (ht *HashTable) InsertUnsafe(hash uint64, row []any) {
	ht.entries[hash] = append(ht.entries[hash], PartitionEntry{Hash: hash, Row: row})
}

// InsertEntry adds a PartitionEntry to the hash table without locking.
func (ht *HashTable) InsertEntry(entry PartitionEntry) {
	ht.entries[entry.Hash] = append(ht.entries[entry.Hash], entry)
}

// Probe finds all matching entries for the given hash and key values.
// Uses read lock for thread-safe concurrent access.
func (ht *HashTable) Probe(hash uint64, keyMatcher func(row []any) bool) []PartitionEntry {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	return ht.probeUnsafe(hash, keyMatcher)
}

// ProbeUnsafe finds all matching entries without locking.
// Use only when no concurrent writes are possible.
func (ht *HashTable) ProbeUnsafe(hash uint64, keyMatcher func(row []any) bool) []PartitionEntry {
	return ht.probeUnsafe(hash, keyMatcher)
}

func (ht *HashTable) probeUnsafe(hash uint64, keyMatcher func(row []any) bool) []PartitionEntry {
	entries, ok := ht.entries[hash]
	if !ok {
		return nil
	}

	var matches []PartitionEntry
	for _, entry := range entries {
		if keyMatcher == nil || keyMatcher(entry.Row) {
			matches = append(matches, entry)
		}
	}
	return matches
}

// Count returns the total number of entries in the hash table.
func (ht *HashTable) Count() int {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	count := 0
	for _, entries := range ht.entries {
		count += len(entries)
	}
	return count
}

// Entries returns all entries in the hash table.
// Used for outer join emission of unmatched rows.
func (ht *HashTable) Entries() []PartitionEntry {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	var all []PartitionEntry
	for _, entries := range ht.entries {
		all = append(all, entries...)
	}
	return all
}

// SpillManager handles partition spilling to disk for large builds.
type SpillManager struct {
	spillDir     string
	spilledParts map[int]string
	threshold    int64
	mu           sync.Mutex
	enabled      bool
}

// NewSpillManager creates a new SpillManager.
// threshold is the number of entries before spilling (not bytes).
// If threshold is 0, spilling is disabled.
func NewSpillManager(spillDir string, threshold int64) *SpillManager {
	return &SpillManager{
		spillDir:     spillDir,
		spilledParts: make(map[int]string),
		threshold:    threshold,
		enabled:      threshold > 0,
	}
}

// ShouldSpill returns whether the given partition should be spilled.
func (sm *SpillManager) ShouldSpill(partitionID int, entryCount int) bool {
	if !sm.enabled {
		return false
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return int64(entryCount) > sm.threshold
}

// Spill writes partition data to disk.
func (sm *SpillManager) Spill(partitionID int, entries []PartitionEntry) (string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.enabled {
		return "", nil
	}

	// Create spill directory if it does not exist
	if err := os.MkdirAll(sm.spillDir, 0755); err != nil {
		return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
	}

	// Create spill file
	filename := filepath.Join(sm.spillDir, fmt.Sprintf("partition_%d.spill", partitionID))
	file, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
	}
	defer func() { _ = file.Close() }()

	// Write entry count
	entryCount := uint64(len(entries))
	if err := binary.Write(file, binary.LittleEndian, entryCount); err != nil {
		return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
	}

	// Write each entry
	for _, entry := range entries {
		// Write hash
		if err := binary.Write(file, binary.LittleEndian, entry.Hash); err != nil {
			return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
		}

		// Write row length
		rowLen := uint32(len(entry.Row))
		if err := binary.Write(file, binary.LittleEndian, rowLen); err != nil {
			return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
		}

		// Write row values (simplified - only handles basic types)
		for _, val := range entry.Row {
			if err := writeValue(file, val); err != nil {
				return "", fmt.Errorf("%w: %v", ErrSpillFailed, err)
			}
		}
	}

	sm.spilledParts[partitionID] = filename
	return filename, nil
}

// ReadSpilled reads spilled partition data from disk.
func (sm *SpillManager) ReadSpilled(partitionID int) ([]PartitionEntry, error) {
	sm.mu.Lock()
	filename, ok := sm.spilledParts[partitionID]
	sm.mu.Unlock()

	if !ok {
		return nil, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSpillFailed, err)
	}
	defer func() { _ = file.Close() }()

	// Read entry count
	var entryCount uint64
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSpillFailed, err)
	}

	entries := make([]PartitionEntry, 0, entryCount)

	for i := uint64(0); i < entryCount; i++ {
		// Read hash
		var hash uint64
		if err := binary.Read(file, binary.LittleEndian, &hash); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("%w: %v", ErrSpillFailed, err)
		}

		// Read row length
		var rowLen uint32
		if err := binary.Read(file, binary.LittleEndian, &rowLen); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSpillFailed, err)
		}

		// Read row values
		row := make([]any, rowLen)
		for j := uint32(0); j < rowLen; j++ {
			val, err := readValue(file)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrSpillFailed, err)
			}
			row[j] = val
		}

		entries = append(entries, PartitionEntry{Hash: hash, Row: row})
	}

	return entries, nil
}

// IsSpilled returns whether the partition has been spilled.
func (sm *SpillManager) IsSpilled(partitionID int) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.spilledParts[partitionID]
	return ok
}

// Cleanup removes all spilled files.
func (sm *SpillManager) Cleanup() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, filename := range sm.spilledParts {
		_ = os.Remove(filename)
	}
	sm.spilledParts = make(map[int]string)

	// Try to remove the spill directory if empty
	_ = os.Remove(sm.spillDir)

	return nil
}

// writeValue writes a value to the file in a simple format.
func writeValue(w io.Writer, val any) error {
	switch v := val.(type) {
	case nil:
		return binary.Write(w, binary.LittleEndian, byte(0))
	case int64:
		if err := binary.Write(w, binary.LittleEndian, byte(1)); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case int32:
		if err := binary.Write(w, binary.LittleEndian, byte(2)); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case float64:
		if err := binary.Write(w, binary.LittleEndian, byte(3)); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case string:
		if err := binary.Write(w, binary.LittleEndian, byte(4)); err != nil {
			return err
		}
		strBytes := []byte(v)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(strBytes))); err != nil {
			return err
		}
		_, err := w.Write(strBytes)
		return err
	case bool:
		if err := binary.Write(w, binary.LittleEndian, byte(5)); err != nil {
			return err
		}
		if v {
			return binary.Write(w, binary.LittleEndian, byte(1))
		}
		return binary.Write(w, binary.LittleEndian, byte(0))
	case int:
		if err := binary.Write(w, binary.LittleEndian, byte(1)); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int64(v))
	default:
		// For unknown types, write as nil
		return binary.Write(w, binary.LittleEndian, byte(0))
	}
}

// readValue reads a value from the file.
func readValue(r io.Reader) (any, error) {
	var typeTag byte
	if err := binary.Read(r, binary.LittleEndian, &typeTag); err != nil {
		return nil, err
	}

	switch typeTag {
	case 0: // nil
		return nil, nil
	case 1: // int64
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case 2: // int32
		var v int32
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case 3: // float64
		var v float64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case 4: // string
		var strLen uint32
		if err := binary.Read(r, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}
		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return nil, err
		}
		return string(strBytes), nil
	case 5: // bool
		var v byte
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v != 0, nil
	default:
		return nil, nil
	}
}

// ParallelHashJoin executes hash join with partition parallelism.
// It uses a three-phase approach: partition, build, and probe.
type ParallelHashJoin struct {
	// BuildSource provides data for the build side of the join.
	BuildSource ParallelSource
	// ProbeSource provides data for the probe side of the join.
	ProbeSource ParallelSource

	// BuildKeyIndices are the column indices for join keys in build tuples.
	BuildKeyIndices []int
	// ProbeKeyIndices are the column indices for join keys in probe tuples.
	ProbeKeyIndices []int

	// JoinType specifies the type of join (INNER, LEFT, RIGHT, FULL).
	JoinType JoinType

	// NumPartitions is the number of hash partitions (should be power of 2).
	NumPartitions int

	// HashTables are the per-partition hash tables.
	HashTables []*HashTable

	// PartitionQueues hold partitioned build data before hash table construction.
	PartitionQueues [][]PartitionEntry

	// BuildMatched tracks which build rows have been matched (for outer joins).
	BuildMatched [][]atomic.Bool

	// BuildColumns are the column names from the build side.
	BuildColumns []string
	// ProbeColumns are the column names from the probe side.
	ProbeColumns []string

	// BuildTypes are the column types from the build side.
	BuildTypes []dukdb.Type
	// ProbeTypes are the column types from the probe side.
	ProbeTypes []dukdb.Type

	// SpillManager handles spilling for large builds.
	SpillManager *SpillManager

	// partitionMask is used for fast partition assignment.
	partitionMask uint64
}

// NewParallelHashJoin creates a new ParallelHashJoin.
func NewParallelHashJoin(
	buildSource, probeSource ParallelSource,
	buildKeyIndices, probeKeyIndices []int,
	joinType JoinType,
	numPartitions int,
) *ParallelHashJoin {
	if numPartitions <= 0 {
		numPartitions = 16 // Default to 16 partitions
	}
	// Ensure power of 2
	numPartitions = nextPowerOf2(numPartitions)

	return &ParallelHashJoin{
		BuildSource:     buildSource,
		ProbeSource:     probeSource,
		BuildKeyIndices: buildKeyIndices,
		ProbeKeyIndices: probeKeyIndices,
		JoinType:        joinType,
		NumPartitions:   numPartitions,
		HashTables:      make([]*HashTable, numPartitions),
		PartitionQueues: make([][]PartitionEntry, numPartitions),
		partitionMask:   uint64(numPartitions - 1),
	}
}

// SetSpillManager sets the spill manager for handling large builds.
func (j *ParallelHashJoin) SetSpillManager(sm *SpillManager) {
	j.SpillManager = sm
}

// SetBuildSchema sets the column names and types for the build side.
func (j *ParallelHashJoin) SetBuildSchema(columns []string, types []dukdb.Type) {
	j.BuildColumns = columns
	j.BuildTypes = types
}

// SetProbeSchema sets the column names and types for the probe side.
func (j *ParallelHashJoin) SetProbeSchema(columns []string, types []dukdb.Type) {
	j.ProbeColumns = columns
	j.ProbeTypes = types
}

// OutputSchema returns the output column types for the join result.
// For inner/left joins: probe columns followed by build columns.
// The probe-side key columns are not duplicated.
func (j *ParallelHashJoin) OutputSchema() []dukdb.Type {
	// All probe columns + all build columns (including key columns)
	outputTypes := make([]dukdb.Type, 0, len(j.ProbeTypes)+len(j.BuildTypes))
	outputTypes = append(outputTypes, j.ProbeTypes...)
	outputTypes = append(outputTypes, j.BuildTypes...)
	return outputTypes
}

// OutputColumns returns the output column names for the join result.
func (j *ParallelHashJoin) OutputColumns() []string {
	outputCols := make([]string, 0, len(j.ProbeColumns)+len(j.BuildColumns))
	outputCols = append(outputCols, j.ProbeColumns...)
	outputCols = append(outputCols, j.BuildColumns...)
	return outputCols
}

// Execute runs all three phases of the hash join with proper synchronization.
// Returns a channel that produces result chunks.
func (j *ParallelHashJoin) Execute(pool *ThreadPool, ctx context.Context) (<-chan *storage.DataChunk, error) {
	// Validate inputs
	if j.BuildSource == nil {
		return nil, ErrNoBuildSource
	}
	if j.ProbeSource == nil {
		return nil, ErrNoProbeSource
	}
	if len(j.BuildKeyIndices) == 0 || len(j.ProbeKeyIndices) == 0 {
		return nil, ErrNoJoinKeys
	}
	if len(j.BuildKeyIndices) != len(j.ProbeKeyIndices) {
		return nil, ErrJoinKeyMismatch
	}

	resultChan := make(chan *storage.DataChunk, pool.NumWorkers*DefaultWorkChannelMultiplier)
	errChan := make(chan error, 1)

	go func() {
		defer close(resultChan)

		// Phase 1: Partition build data
		if err := j.PartitionBuildData(pool.Workers, ctx); err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			select {
			case errChan <- ErrJoinCancelled:
			default:
			}
			return
		default:
		}

		// Phase 2: Build hash tables
		if err := j.BuildHashTables(pool.Workers, ctx); err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			select {
			case errChan <- ErrJoinCancelled:
			default:
			}
			return
		default:
		}

		// Phase 3: Probe
		if err := j.ParallelProbe(pool.Workers, ctx, resultChan); err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}
	}()

	// Return result channel (errors are not currently surfaced via the return)
	return resultChan, nil
}

// PartitionBuildData partitions build-side data using radix partitioning.
// Phase 1: Workers partition build-side data into per-partition queues.
func (j *ParallelHashJoin) PartitionBuildData(workers []*Worker, ctx context.Context) error {
	morsels := j.BuildSource.GenerateMorsels()
	if len(morsels) == 0 {
		return nil
	}

	// Each worker gets its own local partition buffers to avoid contention
	localPartitions := make([][][]PartitionEntry, len(workers))
	for w := range workers {
		localPartitions[w] = make([][]PartitionEntry, j.NumPartitions)
		for p := 0; p < j.NumPartitions; p++ {
			localPartitions[w][p] = make([]PartitionEntry, 0, 64)
		}
	}

	// Create morsel channel
	morselChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		morselChan <- m
	}
	close(morselChan)

	// Workers process morsels and partition locally (no locks needed)
	var wg sync.WaitGroup
	errChan := make(chan error, len(workers))

	for wIdx, worker := range workers {
		wg.Add(1)
		go func(w *Worker, wid int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case morsel, ok := <-morselChan:
					if !ok {
						return
					}

					chunk, err := j.BuildSource.Scan(morsel)
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}
					if chunk == nil {
						continue
					}

					// Process each row in the chunk
					for i := 0; i < chunk.Count(); i++ {
						// Extract row
						row := extractRow(chunk, i)

						// Compute hash of join keys
						rowHash := j.hashRow(row, j.BuildKeyIndices)

						// Determine partition using radix partitioning
						partition := int(rowHash & j.partitionMask)

						// Add to local partition buffer
						localPartitions[wid][partition] = append(
							localPartitions[wid][partition],
							PartitionEntry{Hash: rowHash, Row: row},
						)
					}
				}
			}
		}(worker, wIdx)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// Merge local partitions into global partition queues (single-threaded merge)
	for p := 0; p < j.NumPartitions; p++ {
		totalSize := 0
		for w := range workers {
			totalSize += len(localPartitions[w][p])
		}

		j.PartitionQueues[p] = make([]PartitionEntry, 0, totalSize)
		for w := range workers {
			j.PartitionQueues[p] = append(j.PartitionQueues[p], localPartitions[w][p]...)
		}
	}

	return nil
}

// BuildHashTables builds hash tables from partitioned data.
// Phase 2: Each worker handles a subset of partitions - no locks needed.
func (j *ParallelHashJoin) BuildHashTables(workers []*Worker, ctx context.Context) error {
	// Create partition work channel
	partitionChan := make(chan int, j.NumPartitions)
	for p := 0; p < j.NumPartitions; p++ {
		partitionChan <- p
	}
	close(partitionChan)

	var wg sync.WaitGroup
	errChan := make(chan error, len(workers))

	// Initialize build matched tracking for outer joins
	if j.JoinType == RightJoin || j.JoinType == FullJoin {
		j.BuildMatched = make([][]atomic.Bool, j.NumPartitions)
	}

	for _, worker := range workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case partition, ok := <-partitionChan:
					if !ok {
						return
					}

					entries := j.PartitionQueues[partition]

					// Check if we need to spill
					if j.SpillManager != nil && j.SpillManager.ShouldSpill(partition, len(entries)) {
						_, err := j.SpillManager.Spill(partition, entries)
						if err != nil {
							select {
							case errChan <- err:
							default:
							}
							return
						}
						// Clear partition queue after spilling
						j.PartitionQueues[partition] = nil
						// Leave hash table nil so probe knows to load from disk
						j.HashTables[partition] = nil
						continue
					}

					// Build hash table for this partition - no contention
					ht := NewHashTableWithCapacity(len(entries))
					for _, entry := range entries {
						ht.InsertEntry(entry)
					}
					j.HashTables[partition] = ht

					// Initialize match tracking for outer joins
					if j.JoinType == RightJoin || j.JoinType == FullJoin {
						j.BuildMatched[partition] = make([]atomic.Bool, len(entries))
					}

					// Clear partition queue to free memory
					j.PartitionQueues[partition] = nil
				}
			}
		}(worker)
	}

	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

// ParallelProbe probes hash tables with probe-side data.
// Phase 3: Probe phase is read-only, enabling full parallelism.
func (j *ParallelHashJoin) ParallelProbe(workers []*Worker, ctx context.Context, resultChan chan<- *storage.DataChunk) error {
	morsels := j.ProbeSource.GenerateMorsels()
	if len(morsels) == 0 {
		// For right/full joins, still need to emit unmatched build rows
		if j.JoinType == RightJoin || j.JoinType == FullJoin {
			j.emitUnmatchedBuildRows(resultChan)
		}
		return nil
	}

	// Create morsel channel
	morselChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		morselChan <- m
	}
	close(morselChan)

	var wg sync.WaitGroup
	errChan := make(chan error, len(workers))

	for _, worker := range workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case morsel, ok := <-morselChan:
					if !ok {
						return
					}

					chunk, err := j.ProbeSource.Scan(morsel)
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}
					if chunk == nil {
						continue
					}

					// Process probe chunk
					results := j.probeChunk(chunk)

					for _, result := range results {
						if result != nil && result.Count() > 0 {
							select {
							case resultChan <- result:
							case <-ctx.Done():
								return
							}
						}
					}
				}
			}
		}(worker)
	}

	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// For right/full outer joins, emit unmatched build rows
	if j.JoinType == RightJoin || j.JoinType == FullJoin {
		j.emitUnmatchedBuildRows(resultChan)
	}

	return nil
}

// probeChunk processes a single probe chunk against the hash tables.
// Returns a slice of result chunks to handle cases where output exceeds capacity.
func (j *ParallelHashJoin) probeChunk(chunk *storage.DataChunk) []*storage.DataChunk {
	outputTypes := j.OutputSchema()
	chunkCapacity := storage.StandardVectorSize
	result := storage.NewDataChunkWithCapacity(outputTypes, chunkCapacity)
	var results []*storage.DataChunk

	for i := 0; i < chunk.Count(); i++ {
		probeRow := extractRow(chunk, i)

		// Compute hash of probe keys
		probeHash := j.hashRow(probeRow, j.ProbeKeyIndices)

		// Determine partition
		partition := int(probeHash & j.partitionMask)

		// Get hash table for this partition
		ht := j.HashTables[partition]
		if ht == nil {
			// Check if spilled
			if j.SpillManager != nil && j.SpillManager.IsSpilled(partition) {
				// Load spilled partition (simplified - in practice would need better handling)
				entries, err := j.SpillManager.ReadSpilled(partition)
				if err == nil && len(entries) > 0 {
					ht = NewHashTableWithCapacity(len(entries))
					for _, entry := range entries {
						ht.InsertEntry(entry)
					}
					j.HashTables[partition] = ht
				}
			}
		}

		if ht == nil {
			// No build data for this partition
			if j.JoinType == LeftJoin || j.JoinType == FullJoin {
				// Emit probe row with NULLs for build columns
				if !j.emitWithNullBuild(result, probeRow) {
					// Chunk full, save and create new one
					if result.Count() > 0 {
						results = append(results, result)
					}
					result = storage.NewDataChunkWithCapacity(outputTypes, chunkCapacity)
					j.emitWithNullBuild(result, probeRow)
				}
			}
			continue
		}

		// Create key matcher for probe keys
		keyMatcher := j.createKeyMatcher(probeRow)

		// Probe hash table (read-only, no lock needed after build)
		matches := ht.ProbeUnsafe(probeHash, keyMatcher)

		if len(matches) == 0 {
			// No matches
			if j.JoinType == LeftJoin || j.JoinType == FullJoin {
				if !j.emitWithNullBuild(result, probeRow) {
					if result.Count() > 0 {
						results = append(results, result)
					}
					result = storage.NewDataChunkWithCapacity(outputTypes, chunkCapacity)
					j.emitWithNullBuild(result, probeRow)
				}
			}
		} else {
			// Emit matched rows
			for matchIdx, match := range matches {
				outputRow := make([]any, len(probeRow)+len(match.Row))
				copy(outputRow, probeRow)
				copy(outputRow[len(probeRow):], match.Row)
				if !result.AppendRow(outputRow) {
					// Chunk full, save and create new one
					if result.Count() > 0 {
						results = append(results, result)
					}
					result = storage.NewDataChunkWithCapacity(outputTypes, chunkCapacity)
					result.AppendRow(outputRow)
				}

				// Mark build row as matched for outer joins
				if j.BuildMatched != nil && partition < len(j.BuildMatched) && matchIdx < len(j.BuildMatched[partition]) {
					j.BuildMatched[partition][matchIdx].Store(true)
				}
			}
		}
	}

	// Add the final result chunk if it has data
	if result.Count() > 0 {
		results = append(results, result)
	}

	return results
}

// createKeyMatcher creates a function to match probe keys against build keys.
func (j *ParallelHashJoin) createKeyMatcher(probeRow []any) func(buildRow []any) bool {
	return func(buildRow []any) bool {
		for i := 0; i < len(j.ProbeKeyIndices); i++ {
			probeKey := probeRow[j.ProbeKeyIndices[i]]
			buildKey := buildRow[j.BuildKeyIndices[i]]
			if !equalValues(probeKey, buildKey) {
				return false
			}
		}
		return true
	}
}

// emitWithNullBuild emits a probe row with NULL values for build columns.
// Returns true if the row was added, false if the chunk is full.
func (j *ParallelHashJoin) emitWithNullBuild(result *storage.DataChunk, probeRow []any) bool {
	outputRow := make([]any, len(probeRow)+len(j.BuildTypes))
	copy(outputRow, probeRow)
	// Build columns are already nil (NULL)
	return result.AppendRow(outputRow)
}

// emitUnmatchedBuildRows emits build rows that were not matched (for RIGHT/FULL joins).
func (j *ParallelHashJoin) emitUnmatchedBuildRows(resultChan chan<- *storage.DataChunk) {
	outputTypes := j.OutputSchema()

	for partition := 0; partition < j.NumPartitions; partition++ {
		ht := j.HashTables[partition]
		if ht == nil {
			continue
		}

		entries := ht.Entries()
		if j.BuildMatched == nil || partition >= len(j.BuildMatched) {
			continue
		}

		result := storage.NewDataChunkWithCapacity(outputTypes, len(entries))

		for entryIdx, entry := range entries {
			// Check if this entry was matched
			if entryIdx < len(j.BuildMatched[partition]) && j.BuildMatched[partition][entryIdx].Load() {
				continue // Was matched, skip
			}

			// Emit unmatched build row with NULLs for probe columns
			outputRow := make([]any, len(j.ProbeTypes)+len(entry.Row))
			// Probe columns are nil (NULL)
			copy(outputRow[len(j.ProbeTypes):], entry.Row)
			result.AppendRow(outputRow)
		}

		if result.Count() > 0 {
			resultChan <- result
		}
	}
}

// hashRow computes a hash of the specified key columns in a row.
// Uses FNV-1a hash which is deterministic across all goroutines.
func (j *ParallelHashJoin) hashRow(row []any, keyIndices []int) uint64 {
	h := fnv.New64a()

	for _, idx := range keyIndices {
		if idx >= len(row) {
			continue
		}
		hashValue(h, row[idx])
	}

	return h.Sum64()
}

// hashValue adds a value to the hasher using FNV-1a.
func hashValue(h hash.Hash64, val any) {
	switch v := val.(type) {
	case nil:
		_, _ = h.Write([]byte{0})
	case bool:
		if v {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	case int:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	case int8:
		_, _ = h.Write([]byte{byte(v)})
	case int16:
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(v))
		_, _ = h.Write(buf[:])
	case int32:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v))
		_, _ = h.Write(buf[:])
	case int64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	case uint:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	case uint8:
		_, _ = h.Write([]byte{v})
	case uint16:
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], v)
		_, _ = h.Write(buf[:])
	case uint32:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], v)
		_, _ = h.Write(buf[:])
	case uint64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	case float32:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(v))
		_, _ = h.Write(buf[:])
	case float64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	case string:
		_, _ = h.Write([]byte(v))
	case []byte:
		_, _ = h.Write(v)
	default:
		// For unknown types, write a placeholder
		_, _ = h.Write([]byte{0xFF})
	}
}

// equalValues checks if two values are equal for join key comparison.
func equalValues(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try numeric comparison
	aFloat, aOk := toFloat64Value(a)
	bFloat, bOk := toFloat64Value(b)
	if aOk && bOk {
		return aFloat == bFloat
	}

	// String comparison
	aStr, aStrOk := a.(string)
	bStr, bStrOk := b.(string)
	if aStrOk && bStrOk {
		return aStr == bStr
	}

	// Default equality
	return a == b
}

// toFloat64Value converts a value to float64 for comparison.
func toFloat64Value(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// extractRow extracts a row from a DataChunk at the given index.
func extractRow(chunk *storage.DataChunk, rowIdx int) []any {
	colCount := chunk.ColumnCount()
	row := make([]any, colCount)
	for col := 0; col < colCount; col++ {
		row[col] = chunk.GetValue(rowIdx, col)
	}
	return row
}

// SelectPartitionCount chooses an optimal partition count based on estimated build rows and worker count.
// The result is always a power of 2 for efficient radix partitioning.
func SelectPartitionCount(estimatedBuildRows, numWorkers int) int {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if estimatedBuildRows <= 0 {
		return nextPowerOf2(numWorkers)
	}

	// At least as many partitions as workers for good parallelism
	minPartitions := numWorkers

	// Target ~64K rows per partition for cache efficiency
	const targetRowsPerPartition = 64 * 1024
	targetPartitions := estimatedBuildRows / targetRowsPerPartition
	if targetPartitions < 1 {
		targetPartitions = 1
	}

	// Use the larger of min and target
	partitions := minPartitions
	if targetPartitions > partitions {
		partitions = targetPartitions
	}

	// Cap at reasonable maximum
	const maxPartitions = 1024
	if partitions > maxPartitions {
		partitions = maxPartitions
	}

	return nextPowerOf2(partitions)
}

// nextPowerOf2 returns the smallest power of 2 >= n.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// HashJoinConfig holds configuration for parallel hash join.
type HashJoinConfig struct {
	// NumPartitions is the number of hash partitions to use.
	// If 0, it will be automatically selected.
	NumPartitions int

	// SpillThreshold is the number of entries per partition before spilling.
	// If 0, spilling is disabled.
	SpillThreshold int64

	// SpillDir is the directory for spill files.
	// Required if SpillThreshold > 0.
	SpillDir string

	// EstimatedBuildRows is a hint for partition count selection.
	EstimatedBuildRows int
}

// DefaultHashJoinConfig returns the default hash join configuration.
func DefaultHashJoinConfig() HashJoinConfig {
	return HashJoinConfig{
		NumPartitions:      0, // Auto-select
		SpillThreshold:     0, // Disabled
		SpillDir:           "",
		EstimatedBuildRows: 0,
	}
}

// NewParallelHashJoinWithConfig creates a new ParallelHashJoin with configuration.
func NewParallelHashJoinWithConfig(
	buildSource, probeSource ParallelSource,
	buildKeyIndices, probeKeyIndices []int,
	joinType JoinType,
	config HashJoinConfig,
	numWorkers int,
) *ParallelHashJoin {
	numPartitions := config.NumPartitions
	if numPartitions <= 0 {
		numPartitions = SelectPartitionCount(config.EstimatedBuildRows, numWorkers)
	}

	join := NewParallelHashJoin(
		buildSource, probeSource,
		buildKeyIndices, probeKeyIndices,
		joinType,
		numPartitions,
	)

	if config.SpillThreshold > 0 && config.SpillDir != "" {
		join.SetSpillManager(NewSpillManager(config.SpillDir, config.SpillThreshold))
	}

	return join
}

// Cleanup releases resources used by the hash join.
func (j *ParallelHashJoin) Cleanup() {
	// Clear hash tables
	for i := range j.HashTables {
		j.HashTables[i] = nil
	}

	// Clear partition queues
	for i := range j.PartitionQueues {
		j.PartitionQueues[i] = nil
	}

	// Clear match tracking
	j.BuildMatched = nil

	// Cleanup spill files
	if j.SpillManager != nil {
		_ = j.SpillManager.Cleanup()
	}
}
