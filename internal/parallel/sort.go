// Package parallel provides parallel query execution infrastructure.
// This file implements parallel sort with partition-based local sorting and K-way merge.
package parallel

import (
	"container/heap"
	"context"
	"hash/fnv"
	"sort"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// SortOrder represents the direction of sorting.
type SortOrder int

const (
	// Ascending sorts from smallest to largest.
	Ascending SortOrder = iota
	// Descending sorts from largest to smallest.
	Descending
)

// String returns the string representation of the sort order.
func (o SortOrder) String() string {
	switch o {
	case Ascending:
		return "ASC"
	case Descending:
		return "DESC"
	default:
		return "UNKNOWN"
	}
}

// NullsPosition specifies where NULL values should appear in sorted output.
type NullsPosition int

const (
	// NullsFirst places NULL values before non-NULL values.
	NullsFirst NullsPosition = iota
	// NullsLast places NULL values after non-NULL values.
	NullsLast
)

// String returns the string representation of the nulls position.
func (p NullsPosition) String() string {
	switch p {
	case NullsFirst:
		return "NULLS FIRST"
	case NullsLast:
		return "NULLS LAST"
	default:
		return "UNKNOWN"
	}
}

// SortKey describes one column in ORDER BY clause.
type SortKey struct {
	// Column is the column index to sort by.
	Column int
	// ColumnName is the name of the column (for output/debugging).
	ColumnName string
	// Order specifies ASC or DESC sorting.
	Order SortOrder
	// Nulls specifies NULLS FIRST or NULLS LAST.
	Nulls NullsPosition
}

// NewSortKey creates a new SortKey with default options (ASC, NULLS LAST).
func NewSortKey(column int, columnName string) SortKey {
	return SortKey{
		Column:     column,
		ColumnName: columnName,
		Order:      Ascending,
		Nulls:      NullsLast,
	}
}

// NewSortKeyWithOrder creates a SortKey with specified order.
func NewSortKeyWithOrder(
	column int,
	columnName string,
	order SortOrder,
	nulls NullsPosition,
) SortKey {
	return SortKey{
		Column:     column,
		ColumnName: columnName,
		Order:      order,
		Nulls:      nulls,
	}
}

// SortedRow holds a row with its original index for stability.
type SortedRow struct {
	// Row contains the actual row values.
	Row []any
	// OriginalIndex is the row's position in the original data for stable sorting.
	OriginalIndex int
}

// SortedPartition holds sorted data for one partition.
type SortedPartition struct {
	// Rows contains the sorted rows.
	Rows []SortedRow
	// Keys describes the sort keys.
	Keys []SortKey
	// RowCount is the number of rows in this partition.
	RowCount int
}

// NewSortedPartition creates a new empty SortedPartition.
func NewSortedPartition(keys []SortKey) *SortedPartition {
	return &SortedPartition{
		Rows:     make([]SortedRow, 0),
		Keys:     keys,
		RowCount: 0,
	}
}

// NewSortedPartitionWithCapacity creates a SortedPartition with pre-allocated capacity.
func NewSortedPartitionWithCapacity(keys []SortKey, capacity int) *SortedPartition {
	return &SortedPartition{
		Rows:     make([]SortedRow, 0, capacity),
		Keys:     keys,
		RowCount: 0,
	}
}

// Len returns the number of rows (implements sort.Interface).
func (p *SortedPartition) Len() int {
	return p.RowCount
}

// Less compares two rows for sorting (implements sort.Interface).
func (p *SortedPartition) Less(i, j int) bool {
	cmp := CompareRows(p.Rows[i].Row, p.Rows[j].Row, p.Keys)
	if cmp == 0 {
		// For stability, use original index as tiebreaker
		return p.Rows[i].OriginalIndex < p.Rows[j].OriginalIndex
	}
	return cmp < 0
}

// Swap exchanges two rows (implements sort.Interface).
func (p *SortedPartition) Swap(i, j int) {
	p.Rows[i], p.Rows[j] = p.Rows[j], p.Rows[i]
}

// AddRow adds a row to the partition.
func (p *SortedPartition) AddRow(row []any, originalIndex int) {
	p.Rows = append(p.Rows, SortedRow{
		Row:           row,
		OriginalIndex: originalIndex,
	})
	p.RowCount++
}

// Sort sorts the partition using Go's stable sort.
func (p *SortedPartition) Sort() {
	sort.Stable(p)
}

// MergeHeapItem represents an item in the K-way merge heap.
type MergeHeapItem struct {
	// Row is the current row from this partition.
	Row SortedRow
	// PartitionIndex identifies which partition this row came from.
	PartitionIndex int
}

// MergeHeap implements container/heap.Interface for K-way merge.
type MergeHeap struct {
	items []MergeHeapItem
	keys  []SortKey
}

// NewMergeHeap creates a new merge heap.
func NewMergeHeap(keys []SortKey) *MergeHeap {
	return &MergeHeap{
		items: make([]MergeHeapItem, 0),
		keys:  keys,
	}
}

// Len returns the heap size (implements heap.Interface).
func (h *MergeHeap) Len() int {
	return len(h.items)
}

// Less compares two heap items (implements heap.Interface).
func (h *MergeHeap) Less(i, j int) bool {
	cmp := CompareRows(h.items[i].Row.Row, h.items[j].Row.Row, h.keys)
	if cmp == 0 {
		// For stability across partitions, use original index
		// If from same partition, already stable; if different, use partition index
		if h.items[i].PartitionIndex != h.items[j].PartitionIndex {
			// For cross-partition stability, use partition index as secondary key
			// This maintains global ordering when rows are equal
			return h.items[i].PartitionIndex < h.items[j].PartitionIndex ||
				(h.items[i].PartitionIndex == h.items[j].PartitionIndex &&
					h.items[i].Row.OriginalIndex < h.items[j].Row.OriginalIndex)
		}
		return h.items[i].Row.OriginalIndex < h.items[j].Row.OriginalIndex
	}
	return cmp < 0
}

// Swap exchanges two heap items (implements heap.Interface).
func (h *MergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// Push adds an item to the heap (implements heap.Interface).
func (h *MergeHeap) Push(x any) {
	h.items = append(h.items, x.(MergeHeapItem))
}

// Pop removes and returns the smallest item (implements heap.Interface).
func (h *MergeHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// KWayMerger merges K sorted partitions into a single sorted output.
type KWayMerger struct {
	// Partitions are the sorted partitions to merge.
	Partitions []*SortedPartition
	// Keys describes the sort keys.
	Keys []SortKey
	// Indices tracks the current position in each partition.
	Indices []int
	// heap is the min-heap for K-way merge.
	heap *MergeHeap
	// initialized tracks if the heap has been initialized.
	initialized bool
}

// NewKWayMerger creates a new KWayMerger for the given partitions.
func NewKWayMerger(partitions []*SortedPartition, keys []SortKey) *KWayMerger {
	return &KWayMerger{
		Partitions: partitions,
		Keys:       keys,
		Indices:    make([]int, len(partitions)),
		heap:       NewMergeHeap(keys),
	}
}

// init initializes the heap with the first element from each partition.
func (m *KWayMerger) init() {
	if m.initialized {
		return
	}
	m.initialized = true

	for i, p := range m.Partitions {
		if p != nil && p.RowCount > 0 {
			heap.Push(m.heap, MergeHeapItem{
				Row:            p.Rows[0],
				PartitionIndex: i,
			})
			m.Indices[i] = 1
		}
	}
}

// Next returns the next row in sorted order.
// Returns nil and false when all partitions are exhausted.
func (m *KWayMerger) Next() ([]any, bool) {
	m.init()

	if m.heap.Len() == 0 {
		return nil, false
	}

	// Pop the smallest item
	item := heap.Pop(m.heap).(MergeHeapItem)
	partIdx := item.PartitionIndex

	// Push the next item from the same partition if available
	if m.Indices[partIdx] < m.Partitions[partIdx].RowCount {
		heap.Push(m.heap, MergeHeapItem{
			Row:            m.Partitions[partIdx].Rows[m.Indices[partIdx]],
			PartitionIndex: partIdx,
		})
		m.Indices[partIdx]++
	}

	return item.Row.Row, true
}

// MergeAll merges all partitions into a single DataChunk.
func (m *KWayMerger) MergeAll(columnTypes []dukdb.Type) *storage.DataChunk {
	m.init()

	// Calculate total row count
	totalRows := 0
	for _, p := range m.Partitions {
		if p != nil {
			totalRows += p.RowCount
		}
	}

	// Create result chunk
	chunk := storage.NewDataChunkWithCapacity(columnTypes, totalRows)

	// Merge all rows
	for {
		row, ok := m.Next()
		if !ok {
			break
		}
		chunk.AppendRow(row)
	}

	return chunk
}

// MergeWithLimit merges partitions with a row limit (for ORDER BY ... LIMIT).
func (m *KWayMerger) MergeWithLimit(columnTypes []dukdb.Type, limit int) *storage.DataChunk {
	m.init()

	if limit <= 0 {
		return m.MergeAll(columnTypes)
	}

	// Create result chunk with limited capacity
	chunk := storage.NewDataChunkWithCapacity(columnTypes, limit)

	// Merge up to limit rows
	count := 0
	for count < limit {
		row, ok := m.Next()
		if !ok {
			break
		}
		chunk.AppendRow(row)
		count++
	}

	return chunk
}

// Reset resets the merger for reuse.
func (m *KWayMerger) Reset() {
	m.initialized = false
	m.Indices = make([]int, len(m.Partitions))
	m.heap = NewMergeHeap(m.Keys)
}

// ParallelSort executes ORDER BY with parallel partition sort and K-way merge.
// It uses a three-phase approach:
// 1. Partition data by hash of sort key (for load balancing)
// 2. Each worker sorts its partition locally
// 3. K-way merge combines sorted partitions
type ParallelSort struct {
	// Source provides data for sorting.
	Source ParallelSource

	// SortKeys describes the ORDER BY columns.
	SortKeys []SortKey

	// NumPartitions is the number of partitions to use.
	NumPartitions int

	// Limit is an optional LIMIT clause (0 = no limit).
	Limit int

	// Offset is an optional OFFSET clause (0 = no offset).
	Offset int

	// partitions holds the sorted partitions after execution.
	partitions []*SortedPartition

	// columnNames are the column names from the source.
	columnNames []string

	// columnTypes are the column types from the source.
	columnTypes []dukdb.Type

	// rowCounter tracks original row indices for stability.
	rowCounter int

	// mu protects rowCounter for atomic increments.
	mu sync.Mutex
}

// NewParallelSort creates a new ParallelSort.
func NewParallelSort(source ParallelSource, keys []SortKey) *ParallelSort {
	return &ParallelSort{
		Source:        source,
		SortKeys:      keys,
		NumPartitions: 0, // Will be set during Execute based on worker count
	}
}

// NewParallelSortWithOptions creates a ParallelSort with custom options.
func NewParallelSortWithOptions(
	source ParallelSource,
	keys []SortKey,
	numPartitions, limit, offset int,
) *ParallelSort {
	return &ParallelSort{
		Source:        source,
		SortKeys:      keys,
		NumPartitions: numPartitions,
		Limit:         limit,
		Offset:        offset,
	}
}

// SetSchema sets the column names and types for the output.
func (s *ParallelSort) SetSchema(names []string, types []dukdb.Type) {
	s.columnNames = names
	s.columnTypes = types
}

// SetLimit sets the LIMIT clause.
func (s *ParallelSort) SetLimit(limit int) {
	s.Limit = limit
}

// SetOffset sets the OFFSET clause.
func (s *ParallelSort) SetOffset(offset int) {
	s.Offset = offset
}

// Execute runs parallel sort and returns the sorted result.
func (s *ParallelSort) Execute(pool *ThreadPool, ctx context.Context) (*storage.DataChunk, error) {
	if s.Source == nil {
		return s.emptyResult(), nil
	}

	// Generate morsels
	morsels := s.Source.GenerateMorsels()
	if len(morsels) == 0 {
		return s.emptyResult(), nil
	}

	// Set number of partitions based on worker count if not specified
	if s.NumPartitions <= 0 {
		s.NumPartitions = pool.NumWorkers
		if s.NumPartitions < 1 {
			s.NumPartitions = 1
		}
	}

	// Phase 1: Partition data by sort key hash
	if err := s.PartitionData(pool.Workers, morsels, ctx); err != nil {
		return nil, err
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Phase 2: Sort partitions locally (in parallel)
	if err := s.LocalSort(pool.Workers, ctx); err != nil {
		return nil, err
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Phase 3: K-way merge
	return s.KWayMerge(), nil
}

// PartitionData partitions input data by hash of sort key.
// Phase 1: Workers partition data into per-partition queues.
func (s *ParallelSort) PartitionData(
	workers []*Worker,
	morsels []Morsel,
	ctx context.Context,
) error {
	// Initialize partitions
	s.partitions = make([]*SortedPartition, s.NumPartitions)
	for i := 0; i < s.NumPartitions; i++ {
		s.partitions[i] = NewSortedPartition(s.SortKeys)
	}

	// Each worker gets its own local partition buffers
	localPartitions := make([][]*SortedPartition, len(workers))
	for w := range workers {
		localPartitions[w] = make([]*SortedPartition, s.NumPartitions)
		for p := 0; p < s.NumPartitions; p++ {
			localPartitions[w][p] = NewSortedPartition(s.SortKeys)
		}
	}

	// Create morsel channel
	morselChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		morselChan <- m
	}
	close(morselChan)

	// Track global row indices for stability
	s.rowCounter = 0

	var wg sync.WaitGroup
	errChan := make(chan error, len(workers))

	partitionMask := uint64(s.NumPartitions - 1)

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

					chunk, err := s.Source.Scan(morsel)
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

					// Detect column types from first chunk if not set
					if len(s.columnTypes) == 0 {
						s.mu.Lock()
						if len(s.columnTypes) == 0 {
							s.columnTypes = chunk.Types()
						}
						s.mu.Unlock()
					}

					// Get batch of row indices for stability
					s.mu.Lock()
					startIdx := s.rowCounter
					s.rowCounter += chunk.Count()
					s.mu.Unlock()

					// Process each row
					for i := 0; i < chunk.Count(); i++ {
						row := extractRow(chunk, i)
						rowIdx := startIdx + i

						// Hash by first sort key for partition assignment
						partitionHash := s.hashSortKey(row)
						partition := int(partitionHash & partitionMask)

						// Add to local partition
						localPartitions[wid][partition].AddRow(row, rowIdx)
					}
				}
			}
		}(worker, wIdx)
	}

	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// Merge local partitions into global partitions
	for p := 0; p < s.NumPartitions; p++ {
		totalRows := 0
		for w := range workers {
			totalRows += localPartitions[w][p].RowCount
		}

		// Pre-allocate
		s.partitions[p] = NewSortedPartitionWithCapacity(s.SortKeys, totalRows)

		for w := range workers {
			s.partitions[p].Rows = append(s.partitions[p].Rows, localPartitions[w][p].Rows...)
			s.partitions[p].RowCount += localPartitions[w][p].RowCount
		}
	}

	return nil
}

// LocalSort sorts each partition locally using parallel workers.
// Phase 2: Each worker sorts a subset of partitions.
func (s *ParallelSort) LocalSort(workers []*Worker, ctx context.Context) error {
	// Create partition work channel
	partitionChan := make(chan int, s.NumPartitions)
	for p := 0; p < s.NumPartitions; p++ {
		partitionChan <- p
	}
	close(partitionChan)

	var wg sync.WaitGroup

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

					// Sort this partition (uses stable sort internally)
					if s.partitions[partition] != nil && s.partitions[partition].RowCount > 0 {
						s.partitions[partition].Sort()
					}
				}
			}
		}(worker)
	}

	wg.Wait()

	return nil
}

// KWayMerge merges all sorted partitions into the final result.
// Phase 3: Uses a min-heap for efficient K-way merge.
func (s *ParallelSort) KWayMerge() *storage.DataChunk {
	merger := NewKWayMerger(s.partitions, s.SortKeys)

	// Calculate effective limit considering offset
	effectiveLimit := 0
	if s.Limit > 0 {
		effectiveLimit = s.Limit + s.Offset
	}

	var result *storage.DataChunk
	if effectiveLimit > 0 {
		result = merger.MergeWithLimit(s.columnTypes, effectiveLimit)
	} else {
		result = merger.MergeAll(s.columnTypes)
	}

	// Apply offset if specified
	if s.Offset > 0 && result.Count() > s.Offset {
		return s.applyOffset(result)
	}

	return result
}

// applyOffset creates a new chunk with rows after the offset.
func (s *ParallelSort) applyOffset(chunk *storage.DataChunk) *storage.DataChunk {
	if s.Offset <= 0 || s.Offset >= chunk.Count() {
		if s.Offset >= chunk.Count() {
			return s.emptyResult()
		}
		return chunk
	}

	newCount := chunk.Count() - s.Offset
	if s.Limit > 0 && newCount > s.Limit {
		newCount = s.Limit
	}

	result := storage.NewDataChunkWithCapacity(s.columnTypes, newCount)
	for i := s.Offset; i < s.Offset+newCount; i++ {
		row := make([]any, chunk.ColumnCount())
		for col := 0; col < chunk.ColumnCount(); col++ {
			row[col] = chunk.GetValue(i, col)
		}
		result.AppendRow(row)
	}

	return result
}

// emptyResult returns an empty DataChunk with the correct schema.
func (s *ParallelSort) emptyResult() *storage.DataChunk {
	if len(s.columnTypes) == 0 {
		return storage.NewDataChunkWithCapacity([]dukdb.Type{}, 0)
	}
	return storage.NewDataChunkWithCapacity(s.columnTypes, 0)
}

// hashSortKey computes a hash of the first sort key value for partitioning.
// Uses FNV-1a for deterministic hashing.
func (s *ParallelSort) hashSortKey(row []any) uint64 {
	if len(s.SortKeys) == 0 || len(row) == 0 {
		return 0
	}

	h := fnv.New64a()
	keyIdx := s.SortKeys[0].Column
	if keyIdx >= 0 && keyIdx < len(row) {
		hashSortValue(h, row[keyIdx])
	}
	return h.Sum64()
}

// hashSortValue adds a value to the hasher.
func hashSortValue(h interface{ Write([]byte) (int, error) }, val any) {
	hashAggValue(h, val) // Reuse the function from aggregate.go
}

// CompareValues compares two values for sorting.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareValues(a, b any, order SortOrder, nulls NullsPosition) int {
	// Handle NULL values
	aIsNull := a == nil
	bIsNull := b == nil

	if aIsNull && bIsNull {
		return 0
	}

	if aIsNull {
		if nulls == NullsFirst {
			if order == Ascending {
				return -1
			}
			return 1
		}
		// NullsLast
		if order == Ascending {
			return 1
		}
		return -1
	}

	if bIsNull {
		if nulls == NullsFirst {
			if order == Ascending {
				return 1
			}
			return -1
		}
		// NullsLast
		if order == Ascending {
			return -1
		}
		return 1
	}

	// Compare non-NULL values
	cmp := compareNonNullValues(a, b)

	// Apply sort order
	if order == Descending {
		cmp = -cmp
	}

	return cmp
}

// compareNonNullValues compares two non-NULL values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareNonNullValues(a, b any) int {
	// Try numeric comparison first
	aFloat, aOk := toSortFloat64(a)
	bFloat, bOk := toSortFloat64(b)
	if aOk && bOk {
		switch {
		case aFloat < bFloat:
			return -1
		case aFloat > bFloat:
			return 1
		default:
			return 0
		}
	}

	// Try string comparison
	aStr, aStrOk := a.(string)
	bStr, bStrOk := b.(string)
	if aStrOk && bStrOk {
		switch {
		case aStr < bStr:
			return -1
		case aStr > bStr:
			return 1
		default:
			return 0
		}
	}

	// Try bool comparison (false < true)
	aBool, aBoolOk := a.(bool)
	bBool, bBoolOk := b.(bool)
	if aBoolOk && bBoolOk {
		switch {
		case !aBool && bBool:
			return -1
		case aBool && !bBool:
			return 1
		default:
			return 0
		}
	}

	// Try []byte comparison
	aBytes, aBytesOk := a.([]byte)
	bBytes, bBytesOk := b.([]byte)
	if aBytesOk && bBytesOk {
		return compareBytesSlice(aBytes, bBytes)
	}

	// Default: treat as equal
	return 0
}

// compareBytesSlice compares two byte slices lexicographically.
func compareBytesSlice(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	// Prefixes are equal, compare lengths
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// toSortFloat64 converts a value to float64 for comparison.
func toSortFloat64(v any) (float64, bool) {
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

// CompareRows compares two rows by multiple sort keys.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func CompareRows(a, b []any, keys []SortKey) int {
	for _, key := range keys {
		if key.Column < 0 || key.Column >= len(a) || key.Column >= len(b) {
			continue
		}

		cmp := CompareValues(a[key.Column], b[key.Column], key.Order, key.Nulls)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// SortConfig holds configuration for parallel sort.
type SortConfig struct {
	// NumPartitions is the number of partitions for parallel sorting.
	// If 0, defaults to number of workers.
	NumPartitions int

	// Limit is the optional LIMIT clause.
	Limit int

	// Offset is the optional OFFSET clause.
	Offset int
}

// DefaultSortConfig returns the default sort configuration.
func DefaultSortConfig() SortConfig {
	return SortConfig{
		NumPartitions: 0,
		Limit:         0,
		Offset:        0,
	}
}

// Partitions returns the sorted partitions (for testing/debugging).
func (s *ParallelSort) Partitions() []*SortedPartition {
	return s.partitions
}

// TotalRows returns the total number of rows across all partitions.
func (s *ParallelSort) TotalRows() int {
	total := 0
	for _, p := range s.partitions {
		if p != nil {
			total += p.RowCount
		}
	}
	return total
}
