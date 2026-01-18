// Package parallel provides parallel query execution infrastructure.
// This file implements parallel aggregation with two-phase local/global merge.
package parallel

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// AggregateType represents the type of aggregate function.
type AggregateType int

const (
	// AggSum computes the sum of values.
	AggSum AggregateType = iota
	// AggCount counts non-null values.
	AggCount
	// AggCountStar counts all rows including nulls.
	AggCountStar
	// AggAvg computes the average of values.
	AggAvg
	// AggMin finds the minimum value.
	AggMin
	// AggMax finds the maximum value.
	AggMax
	// AggFirst returns the first value.
	AggFirst
	// AggLast returns the last value.
	AggLast
)

// String returns the string representation of the aggregate type.
func (t AggregateType) String() string {
	switch t {
	case AggSum:
		return "SUM"
	case AggCount:
		return "COUNT"
	case AggCountStar:
		return "COUNT(*)"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	case AggFirst:
		return "FIRST"
	case AggLast:
		return "LAST"
	default:
		return "UNKNOWN"
	}
}

// AggregateState tracks state for one aggregate function.
// Different aggregate functions have different state requirements.
type AggregateState interface {
	// Update adds a new value to the aggregate state.
	Update(value any)
	// Merge combines another state into this one.
	Merge(other AggregateState)
	// Finalize returns the final aggregate result.
	Finalize() any
	// Clone creates a deep copy of this state.
	Clone() AggregateState
	// Reset clears the state for reuse.
	Reset()
}

// SumState implements AggregateState for SUM aggregate.
type SumState struct {
	sum     float64
	hasData bool
}

// NewSumState creates a new SumState.
func NewSumState() *SumState {
	return &SumState{}
}

// Update adds a value to the sum.
func (s *SumState) Update(value any) {
	if value == nil {
		return
	}
	if f, ok := toAggFloat64(value); ok {
		s.sum += f
		s.hasData = true
	}
}

// Merge combines another SumState into this one.
func (s *SumState) Merge(other AggregateState) {
	if o, ok := other.(*SumState); ok {
		if o.hasData {
			s.sum += o.sum
			s.hasData = true
		}
	}
}

// Finalize returns the final sum.
func (s *SumState) Finalize() any {
	if !s.hasData {
		return nil
	}
	return s.sum
}

// Clone creates a copy of the SumState.
func (s *SumState) Clone() AggregateState {
	return &SumState{sum: s.sum, hasData: s.hasData}
}

// Reset clears the state.
func (s *SumState) Reset() {
	s.sum = 0
	s.hasData = false
}

// CountState implements AggregateState for COUNT aggregate.
type CountState struct {
	count int64
}

// NewCountState creates a new CountState.
func NewCountState() *CountState {
	return &CountState{}
}

// Update increments the count for non-null values.
func (c *CountState) Update(value any) {
	if value != nil {
		c.count++
	}
}

// Merge combines another CountState into this one.
func (c *CountState) Merge(other AggregateState) {
	if o, ok := other.(*CountState); ok {
		c.count += o.count
	}
}

// Finalize returns the final count.
func (c *CountState) Finalize() any {
	return c.count
}

// Clone creates a copy of the CountState.
func (c *CountState) Clone() AggregateState {
	return &CountState{count: c.count}
}

// Reset clears the state.
func (c *CountState) Reset() {
	c.count = 0
}

// CountStarState implements AggregateState for COUNT(*) aggregate.
// Unlike COUNT, it counts all rows including nulls.
type CountStarState struct {
	count int64
}

// NewCountStarState creates a new CountStarState.
func NewCountStarState() *CountStarState {
	return &CountStarState{}
}

// Update increments the count for all values (including null).
func (c *CountStarState) Update(value any) {
	c.count++
}

// Merge combines another CountStarState into this one.
func (c *CountStarState) Merge(other AggregateState) {
	if o, ok := other.(*CountStarState); ok {
		c.count += o.count
	}
}

// Finalize returns the final count.
func (c *CountStarState) Finalize() any {
	return c.count
}

// Clone creates a copy of the CountStarState.
func (c *CountStarState) Clone() AggregateState {
	return &CountStarState{count: c.count}
}

// Reset clears the state.
func (c *CountStarState) Reset() {
	c.count = 0
}

// AvgState implements AggregateState for AVG aggregate.
type AvgState struct {
	sum   float64
	count int64
}

// NewAvgState creates a new AvgState.
func NewAvgState() *AvgState {
	return &AvgState{}
}

// Update adds a value to the average computation.
func (a *AvgState) Update(value any) {
	if value == nil {
		return
	}
	if f, ok := toAggFloat64(value); ok {
		a.sum += f
		a.count++
	}
}

// Merge combines another AvgState into this one.
func (a *AvgState) Merge(other AggregateState) {
	if o, ok := other.(*AvgState); ok {
		a.sum += o.sum
		a.count += o.count
	}
}

// Finalize returns the final average.
func (a *AvgState) Finalize() any {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

// Clone creates a copy of the AvgState.
func (a *AvgState) Clone() AggregateState {
	return &AvgState{sum: a.sum, count: a.count}
}

// Reset clears the state.
func (a *AvgState) Reset() {
	a.sum = 0
	a.count = 0
}

// MinState implements AggregateState for MIN aggregate.
type MinState struct {
	min     any
	hasData bool
}

// NewMinState creates a new MinState.
func NewMinState() *MinState {
	return &MinState{}
}

// Update potentially updates the minimum with a new value.
func (m *MinState) Update(value any) {
	if value == nil {
		return
	}
	if !m.hasData {
		m.min = value
		m.hasData = true
		return
	}
	if compareAggValues(value, m.min) < 0 {
		m.min = value
	}
}

// Merge combines another MinState into this one.
func (m *MinState) Merge(other AggregateState) {
	if o, ok := other.(*MinState); ok {
		if o.hasData {
			m.Update(o.min)
		}
	}
}

// Finalize returns the final minimum.
func (m *MinState) Finalize() any {
	if !m.hasData {
		return nil
	}
	return m.min
}

// Clone creates a copy of the MinState.
func (m *MinState) Clone() AggregateState {
	return &MinState{min: m.min, hasData: m.hasData}
}

// Reset clears the state.
func (m *MinState) Reset() {
	m.min = nil
	m.hasData = false
}

// MaxState implements AggregateState for MAX aggregate.
type MaxState struct {
	max     any
	hasData bool
}

// NewMaxState creates a new MaxState.
func NewMaxState() *MaxState {
	return &MaxState{}
}

// Update potentially updates the maximum with a new value.
func (m *MaxState) Update(value any) {
	if value == nil {
		return
	}
	if !m.hasData {
		m.max = value
		m.hasData = true
		return
	}
	if compareAggValues(value, m.max) > 0 {
		m.max = value
	}
}

// Merge combines another MaxState into this one.
func (m *MaxState) Merge(other AggregateState) {
	if o, ok := other.(*MaxState); ok {
		if o.hasData {
			m.Update(o.max)
		}
	}
}

// Finalize returns the final maximum.
func (m *MaxState) Finalize() any {
	if !m.hasData {
		return nil
	}
	return m.max
}

// Clone creates a copy of the MaxState.
func (m *MaxState) Clone() AggregateState {
	return &MaxState{max: m.max, hasData: m.hasData}
}

// Reset clears the state.
func (m *MaxState) Reset() {
	m.max = nil
	m.hasData = false
}

// FirstState implements AggregateState for FIRST aggregate.
type FirstState struct {
	first   any
	hasData bool
}

// NewFirstState creates a new FirstState.
func NewFirstState() *FirstState {
	return &FirstState{}
}

// Update sets the first non-null value encountered.
func (f *FirstState) Update(value any) {
	if !f.hasData && value != nil {
		f.first = value
		f.hasData = true
	}
}

// Merge keeps the first state's value if it has data.
func (f *FirstState) Merge(other AggregateState) {
	if !f.hasData {
		if o, ok := other.(*FirstState); ok && o.hasData {
			f.first = o.first
			f.hasData = true
		}
	}
}

// Finalize returns the first value.
func (f *FirstState) Finalize() any {
	if !f.hasData {
		return nil
	}
	return f.first
}

// Clone creates a copy of the FirstState.
func (f *FirstState) Clone() AggregateState {
	return &FirstState{first: f.first, hasData: f.hasData}
}

// Reset clears the state.
func (f *FirstState) Reset() {
	f.first = nil
	f.hasData = false
}

// LastState implements AggregateState for LAST aggregate.
type LastState struct {
	last    any
	hasData bool
}

// NewLastState creates a new LastState.
func NewLastState() *LastState {
	return &LastState{}
}

// Update sets the value to the latest non-null value encountered.
func (l *LastState) Update(value any) {
	if value != nil {
		l.last = value
		l.hasData = true
	}
}

// Merge takes the other state's value if it has data.
func (l *LastState) Merge(other AggregateState) {
	if o, ok := other.(*LastState); ok && o.hasData {
		l.last = o.last
		l.hasData = true
	}
}

// Finalize returns the last value.
func (l *LastState) Finalize() any {
	if !l.hasData {
		return nil
	}
	return l.last
}

// Clone creates a copy of the LastState.
func (l *LastState) Clone() AggregateState {
	return &LastState{last: l.last, hasData: l.hasData}
}

// Reset clears the state.
func (l *LastState) Reset() {
	l.last = nil
	l.hasData = false
}

// NewAggregateState creates a new AggregateState for the given type.
func NewAggregateState(aggType AggregateType) AggregateState {
	switch aggType {
	case AggSum:
		return NewSumState()
	case AggCount:
		return NewCountState()
	case AggCountStar:
		return NewCountStarState()
	case AggAvg:
		return NewAvgState()
	case AggMin:
		return NewMinState()
	case AggMax:
		return NewMaxState()
	case AggFirst:
		return NewFirstState()
	case AggLast:
		return NewLastState()
	default:
		return NewCountState() // Default to count
	}
}

// AggregateFunc describes an aggregate function to compute.
type AggregateFunc struct {
	// Type is the type of aggregate (SUM, COUNT, AVG, etc.).
	Type AggregateType
	// Column is the input column index (-1 for COUNT(*)).
	Column int
	// OutputCol is the name for the output column.
	OutputCol string
	// OutputType is the output type for the aggregate.
	OutputType dukdb.Type
}

// NewAggregateFunc creates a new AggregateFunc.
func NewAggregateFunc(aggType AggregateType, column int, outputCol string) AggregateFunc {
	outputType := dukdb.TYPE_DOUBLE
	switch aggType {
	case AggSum, AggAvg:
		outputType = dukdb.TYPE_DOUBLE
	case AggCount, AggCountStar:
		outputType = dukdb.TYPE_BIGINT
	case AggMin, AggMax, AggFirst, AggLast:
		outputType = dukdb.TYPE_ANY // Will be determined at runtime
	}

	return AggregateFunc{
		Type:       aggType,
		Column:     column,
		OutputCol:  outputCol,
		OutputType: outputType,
	}
}

// AggregateEntry holds states for one group.
type AggregateEntry struct {
	// GroupKey is the tuple of group-by column values.
	GroupKey []any
	// States holds the aggregate state for each aggregate function.
	States []AggregateState
}

// NewAggregateEntry creates a new AggregateEntry for the given group key.
func NewAggregateEntry(groupKey []any, aggregates []AggregateFunc) *AggregateEntry {
	states := make([]AggregateState, len(aggregates))
	for i, agg := range aggregates {
		states[i] = NewAggregateState(agg.Type)
	}
	return &AggregateEntry{
		GroupKey: groupKey,
		States:   states,
	}
}

// Clone creates a deep copy of the AggregateEntry.
func (e *AggregateEntry) Clone() *AggregateEntry {
	groupKey := make([]any, len(e.GroupKey))
	copy(groupKey, e.GroupKey)
	states := make([]AggregateState, len(e.States))
	for i, s := range e.States {
		states[i] = s.Clone()
	}
	return &AggregateEntry{
		GroupKey: groupKey,
		States:   states,
	}
}

// AggregateHashTable is a hash table for GROUP BY aggregation.
// It maps group keys to their aggregate states.
type AggregateHashTable struct {
	// entries maps hash values to entries (for collision handling).
	entries map[uint64][]*AggregateEntry
	// aggregates defines the aggregate functions to compute.
	aggregates []AggregateFunc
	// entryCount tracks the total number of entries.
	entryCount int
	// mu protects concurrent access.
	mu sync.RWMutex
}

// NewAggregateHashTable creates a new AggregateHashTable.
func NewAggregateHashTable(aggregates []AggregateFunc) *AggregateHashTable {
	return &AggregateHashTable{
		entries:    make(map[uint64][]*AggregateEntry),
		aggregates: aggregates,
	}
}

// NewAggregateHashTableWithCapacity creates an AggregateHashTable with pre-allocated capacity.
func NewAggregateHashTableWithCapacity(
	aggregates []AggregateFunc,
	capacity int,
) *AggregateHashTable {
	return &AggregateHashTable{
		entries:    make(map[uint64][]*AggregateEntry, capacity),
		aggregates: aggregates,
	}
}

// GetOrCreate returns the entry for the given group key, creating it if necessary.
// This method is NOT thread-safe; use for single-threaded local aggregation.
func (t *AggregateHashTable) GetOrCreate(groupKey []any) *AggregateEntry {
	hash := hashGroupKey(groupKey)
	entries, ok := t.entries[hash]
	if ok {
		// Check for existing entry with matching key
		for _, entry := range entries {
			if groupKeysEqual(entry.GroupKey, groupKey) {
				return entry
			}
		}
	}
	// Create new entry
	entry := NewAggregateEntry(groupKey, t.aggregates)
	t.entries[hash] = append(entries, entry)
	t.entryCount++
	return entry
}

// GetOrCreateSafe returns the entry for the given group key with thread safety.
func (t *AggregateHashTable) GetOrCreateSafe(groupKey []any) *AggregateEntry {
	hash := hashGroupKey(groupKey)

	// Fast path: check with read lock
	t.mu.RLock()
	entries, ok := t.entries[hash]
	if ok {
		for _, entry := range entries {
			if groupKeysEqual(entry.GroupKey, groupKey) {
				t.mu.RUnlock()
				return entry
			}
		}
	}
	t.mu.RUnlock()

	// Slow path: create with write lock
	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	entries, ok = t.entries[hash]
	if ok {
		for _, entry := range entries {
			if groupKeysEqual(entry.GroupKey, groupKey) {
				return entry
			}
		}
	}

	// Create new entry
	entry := NewAggregateEntry(groupKey, t.aggregates)
	t.entries[hash] = append(entries, entry)
	t.entryCount++
	return entry
}

// Merge combines another AggregateHashTable into this one.
// This is used for the global merge phase.
func (t *AggregateHashTable) Merge(other *AggregateHashTable) error {
	if other == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for hash, otherEntries := range other.entries {
		for _, otherEntry := range otherEntries {
			// Find or create matching entry
			found := false
			entries := t.entries[hash]
			for _, entry := range entries {
				if groupKeysEqual(entry.GroupKey, otherEntry.GroupKey) {
					// Merge states
					for i, state := range entry.States {
						if i < len(otherEntry.States) {
							state.Merge(otherEntry.States[i])
						}
					}
					found = true
					break
				}
			}
			if !found {
				// Clone and add new entry
				t.entries[hash] = append(entries, otherEntry.Clone())
				t.entryCount++
			}
		}
	}

	return nil
}

// ToDataChunk converts the hash table to a DataChunk.
// groupByCols are the names of the group-by columns.
func (t *AggregateHashTable) ToDataChunk(
	groupByCols []string,
	groupByTypes []dukdb.Type,
) *storage.DataChunk {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Build output types: group columns + aggregate columns
	outputTypes := make([]dukdb.Type, len(groupByCols)+len(t.aggregates))
	copy(outputTypes, groupByTypes)
	for i, agg := range t.aggregates {
		outputTypes[len(groupByCols)+i] = agg.OutputType
	}

	// Create chunk with capacity for all entries
	chunk := storage.NewDataChunkWithCapacity(outputTypes, t.entryCount)

	// Add all entries to the chunk
	for _, entries := range t.entries {
		for _, entry := range entries {
			row := make([]any, len(groupByCols)+len(t.aggregates))
			// Copy group key values
			copy(row, entry.GroupKey)
			// Finalize aggregate values
			for i, state := range entry.States {
				row[len(groupByCols)+i] = state.Finalize()
			}
			chunk.AppendRow(row)
		}
	}

	return chunk
}

// Count returns the number of groups in the hash table.
func (t *AggregateHashTable) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.entryCount
}

// Entries returns all entries in the hash table.
func (t *AggregateHashTable) Entries() []*AggregateEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	all := make([]*AggregateEntry, 0, t.entryCount)
	for _, entries := range t.entries {
		all = append(all, entries...)
	}
	return all
}

// Clear removes all entries from the hash table.
func (t *AggregateHashTable) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = make(map[uint64][]*AggregateEntry)
	t.entryCount = 0
}

// AggregationSink handles incremental combining of local aggregation results.
// Workers combine their results into the sink as they finish.
type AggregationSink struct {
	// GlobalTable holds the merged aggregation results.
	GlobalTable *AggregateHashTable
	// mu protects concurrent access to the global table.
	mu sync.Mutex
}

// NewAggregationSink creates a new AggregationSink.
func NewAggregationSink(aggregates []AggregateFunc) *AggregationSink {
	return &AggregationSink{
		GlobalTable: NewAggregateHashTable(aggregates),
	}
}

// Combine merges a local hash table into the global table.
// This is called as each worker finishes (not at the end).
func (s *AggregationSink) Combine(local *AggregateHashTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.GlobalTable.Merge(local)
}

// ParallelAggregate executes GROUP BY with local/global merge.
// It uses a two-phase approach:
// 1. Local aggregation: Each worker aggregates into its own hash table
// 2. Global merge: Worker results are combined incrementally
type ParallelAggregate struct {
	// Source provides data chunks for aggregation.
	Source ParallelSource

	// GroupBy contains the column indices to group by.
	GroupBy []int
	// GroupByCols contains the column names of the group-by columns.
	GroupByCols []string
	// GroupByTypes contains the types of the group-by columns.
	GroupByTypes []dukdb.Type

	// Aggregates defines the aggregate functions to compute.
	Aggregates []AggregateFunc

	// Sink handles incremental combining of results.
	Sink *AggregationSink

	// LocalTables holds per-worker hash tables.
	LocalTables []*AggregateHashTable

	// HighCardinalityThreshold triggers parallel merge when exceeded.
	// If the number of groups exceeds this, parallel merge is used.
	HighCardinalityThreshold int

	// numWorkers is the number of workers to use.
	numWorkers int
}

// NewParallelAggregate creates a new ParallelAggregate.
func NewParallelAggregate(
	source ParallelSource,
	groupBy []int,
	groupByCols []string,
	groupByTypes []dukdb.Type,
	aggregates []AggregateFunc,
	numWorkers int,
) *ParallelAggregate {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Create per-worker local tables
	localTables := make([]*AggregateHashTable, numWorkers)
	for i := 0; i < numWorkers; i++ {
		localTables[i] = NewAggregateHashTable(aggregates)
	}

	return &ParallelAggregate{
		Source:                   source,
		GroupBy:                  groupBy,
		GroupByCols:              groupByCols,
		GroupByTypes:             groupByTypes,
		Aggregates:               aggregates,
		Sink:                     NewAggregationSink(aggregates),
		LocalTables:              localTables,
		HighCardinalityThreshold: 10000, // Default threshold
		numWorkers:               numWorkers,
	}
}

// Execute runs parallel aggregation and returns the result.
func (a *ParallelAggregate) Execute(
	pool *ThreadPool,
	ctx context.Context,
) (*storage.DataChunk, error) {
	if a.Source == nil {
		// No source, return empty result
		return a.emptyResult(), nil
	}

	// Generate morsels from source
	morsels := a.Source.GenerateMorsels()
	if len(morsels) == 0 {
		return a.emptyResult(), nil
	}

	// Create morsel channel
	morselChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		morselChan <- m
	}
	close(morselChan)

	// Phase 1: Local aggregation (per worker)
	var wg sync.WaitGroup
	errChan := make(chan error, pool.NumWorkers)

	for workerID := 0; workerID < pool.NumWorkers; workerID++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			a.localAggregate(wid, morselChan, ctx, errChan)
		}(workerID)
	}

	// Wait for local aggregation to complete
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Phase 2: Global merge
	// Check if we should use parallel merge
	totalGroups := 0
	for _, table := range a.LocalTables {
		totalGroups += table.Count()
	}

	if totalGroups > a.HighCardinalityThreshold && pool.NumWorkers > 1 {
		// High cardinality: use parallel merge
		return a.parallelMerge(pool.NumWorkers)
	}

	// Low cardinality: use sequential merge
	return a.Finalize()
}

// localAggregate performs local aggregation for a single worker.
func (a *ParallelAggregate) localAggregate(
	workerID int,
	morselChan <-chan Morsel,
	ctx context.Context,
	errChan chan<- error,
) {
	localTable := a.LocalTables[workerID]

	for {
		select {
		case <-ctx.Done():
			return
		case morsel, ok := <-morselChan:
			if !ok {
				// Channel closed, combine local results into sink
				if err := a.Sink.Combine(localTable); err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
				return
			}

			// Scan morsel to get chunk
			chunk, err := a.Source.Scan(morsel)
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
				// Extract group key
				groupKey := a.extractGroupKey(chunk, i)

				// Get or create entry for this group
				entry := localTable.GetOrCreate(groupKey)

				// Update aggregate states
				for j, agg := range a.Aggregates {
					var value any
					if agg.Column >= 0 {
						value = chunk.GetValue(i, agg.Column)
					}
					entry.States[j].Update(value)
				}
			}
		}
	}
}

// extractGroupKey extracts the group key values from a chunk row.
func (a *ParallelAggregate) extractGroupKey(chunk *storage.DataChunk, rowIdx int) []any {
	groupKey := make([]any, len(a.GroupBy))
	for i, colIdx := range a.GroupBy {
		groupKey[i] = chunk.GetValue(rowIdx, colIdx)
	}
	return groupKey
}

// parallelMerge performs parallel merge for high-cardinality groups.
// It partitions local tables by group key hash and merges partitions in parallel.
func (a *ParallelAggregate) parallelMerge(numMergers int) (*storage.DataChunk, error) {
	// Determine number of partitions (power of 2 for efficient masking)
	numPartitions := nextPowerOf2(numMergers)
	partitionMask := uint64(numPartitions - 1)

	// Create per-partition hash tables
	partitionTables := make([]*AggregateHashTable, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitionTables[i] = NewAggregateHashTable(a.Aggregates)
	}

	// Partition entries from all local tables
	for _, localTable := range a.LocalTables {
		for hash, entries := range localTable.entries {
			partition := int(hash & partitionMask)
			for _, entry := range entries {
				// Merge entry into partition table
				partitionEntry := partitionTables[partition].GetOrCreate(entry.GroupKey)
				for i, state := range partitionEntry.States {
					if i < len(entry.States) {
						state.Merge(entry.States[i])
					}
				}
			}
		}
	}

	// Merge partition tables into final result
	// Each partition can be merged independently (parallel)
	var wg sync.WaitGroup
	partitionChan := make(chan int, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partitionChan <- i
	}
	close(partitionChan)

	finalTable := NewAggregateHashTable(a.Aggregates)
	var mergeMu sync.Mutex

	for i := 0; i < numMergers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partition := range partitionChan {
				pt := partitionTables[partition]
				mergeMu.Lock()
				_ = finalTable.Merge(pt)
				mergeMu.Unlock()
			}
		}()
	}

	wg.Wait()

	return finalTable.ToDataChunk(a.GroupByCols, a.GroupByTypes), nil
}

// Finalize returns the final aggregated result after all workers have combined.
func (a *ParallelAggregate) Finalize() (*storage.DataChunk, error) {
	return a.Sink.GlobalTable.ToDataChunk(a.GroupByCols, a.GroupByTypes), nil
}

// emptyResult creates an empty result DataChunk with the correct schema.
func (a *ParallelAggregate) emptyResult() *storage.DataChunk {
	outputTypes := make([]dukdb.Type, len(a.GroupByCols)+len(a.Aggregates))
	copy(outputTypes, a.GroupByTypes)
	for i, agg := range a.Aggregates {
		outputTypes[len(a.GroupByCols)+i] = agg.OutputType
	}
	return storage.NewDataChunkWithCapacity(outputTypes, 0)
}

// SetHighCardinalityThreshold sets the threshold for parallel merge.
func (a *ParallelAggregate) SetHighCardinalityThreshold(threshold int) {
	a.HighCardinalityThreshold = threshold
}

// hashGroupKey computes a hash of the group key values.
func hashGroupKey(groupKey []any) uint64 {
	h := fnv.New64a()
	for _, val := range groupKey {
		hashAggValue(h, val)
	}
	return h.Sum64()
}

// hashAggValue adds a value to the hasher.
func hashAggValue(h interface{ Write([]byte) (int, error) }, val any) {
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
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
		_, _ = h.Write(buf[:])
	case float64:
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		_, _ = h.Write(buf[:])
	case string:
		_, _ = h.Write([]byte(v))
	case []byte:
		_, _ = h.Write(v)
	default:
		_, _ = h.Write([]byte{0xFF})
	}
}

// groupKeysEqual checks if two group keys are equal.
func groupKeysEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !aggValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// aggValuesEqual checks if two values are equal for grouping purposes.
func aggValuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try numeric comparison
	aFloat, aOk := toAggFloat64(a)
	bFloat, bOk := toAggFloat64(b)
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

// toAggFloat64 converts a value to float64 for aggregation.
func toAggFloat64(v any) (float64, bool) {
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

// compareAggValues compares two values for MIN/MAX aggregates.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareAggValues(a, b any) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	aFloat, aOk := toAggFloat64(a)
	bFloat, bOk := toAggFloat64(b)
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

	// String comparison
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

	// Default: treat as equal
	return 0
}

// AggregateConfig holds configuration for parallel aggregation.
type AggregateConfig struct {
	// HighCardinalityThreshold triggers parallel merge when exceeded.
	HighCardinalityThreshold int
	// NumWorkers is the number of workers for local aggregation.
	NumWorkers int
}

// DefaultAggregateConfig returns the default aggregation configuration.
func DefaultAggregateConfig() AggregateConfig {
	return AggregateConfig{
		HighCardinalityThreshold: 10000,
		NumWorkers:               0, // Use pool default
	}
}

// SimpleAggregation performs aggregation without grouping (whole-table aggregate).
// This is more efficient when there are no GROUP BY columns.
type SimpleAggregation struct {
	// Source provides data chunks for aggregation.
	Source ParallelSource
	// Aggregates defines the aggregate functions to compute.
	Aggregates []AggregateFunc
	// States holds the aggregate states (merged sequentially after parallel phase).
	States []AggregateState
}

// NewSimpleAggregation creates a new SimpleAggregation.
func NewSimpleAggregation(source ParallelSource, aggregates []AggregateFunc) *SimpleAggregation {
	states := make([]AggregateState, len(aggregates))
	for i, agg := range aggregates {
		states[i] = NewAggregateState(agg.Type)
	}
	return &SimpleAggregation{
		Source:     source,
		Aggregates: aggregates,
		States:     states,
	}
}

// Execute runs simple aggregation and returns the result.
func (a *SimpleAggregation) Execute(
	pool *ThreadPool,
	ctx context.Context,
) (*storage.DataChunk, error) {
	if a.Source == nil {
		return a.emptyResult(), nil
	}

	// Generate morsels
	morsels := a.Source.GenerateMorsels()
	if len(morsels) == 0 {
		return a.emptyResult(), nil
	}

	// Create morsel channel
	morselChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		morselChan <- m
	}
	close(morselChan)

	// Create per-worker local states
	localStates := make([][]AggregateState, pool.NumWorkers)
	for i := 0; i < pool.NumWorkers; i++ {
		localStates[i] = make([]AggregateState, len(a.Aggregates))
		for j, agg := range a.Aggregates {
			localStates[i][j] = NewAggregateState(agg.Type)
		}
	}

	// Process morsels in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, pool.NumWorkers)

	for workerID := 0; workerID < pool.NumWorkers; workerID++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case morsel, ok := <-morselChan:
					if !ok {
						return
					}

					chunk, err := a.Source.Scan(morsel)
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

					// Update local states
					for i := 0; i < chunk.Count(); i++ {
						for j, agg := range a.Aggregates {
							var value any
							if agg.Column >= 0 {
								value = chunk.GetValue(i, agg.Column)
							}
							localStates[wid][j].Update(value)
						}
					}
				}
			}
		}(workerID)
	}

	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	// Merge all local states into global states
	for _, ls := range localStates {
		for i, state := range ls {
			a.States[i].Merge(state)
		}
	}

	// Build result
	return a.buildResult(), nil
}

// buildResult creates the result DataChunk.
func (a *SimpleAggregation) buildResult() *storage.DataChunk {
	types := make([]dukdb.Type, len(a.Aggregates))
	for i, agg := range a.Aggregates {
		types[i] = agg.OutputType
	}

	chunk := storage.NewDataChunkWithCapacity(types, 1)
	row := make([]any, len(a.Aggregates))
	for i, state := range a.States {
		row[i] = state.Finalize()
	}
	chunk.AppendRow(row)

	return chunk
}

// emptyResult creates an empty result.
func (a *SimpleAggregation) emptyResult() *storage.DataChunk {
	types := make([]dukdb.Type, len(a.Aggregates))
	for i, agg := range a.Aggregates {
		types[i] = agg.OutputType
	}
	return storage.NewDataChunkWithCapacity(types, 0)
}
