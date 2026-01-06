// Package parallel provides parallel query execution infrastructure.
// This file implements the pipeline execution model with pipeline breakers,
// exchange operators, and a pipeline compiler that transforms physical plans
// into executable pipelines.
package parallel

import (
	"context"
	"fmt"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ColumnDef describes a column in a pipeline schema.
type ColumnDef struct {
	Name string
	Type dukdb.Type
}

// PipelineSource produces chunks for a pipeline.
// This extends the existing ParallelSource with schema information.
type PipelineSource interface {
	// GenerateMorsels returns all morsels that can be processed in parallel.
	GenerateMorsels() []Morsel
	// Scan reads data for the given morsel and returns a DataChunk.
	Scan(morsel Morsel) (*storage.DataChunk, error)
	// Schema returns the output schema for this source.
	Schema() []ColumnDef
}

// PipelineSinkInterface is the interface for pipeline sinks.
// It extends the basic PipelineSink with finalization.
type PipelineSinkInterface interface {
	// Combine adds processed data to the sink. Must be thread-safe.
	Combine(chunk *storage.DataChunk) error
	// Finalize returns the final result after all chunks have been combined.
	Finalize() (*storage.DataChunk, error)
}

// PipelineEvent coordinates pipeline dependencies.
// It allows pipelines to wait for other pipelines to complete before starting.
type PipelineEvent struct {
	Name         string
	Dependencies []*PipelineEvent
	completed    chan struct{}
	once         sync.Once
}

// NewPipelineEvent creates a new PipelineEvent with the given name and dependencies.
func NewPipelineEvent(name string, deps ...*PipelineEvent) *PipelineEvent {
	return &PipelineEvent{
		Name:         name,
		Dependencies: deps,
		completed:    make(chan struct{}),
	}
}

// Complete marks this event as completed.
// This is idempotent - calling it multiple times has no effect.
func (e *PipelineEvent) Complete() {
	e.once.Do(func() {
		close(e.completed)
	})
}

// Wait blocks until all dependencies have completed.
func (e *PipelineEvent) Wait() {
	for _, dep := range e.Dependencies {
		<-dep.completed
	}
}

// WaitContext blocks until all dependencies have completed or context is cancelled.
func (e *PipelineEvent) WaitContext(ctx context.Context) error {
	for _, dep := range e.Dependencies {
		select {
		case <-dep.completed:
			// Dependency completed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// IsComplete returns whether this event has been completed.
func (e *PipelineEvent) IsComplete() bool {
	select {
	case <-e.completed:
		return true
	default:
		return false
	}
}

// PipelineBreaker indicates operators that materialize intermediate results.
// These operators cannot stream data and must collect all input before producing output.
type PipelineBreaker interface {
	// BreakPipeline returns true if this operator breaks the pipeline.
	BreakPipeline() bool
}

// Pipeline represents a sequence of operators that can execute in parallel.
// A pipeline consists of a source, zero or more operators, and an optional sink.
type Pipeline struct {
	// ID is a unique identifier for this pipeline.
	ID int
	// Name is a human-readable name for debugging.
	Name string
	// Source produces data chunks for processing.
	Source PipelineSource
	// Operators transform data chunks in sequence.
	Operators []PipelineOp
	// Sink consumes the final output chunks.
	Sink PipelineSinkInterface
	// Parallel indicates whether this pipeline can run in parallel.
	Parallel bool
	// Dependencies are events to wait for before starting this pipeline.
	Dependencies []*PipelineEvent
	// CompletionEvent is signaled when this pipeline completes.
	CompletionEvent *PipelineEvent
	// Schema is the output schema of this pipeline.
	Schema []ColumnDef
}

// NewPipeline creates a new Pipeline with the given ID and name.
func NewPipeline(id int, name string) *Pipeline {
	return &Pipeline{
		ID:              id,
		Name:            name,
		Parallel:        true, // Default to parallel execution
		Operators:       make([]PipelineOp, 0),
		CompletionEvent: NewPipelineEvent(name),
	}
}

// SetSource sets the source for this pipeline.
func (p *Pipeline) SetSource(source PipelineSource) {
	p.Source = source
	if source != nil {
		p.Schema = source.Schema()
	}
}

// AddOperator adds an operator to the end of the pipeline.
func (p *Pipeline) AddOperator(op PipelineOp) {
	p.Operators = append(p.Operators, op)
}

// SetSink sets the sink for this pipeline.
func (p *Pipeline) SetSink(sink PipelineSinkInterface) {
	p.Sink = sink
}

// AddDependency adds a pipeline event dependency.
func (p *Pipeline) AddDependency(event *PipelineEvent) {
	p.Dependencies = append(p.Dependencies, event)
}

// Execute runs the pipeline to completion using the given thread pool.
func (p *Pipeline) Execute(pool *ThreadPool, ctx context.Context) error {
	// Wait for dependencies first
	for _, dep := range p.Dependencies {
		if err := dep.WaitContext(ctx); err != nil {
			return err
		}
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// If no source, nothing to do
	if p.Source == nil {
		p.CompletionEvent.Complete()
		return nil
	}

	// Generate morsels
	morsels := p.Source.GenerateMorsels()
	if len(morsels) == 0 {
		p.CompletionEvent.Complete()
		return nil
	}

	var err error
	if p.Parallel && pool.NumWorkers > 1 {
		err = p.executeParallel(pool, ctx, morsels)
	} else {
		err = p.executeSequential(ctx, morsels)
	}

	// Signal completion
	p.CompletionEvent.Complete()
	return err
}

// executeParallel runs the pipeline with multiple workers.
func (p *Pipeline) executeParallel(pool *ThreadPool, ctx context.Context, morsels []Morsel) error {
	// Create work channel
	workChan := make(chan Morsel, len(morsels))
	for _, m := range morsels {
		workChan <- m
	}
	close(workChan)

	// Create error channel
	errChan := make(chan error, pool.NumWorkers)

	var wg sync.WaitGroup
	for workerID := 0; workerID < pool.NumWorkers; workerID++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			localState := make(map[int]any)

			for {
				select {
				case <-ctx.Done():
					return
				case morsel, ok := <-workChan:
					if !ok {
						return
					}

					// Scan morsel
					chunk, err := p.Source.Scan(morsel)
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}

					if chunk == nil || chunk.Count() == 0 {
						continue
					}

					// Process through operators
					for _, op := range p.Operators {
						chunk, err = op.Execute(localState, chunk)
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

					// Send to sink if we have output
					if chunk != nil && chunk.Count() > 0 && p.Sink != nil {
						if err := p.Sink.Combine(chunk); err != nil {
							select {
							case errChan <- err:
							default:
							}
							return
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
		return err
	default:
		return nil
	}
}

// executeSequential runs the pipeline with a single thread.
func (p *Pipeline) executeSequential(ctx context.Context, morsels []Morsel) error {
	localState := make(map[int]any)

	for _, morsel := range morsels {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Scan morsel
		chunk, err := p.Source.Scan(morsel)
		if err != nil {
			return err
		}

		if chunk == nil || chunk.Count() == 0 {
			continue
		}

		// Process through operators
		for _, op := range p.Operators {
			chunk, err = op.Execute(localState, chunk)
			if err != nil {
				return err
			}
			if chunk == nil || chunk.Count() == 0 {
				break
			}
		}

		// Send to sink
		if chunk != nil && chunk.Count() > 0 && p.Sink != nil {
			if err := p.Sink.Combine(chunk); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetResult retrieves the final result from the pipeline's sink.
func (p *Pipeline) GetResult() (*storage.DataChunk, error) {
	if p.Sink == nil {
		return nil, nil
	}
	return p.Sink.Finalize()
}

// ExchangeType represents the type of data exchange between pipelines.
type ExchangeType int

const (
	// ExchangeGather collects data from multiple sources to one destination (many-to-one).
	ExchangeGather ExchangeType = iota
	// ExchangeScatter distributes data from one source to multiple destinations (one-to-many).
	ExchangeScatter
	// ExchangeRepartition redistributes data by hash key (many-to-many).
	ExchangeRepartition
	// ExchangeBroadcast duplicates data to all destinations.
	ExchangeBroadcast
)

// String returns the string representation of the exchange type.
func (t ExchangeType) String() string {
	switch t {
	case ExchangeGather:
		return "GATHER"
	case ExchangeScatter:
		return "SCATTER"
	case ExchangeRepartition:
		return "REPARTITION"
	case ExchangeBroadcast:
		return "BROADCAST"
	default:
		return "UNKNOWN"
	}
}

// ExchangeOp redistributes data between pipelines.
// It implements the PipelineOp interface.
type ExchangeOp struct {
	// Type specifies the exchange pattern.
	Type ExchangeType
	// Partitions is the number of output partitions.
	Partitions int
	// KeyColumns are the column indices used for hash partitioning.
	KeyColumns []int
	// OutputQueues are channels for sending data to partitions.
	OutputQueues []chan *storage.DataChunk
	// mu protects concurrent access to partition state.
	mu sync.Mutex
}

// NewExchangeOp creates a new ExchangeOp.
func NewExchangeOp(exchangeType ExchangeType, partitions int, keyColumns []int) *ExchangeOp {
	return &ExchangeOp{
		Type:         exchangeType,
		Partitions:   partitions,
		KeyColumns:   keyColumns,
		OutputQueues: make([]chan *storage.DataChunk, partitions),
	}
}

// Name returns the operator name.
func (e *ExchangeOp) Name() string {
	return fmt.Sprintf("Exchange(%s)", e.Type.String())
}

// Execute processes input chunk and redistributes according to exchange type.
func (e *ExchangeOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return input, nil
	}

	switch e.Type {
	case ExchangeGather:
		// Gather just passes through - actual gathering happens at sink
		return input, nil

	case ExchangeScatter:
		// Scatter distributes rows round-robin to partitions
		return e.scatter(input), nil

	case ExchangeRepartition:
		// Repartition by hash of key columns
		return e.repartition(input), nil

	case ExchangeBroadcast:
		// Broadcast sends copy to all partitions (returns clone for this worker)
		return input.Clone(), nil

	default:
		return input, nil
	}
}

// scatter distributes rows round-robin across partitions.
func (e *ExchangeOp) scatter(input *storage.DataChunk) *storage.DataChunk {
	// For scatter, we keep the chunk as-is for the current partition
	// and just return it. Real scattering happens via OutputQueues.
	return input
}

// repartition redistributes data by hash of key columns.
func (e *ExchangeOp) repartition(input *storage.DataChunk) *storage.DataChunk {
	if len(e.KeyColumns) == 0 || e.Partitions <= 1 {
		return input
	}

	// Create per-partition chunks
	types := input.Types()
	partitionChunks := make([]*storage.DataChunk, e.Partitions)
	for i := 0; i < e.Partitions; i++ {
		partitionChunks[i] = storage.NewDataChunkWithCapacity(types, input.Count()/e.Partitions+1)
	}

	// Hash each row and assign to partition
	partitionMask := uint64(e.Partitions - 1)
	for row := 0; row < input.Count(); row++ {
		// Extract key values
		keyVals := make([]any, len(e.KeyColumns))
		for i, col := range e.KeyColumns {
			keyVals[i] = input.GetValue(row, col)
		}

		// Hash and assign to partition
		hash := hashGroupKey(keyVals)
		partition := int(hash & partitionMask)

		// Copy row to partition chunk
		rowVals := make([]any, input.ColumnCount())
		for col := 0; col < input.ColumnCount(); col++ {
			rowVals[col] = input.GetValue(row, col)
		}
		partitionChunks[partition].AppendRow(rowVals)
	}

	// Send to output queues if configured
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, chunk := range partitionChunks {
		if chunk.Count() > 0 && e.OutputQueues[i] != nil {
			select {
			case e.OutputQueues[i] <- chunk:
			default:
				// Queue full, keep for local processing
			}
		}
	}

	// Return first non-empty chunk for this worker (or empty chunk)
	for _, chunk := range partitionChunks {
		if chunk.Count() > 0 {
			return chunk
		}
	}

	return storage.NewDataChunkWithCapacity(types, 0)
}

// FilterOp applies a filter predicate to chunks.
// It implements the PipelineOp interface.
type FilterOp struct {
	Filter FilterExpr
}

// NewFilterOp creates a new FilterOp.
func NewFilterOp(filter FilterExpr) *FilterOp {
	return &FilterOp{Filter: filter}
}

// Name returns the operator name.
func (f *FilterOp) Name() string {
	return "Filter"
}

// Execute applies the filter to the input chunk.
func (f *FilterOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if f.Filter == nil || input == nil {
		return input, nil
	}
	return ApplyFilter(input, f.Filter), nil
}

// ProjectOp applies column projection to chunks.
// It implements the PipelineOp interface.
type ProjectOp struct {
	// Projections are the column indices to project.
	Projections []int
}

// NewProjectOp creates a new ProjectOp.
func NewProjectOp(projections []int) *ProjectOp {
	return &ProjectOp{Projections: projections}
}

// Name returns the operator name.
func (p *ProjectOp) Name() string {
	return "Project"
}

// Execute applies the projection to the input chunk.
func (p *ProjectOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if len(p.Projections) == 0 || input == nil {
		return input, nil
	}
	return ApplyProjection(input, p.Projections), nil
}

// LimitOp applies LIMIT/OFFSET to chunks.
// It implements the PipelineOp interface.
type LimitOp struct {
	Limit  int
	Offset int
	// State key for tracking rows seen
	stateKey int
}

// NewLimitOp creates a new LimitOp.
func NewLimitOp(limit, offset int) *LimitOp {
	return &LimitOp{
		Limit:    limit,
		Offset:   offset,
		stateKey: generateStateKey(),
	}
}

// Name returns the operator name.
func (l *LimitOp) Name() string {
	return fmt.Sprintf("Limit(%d, %d)", l.Limit, l.Offset)
}

// Execute applies limit and offset to the input chunk.
func (l *LimitOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return input, nil
	}

	// Get or initialize row counter
	rowsSeen := 0
	if v, ok := state[l.stateKey]; ok {
		rowsSeen = v.(int)
	}

	// If we've already output enough rows, return empty
	if l.Limit > 0 && rowsSeen >= l.Offset+l.Limit {
		return storage.NewDataChunkWithCapacity(input.Types(), 0), nil
	}

	count := input.Count()
	startRow := 0
	endRow := count

	// Apply offset
	if rowsSeen < l.Offset {
		rowsToSkip := l.Offset - rowsSeen
		if rowsToSkip >= count {
			// Skip entire chunk
			state[l.stateKey] = rowsSeen + count
			return storage.NewDataChunkWithCapacity(input.Types(), 0), nil
		}
		startRow = rowsToSkip
	}

	// Apply limit
	if l.Limit > 0 {
		rowsAvailable := rowsSeen + count - l.Offset
		if rowsAvailable > l.Limit {
			rowsToTake := l.Limit - (rowsSeen - l.Offset)
			if rowsSeen < l.Offset {
				rowsToTake = l.Limit
			}
			endRow = startRow + rowsToTake
			if endRow > count {
				endRow = count
			}
		}
	}

	// Update row counter
	state[l.stateKey] = rowsSeen + count

	// If taking all rows, return as-is
	if startRow == 0 && endRow == count {
		return input, nil
	}

	// Create new chunk with limited rows
	result := storage.NewDataChunkWithCapacity(input.Types(), endRow-startRow)
	for row := startRow; row < endRow; row++ {
		rowVals := make([]any, input.ColumnCount())
		for col := 0; col < input.ColumnCount(); col++ {
			rowVals[col] = input.GetValue(row, col)
		}
		result.AppendRow(rowVals)
	}

	return result, nil
}

var stateKeyCounter int
var stateKeyMu sync.Mutex

func generateStateKey() int {
	stateKeyMu.Lock()
	defer stateKeyMu.Unlock()
	stateKeyCounter++
	return stateKeyCounter
}

// ChunkSink collects chunks into a slice.
// It implements the PipelineSinkInterface.
type ChunkSink struct {
	chunks []*storage.DataChunk
	mu     sync.Mutex
}

// NewChunkSink creates a new ChunkSink.
func NewChunkSink() *ChunkSink {
	return &ChunkSink{
		chunks: make([]*storage.DataChunk, 0),
	}
}

// Combine adds a chunk to the sink.
func (s *ChunkSink) Combine(chunk *storage.DataChunk) error {
	if chunk == nil || chunk.Count() == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, chunk.Clone())
	return nil
}

// Finalize returns all collected chunks merged into one.
func (s *ChunkSink) Finalize() (*storage.DataChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.chunks) == 0 {
		return nil, nil
	}

	// Calculate total rows and get types
	totalRows := 0
	for _, chunk := range s.chunks {
		totalRows += chunk.Count()
	}

	if totalRows == 0 {
		return nil, nil
	}

	types := s.chunks[0].Types()
	result := storage.NewDataChunkWithCapacity(types, totalRows)

	// Merge all chunks
	for _, chunk := range s.chunks {
		for row := 0; row < chunk.Count(); row++ {
			rowVals := make([]any, chunk.ColumnCount())
			for col := 0; col < chunk.ColumnCount(); col++ {
				rowVals[col] = chunk.GetValue(row, col)
			}
			result.AppendRow(rowVals)
		}
	}

	return result, nil
}

// Chunks returns the collected chunks without merging.
func (s *ChunkSink) Chunks() []*storage.DataChunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunks
}

// PipelineCompiler transforms a physical plan into executable pipelines.
type PipelineCompiler struct {
	NumWorkers   int
	nextID       int
	pipelines    []*Pipeline
	currentPipe  *Pipeline
	breakerStack []*Pipeline
}

// NewPipelineCompiler creates a new PipelineCompiler.
func NewPipelineCompiler(numWorkers int) *PipelineCompiler {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	return &PipelineCompiler{
		NumWorkers: numWorkers,
		pipelines:  make([]*Pipeline, 0),
	}
}

// Compile transforms a physical plan into a list of pipelines.
// The plan parameter should be a PhysicalPlan from the planner package.
func (c *PipelineCompiler) Compile(plan interface{}) ([]*Pipeline, error) {
	c.pipelines = make([]*Pipeline, 0)
	c.nextID = 0

	// Start a new pipeline
	c.currentPipe = c.newPipeline("main")

	// Visit the plan tree
	if err := c.visit(plan); err != nil {
		return nil, err
	}

	// Finalize current pipeline if it has content
	if c.currentPipe != nil && (c.currentPipe.Source != nil || len(c.currentPipe.Operators) > 0) {
		c.pipelines = append(c.pipelines, c.currentPipe)
	}

	return c.pipelines, nil
}

// newPipeline creates a new pipeline with an auto-generated ID.
func (c *PipelineCompiler) newPipeline(name string) *Pipeline {
	id := c.nextID
	c.nextID++
	return NewPipeline(id, fmt.Sprintf("%s_%d", name, id))
}

// visit recursively processes plan nodes.
func (c *PipelineCompiler) visit(plan interface{}) error {
	if plan == nil {
		return nil
	}

	// Check if this node is a pipeline breaker
	if breaker, ok := plan.(PipelineBreaker); ok && breaker.BreakPipeline() {
		return c.handleBreaker(plan)
	}

	// Handle different plan types
	switch node := plan.(type) {
	case interface{ Children() []interface{} }:
		// Process children first (bottom-up)
		children := node.Children()
		for _, child := range children {
			if err := c.visit(child); err != nil {
				return err
			}
		}

	default:
		// Node has no children, treat as leaf
	}

	return nil
}

// handleBreaker handles pipeline breaker nodes.
func (c *PipelineCompiler) handleBreaker(plan interface{}) error {
	// Save current pipeline
	prevPipe := c.currentPipe
	c.pipelines = append(c.pipelines, prevPipe)

	// Create new pipeline after the breaker
	c.currentPipe = c.newPipeline("post_break")

	// Add dependency from new pipeline to the breaker pipeline
	c.currentPipe.AddDependency(prevPipe.CompletionEvent)

	return nil
}

// PipelineExecutor executes a list of pipelines.
type PipelineExecutor struct {
	Pool      *ThreadPool
	Pipelines []*Pipeline
}

// NewPipelineExecutor creates a new PipelineExecutor.
func NewPipelineExecutor(pool *ThreadPool, pipelines []*Pipeline) *PipelineExecutor {
	return &PipelineExecutor{
		Pool:      pool,
		Pipelines: pipelines,
	}
}

// Execute runs all pipelines respecting dependencies.
func (e *PipelineExecutor) Execute(ctx context.Context) error {
	if len(e.Pipelines) == 0 {
		return nil
	}

	// Execute pipelines concurrently, respecting dependencies
	errChan := make(chan error, len(e.Pipelines))
	var wg sync.WaitGroup

	for _, pipe := range e.Pipelines {
		wg.Add(1)
		go func(p *Pipeline) {
			defer wg.Done()
			if err := p.Execute(e.Pool, ctx); err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(pipe)
	}

	// Wait for all pipelines
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// ExecuteSequential runs pipelines one at a time in dependency order.
func (e *PipelineExecutor) ExecuteSequential(ctx context.Context) error {
	for _, pipe := range e.Pipelines {
		if err := pipe.Execute(e.Pool, ctx); err != nil {
			return err
		}
	}
	return nil
}

// GetFinalResult retrieves the result from the last pipeline.
func (e *PipelineExecutor) GetFinalResult() (*storage.DataChunk, error) {
	if len(e.Pipelines) == 0 {
		return nil, nil
	}
	return e.Pipelines[len(e.Pipelines)-1].GetResult()
}

// SortBreakerOp represents a Sort operator that breaks the pipeline.
// It collects all input chunks, sorts them, and outputs sorted chunks.
type SortBreakerOp struct {
	SortKeys   []SortKey
	chunks     []*storage.DataChunk
	sorted     bool
	sortedData *storage.DataChunk
	mu         sync.Mutex
}

// NewSortBreakerOp creates a new SortBreakerOp.
func NewSortBreakerOp(keys []SortKey) *SortBreakerOp {
	return &SortBreakerOp{
		SortKeys: keys,
		chunks:   make([]*storage.DataChunk, 0),
	}
}

// BreakPipeline returns true - Sort is a pipeline breaker.
func (s *SortBreakerOp) BreakPipeline() bool {
	return true
}

// Name returns the operator name.
func (s *SortBreakerOp) Name() string {
	return "Sort"
}

// Execute collects input chunks for sorting.
func (s *SortBreakerOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.chunks = append(s.chunks, input.Clone())
	return nil, nil
}

// Finalize sorts all collected data and returns the result.
func (s *SortBreakerOp) Finalize() (*storage.DataChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sorted {
		return s.sortedData, nil
	}

	if len(s.chunks) == 0 {
		s.sorted = true
		return nil, nil
	}

	// Merge all chunks
	types := s.chunks[0].Types()
	totalRows := 0
	for _, chunk := range s.chunks {
		totalRows += chunk.Count()
	}

	// Create single partition for sorting
	partition := NewSortedPartitionWithCapacity(s.SortKeys, totalRows)
	rowIdx := 0
	for _, chunk := range s.chunks {
		for row := 0; row < chunk.Count(); row++ {
			rowVals := make([]any, chunk.ColumnCount())
			for col := 0; col < chunk.ColumnCount(); col++ {
				rowVals[col] = chunk.GetValue(row, col)
			}
			partition.AddRow(rowVals, rowIdx)
			rowIdx++
		}
	}

	// Sort the partition
	partition.Sort()

	// Convert back to DataChunk
	s.sortedData = storage.NewDataChunkWithCapacity(types, totalRows)
	for _, sortedRow := range partition.Rows {
		s.sortedData.AppendRow(sortedRow.Row)
	}

	s.sorted = true
	return s.sortedData, nil
}

// AggregateBreakerOp represents an Aggregate operator that breaks the pipeline.
type AggregateBreakerOp struct {
	GroupBy      []int
	GroupByCols  []string
	GroupByTypes []dukdb.Type
	Aggregates   []AggregateFunc
	hashTable    *AggregateHashTable
	finalized    bool
	result       *storage.DataChunk
	mu           sync.Mutex
}

// NewAggregateBreakerOp creates a new AggregateBreakerOp.
func NewAggregateBreakerOp(groupBy []int, groupByCols []string, groupByTypes []dukdb.Type, aggregates []AggregateFunc) *AggregateBreakerOp {
	return &AggregateBreakerOp{
		GroupBy:      groupBy,
		GroupByCols:  groupByCols,
		GroupByTypes: groupByTypes,
		Aggregates:   aggregates,
		hashTable:    NewAggregateHashTable(aggregates),
	}
}

// BreakPipeline returns true - Aggregate with GROUP BY is a pipeline breaker.
func (a *AggregateBreakerOp) BreakPipeline() bool {
	return len(a.GroupBy) > 0
}

// Name returns the operator name.
func (a *AggregateBreakerOp) Name() string {
	return "Aggregate"
}

// Execute processes input chunks for aggregation.
func (a *AggregateBreakerOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return nil, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Process each row
	for row := 0; row < input.Count(); row++ {
		// Extract group key
		groupKey := make([]any, len(a.GroupBy))
		for i, colIdx := range a.GroupBy {
			groupKey[i] = input.GetValue(row, colIdx)
		}

		// Get or create entry
		entry := a.hashTable.GetOrCreate(groupKey)

		// Update aggregates
		for j, agg := range a.Aggregates {
			var value any
			if agg.Column >= 0 {
				value = input.GetValue(row, agg.Column)
			}
			entry.States[j].Update(value)
		}
	}

	return nil, nil
}

// Finalize returns the aggregated result.
func (a *AggregateBreakerOp) Finalize() (*storage.DataChunk, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.finalized {
		return a.result, nil
	}

	a.result = a.hashTable.ToDataChunk(a.GroupByCols, a.GroupByTypes)
	a.finalized = true
	return a.result, nil
}

// HashBuildOp represents the build side of a hash join.
// It breaks the pipeline because it must collect all build tuples before probe can start.
type HashBuildOp struct {
	JoinKeys   []int
	HashTables []*HashTable
	Built      bool
	mu         sync.Mutex
}

// NewHashBuildOp creates a new HashBuildOp.
func NewHashBuildOp(joinKeys []int, numPartitions int) *HashBuildOp {
	hashTables := make([]*HashTable, numPartitions)
	for i := 0; i < numPartitions; i++ {
		hashTables[i] = NewHashTable()
	}
	return &HashBuildOp{
		JoinKeys:   joinKeys,
		HashTables: hashTables,
	}
}

// BreakPipeline returns true - HashBuild is a pipeline breaker.
func (h *HashBuildOp) BreakPipeline() bool {
	return true
}

// Name returns the operator name.
func (h *HashBuildOp) Name() string {
	return "HashBuild"
}

// Execute inserts build tuples into the hash tables.
func (h *HashBuildOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return nil, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	numPartitions := len(h.HashTables)
	partitionMask := uint64(numPartitions - 1)

	for row := 0; row < input.Count(); row++ {
		// Extract key values
		keyVals := make([]any, len(h.JoinKeys))
		for i, col := range h.JoinKeys {
			keyVals[i] = input.GetValue(row, col)
		}

		// Hash and assign to partition
		hash := hashGroupKey(keyVals)
		partition := int(hash & partitionMask)

		// Extract full row
		rowData := make([]any, input.ColumnCount())
		for col := 0; col < input.ColumnCount(); col++ {
			rowData[col] = input.GetValue(row, col)
		}

		// Insert into hash table
		h.HashTables[partition].Insert(hash, rowData)
	}

	return nil, nil
}

// MarkBuilt marks the hash tables as fully built.
func (h *HashBuildOp) MarkBuilt() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Built = true
}

// WindowBreakerOp represents a Window function operator that breaks the pipeline.
type WindowBreakerOp struct {
	PartitionBy []int
	OrderBy     []SortKey
	WindowFuncs []WindowFunc
	chunks      []*storage.DataChunk
	finalized   bool
	result      *storage.DataChunk
	mu          sync.Mutex
}

// WindowFunc describes a window function to compute.
type WindowFunc struct {
	Type       string // ROW_NUMBER, RANK, DENSE_RANK, SUM, AVG, etc.
	Column     int    // Input column for aggregates
	OutputCol  string
	OutputType dukdb.Type
}

// NewWindowBreakerOp creates a new WindowBreakerOp.
func NewWindowBreakerOp(partitionBy []int, orderBy []SortKey, funcs []WindowFunc) *WindowBreakerOp {
	return &WindowBreakerOp{
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
		WindowFuncs: funcs,
		chunks:      make([]*storage.DataChunk, 0),
	}
}

// BreakPipeline returns true - Window functions are pipeline breakers.
func (w *WindowBreakerOp) BreakPipeline() bool {
	return true
}

// Name returns the operator name.
func (w *WindowBreakerOp) Name() string {
	return "Window"
}

// Execute collects input chunks for window processing.
func (w *WindowBreakerOp) Execute(state map[int]any, input *storage.DataChunk) (*storage.DataChunk, error) {
	if input == nil || input.Count() == 0 {
		return nil, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.chunks = append(w.chunks, input.Clone())
	return nil, nil
}

// Finalize processes window functions and returns the result.
func (w *WindowBreakerOp) Finalize() (*storage.DataChunk, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.finalized {
		return w.result, nil
	}

	// Window function implementation is complex
	// For now, return merged chunks without window computation
	// Full implementation would partition, sort, and compute window values
	if len(w.chunks) == 0 {
		w.finalized = true
		return nil, nil
	}

	// Merge chunks
	types := w.chunks[0].Types()
	totalRows := 0
	for _, chunk := range w.chunks {
		totalRows += chunk.Count()
	}

	w.result = storage.NewDataChunkWithCapacity(types, totalRows)
	for _, chunk := range w.chunks {
		for row := 0; row < chunk.Count(); row++ {
			rowVals := make([]any, chunk.ColumnCount())
			for col := 0; col < chunk.ColumnCount(); col++ {
				rowVals[col] = chunk.GetValue(row, col)
			}
			w.result.AppendRow(rowVals)
		}
	}

	w.finalized = true
	return w.result, nil
}

// Note: nextPowerOf2 is defined in hash_join.go
