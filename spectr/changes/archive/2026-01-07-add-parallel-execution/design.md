# Design: Parallel Query Execution

This document captures architectural decisions for implementing parallel query execution in dukdb-go using Go's native concurrency primitives.

## Architecture Overview

```
Query
    ↓
Cost-Based Optimizer (adds parallelism hints)
    ↓
Pipeline Compiler
    ├─→ Pipeline 1: Scan → Filter → Project
    ├─→ Pipeline 2: HashBuild (parallel)
    └─→ Pipeline 3: HashProbe → Aggregate → Output
    ↓
Parallel Scheduler
    ├─→ Worker 1 (goroutine)
    ├─→ Worker 2 (goroutine)
    └─→ Worker N (goroutine)
    ↓
Results
```

---

## Decision 1: Execution Model - Task-Batched Pipeline Parallelism

### Context
DuckDB uses a pipeline execution model where work is divided into chunks (morsels) that workers can process independently.

> **Note**: This is inspired by DuckDB's pipeline model but adapted for Go's channel-based concurrency. DuckDB uses event-based task scheduling; dukdb-go uses a simpler channel-driven approach suitable for Go's goroutine model.

### Decision
Implement task-batched pipeline parallelism using Go channels and goroutines:

```go
// Morsel represents a unit of parallel work
type Morsel struct {
    TableID   uint64
    StartRow  uint64
    EndRow    uint64
    RowGroup  int
}

// ParallelPipeline represents an executable parallel pipeline
type ParallelPipeline struct {
    Source       ParallelSource   // Produces morsels
    Operators    []PipelineOp     // Transform operators
    Sink         PipelineSink     // Final aggregation
    Parallelism  int              // Number of workers
}

// Worker processes morsels from the source
type Worker struct {
    ID          int
    Arena       *MemoryArena    // Per-worker memory
    LocalState  map[int]any     // Thread-local operator state
}

func (w *Worker) Run(ctx context.Context, morselChan <-chan Morsel, pipeline *ParallelPipeline) error {
    for morsel := range morselChan {
        chunk, err := pipeline.Source.Scan(morsel)
        if err != nil {
            return err
        }

        // Process through pipeline operators
        for _, op := range pipeline.Operators {
            chunk, err = op.Execute(w.LocalState, chunk)
            if err != nil {
                return err
            }
        }

        // Send to sink
        if err := pipeline.Sink.Combine(chunk); err != nil {
            return err
        }
    }
    return nil
}
```

### Rationale
- Task-batched execution provides good load balancing
- Workers can steal work when their queue is empty
- Natural fit for Go's channel-based concurrency
- DuckDB has proven this model scales well

---

## Decision 1.5: Pipeline Dependencies and Synchronization

### Context
Pipelines may have dependencies on each other - for example, a hash join's probe phase cannot start until the build phase completes. We need explicit coordination to ensure correctness.

### Decision
Implement a PipelineEvent system to coordinate pipeline dependencies:

```go
// PipelineEvent coordinates pipeline dependencies
type PipelineEvent struct {
    Name         string
    Dependencies []*PipelineEvent
    completed    chan struct{}
}

func NewPipelineEvent(name string, deps ...*PipelineEvent) *PipelineEvent {
    return &PipelineEvent{
        Name:         name,
        Dependencies: deps,
        completed:    make(chan struct{}),
    }
}

func (e *PipelineEvent) Complete() {
    close(e.completed)
}

func (e *PipelineEvent) Wait() {
    for _, dep := range e.Dependencies {
        <-dep.completed
    }
}
```

This ensures build phase completes before probe phase starts.

### Example Usage
```go
// Hash join with explicit pipeline dependencies
buildEvent := NewPipelineEvent("hash_build")
probeEvent := NewPipelineEvent("hash_probe", buildEvent)

// Build pipeline
go func() {
    executeBuildPipeline()
    buildEvent.Complete()
}()

// Probe pipeline waits for build
go func() {
    probeEvent.Wait() // Blocks until build completes
    executeProbePipeline()
    probeEvent.Complete()
}()
```

### Rationale
- Explicit synchronization prevents race conditions between pipeline phases
- Channel-based coordination fits Go's concurrency model
- Simple API that is easy to reason about
- Supports arbitrary dependency graphs for complex queries

---

## Decision 2: Thread Pool and Work Stealing

### Context
Go's goroutine scheduler is efficient, but we need controlled parallelism and work distribution.

### Decision
Implement a fixed-size worker pool with channel-based work distribution:

```go
// ThreadPool manages parallel execution workers
type ThreadPool struct {
    NumWorkers  int
    Workers     []*Worker
    WorkChan    chan Morsel
    ResultChan  chan *DataChunk
    ErrChan     chan error
    wg          sync.WaitGroup
}

func NewThreadPool(numWorkers int) *ThreadPool {
    if numWorkers <= 0 {
        numWorkers = runtime.GOMAXPROCS(0)
    }

    pool := &ThreadPool{
        NumWorkers:  numWorkers,
        Workers:     make([]*Worker, numWorkers),
        WorkChan:    make(chan Morsel, numWorkers*2), // Buffered for prefetch
        ResultChan:  make(chan *DataChunk, numWorkers*2),
        ErrChan:     make(chan error, numWorkers),
    }

    // Create workers with per-worker memory arenas
    for i := 0; i < numWorkers; i++ {
        pool.Workers[i] = &Worker{
            ID:         i,
            Arena:      NewMemoryArena(64 * 1024 * 1024), // 64MB per worker
            LocalState: make(map[int]any),
        }
    }

    return pool
}

func (p *ThreadPool) Execute(ctx context.Context, pipeline *ParallelPipeline) error {
    // Start workers
    for _, worker := range p.Workers {
        p.wg.Add(1)
        go func(w *Worker) {
            defer p.wg.Done()
            if err := w.Run(ctx, p.WorkChan, pipeline); err != nil {
                select {
                case p.ErrChan <- err:
                default:
                }
            }
        }(worker)
    }

    // Distribute morsels
    go p.distributeMorsels(ctx, pipeline.Source)

    // Wait for completion
    p.wg.Wait()
    close(p.ResultChan)

    // Check for errors
    select {
    case err := <-p.ErrChan:
        return err
    default:
        return nil
    }
}
```

### Configuration
```go
const (
    DefaultParallelism    = 0     // Use GOMAXPROCS
    MinMorselSize         = 1024  // Minimum rows per morsel
    MaxMorselSize         = 122880 // One row group
)
```

### Rationale
- Fixed worker count prevents goroutine explosion
- Buffered channels enable prefetching
- Per-worker arenas reduce memory contention
- Context cancellation enables clean shutdown

---

## Decision 3: Parallel Table Scan

### Context
Table scans are embarrassingly parallel - each row group can be scanned independently.

### Decision
Implement row group-based parallel scanning:

```go
// ParallelTableScan partitions table into morsels
type ParallelTableScan struct {
    Table       *TableDef
    Projections []int
    Filter      BoundExpr
    RowGroups   []RowGroupMeta
}

func (s *ParallelTableScan) GenerateMorsels() []Morsel {
    var morsels []Morsel

    for i, rg := range s.RowGroups {
        // One morsel per row group
        morsels = append(morsels, Morsel{
            TableID:  s.Table.OID,
            RowGroup: i,
            StartRow: rg.StartRow,
            EndRow:   rg.StartRow + rg.RowCount,
        })
    }

    return morsels
}

func (s *ParallelTableScan) Scan(morsel Morsel) (*DataChunk, error) {
    // Read specific row group
    rg := s.RowGroups[morsel.RowGroup]

    // Apply projection pushdown
    chunk, err := rg.Read(s.Projections)
    if err != nil {
        return nil, err
    }

    // Apply filter if present
    if s.Filter != nil {
        chunk = applyFilter(chunk, s.Filter)
    }

    return chunk, nil
}
```

### Rationale
- Row groups are natural partition boundaries
- Filter pushdown reduces data movement
- Projection pushdown reduces memory bandwidth

---

## Decision 4: Parallel Hash Join

### Context
Hash joins are the most important operator to parallelize for analytical queries.

### Decision
Implement radix-partitioned parallel hash join with explicit phase synchronization:

```go
// ParallelHashJoin executes hash join with partition parallelism
type ParallelHashJoin struct {
    BuildSide     ParallelSource
    ProbeSide     ParallelSource
    JoinKey       []int
    JoinType      JoinType
    NumPartitions int                  // Power of 2 for radix partitioning
    HashTables    []*HashTable         // One per partition
    PartitionQueues [][]PartitionEntry // Per-partition build data
    buildWg       sync.WaitGroup
    partitionWg   sync.WaitGroup
}

// PartitionEntry holds a row destined for a specific partition
type PartitionEntry struct {
    Hash uint64
    Row  Row
}

// Phase 1: Workers partition build-side data into per-partition queues (radix)
func (j *ParallelHashJoin) PartitionBuildData(workers []*Worker) error {
    morsels := j.BuildSide.GenerateMorsels()

    // Each worker gets its own local partition buffers to avoid contention
    localPartitions := make([][][]PartitionEntry, len(workers))
    for w := range workers {
        localPartitions[w] = make([][]PartitionEntry, j.NumPartitions)
        for p := 0; p < j.NumPartitions; p++ {
            localPartitions[w][p] = make([]PartitionEntry, 0)
        }
    }

    // Workers process morsels and partition locally (no locks needed)
    var wg sync.WaitGroup
    morselChan := make(chan Morsel, len(morsels))
    for _, m := range morsels {
        morselChan <- m
    }
    close(morselChan)

    for wIdx, worker := range workers {
        wg.Add(1)
        go func(w *Worker, wid int) {
            defer wg.Done()
            for morsel := range morselChan {
                chunk, _ := j.BuildSide.Scan(morsel)
                for i := 0; i < chunk.Count; i++ {
                    hash := hashJoinKey(chunk, j.JoinKey, i)
                    partition := hash & uint64(j.NumPartitions - 1)
                    localPartitions[wid][partition] = append(
                        localPartitions[wid][partition],
                        PartitionEntry{Hash: hash, Row: chunk.GetRow(i)},
                    )
                }
            }
        }(worker, wIdx)
    }
    wg.Wait() // Synchronization barrier

    // Merge local partitions into global partition queues (single pass)
    for p := 0; p < j.NumPartitions; p++ {
        for w := range workers {
            j.PartitionQueues[p] = append(j.PartitionQueues[p], localPartitions[w][p]...)
        }
    }

    return nil
}

// Phase 2: Workers build hash tables from partitioned data (lock-free per partition)
func (j *ParallelHashJoin) BuildHashTables(workers []*Worker) error {
    // Each worker handles a subset of partitions - no locks needed
    partitionChan := make(chan int, j.NumPartitions)
    for p := 0; p < j.NumPartitions; p++ {
        partitionChan <- p
    }
    close(partitionChan)

    var wg sync.WaitGroup
    for _, worker := range workers {
        wg.Add(1)
        go func(w *Worker) {
            defer wg.Done()
            for partition := range partitionChan {
                // Build hash table for this partition - no contention
                j.HashTables[partition] = NewHashTable()
                for _, entry := range j.PartitionQueues[partition] {
                    j.HashTables[partition].Insert(entry.Hash, entry.Row)
                }
            }
        }(worker)
    }
    wg.Wait() // Synchronization barrier

    return nil
}

// Phase 3: Probe phase starts (only after build completes)
func (j *ParallelHashJoin) ParallelProbe(workers []*Worker, resultChan chan<- *DataChunk) error {
    // Each worker probes with probe morsels
    // Partition matching ensures no contention

    for _, morsel := range j.ProbeSide.GenerateMorsels() {
        chunk, _ := j.ProbeSide.Scan(morsel)
        result := NewDataChunk(j.OutputSchema())

        for i := 0; i < chunk.Count; i++ {
            hash := hashJoinKey(chunk, j.JoinKey, i)
            partition := hash & uint64(j.NumPartitions - 1)

            // No lock needed - probe is read-only
            matches := j.HashTables[partition].Probe(hash, chunk.GetRow(i))
            for _, match := range matches {
                result.AppendJoinedRow(chunk.GetRow(i), match)
            }
        }

        if result.Count > 0 {
            resultChan <- result
        }
    }

    return nil
}

// Execute runs all three phases with proper synchronization
func (j *ParallelHashJoin) Execute(workers []*Worker, resultChan chan<- *DataChunk) error {
    // Phase 1: Partition build data
    if err := j.PartitionBuildData(workers); err != nil {
        return err
    }
    // sync.WaitGroup barrier ensures partition phase completes

    // Phase 2: Build hash tables
    if err := j.BuildHashTables(workers); err != nil {
        return err
    }
    // sync.WaitGroup barrier ensures build phase completes

    // Phase 3: Probe phase starts
    return j.ParallelProbe(workers, resultChan)
}
```

### Partition Count Selection
```go
func selectPartitionCount(buildRows, numWorkers int) int {
    // At least as many partitions as workers
    minPartitions := numWorkers

    // Target ~64K rows per partition for cache efficiency
    targetPartitions := buildRows / (64 * 1024)

    // Round up to power of 2
    partitions := max(minPartitions, targetPartitions)
    return nextPowerOf2(partitions)
}
```

### Rationale
- Three-phase approach with explicit barriers ensures correctness
- Phase 1 uses local partitioning to avoid contention
- Phase 2 is lock-free because each partition is handled by one worker
- Phase 3 probe is read-only, enabling full parallelism

---

## Decision 5: Parallel Aggregation

### Context
Aggregations with GROUP BY can be parallelized using local aggregation followed by incremental combining.

### Decision
Implement two-phase parallel aggregation with incremental Combine semantics:

```go
// AggregationSink handles incremental combining of local aggregation results
type AggregationSink struct {
    GlobalTable *AggregateHashTable
    mu          sync.Mutex
}

// Combine is called as each worker finishes (not at the end)
func (s *AggregationSink) Combine(local *AggregateHashTable) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.GlobalTable.Merge(local)
}

// ParallelAggregate executes GROUP BY with local/global merge
type ParallelAggregate struct {
    Child        ParallelSource
    GroupBy      []int
    Aggregates   []AggregateFunc
    Sink         *AggregationSink

    // Per-worker local hash tables
    LocalTables  []*AggregateHashTable
}

// Phase 1: Local Aggregation (per worker)
func (a *ParallelAggregate) LocalAggregate(workerID int, chunks <-chan *DataChunk) {
    localTable := a.LocalTables[workerID]

    for chunk := range chunks {
        for i := 0; i < chunk.Count; i++ {
            groupKey := extractGroupKey(chunk, a.GroupBy, i)
            entry := localTable.GetOrCreate(groupKey)

            for j, agg := range a.Aggregates {
                value := chunk.GetValue(agg.Column, i)
                entry.States[j].Update(value)
            }
        }
    }

    // Combine immediately when this worker finishes
    a.Sink.Combine(localTable)
}

// Finalize returns the final aggregated result
func (a *ParallelAggregate) Finalize() *DataChunk {
    // All workers have already combined their results into Sink.GlobalTable
    return a.Sink.GlobalTable.ToDataChunk()
}
```

### High-Cardinality Optimization
For high-cardinality GROUP BY, use parallel merge with partitioning:
```go
func (a *ParallelAggregate) ParallelMerge(numMergers int) *DataChunk {
    // Partition local tables by hash of group key
    // Merge partitions in parallel
    // Combine final results
}
```

### Rationale
- Local aggregation reduces contention during the main processing phase
- Incremental Combine allows overlap between aggregation and merging
- Workers combine results as they finish, reducing final merge latency
- Per-worker hash tables eliminate locking during aggregation

---

## Decision 6: Pipeline Execution Model

### Context
Modern query engines use pipeline execution to reduce materialization and improve cache efficiency.

### Decision
Implement operator pipelines with explicit pipeline breakers:

```go
// Pipeline represents a sequence of operators that can execute in parallel
type Pipeline struct {
    ID        int
    Operators []PipelineOp
    Source    PipelineSource
    Sink      PipelineSink
    Parallel  bool  // Can this pipeline run in parallel?
}

// PipelineBreaker indicates operators that materialize intermediate results
type PipelineBreaker interface {
    // BreakPipeline returns true if this operator cannot stream
    BreakPipeline() bool
}

// Pipeline breakers:
// - HashJoin (build side)
// - Sort
// - Aggregate (with GROUP BY)
// - Window functions

// PipelineCompiler transforms physical plan into pipelines
type PipelineCompiler struct{}

func (c *PipelineCompiler) Compile(plan PhysicalPlan) []*Pipeline {
    var pipelines []*Pipeline

    // Walk plan tree, creating pipelines between breakers
    c.visit(plan, &pipelines, nil)

    return pipelines
}
```

### Rationale
- Pipelines minimize intermediate materialization
- Clear separation between parallel and sequential sections
- Pipeline breakers define synchronization points

---

## Decision 7: Memory Management

### Context
Parallel execution requires careful memory management to avoid contention and ensure correctness.

### Decision
Use per-worker memory arenas with dynamic sizing and overflow handling:

```go
// MemoryArena provides fast allocation for a single worker
type MemoryArena struct {
    blocks       [][]byte
    current      int
    offset       int
    blockSize    int
    maxSize      int64          // Maximum arena size
    currentSize  int64          // Current allocated size
    overflowSize int64          // Heap overflow tracking
    memLimit     *MemoryLimit   // Shared memory accounting
}

// MemoryLimit tracks total memory usage across all workers
type MemoryLimit struct {
    maxTotal    int64
    currentUsed int64
    mu          sync.Mutex
}

func NewMemoryLimit(maxTotal int64) *MemoryLimit {
    return &MemoryLimit{maxTotal: maxTotal}
}

func (m *MemoryLimit) Reserve(size int64) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if m.currentUsed+size > m.maxTotal {
        return false
    }
    m.currentUsed += size
    return true
}

func (m *MemoryLimit) Release(size int64) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.currentUsed -= size
}

// NewMemoryArenaWithLimit creates an arena with dynamic sizing
func NewMemoryArenaWithLimit(blockSize int, memLimit *MemoryLimit, numWorkers int) *MemoryArena {
    // Calculate per-worker limit based on available memory
    availableMem := getAvailableSystemMemory()
    perWorkerLimit := availableMem / int64(numWorkers)

    // Cap at reasonable maximum (e.g., 1GB per worker)
    if perWorkerLimit > 1<<30 {
        perWorkerLimit = 1 << 30
    }

    return &MemoryArena{
        blocks:    make([][]byte, 0),
        blockSize: blockSize,
        maxSize:   perWorkerLimit,
        memLimit:  memLimit,
    }
}

func getAvailableSystemMemory() int64 {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    // Use 75% of available memory, leaving room for Go runtime
    return int64(m.Sys) * 75 / 100
}

func (a *MemoryArena) Allocate(size int) ([]byte, error) {
    // Check if we can allocate within arena
    if a.currentSize+int64(size) <= a.maxSize {
        if a.current >= len(a.blocks) || a.offset+size > a.blockSize {
            // Allocate new block
            newBlockSize := a.blockSize
            if size > newBlockSize {
                newBlockSize = size
            }

            // Check memory limit
            if a.memLimit != nil && !a.memLimit.Reserve(int64(newBlockSize)) {
                return nil, ErrMemoryLimitExceeded
            }

            a.blocks = append(a.blocks, make([]byte, newBlockSize))
            a.current = len(a.blocks) - 1
            a.offset = 0
            a.currentSize += int64(newBlockSize)
        }

        ptr := a.blocks[a.current][a.offset : a.offset+size]
        a.offset += size
        return ptr, nil
    }

    // Arena overflow - allocate from heap with tracking
    if a.memLimit != nil && !a.memLimit.Reserve(int64(size)) {
        return nil, ErrMemoryLimitExceeded
    }
    a.overflowSize += int64(size)
    return make([]byte, size), nil
}

func (a *MemoryArena) Reset() {
    // Release memory limit reservations
    if a.memLimit != nil {
        a.memLimit.Release(a.currentSize + a.overflowSize)
    }

    // Keep first block, release others
    if len(a.blocks) > 1 {
        a.blocks = a.blocks[:1]
    }
    a.current = 0
    a.offset = 0
    a.currentSize = int64(a.blockSize)
    a.overflowSize = 0
}

var ErrMemoryLimitExceeded = errors.New("memory limit exceeded")
```

### Rationale
- Arena allocation is faster than heap allocation
- Per-worker arenas eliminate contention
- Dynamic sizing based on available system memory prevents OOM
- Overflow handling allows graceful degradation when arena is full
- Global memory accounting prevents total memory from exceeding system limits
- Periodic reset prevents memory leaks

---

## Decision 8: Parallel Execution Threshold

### Context
Parallel execution has overhead that may hurt small queries.

### Decision
Only parallelize when expected benefit exceeds overhead:

```go
const (
    MinRowsForParallel = 10000  // Don't parallelize below this
    MinCostForParallel = 100.0  // Based on cost model
)

func shouldParallelize(plan PhysicalPlan, numWorkers int) bool {
    // Check estimated cardinality
    if plan.EstimatedRows() < MinRowsForParallel {
        return false
    }

    // Check cost model
    seqCost := estimateSequentialCost(plan)
    parCost := estimateParallelCost(plan, numWorkers)

    // Parallelization overhead factor
    overhead := 1.2 // 20% overhead

    return parCost * overhead < seqCost
}
```

### Rationale
- Avoids overhead for small queries
- Cost-based decision using optimizer estimates
- Configurable thresholds for tuning

---

## File Organization

```
internal/
├── parallel/
│   ├── pool.go           // ThreadPool and Worker
│   ├── morsel.go         // Morsel and work distribution
│   ├── arena.go          // MemoryArena
│   ├── pipeline.go       // Pipeline and PipelineCompiler
│   ├── event.go          // PipelineEvent for synchronization
│   ├── scan.go           // ParallelTableScan
│   ├── hash_join.go      // ParallelHashJoin
│   ├── aggregate.go      // ParallelAggregate
│   ├── sort.go           // ParallelSort
│   └── parallel_test.go  // Tests
└── executor/
    └── parallel.go       // Integration with executor
```

---

## Test Strategy

1. **Correctness Tests**: Compare parallel vs sequential results
2. **Race Detection**: Run with `-race` flag
3. **Scaling Tests**: Measure speedup with varying workers
4. **Memory Tests**: Verify no leaks with repeated execution
5. **Stress Tests**: High concurrency, large data volumes
