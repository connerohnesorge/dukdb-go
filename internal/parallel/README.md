# Parallel Execution Package

This package provides parallel query execution infrastructure for dukdb-go using Go's native concurrency primitives. It implements a morsel-driven pipeline execution model inspired by DuckDB's architecture.

## Overview

The parallel package enables efficient parallel execution of database queries by:

- Partitioning work into morsels (small units of data)
- Distributing morsels across worker goroutines
- Using pipeline execution to minimize intermediate materialization
- Providing thread-local memory arenas to reduce contention

## Architecture

```
Query
    |
    v
Pipeline Compiler (creates pipelines from physical plan)
    |
    v
Pipeline Executor (executes pipelines respecting dependencies)
    |
    +---> Pipeline 1: Scan -> Filter -> Project
    +---> Pipeline 2: HashBuild (parallel)
    +---> Pipeline 3: HashProbe -> Aggregate -> Output
    |
    v
Parallel Scheduler (ThreadPool)
    |
    +---> Worker 1 (goroutine + memory arena)
    +---> Worker 2 (goroutine + memory arena)
    +---> Worker N (goroutine + memory arena)
    |
    v
Results
```

## Core Components

### ThreadPool (`pool.go`)

The `ThreadPool` manages parallel execution workers:

```go
// Create a pool with default worker count (GOMAXPROCS)
pool := NewThreadPool(0)
defer pool.Shutdown()

// Create a pool with specific worker count
pool := NewThreadPool(4)

// Create a pool with memory limiting
pool := NewThreadPoolWithLimit(4, 1<<30) // 4 workers, 1GB limit
```

### Workers

Each worker has:
- A unique ID
- A per-worker memory arena (reduces allocation contention)
- Thread-local state for operators

### Morsels (`morsel.go`)

Morsels represent units of parallel work:

```go
type Morsel struct {
    TableID  uint64  // Table identifier
    StartRow uint64  // First row in this morsel
    EndRow   uint64  // One past the last row
    RowGroup int     // Row group index
    Priority int     // Higher = processed first
}
```

### Memory Arena (`arena.go`)

Per-worker memory arenas provide fast allocation:

```go
arena := NewMemoryArena(64 * 1024) // 64KB block size

// Allocate memory (fast, no global locking)
data, err := arena.Allocate(1024)

// Reset for reuse (keeps first block)
arena.Reset()
```

## Parallel Operators

### Parallel Table Scan (`scan.go`)

Scans table data in parallel with filter and projection pushdown:

```go
scan := NewParallelTableScan(tableOID, tableName, columns, types, dataReader)
scan.SetProjections([]int{0, 2})  // Project specific columns
scan.SetFilter(&SimpleCompareFilter{ColumnIdx: 0, Op: ">", Value: 100})
```

### Parallel Hash Join (`hash_join.go`)

Executes hash join with radix partitioning:

```go
join := NewParallelHashJoin(
    buildSource, probeSource,
    []int{0}, []int{0},  // Join key indices
    InnerJoin,
    16,  // Number of partitions
)
join.SetBuildSchema(buildCols, buildTypes)
join.SetProbeSchema(probeCols, probeTypes)

resultChan, err := join.Execute(pool, ctx)
```

Join types supported:
- `InnerJoin`
- `LeftJoin`
- `RightJoin`
- `FullJoin`

### Parallel Aggregation (`aggregate.go`)

Two-phase parallel aggregation (local + global merge):

```go
aggregates := []AggregateFunc{
    NewAggregateFunc(AggSum, 1, "sum_value"),
    NewAggregateFunc(AggCount, -1, "count"),
    NewAggregateFunc(AggAvg, 1, "avg_value"),
}

agg := NewParallelAggregate(
    source,
    []int{0},             // Group by column 0
    []string{"group_id"}, // Group column names
    groupTypes,
    aggregates,
    numWorkers,
)

result, err := agg.Execute(pool, ctx)
```

Aggregate types:
- `AggSum`, `AggCount`, `AggCountStar`, `AggAvg`
- `AggMin`, `AggMax`, `AggFirst`, `AggLast`

### Parallel Sort (`sort.go`)

Parallel sort with K-way merge:

```go
sortKeys := []SortKey{
    NewSortKeyWithOrder(0, "value", Ascending, NullsLast),
    NewSortKeyWithOrder(1, "name", Descending, NullsFirst),
}

sortOp := NewParallelSort(source, sortKeys)
sortOp.SetLimit(100)   // Optional LIMIT
sortOp.SetOffset(10)   // Optional OFFSET

result, err := sortOp.Execute(pool, ctx)
```

### Pipeline Execution (`pipeline.go`)

Pipelines chain operators together:

```go
pipe := NewPipeline(1, "main_pipeline")
pipe.SetSource(source)
pipe.AddOperator(filterOp)
pipe.AddOperator(projectOp)
pipe.SetSink(sink)

// Add dependencies
pipe.AddDependency(buildPipeline.CompletionEvent)

err := pipe.Execute(pool, ctx)
```

Pipeline breakers (materialize intermediate results):
- `SortBreakerOp`
- `AggregateBreakerOp`
- `HashBuildOp`
- `WindowBreakerOp`

## Configuration

### Parallelism Thresholds

```go
const (
    MinRowsForParallel = 10000  // Don't parallelize below this
    MinMorselSize      = 1024   // Minimum rows per morsel
    MaxMorselSize      = 122880 // Maximum rows per morsel (one row group)
)
```

### Morsel Configuration

```go
config := MorselConfig{
    MinSize:    1024,  // Minimum morsel size
    MaxSize:    8192,  // Maximum morsel size
    TargetSize: 4096,  // Preferred morsel size
}
gen := NewMorselGeneratorWithConfig(config)
```

### Memory Limits

```go
// Global memory limit across all workers
pool := NewThreadPoolWithLimit(4, 4*1024*1024*1024) // 4GB total

// Per-worker limit is automatically calculated
// (total / numWorkers, capped at 1GB per worker)
```

### Hash Join Configuration

```go
config := HashJoinConfig{
    NumPartitions:      32,     // 0 = auto-select
    SpillThreshold:     100000, // Entries before spilling
    SpillDir:           "/tmp/dukdb_spill",
    EstimatedBuildRows: 1000000,
}
join := NewParallelHashJoinWithConfig(
    buildSource, probeSource,
    buildKeys, probeKeys,
    InnerJoin,
    config,
    numWorkers,
)
```

## When Parallelism is Used

The parallel execution engine makes cost-based decisions:

1. **Row count threshold**: Queries with fewer than 10,000 rows typically run sequentially
2. **Cost model**: Compares estimated parallel cost (including overhead) to sequential cost
3. **Parallelism factor**: Overhead factor of 1.2 (20%) accounts for coordination costs

```go
func shouldParallelize(plan PhysicalPlan, numWorkers int) bool {
    if plan.EstimatedRows() < MinRowsForParallel {
        return false
    }
    seqCost := estimateSequentialCost(plan)
    parCost := estimateParallelCost(plan, numWorkers)
    overhead := 1.2
    return parCost * overhead < seqCost
}
```

## Performance Tuning

### Optimal Worker Count

- **CPU-bound queries**: Use `runtime.GOMAXPROCS(0)` workers (default)
- **I/O-bound queries**: May benefit from more workers than CPU cores
- **Memory pressure**: Reduce workers to decrease memory usage

### Morsel Size Tuning

- **Smaller morsels** (1024-4096): Better load balancing, higher overhead
- **Larger morsels** (8192-32768): Better cache efficiency, potential load imbalance

### Partition Count for Hash Join

```go
// Auto-select based on data size and workers
partitions := SelectPartitionCount(estimatedBuildRows, numWorkers)

// Manual tuning: at least as many partitions as workers
// Target ~64K rows per partition for cache efficiency
```

### Memory Arena Size

```go
// Default: 64KB blocks, 64MB max per worker
arena := NewMemoryArena(64 * 1024)

// For larger allocations, use larger blocks
arena := NewMemoryArena(256 * 1024)
```

## Work Stealing

The package supports optional work stealing for better load balancing:

```go
// Enable work stealing
wd := NewWorkDistributor(4, true)

// Distribute morsels
wd.Distribute(morsels)

// Workers can steal from others when their queue is empty
morsel, ok := wd.GetWork(workerID)
```

## Error Handling

All parallel operations support context cancellation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

err := pool.Execute(ctx, pipeline)
if err == context.DeadlineExceeded {
    // Query timed out
}
```

## Testing

Run tests with the race detector:

```bash
go test -race ./internal/parallel/...
```

Run benchmarks:

```bash
go test -bench=. ./internal/parallel/... -benchmem
```

Run specific benchmark with scaling:

```bash
go test -bench=BenchmarkScaling ./internal/parallel/... -v
```

## File Organization

```
internal/parallel/
    pool.go           // ThreadPool and Worker
    morsel.go         // Morsel generation and work distribution
    arena.go          // MemoryArena and MemoryLimit
    scan.go           // ParallelTableScan
    hash_join.go      // ParallelHashJoin
    aggregate.go      // ParallelAggregate
    sort.go           // ParallelSort
    pipeline.go       // Pipeline execution model
    errors.go         // Error definitions

    parallel_test.go      // Basic unit tests
    correctness_test.go   // Parallel vs sequential correctness
    benchmark_test.go     // Scaling benchmarks
    stress_test.go        // Memory and concurrency stress tests
    sequential_test.go    // Sequential execution regression tests
```

## Known Limitations

1. **Window functions**: Basic support only; full window function parallelism is future work
2. **Spill to disk**: Hash join spilling is supported but not optimized for all cases
3. **Skewed data**: Work stealing helps but extreme skew may still cause imbalance
4. **Memory estimation**: Arena sizing uses heuristics that may not be optimal for all workloads

## See Also

- Design document: `spectr/changes/add-parallel-execution/design.md`
- DuckDB paper: "Push-Based Execution in DuckDB" (CIDR 2024)
