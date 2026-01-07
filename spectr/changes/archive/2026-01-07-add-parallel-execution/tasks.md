# Tasks: Parallel Query Execution

## Phase 1: Parallel Infrastructure

- [x] 1.1 Create internal/parallel/ package with pool.go
- [x] 1.2 Implement ThreadPool with configurable worker count
- [x] 1.3 Implement Worker struct with goroutine lifecycle
- [x] 1.4 Implement MemoryArena for per-worker allocation
- [x] 1.5 Add context-based cancellation and error handling
- [x] 1.6 Implement work channel with buffering for prefetch

## Phase 2: Morsel-Driven Execution

- [x] 2.1 Define Morsel struct for work units
- [x] 2.2 Implement morsel generation from table metadata
- [x] 2.3 Implement work distribution via channels
- [x] 2.4 Add morsel size configuration (min/max rows)
- [x] 2.5 Implement work stealing for load balancing

## Phase 3: Parallel Table Scan

- [x] 3.1 Implement ParallelTableScan operator
- [x] 3.2 Partition table by row groups into morsels
- [x] 3.3 Implement parallel row group reading
- [x] 3.4 Add filter pushdown to parallel scan
- [x] 3.5 Add projection pushdown to parallel scan
- [x] 3.6 Test parallel scan correctness and performance

## Phase 4: Parallel Hash Join

- [x] 4.1 Implement radix partitioning for build tuples
- [x] 4.2 Implement per-partition hash tables with fine-grained locking
- [x] 4.3 Implement parallel build phase
- [x] 4.4 Implement lock-free parallel probe phase
- [x] 4.5 Add partition count selection heuristic
- [x] 4.6 Handle outer joins (LEFT, RIGHT, FULL)
- [x] 4.7 Implement partition spilling for large builds
- [x] 4.8 Test hash join correctness with race detector

## Phase 5: Parallel Aggregation

- [x] 5.1 Implement per-worker AggregateHashTable
- [x] 5.2 Implement parallel local aggregation phase
- [x] 5.3 Implement global merge for low-cardinality groups
- [x] 5.4 Implement parallel merge for high-cardinality groups
- [x] 5.5 Handle all aggregate functions (SUM, COUNT, AVG, MIN, MAX, etc.)
- [x] 5.6 Test aggregation correctness with various group sizes

## Phase 6: Parallel Sort

- [x] 6.1 Implement parallel partitioning by sort key
- [x] 6.2 Implement parallel local sorting per partition
- [x] 6.3 Implement K-way merge of sorted partitions
- [x] 6.4 Handle multi-column sort keys
- [x] 6.5 Test sort correctness and stability

## Phase 7: Pipeline Execution Model

- [x] 7.1 Define Pipeline and PipelineOp interfaces
- [x] 7.2 Implement PipelineCompiler to transform physical plan
- [x] 7.3 Identify and handle pipeline breakers (Sort, Aggregate, HashBuild)
- [x] 7.4 Implement pipeline execution with parallel sections
- [x] 7.5 Add exchange operators for data redistribution

## Phase 8: Integration

- [x] 8.1 Integrate parallel execution with executor
- [x] 8.2 Add parallelism configuration (PRAGMA threads)
- [x] 8.3 Implement parallel execution threshold decision
- [x] 8.4 Add cost model integration for parallel plan selection
- [x] 8.5 Add parallel operator annotations to EXPLAIN

## Phase 9: Testing and Validation

- [x] 9.1 Create parallel correctness test suite
- [x] 9.2 Run all tests with race detector
- [x] 9.3 Create scaling benchmark (1, 2, 4, 8 workers)
- [x] 9.4 Create memory stress tests
- [x] 9.5 Verify no regressions in sequential execution
- [x] 9.6 Add documentation for parallel configuration
