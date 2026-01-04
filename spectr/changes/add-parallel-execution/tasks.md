# Tasks: Parallel Query Execution

## Phase 1: Parallel Infrastructure

- [ ] 1.1 Create internal/parallel/ package with pool.go
- [ ] 1.2 Implement ThreadPool with configurable worker count
- [ ] 1.3 Implement Worker struct with goroutine lifecycle
- [ ] 1.4 Implement MemoryArena for per-worker allocation
- [ ] 1.5 Add context-based cancellation and error handling
- [ ] 1.6 Implement work channel with buffering for prefetch

## Phase 2: Morsel-Driven Execution

- [ ] 2.1 Define Morsel struct for work units
- [ ] 2.2 Implement morsel generation from table metadata
- [ ] 2.3 Implement work distribution via channels
- [ ] 2.4 Add morsel size configuration (min/max rows)
- [ ] 2.5 Implement work stealing for load balancing

## Phase 3: Parallel Table Scan

- [ ] 3.1 Implement ParallelTableScan operator
- [ ] 3.2 Partition table by row groups into morsels
- [ ] 3.3 Implement parallel row group reading
- [ ] 3.4 Add filter pushdown to parallel scan
- [ ] 3.5 Add projection pushdown to parallel scan
- [ ] 3.6 Test parallel scan correctness and performance

## Phase 4: Parallel Hash Join

- [ ] 4.1 Implement radix partitioning for build tuples
- [ ] 4.2 Implement per-partition hash tables with fine-grained locking
- [ ] 4.3 Implement parallel build phase
- [ ] 4.4 Implement lock-free parallel probe phase
- [ ] 4.5 Add partition count selection heuristic
- [ ] 4.6 Handle outer joins (LEFT, RIGHT, FULL)
- [ ] 4.7 Implement partition spilling for large builds
- [ ] 4.8 Test hash join correctness with race detector

## Phase 5: Parallel Aggregation

- [ ] 5.1 Implement per-worker AggregateHashTable
- [ ] 5.2 Implement parallel local aggregation phase
- [ ] 5.3 Implement global merge for low-cardinality groups
- [ ] 5.4 Implement parallel merge for high-cardinality groups
- [ ] 5.5 Handle all aggregate functions (SUM, COUNT, AVG, MIN, MAX, etc.)
- [ ] 5.6 Test aggregation correctness with various group sizes

## Phase 6: Parallel Sort

- [ ] 6.1 Implement parallel partitioning by sort key
- [ ] 6.2 Implement parallel local sorting per partition
- [ ] 6.3 Implement K-way merge of sorted partitions
- [ ] 6.4 Handle multi-column sort keys
- [ ] 6.5 Test sort correctness and stability

## Phase 7: Pipeline Execution Model

- [ ] 7.1 Define Pipeline and PipelineOp interfaces
- [ ] 7.2 Implement PipelineCompiler to transform physical plan
- [ ] 7.3 Identify and handle pipeline breakers (Sort, Aggregate, HashBuild)
- [ ] 7.4 Implement pipeline execution with parallel sections
- [ ] 7.5 Add exchange operators for data redistribution

## Phase 8: Integration

- [ ] 8.1 Integrate parallel execution with executor
- [ ] 8.2 Add parallelism configuration (PRAGMA threads)
- [ ] 8.3 Implement parallel execution threshold decision
- [ ] 8.4 Add cost model integration for parallel plan selection
- [ ] 8.5 Add parallel operator annotations to EXPLAIN

## Phase 9: Testing and Validation

- [ ] 9.1 Create parallel correctness test suite
- [ ] 9.2 Run all tests with race detector
- [ ] 9.3 Create scaling benchmark (1, 2, 4, 8 workers)
- [ ] 9.4 Create memory stress tests
- [ ] 9.5 Verify no regressions in sequential execution
- [ ] 9.6 Add documentation for parallel configuration
