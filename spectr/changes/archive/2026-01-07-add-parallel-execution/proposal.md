# Proposal: Parallel Query Execution

## Summary

Implement parallel query execution using Go's goroutine-based concurrency to enable multi-threaded execution of table scans, hash joins, aggregations, and other operators, achieving significant performance improvements for analytical queries.

## Motivation

Currently, dukdb-go executes all queries on a single goroutine:
- Table scans read data sequentially
- Hash joins build and probe on single thread
- Aggregations process all tuples sequentially
- No utilization of multi-core processors

This results in:
- Suboptimal performance on modern multi-core systems
- Poor analytical query throughput
- Inability to compete with DuckDB's parallel execution

## Problem Statement

### Current State
- All operators execute on calling goroutine
- DataChunks processed sequentially
- No partitioning or parallel scan support
- Memory management assumes single-threaded access

### Target State
- Configurable parallelism (default: GOMAXPROCS)
- Parallel table scans with row group partitioning
- Parallel hash join with partition-wise build/probe
- Parallel aggregation with local/global merge
- Thread-safe memory management with arena allocation

## Scope

### In Scope
1. **Parallel Table Scan**
   - Partition tables into independent chunks
   - Parallel row group reading
   - Vectorized parallel processing

2. **Parallel Hash Join**
   - Partitioned build phase
   - Parallel probe phase
   - Spill-to-disk for large tables

3. **Parallel Aggregation**
   - Thread-local aggregation states
   - Parallel local aggregation
   - Global merge of partial results

4. **Parallel Sort**
   - Parallel partitioning phase
   - Local sorting per partition
   - K-way merge of sorted partitions

5. **Pipeline Parallelism**
   - Operator pipeline execution model
   - Exchange operators for data distribution
   - Pipeline breakers (sort, aggregate)

6. **Resource Management**
   - Thread pool with work stealing
   - Memory arena per worker
   - Backpressure and flow control

### Out of Scope
- Distributed query execution across nodes
- GPU acceleration
- SIMD vectorization (future work)
- Inter-query parallelism

## Approach

### Phase 1: Parallel Infrastructure
1. Create thread pool with configurable worker count
2. Implement work-stealing scheduler
3. Create per-worker memory arenas
4. Add synchronization primitives

### Phase 2: Parallel Table Scan
1. Partition table by row groups
2. Create parallel scan operator
3. Implement work distribution
4. Add scan coordination and termination

### Phase 3: Parallel Hash Join
1. Implement radix partitioning for build
2. Parallel hash table construction per partition
3. Parallel probe with partition matching
4. Handle partition spilling

### Phase 4: Parallel Aggregation
1. Create thread-local aggregation states
2. Implement parallel local aggregation
3. Add global merge phase
4. Handle high-cardinality groups

### Phase 5: Pipeline Execution
1. Design pipeline execution model
2. Implement exchange operators
3. Add pipeline breaker handling
4. Create query pipeline compiler

## Success Criteria

1. **Speedup**: Linear speedup up to 8 cores on analytical queries
2. **Correctness**: All query results identical to sequential execution
3. **Memory**: Parallel execution memory overhead < 2x sequential
4. **Latency**: Small queries not significantly impacted
5. **Existing Tests**: All existing tests continue to pass

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Race conditions | High | High | Extensive testing, race detector |
| Memory contention | Medium | Medium | Per-worker arenas |
| Overhead for small queries | Medium | Medium | Threshold-based parallelization |
| Debugging complexity | High | Low | Comprehensive logging |

## Dependencies

- Cost-based optimizer (for parallel plan selection)
- Current executor provides sequential baseline
- DataChunk API for vectorized processing

## Affected Specs

- **NEW**: `parallel-execution` - Parallel execution infrastructure
- **MODIFIED**: `execution-engine` - Add parallel operators
- **MODIFIED**: `storage` - Thread-safe data access
