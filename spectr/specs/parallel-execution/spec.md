# Parallel Execution Specification

## Requirements

### Requirement: Parallel Thread Pool

The system MUST provide a configurable thread pool for parallel execution.

#### Scenario: Default parallelism uses GOMAXPROCS
- **Given** no parallelism configuration is set
- **When** a parallel query executes
- **Then** worker count equals runtime.GOMAXPROCS(0)

#### Scenario: Configure parallelism via PRAGMA
- **Given** PRAGMA threads = 4 is executed
- **When** subsequent queries execute
- **Then** at most 4 workers are used for parallel execution

#### Scenario: Clean shutdown on context cancellation
- **Given** a parallel query is running
- **When** context is cancelled
- **Then** all workers stop gracefully
- **And** partial results are discarded
- **And** no goroutine leaks occur

---

### Requirement: Parallel Table Scan

The system MUST support parallel scanning of tables by partitioning into morsels.

#### Scenario: Partition table by row groups
- **Given** a table with multiple row groups
- **When** a parallel scan is initiated
- **Then** each row group becomes a separate morsel
- **And** morsels are distributed to available workers

#### Scenario: Apply filter pushdown in parallel scan
- **Given** a table scan with WHERE clause
- **When** parallel scan executes
- **Then** each worker applies the filter independently
- **And** only matching rows are returned

#### Scenario: Apply projection pushdown
- **Given** a query selecting specific columns
- **When** parallel scan executes
- **Then** only requested columns are read per morsel
- **And** memory usage is minimized

#### Scenario: Combine results from parallel scan
- **Given** parallel scan with N workers
- **When** all morsels are processed
- **Then** results are combined in deterministic order
- **And** total row count matches sequential scan

---

### Requirement: Parallel Hash Join

The system MUST support parallel execution of hash joins.

#### Scenario: Parallel build phase
- **Given** a hash join with build table
- **When** build phase executes in parallel
- **Then** tuples are radix-partitioned by hash
- **And** each partition has its own hash table
- **And** build completes before probe starts

#### Scenario: Parallel probe phase
- **Given** hash tables are built
- **When** probe phase executes in parallel
- **Then** each worker probes its assigned morsels
- **And** probe is lock-free (partition isolation)
- **And** matching tuples are emitted

#### Scenario: Preserve join semantics
- **Given** a LEFT/RIGHT/FULL OUTER JOIN
- **When** parallel hash join executes
- **Then** outer join semantics are preserved
- **And** NULL padding is applied correctly

#### Scenario: Handle build side larger than memory
- **Given** build table exceeds memory limit
- **When** hash join executes
- **Then** partitions spill to disk
- **And** probe phase reads spilled partitions
- **And** results remain correct

---

### Requirement: Parallel Aggregation

The system MUST support parallel GROUP BY aggregation.

#### Scenario: Two-phase aggregation
- **Given** a GROUP BY with aggregates
- **When** parallel aggregation executes
- **Then** phase 1 aggregates locally per worker
- **And** phase 2 merges local results into global

#### Scenario: Handle all aggregate functions
- **Given** aggregates like SUM, COUNT, AVG, MIN, MAX
- **When** parallel aggregation executes
- **Then** all functions produce correct results
- **And** partial states combine correctly

#### Scenario: Low-cardinality GROUP BY
- **Given** GROUP BY with few distinct groups
- **When** parallel aggregation executes
- **Then** global merge is single-threaded
- **And** results are equivalent to sequential

#### Scenario: High-cardinality GROUP BY
- **Given** GROUP BY with many distinct groups
- **When** parallel aggregation executes
- **Then** global merge may run in parallel
- **And** results are equivalent to sequential

---

### Requirement: Parallel Sort

The system MUST support parallel sorting.

#### Scenario: Parallel sort with single key
- **Given** ORDER BY on single column
- **When** parallel sort executes
- **Then** data is partitioned by key range
- **And** each partition is sorted locally
- **And** results are merged via K-way merge

#### Scenario: Parallel sort with multiple keys
- **Given** ORDER BY on multiple columns
- **When** parallel sort executes
- **Then** composite key is used for partitioning
- **And** final order matches specification

#### Scenario: Sort stability
- **Given** ORDER BY with duplicate keys
- **When** parallel sort executes
- **Then** relative order of duplicates is preserved
- **And** behavior matches sequential sort

---

### Requirement: Pipeline Execution

The system MUST execute operators in pipelines for efficiency.

#### Scenario: Streaming pipeline execution
- **Given** operators without pipeline breakers
- **When** pipeline executes
- **Then** data streams through operators
- **And** intermediate results are not materialized

#### Scenario: Pipeline breaker handling
- **Given** operators that require materialization (Sort, Aggregate)
- **When** pipeline executes
- **Then** pipeline breaks at these operators
- **And** subsequent pipeline starts after materialization

---

### Requirement: Parallelization Threshold

The system MUST avoid parallel overhead for small queries.

#### Scenario: Skip parallelism for small tables
- **Given** a table with fewer than 10,000 rows
- **When** query is planned
- **Then** sequential execution is chosen
- **And** parallel overhead is avoided

#### Scenario: Parallelize large analytical queries
- **Given** a table with millions of rows
- **When** query is planned
- **Then** parallel execution is chosen
- **And** multiple workers are utilized

---

### Requirement: Memory Management

The system MUST manage memory efficiently for parallel execution.

#### Scenario: Per-worker memory arenas
- **Given** parallel execution with N workers
- **When** workers allocate memory
- **Then** each worker uses its own arena
- **And** no allocation contention occurs

#### Scenario: Arena cleanup after query
- **Given** parallel query completes
- **When** results are returned
- **Then** worker arenas are reset
- **And** memory is available for next query

---

### Requirement: Correctness Under Parallelism

The system MUST produce identical results for parallel and sequential execution.

#### Scenario: Deterministic results
- **Given** any query
- **When** executed with parallel and sequential modes
- **Then** results are identical
- **And** row ordering matches ORDER BY specification

#### Scenario: No race conditions
- **Given** parallel execution with Go race detector
- **When** any query executes
- **Then** no race conditions are detected
- **And** all shared state is properly synchronized

---

