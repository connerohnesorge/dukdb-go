# Parallel Execution Planning - Delta Spec

## ADDED Requirements

### Requirement: Automatic Parallel Plan Generation

The system MUST automatically generate parallel execution plans when beneficial for analytical queries.

#### Scenario: Generate parallel plan for large table scan
- WHEN a query scans a table with more than 100k rows
- THEN planner generates parallel table scan operators (one per worker)
- AND each worker scans independent row groups (morsels)
- AND results are gathered at top level

#### Scenario: Skip parallel plan for small tables
- WHEN a query scans a table with fewer than 10k rows
- THEN planner generates sequential table scan
- AND parallel overhead is avoided

#### Scenario: Generate parallel plan for hash join
- WHEN a query joins two tables with estimated output > 100k rows
- THEN planner generates parallel join plan
- AND builds repartition exchange on join key before join
- AND each worker executes hash join independently
- AND results are gathered

#### Scenario: Generate parallel plan for GROUP BY aggregation
- WHEN a query has GROUP BY with estimated output > 50k rows
- THEN planner generates two-phase aggregation plan
- AND phase 1: partial aggregates computed locally per worker
- AND phase 2: global merge of partial aggregates

#### Scenario: Compare sequential and parallel costs
- WHEN planning any query
- THEN planner estimates both sequential and parallel execution costs
- AND selects plan with lower total cost
- AND logs decision in EXPLAIN output for debugging

---

### Requirement: Pipeline Analysis

The system MUST analyze query plans to identify parallelizable pipeline stages.

#### Scenario: Identify blocking operators
- GIVEN a query with Sort, Aggregate, Distinct, Limit
- WHEN pipeline analysis runs
- THEN these operators are marked as pipeline breakers
- AND streaming operators between breakers form independent stages

#### Scenario: Classify streaming operators
- GIVEN a query with Filter, Project, Unnest, LateralJoin
- WHEN pipeline analysis runs
- THEN these operators are classified as streaming (non-blocking)
- AND can execute within parallel pipelines

#### Scenario: Build pipeline chain
- GIVEN a complex query with multiple pipeline stages
- WHEN pipeline analysis runs
- THEN stages are identified and ordered
- AND dependencies between stages are captured
- AND planner uses this information for Exchange operator placement

#### Scenario: Handle subqueries in pipeline analysis
- GIVEN a query with correlated subqueries
- WHEN pipeline analysis runs
- THEN subqueries are analyzed recursively
- AND pipeline structure accounts for subquery semantics

---

### Requirement: Exchange Operators

The system MUST support data exchange operators for inter-worker communication.

#### Scenario: Gather exchange collects results
- GIVEN a parallel execution stage with N workers
- WHEN gather exchange executes
- THEN results from all workers are collected into single stream
- AND ordering is deterministic (by worker ID, then input order)
- AND no data is lost or duplicated

#### Scenario: Repartition exchange by hash
- GIVEN data to distribute based on join key
- WHEN repartition exchange executes
- THEN data is hashed on specified column(s)
- AND distributed to workers based on hash modulo worker count
- AND each worker receives its partition exclusively

#### Scenario: Broadcast exchange replicates data
- GIVEN data to broadcast to all workers
- AND data size is less than broadcast limit (100MB default)
- WHEN broadcast exchange executes
- THEN data is replicated to all workers
- AND each worker receives complete copy

#### Scenario: Round-robin exchange distributes evenly
- GIVEN data to distribute for load balancing
- WHEN round-robin exchange executes
- THEN data is distributed in round-robin order
- AND each worker receives approximately equal tuples

#### Scenario: Exchange respects memory limits
- GIVEN exchange buffer exceeding memory limit
- WHEN exchange executes
- THEN remaining data spills to temporary storage
- AND reads are fetched from disk as needed
- AND memory is released after spill is materialized

#### Scenario: Exchange provides progress feedback
- GIVEN exchange operators in execution plan
- WHEN query executes
- THEN progress information is available (tuples processed, bytes transferred)
- AND can be used for adaptive query optimization

---

### Requirement: Cost Model for Parallel Execution

The system MUST estimate costs accurately for parallel execution plans.

#### Scenario: Cost parallel table scan
- GIVEN parallel table scan with N workers
- WHEN cost is estimated
- THEN cost = sequential_cost / parallelism_gain
- AND parallelism_gain = min(worker_count, io_bound_factor)
- AND 0.8 <= parallelism_gain <= worker_count

#### Scenario: Cost parallel join
- GIVEN parallel hash join on join key
- WHEN cost is estimated
- THEN cost includes build cost, probe cost, exchange cost
- AND parallelism_gain = 0.8 * worker_count (accounting for hash table overhead)
- AND repartition exchange cost is included

#### Scenario: Cost parallel aggregation
- GIVEN parallel GROUP BY aggregation
- WHEN cost is estimated
- THEN cost = (local_agg_cost / worker_count) + global_merge_cost + exchange_cost
- AND parallelism_gain = 0.9 * worker_count
- AND accounts for cardinality reduction before merge

#### Scenario: Choose sequential when parallel is slower
- GIVEN query where parallel cost > sequential cost
- WHEN plan is selected
- THEN sequential plan is chosen
- AND parallel plan is not generated

#### Scenario: Add exchange cost to plan
- GIVEN any exchange operator in plan
- WHEN cost is estimated
- THEN operator cost is included in total
- AND cost reflects data volume and transfer type

---

### Requirement: Threshold-Based Parallelization

The system MUST avoid parallelization overhead for queries that don't benefit.

#### Scenario: Respect cardinality threshold
- GIVEN table with fewer than 10,000 rows estimated
- WHEN query is planned
- THEN sequential execution is chosen regardless of parallelism gain
- AND parallel execution logic is skipped

#### Scenario: Respect cost threshold
- GIVEN parallelism gain less than 10%
- WHEN query is planned
- THEN sequential execution is chosen
- AND parallel plan overhead not justified

#### Scenario: Respect single-table queries
- GIVEN query on single table with no joins
- WHEN query is planned
- THEN sequential execution is chosen if cardinality < threshold
- AND parallel execution considered if cardinality is large

#### Scenario: Apply thresholds in cost comparison
- GIVEN query selection between sequential and parallel
- WHEN comparing costs
- THEN thresholds are enforced before cost-based selection
- AND ensures small queries stay sequential

---

### Requirement: Configuration and Control

The system MUST provide configuration options for parallelization behavior.

#### Scenario: Configure max parallelism via PRAGMA
- GIVEN PRAGMA threads = 4
- WHEN query is planned
- THEN maximum 4 workers are used for parallel execution
- AND existing PRAGMA setting is respected

#### Scenario: Enable/disable parallel plans via PRAGMA
- GIVEN PRAGMA enable_parallel_plans = false
- WHEN query is planned
- THEN sequential plans are always generated
- AND parallel execution is completely disabled

#### Scenario: Configure broadcast threshold via PRAGMA
- GIVEN PRAGMA broadcast_threshold = 200MB
- WHEN broadcast exchange evaluates data size
- THEN broadcast is used for data < 200MB
- AND larger data uses repartition

#### Scenario: Configure cardinality threshold via PRAGMA
- GIVEN PRAGMA parallel_cardinality_threshold = 50000
- WHEN parallelization threshold is checked
- THEN queries with output > 50k rows are parallelized
- AND default is 10k rows

---

### Requirement: EXPLAIN Output for Parallel Plans

The system MUST show parallel execution strategy in EXPLAIN output.

#### Scenario: Show worker count and morsel distribution
- GIVEN parallel table scan in plan
- WHEN EXPLAIN is executed
- THEN output shows "Parallel Table Scan (4 workers, 8 morsels)"
- AND distribution strategy is visible

#### Scenario: Show exchange operators in plan
- GIVEN exchange operators in plan
- WHEN EXPLAIN is executed
- THEN each exchange operator is shown with type and parameters
- AND repartition shows join key, broadcast shows data size estimate

#### Scenario: Compare parallel vs sequential cost
- GIVEN parallel and sequential plans both considered
- WHEN EXPLAIN is executed
- THEN output shows "Sequential Cost: 1000, Parallel Cost: 150"
- AND reason for selection is indicated

#### Scenario: Show parallelism gain in EXPLAIN
- GIVEN parallel plan selected
- WHEN EXPLAIN is executed
- THEN "Parallelism Gain: 3.2x" is shown
- AND accounts for all overhead (exchange, sync, etc.)

#### Scenario: Show actual parallelism in EXPLAIN ANALYZE
- GIVEN parallel plan with EXPLAIN ANALYZE
- WHEN query executes
- THEN output shows actual worker utilization
- AND actual speedup vs sequential is calculated
- AND any skewed work distribution is highlighted

---

### Requirement: Correctness Under Parallelism

The system MUST produce identical results whether plans execute sequentially or in parallel.

#### Scenario: Deterministic result ordering
- GIVEN any query executed with parallel and sequential plans
- WHEN results are returned
- THEN results are identical
- AND row ordering matches ORDER BY specification exactly

#### Scenario: No race conditions
- GIVEN parallel plan execution
- WHEN Go race detector is enabled
- THEN no race conditions are detected
- AND all shared state is properly synchronized

#### Scenario: Aggregate correctness
- GIVEN GROUP BY aggregation with parallel execution
- WHEN results are computed
- THEN aggregates match sequential execution exactly
- AND handles all data types and functions

#### Scenario: Join semantics preserved
- GIVEN outer join with parallel execution
- WHEN results are returned
- THEN outer join semantics are preserved
- AND NULL padding is applied correctly
- AND row counts match sequential join

#### Scenario: Handle empty tables
- GIVEN parallel plan on empty table
- WHEN query executes
- THEN results are correct (empty or aggregates with NULL/0)
- AND behaves identically to sequential

---

### Requirement: Performance Guarantees

The system MUST deliver meaningful performance improvements for parallelizable queries.

#### Scenario: Speedup for large table scan
- GIVEN query scanning 1M rows with sequential cost 1000
- WHEN executed with 4 workers
- THEN execution time is less than 300 units (> 3x speedup)
- AND parallelization overhead is minimal

#### Scenario: Speedup for multi-table join
- GIVEN query joining 3 tables with total 10M rows
- WHEN executed with 4 workers
- THEN execution time is less than 600 units (> 1.67x speedup)
- AND join repartitioning is efficient

#### Scenario: Speedup for GROUP BY
- GIVEN query with GROUP BY on 5M rows
- WHEN executed with 4 workers
- THEN execution time is less than 400 units (> 2.5x speedup)
- AND aggregation merge is efficient

#### Scenario: No slowdown for small queries
- GIVEN query on 1000 rows
- WHEN executed sequentially (threshold enforcement)
- THEN execution time matches sequential execution cost
- AND parallel overhead is avoided
