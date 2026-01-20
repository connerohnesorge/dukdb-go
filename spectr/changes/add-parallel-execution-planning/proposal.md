# Change: Add Parallel Execution Planning

## Why

Currently, dukdb-go's query planner generates single-threaded execution plans. While the execution engine supports parallel operators (parallel scan, hash join, aggregation), the planner doesn't automatically generate plans designed for parallelization. This misses significant performance optimization opportunities for analytical workloads, particularly queries with large table scans, multi-way joins, and aggregations that could benefit from work distribution across CPU cores.

DuckDB v1.4.3's planner generates parallel-aware execution plans that consider:
- Data partitioning strategies for scan operators
- Pipeline barriers and operator compatibility for parallelization
- Worker thread allocation based on data volume and operation cost
- Exchange operators for data repartitioning between parallel stages

Without parallel plan generation, dukdb-go cannot achieve performance parity with DuckDB for analytical queries on multi-core systems.

## What Changes

- Add parallel plan generation capability to the query planner
- Introduce pipeline analysis to identify where parallelization can occur
- Add exchange operators for data repartitioning between parallel stages
- Enhance cost model to account for parallel execution efficiency gains
- Generate morsel-based task graphs for parallel execution
- Add EXPLAIN output to show parallelization strategy
- Integrate with existing parallel execution engine

BREAKING: No breaking changes to public API. Changes are internal to optimizer/planner.

## Impact

- **Affected specs**:
  - `planner` (add parallel plan generation)
  - `cost-based-optimizer` (enhance cost model for parallelism)
  - `parallel-execution` (consume new parallel plans)

- **Affected code**:
  - `internal/planner/` - Add parallel planning logic
  - `internal/optimizer/` - Enhance cost model
  - `internal/executor/` - Consume parallel plans (existing infrastructure reused)
  - `internal/parser/` - Potentially add EXPLAIN PARALLEL for diagnostics

- **New operators**:
  - Exchange (repartition, broadcast, gather)
  - ParallelWorkerLimit (throttle parallelism)

- **Dependencies**:
  - None on external packages
  - Builds on existing parallel execution infrastructure
