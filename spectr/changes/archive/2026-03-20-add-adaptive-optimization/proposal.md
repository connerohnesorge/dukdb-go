# Change: Add Adaptive Query Optimization

## Why

Static query optimization relies on estimates that can be wrong, leading to suboptimal plans. DuckDB v1.4.3 implements adaptive optimization that:
- Tracks actual vs estimated cardinalities during execution
- Learns correction factors for future similar queries
- Adjusts cost constants based on observed performance
- Detects plan anomalies and triggers re-optimization mid-query

Currently, dukdb-go uses static optimization based on initial cost estimates. Adaptive optimization would:
- Improve estimates for queries that repeat with similar parameters
- Self-tune performance on new hardware/workloads
- Handle changing data distributions automatically
- Provide visibility into estimation errors for debugging

This enables dukdb-go to achieve near-optimal plans even with imperfect statistics, matching DuckDB's adaptability.

## What Changes

- Add cardinality learning mechanism to track actual vs estimated
- Implement correction factor application for similar queries
- Build execution profile collection during query execution
- Extend EXPLAIN ANALYZE to show estimation accuracy
- Add statistics invalidation when data changes significantly
- Integrate learning with planner for future query optimization

BREAKING: No breaking changes. Internal optimizer improvements only.

## Impact

- **Affected specs**:
  - `cost-based-optimizer` - Extend with learning mechanisms
  - `planner` - Use learned corrections
  - New spec: `adaptive-optimization` - Formalize adaptive techniques

- **Affected code**:
  - `internal/optimizer/` - Add learning mechanisms
  - `internal/executor/` - Collect execution profiles
  - `internal/storage/` - Track metadata changes

- **New components**:
  - Cardinality learning cache
  - Execution profile collector
  - Correction factor calculator

- **Dependencies**:
  - None on external packages
  - Builds on existing cost model and execution tracking
