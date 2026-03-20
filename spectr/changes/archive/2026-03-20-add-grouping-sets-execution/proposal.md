# Change: Complete GROUPING SETS/ROLLUP/CUBE Execution

## Why

dukdb-go has parser and binder support for GROUPING SETS, ROLLUP, and CUBE, but the executor path has significant gaps. The binder correctly expands ROLLUP/CUBE into grouping sets, and the planner propagates GroupingSets and GroupingCalls through LogicalAggregate and PhysicalHashAggregate nodes. An initial `executeHashAggregateWithGroupingSets` implementation exists in `internal/executor/operator.go` along with `computeGroupingBitmask`, but end-to-end execution has known issues:

1. GROUPING() column naming is broken: the loop in `executeHashAggregateWithGroupingSets` (operator.go:1356-1359) hardcodes the column name `"GROUPING"` for every GROUPING() call. When multiple GROUPING() calls appear in the SELECT list (e.g., `GROUPING(region) AS g1, GROUPING(product) AS g2`), all columns get the same name `"GROUPING"`, overwriting each other in the row map. The fix is to use `plan.Aliases[numGroupBy+numAgg+i]` to assign the correct alias for each GROUPING() column.
2. The `extractGroupingSets` planner function does not combine regular GROUP BY columns with grouping set expressions. When a query uses `GROUP BY department, ROLLUP(region, product)`, the regular column `department` must be prepended to each expanded grouping set so that every set groups by `department` in addition to the rollup columns. Currently, regular columns and grouping set columns are collected separately without merging.
3. No integration tests validate the full parser-to-result pipeline.

These gaps prevent users from writing standard OLAP queries with multi-level subtotals.

## What Changes

- Fix GROUPING() column naming in `executeHashAggregateWithGroupingSets` to use `plan.Aliases[numGroupBy+numAgg+i]` instead of hardcoding `"GROUPING"` for all GROUPING() columns
- Extend `extractGroupingSets` to prepend regular GROUP BY columns to each expanded grouping set when mixed GROUP BY clauses are used (e.g., `GROUP BY department, ROLLUP(region, product)`)
- Add comprehensive integration tests covering GROUPING SETS, ROLLUP, CUBE, and GROUPING()
- Add a delta spec for the advanced-grouping capability

## Impact

- Affected specs: `advanced-grouping` (new), `query-execution` (no changes needed)
- Affected code:
  - `internal/executor/operator.go` - fix GROUPING() column naming in `executeHashAggregateWithGroupingSets`
  - `internal/planner/physical.go` - fix `extractGroupingSets` to prepend regular columns to each grouping set
  - `tests/` - new integration test file for grouping sets
