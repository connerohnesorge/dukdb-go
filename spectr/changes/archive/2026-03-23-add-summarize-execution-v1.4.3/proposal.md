# Change: Wire SUMMARIZE Statement Execution for DuckDB v1.4.3 Compatibility

**Change ID:** `add-summarize-execution-v1.4.3`
**Created:** 2026-03-22
**Status:** PROPOSED
**Scope:** Small — Planner, executor
**Estimated Complexity:** Small-Medium — Parser exists; need planner node and executor that computes per-column statistics
**User-Visible:** Yes — Enables quick table/query statistics inspection

## Why

DuckDB v1.4.3 supports `SUMMARIZE table_name` and `SUMMARIZE SELECT ...` to quickly show per-column statistics (column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage). The dukdb-go parser already has `SummarizeStmt` (ast.go:1742) but no planner or executor handles it. This is a commonly used data exploration feature.

## What Changes

- **Planner**: Add `PhysicalSummarize` plan node
- **Executor**: Compute per-column statistics by scanning all rows, then return a result set with one row per column containing: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage

## Impact

- Affected specs: `summarize-statement` (new capability)
- Affected code:
  - `internal/planner/physical.go` — new physical plan node
  - `internal/executor/physical_summarize.go` — new executor file
  - `internal/executor/operator.go` — operator type registration
