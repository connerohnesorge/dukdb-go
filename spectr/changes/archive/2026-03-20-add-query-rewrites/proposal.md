# Change: Add Query Rewrite Transformations

## Why

Query rewriting is a fundamental optimization technique that transforms logical plans into semantically equivalent but more efficient forms. DuckDB v1.4.3 implements a comprehensive set of query rewrite rules that:
- Simplify expressions (e.g., `x AND TRUE` → `x`)
- Eliminate redundant operations (e.g., `DISTINCT` after unique join)
- Reorder operations for efficiency (e.g., move filters earlier)
- Convert between equivalent forms (e.g., `NOT IN` → `LEFT JOIN`)

Currently, dukdb-go lacks a systematic query rewrite framework. This results in suboptimal plans that execute unnecessary operations. A rule-based rewrite engine would significantly improve query efficiency and reduce execution time without requiring changes to user queries.

## What Changes

- Add a rule-based query rewrite engine
- Implement standard rewrite rules (50+ rules based on DuckDB)
- Add cost-aware rule application
- Integrate with planner to apply rewrites before physical planning
- Add EXPLAIN diagnostics showing applied rewrite rules
- Build infrastructure for users to register custom rules (future)

BREAKING: No breaking changes. Internal optimizer improvements only.

## Impact

- **Affected specs**:
  - `planner` - Add rewrite phase
  - `cost-based-optimizer` - Enhanced logical optimizations
  - New spec: `query-rewrites` - Formalize rewrite rules

- **Affected code**:
  - `internal/planner/` - Add rewrite engine
  - `internal/optimizer/` - Move rewrite logic if any exists
  - `internal/parser/` - Expression rewriting infrastructure

- **New operators**: None (transforms existing operators)

- **Dependencies**:
  - None on external packages
  - Builds on existing logical plan structures
