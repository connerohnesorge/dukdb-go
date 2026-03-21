# Add Standalone Aggregate FILTER Clause

**Change ID:** `add-standalone-aggregate-filter-v1.4.3`
**Created:** 2026-03-21
**Status:** PROPOSED
**Scope:** Small — Add FILTER (WHERE ...) support to non-window aggregates
**Estimated Complexity:** Small — Parser already parses FILTER, just need to allow it without OVER
**User-Visible:** Yes — New SQL syntax for filtered aggregates

## Summary

Add support for the FILTER clause on non-window aggregate functions. DuckDB v1.4.3 supports `SELECT COUNT(*) FILTER (WHERE active) FROM users` as a standalone aggregate without requiring an OVER clause. Currently, dukdb-go parses FILTER but rejects it with "IGNORE NULLS and FILTER require OVER clause" (parser.go:5382).

## Verification

- FunctionCall struct (ast.go:839-846) has NO Filter field — only WindowExpr has it (ast.go:1148)
- BoundFunctionCall (binder/expressions.go:64-72) has NO Filter field
- Parser already parses FILTER (WHERE ...) at parser.go:5359-5376 but rejects without OVER at line 5382
- computeAggregate() at physical_aggregate.go:295 has no filter condition check
- Window aggregates already support FILTER (physical_window.go:1469-1475 shows the pattern)

## Current Infrastructure

- `FunctionCall` — ast.go:839-846 — Name, Args, NamedArgs, Distinct, Star, OrderBy (no Filter)
- `WindowExpr` — ast.go:1141-1150 — has `Filter Expr` at line 1148
- FILTER parsing — parser.go:5359-5376 — parses `FILTER (WHERE expr)` into windowExpr.Filter
- FILTER rejection — parser.go:5381-5382 — `"IGNORE NULLS and FILTER require OVER clause"`
- `BoundFunctionCall` — binder/expressions.go:64-72 — has no Filter field
- `computeAggregate()` — physical_aggregate.go:295 — iterates rows without filter check
- Window FILTER pattern — physical_window.go:1469-1475 — checks filter before evaluating value
- `isAggregateFunc()` — operator.go:99-122 — identifies aggregate functions

## Goals

1. Add `Filter Expr` field to FunctionCall struct
2. Remove the FILTER-requires-OVER restriction in parser
3. Add `Filter BoundExpr` field to BoundFunctionCall and bind it
4. Apply filter condition in computeAggregate() before evaluating each row
5. Follow the existing window FILTER pattern (physical_window.go:1469-1475)
