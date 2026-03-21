# Add Named Window Definitions (WINDOW Clause) for DuckDB v1.4.3 Compatibility

**Change ID:** `add-named-windows-v1.4.3`
**Created:** 2026-03-20
**Scope:** Small — Parser + binder changes, no executor changes
**Estimated Complexity:** Low-Medium — Parser addition straightforward, binder merge logic moderate
**User-Visible:** Yes — New SQL syntax available

## Summary

Add support for the SQL standard WINDOW clause, which defines named window specifications reusable across multiple window functions in the same query:

```sql
SELECT
  ROW_NUMBER() OVER w AS rn,
  SUM(amount) OVER w AS total,
  AVG(amount) OVER w AS avg_amount
FROM sales
WINDOW w AS (PARTITION BY dept ORDER BY salary)
```

Also support window inheritance where an OVER clause references a named window and adds clauses:

```sql
SELECT SUM(x) OVER (w ORDER BY y)
FROM t
WINDOW w AS (PARTITION BY dept)
```

## Verification

- No `WindowDef` or `NamedWindow` types exist in `internal/parser/ast.go`
- `SelectStmt` (ast.go:34-53) has no `Windows` field
- OVER clause at `parser.go:5306-5321` only accepts `(` — does not allow bare identifier for named window reference
- `WindowExpr` (ast.go:1114-1122) has no `RefName` field for referencing named windows
- Window function execution in `physical_window.go` operates on `BoundWindowExpr` which is fully resolved — no executor changes needed

## Current Infrastructure

- Window functions fully implemented in `internal/executor/physical_window.go` (1724 lines)
- WindowExpr AST at `internal/parser/ast.go:1114-1122` with PartitionBy, OrderBy, Frame, IgnoreNulls, Filter, Distinct
- BoundWindowExpr at `internal/binder/expressions.go:270-288`
- OVER clause parsing at `parser.go:5254-5321` via `maybeParseWindowExpr()`
- Window spec parsing at `parser.go:5324` via `parseWindowSpec()`
- Binder resolves window expressions at `bind_expr.go:785-926` via `bindWindowExpr()`
- SelectStmt clause ordering: FROM → WHERE → GROUP BY → HAVING → QUALIFY → ORDER BY → LIMIT

## Goals

1. Parse `WINDOW name AS (spec) [, ...]` clause in SELECT statements
2. Allow `OVER name` (bare identifier) in addition to `OVER (spec)`
3. Support window inheritance: `OVER (name ORDER BY ...)` adds clauses to base window
4. Resolve named window references in the binder before passing to executor
5. No executor changes — binder produces fully-resolved BoundWindowExpr

## Non-Goals

- Window function implementation changes (already complete)
- New window function types
- WINDOW clause in non-SELECT contexts (INSERT, UPDATE, etc.)
