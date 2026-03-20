# Add SELECT * Modifiers for DuckDB v1.4.3 Compatibility

**Change ID:** `add-select-star-modifiers-v1.4.3`
**Created:** 2026-03-20
**Scope:** Medium — Adds EXCLUDE, REPLACE modifiers to star expressions and COLUMNS() expression
**Estimated Complexity:** Medium — Touches parser, AST, binder
**User-Visible:** Yes — New SQL syntax available

## Summary

This proposal adds three column selection features that DuckDB v1.4.3 supports:

1. **SELECT * EXCLUDE(col1, col2)** — Select all columns except the named ones
2. **SELECT * REPLACE(expr AS col)** — Select all columns with specified replacements
3. **COLUMNS(regex)** — Select columns matching a regex pattern

## Verification

All features confirmed missing via code search:

- `EXCLUDE` in star context: Not in `internal/parser/parser.go` `parseSelectColumns()` (line 576). The `StarExpr` struct (ast.go:892) has no Exclude field. `bindStarExpr()` (bind_expr.go:643) has no exclusion logic.
- `REPLACE` in star context: Not in parser or binder. `StarExpr` has no Replace field.
- `COLUMNS` expression: No `ColumnsExpr` type in ast.go. No COLUMNS keyword handling in expression parser.

## Current Star Expression Architecture

```
Parser: parseSelectColumns() at parser.go:576
  → Detects tokenStar at line 580
  → Creates SelectColumn{Star: true, Expr: &StarExpr{Table: ""}} at lines 585-586

AST: StarExpr{Table string} at ast.go:892-894

Binder: bindSelect() at bind_stmt.go:84-108
  → Checks col.Star at line 85
  → Calls bindStarExpr() at bind_expr.go:643
  → Expands to all columns from scope.tables

Bound: BoundStarExpr{Table string, Columns []*BoundColumn} at expressions.go:188-191
```

## Goals

1. Extend `StarExpr` AST node with EXCLUDE and REPLACE fields
2. Parse EXCLUDE(col, ...) and REPLACE(expr AS col, ...) after star
3. Apply exclusions and replacements during star expansion in binder
4. Add `ColumnsExpr` AST node with regex pattern
5. Expand COLUMNS in binder using table schema column names

## Non-Goals

- COLUMNS with lambda expressions (DuckDB advanced feature)
- COLUMNS in non-SELECT contexts (GROUP BY, ORDER BY) — future enhancement
- Combining EXCLUDE + REPLACE in a single expression (DuckDB allows it, but defer for simplicity)

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| EXCLUDE keyword conflicts with window frame EXCLUDE | Low | Medium | Context-sensitive: only after star, not in OVER clause |
| COLUMNS regex performance on wide tables | Low | Low | Regex compiled once per query, matched against column names |
| REPLACE expression binding scope | Medium | Low | Expressions in REPLACE can reference same table columns |
