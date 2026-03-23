# Proposal: Add GROUP BY ALL

## Summary

Implement GROUP BY ALL syntax that automatically groups by all non-aggregate columns in the SELECT list. This is a DuckDB-specific convenience feature that eliminates the need to manually list all grouping columns.

## Motivation

GROUP BY ALL is one of DuckDB's most popular usability features. Instead of writing `SELECT a, b, SUM(c) FROM t GROUP BY a, b` (repeating column references), users can write `SELECT a, b, SUM(c) FROM t GROUP BY ALL`. This is especially valuable for queries with many grouping columns.

## Scope

- **AST**: Add GroupByAll bool field to SelectStmt at ast.go:34
- **Parser**: Detect `GROUP BY ALL` keyword at parse time
- **Binder/Planner**: Expand GroupByAll into actual GROUP BY columns by identifying non-aggregate SELECT expressions

## Files Affected

- `internal/parser/ast.go` — SelectStmt (line 34): add GroupByAll field
- `internal/parser/parser.go` — parseGroupBy(): detect ALL keyword
- `internal/binder/` or `internal/planner/` — expand GroupByAll to concrete columns
