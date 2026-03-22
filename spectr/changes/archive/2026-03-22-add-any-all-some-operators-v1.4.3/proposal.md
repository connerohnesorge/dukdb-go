# Proposal: ANY/ALL/SOME Quantified Comparison Operators

## Summary

Implement SQL standard quantified comparison operators: `expr op ANY(subquery)`, `expr op ALL(subquery)`, and `expr op SOME(subquery)`. These allow comparing a scalar value against all rows returned by a subquery using any comparison operator.

## Motivation

ANY/ALL/SOME are SQL standard operators supported by DuckDB v1.4.3, PostgreSQL, and most SQL databases. They enable patterns like `WHERE price > ALL(SELECT avg_price FROM benchmarks)` and `WHERE id = ANY(SELECT related_id FROM links)`. Currently, dukdb-go has no parser, binder, or executor support for these operators.

## Scope

- **Parser**: New AST node (QuantifiedComparisonExpr), parse `op ANY/ALL/SOME (subquery)` after comparison operator
- **Binder**: New bound expression (BoundQuantifiedComparison), bind the subquery
- **Executor**: Evaluate by running subquery and comparing each row
- **Planner**: Thread through existing subquery planning

## Files Affected

- `internal/parser/ast.go` — new QuantifiedComparisonExpr AST node
- `internal/parser/parser.go` — parse ANY/ALL/SOME after comparison operators
- `internal/binder/expressions.go` — new BoundQuantifiedComparison
- `internal/binder/bind_expr.go` — bind quantified comparisons
- `internal/executor/expr.go` — evaluate quantified comparisons
