# Tasks: Standalone Aggregate FILTER Clause

- [ ] 1. Add Filter field to FunctionCall — Add `Filter Expr` field to FunctionCall struct (ast.go:839-846). Modify FILTER parsing at parser.go:5359-5382 to store Filter on FunctionCall when no OVER clause follows. Keep IGNORE NULLS restriction but allow FILTER without OVER. Validate: `SELECT COUNT(*) FILTER (WHERE x > 5) FROM t` parses correctly.

- [ ] 2. Add Filter to BoundFunctionCall — Add `Filter BoundExpr` field to BoundFunctionCall (binder/expressions.go:64-72). In bind_expr.go:430-451, bind the Filter expression with target type TYPE_BOOLEAN after binding OrderBy. Validate: Filter expression is bound correctly.

- [ ] 3. Apply Filter in computeAggregate — In computeAggregate() (physical_aggregate.go:295), pre-filter rows using fn.Filter before passing to aggregate logic. Follow the window FILTER pattern (physical_window.go:1469-1475): evaluate filter expression, use toBool() (expr.go:4184), skip rows where filter is false/NULL. Validate: `SELECT COUNT(*) FILTER (WHERE active) FROM t` returns correct count.

- [ ] 4. Integration tests — Test FILTER with COUNT, SUM, AVG, MIN, MAX. Test FILTER with GROUP BY. Test multiple aggregates with different filters. Test NULL filter handling. Verify FILTER still works with window OVER clause.
