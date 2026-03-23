# Tasks: GROUP BY ALL

- [ ] 1. AST addition — Add GroupByAll bool field to SelectStmt at ast.go:34. Validate: Compiles without error.

- [ ] 2. Parser detection — In GROUP BY parsing at parser.go:364-373, detect `GROUP BY ALL` keyword (case-insensitive). Set stmt.GroupByAll = true. Handle disambiguation with column named "all" by checking next token. Validate: `SELECT a, SUM(b) FROM t GROUP BY ALL` parses without error.

- [ ] 3. Binder expansion — In SELECT binding logic, when GroupByAll is true, iterate SELECT columns, identify non-aggregate expressions using isAggregateFunc() at operator.go:99, and populate GroupBy with those expressions. Validate: `SELECT a, SUM(b) FROM t GROUP BY ALL` produces correct GROUP BY a plan.

- [ ] 4. Integration tests — Test with single/multiple non-aggregate columns, all-aggregate case, expressions, HAVING, ORDER BY. Verify GROUP BY ALL is equivalent to explicit GROUP BY.
