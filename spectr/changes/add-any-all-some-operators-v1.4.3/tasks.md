# Tasks: ANY/ALL/SOME Quantified Comparison Operators

- [ ] 1. AST node — Add QuantifiedComparisonExpr to ast.go after InSubqueryExpr (line 924). Fields: Left (Expr), Op (BinaryOp), Quantifier (string), Subquery (*SelectStmt). Add exprNode() method. Add to Visitor interface and table_extractor.go.

- [ ] 2. Parser — In the binary expression parser, after parsing a comparison operator (=, <>, <, <=, >, >=), check if next token is ANY/ALL/SOME identifier followed by `(`. If so, parse subquery inside parens and return QuantifiedComparisonExpr. Normalize SOME → ANY. Validate: `SELECT 1 = ANY(SELECT 1)` parses without error.

- [ ] 3. Binder — Add BoundQuantifiedComparison to binder/expressions.go. Add case for QuantifiedComparisonExpr in bind_expr.go following InSubqueryExpr binding pattern. ResultType() returns TYPE_BOOLEAN. Validate: Binds without error.

- [ ] 4. Executor — Add case for BoundQuantifiedComparison in expr.go expression evaluation. Execute subquery, iterate results. ANY: return true if any comparison matches. ALL: return true if all comparisons match. Handle NULL semantics. Empty set: ANY→false, ALL→true. Validate: `SELECT 1 = ANY(SELECT 1 UNION ALL SELECT 2)` returns true.

- [ ] 5. Integration tests — Test ANY, ALL, SOME with all comparison operators. Test NULL handling. Test empty subqueries. Test with WHERE clauses. Test nested usage.
