# Tasks: Add SELECT * Modifiers for DuckDB v1.4.3

- [ ] 1. Extend StarExpr AST node with EXCLUDE and REPLACE fields — Add `Exclude []string` and `Replace []ReplaceColumn` fields to `StarExpr` in `internal/parser/ast.go:892`. Add `ReplaceColumn` struct with `Expr Expr` and `Column string` fields. Validate: AST compiles with no errors.

- [ ] 2. Add ColumnsExpr AST node — Add `ColumnsExpr` struct with `Pattern string` field to `internal/parser/ast.go`. Implement the `Expr` interface. Validate: AST compiles.

- [ ] 3. Parse EXCLUDE modifier after star — In `parseSelectColumns()` at `internal/parser/parser.go:576`, after creating StarExpr (line 586), check for `EXCLUDE` keyword and parse parenthesized column name list. Validate: `SELECT * EXCLUDE(col1, col2) FROM t` parses to StarExpr with Exclude=["col1","col2"]. `SELECT * FROM t` still works unchanged.

- [ ] 4. Parse REPLACE modifier after star — In `parseSelectColumns()`, after EXCLUDE parsing, check for `REPLACE` keyword and parse parenthesized `expr AS col` list. Validate: `SELECT * REPLACE(UPPER(name) AS name) FROM t` parses correctly. Multiple replacements: `SELECT * REPLACE(a+1 AS a, b*2 AS b) FROM t`.

- [ ] 5. Parse COLUMNS expression — In `parseUnaryOrPrimary()` in `internal/parser/parser.go` (around line 4793), before function call parsing, detect `COLUMNS` keyword followed by parenthesized string literal. Create `ColumnsExpr{Pattern: pattern}`. Validate: `SELECT COLUMNS('price_.*') FROM t` parses correctly.

- [ ] 6. Apply EXCLUDE in binder star expansion — Modify `bindStarExpr()` in `internal/binder/bind_expr.go:643` to filter out excluded columns after collecting them. Validate excluded column names exist (error if not). Validate: `CREATE TABLE t(a INT, b INT, c INT); SELECT * EXCLUDE(b) FROM t` returns columns a, c.

- [ ] 7. Apply REPLACE in binder star expansion — Extend `BoundStarExpr` (expressions.go:188) with `Replacements map[string]BoundExpr`. In `bindStarExpr()`, bind replacement expressions and store in map. In `bindSelect()` (bind_stmt.go:85-108), substitute replacement expressions when expanding star columns. Validate: `CREATE TABLE t(a INT, b VARCHAR); SELECT * REPLACE(UPPER(b) AS b) FROM t` returns a as-is and b uppercased.

- [ ] 8. Bind COLUMNS expression — Add `bindColumnsExpr()` to `internal/binder/bind_expr.go`. Compile regex, match against all columns in scope, return `BoundStarExpr` with matched columns. Error on invalid regex or no matches. Validate: `CREATE TABLE t(price_usd INT, price_eur INT, name VARCHAR); SELECT COLUMNS('price_.*') FROM t` returns price_usd and price_eur.

- [ ] 9. Integration tests — Write comprehensive tests covering: EXCLUDE with single/multiple columns, EXCLUDE with table-qualified star (`t.* EXCLUDE(col)`), REPLACE with expressions, COLUMNS with various regex patterns, error cases (non-existent column in EXCLUDE, invalid regex in COLUMNS, empty COLUMNS match). Verify no regressions in existing SELECT * tests.
