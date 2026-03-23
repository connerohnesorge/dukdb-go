# Design: GROUP BY ALL

## Architecture

Two-phase change: (1) parser detects `GROUP BY ALL` keyword and sets flag, (2) binder expands the flag into concrete grouping columns based on non-aggregate SELECT expressions.

## 1. AST Change (ast.go:34)

Add GroupByAll field to SelectStmt:

```go
type SelectStmt struct {
    CTEs       []CTE
    Distinct   bool
    DistinctOn []Expr
    Columns    []SelectColumn
    From       *FromClause
    Where      Expr
    GroupBy    []Expr
    GroupByAll bool   // NEW: true when GROUP BY ALL is used
    Having     Expr
    // ... rest unchanged
}
```

When GroupByAll is true, GroupBy should be empty (the actual grouping columns are computed later by the binder).

## 2. Parser Change (parser.go:363-373)

The GROUP BY parsing at line 364-373 calls `parseGroupByList()`. Before calling it, check for `GROUP BY ALL`:

```go
// GROUP BY
if p.isKeyword("GROUP") {
    p.advance()
    if err := p.expectKeyword("BY"); err != nil {
        return nil, err
    }
    // Check for GROUP BY ALL
    if p.isKeyword("ALL") {
        p.advance()
        stmt.GroupByAll = true
    } else {
        groupBy, err := p.parseGroupByList()
        if err != nil {
            return nil, err
        }
        stmt.GroupBy = groupBy
    }

    // HAVING (unchanged)
    if p.isKeyword("HAVING") {
```

Note: `p.isKeyword("ALL")` checks if the current token is the keyword ALL. This is at parser.go:364, before line 369 calls parseGroupByList().

### Disambiguation: ALL as column name vs keyword

`ALL` might be a column name in `GROUP BY all` (lowercase column reference). In DuckDB, GROUP BY ALL is case-insensitive. We check: if the token after GROUP BY is the identifier "ALL" (case-insensitive) AND it's not followed by a comma or expression operator, treat it as GROUP BY ALL. Otherwise, fall through to normal parsing.

More precisely: if the current token is "ALL" (case-insensitive) and the NEXT token is NOT a comma, dot, or operator (meaning it's not part of an expression like `all.col` or `all + 1`), treat it as GROUP BY ALL.

```go
if p.current().typ == tokenIdent && strings.ToUpper(p.current().value) == "ALL" {
    next := p.peek()
    // GROUP BY ALL must be followed by HAVING, QUALIFY, WINDOW, ORDER, LIMIT, OFFSET, FETCH, ), ;, or EOF
    if next.typ == tokenSemicolon || next.typ == tokenEOF || next.typ == tokenRParen ||
       (next.typ == tokenIdent && isClauseKeyword(next.value)) {
        p.advance()
        stmt.GroupByAll = true
    } else {
        // "all" is a column name, parse normally
        groupBy, err := p.parseGroupByList()
        // ...
    }
}
```

## 3. Binder/Planner Expansion

The binder must expand GroupByAll into concrete GROUP BY columns. This requires:

1. Identify all SELECT columns
2. Identify which columns contain aggregate functions
3. GROUP BY = all non-aggregate columns

### Implementation location

The binder processes SELECT statements in bind_stmt.go. After binding SELECT columns and before processing GROUP BY, check if GroupByAll is true and compute the grouping columns.

```go
// In the SELECT binding logic:
if stmt.GroupByAll {
    var groupByExprs []Expr
    for _, col := range stmt.Columns {
        if !containsAggregate(col.Expr) {
            groupByExprs = append(groupByExprs, col.Expr)
        }
    }
    stmt.GroupBy = groupByExprs
}
```

The `containsAggregate()` function can walk the expression tree and check if any node is an aggregate function call (using isAggregateFunc() at operator.go:99). Window functions should NOT be considered aggregates for this purpose.

### Edge cases:

1. `SELECT *, SUM(x) FROM t GROUP BY ALL` — `*` expands to all columns first, then non-aggregates become GROUP BY
2. `SELECT a + b, SUM(c) FROM t GROUP BY ALL` — expression `a + b` becomes a GROUP BY expression
3. `SELECT SUM(a), SUM(b) FROM t GROUP BY ALL` — all columns are aggregates, GROUP BY is empty (equivalent to no GROUP BY with aggregates)
4. `SELECT a, SUM(b) OVER () FROM t GROUP BY ALL` — window functions are NOT aggregates, so `a` is in GROUP BY and the window function is not

## Helper Signatures Reference (Verified)

- SelectStmt — ast.go:34-54 — SELECT statement AST node
- GroupBy field — ast.go:41 — current GROUP BY columns
- GROUP BY parsing — parser.go:363-373 — where GROUP BY is parsed
- parseGroupByList() — parser.go:3951 — parses GROUP BY expression list
- isAggregateFunc() — operator.go:99-124 — checks if function name is aggregate
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeBinder, Msg: ...}`

## Testing Strategy

1. `SELECT a, SUM(b) FROM t GROUP BY ALL` → groups by a
2. `SELECT a, b, COUNT(*) FROM t GROUP BY ALL` → groups by a, b
3. `SELECT a + 1, SUM(b) FROM t GROUP BY ALL` → groups by a + 1
4. `SELECT SUM(a), AVG(b) FROM t GROUP BY ALL` → no grouping (all aggregates)
5. `SELECT a, SUM(b) FROM t GROUP BY ALL HAVING SUM(b) > 10` → works with HAVING
6. `SELECT a, SUM(b) FROM t GROUP BY ALL ORDER BY a` → works with ORDER BY
