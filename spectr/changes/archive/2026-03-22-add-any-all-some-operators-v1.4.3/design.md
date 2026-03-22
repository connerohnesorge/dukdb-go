# Design: ANY/ALL/SOME Quantified Comparison Operators

## Architecture

This is a full-stack feature requiring changes to parser, binder, and executor. The pattern `expr op ANY(subquery)` is syntactically similar to `expr IN (subquery)` which is already implemented (InSubqueryExpr at ast.go:917-924).

## 1. AST Changes (ast.go)

### New AST Node

Add after InSubqueryExpr (ast.go:924):

```go
// QuantifiedComparisonExpr represents expr op ANY|ALL|SOME (subquery).
type QuantifiedComparisonExpr struct {
    Left     Expr          // Left-hand expression
    Op       BinaryOp      // Comparison operator (=, <>, <, <=, >, >=)
    Quantifier string      // "ANY", "ALL", or "SOME"
    Subquery *SelectStmt   // Right-hand subquery
}

func (*QuantifiedComparisonExpr) exprNode() {}
```

### Quantifier type

SOME is a synonym for ANY (SQL standard). We normalize SOME → ANY during parsing.

## 2. Parser Changes (parser.go)

### Where to parse

Comparison expressions are parsed in the expression precedence parser. After parsing `left op right`, we need to check if `right` is an ANY/ALL/SOME keyword followed by a subquery.

The comparison parsing happens in `parseComparison()` or the binary expression parsing chain. The key is: when we see `> ANY(`, `= ALL(`, etc., we need to intercept before treating the right side as a regular expression.

In parseIdentExpr() at parser.go:5035, the identifiers ANY, ALL, SOME would be parsed as regular identifiers. We need to handle them in the comparison context.

### Approach

After parsing a comparison operator (=, <>, <, <=, >, >=), check if the next token is an identifier matching ANY/ALL/SOME followed by `(`. If so, parse a subquery inside the parentheses.

The binary expression parser at parser.go (parseBinaryExprPrec or similar) handles operator precedence. After parsing `left op`, it parses the right side. We intercept here:

```go
// In the binary expression parser, after parsing the operator:
if isComparisonOp(op) {
    // Check for quantified comparison: op ANY|ALL|SOME (subquery)
    if p.current().typ == tokenIdent {
        upper := strings.ToUpper(p.current().value)
        if (upper == "ANY" || upper == "ALL" || upper == "SOME") && p.peek().typ == tokenLParen {
            quantifier := upper
            if quantifier == "SOME" {
                quantifier = "ANY"  // SOME is alias for ANY
            }
            p.advance() // consume ANY/ALL/SOME
            p.advance() // consume (
            subquery, err := p.parseSelect()
            if err != nil {
                return nil, err
            }
            if _, err := p.expect(tokenRParen); err != nil {
                return nil, err
            }
            return &QuantifiedComparisonExpr{
                Left:       left,
                Op:         op,
                Quantifier: quantifier,
                Subquery:   subquery,
            }, nil
        }
    }
}
```

### Finding the exact parse location

The comparison parsing in the parser likely happens in `parseBinaryExpr()` or a similar function. Need to find where binary comparison expressions are constructed. The key functions to check:

- Look for where `BinaryExpr` with comparison operators is created
- Likely in the expression precedence climbing parser
- After parsing `left op`, before parsing the right operand

Let me identify the exact location by looking at how the parser handles binary expressions:

The parser uses precedence climbing. The function that handles this is likely `parseBinary()` or `parseExprPrec()`. The comparison operators are parsed as part of the binary expression chain.

The approach: in the parseComparisonOrBinary function, after reading a comparison operator token, peek at the next token. If it's ANY/ALL/SOME followed by `(`, create a QuantifiedComparisonExpr instead of a regular BinaryExpr.

## 3. Binder Changes (binder/expressions.go + bind_expr.go)

### New Bound Expression

```go
// BoundQuantifiedComparison represents a bound expr op ANY|ALL (subquery).
type BoundQuantifiedComparison struct {
    Left       BoundExpr
    Op         parser.BinaryOp
    Quantifier string        // "ANY" or "ALL"
    Subquery   *BoundSelectStmt  // or whatever the bound subquery type is
    ResType    dukdb.Type
}

func (*BoundQuantifiedComparison) boundExprNode() {}
func (q *BoundQuantifiedComparison) ResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }
```

### Binding

Follow the InSubqueryExpr binding pattern (search for `InSubqueryExpr` in bind_expr.go):

```go
case *parser.QuantifiedComparisonExpr:
    left, err := b.bindExpr(e.Left)
    if err != nil {
        return nil, err
    }
    // Bind subquery following InSubqueryExpr pattern
    subquery, err := b.bindSubquery(e.Subquery)
    if err != nil {
        return nil, err
    }
    return &BoundQuantifiedComparison{
        Left:       left,
        Op:         e.Op,
        Quantifier: e.Quantifier,
        Subquery:   subquery,
    }, nil
```

## 4. Executor Changes (expr.go)

### Evaluation

Follow the InSubqueryExpr evaluation pattern. Run the subquery, iterate results:

```go
case *binder.BoundQuantifiedComparison:
    // Evaluate left expression
    leftVal, err := e.evaluateExpr(ctx, expr.Left, row)
    if err != nil {
        return nil, err
    }
    if leftVal == nil {
        return nil, nil  // NULL op ANY/ALL → NULL
    }

    // Execute subquery
    subRows, err := e.executeSubquery(ctx, expr.Subquery)
    if err != nil {
        return nil, err
    }

    if expr.Quantifier == "ANY" {
        // ANY: true if comparison is true for at least one row
        hasNull := false
        for _, subRow := range subRows {
            // Get first column of subquery result
            for _, v := range subRow {
                if v == nil {
                    hasNull = true
                    continue
                }
                result, err := evaluateBinaryOp(leftVal, expr.Op, v)
                if err != nil {
                    return nil, err
                }
                if toBool(result) {
                    return true, nil
                }
                break // only first column
            }
        }
        if hasNull {
            return nil, nil  // NULL if any comparison was against NULL and none were true
        }
        return false, nil
    } else { // ALL
        // ALL: true if comparison is true for every row
        hasNull := false
        for _, subRow := range subRows {
            for _, v := range subRow {
                if v == nil {
                    hasNull = true
                    continue
                }
                result, err := evaluateBinaryOp(leftVal, expr.Op, v)
                if err != nil {
                    return nil, err
                }
                if !toBool(result) {
                    return false, nil
                }
                break // only first column
            }
        }
        if len(subRows) == 0 {
            return true, nil  // ALL of empty set is true
        }
        if hasNull {
            return nil, nil
        }
        return true, nil
    }
```

### evaluateBinaryOp helper

Need a helper that takes two values and a BinaryOp and returns the comparison result. This may already exist in the executor — check for `evaluateBinaryExpr` or similar that can be reused.

## Semantics

- `x = ANY(subquery)` — equivalent to `x IN (subquery)`
- `x <> ALL(subquery)` — equivalent to `x NOT IN (subquery)`
- `x > ANY(subquery)` — true if x > at least one subquery value
- `x > ALL(subquery)` — true if x > every subquery value
- `x op ANY(empty)` — false (no values match)
- `x op ALL(empty)` — true (vacuous truth)
- NULL handling: if left is NULL → NULL; if comparing against NULL row → NULL unless already determined

## Helper Signatures Reference (Verified)

- InSubqueryExpr — ast.go:917-924 — pattern for subquery expressions
- ExistsExpr — ast.go:926-932 — pattern for subquery expressions
- BinaryExpr — ast.go:743-750 — comparison expression
- BinaryOp — ast.go:771-793 — operator constants (OpEq, OpNe, OpLt, OpLe, OpGt, OpGe)
- parseIdentExpr() — parser.go:5035 — identifier parsing (where ANY/ALL/SOME keywords appear)
- InSubqueryExpr binding — binder/bind_expr.go — pattern for subquery binding
- InSubqueryExpr evaluation — executor/expr.go — pattern for subquery execution
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT 1 = ANY(SELECT 1 UNION SELECT 2)` → true
2. `SELECT 3 = ANY(SELECT 1 UNION SELECT 2)` → false
3. `SELECT 1 > ALL(SELECT 0 UNION SELECT -1)` → true
4. `SELECT 1 > ALL(SELECT 0 UNION SELECT 2)` → false
5. `SELECT 1 = SOME(SELECT 1)` → true (SOME = ANY alias)
6. `SELECT 1 = ANY(SELECT NULL)` → NULL (comparison with NULL)
7. `SELECT 1 = ALL(SELECT * FROM empty_table)` → true (vacuous truth)
8. `SELECT NULL = ANY(SELECT 1)` → NULL (NULL left side)
9. `SELECT x FROM t WHERE x > ANY(SELECT threshold FROM config)` → subquery in WHERE
