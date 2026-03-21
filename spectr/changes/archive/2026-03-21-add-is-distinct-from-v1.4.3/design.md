# Design: IS DISTINCT FROM / IS NOT DISTINCT FROM

## Architecture

This change touches three layers:

1. **Parser** (`ast.go`, `parser.go`): New BinaryOp values, extend IS keyword handling
2. **Binder** (`bind_expr.go`): Return TYPE_BOOLEAN for these operators
3. **Executor** (`expr.go`): NULL-safe comparison logic

## 1. AST Changes (internal/parser/ast.go)

Add two new `BinaryOp` constants after `OpShiftRight` (line 788):

```go
// Distinct comparison operators (NULL-safe)
OpIsDistinctFrom    // IS DISTINCT FROM (NULL-safe not-equal)
OpIsNotDistinctFrom // IS NOT DISTINCT FROM (NULL-safe equal)
```

## 2. Parser Changes (internal/parser/parser.go)

### Current IS handling

The IS keyword is parsed in `parseComparisonExpr()` at line 4115. Currently (lines 4121-4143), it ONLY handles `IS [NOT] NULL` and creates **UnaryExpr** objects:

```go
// IS NULL / IS NOT NULL
if p.isKeyword("IS") {
    p.advance()
    not := false
    if p.isKeyword("NOT") {
        p.advance()
        not = true
    }
    if err := p.expectKeyword("NULL"); err != nil {  // ← ONLY expects NULL
        return nil, err
    }
    // ... creates UnaryExpr with OpIsNull / OpIsNotNull
}
```

**Important:** The `OpIs` and `OpIsNot` BinaryOp enum values exist (ast.go:775-776) but are NEVER created by the parser. They are handled in the executor but unreachable. IS TRUE/IS FALSE are also not parsed here.

### Required changes

Replace the `if err := p.expectKeyword("NULL")` with a branch that also checks for DISTINCT:

```go
// IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM
if p.isKeyword("IS") {
    p.advance()
    not := false
    if p.isKeyword("NOT") {
        p.advance()
        not = true
    }

    // IS [NOT] DISTINCT FROM expr
    if p.isKeyword("DISTINCT") {
        p.advance() // consume DISTINCT
        if err := p.expectKeyword("FROM"); err != nil {
            return nil, err
        }
        right, err := p.parseBitwiseOrExpr()
        if err != nil {
            return nil, err
        }
        op := OpIsDistinctFrom
        if not {
            op = OpIsNotDistinctFrom
        }
        return &BinaryExpr{Left: left, Op: op, Right: right}, nil
    }

    // IS [NOT] NULL (existing behavior)
    if err := p.expectKeyword("NULL"); err != nil {
        return nil, err
    }
    if not {
        return &UnaryExpr{Op: OpIsNotNull, Expr: left}, nil
    }
    return &UnaryExpr{Op: OpIsNull, Expr: left}, nil
}
```

**Key insight:** The DISTINCT check must come BEFORE the NULL expectation. After consuming IS [NOT], check for DISTINCT first. If not DISTINCT, fall through to the existing NULL handling.

**Right-hand expression:** Uses `p.parseBitwiseOrExpr()` (NOT `p.parseExpr()`) to match the pattern used by other binary operators in `parseComparisonExpr()` (see LIKE at line 4272, SIMILAR TO at line 4348).

## 3. Executor Changes (internal/executor/expr.go)

There are two code paths that evaluate binary expressions with NULL handling:

### Path 1: evaluateBinaryExpr (lines 317-355)

This handles the case where one or both operands is nil. OpIs/OpIsNot are handled at lines 347-352 inside the `if left == nil || right == nil` block. Add the new operators in the same block:

```go
case parser.OpIsDistinctFrom:
    if left == nil && right == nil {
        return false, nil  // both NULL → not distinct
    }
    if left == nil || right == nil {
        return true, nil   // one NULL → distinct
    }
    // Both non-NULL: fall through to normal comparison below
    return compareValues(left, right) != 0, nil

case parser.OpIsNotDistinctFrom:
    if left == nil && right == nil {
        return true, nil   // both NULL → not distinct (equal)
    }
    if left == nil || right == nil {
        return false, nil  // one NULL → distinct (not equal)
    }
    return compareValues(left, right) == 0, nil
```

**Placement:** These cases must be INSIDE the `if left == nil || right == nil` block alongside OpIs/OpIsNot, BUT they also need to handle the case where NEITHER is nil (both non-NULL comparison). So they should be placed as early-return cases that handle ALL NULL combinations plus the non-NULL case, BEFORE the block's general `return nil, nil` fallthrough.

### Path 2: evaluateParserBinaryOp (lines 476-509)

Same pattern — OpIs/OpIsNot at lines 502-505. Add identical cases here.

### compareValues location

`compareValues(a, b any) int` is at `expr.go:3766` (NOT 3457 as previously cited). Returns 0 for equal, <0 for a<b, >0 for a>b.

## 4. Binder Changes (internal/binder/bind_expr.go)

Binary operator type inference is at `bind_expr.go:239-256`. OpIs/OpIsNot already return TYPE_BOOLEAN (lines 254-256). Add the new operators to the same case:

```go
case parser.OpIs, parser.OpIsNot, parser.OpIsDistinctFrom, parser.OpIsNotDistinctFrom:
    return dukdb.TYPE_BOOLEAN, nil
```

## Semantics Reference

| a       | b       | a IS DISTINCT FROM b | a IS NOT DISTINCT FROM b |
|---------|---------|---------------------|-------------------------|
| 1       | 1       | false               | true                    |
| 1       | 2       | true                | false                   |
| 1       | NULL    | true                | false                   |
| NULL    | 1       | true                | false                   |
| NULL    | NULL    | false               | true                    |

## Helper Signatures Reference

- `compareValues(a, b any) int` — `expr.go:3766` — returns 0 for equal, <0 for a<b, >0 for a>b
- `parseBitwiseOrExpr()` — used for right-hand side of binary operators in parseComparisonExpr

## Testing Strategy

Integration tests:
1. Both non-NULL equal: `SELECT 1 IS DISTINCT FROM 1` → false
2. Both non-NULL different: `SELECT 1 IS DISTINCT FROM 2` → true
3. Left NULL: `SELECT NULL IS DISTINCT FROM 1` → true
4. Right NULL: `SELECT 1 IS DISTINCT FROM NULL` → true
5. Both NULL: `SELECT NULL IS DISTINCT FROM NULL` → false
6. IS NOT DISTINCT FROM: mirror tests with opposite results
7. In WHERE clause: `SELECT * FROM t WHERE a IS NOT DISTINCT FROM b`
8. With different types: strings, dates, etc.
9. In JOIN condition: `ON a.x IS NOT DISTINCT FROM b.x`
