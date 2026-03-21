# Design: IS DISTINCT FROM / IS NOT DISTINCT FROM

## Architecture

This change touches three layers:

1. **Parser** (`ast.go`, `parser.go`): New BinaryOp values, parse IS [NOT] DISTINCT FROM syntax
2. **Binder** (`utils.go`): Return TYPE_BOOLEAN for these operators
3. **Executor** (`expr.go`): NULL-safe comparison logic

## 1. AST Changes (internal/parser/ast.go)

Add two new `BinaryOp` constants after `OpShiftRight` (line 788):

```go
// Distinct comparison operators (NULL-safe)
OpIsDistinctFrom    // IS DISTINCT FROM (NULL-safe not-equal)
OpIsNotDistinctFrom // IS NOT DISTINCT FROM (NULL-safe equal)
```

## 2. Parser Changes (internal/parser/parser.go)

The parser already handles `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`, `IS`, `IS NOT` in expression parsing. The IS keyword parsing needs to be extended to also recognize `IS [NOT] DISTINCT FROM`.

Find where `IS` is parsed in postfix expression handling. The pattern `IS` followed by `DISTINCT` followed by `FROM` should produce `OpIsDistinctFrom`. The pattern `IS NOT DISTINCT FROM` should produce `OpIsNotDistinctFrom`.

Look for the IS handling in `parseComparison()` or equivalent function. The current logic likely:
1. Sees IS keyword
2. Checks for NOT → IS NOT
3. Checks for NULL/TRUE/FALSE
4. Falls through to IS (OpIs) / IS NOT (OpIsNot)

Insert check for DISTINCT keyword before the fallthrough:

```go
// After IS or IS NOT keyword...
if p.isKeyword("DISTINCT") {
    p.advance() // consume DISTINCT
    if err := p.expectKeyword("FROM"); err != nil {
        return nil, err
    }
    right, err := p.parseExpr()
    if err != nil {
        return nil, err
    }
    if isNot {
        return &BinaryExpr{Left: left, Op: OpIsNotDistinctFrom, Right: right}, nil
    }
    return &BinaryExpr{Left: left, Op: OpIsDistinctFrom, Right: right}, nil
}
```

## 3. Executor Changes (internal/executor/expr.go)

Add cases in the binary expression evaluation. The key locations are:

**NULL handling path** (around line 337-355, where OpIs/OpIsNot are handled):

```go
case parser.OpIsDistinctFrom:
    // NULL-safe not-equal: true if values differ or exactly one is NULL
    if left == nil && right == nil {
        return false, nil  // both NULL → not distinct
    }
    if left == nil || right == nil {
        return true, nil   // exactly one NULL → distinct
    }
    // Both non-NULL: compare normally
    return compareValues(left, right) != 0, nil

case parser.OpIsNotDistinctFrom:
    // NULL-safe equal: true if both NULL or both equal
    if left == nil && right == nil {
        return true, nil   // both NULL → not distinct
    }
    if left == nil || right == nil {
        return false, nil  // exactly one NULL → distinct
    }
    // Both non-NULL: compare normally
    return compareValues(left, right) == 0, nil
```

**Important:** These cases must be placed in the NULL-handling path (before the `if left == nil || right == nil` early return that handles other operators), similar to how OpIs/OpIsNot are placed.

There are two NULL-handling blocks in the executor:
1. Line 337-355: Handles the case where one or both operands is nil
2. Line 502-508: Similar handler in a different code path

Both need the new cases added.

## 4. Binder Changes (internal/binder)

In the binary operator type inference, these operators always return `dukdb.TYPE_BOOLEAN`:

```go
case parser.OpIsDistinctFrom, parser.OpIsNotDistinctFrom:
    return dukdb.TYPE_BOOLEAN
```

## Semantics Reference

| a       | b       | a IS DISTINCT FROM b | a IS NOT DISTINCT FROM b |
|---------|---------|---------------------|-------------------------|
| 1       | 1       | false               | true                    |
| 1       | 2       | true                | false                   |
| 1       | NULL    | true                | false                   |
| NULL    | 1       | true                | false                   |
| NULL    | NULL    | false               | true                    |

Equivalent to:
- `a IS DISTINCT FROM b` ↔ `NOT (a IS NOT DISTINCT FROM b)`
- `a IS NOT DISTINCT FROM b` ↔ `(a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)`

## Helper Signatures Reference

- `compareValues(a, b any) int` — `expr.go:3457` — returns 0 for equal, <0 for a<b, >0 for a>b
- Used by existing comparison operators (OpEq, OpNe, OpLt, etc.)

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
