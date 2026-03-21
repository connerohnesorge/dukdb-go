# Design: Ordered-Set Aggregates (WITHIN GROUP + LISTAGG)

## Architecture

This change touches three layers:
1. **Parser** (`parser.go`): Parse WITHIN GROUP syntax after function calls
2. **Binder** (`utils.go`): Add LISTAGG type inference
3. **Executor** (`physical_aggregate.go`): Add LISTAGG dispatch

No new AST types needed — WITHIN GROUP maps to the existing `FunctionCall.OrderBy` field.

## 1. Parser Changes (internal/parser/parser.go)

### Parse WITHIN GROUP after function call

The WITHIN GROUP clause appears AFTER the closing parenthesis of the function call. Currently, after parsing the function call and closing paren, the parser returns. We need to check for WITHIN GROUP before returning.

**Location:** In `parseFunctionCall()` or wherever function call parsing completes — after the closing `)` is consumed.

The current flow in parsing function calls (around line 5130-5160):
1. Parse function name
2. Consume `(`
3. Parse arguments (with internal ORDER BY for aggregates like STRING_AGG)
4. Consume `)`
5. Return FunctionCall

**New step 4.5:** After consuming `)`, check for WITHIN GROUP:

```go
// After consuming closing paren of function call
// Check for WITHIN GROUP (ORDER BY ...) — ordered-set aggregate syntax
if p.isKeyword("WITHIN") {
    p.advance() // consume WITHIN
    if err := p.expectKeyword("GROUP"); err != nil {
        return nil, err
    }
    if _, err := p.expect(tokenLParen); err != nil {
        return nil, err
    }
    if err := p.expectKeyword("ORDER"); err != nil {
        return nil, p.errorf("expected ORDER BY inside WITHIN GROUP")
    }
    if err := p.expectKeyword("BY"); err != nil {
        return nil, err
    }
    orderBy, err := p.parseOrderBy()
    if err != nil {
        return nil, err
    }
    // WITHIN GROUP ordering maps to the existing OrderBy field
    if len(fn.OrderBy) > 0 {
        return nil, p.errorf("cannot use both internal ORDER BY and WITHIN GROUP")
    }
    fn.OrderBy = orderBy
    if _, err := p.expect(tokenRParen); err != nil {
        return nil, err
    }
}
```

**Key insight:** WITHIN GROUP (ORDER BY ...) is semantically identical to the internal ORDER BY that already exists for aggregates like STRING_AGG. By mapping it to the same `FunctionCall.OrderBy` field, no binder or executor changes are needed for existing ordered-set aggregates (PERCENTILE_CONT, PERCENTILE_DISC, MODE).

### Keyword stop conditions

Add "WITHIN" to expression stop conditions so that `SELECT agg(x) WITHIN GROUP (ORDER BY y)` correctly stops parsing the function call before WITHIN. However, since WITHIN appears after the closing paren, this should not be needed — the function call parsing already terminates at `)`.

**Verify:** Check that `parseOrderBy()` exists and is reusable. It's at parser.go (used by ORDER BY clause parsing). The same function is used for internal aggregate ORDER BY at line 5143.

## 2. LISTAGG Implementation

### Executor (internal/executor/physical_aggregate.go)

Add LISTAGG dispatch near STRING_AGG (line 630-649). LISTAGG is semantically identical to STRING_AGG:

```go
case "LISTAGG":
    if len(fn.Args) == 0 {
        return nil, nil
    }
    values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
    if err != nil {
        return nil, err
    }
    // Get delimiter from second argument, default to empty string
    // (DuckDB LISTAGG defaults to empty string, unlike STRING_AGG which defaults to comma)
    delimiter := ""
    if len(fn.Args) >= 2 {
        delimVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
        if err != nil {
            return nil, err
        }
        if delimVal != nil {
            delimiter = toString(delimVal)
        }
    }
    return computeStringAgg(values, delimiter)
```

**Semantic difference from STRING_AGG:** LISTAGG defaults to empty string delimiter (""), while STRING_AGG defaults to comma (","). The `computeStringAgg()` function at aggregate_string.go:19 handles the actual concatenation.

### Register in aggregate function list

Add "LISTAGG" to the aggregate function name list in `operator.go:109` where STRING_AGG and GROUP_CONCAT are listed:

```go
"STRING_AGG", "GROUP_CONCAT", "LISTAGG", "LIST", "ARRAY_AGG", ...
```

### Binder (internal/binder/utils.go)

Add LISTAGG to the type inference in `inferFunctionResultType()` near STRING_AGG (line 554):

```go
case "STRING_AGG", "GROUP_CONCAT", "LISTAGG":
    return dukdb.TYPE_VARCHAR
```

## 3. Existing Ordered-Set Aggregates

These aggregates already work with internal ORDER BY syntax. After parsing WITHIN GROUP into the same OrderBy field, they'll work with WITHIN GROUP syntax automatically:

- **PERCENTILE_CONT(fraction)** — uses `collectValuesWithOrderBy()` internally
- **PERCENTILE_DISC(fraction)** — uses `collectValuesWithOrderBy()` internally
- **MODE()** — uses collected values

No executor changes needed for these — the WITHIN GROUP parser change handles them.

## Helper Signatures Reference

- `collectValuesWithOrderBy(valueExpr, orderBy, rows)` — physical_aggregate.go:1045 — collects and sorts values
- `computeStringAgg(values []any, delimiter string) (any, error)` — aggregate_string.go:19 — concatenates with delimiter
- `parseOrderBy()` — parser.go — parses ORDER BY expression list
- `toString(v any) string` — expr.go:3710 — converts any value to string
- FunctionCall.OrderBy — ast.go:822 — existing ORDER BY field in function call AST
- BoundFunctionCall.OrderBy — binder/expressions.go:72 — bound ORDER BY expressions

## Testing Strategy

Integration tests:
1. WITHIN GROUP basic: `SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM emp` — same result as internal ORDER BY
2. LISTAGG basic: `SELECT LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) FROM emp` — concatenated names
3. LISTAGG default delimiter: `SELECT LISTAGG(name) WITHIN GROUP (ORDER BY name) FROM emp` — no separator
4. LISTAGG with GROUP BY: `SELECT dept, LISTAGG(name, ',') WITHIN GROUP (ORDER BY name) FROM emp GROUP BY dept`
5. Error: both internal ORDER BY and WITHIN GROUP
6. LISTAGG NULL handling: NULL values should be skipped (same as STRING_AGG)
7. MODE with WITHIN GROUP: `SELECT MODE() WITHIN GROUP (ORDER BY value) FROM t`
