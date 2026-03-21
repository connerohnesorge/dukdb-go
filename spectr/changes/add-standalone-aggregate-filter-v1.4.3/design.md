# Design: Standalone Aggregate FILTER Clause

## Architecture

The FILTER clause is already parsed and supported for window aggregates. This change lifts the restriction that FILTER requires OVER, adds a Filter field to FunctionCall and BoundFunctionCall, and applies filter evaluation in the aggregate executor.

## 1. Parser Changes

### 1.1 Add Filter to FunctionCall (ast.go:839-846)

```go
type FunctionCall struct {
    Name      string
    Args      []Expr
    NamedArgs map[string]Expr
    Distinct  bool
    Star      bool
    OrderBy   []OrderByExpr
    Filter    Expr            // NEW: FILTER (WHERE ...) clause
}
```

### 1.2 Allow FILTER without OVER (parser.go:5359-5382)

Current code at parser.go:5359-5376 parses FILTER into `windowExpr.Filter`. When there's no OVER clause, line 5381-5382 rejects it:

```go
// CURRENT (lines 5381-5382):
if windowExpr.IgnoreNulls || windowExpr.Filter != nil {
    return nil, p.errorf("IGNORE NULLS and FILTER require OVER clause")
}
```

Change to: store Filter on the FunctionCall when there's no OVER clause:

```go
// NEW: Allow FILTER without OVER for aggregate functions
if windowExpr.Filter != nil {
    fn.Filter = windowExpr.Filter
}
if windowExpr.IgnoreNulls {
    return nil, p.errorf("IGNORE NULLS requires OVER clause")
}
return fn, nil
```

This keeps the IGNORE NULLS restriction (which only makes sense for window value functions) but allows FILTER on standalone aggregates.

## 2. Binder Changes

### 2.1 Add Filter to BoundFunctionCall (binder/expressions.go:64-72)

```go
type BoundFunctionCall struct {
    Name      string
    Args      []BoundExpr
    NamedArgs map[string]BoundExpr
    Distinct  bool
    Star      bool
    OrderBy   []BoundOrderByExpr
    ResType   dukdb.Type
    Filter    BoundExpr         // NEW: bound FILTER expression
}
```

### 2.2 Bind Filter Expression (binder/bind_expr.go:430-451)

After binding OrderBy (line 441), bind the Filter expression:

```go
// Bind FILTER expression
var boundFilter BoundExpr
if f.Filter != nil {
    boundFilter, err = b.bindExpr(f.Filter, dukdb.TYPE_BOOLEAN)
    if err != nil {
        return nil, fmt.Errorf("binding FILTER in %s: %w", f.Name, err)
    }
}

return &BoundFunctionCall{
    Name:      f.Name,
    Args:      args,
    NamedArgs: namedArgs,
    Distinct:  f.Distinct,
    Star:      f.Star,
    OrderBy:   boundOrderBy,
    ResType:   resType,
    Filter:    boundFilter,  // NEW
}, nil
```

## 3. Executor Changes

### 3.1 Apply Filter in computeAggregate() (physical_aggregate.go:295)

Follow the window FILTER pattern from physical_window.go:1469-1475. Add filter check at the start of each row iteration in computeAggregate():

```go
func (op *PhysicalAggregateOperator) computeAggregate(
    expr binder.BoundExpr,
    rows []map[string]any,
) (any, error) {
    fn, ok := expr.(*binder.BoundFunctionCall)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "expected aggregate function",
        }
    }

    // Apply FILTER: pre-filter rows before aggregation
    filteredRows := rows
    if fn.Filter != nil {
        filteredRows = make([]map[string]any, 0, len(rows))
        for _, row := range rows {
            filterVal, err := op.executor.evaluateExpr(op.ctx, fn.Filter, row)
            if err != nil {
                continue  // Skip rows where filter errors
            }
            if toBool(filterVal) {
                filteredRows = append(filteredRows, row)
            }
        }
    }

    // Use filteredRows instead of rows for all aggregate computation
    switch fn.Name {
    case "COUNT":
        if fn.Star {
            return int64(len(filteredRows)), nil
        }
        // ... rest uses filteredRows instead of rows
    }
}
```

Alternative approach: filter rows once at the top, then pass `filteredRows` to existing logic. This is cleaner than adding filter checks in every case branch (as the window executor does).

## Helper Signatures Reference (Verified)

- `FunctionCall` — ast.go:839-846 — parser AST for function calls
- `WindowExpr` — ast.go:1141-1150 — has Filter at line 1148
- FILTER parsing — parser.go:5359-5376 — parses FILTER (WHERE expr)
- FILTER rejection — parser.go:5381-5382 — "IGNORE NULLS and FILTER require OVER clause"
- `BoundFunctionCall` — binder/expressions.go:64-72 — bound function call
- Binding logic — binder/bind_expr.go:430-451 — OrderBy binding + return
- `computeAggregate()` — physical_aggregate.go:295 — aggregate computation entry point
- Window FILTER pattern — physical_window.go:1469-1475 — `if filter != nil { check condition }`
- `toBool()` — expr.go:4184 — converts any to bool
- `isAggregateFunc()` — operator.go:99-122 — aggregate function identification
- Error pattern — `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT COUNT(*) FILTER (WHERE active) FROM users` — count with filter
2. `SELECT SUM(amount) FILTER (WHERE type = 'credit') FROM transactions` — sum with filter
3. `SELECT AVG(score) FILTER (WHERE score > 0) FROM results` — avg with filter
4. `SELECT COUNT(*) FILTER (WHERE x > 5), COUNT(*) FROM t` — mixed filtered/unfiltered
5. `SELECT dept, COUNT(*) FILTER (WHERE active) FROM employees GROUP BY dept` — with GROUP BY
6. NULL filter condition — should exclude the row
7. FILTER on non-aggregate function — should error or be ignored
