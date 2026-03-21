# Design: Missing Aggregates Round 3

## Architecture

All five aggregates are added to `computeAggregate()` (physical_aggregate.go:295) and registered in `isAggregateFunc()` (operator.go:99-122). They follow established patterns from SUM/AVG/MEDIAN.

## 1. PRODUCT Aggregate

Multiplicative accumulator, identity = 1.0. Follows the SUM pattern (physical_aggregate.go:342).

```go
case "PRODUCT":
    product := 1.0
    hasValue := false
    for _, row := range rows {
        if len(fn.Args) > 0 {
            val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
            if err != nil {
                return nil, err
            }
            if val != nil {
                product *= toFloat64Value(val)
                hasValue = true
            }
        }
    }
    if !hasValue {
        return nil, nil  // NULL if no non-NULL values
    }
    return product, nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

## 2. MAD (Median Absolute Deviation)

Follows the MEDIAN pattern (physical_aggregate.go:449). Collect all values, compute median, then compute median of |value - median|.

```go
case "MAD":
    // collectValues signature: op.collectValues(expr BoundExpr, rows) ([]any, error)
    // at physical_aggregate.go:1094
    values, err := op.collectValues(fn.Args[0], rows)
    if err != nil {
        return nil, err
    }
    if len(values) == 0 {
        return nil, nil
    }
    // computeMedian signature: computeMedian(values []any) (any, error)
    // at aggregate_stats.go:135 — returns (any, error), NOT single value
    medianVal, err := computeMedian(values)
    if err != nil {
        return nil, err
    }
    median := toFloat64Value(medianVal)

    // Compute absolute deviations
    // Note: math.Abs needs "math" import in physical_aggregate.go
    deviations := make([]any, len(values))
    for i, v := range values {
        deviations[i] = math.Abs(toFloat64Value(v) - median)
    }
    // Median of deviations
    return computeMedian(deviations)
```

Type inference: `return dukdb.TYPE_DOUBLE`

Important implementation notes:
- `collectValues()` is a method on the operator: `op.collectValues(expr, rows)` at physical_aggregate.go:1094 — takes BoundExpr directly, not function call wrapper
- `computeMedian()` at aggregate_stats.go:135 returns `(any, error)` — must handle error return
- `math` package may need to be imported in physical_aggregate.go for math.Abs
- `collectNonNullFloats()` is a private helper at aggregate_stats.go:120, used internally by computeMedian

## 3. FAVG (Fast Average with Kahan Summation)

Like AVG but uses Kahan summation algorithm for better floating-point accuracy:

```go
case "FAVG":
    // Kahan summation for numerically stable average
    sum := 0.0
    compensation := 0.0  // Kahan compensation term
    count := int64(0)
    for _, row := range rows {
        if len(fn.Args) > 0 {
            val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
            if err != nil {
                return nil, err
            }
            if val != nil {
                y := toFloat64Value(val) - compensation
                t := sum + y
                compensation = (t - sum) - y
                sum = t
                count++
            }
        }
    }
    if count == 0 {
        return nil, nil
    }
    return sum / float64(count), nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

## 4. FSUM (Fast Sum with Kahan Summation)

Like SUM but uses Kahan summation:

```go
case "FSUM":
    sum := 0.0
    compensation := 0.0
    hasValue := false
    for _, row := range rows {
        if len(fn.Args) > 0 {
            val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
            if err != nil {
                return nil, err
            }
            if val != nil {
                y := toFloat64Value(val) - compensation
                t := sum + y
                compensation = (t - sum) - y
                sum = t
                hasValue = true
            }
        }
    }
    if !hasValue {
        return nil, nil
    }
    return sum, nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

## 5. BITSTRING_AGG

Aggregates boolean values into a bitstring representation. In DuckDB, BITSTRING_AGG takes a boolean expression and produces a BIT/VARCHAR result.

```go
case "BITSTRING_AGG":
    var bits []byte
    for _, row := range rows {
        if len(fn.Args) > 0 {
            val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
            if err != nil {
                return nil, err
            }
            if val != nil {
                if toBool(val) {  // toBool at expr.go:4184
                    bits = append(bits, '1')
                } else {
                    bits = append(bits, '0')
                }
            }
        }
    }
    if len(bits) == 0 {
        return nil, nil
    }
    return string(bits), nil
```

Type inference: `return dukdb.TYPE_VARCHAR`

## Registration

### isAggregateFunc() (operator.go:99-122)

Add to the aggregate name list:

```go
"PRODUCT", "MAD", "FAVG", "FSUM", "BITSTRING_AGG",
```

### inferFunctionResultType() (binder/utils.go:347)

```go
case "PRODUCT", "FAVG", "FSUM", "MAD":
    return dukdb.TYPE_DOUBLE
case "BITSTRING_AGG":
    return dukdb.TYPE_VARCHAR
```

## Helper Signatures Reference (Verified)

- `computeAggregate()` — physical_aggregate.go:295 — aggregate dispatch
- `isAggregateFunc()` — operator.go:99-122 — registers aggregate names
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- SUM pattern — physical_aggregate.go:342 — additive accumulation
- AVG pattern — physical_aggregate.go:368 — sum/count
- MEDIAN pattern — physical_aggregate.go:449 — collect + compute
- `toFloat64Value()` — numeric conversion helper
- `toBool()` — expr.go:4184 — boolean conversion
- `op.collectValues(expr, rows)` — physical_aggregate.go:1094 — method on operator, takes BoundExpr directly
- `computeMedian(values []any)` — aggregate_stats.go:135 — returns (any, error)
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. PRODUCT([2, 3, 4]) → 24.0
2. PRODUCT with NULLs — skip NULLs, return product of non-NULLs
3. PRODUCT of empty set → NULL
4. MAD([1, 2, 3, 4, 5]) → 1.0 (median=3, deviations=[2,1,0,1,2], MAD=1)
5. FAVG — compare to AVG, should be identical for small sets
6. FAVG with many small numbers — should be more accurate than naive AVG
7. FSUM — compare to SUM, verify Kahan summation accuracy
8. BITSTRING_AGG — aggregate booleans to '101011' style string
