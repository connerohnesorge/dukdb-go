# Design: Missing Numeric Functions

## Architecture

Two areas: (1) scalar functions added to evaluateFunctionCall(), (2) conditional aggregates added to computeAggregate() and isAggregateFunc().

## 1. SIGNBIT Scalar Function

Returns true if the sign bit is set (value is negative or -0.0):

```go
case "SIGNBIT":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SIGNBIT requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    f := toFloat64Value(args[0])
    return math.Signbit(f), nil
```

Type inference: `return dukdb.TYPE_BOOLEAN`

Note: `math.Signbit` is the Go standard library function. `math` is already imported in expr.go.

## 2. WIDTH_BUCKET Scalar Function

WIDTH_BUCKET(value, min, max, num_buckets) — assigns a value to a bucket in an equi-width histogram. SQL standard function.

```go
case "WIDTH_BUCKET":
    if len(args) != 4 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "WIDTH_BUCKET requires 4 arguments (value, min, max, num_buckets)",
        }
    }
    for _, a := range args {
        if a == nil { return nil, nil }
    }
    value := toFloat64Value(args[0])
    minVal := toFloat64Value(args[1])
    maxVal := toFloat64Value(args[2])
    numBuckets := toInt64Value(args[3])
    if numBuckets <= 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "WIDTH_BUCKET: number of buckets must be positive",
        }
    }
    if minVal >= maxVal {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "WIDTH_BUCKET: min must be less than max",
        }
    }
    // Below range → bucket 0, above range → numBuckets+1
    if value < minVal {
        return int64(0), nil
    }
    if value >= maxVal {
        return numBuckets + 1, nil
    }
    // Calculate bucket (1-indexed)
    bucket := int64((value-minVal)/(maxVal-minVal)*float64(numBuckets)) + 1
    if bucket > numBuckets {
        bucket = numBuckets
    }
    return bucket, nil
```

Type inference: `return dukdb.TYPE_INTEGER`

## 3. BETA Scalar Function

BETA(a, b) — returns the beta function value B(a,b) = Gamma(a) * Gamma(b) / Gamma(a+b):

```go
case "BETA":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "BETA requires 2 arguments",
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    a := toFloat64Value(args[0])
    b := toFloat64Value(args[1])
    // B(a,b) = exp(lgamma(a) + lgamma(b) - lgamma(a+b))
    lga, _ := math.Lgamma(a)
    lgb, _ := math.Lgamma(b)
    lgab, _ := math.Lgamma(a + b)
    return math.Exp(lga + lgb - lgab), nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

Note: Using Lgamma for numerical stability (avoids overflow for large a, b). GAMMA function already exists in the codebase — BETA follows the same pattern.

## 4. Conditional Aggregates (SUM_IF, AVG_IF, MIN_IF, MAX_IF)

These are aggregate functions that take a condition as the second argument. Only rows where the condition is true are included.

### 4.1 Registration in isAggregateFunc() (operator.go:99-122)

Add to the aggregate name list:
```go
"SUM_IF", "AVG_IF", "MIN_IF", "MAX_IF",
```

### 4.2 Implementation in computeAggregate() (physical_aggregate.go:295)

Follow the COUNT_IF pattern at physical_aggregate.go:764:

```go
case "SUM_IF":
    sum := 0.0
    hasValue := false
    for _, row := range rows {
        if len(fn.Args) >= 2 {
            condVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], row)
            if err != nil {
                return nil, err
            }
            if condVal != nil && toBool(condVal) {
                val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
                if err != nil {
                    return nil, err
                }
                if val != nil {
                    sum += toFloat64Value(val)
                    hasValue = true
                }
            }
        }
    }
    if !hasValue {
        return nil, nil
    }
    return sum, nil

case "AVG_IF":
    sum := 0.0
    count := int64(0)
    for _, row := range rows {
        if len(fn.Args) >= 2 {
            condVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], row)
            if err != nil {
                return nil, err
            }
            if condVal != nil && toBool(condVal) {
                val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
                if err != nil {
                    return nil, err
                }
                if val != nil {
                    sum += toFloat64Value(val)
                    count++
                }
            }
        }
    }
    if count == 0 {
        return nil, nil
    }
    return sum / float64(count), nil

case "MIN_IF":
    var minVal any
    for _, row := range rows {
        if len(fn.Args) >= 2 {
            condVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], row)
            if err != nil {
                return nil, err
            }
            if condVal != nil && toBool(condVal) {
                val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
                if err != nil {
                    return nil, err
                }
                if val != nil {
                    if minVal == nil || compareValues(val, minVal) < 0 {
                        minVal = val
                    }
                }
            }
        }
    }
    return minVal, nil

case "MAX_IF":
    var maxVal any
    for _, row := range rows {
        if len(fn.Args) >= 2 {
            condVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], row)
            if err != nil {
                return nil, err
            }
            if condVal != nil && toBool(condVal) {
                val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
                if err != nil {
                    return nil, err
                }
                if val != nil {
                    if maxVal == nil || compareValues(val, maxVal) > 0 {
                        maxVal = val
                    }
                }
            }
        }
    }
    return maxVal, nil
```

### 4.3 Type Inference (binder/utils.go:347)

```go
case "SUM_IF", "AVG_IF":
    return dukdb.TYPE_DOUBLE
case "MIN_IF", "MAX_IF":
    // Return type matches first argument type
    if len(args) > 0 {
        return args[0].ResultType()
    }
    return dukdb.TYPE_ANY
```

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:659 — scalar function dispatch
- `computeAggregate()` — physical_aggregate.go:295 — aggregate dispatch
- `isAggregateFunc()` — operator.go:99-124 — aggregate name registration
- COUNT_IF pattern — physical_aggregate.go:765 — conditional aggregate reference
- FILTER clause pre-filtering — physical_aggregate.go:308-319 — already implemented
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- `toFloat64Value()` — expr.go:4509 — numeric conversion
- `toInt64Value()` — expr.go:4487 — integer conversion (NOT toInt64)
- `toBool()` — expr.go:4461 — boolean conversion
- `compareValues()` — expr.go:4575 — value comparison for MIN/MAX
- `math.Signbit()` — Go stdlib — sign bit check
- `math.Lgamma()` — Go stdlib — log gamma function (returns lgamma float64, sign int)
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. SIGNBIT(-1.0) → true
2. SIGNBIT(1.0) → false
3. SIGNBIT(-0.0) → true (important: negative zero)
4. WIDTH_BUCKET(5.0, 0.0, 10.0, 5) → 3
5. WIDTH_BUCKET(-1.0, 0.0, 10.0, 5) → 0 (below range)
6. WIDTH_BUCKET(10.0, 0.0, 10.0, 5) → 6 (above range)
7. BETA(1, 1) → 1.0
8. BETA(2, 3) → 0.0833... (1/12)
9. SUM_IF(amount, status = 'paid') → sum of paid amounts
10. AVG_IF(score, passed = true) → average of passed scores
11. MIN_IF(price, category = 'A') → min price in category A
12. MAX_IF(price, category = 'A') → max price in category A
13. NULL propagation for all functions
