# Design: Aggregate Registration Fixes and Missing Aggregates Round 4

## Architecture

Two changes: (1) fix isAggregateFunc() registration for 12 implemented aggregates, (2) add 4 new aggregate implementations.

## 1. Registration Fix: isAggregateFunc() (operator.go:99-124)

12 aggregate functions are implemented in computeAggregate() (physical_aggregate.go:295) but NOT listed in isAggregateFunc(). This means the planner doesn't recognize them as aggregates and generates incorrect query plans.

### Current isAggregateFunc (operator.go:99-124):

```go
func isAggregateFunc(name string) bool {
    switch name {
    case "COUNT", "SUM", "AVG", "MIN", "MAX",
        "MEDIAN", "MODE", "QUANTILE", "PERCENTILE_CONT", "PERCENTILE_DISC",
        "ENTROPY", "SKEWNESS", "KURTOSIS",
        "VAR_POP", "VAR_SAMP", "VARIANCE", "STDDEV_POP", "STDDEV_SAMP", "STDDEV",
        "APPROX_COUNT_DISTINCT", "APPROX_MEDIAN", "APPROX_QUANTILE",
        "STRING_AGG", "GROUP_CONCAT", "LISTAGG", "LIST", "ARRAY_AGG", "LIST_DISTINCT",
        "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT",
        "COUNT_IF", "SUM_IF", "AVG_IF", "MIN_IF", "MAX_IF", "FIRST", "LAST", "ANY_VALUE",
        "ARGMIN", "ARG_MIN", "ARGMAX", "ARG_MAX", "MIN_BY", "MAX_BY",
        "HISTOGRAM",
        "REGR_SLOPE", "REGR_INTERCEPT", "REGR_R2",
        "CORR", "COVAR_POP", "COVAR_SAMP",
        "PRODUCT", "MAD", "FAVG", "FSUM", "BITSTRING_AGG":
        return true
    }
    return false
}
```

### Missing from registration (all have implementations in physical_aggregate.go):

| Function | Dispatch Line | Description |
|----------|--------------|-------------|
| BOOL_AND | 1105 | Boolean AND of all values |
| BOOL_OR | 1115 | Boolean OR of all values |
| EVERY | 1125 | Alias for BOOL_AND (SQL standard) |
| BIT_AND | 1137 | Bitwise AND of all values |
| BIT_OR | 1147 | Bitwise OR of all values |
| BIT_XOR | 1157 | Bitwise XOR of all values |
| REGR_COUNT | 1020 | Regression: count of non-null pairs |
| REGR_AVGX | 1034 | Regression: average of X |
| REGR_AVGY | 1048 | Regression: average of Y |
| REGR_SXX | 1062 | Regression: sum of squares of X deviations |
| REGR_SYY | 1076 | Regression: sum of squares of Y deviations |
| REGR_SXY | 1090 | Regression: sum of products of deviations |

### Fix: Add these names to the switch statement:

```go
// Add after the CORR, COVAR_POP, COVAR_SAMP line (line 118):
"REGR_COUNT", "REGR_AVGX", "REGR_AVGY", "REGR_SXX", "REGR_SYY", "REGR_SXY",
// Add after the PRODUCT, MAD, FAVG, FSUM, BITSTRING_AGG line (line 120):
"BOOL_AND", "BOOL_OR", "EVERY",
"BIT_AND", "BIT_OR", "BIT_XOR",
```

Also add the 4 new aggregates being implemented:

```go
"GEOMETRIC_MEAN", "GEOMEAN", "WEIGHTED_AVG",
"ARBITRARY", "MEAN",
```

## 2. New Aggregate: GEOMETRIC_MEAN / GEOMEAN

Computes the geometric mean: nth root of the product of n values. Equivalent to `EXP(AVG(LN(x)))`.

Add in computeAggregate() near PRODUCT:

```go
case "GEOMETRIC_MEAN", "GEOMEAN":
    // Geometric mean = exp(avg(ln(x)))
    sumLog := 0.0
    count := int64(0)
    for _, row := range rows {
        val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
        if err != nil {
            return nil, err
        }
        if val != nil {
            f := toFloat64Value(val)
            if f <= 0 {
                return nil, nil // Geometric mean undefined for non-positive values
            }
            sumLog += math.Log(f)
            count++
        }
    }
    if count == 0 {
        return nil, nil
    }
    return math.Exp(sumLog / float64(count)), nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

## 3. New Aggregate: WEIGHTED_AVG(value, weight)

Weighted average: sum(value * weight) / sum(weight).

```go
case "WEIGHTED_AVG":
    if len(fn.Args) < 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "WEIGHTED_AVG requires 2 arguments (value, weight)",
        }
    }
    sumWeighted := 0.0
    sumWeights := 0.0
    for _, row := range rows {
        val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
        if err != nil {
            return nil, err
        }
        weight, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], row)
        if err != nil {
            return nil, err
        }
        if val != nil && weight != nil {
            v := toFloat64Value(val)
            w := toFloat64Value(weight)
            sumWeighted += v * w
            sumWeights += w
        }
    }
    if sumWeights == 0 {
        return nil, nil
    }
    return sumWeighted / sumWeights, nil
```

Type inference: `return dukdb.TYPE_DOUBLE`

## 4. New Aggregate: ARBITRARY (alias for FIRST)

Returns an arbitrary non-NULL value from the group. In DuckDB, this is equivalent to FIRST.

```go
case "ARBITRARY":
    // Alias for FIRST — returns first non-null value
    for _, row := range rows {
        val, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], row)
        if err != nil {
            return nil, err
        }
        if val != nil {
            return val, nil
        }
    }
    return nil, nil
```

Note: FIRST is already implemented. ARBITRARY can alternatively be added as an alias to the existing FIRST case label:

```go
// Modify existing FIRST case to include ARBITRARY:
case "FIRST", "ARBITRARY":
```

The alias approach is simpler and more maintainable.

Type inference: Match FIRST — `return args[0].ResultType()` if args available, else `dukdb.TYPE_ANY`.

## 5. New Aggregate: MEAN (alias for AVG)

MEAN is simply an alias for AVG. Add to the existing AVG case label:

```go
// Modify existing AVG case to include MEAN:
case "AVG", "MEAN":
```

No new implementation needed. Type inference: same as AVG (`dukdb.TYPE_DOUBLE`).

## Helper Signatures Reference (Verified)

- `isAggregateFunc()` — operator.go:99-124 — aggregate name registration
- `computeAggregate()` — physical_aggregate.go:295 — aggregate dispatch
- BOOL_AND — physical_aggregate.go:1105 — boolean AND implementation
- BOOL_OR — physical_aggregate.go:1115 — boolean OR implementation
- EVERY — physical_aggregate.go:1125 — alias for BOOL_AND
- BIT_AND — physical_aggregate.go:1137 — bitwise AND implementation
- BIT_OR — physical_aggregate.go:1147 — bitwise OR implementation
- BIT_XOR — physical_aggregate.go:1157 — bitwise XOR implementation
- REGR_COUNT — physical_aggregate.go:1020 — regression count
- REGR_AVGX — physical_aggregate.go:1034 — regression avg X
- FIRST case — physical_aggregate.go — existing FIRST implementation
- AVG case — physical_aggregate.go — existing AVG implementation
- `toFloat64Value()` — expr.go:4509 — numeric conversion
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT BOOL_AND(x) FROM (VALUES (true), (true), (true)) t(x)` → true
2. `SELECT BOOL_OR(x) FROM (VALUES (false), (true), (false)) t(x)` → true
3. `SELECT EVERY(x > 0) FROM (VALUES (1), (2), (3)) t(x)` → true
4. `SELECT BIT_AND(x) FROM (VALUES (7), (3)) t(x)` → 3 (binary: 111 & 011 = 011)
5. `SELECT BIT_OR(x) FROM (VALUES (1), (2)) t(x)` → 3 (binary: 01 | 10 = 11)
6. `SELECT BIT_XOR(x) FROM (VALUES (3), (1)) t(x)` → 2 (binary: 11 ^ 01 = 10)
7. `SELECT GEOMETRIC_MEAN(x) FROM (VALUES (2), (8)) t(x)` → 4.0
8. `SELECT WEIGHTED_AVG(score, weight) FROM (VALUES (90, 3), (80, 1)) t(score, weight)` → 87.5
9. `SELECT ARBITRARY(x) FROM (VALUES (1), (2), (3)) t(x)` → 1 (or any non-null value)
10. `SELECT MEAN(x) FROM (VALUES (10), (20), (30)) t(x)` → 20.0
