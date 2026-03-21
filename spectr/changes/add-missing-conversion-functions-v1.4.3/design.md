# Design: Missing Conversion Functions

## Architecture

All three functions follow established patterns. TO_DATE reuses STRPTIME parsing with DATE cast. TO_CHAR is a simple alias for STRFTIME. GENERATE_SUBSCRIPTS follows the GENERATE_SERIES table function pattern.

## 1. TO_DATE(string, format)

### 1.1 Implementation (executor/expr.go)

Add case label in evaluateFunctionCall() around the existing STRPTIME block (expr.go near line 1953):

```go
case "TO_DATE":
    // TO_DATE(string, format) — parse string to DATE
    // Reuse STRPTIME logic but cast result to DATE
    if len(args) < 1 || len(args) > 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TO_DATE requires 1 or 2 arguments, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil  // NULL propagation
    }
    if len(args) == 2 {
        // TO_DATE(string, format) — use STRPTIME-style parsing
        result, err := evalStrptime(args)  // temporal_functions.go:1715
        if err != nil {
            return nil, err
        }
        // Convert TIMESTAMP (int64 micros) to DATE (int32 days)
        if ts, ok := result.(int64); ok {
            t := timestampToTime(ts)       // temporal_functions.go:95
            return timeToDate(t), nil      // temporal_functions.go:461 — returns int32
        }
        return result, nil
    }
    // TO_DATE(string) — auto-detect format using convertToTime pattern (convert.go:338-362)
    s := toString(args[0])  // expr.go:4202
    t, err := time.Parse("2006-01-02", s)
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TO_DATE: cannot parse %q as date", s),
        }
    }
    return timeToDate(t), nil
```

### 1.2 Type Inference (binder/utils.go)

Add to inferFunctionResultType() around line 512:

```go
case "TO_DATE":
    return dukdb.TYPE_DATE  // type_enum.go: Type = 13
```

## 2. TO_CHAR(value, format)

### 2.1 Implementation (executor/expr.go)

Add simple alias next to STRFTIME (expr.go near line 1950):

```go
case "STRFTIME", "TO_CHAR":
    return evalStrftime(args)  // temporal_functions.go:1674
```

This is just adding `"TO_CHAR"` to the existing STRFTIME case label.

### 2.2 Type Inference (binder/utils.go)

Add alias to existing STRFTIME case:

```go
case "STRFTIME", "TO_CHAR":
    return dukdb.TYPE_VARCHAR
```

## 3. GENERATE_SUBSCRIPTS(array, dim)

### 3.1 Implementation

GENERATE_SUBSCRIPTS takes an array and a dimension number, returns integer indices from 1 to array length. In DuckDB, arrays are 1-indexed.

This is a table-returning function, similar to GENERATE_SERIES. Register it in the table function dispatch.

**Option A**: Implement as scalar function returning a list (simpler):

```go
case "GENERATE_SUBSCRIPTS":
    if len(args) < 1 || len(args) > 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("GENERATE_SUBSCRIPTS requires 1 or 2 arguments, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    arr, ok := args[0].([]any)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "GENERATE_SUBSCRIPTS requires a list argument",
        }
    }
    // Generate 1-based indices
    result := make([]any, len(arr))
    for i := range arr {
        result[i] = int64(i + 1)  // 1-indexed
    }
    return result, nil
```

**Option B**: Implement as table function (full DuckDB compatibility):

Follow the executeGenerateSeries() pattern (table_function_series.go:13-127). Register in table function dispatch. Return ExecutionResult with rows of integers 1..len(array).

### 3.2 Type Inference

```go
case "GENERATE_SUBSCRIPTS":
    return dukdb.TYPE_INTEGER  // or TYPE_BIGINT for large arrays
```

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:630 — function dispatch
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- `evalStrptime()` — temporal_functions.go:1718-1758 — string→timestamp parsing
- `evalStrftime()` — temporal_functions.go:1677-1713 — timestamp→string formatting
- `timestampToTime()` — temporal_functions.go:95-97 — int64 micros → time.Time
- `timeToDate()` — temporal_functions.go:461-463 — time.Time → int32 days
- `dateToTime()` — temporal_functions.go:89-91 — int32 days → time.Time
- `toString()` — expr.go:4202-4208 — any → string
- `executeGenerateSeries()` — table_function_series.go:13-127 — table function pattern
- TYPE_DATE — type_enum.go — Type = 13
- TYPE_VARCHAR — standard string type
- TYPE_INTEGER — for subscripts
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. TO_DATE('2024-01-15', '%Y-%m-%d') → DATE value for Jan 15, 2024
2. TO_DATE('2024-01-15') → auto-detect ISO format
3. TO_DATE(NULL) → NULL
4. TO_CHAR(DATE '2024-01-15', '%Y/%m/%d') → '2024/01/15'
5. TO_CHAR(TIMESTAMP '2024-01-15 10:30:00', '%Y-%m-%d %H:%M') → formatted string
6. GENERATE_SUBSCRIPTS([10, 20, 30], 1) → [1, 2, 3]
7. GENERATE_SUBSCRIPTS(empty array) → empty result
8. GENERATE_SUBSCRIPTS(NULL) → NULL
