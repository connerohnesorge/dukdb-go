# Design: Missing List/Array Functions for DuckDB v1.4.3

## Architecture

All functions follow the established two-layer pattern:

1. **Binder** (`internal/binder/utils.go`, `inferFunctionResultType()`): Add return type inference
2. **Executor** (`internal/executor/expr.go` function dispatch + `internal/executor/list_functions.go` implementations): Add evaluation logic

No parser changes needed — all functions use standard function call syntax which the parser already handles.

## Existing Infrastructure

### List function dispatch (internal/executor/expr.go)

| Function | Line(s) | Status |
|----------|---------|--------|
| LIST_TRANSFORM / ARRAY_APPLY | expr.go:595 | Implemented (lambda) |
| LIST_FILTER / ARRAY_FILTER | expr.go:597 | Implemented (lambda) |
| LIST_SORT / ARRAY_SORT | expr.go:599 | Implemented (lambda, ascending only) |
| LIST_CONTAINS / ARRAY_CONTAINS | expr.go:2197 | Implemented |
| LIST_POSITION / ARRAY_POSITION | expr.go:2223 | Implemented |
| LIST_CONCAT / ARRAY_CONCAT | expr.go:2249 | Implemented |
| LIST_DISTINCT / ARRAY_DISTINCT | expr.go:2282 | Implemented |
| LIST_REVERSE / ARRAY_REVERSE | expr.go:2310 | Implemented |
| LIST_SLICE / ARRAY_SLICE | expr.go:2333 | Implemented |
| FLATTEN | expr.go:2364 | Implemented |

### Key helpers (internal/executor/list_functions.go)

- `toSlice(v any) ([]any, bool)` at line 401 — converts typed slices to `[]any`
- `evaluateListSort(ctx, args, row) (any, error)` at line 150 — ascending sort only
- `compareValues(a, b any) int` — comparison for sort, used by LIST_SORT

### Binder type inference (internal/binder/utils.go)

List functions at lines 545-574 return `dukdb.TYPE_LIST` or `dukdb.TYPE_ANY` depending on the function.

## 1. LIST_ELEMENT / ARRAY_EXTRACT Implementation

### Binder (internal/binder/utils.go)

Add to `inferFunctionResultType()`:
```go
case "LIST_ELEMENT", "ARRAY_EXTRACT":
    return dukdb.TYPE_ANY // element type depends on list contents
```

### Executor (internal/executor/list_functions.go)

New function `evaluateListElement`:
```go
func evaluateListElement(args []any) (any, error) {
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_ELEMENT requires 2 arguments: LIST_ELEMENT(list, index)",
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    list, ok := toSlice(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_ELEMENT first argument must be a list",
        }
    }
    idx, ok := toInt64(args[1])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_ELEMENT second argument must be an integer",
        }
    }
    // DuckDB uses 1-based indexing; negative indices count from end
    length := int64(len(list))
    if idx > 0 {
        idx = idx - 1 // convert to 0-based
    } else if idx < 0 {
        idx = length + idx // -1 = last element
    } else {
        return nil, nil // index 0 is out of bounds in 1-based
    }
    if idx < 0 || idx >= length {
        return nil, nil // out of bounds → NULL
    }
    return list[idx], nil
}
```

### Executor dispatch (internal/executor/expr.go)

Add near the existing list functions block (around line 2389):
```go
case "LIST_ELEMENT", "ARRAY_EXTRACT":
    return evaluateListElement(args)
```

## 2. LIST_AGGREGATE / ARRAY_AGGREGATE Implementation

### Binder (internal/binder/utils.go)

```go
case "LIST_AGGREGATE", "ARRAY_AGGREGATE":
    return dukdb.TYPE_ANY // depends on aggregate and element type
```

### Executor (internal/executor/list_functions.go)

New function `evaluateListAggregate`:
```go
func evaluateListAggregate(args []any) (any, error) {
    if len(args) < 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_AGGREGATE requires at least 2 arguments: LIST_AGGREGATE(list, name)",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    list, ok := toSlice(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_AGGREGATE first argument must be a list",
        }
    }
    aggName, ok := args[1].(string)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_AGGREGATE second argument must be a string aggregate name",
        }
    }

    // Filter out NULLs for most aggregates
    var nonNull []any
    for _, v := range list {
        if v != nil {
            nonNull = append(nonNull, v)
        }
    }

    switch strings.ToLower(aggName) {
    case "sum":
        return listAggSum(nonNull)
    case "avg", "mean":
        return listAggAvg(nonNull)
    case "min":
        return listAggMin(nonNull)
    case "max":
        return listAggMax(nonNull)
    case "count":
        return int64(len(nonNull)), nil
    case "first":
        if len(nonNull) == 0 {
            return nil, nil
        }
        return nonNull[0], nil
    case "last":
        if len(nonNull) == 0 {
            return nil, nil
        }
        return nonNull[len(nonNull)-1], nil
    case "string_agg":
        sep := ","
        if len(args) > 2 {
            if s, ok := args[2].(string); ok {
                sep = s
            }
        }
        return listAggStringAgg(nonNull, sep), nil
    case "bool_and":
        return listAggBoolAnd(nonNull), nil
    case "bool_or":
        return listAggBoolOr(nonNull), nil
    default:
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("LIST_AGGREGATE: unsupported aggregate '%s'", aggName),
        }
    }
}
```

The `listAggSum`, `listAggAvg`, `listAggMin`, `listAggMax`, `listAggStringAgg`, `listAggBoolAnd`, `listAggBoolOr` helpers are straightforward:

- `listAggSum`: iterate, `toFloat64()` each, accumulate
- `listAggAvg`: sum / count
- `listAggMin`/`listAggMax`: iterate, `compareValues()` each
- `listAggStringAgg`: `toString()` each, `strings.Join()`
- `listAggBoolAnd`/`listAggBoolOr`: `toBool()` each, AND/OR

**Note on helper signatures:**
- `toInt64(v any) (int64, bool)` — in `internal/executor/math.go`
- `toFloat64(v any) (float64, bool)` — in `internal/executor/math.go`
- `toBool(v any) bool` — in `internal/executor/expr.go:3343` (single return)
- `toString(v any) string` — in `internal/executor/expr.go:3361`
- `compareValues(a, b any) int` — in `internal/executor/list_functions.go`

## 3. LIST_REVERSE_SORT Implementation

### Binder (internal/binder/utils.go)

```go
case "LIST_REVERSE_SORT", "ARRAY_REVERSE_SORT":
    return dukdb.TYPE_LIST
```

### Executor (internal/executor/list_functions.go)

New function `evaluateListReverseSort`:
```go
func evaluateListReverseSort(args []any) (any, error) {
    if len(args) < 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_REVERSE_SORT requires at least 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    list, ok := toSlice(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_REVERSE_SORT argument must be a list",
        }
    }
    // Copy to avoid mutating the original
    result := make([]any, len(list))
    copy(result, list)
    // Sort descending using compareValues (same comparator as LIST_SORT, reversed)
    sort.SliceStable(result, func(i, j int) bool {
        return compareValues(result[i], result[j]) > 0
    })
    return result, nil
}
```

**NULL handling for sort:** NULLs sort last in ascending (LIST_SORT), first in descending (LIST_REVERSE_SORT). The `compareValues()` function returns 0 for nil comparisons — the sort is stable so NULLs maintain relative position. To match DuckDB, NULLs should be placed at end for both ascending and descending. The implementation should partition NULLs to the end before sorting non-NULLs.

### Executor dispatch (internal/executor/expr.go)

```go
case "LIST_REVERSE_SORT", "ARRAY_REVERSE_SORT":
    return evaluateListReverseSort(args)
```

## 4. ARRAY_TO_STRING Implementation

### Binder (internal/binder/utils.go)

```go
case "ARRAY_TO_STRING", "LIST_TO_STRING":
    return dukdb.TYPE_VARCHAR
```

### Executor (internal/executor/list_functions.go)

New function `evaluateArrayToString`:
```go
func evaluateArrayToString(args []any) (any, error) {
    if len(args) < 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "ARRAY_TO_STRING requires at least 2 arguments: ARRAY_TO_STRING(list, separator [, null_string])",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    list, ok := toSlice(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "ARRAY_TO_STRING first argument must be a list",
        }
    }
    sep := toString(args[1])
    hasNullStr := len(args) >= 3 && args[2] != nil
    nullStr := ""
    if hasNullStr {
        nullStr = toString(args[2])
    }

    var parts []string
    for _, v := range list {
        if v == nil {
            if hasNullStr {
                parts = append(parts, nullStr)
            }
            // else: skip NULLs
            continue
        }
        parts = append(parts, toString(v))
    }
    return strings.Join(parts, sep), nil
}
```

## 5. LIST_ZIP Implementation

### Binder (internal/binder/utils.go)

```go
case "LIST_ZIP":
    return dukdb.TYPE_LIST // list of structs
```

### Executor (internal/executor/list_functions.go)

New function `evaluateListZip`:
```go
func evaluateListZip(args []any) (any, error) {
    if len(args) < 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_ZIP requires at least 2 list arguments",
        }
    }

    // Convert all arguments to slices
    lists := make([][]any, len(args))
    maxLen := 0
    for i, arg := range args {
        if arg == nil {
            return nil, nil // any NULL list → NULL result
        }
        var ok bool
        lists[i], ok = toSlice(arg)
        if !ok {
            return nil, &dukdb.Error{
                Type: dukdb.ErrorTypeExecutor,
                Msg:  fmt.Sprintf("LIST_ZIP argument %d must be a list", i+1),
            }
        }
        if len(lists[i]) > maxLen {
            maxLen = len(lists[i])
        }
    }

    // Build result: list of structs (map[string]any)
    result := make([]any, maxLen)
    for i := 0; i < maxLen; i++ {
        entry := make(map[string]any, len(lists))
        for j, list := range lists {
            key := fmt.Sprintf("f%d", j+1) // f1, f2, f3, ...
            if i < len(list) {
                entry[key] = list[i]
            } else {
                entry[key] = nil // pad with NULL
            }
        }
        result[i] = entry
    }
    return result, nil
}
```

**Struct field naming:** DuckDB names fields `f1`, `f2`, etc. matching positional arguments. The `map[string]any` representation matches how structs are represented elsewhere in the codebase.

## 6. LIST_RESIZE Implementation

### Binder (internal/binder/utils.go)

```go
case "LIST_RESIZE", "ARRAY_RESIZE":
    return dukdb.TYPE_LIST
```

### Executor (internal/executor/list_functions.go)

New function `evaluateListResize`:
```go
func evaluateListResize(args []any) (any, error) {
    if len(args) < 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_RESIZE requires at least 2 arguments: LIST_RESIZE(list, size [, value])",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    list, ok := toSlice(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_RESIZE first argument must be a list",
        }
    }
    size, ok := toInt64(args[1])
    if !ok || size < 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_RESIZE second argument must be a non-negative integer",
        }
    }

    var fillValue any
    if len(args) >= 3 {
        fillValue = args[2]
    }

    targetSize := int(size)
    if targetSize <= len(list) {
        // Truncate
        result := make([]any, targetSize)
        copy(result, list[:targetSize])
        return result, nil
    }
    // Extend
    result := make([]any, targetSize)
    copy(result, list)
    for i := len(list); i < targetSize; i++ {
        result[i] = fillValue
    }
    return result, nil
}
```

## Import Dependencies

No new imports needed — all functions use existing standard library packages (`sort`, `strings`, `fmt`) already imported in `list_functions.go`.

## Testing Strategy

Each function gets integration tests via `database/sql`. Tests verify:

1. **Happy path:** Normal inputs produce expected outputs
2. **NULL propagation:** NULL list → NULL return
3. **Edge cases:** Empty lists, single-element lists, negative indices, out-of-bounds
4. **Type handling:** Integer lists, string lists, mixed-type lists
5. **DuckDB compatibility:** Output matches DuckDB CLI for same inputs
