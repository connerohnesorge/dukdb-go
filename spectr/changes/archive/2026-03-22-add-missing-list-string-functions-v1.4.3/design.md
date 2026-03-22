# Design: Missing List/String Functions

## Architecture

Three of five functions are simple alias additions to existing case labels. Two (LIST_APPEND, LIST_PREPEND) are new implementations following the LIST_CONCAT pattern (expr.go:2590).

## 1. LIST_APPEND / ARRAY_APPEND / ARRAY_PUSH_BACK

### 1.1 Implementation (expr.go)

Add near the LIST_CONCAT block (expr.go:2590):

```go
case "LIST_APPEND", "ARRAY_APPEND", "ARRAY_PUSH_BACK":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("LIST_APPEND requires 2 arguments, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    list, ok := args[0].([]any)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_APPEND first argument must be a list",
        }
    }
    // Append element to end of list
    result := make([]any, len(list)+1)
    copy(result, list)
    result[len(list)] = args[1]
    return result, nil
```

### 1.2 Type Inference (binder/utils.go)

Add to inferFunctionResultType() at line 347:

```go
case "LIST_APPEND", "ARRAY_APPEND", "ARRAY_PUSH_BACK",
     "LIST_PREPEND", "ARRAY_PREPEND", "ARRAY_PUSH_FRONT":
    // Returns same list type as input
    if len(args) > 0 {
        return args[0].ResultType()
    }
    return dukdb.TYPE_ANY
```

## 2. LIST_PREPEND / ARRAY_PREPEND / ARRAY_PUSH_FRONT

### 2.1 Implementation (expr.go)

```go
case "LIST_PREPEND", "ARRAY_PREPEND", "ARRAY_PUSH_FRONT":
    // Note: DuckDB signature is LIST_PREPEND(element, list) — element first!
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("LIST_PREPEND requires 2 arguments, got %d", len(args)),
        }
    }
    if args[1] == nil {
        return nil, nil
    }
    list, ok := args[1].([]any)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LIST_PREPEND second argument must be a list",
        }
    }
    // Prepend element to start of list
    result := make([]any, len(list)+1)
    result[0] = args[0]
    copy(result[1:], list)
    return result, nil
```

**Important**: DuckDB's LIST_PREPEND takes `(element, list)` — element is first argument, list is second. This is the opposite of LIST_APPEND.

## 3. LIST_HAS (Alias)

Add "LIST_HAS" to the existing LIST_CONTAINS case label at expr.go:2538:

```go
// BEFORE:
case "LIST_CONTAINS", "ARRAY_CONTAINS":
// AFTER:
case "LIST_CONTAINS", "ARRAY_CONTAINS", "LIST_HAS":
```

Also add to inferFunctionResultType() alongside existing LIST_CONTAINS entry.

## 4. STRING_TO_ARRAY (Alias)

Add "STRING_TO_ARRAY" to the existing STRING_SPLIT case label at expr.go:1522:

```go
// BEFORE:
case "STRING_SPLIT":
// AFTER:
case "STRING_SPLIT", "STRING_TO_ARRAY":
```

Note: STRING_SPLIT currently has NO aliases (no STR_SPLIT).

Also add to inferFunctionResultType() alongside existing STRING_SPLIT entry.

## 5. REGEXP_FULL_MATCH

### 5.1 Implementation (expr.go)

Add near REGEXP_MATCHES (expr.go:1455). Note: regexp is NOT imported in expr.go — it's in internal/executor/regex.go. Either add `"regexp"` import to expr.go or implement as a helper in regex.go following the existing regexpMatchesValue() pattern:

```go
case "REGEXP_FULL_MATCH":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("REGEXP_FULL_MATCH requires 2 arguments, got %d", len(args)),
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    s := toString(args[0])       // expr.go:4202
    pattern := toString(args[1])
    // Full match: anchor pattern with ^ and $
    fullPattern := "^(?:" + pattern + ")$"
    re, err := regexp.Compile(fullPattern)
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("REGEXP_FULL_MATCH: invalid pattern: %s", err),
        }
    }
    return re.MatchString(s), nil
```

### 5.2 Type Inference

```go
case "REGEXP_FULL_MATCH":
    return dukdb.TYPE_BOOLEAN
```

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:630 — function dispatch
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- `LIST_CONCAT` — expr.go:2590 — concatenate lists (pattern for append)
- `LIST_CONTAINS` — expr.go:2538 — membership check (add LIST_HAS alias)
- `STRING_SPLIT` — expr.go:1522 — split string (add STRING_TO_ARRAY alias)
- `REGEXP_MATCHES` — expr.go:1455 — regex matches (pattern for REGEXP_FULL_MATCH)
- `toString()` — expr.go:4202 — any → string
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. LIST_APPEND([1,2,3], 4) → [1,2,3,4]
2. LIST_APPEND([], 1) → [1]
3. LIST_APPEND(NULL, 1) → NULL
4. LIST_PREPEND(0, [1,2,3]) → [0,1,2,3]
5. LIST_PREPEND(1, []) → [1]
6. LIST_HAS([1,2,3], 2) → true (same as LIST_CONTAINS)
7. STRING_TO_ARRAY('a,b,c', ',') → ['a','b','c'] (same as STRING_SPLIT)
8. REGEXP_FULL_MATCH('hello', 'h.*o') → true
9. REGEXP_FULL_MATCH('hello world', 'h.*o') → false (doesn't match full string)
10. REGEXP_FULL_MATCH('hello', 'hell') → false (partial match, not full)
