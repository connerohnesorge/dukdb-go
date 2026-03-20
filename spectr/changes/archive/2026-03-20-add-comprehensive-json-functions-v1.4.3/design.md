## Implementation Details

### Existing JSON infrastructure

The project already has these JSON functions implemented in `internal/executor/expr.go`:

| Function | Line(s) | Status |
|----------|---------|--------|
| JSON_EXTRACT | expr.go:1669 | Implemented |
| JSON_EXTRACT_STRING / JSON_EXTRACT_TEXT | expr.go:1678 | Implemented |
| JSON_VALID | expr.go:1687 | Implemented |
| JSON_TYPE | expr.go:1696 | Implemented |
| JSON_KEYS | expr.go:1708 | Implemented |
| JSON_ARRAY_LENGTH | expr.go:1720 | Implemented |
| TO_JSON | expr.go:1732 | Implemented |
| JSON_MERGE_PATCH | expr.go:1744 | Implemented |
| JSON_OBJECT / JSON_BUILD_OBJECT | expr.go:1756 | Implemented |
| JSON_ARRAY | expr.go:1759 | Implemented |

The `->` and `->>` operators are handled as `parser.OpJSONExtract` and `parser.OpJSONText` at expr.go:442-445 via `extractJSONValue()`.

### Functions to add

#### 1. JSON_CONTAINS(json, value) -- scalar

Location: `internal/executor/expr.go`, inside the function-name switch (around line 1759).

```go
case "JSON_CONTAINS":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "JSON_CONTAINS requires 2 arguments",
        }
    }
    return evalJSONContains(args[0], args[1])
```

Helper `evalJSONContains` parses the first argument as JSON, marshals the second argument to JSON, then walks the parsed structure to check containment. Returns `bool`.

#### 2. JSON_QUOTE(value) -- scalar

Location: `internal/executor/expr.go`, inside the function-name switch.

```go
case "JSON_QUOTE":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "JSON_QUOTE requires 1 argument",
        }
    }
    return evalJSONQuote(args[0])
```

Helper `evalJSONQuote` uses `json.Marshal` on the value and returns the resulting string. NULL input returns `"null"`.

#### 3. JSON_GROUP_ARRAY(expr) -- aggregate

Location: `internal/executor/physical_aggregate.go`, inside `computeAggregate` switch (near the `LIST`/`ARRAY_AGG` case at line 672).

Pattern follows `LIST` / `ARRAY_AGG` at line 672. Collects values via `op.collectValuesWithOrderBy`, then marshals the resulting slice with `json.Marshal` to produce a JSON array string. NULL values in the group SHALL be included as JSON `null` entries (not filtered out).

Also requires recognition in the aggregate-name switch in `internal/executor/expr.go` around line 2575 (the `case "STRING_AGG", "GROUP_CONCAT", "LIST", ...` block at line 2557, and the `case "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT":` block at line 2575) so that it is classified as an aggregate during expression evaluation.

#### 4. JSON_GROUP_OBJECT(key, value) -- aggregate

Location: Same file as above. Requires 2 arguments. Collects key-value pairs using `op.collectValues` on both `fn.Args[0]` and `fn.Args[1]`, builds an `ordered map` (or slice of key-value pairs to preserve order), then marshals with `json.Marshal`.

#### 5. JSON_EACH(json) -- table function

New file: `internal/executor/table_function_json_each.go`.

Dispatch location: `internal/executor/table_function_csv.go:77` inside `executeTableFunctionScan`, add:

```go
case "json_each":
    return e.executeJSONEach(ctx, plan)
```

The function parses the JSON argument. For objects, it produces rows with `(key string, value string)` where key is the object key and value is the JSON-encoded value. For arrays, key is the integer index (as string) and value is the JSON-encoded element.

Output columns: `key VARCHAR, value VARCHAR`.

### Key utility functions and their signatures

- `toString(v any) string` -- expr.go:3361
- `toBool(v any) bool` -- expr.go:3343, returns single bool
- `toInt64Value(v any) int64` -- expr.go:3369
- `toFloat64Value(v any) float64` -- expr.go:3391
- `promoteType(t1, t2 dukdb.Type) dukdb.Type` -- internal/binder/utils.go:186
- Error construction uses `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "..."}` (field is `Msg`, not `Message`)
- JSON utility helpers in `internal/io/json/` package (imported as `jsonutil`)

### Testing approach

Each function gets integration tests via `database/sql`:

1. `TestJSONContains` -- positive/negative containment, nested objects, arrays
2. `TestJSONQuote` -- strings, numbers, booleans, null
3. `TestJSONGroupArray` -- aggregate over groups, empty groups, NULL handling
4. `TestJSONGroupObject` -- aggregate key-value pairs, duplicate keys
5. `TestJSONEach` -- object expansion, array expansion, nested values, empty input

## Context

DuckDB v1.4.3 includes a comprehensive JSON extension. Applications migrating from DuckDB expect these functions to be available. The five missing functions represent the most commonly used remaining gaps.

## Goals / Non-Goals

- Goals: Feature parity with DuckDB v1.4.3 JSON functions; correct NULL handling; proper error messages
- Non-Goals: JSONPath support (DuckDB's `json_extract_path` family); JSON indexing; JSON storage type (these are separate proposals)

## Decisions

- Decision: Implement JSON_EACH as a table function (not a scalar) to match DuckDB semantics where it returns multiple rows
- Alternatives considered: Implementing as unnest(json_keys()) + json_extract -- rejected because it doesn't match DuckDB's API and loses array index information
- Decision: JSON_GROUP_ARRAY and JSON_GROUP_OBJECT return VARCHAR (JSON strings), not a native JSON type, matching DuckDB behavior
- Decision: JSON_CONTAINS performs deep equality checking, matching MySQL/DuckDB semantics

## Risks / Trade-offs

- JSON parsing overhead: Each call to JSON_CONTAINS parses JSON from scratch. Mitigation: acceptable for correctness-first approach; caching can be added later.
- JSON_EACH table function requires planner awareness: The binder/planner must recognize `json_each` as a table function name. Mitigation: follows the same pattern as `unnest` and `generate_series`.

## Open Questions

- None at this time. All five functions have well-defined semantics from DuckDB documentation.
