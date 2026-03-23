# Design: Missing Scalar Functions Round 5

## Architecture

All functions are simple case additions to evaluateFunctionCall() dispatch at expr.go:659. No parser or planner changes needed. Functions grouped by category.

## 1. Math Constants: E(), INF(), NAN()

Zero-argument functions returning mathematical constants. Follow piValue() pattern at math.go:760.

Add near PI() case at expr.go:1030:

```go
case "E":
    if len(args) != 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "E requires 0 arguments",
        }
    }
    return math.E, nil

case "INF", "INFINITY":
    if len(args) != 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "INF requires 0 arguments",
        }
    }
    return math.Inf(1), nil

case "NAN":
    if len(args) != 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "NAN requires 0 arguments",
        }
    }
    return math.NaN(), nil
```

Note: `math.E`, `math.Inf()`, `math.NaN()` are Go stdlib. `math` is already imported in expr.go.

Type inference: all three return `dukdb.TYPE_DOUBLE`.

## 2. UUID Generation: UUID(), GEN_RANDOM_UUID()

Non-deterministic function returning a random UUID v4 string. The `github.com/google/uuid` package is already in go.mod (v1.6.0).

Add near RANDOM case at expr.go:1039:

```go
case "UUID", "GEN_RANDOM_UUID":
    if len(args) != 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "UUID requires 0 arguments",
        }
    }
    return uuid.New().String(), nil
```

Note: Must add `"github.com/google/uuid"` import to expr.go. Package is already in go.mod.

Type inference: `return dukdb.TYPE_VARCHAR` (DuckDB returns UUID type, but our string representation is compatible).

Query cache: Add `"UUID": {}` and `"GEN_RANDOM_UUID": {}` to volatileFuncs map at query_cache.go:211-217 since these are non-deterministic (like RANDOM).

## 3. SPLIT_PART(string, delimiter, index)

Splits string by delimiter and returns the 1-based indexed part. PostgreSQL/DuckDB standard function.

Add near string functions (around expr.go:1257 near LENGTH):

```go
case "SPLIT_PART":
    if len(args) != 3 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SPLIT_PART requires 3 arguments (string, delimiter, index)",
        }
    }
    if args[0] == nil || args[1] == nil || args[2] == nil {
        return nil, nil
    }
    s := toString(args[0])
    delim := toString(args[1])
    idx := int(toInt64Value(args[2]))

    parts := strings.Split(s, delim)

    // Negative index counts from end (-1 = last part)
    if idx < 0 {
        idx = len(parts) + idx + 1
    }

    // 1-based indexing, out of range returns empty string
    if idx < 1 || idx > len(parts) {
        return "", nil
    }
    return parts[idx-1], nil
```

Note: `toString()` is at expr.go:4922, `toInt64Value()` is at expr.go:4930. `strings` is already imported in expr.go.

Type inference: `return dukdb.TYPE_VARCHAR`.

## 4. LOG(x, base) — 2-argument variant

The existing LOG case at expr.go:840 handles `LOG(x)` as LOG10. DuckDB also supports `LOG(x, base)` for logarithm with arbitrary base.

Modify existing case at expr.go:840:

```go
case "LOG", "LOG10":
    if len(args) == 1 {
        // Existing behavior: LOG10(x)
        if args[0] == nil {
            return nil, nil
        }
        f := toFloat64Value(args[0])
        return math.Log10(f), nil
    } else if len(args) == 2 {
        // LOG(x, base) — logarithm of x with given base
        if args[0] == nil || args[1] == nil {
            return nil, nil
        }
        x := toFloat64Value(args[0])
        base := toFloat64Value(args[1])
        if base <= 0 || base == 1 {
            return math.NaN(), nil
        }
        return math.Log(x) / math.Log(base), nil
    } else {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "LOG requires 1 or 2 arguments",
        }
    }
```

Note: Current code at line 840-845 requires exactly 1 argument. Must change to accept 1 or 2 args. The existing `case "LOG", "LOG10":` already combines both names.

## 5. SHA512(value)

Returns SHA-512 hex digest. Follow sha256Value() pattern at hash.go:25-35.

Add sha512Value() function to hash.go:

```go
func sha512Value(str any) (any, error) {
    if str == nil {
        return nil, nil
    }
    s := toString(str)
    hash := sha512.Sum512([]byte(s))
    return hex.EncodeToString(hash[:]), nil
}
```

Add case in expr.go near SHA256 at line 1967:

```go
case "SHA512":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SHA512 requires 1 argument",
        }
    }
    return sha512Value(args[0])
```

Note: Must add `"crypto/sha512"` import to hash.go. `hex` and `toString()` are already available there.

Type inference: `return dukdb.TYPE_VARCHAR`.

## 6. MILLISECOND(timestamp), MICROSECOND(timestamp)

Extract sub-second components from timestamps. Follow evalYear() pattern at temporal_functions.go:130 and SECOND case at expr.go:2093.

Add near SECOND case at expr.go:2093:

```go
case "MILLISECOND":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "MILLISECOND requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    ts, err := toTime(args[0])
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("MILLISECOND: invalid timestamp: %v", err),
        }
    }
    return int64(ts.Nanosecond() / 1_000_000), nil

case "MICROSECOND":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "MICROSECOND requires 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    ts, err := toTime(args[0])
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("MICROSECOND: invalid timestamp: %v", err),
        }
    }
    return int64(ts.Nanosecond() / 1_000), nil
```

Note: `toTime()` is at temporal_functions.go:721. Returns (time.Time, error).

DuckDB MILLISECOND behavior: Returns the millisecond component only (0-999), NOT total milliseconds. Same for MICROSECOND: returns 0-999999 component.

Type inference: both return `dukdb.TYPE_INTEGER`.

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:659 — function dispatch entry point
- Main switch fn.Name — expr.go:717 — function name dispatch
- PI case — expr.go:1030 — zero-arg constant pattern
- `piValue()` — math.go:760 — returns math.Pi
- RANDOM case — expr.go:1039 — zero-arg non-deterministic pattern
- LOG case — expr.go:840 — current 1-arg LOG10 implementation
- SHA256 case — expr.go:1967 — hash function pattern
- `sha256Value()` — hash.go:25-35 — hash helper pattern (NULL check, toString, hex encode)
- YEAR case — expr.go:2078 — temporal extraction pattern
- SECOND case — expr.go:2093 — temporal extraction (nearest to MILLISECOND)
- `toTime()` — temporal_functions.go:721 — any → time.Time conversion
- `toString()` — expr.go:4922 — any → string conversion
- `toInt64Value()` — expr.go:4930 — any → int64 conversion
- `toFloat64Value()` — expr.go:4952 — any → float64 conversion
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- `volatileFuncs` — query_cache.go:211-217 — non-deterministic function exclusion map
- `github.com/google/uuid` — go.mod:14 — UUID v4 generation (already in go.mod)
- `crypto/sha512` — Go stdlib — SHA-512 hash (needs import in hash.go)
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT E()` → 2.718281828459045 (math.E)
2. `SELECT INF()` → +Inf
3. `SELECT NAN()` → NaN
4. `SELECT NAN() != NAN()` → true (NaN is not equal to itself)
5. `SELECT UUID()` → UUID v4 string (e.g., '550e8400-e29b-41d4-a716-446655440000')
6. `SELECT UUID() != UUID()` → true (each call returns different value)
7. `SELECT GEN_RANDOM_UUID()` → same behavior as UUID()
8. `SELECT SPLIT_PART('a-b-c', '-', 2)` → 'b'
9. `SELECT SPLIT_PART('a-b-c', '-', -1)` → 'c' (negative index)
10. `SELECT SPLIT_PART('hello', '-', 1)` → 'hello' (delimiter not found, idx=1)
11. `SELECT SPLIT_PART('hello', '-', 2)` → '' (delimiter not found, idx>1)
12. `SELECT LOG(100)` → 2.0 (existing LOG10 behavior preserved)
13. `SELECT LOG(8, 2)` → 3.0 (log base 2 of 8)
14. `SELECT LOG(27, 3)` → 3.0 (log base 3 of 27)
15. `SELECT SHA512('hello')` → hex string (128 chars)
16. `SELECT SHA512(NULL)` → NULL
17. `SELECT MILLISECOND(TIMESTAMP '2024-01-01 12:34:56.789')` → 789
18. `SELECT MICROSECOND(TIMESTAMP '2024-01-01 12:34:56.789012')` → 789012
19. NULL propagation for all functions (except E/INF/NAN/UUID which take no args)
