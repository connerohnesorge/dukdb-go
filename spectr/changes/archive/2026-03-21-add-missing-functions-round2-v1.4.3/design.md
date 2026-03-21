# Design: Missing Functions Round 2

## Architecture

Changes in three files:
1. **Executor** (`internal/executor/expr.go`, `internal/executor/hash.go`, `internal/executor/physical_aggregate.go`): Function implementations
2. **Binder** (`internal/binder/utils.go`): Type inference entries
3. **Executor** (`internal/executor/operator.go`): Aggregate function name registration

## 1. SHA1 Hash Function

### Implementation (internal/executor/hash.go)

Follow the SHA256 pattern at hash.go:24:

```go
import "crypto/sha1"

func sha1Value(str any) (any, error) {
    if str == nil {
        return nil, nil
    }
    s := toString(str)
    hash := sha1.Sum([]byte(s))
    return hex.EncodeToString(hash[:]), nil
}
```

### Dispatch (internal/executor/expr.go)

Add near SHA256 case at line 1749:

```go
case "SHA1":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SHA1 requires 1 argument",
        }
    }
    return sha1Value(args[0])
```

### Binder

Add "SHA1" to the VARCHAR return case alongside SHA256 and MD5.

## 2. SETSEED Function

### Implementation (internal/executor/expr.go)

Add near RANDOM case at line 953:

```go
case "SETSEED":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SETSEED requires exactly 1 argument",
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    // DuckDB SETSEED takes a value between 0 and 1
    // toFloat64 returns (float64, bool) — bool indicates success
    seedFloat, ok := toFloat64(args[0])
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SETSEED: argument must be a numeric value",
        }
    }
    if seedFloat < 0 || seedFloat > 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "SETSEED: seed must be between 0 and 1",
        }
    }
    // Store seed as setting for use by RANDOM()
    seed := int64(seedFloat * float64(1<<63))
    ctx.conn.SetSetting("random_seed", fmt.Sprintf("%d", seed))
    return nil, nil
```

### Modify RANDOM (internal/executor/math.go)

Update `randomValue()` to check for a set seed. This requires passing the execution context:

```go
func randomValueWithCtx(ctx *ExecutionContext) (any, error) {
    seedStr := ctx.conn.GetSetting("random_seed")
    if seedStr != "" {
        seed, _ := strconv.ParseInt(seedStr, 10, 64)
        src := rand.NewSource(seed)
        rng := rand.New(src)
        // Increment seed so next call gives different value
        ctx.conn.SetSetting("random_seed", fmt.Sprintf("%d", seed+1))
        return rng.Float64(), nil
    }
    return rand.Float64(), nil
}
```

**Note:** The RANDOM dispatch at expr.go:953 would need to pass `ctx` to this function.

### Binder

SETSEED returns void (no meaningful return type). Add case returning `dukdb.TYPE_ANY` or handle as special void function.

## 3. LIST_VALUE / LIST_PACK

### Implementation (internal/executor/expr.go)

Add near MAP case at line 2222:

```go
case "LIST_VALUE", "LIST_PACK":
    // Create a list from all arguments
    result := make([]any, 0, len(args))
    for _, arg := range args {
        result = append(result, arg)  // Include NULLs in list
    }
    return result, nil
```

**Semantics:** Unlike aggregates, LIST_VALUE includes NULL values in the result list. `LIST_VALUE(1, NULL, 3)` → `[1, NULL, 3]`.

### Binder

Add "LIST_VALUE", "LIST_PACK" returning `dukdb.TYPE_ANY` (list type).

## 4. ANY_VALUE Aggregate

### Implementation (internal/executor/physical_aggregate.go)

Add after FIRST case at line 707:

```go
case "ANY_VALUE":
    if len(fn.Args) == 0 {
        return nil, nil
    }
    values, err := op.collectValues(fn.Args[0], rows)
    if err != nil {
        return nil, err
    }
    return computeFirst(values)
```

Uses existing `computeFirst()` at aggregate_time.go:24 which returns the first non-NULL value.

### Registration

Add "ANY_VALUE" to `isAggregateFunc()` at operator.go:99-118. Note: FIRST, LAST, ARGMIN, ARGMAX, BIT_AND/OR/XOR, BOOL_AND/OR, EVERY are already handled in `computeAggregate()` (physical_aggregate.go) but are NOT listed in `isAggregateFunc()`. This may mean they're identified as aggregates through another mechanism (e.g., the binder). For consistency, add ANY_VALUE to `isAggregateFunc()` AND to `computeAggregate()`.

### Binder

Add "ANY_VALUE" returning the first argument's type (use `args[0].ResultType()`).

## 5. HISTOGRAM Aggregate

### Implementation (internal/executor/physical_aggregate.go)

Add new case:

```go
case "HISTOGRAM":
    if len(fn.Args) != 1 {
        return nil, nil
    }
    values, err := op.collectValues(fn.Args[0], rows)
    if err != nil {
        return nil, err
    }
    return computeHistogram(values)
```

### Compute Function (new, in aggregate_string.go or new file)

```go
func computeHistogram(values []any) (any, error) {
    counts := make(map[string]any)
    for _, v := range values {
        if v == nil {
            continue
        }
        key := toString(v)
        if existing, ok := counts[key]; ok {
            counts[key] = existing.(int64) + 1
        } else {
            counts[key] = int64(1)
        }
    }
    if len(counts) == 0 {
        return nil, nil
    }
    return counts, nil
}
```

Returns `map[string]any` where keys are stringified values and values are int64 counts.

### Registration

Add "HISTOGRAM" to `isAggregateFunc()` at operator.go:99-118 and implement in `computeAggregate()` in physical_aggregate.go.

### Binder

Add "HISTOGRAM" returning `dukdb.TYPE_ANY` (map type).

## 6. ARG_MIN / ARG_MAX Aliases

### Implementation (internal/executor/physical_aggregate.go)

Change line 727:
```go
case "ARGMIN", "MIN_BY":
```
to:
```go
case "ARGMIN", "ARG_MIN", "MIN_BY":
```

Change line 742:
```go
case "ARGMAX", "MAX_BY":
```
to:
```go
case "ARGMAX", "ARG_MAX", "MAX_BY":
```

### Registration

Add "ARG_MIN", "ARG_MAX" to `isAggregateFunc()` at operator.go:99-118 (note: ARGMIN/ARGMAX are also not in this list but work — add all four variants for consistency).

### Binder

Add "ARG_MIN", "ARG_MAX" to the existing ARGMIN/ARGMAX case that returns the first argument's type.

## Helper Signatures Reference

- `sha256Value(str any) (any, error)` — hash.go:24 — pattern for SHA1
- `randomValue() (any, error)` — math.go:763 — current RANDOM implementation
- `computeFirst(values []any) (any, error)` — aggregate_time.go:24 — reuse for ANY_VALUE
- `computeArgmin(argValues, valValues []any) (any, error)` — aggregate_time.go:47
- `computeArgmax(argValues, valValues []any) (any, error)` — aggregate_time.go:87
- `toString(v any) string` — expr.go:3710
- `toFloat64(v any) (float64, bool)` — math.go:41 — numeric conversion, returns (value, ok) pair
- `ctx.conn.SetSetting(key, value string)` — operator.go:23 — per-connection settings
- `ctx.conn.GetSetting(key string) string` — operator.go:22 — read connection setting

## Testing Strategy

1. SHA1: `SELECT SHA1('hello')` → 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' (verified: `echo -n hello | sha1sum`)
2. SETSEED: `SELECT SETSEED(0.5); SELECT RANDOM()` → deterministic value; same seed → same first value
3. LIST_VALUE: `SELECT LIST_VALUE(1, 2, 3)` → [1, 2, 3]; `SELECT LIST_VALUE(1, NULL, 3)` → [1, NULL, 3]
4. ANY_VALUE: `SELECT ANY_VALUE(x) FROM t` → any non-NULL value from x column
5. HISTOGRAM: `SELECT HISTOGRAM(x) FROM (VALUES (1), (1), (2), (3)) t(x)` → {1: 2, 2: 1, 3: 1}
6. ARG_MIN: `SELECT ARG_MIN(name, age) FROM emp` → same as ARGMIN
7. NULL propagation for all scalar functions
8. Empty input for all aggregate functions → NULL
