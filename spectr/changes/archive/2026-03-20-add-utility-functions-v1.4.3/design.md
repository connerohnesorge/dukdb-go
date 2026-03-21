# Design: Missing Utility Functions for DuckDB v1.4.3

## Architecture

All functions follow the established two-layer pattern:
1. **Binder** (`internal/binder/utils.go`): Add return type inference
2. **Executor** (`internal/executor/expr.go`): Add evaluation logic

System functions additionally need:
3. **Connection context** (`internal/executor/operator.go`): Access database/schema info via `ctx.conn`

## 1. System Functions

### CURRENT_DATABASE()

Returns the database name. The `registerInformationSchema()` function at `engine.go:258-278` computes `dbName` from the path:
- `:memory:` → `"memory"`
- file path → `filepath.Base(path)` (e.g., `/path/to/test.db` → `"test.db"`)
- empty → `"dukdb"`

**Approach:** Set `"database_name"` as a default setting when creating connections. The `Open()` method in `engine.go` already has the path — after calling `registerInformationSchema()`, compute `dbName` the same way and call `conn.SetSetting("database_name", dbName)` on the new connection. Then the executor reads it via `ctx.conn.GetSetting("database_name")`.

**Executor:**
```go
case "CURRENT_DATABASE":
    if ctx.conn != nil {
        return ctx.conn.GetSetting("database_name"), nil
    }
    return "memory", nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

### CURRENT_SCHEMA()

Returns the current schema name. DuckDB defaults to `"main"`.

**Executor:**
```go
case "CURRENT_SCHEMA":
    if ctx.conn != nil {
        schema := ctx.conn.GetSetting("search_path")
        if schema != "" {
            return schema, nil
        }
    }
    return "main", nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

### VERSION()

Returns a version string indicating DuckDB compatibility level.

**Executor:**
```go
case "VERSION":
    return "v1.4.3 (dukdb-go)", nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

## 2. Date/Time Functions

### Existing date infrastructure

The executor already handles EPOCH (line 1624) and EPOCH_MS (line 1627). These delegate to `evalEpoch(args)` and `evalEpochMs(args)` helper functions respectively. The key helper for date conversion:

- `toTime(v any) (time.Time, error)` — in `internal/executor/temporal_functions.go:708`
- Handles `time.Time`, `string` (parsed), `int64` (epoch)
- Returns `error` (not `bool`) on conversion failure

### DAYNAME(date)

**Executor:**
```go
case "DAYNAME":
    if len(args) != 1 || args[0] == nil {
        return nil, nil
    }
    t, err := toTime(args[0])
    if err != nil {
        return nil, fmt.Errorf("DAYNAME requires a date argument")
    }
    return t.Weekday().String(), nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

### MONTHNAME(date)

**Executor:**
```go
case "MONTHNAME":
    if len(args) != 1 || args[0] == nil {
        return nil, nil
    }
    t, err := toTime(args[0])
    if err != nil {
        return nil, fmt.Errorf("MONTHNAME requires a date argument")
    }
    return t.Month().String(), nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

### YEARWEEK(date)

Returns ISO year and week as YYYYWW integer.

**Executor:**
```go
case "YEARWEEK":
    if len(args) != 1 || args[0] == nil {
        return nil, nil
    }
    t, err := toTime(args[0])
    if err != nil {
        return nil, fmt.Errorf("YEARWEEK requires a date argument")
    }
    year, week := t.ISOWeek()
    return int64(year*100 + week), nil
```

**Binder:** `return dukdb.TYPE_BIGINT` (executor returns int64, so BIGINT is the correct type)

### EPOCH_US(timestamp)

Returns epoch in microseconds. Complements EPOCH (seconds, line 1624) and EPOCH_MS (milliseconds, line 1627).

**Executor:**
```go
case "EPOCH_US":
    if len(args) != 1 || args[0] == nil {
        return nil, nil
    }
    t, err := toTime(args[0])
    if err != nil {
        return nil, fmt.Errorf("EPOCH_US requires a timestamp argument")
    }
    return t.UnixMicro(), nil
```

**Binder:** `return dukdb.TYPE_BIGINT`

### NOW() / CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME

SQL standard temporal functions. These return the current date/time. Confirmed missing from the function dispatch — only referenced in `physical_maintenance.go:252` as keyword list but not implemented as callable functions.

**Executor:**
```go
case "NOW", "CURRENT_TIMESTAMP":
    return time.Now(), nil

case "CURRENT_DATE":
    now := time.Now()
    return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()), nil

case "CURRENT_TIME":
    now := time.Now()
    return now.Format("15:04:05"), nil
```

**Binder:** NOW/CURRENT_TIMESTAMP → `return dukdb.TYPE_TIMESTAMP`, CURRENT_DATE → `return dukdb.TYPE_DATE`, CURRENT_TIME → `return dukdb.TYPE_VARCHAR` (returns formatted string "HH:MM:SS", not a time.Time object)

**Note:** These are zero-argument functions and require parentheses in SELECT expressions (e.g., `SELECT NOW()`, `SELECT CURRENT_DATE()`). The parser handles them as keywords in some contexts (DEFAULT values), but in SELECT they must be called as functions. If bare-keyword support is needed (e.g., `SELECT CURRENT_DATE` without parens), the parser's `parseIdentExpr()` would need special-casing to recognize these identifiers and convert them to zero-argument function calls.

## 3. String Functions

### TRANSLATE(string, from, to)

Replaces characters: each char in `from` is replaced by the corresponding char in `to`. If `to` is shorter, extra `from` chars are deleted.

**Executor:**
```go
case "TRANSLATE":
    if len(args) != 3 {
        return nil, fmt.Errorf("TRANSLATE requires 3 arguments")
    }
    if args[0] == nil {
        return nil, nil
    }
    str := toString(args[0])
    from := toString(args[1])
    to := toString(args[2])

    fromRunes := []rune(from)
    toRunes := []rune(to)
    mapping := make(map[rune]rune, len(fromRunes))
    deleteSet := make(map[rune]bool)
    for i, r := range fromRunes {
        if i < len(toRunes) {
            mapping[r] = toRunes[i]
        } else {
            deleteSet[r] = true
        }
    }

    var result strings.Builder
    for _, r := range str {
        if deleteSet[r] {
            continue
        }
        if rep, ok := mapping[r]; ok {
            result.WriteRune(rep)
        } else {
            result.WriteRune(r)
        }
    }
    return result.String(), nil
```

**Binder:** `return dukdb.TYPE_VARCHAR`

### STRIP_ACCENTS(string)

Removes accent marks using Unicode NFD normalization.

**Executor:**
```go
case "STRIP_ACCENTS":
    if len(args) != 1 || args[0] == nil {
        return nil, nil
    }
    str := toString(args[0])
    // NFD normalize to decompose accented characters
    nfd := norm.NFD.String(str)
    // Filter out combining diacritical marks (unicode.Mn category)
    var result strings.Builder
    for _, r := range nfd {
        if !unicode.Is(unicode.Mn, r) {
            result.WriteRune(r)
        }
    }
    return result.String(), nil
```

**Import:** `golang.org/x/text/unicode/norm` (already in go.mod as indirect dependency via golang.org/x/text v0.34.0)

**Binder:** `return dukdb.TYPE_VARCHAR`

## 4. NULL Handling Functions

### IFNULL(expr, default) / NVL(expr, default)

Returns `expr` if it's not NULL, otherwise returns `default`. Equivalent to `COALESCE(expr, default)` but always takes exactly 2 arguments. COALESCE already exists at expr.go:1120-1127 — IFNULL/NVL are 2-argument aliases.

**Executor:**
```go
case "IFNULL", "NVL":
    if len(args) != 2 {
        return nil, fmt.Errorf("IFNULL requires 2 arguments")
    }
    if args[0] != nil {
        return args[0], nil
    }
    return args[1], nil
```

**Binder:** Returns the common type of both arguments (same logic as COALESCE).

## Helper Signatures Reference

- `toString(v any) string` — `internal/executor/expr.go:3361`
- `toTime(v any) (time.Time, error)` — `internal/executor/temporal_functions.go:708`
- `toInt64(v any) (int64, bool)` — `internal/executor/math.go`
- Error pattern: `fmt.Errorf("...")`

## Testing Strategy

Integration tests via `database/sql` for all 15 functions:
- System: CURRENT_DATABASE returns non-empty, CURRENT_SCHEMA returns 'main', VERSION contains 'v1.4.3'
- Date: DAYNAME('2024-01-15') = 'Monday', MONTHNAME('2024-01-15') = 'January', YEARWEEK('2024-01-15') = 202403
- Temporal: NOW() returns non-null timestamp, CURRENT_DATE returns today's date, CURRENT_TIME returns valid time
- String: TRANSLATE('hello', 'el', 'ip') = 'hippo', STRIP_ACCENTS('café') = 'cafe'
- NULL: IFNULL(NULL, 42) = 42, IFNULL(1, 42) = 1, NVL(NULL, 'default') = 'default'
