# Design: Temporal Functions Round 2

## Architecture

Three areas of change: (1) new date part specifiers in parseDatePart/extractPart, (2) new scalar functions, (3) DATEPART alias.

## 1. New Date Part Specifiers (temporal_functions.go)

### 1.1 parseDatePart() additions (line 35)

Add to the switch at temporal_functions.go:36-62:

```go
case "isodow":
    return DatePartISODow, nil
case "isoyear":
    return DatePartISOYear, nil
case "weekday":
    return DatePartDayOfWeek, nil  // Alias for DOW
case "weekofyear":
    return DatePartWeek, nil       // Alias for WEEK
case "nanosecond", "nanoseconds", "ns":
    return DatePartNanosecond, nil
```

### 1.2 New DatePart constants (temporal_functions.go:19-31)

Add new constants:

```go
DatePartISODow      DatePart = "isodow"
DatePartISOYear     DatePart = "isoyear"
DatePartNanosecond  DatePart = "nanosecond"
```

Note: WEEKDAY maps to existing DatePartDayOfWeek, WEEKOFYEAR maps to existing DatePartWeek — no new constants needed for those.

### 1.3 extractPart() additions (temporal_functions.go:946)

Add cases in extractPart() switch:

```go
case DatePartISODow:
    // ISO day of week: Monday=1 through Sunday=7
    dow := int(t.Weekday())
    if dow == 0 {
        dow = 7  // Sunday is 7 in ISO, 0 in Go
    }
    return float64(dow)

case DatePartISOYear:
    year, _ := t.ISOWeek()
    return float64(year)

case DatePartNanosecond:
    return float64(t.Nanosecond())
```

Note on DayOfWeek behavior:
- Go's `t.Weekday()` returns Sunday=0, Monday=1, ..., Saturday=6
- DuckDB's DOW also returns Sunday=0 (same as Go) — the existing DatePartDayOfWeek is correct
- DuckDB's ISODOW returns Monday=1, ..., Sunday=7 (ISO 8601 standard) — needs the conversion above

## 2. DATEPART Alias (expr.go)

DATEPART is an alias for DATE_PART. Add to the existing case:

```go
// BEFORE (expr.go:1961):
case "DATE_PART":
// AFTER:
case "DATE_PART", "DATEPART":
```

## 3. TIME_BUCKET Function

TIME_BUCKET(bucket_width, timestamp [, origin]) — truncates a timestamp to the nearest bucket boundary. Critical for time-series analytics.

```go
case "TIME_BUCKET":
    if len(args) < 2 || len(args) > 3 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "TIME_BUCKET requires 2 or 3 arguments (interval, timestamp [, origin])",
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    // Parse bucket width as interval
    interval, err := toInterval(args[0])
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TIME_BUCKET: invalid interval: %v", err),
        }
    }
    // Get timestamp
    ts, err := toTime(args[1])
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TIME_BUCKET: invalid timestamp: %v", err),
        }
    }
    // Optional origin
    origin := time.Date(2000, 1, 3, 0, 0, 0, 0, ts.Location()) // DuckDB default origin (Monday)
    if len(args) == 3 && args[2] != nil {
        origin, err = toTime(args[2])
        if err != nil {
            return nil, &dukdb.Error{
                Type: dukdb.ErrorTypeExecutor,
                Msg:  fmt.Sprintf("TIME_BUCKET: invalid origin: %v", err),
            }
        }
    }
    // Calculate bucket
    bucketMicros := intervalToMicros(interval)
    if bucketMicros <= 0 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "TIME_BUCKET: bucket width must be positive",
        }
    }
    tsMicros := ts.UnixMicro()
    originMicros := origin.UnixMicro()
    diff := tsMicros - originMicros
    bucketStart := originMicros + (diff/bucketMicros)*bucketMicros
    if diff < 0 && diff%bucketMicros != 0 {
        bucketStart -= bucketMicros  // Floor for negative offsets
    }
    return time.UnixMicro(bucketStart), nil
```

Note: Need helper `intervalToMicros(interval)` to convert Interval to total microseconds. The Interval type has Months, Days, Micros fields (types.go). For TIME_BUCKET, only the Micros and Days fields are meaningful (months have variable length).

```go
func intervalToMicros(iv dukdb.Interval) int64 {
    return iv.Micros + int64(iv.Days)*24*60*60*1_000_000
}
```

Type inference: `return dukdb.TYPE_TIMESTAMP`

## 4. MAKE_TIMESTAMPTZ Function

Completes the MAKE_* family. Same as MAKE_TIMESTAMP but includes timezone:

```go
case "MAKE_TIMESTAMPTZ":
    // MAKE_TIMESTAMPTZ(year, month, day, hour, min, sec [, timezone])
    if len(args) < 6 || len(args) > 7 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "MAKE_TIMESTAMPTZ requires 6 or 7 arguments",
        }
    }
    for _, a := range args[:6] {
        if a == nil { return nil, nil }
    }
    year := int(toInt64(args[0]))
    month := time.Month(toInt64(args[1]))
    day := int(toInt64(args[2]))
    hour := int(toInt64(args[3]))
    min := int(toInt64(args[4]))
    sec := toFloat64Value(args[5])
    wholeSec := int(sec)
    nsec := int((sec - float64(wholeSec)) * 1e9)
    loc := time.UTC
    if len(args) == 7 && args[6] != nil {
        tzName := toString(args[6])
        var err error
        loc, err = time.LoadLocation(tzName)
        if err != nil {
            return nil, &dukdb.Error{
                Type: dukdb.ErrorTypeExecutor,
                Msg:  fmt.Sprintf("MAKE_TIMESTAMPTZ: unknown timezone: %s", tzName),
            }
        }
    }
    return time.Date(year, month, day, hour, min, wholeSec, nsec, loc), nil
```

Type inference: `return dukdb.TYPE_TIMESTAMP_TZ`

## 5. TIMEZONE Function

TIMEZONE(tz, timestamp) — converts timestamp to specified timezone. Equivalent to `timestamp AT TIME ZONE 'tz'`:

```go
case "TIMEZONE":
    if len(args) != 2 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "TIMEZONE requires 2 arguments (timezone, timestamp)",
        }
    }
    if args[0] == nil || args[1] == nil {
        return nil, nil
    }
    tzName := toString(args[0])
    loc, err := time.LoadLocation(tzName)
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TIMEZONE: unknown timezone: %s", tzName),
        }
    }
    ts, err := toTime(args[1])
    if err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("TIMEZONE: invalid timestamp: %v", err),
        }
    }
    return ts.In(loc), nil
```

Note: `AT TIME ZONE` SQL syntax would require parser changes (parsing `expr AT TIME ZONE 'tz'` as a function call). This is deferred — only the TIMEZONE() function form is included in this proposal.

Type inference: `return dukdb.TYPE_TIMESTAMP_TZ`

## 6. EPOCH_NS (date part)

Already handled by adding DatePartNanosecond in section 1. The EPOCH_NS function (convert epoch nanoseconds to timestamp) is separate:

```go
case "EPOCH_NS":
    // Convert nanoseconds since epoch to timestamp
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  "EPOCH_NS requires 1 argument",
        }
    }
    if args[0] == nil { return nil, nil }
    ns := toInt64(args[0])
    return time.Unix(0, ns), nil
```

Type inference: `return dukdb.TYPE_TIMESTAMP`

## Helper Signatures Reference (Verified)

- `evaluateFunctionCall()` — expr.go:661 — function dispatch
- DATE_PART case — expr.go:1961 — existing date part dispatch
- `evalDatePart()` — temporal_functions.go:904 — calls parseDatePart + extractPart
- `parseDatePart()` — temporal_functions.go:35 — string → DatePart mapping
- `extractPart()` — temporal_functions.go:946 — DatePart → float64 extraction
- DatePart constants — temporal_functions.go:19-31 — Year, Quarter, Month, etc.
- MAKE_TIMESTAMP — expr.go:1974 — `evalMakeTimestamp(args)`
- MAKE_DATE — expr.go:1971 — `evalMakeDate(args)`
- `toTime()` — temporal_functions.go helper for any → time.Time
- `toInt64()` — expr.go numeric conversion
- `toFloat64Value()` — expr.go numeric conversion
- `toString()` — expr.go:4202 — any → string
- Interval type — dukdb.Interval{Months int32, Days int32, Micros int64}
- `inferFunctionResultType()` — binder/utils.go:347 — type inference
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT DATE_PART('isodow', TIMESTAMP '2024-01-01')` → 1 (Monday)
2. `SELECT DATE_PART('isodow', TIMESTAMP '2024-01-07')` → 7 (Sunday)
3. `SELECT DATE_PART('isoyear', TIMESTAMP '2024-12-30')` → 2025 (ISO year)
4. `SELECT DATEPART('year', TIMESTAMP '2024-06-15')` → 2024 (alias)
5. `SELECT TIME_BUCKET(INTERVAL '1 hour', TIMESTAMP '2024-01-01 14:37:00')` → '2024-01-01 14:00:00'
6. `SELECT TIME_BUCKET(INTERVAL '5 minutes', TIMESTAMP '2024-01-01 14:37:22')` → '2024-01-01 14:35:00'
7. `SELECT MAKE_TIMESTAMPTZ(2024, 1, 1, 12, 0, 0, 'America/New_York')` → timestamp with EST
8. `SELECT TIMEZONE('UTC', TIMESTAMP '2024-01-01 12:00:00')` → UTC conversion
9. `SELECT EPOCH_NS(1704067200000000000)` → '2024-01-01 00:00:00'
10. NULL propagation for all functions
