# Design: Add Date/Time Functions

## Overview

This document describes the architecture for adding comprehensive date/time function support to dukdb-go. The design builds on the existing function registry pattern in `internal/binder/utils.go` and adds temporal-specific evaluation logic.

## Architecture

### Function Categories

Date/time functions are organized into five categories:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Date/Time Functions                          │
├─────────────────┬─────────────────┬─────────────────────────────────┤
│   Extraction    │   Arithmetic    │     Formatting/Parsing          │
│   YEAR, MONTH   │   DATE_ADD      │     STRFTIME, STRPTIME          │
│   DAY, HOUR     │   DATE_SUB      │     TO_TIMESTAMP, EPOCH         │
│   MINUTE, etc.  │   DATE_DIFF     │                                 │
├─────────────────┼─────────────────┼─────────────────────────────────┤
│  Construction   │    Interval     │                                 │
│  MAKE_DATE      │   TO_YEARS      │                                 │
│  MAKE_TIMESTAMP │   TO_MONTHS     │                                 │
│  MAKE_TIME      │   Arithmetic    │                                 │
└─────────────────┴─────────────────┴─────────────────────────────────┘
```

### Type Mappings

| Input Type | Functions Applicable |
|------------|---------------------|
| DATE | YEAR, MONTH, DAY, DAYOFWEEK, DAYOFYEAR, WEEK, QUARTER |
| TIMESTAMP | All extraction functions |
| TIME | HOUR, MINUTE, SECOND |
| INTERVAL | TO_YEARS, TO_MONTHS, TO_DAYS, TO_HOURS, TO_MINUTES, TO_SECONDS |
| VARCHAR | STRPTIME (parsing) |

### Function Signatures

#### Extraction Functions

| Function | Signature | Return Type |
|----------|-----------|-------------|
| YEAR | YEAR(date\|timestamp) | INTEGER |
| MONTH | MONTH(date\|timestamp) | INTEGER |
| DAY | DAY(date\|timestamp) | INTEGER |
| HOUR | HOUR(time\|timestamp) | INTEGER |
| MINUTE | MINUTE(time\|timestamp) | INTEGER |
| SECOND | SECOND(time\|timestamp) | DOUBLE |
| DAYOFWEEK | DAYOFWEEK(date\|timestamp) | INTEGER |
| DAYOFYEAR | DAYOFYEAR(date\|timestamp) | INTEGER |
| WEEK | WEEK(date\|timestamp) | INTEGER |
| QUARTER | QUARTER(date\|timestamp) | INTEGER |

#### Arithmetic Functions

| Function | Signature | Return Type |
|----------|-----------|-------------|
| DATE_ADD | DATE_ADD(date\|timestamp, interval) | same as input |
| DATE_SUB | DATE_SUB(date\|timestamp, interval) | same as input |
| DATE_DIFF | DATE_DIFF(part, start, end) | BIGINT |
| DATE_TRUNC | DATE_TRUNC(part, timestamp) | TIMESTAMP |
| DATE_PART | DATE_PART(part, timestamp) | DOUBLE |
| AGE | AGE(timestamp, timestamp) | INTERVAL |
| LAST_DAY | LAST_DAY(date) | DATE |

#### Construction Functions

| Function | Signature | Return Type |
|----------|-----------|-------------|
| MAKE_DATE | MAKE_DATE(year, month, day) | DATE |
| MAKE_TIMESTAMP | MAKE_TIMESTAMP(y, m, d, h, min, sec) | TIMESTAMP |
| MAKE_TIME | MAKE_TIME(hour, minute, second) | TIME |

#### Formatting/Parsing Functions

| Function | Signature | Return Type |
|----------|-----------|-------------|
| STRFTIME | STRFTIME(format, timestamp) | VARCHAR |
| STRPTIME | STRPTIME(string, format) | TIMESTAMP |
| TO_TIMESTAMP | TO_TIMESTAMP(seconds) | TIMESTAMP |
| EPOCH | EPOCH(timestamp) | DOUBLE |
| EPOCH_MS | EPOCH_MS(timestamp) | BIGINT |

### Part Specifier Enum

Functions like DATE_DIFF, DATE_TRUNC, DATE_PART accept a "part" specifier:

```go
type DatePart string

const (
    DatePartYear        DatePart = "year"
    DatePartQuarter     DatePart = "quarter"
    DatePartMonth       DatePart = "month"
    DatePartWeek        DatePart = "week"
    DatePartDay         DatePart = "day"
    DatePartDayOfWeek   DatePart = "dayofweek"
    DatePartDayOfYear   DatePart = "dayofyear"
    DatePartHour        DatePart = "hour"
    DatePartMinute      DatePart = "minute"
    DatePartSecond      DatePart = "second"
    DatePartMillisecond DatePart = "millisecond"
    DatePartMicrosecond DatePart = "microsecond"
    DatePartEpoch       DatePart = "epoch"
)
```

### STRFTIME Format Specifiers

Supported format specifiers (subset of C strftime):

| Specifier | Description | Example |
|-----------|-------------|---------|
| %Y | 4-digit year | 2024 |
| %y | 2-digit year | 24 |
| %m | Month (01-12) | 03 |
| %d | Day of month (01-31) | 15 |
| %H | Hour 24h (00-23) | 14 |
| %I | Hour 12h (01-12) | 02 |
| %M | Minute (00-59) | 30 |
| %S | Second (00-59) | 45 |
| %f | Microseconds | 123456 |
| %p | AM/PM | PM |
| %j | Day of year (001-366) | 074 |
| %W | Week of year (00-53) | 11 |
| %w | Weekday (0-6, Sunday=0) | 5 |
| %a | Abbreviated weekday | Fri |
| %A | Full weekday | Friday |
| %b | Abbreviated month | Mar |
| %B | Full month | March |
| %% | Literal % | % |

### INTERVAL Parsing

INTERVAL literals follow DuckDB syntax:

```sql
-- Single unit
INTERVAL '5' DAY
INTERVAL '2' HOUR
INTERVAL '30' MINUTE

-- Compound (Phase 1: simple parsing)
INTERVAL '1 day'
INTERVAL '2 hours 30 minutes'

-- Compound with parts
INTERVAL '1-2' YEAR TO MONTH  -- 1 year 2 months
INTERVAL '3 04:05:06' DAY TO SECOND  -- 3 days, 4:05:06
```

### Internal Representation

Extend the existing temporal type handling in the executor:

```go
// Date extraction from timestamp (Unix microseconds)
func extractYear(timestamp int64) int {
    t := time.UnixMicro(timestamp)
    return t.Year()
}

func extractMonth(timestamp int64) int {
    t := time.UnixMicro(timestamp)
    return int(t.Month())
}

func extractDay(timestamp int64) int {
    t := time.UnixMicro(timestamp)
    return t.Day()
}

// Date from days since epoch (1970-01-01)
func dateToTime(days int32) time.Time {
    return time.Unix(int64(days)*86400, 0).UTC()
}

// Time from microseconds since midnight
func timeToComponents(micros int64) (hour, minute, second int, frac float64) {
    hour = int(micros / (3600 * 1_000_000))
    micros %= 3600 * 1_000_000
    minute = int(micros / (60 * 1_000_000))
    micros %= 60 * 1_000_000
    second = int(micros / 1_000_000)
    frac = float64(micros%1_000_000) / 1_000_000
    return
}
```

### Error Handling

| Error Condition | Error Type |
|-----------------|------------|
| Invalid date part specifier | ErrorTypeBinder |
| Invalid format string | ErrorTypeExecutor |
| Date out of range | ErrorTypeExecutor |
| Invalid date components (Feb 30) | ErrorTypeExecutor |
| NULL input (depending on function) | Returns NULL |

### NULL Handling

Following DuckDB semantics:
- All extraction functions: NULL input -> NULL output
- DATE_ADD/DATE_SUB: NULL date or interval -> NULL output
- DATE_DIFF: NULL for any argument -> NULL output
- STRFTIME: NULL timestamp -> NULL output
- STRPTIME: NULL string or unparseable string -> NULL output
- MAKE_DATE/TIMESTAMP: NULL for any argument -> NULL output

### Integration with Existing Code

#### Binder Changes (internal/binder/utils.go)

Extend `inferFunctionResultType`:

```go
case "YEAR", "MONTH", "DAY", "DAYOFWEEK", "DAYOFYEAR", "WEEK", "QUARTER":
    return dukdb.TYPE_INTEGER
case "HOUR", "MINUTE":
    return dukdb.TYPE_INTEGER
case "SECOND", "DATE_PART", "EPOCH":
    return dukdb.TYPE_DOUBLE
case "DATE_ADD", "DATE_SUB":
    if len(args) > 0 {
        return args[0].ResultType() // Same as input type
    }
    return dukdb.TYPE_TIMESTAMP
case "DATE_TRUNC":
    return dukdb.TYPE_TIMESTAMP
case "DATE_DIFF", "EPOCH_MS":
    return dukdb.TYPE_BIGINT
case "AGE":
    return dukdb.TYPE_INTERVAL
case "MAKE_DATE", "LAST_DAY":
    return dukdb.TYPE_DATE
case "MAKE_TIMESTAMP", "STRPTIME", "TO_TIMESTAMP":
    return dukdb.TYPE_TIMESTAMP
case "MAKE_TIME":
    return dukdb.TYPE_TIME
case "STRFTIME":
    return dukdb.TYPE_VARCHAR
```

Extend `getFunctionArgTypes`:

```go
case "YEAR", "MONTH", "DAY", "DAYOFWEEK", "DAYOFYEAR", "WEEK", "QUARTER":
    return []dukdb.Type{dukdb.TYPE_DATE}
case "HOUR", "MINUTE", "SECOND":
    return []dukdb.Type{dukdb.TYPE_TIMESTAMP}
case "DATE_ADD", "DATE_SUB":
    return []dukdb.Type{dukdb.TYPE_TIMESTAMP, dukdb.TYPE_INTERVAL}
case "DATE_DIFF":
    return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP}
case "DATE_TRUNC", "DATE_PART":
    return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP}
case "STRFTIME":
    return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_TIMESTAMP}
case "STRPTIME":
    return []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR}
case "MAKE_DATE":
    return []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER}
case "MAKE_TIMESTAMP":
    return []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER,
        dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
case "TO_TIMESTAMP":
    return []dukdb.Type{dukdb.TYPE_DOUBLE}
case "EPOCH", "EPOCH_MS":
    return []dukdb.Type{dukdb.TYPE_TIMESTAMP}
```

#### Executor Changes (internal/executor/)

Create new file `temporal_functions.go`:

```go
package executor

import (
    "fmt"
    "time"
)

// evaluateTemporalFunction evaluates date/time functions
func (e *Executor) evaluateTemporalFunction(name string, args []any) (any, error) {
    switch strings.ToUpper(name) {
    case "YEAR":
        return e.evalYear(args)
    case "MONTH":
        return e.evalMonth(args)
    // ... other functions
    }
}

func (e *Executor) evalYear(args []any) (any, error) {
    if len(args) != 1 {
        return nil, fmt.Errorf("YEAR requires exactly 1 argument")
    }
    if args[0] == nil {
        return nil, nil // NULL propagation
    }

    switch v := args[0].(type) {
    case int32: // DATE (days since epoch)
        t := time.Unix(int64(v)*86400, 0).UTC()
        return int32(t.Year()), nil
    case int64: // TIMESTAMP (microseconds since epoch)
        t := time.UnixMicro(v).UTC()
        return int32(t.Year()), nil
    default:
        return nil, fmt.Errorf("YEAR: unsupported type %T", args[0])
    }
}
```

### Testing Strategy

1. **Unit tests** for each function in isolation
2. **Type coercion tests** (DATE vs TIMESTAMP inputs)
3. **NULL handling tests** for all functions
4. **Edge case tests** (leap years, DST boundaries, epoch boundaries)
5. **Format specifier tests** for STRFTIME/STRPTIME
6. **Compatibility tests** against DuckDB CLI reference

### Performance Considerations

- Date extraction functions are O(1) - simple arithmetic
- STRFTIME/STRPTIME are O(n) where n is format string length
- Interval arithmetic is O(1)
- No special optimization needed for initial implementation
