# Design: Result Handling Layer

## Architecture Overview

The result handling layer converts backend JSON responses into database/sql compatible types. Unlike duckdb-go's streaming approach, we buffer all results since the CLI returns complete JSON output.

```
Backend Response (JSON) ──→ Rows ──→ Scan ──→ Go values
     │                       │        │
     │                       │        └─ Type conversion
     │                       └─ Pre-parsed [][]any storage
     └─ {"col1": val1, "col2": val2, ...} per row
```

## Design Decisions

### Decision 1: Error Types

All error types are defined in add-project-foundation:
- **ErrorTypeClosed**: Used when operating on closed Rows
- **ErrorTypeInvalid**: Used for type conversion failures
- **ErrorTypeBadState**: Used for unexpected state (e.g., wrong destination count)

**Note:** These error types must be implemented in add-project-foundation first.

### Decision 2: Type Conversion Mapping

Complete mapping from DuckDB types to Go types:

| DuckDB Type | Go Type | JSON Source |
|-------------|---------|-------------|
| BOOLEAN | bool | JSON boolean |
| TINYINT | int8 | JSON number |
| SMALLINT | int16 | JSON number |
| INTEGER | int32 | JSON number |
| BIGINT | int64 | JSON number |
| UTINYINT | uint8 | JSON number |
| USMALLINT | uint16 | JSON number |
| UINTEGER | uint32 | JSON number |
| UBIGINT | uint64 | JSON number |
| FLOAT | float32 | JSON number |
| DOUBLE | float64 | JSON number |
| VARCHAR | string | JSON string |
| BLOB | []byte | Hex string "\x..." |
| DATE | time.Time | ISO date "2024-01-15" |
| TIME | time.Time | ISO time "15:04:05.000000" |
| TIMESTAMP | time.Time | ISO datetime "2024-01-15 15:04:05.000000" |
| TIMESTAMP_TZ | time.Time | ISO datetime with zone |
| UUID | UUID | Hyphenated string |
| INTERVAL | Interval | Object {"months", "days", "micros"} |
| DECIMAL | Decimal | String representation |
| HUGEINT | *big.Int | String representation |
| LIST | []any | JSON array |
| STRUCT | map[string]any | JSON object |
| MAP | Map | Array of {"key", "value"} objects |
| UNION | Union | Object with "tag" and value field |

### Decision 3: Compatible Conversions

**Compatible:** Conversion succeeds without error
- Any numeric type to any numeric type (may truncate)
- String to numeric (if string is valid number)
- Numeric to string (formatted)
- time.Time to string (ISO format)
- []byte to string (UTF-8)
- Any type to any (no conversion)
- nil to pointer type (sets to nil)

**Incompatible:** Conversion returns error
- String to numeric (if string is not valid number)
- Bool to numeric
- Numeric to bool
- Struct/Map/List to primitive
- Primitive to Struct/Map/List

**Error format for incompatible:**
```go
&Error{Type: ErrorTypeInvalid, Msg: "cannot scan TYPE_VARCHAR into int64"}
```

### Decision 4: NULL Handling

**Rules:**
| Destination Type | NULL Behavior |
|-----------------|---------------|
| *T (pointer) | Set pointer to nil |
| T (non-pointer) | Set to zero value of T |
| sql.Scanner | Call Scan(nil) |

**Examples:**
- NULL into *int → *int is nil
- NULL into int → int is 0
- NULL into *string → *string is nil
- NULL into string → string is "" (empty string, which is zero value)
- NULL into *bool → *bool is nil
- NULL into bool → bool is false

**Implementation:**
```go
func scanValue(src any, dest any) error {
    dv := reflect.ValueOf(dest)
    if dv.Kind() != reflect.Ptr {
        return &Error{Type: ErrorTypeInvalid, Msg: "dest must be pointer"}
    }
    dv = dv.Elem()

    if src == nil {
        // Handle NULL
        if dv.Kind() == reflect.Ptr {
            dv.Set(reflect.Zero(dv.Type()))  // Set pointer to nil
        } else {
            dv.Set(reflect.Zero(dv.Type()))  // Set to zero value
        }
        return nil
    }
    // ... handle non-NULL values
}
```

### Decision 5: Destination Count Validation

Rows.Scan() validates destination count before scanning:

```go
func (r *Rows) Scan(dest ...any) error {
    if r.closed {
        return &Error{Type: ErrorTypeClosed, Msg: "rows are closed"}
    }
    if r.index < 0 || r.index >= len(r.data) {
        return &Error{Type: ErrorTypeBadState, Msg: "no current row"}
    }
    if len(dest) != len(r.columns) {
        return &Error{Type: ErrorTypeInvalid,
            Msg: fmt.Sprintf("expected %d destinations, got %d", len(r.columns), len(dest))}
    }
    // ... scan each value
}
```

### Decision 6: sql.Scanner Support

When destination implements sql.Scanner, delegate scanning:

```go
func scanValue(src any, dest any) error {
    // Check for sql.Scanner
    if scanner, ok := dest.(sql.Scanner); ok {
        return scanner.Scan(src)  // Pass raw value (including nil)
    }
    // ... regular conversion
}
```

**Note:** sql.Scanner.Scan receives the raw JSON-parsed value, not a driver.Value.

### Decision 7: Composite[T] Scanning

For Composite[T] (UUID, Map, etc.) types from add-type-system:

```go
// For UUID
type UUID struct {
    data [16]byte
}

func (u *UUID) Scan(src any) error {
    switch v := src.(type) {
    case string:
        parsed, err := parseUUID(v)
        if err != nil {
            return err
        }
        *u = parsed
        return nil
    case []byte:
        return u.Scan(string(v))
    case nil:
        *u = UUID{}  // Zero UUID
        return nil
    default:
        return &Error{Type: ErrorTypeInvalid,
            Msg: fmt.Sprintf("cannot scan %T into UUID", src)}
    }
}
```

### Decision 8: ColumnTypeScanType Mapping

Complete mapping for ColumnTypeScanType:

```go
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
    switch r.colTypes[index] {
    case TYPE_BOOLEAN:
        return reflect.TypeOf(false)
    case TYPE_TINYINT:
        return reflect.TypeOf(int8(0))
    case TYPE_SMALLINT:
        return reflect.TypeOf(int16(0))
    case TYPE_INTEGER:
        return reflect.TypeOf(int32(0))
    case TYPE_BIGINT:
        return reflect.TypeOf(int64(0))
    case TYPE_UTINYINT:
        return reflect.TypeOf(uint8(0))
    case TYPE_USMALLINT:
        return reflect.TypeOf(uint16(0))
    case TYPE_UINTEGER:
        return reflect.TypeOf(uint32(0))
    case TYPE_UBIGINT:
        return reflect.TypeOf(uint64(0))
    case TYPE_FLOAT:
        return reflect.TypeOf(float32(0))
    case TYPE_DOUBLE:
        return reflect.TypeOf(float64(0))
    case TYPE_VARCHAR:
        return reflect.TypeOf("")
    case TYPE_BLOB:
        return reflect.TypeOf([]byte{})
    case TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
        return reflect.TypeOf(time.Time{})
    case TYPE_UUID:
        return reflect.TypeOf(UUID{})
    case TYPE_INTERVAL:
        return reflect.TypeOf(Interval{})
    case TYPE_DECIMAL:
        return reflect.TypeOf(Decimal{})
    case TYPE_HUGEINT:
        return reflect.TypeOf((*big.Int)(nil))
    case TYPE_LIST:
        return reflect.TypeOf([]any{})
    case TYPE_STRUCT:
        return reflect.TypeOf(map[string]any{})
    case TYPE_MAP:
        return reflect.TypeOf(Map{})
    case TYPE_UNION:
        return reflect.TypeOf(Union{})
    default:
        return reflect.TypeOf((*any)(nil)).Elem()  // any
    }
}
```

## Thread Safety

Rows is not thread-safe. Each Rows instance should be used from a single goroutine. This matches database/sql behavior where rows returned from QueryContext are owned by one goroutine.

## Error Types Summary

| Scenario | Error Type | Message Format |
|----------|------------|----------------|
| Scan after Close | ErrorTypeClosed | "rows are closed" |
| Wrong dest count | ErrorTypeInvalid | "expected N destinations, got M" |
| Type mismatch | ErrorTypeInvalid | "cannot scan TYPE_X into T" |
| No current row | ErrorTypeBadState | "no current row" |
