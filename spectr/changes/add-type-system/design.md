## Context

DuckDB has a rich type system including primitive types, temporal types, and nested/composite types. The duckdb-go CGO driver (in `duckdb-go/` reference folder) exports specific Go types. This change creates NEW implementations of these types in the root package for the pure Go `dukdb-go` driver.

**Important:** We are NOT modifying `duckdb-go/` - it is reference material only. All new code goes in the root package.

## Goals / Non-Goals

**Goals:**
- Exact API parity with duckdb-go exported types (signature compatibility)
- Correct JSON unmarshaling for DuckDB CLI output parsing
- Full sql.Scanner/driver.Valuer support for all exported types
- Support all 54 DuckDB logical types

**Non-Goals:**
- Vector/columnar operations (handled in result-handling)
- Type registration with DuckDB (not applicable to subprocess model)
- CGO bindings (this is pure Go)

## Decisions

### Decision 1: Exported Type Definitions

**What:** Create these exact type signatures in `types.go`:
```go
// UUID is a 16-byte universally unique identifier
type UUID [16]byte

// Map is a key-value mapping with any types
type Map map[any]any

// Interval represents a time interval with months, days, and microseconds
type Interval struct {
    Days   int32 `json:"days"`
    Months int32 `json:"months"`
    Micros int64 `json:"micros"`
}

// Decimal represents a fixed-point decimal number
type Decimal struct {
    Width uint8    // Total digits (1-38)
    Scale uint8    // Digits after decimal point
    Value *big.Int // Unscaled value
}

// Union represents a tagged union value
type Union struct {
    Tag   string       `json:"tag"`
    Value driver.Value `json:"value"`
}

// Composite wraps a struct/list type for scanning
type Composite[T any] struct {
    t T
}
```

**Why:** Drop-in replacement requires identical type signatures.

### Decision 2: Type Enumeration (54 constants)

**What:** Define Type enumeration in `type_enum.go` matching DuckDB's duckdb_type values:
```go
type Type uint8

const (
    TYPE_INVALID    Type = 0
    TYPE_BOOLEAN    Type = 1
    TYPE_TINYINT    Type = 2
    TYPE_SMALLINT   Type = 3
    TYPE_INTEGER    Type = 4
    TYPE_BIGINT     Type = 5
    TYPE_UTINYINT   Type = 6
    TYPE_USMALLINT  Type = 7
    TYPE_UINTEGER   Type = 8
    TYPE_UBIGINT    Type = 9
    TYPE_FLOAT      Type = 10
    TYPE_DOUBLE     Type = 11
    TYPE_TIMESTAMP  Type = 12
    TYPE_DATE       Type = 13
    TYPE_TIME       Type = 14
    TYPE_INTERVAL   Type = 15
    TYPE_HUGEINT    Type = 16
    TYPE_UHUGEINT   Type = 32
    TYPE_VARCHAR    Type = 17
    TYPE_BLOB       Type = 18
    TYPE_DECIMAL    Type = 19
    TYPE_TIMESTAMP_S  Type = 20
    TYPE_TIMESTAMP_MS Type = 21
    TYPE_TIMESTAMP_NS Type = 22
    TYPE_ENUM         Type = 23
    TYPE_LIST         Type = 24
    TYPE_STRUCT       Type = 25
    TYPE_MAP          Type = 26
    TYPE_ARRAY        Type = 33
    TYPE_UUID         Type = 27
    TYPE_UNION        Type = 28
    TYPE_BIT          Type = 29
    TYPE_TIME_TZ      Type = 30
    TYPE_TIMESTAMP_TZ Type = 31
    TYPE_ANY          Type = 34
    TYPE_VARINT       Type = 35
    TYPE_SQLNULL      Type = 36
    // ... additional types through 54
)

// String returns the uppercase type name (e.g., "VARCHAR", "TIMESTAMP_TZ")
func (t Type) String() string { ... }

// Category returns: "primitive", "temporal", "nested", or "special"
func (t Type) Category() string { ... }
```

**Why:** Required for type identification in JSON parsing and parameter binding.

### Decision 3: JSON Parsing Strategy

**What:** Implement JSON unmarshaling for DuckDB CLI output format in `type_json.go`:

| Type | DuckDB CLI JSON Format | Go Parsing |
|------|------------------------|------------|
| NULL | `null` | Return nil, no error |
| BOOLEAN | `true`/`false` | Direct json.Unmarshal |
| INTEGER variants | Number literal | json.Number → int64 → cast |
| FLOAT/DOUBLE | Number or `"Infinity"`/`"-Infinity"`/`"NaN"` | Special string handling |
| VARCHAR | `"string"` | Direct string |
| BLOB | `"\\x48454C4C4F"` (hex with `\x` prefix) | Strip prefix, hex decode |
| DATE | `"2024-01-15"` | time.Parse("2006-01-02") |
| TIME | `"10:30:45.123456"` | Custom parser with µs precision |
| TIMESTAMP | `"2024-01-15 10:30:45.123456"` | time.Parse with µs precision |
| TIMESTAMP_TZ | `"2024-01-15 10:30:45.123456+00"` | time.Parse with timezone |
| INTERVAL | `{"months":1,"days":2,"micros":3000000}` | Direct struct unmarshal |
| UUID | `"550e8400-e29b-41d4-a716-446655440000"` | Parse hyphenated format |
| DECIMAL | `"123.45"` | Parse string, compute big.Int |
| HUGEINT | `"340282366920938463463374607431768211455"` | big.Int.SetString |
| LIST | `[1, 2, 3]` | Recursive []any parsing |
| STRUCT | `{"name":"Alice","age":30}` | map[string]any parsing |
| MAP | `[{"key":1,"value":"a"},{"key":2,"value":"b"}]` | Array of {key,value} objects |
| UNION | `{"tag":"int","value":42}` | Object with tag field |
| ENUM | `"enum_value"` | String value |

**Why:** The process backend receives JSON from DuckDB CLI. Must handle all formats correctly.

### Decision 4: Special Float Values

**What:** Handle IEEE 754 special values explicitly:
```go
func parseFloat(data []byte) (float64, error) {
    var s string
    if err := json.Unmarshal(data, &s); err == nil {
        switch s {
        case "Infinity":
            return math.Inf(1), nil
        case "-Infinity":
            return math.Inf(-1), nil
        case "NaN":
            return math.NaN(), nil
        }
    }
    var f float64
    if err := json.Unmarshal(data, &f); err != nil {
        return 0, err
    }
    return f, nil
}
```

**Why:** DuckDB CLI outputs these as strings, not JSON numbers.

### Decision 5: Scanner/Valuer Implementations

**What:** All exported types implement both interfaces:

```go
// UUID implements sql.Scanner
func (u *UUID) Scan(src any) error {
    switch v := src.(type) {
    case string:
        return u.parseString(v)
    case []byte:
        if len(v) == 16 {
            copy(u[:], v)
            return nil
        }
        return u.parseString(string(v))
    case nil:
        *u = UUID{} // Zero value for NULL
        return nil
    default:
        return fmt.Errorf("cannot scan %T into UUID", src)
    }
}

// UUID implements driver.Valuer
func (u UUID) Value() (driver.Value, error) {
    return u.String(), nil
}

// Interval implements sql.Scanner
func (i *Interval) Scan(src any) error { ... }

// Interval implements driver.Valuer
func (i Interval) Value() (driver.Value, error) {
    return fmt.Sprintf("INTERVAL '%d months %d days %d microseconds'",
        i.Months, i.Days, i.Micros), nil
}

// Decimal implements sql.Scanner
func (d *Decimal) Scan(src any) error {
    switch v := src.(type) {
    case string:
        return d.parseString(v)
    case float64:
        // Infer scale from float representation
        s := strconv.FormatFloat(v, 'f', -1, 64)
        return d.parseString(s)
    case nil:
        d.Value = nil
        return nil
    default:
        return fmt.Errorf("cannot scan %T into Decimal", src)
    }
}

// Decimal implements driver.Valuer
func (d Decimal) Value() (driver.Value, error) {
    return d.String(), nil
}

// Map implements sql.Scanner
func (m *Map) Scan(src any) error { ... }

// Map implements driver.Valuer
func (m Map) Value() (driver.Value, error) {
    return json.Marshal(m)
}

// Union implements sql.Scanner
func (u *Union) Scan(src any) error { ... }

// Union implements driver.Valuer
func (u Union) Value() (driver.Value, error) {
    return json.Marshal(u)
}

// Composite[T] implements sql.Scanner
func (c *Composite[T]) Scan(src any) error {
    switch v := src.(type) {
    case map[string]any:
        // Uses mapstructure.Decode for struct mapping
        return mapstructure.Decode(v, &c.t)
    case []any:
        // For LIST types, decode into slice
        return mapstructure.Decode(v, &c.t)
    case nil:
        var zero T
        c.t = zero
        return nil
    default:
        return fmt.Errorf("cannot scan %T into Composite[%T]", src, c.t)
    }
}

// Composite[T] implements driver.Valuer
func (c Composite[T]) Value() (driver.Value, error) {
    // Serialize to JSON for DuckDB consumption
    return json.Marshal(c.t)
}

// Composite[T].Get() returns the scanned value
func (c Composite[T]) Get() T { return c.t }

// Composite[T].Set() sets the internal value
func (c *Composite[T]) Set(v T) { c.t = v }
```

**Why:** Required for database/sql compatibility.

### Decision 6: HugeInt Conversion

**What:** Provide bidirectional conversion with overflow detection:
```go
// HugeInt represents DuckDB's 128-bit signed integer internally
type hugeInt struct {
    lower uint64
    upper int64
}

// hugeIntToBigInt converts hugeint to *big.Int
func hugeIntToBigInt(h hugeInt) *big.Int {
    result := new(big.Int).SetInt64(h.upper)
    result.Lsh(result, 64)
    result.Or(result, new(big.Int).SetUint64(h.lower))
    return result
}

// bigIntToHugeInt converts *big.Int to hugeint with overflow check
func bigIntToHugeInt(b *big.Int) (hugeInt, error) {
    // Check range: -2^127 to 2^127-1
    maxPos := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
    minNeg := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))

    if b.Cmp(maxPos) > 0 || b.Cmp(minNeg) < 0 {
        return hugeInt{}, fmt.Errorf("value %s overflows HUGEINT range", b.String())
    }

    // Handle negative numbers using two's complement
    var result hugeInt
    if b.Sign() >= 0 {
        // Positive: extract lower 64 bits and upper 64 bits
        lower := new(big.Int).And(b, new(big.Int).SetUint64(^uint64(0)))
        upper := new(big.Int).Rsh(b, 64)
        result.lower = lower.Uint64()
        result.upper = upper.Int64()
    } else {
        // Negative: compute two's complement for 128 bits
        // Add 2^128 to get the positive representation
        twoTo128 := new(big.Int).Lsh(big.NewInt(1), 128)
        pos := new(big.Int).Add(b, twoTo128)
        lower := new(big.Int).And(pos, new(big.Int).SetUint64(^uint64(0)))
        upper := new(big.Int).Rsh(pos, 64)
        result.lower = lower.Uint64()
        result.upper = int64(upper.Uint64())
    }
    return result, nil
}
```

**Why:** HUGEINT is 128-bit; Go's int64 is insufficient.

## Risks / Trade-offs

- **Risk:** JSON parsing edge cases with malformed CLI output
  - Mitigation: Comprehensive error messages, unit tests for each format

- **Trade-off:** big.Int allocations for HUGEINT/DECIMAL
  - Mitigation: Acceptable for correctness; can pool if profiling shows issues

- **Risk:** Nested type parsing without schema information
  - Mitigation: Use []any and map[string]any for untyped parsing; Composite[T] for typed

## Type Mapping Reference

| DuckDB Type | Go Type | JSON Format | Scan() | Value() |
|-------------|---------|-------------|--------|---------|
| BOOLEAN | bool | true/false | ✓ | ✓ |
| TINYINT | int8 | number | ✓ | ✓ |
| SMALLINT | int16 | number | ✓ | ✓ |
| INTEGER | int32 | number | ✓ | ✓ |
| BIGINT | int64 | number | ✓ | ✓ |
| UTINYINT | uint8 | number | ✓ | ✓ |
| USMALLINT | uint16 | number | ✓ | ✓ |
| UINTEGER | uint32 | number | ✓ | ✓ |
| UBIGINT | uint64 | number | ✓ | ✓ |
| FLOAT | float32 | number/"Infinity"/"NaN" | ✓ | ✓ |
| DOUBLE | float64 | number/"Infinity"/"NaN" | ✓ | ✓ |
| VARCHAR | string | string | ✓ | ✓ |
| BLOB | []byte | "\x..." hex | ✓ | ✓ |
| DATE | time.Time | "YYYY-MM-DD" | ✓ | ✓ |
| TIME | time.Time | "HH:MM:SS.µs" | ✓ | ✓ |
| TIMESTAMP | time.Time | "YYYY-MM-DD HH:MM:SS.µs" | ✓ | ✓ |
| TIMESTAMP_S | time.Time | truncated to seconds | ✓ | ✓ |
| TIMESTAMP_MS | time.Time | truncated to milliseconds | ✓ | ✓ |
| TIMESTAMP_NS | time.Time | nanosecond precision | ✓ | ✓ |
| TIMESTAMP_TZ | time.Time | with +HH offset | ✓ | ✓ |
| INTERVAL | Interval | {"months","days","micros"} | ✓ | ✓ |
| HUGEINT | *big.Int | string | ✓ | ✓ |
| UHUGEINT | *big.Int | string | ✓ | ✓ |
| UUID | UUID | hyphenated string | ✓ | ✓ |
| DECIMAL | Decimal | string | ✓ | ✓ |
| LIST | []any | array | ✓ | ✓ |
| STRUCT | map[string]any | object | ✓ | ✓ |
| MAP | Map | [{key,value}...] | ✓ | ✓ |
| UNION | Union | {tag,value} | ✓ | ✓ |
| ENUM | string | string | ✓ | ✓ |
| ARRAY | []any | array | ✓ | ✓ |
