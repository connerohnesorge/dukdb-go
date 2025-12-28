## Context

DuckDB has a rich type system including primitive types, temporal types, and nested/composite types. The duckdb-go CGO driver (in `duckdb-go/` reference folder) exports specific Go types. This change creates NEW implementations of these types in the root package for the pure Go `dukdb-go` driver.

**Important:** We are NOT modifying `duckdb-go/` - it is reference material only. All new code goes in the root package.

## Goals / Non-Goals

**Goals:**
- Exact API parity with duckdb-go exported types (signature compatibility)
- Correct JSON unmarshaling for native engine type conversion
- Scanner/Valuer support matching duckdb-go exactly (only types that have it in duckdb-go)
- Support all 37 DuckDB type constants exported by duckdb-go

**Non-Goals:**
- Vector/columnar operations (handled in result-handling and execution engine)
- Adding Scanner/Valuer to types that don't have them in duckdb-go
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

// Union represents a tagged union value (note: Value before Tag, matching duckdb-go)
type Union struct {
    Value driver.Value `json:"value"`
    Tag   string       `json:"tag"`
}

// Composite wraps a struct/list type for scanning
type Composite[T any] struct {
    t T
}
```

**Why:** Drop-in replacement requires identical type signatures.

### Decision 2: Type Enumeration (37 constants matching duckdb-go)

**What:** Define Type enumeration in `type.go` matching duckdb-go's exported constants:
```go
// Type is the underlying type enumeration (using uint8 values matching DuckDB)
type Type uint8

const (
    TYPE_INVALID      Type = 0
    TYPE_BOOLEAN      Type = 1
    TYPE_TINYINT      Type = 2
    TYPE_SMALLINT     Type = 3
    TYPE_INTEGER      Type = 4
    TYPE_BIGINT       Type = 5
    TYPE_UTINYINT     Type = 6
    TYPE_USMALLINT    Type = 7
    TYPE_UINTEGER     Type = 8
    TYPE_UBIGINT      Type = 9
    TYPE_FLOAT        Type = 10
    TYPE_DOUBLE       Type = 11
    TYPE_TIMESTAMP    Type = 12
    TYPE_DATE         Type = 13
    TYPE_TIME         Type = 14
    TYPE_INTERVAL     Type = 15
    TYPE_HUGEINT      Type = 16
    TYPE_VARCHAR      Type = 17
    TYPE_BLOB         Type = 18
    TYPE_DECIMAL      Type = 19
    TYPE_TIMESTAMP_S  Type = 20
    TYPE_TIMESTAMP_MS Type = 21
    TYPE_TIMESTAMP_NS Type = 22
    TYPE_ENUM         Type = 23
    TYPE_LIST         Type = 24
    TYPE_STRUCT       Type = 25
    TYPE_MAP          Type = 26
    TYPE_UUID         Type = 27
    TYPE_UNION        Type = 28
    TYPE_BIT          Type = 29
    TYPE_TIME_TZ      Type = 30
    TYPE_TIMESTAMP_TZ Type = 31
    TYPE_UHUGEINT     Type = 32
    TYPE_ARRAY        Type = 33
    TYPE_ANY          Type = 34
    TYPE_BIGNUM       Type = 35  // Note: duckdb-go uses BIGNUM, not VARINT
    TYPE_SQLNULL      Type = 36
)

// typeToStringMap maps Type to uppercase string names (matching duckdb-go exactly)
var typeToStringMap = map[Type]string{
    TYPE_INVALID:      "INVALID",
    TYPE_BOOLEAN:      "BOOLEAN",
    TYPE_TINYINT:      "TINYINT",
    TYPE_SMALLINT:     "SMALLINT",
    TYPE_INTEGER:      "INTEGER",
    TYPE_BIGINT:       "BIGINT",
    TYPE_UTINYINT:     "UTINYINT",
    TYPE_USMALLINT:    "USMALLINT",
    TYPE_UINTEGER:     "UINTEGER",
    TYPE_UBIGINT:      "UBIGINT",
    TYPE_FLOAT:        "FLOAT",
    TYPE_DOUBLE:       "DOUBLE",
    TYPE_TIMESTAMP:    "TIMESTAMP",
    TYPE_DATE:         "DATE",
    TYPE_TIME:         "TIME",
    TYPE_INTERVAL:     "INTERVAL",
    TYPE_HUGEINT:      "HUGEINT",
    TYPE_UHUGEINT:     "UHUGEINT",
    TYPE_VARCHAR:      "VARCHAR",
    TYPE_BLOB:         "BLOB",
    TYPE_DECIMAL:      "DECIMAL",
    TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
    TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
    TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
    TYPE_ENUM:         "ENUM",
    TYPE_LIST:         "LIST",
    TYPE_STRUCT:       "STRUCT",
    TYPE_MAP:          "MAP",
    TYPE_ARRAY:        "ARRAY",
    TYPE_UUID:         "UUID",
    TYPE_UNION:        "UNION",
    TYPE_BIT:          "BIT",
    TYPE_TIME_TZ:      "TIMETZ",        // Note: no underscore
    TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",   // Note: no underscore
    TYPE_ANY:          "ANY",
    TYPE_BIGNUM:       "BIGNUM",
    TYPE_SQLNULL:      "SQLNULL",
}

// unsupportedTypeToStringMap lists types not yet fully supported
var unsupportedTypeToStringMap = map[Type]string{
    TYPE_INVALID:  "INVALID",
    TYPE_UHUGEINT: "UHUGEINT",
    TYPE_BIT:      "BIT",
    TYPE_ANY:      "ANY",
    TYPE_BIGNUM:   "BIGNUM",
}
```

**Why:** Required for type identification in result handling and parameter binding. Matches duckdb-go exactly.

**Note:** Type does not have a String() method directly - string lookup is done via typeToStringMap.

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

### Decision 4: Special Float Values (dukdb-go specific)

**What:** Handle IEEE 754 special values explicitly in `type_json.go`:
```go
// parseFloat handles DuckDB CLI's JSON representation of floats.
// DuckDB outputs Infinity, -Infinity, and NaN as JSON strings, not numbers.
// This is a dukdb-go addition not present in duckdb-go (CGO wrapper doesn't need it).
func parseFloat(data []byte) (float64, error) {
    // First try to parse as string for special values
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
    // Otherwise parse as regular JSON number
    var f float64
    if err := json.Unmarshal(data, &f); err != nil {
        return 0, err
    }
    return f, nil
}

// floatToJSON converts float64 to JSON representation, handling special values.
func floatToJSON(f float64) any {
    if math.IsInf(f, 1) {
        return "Infinity"
    }
    if math.IsInf(f, -1) {
        return "-Infinity"
    }
    if math.IsNaN(f) {
        return "NaN"
    }
    return f
}
```

**Why:** DuckDB CLI outputs these as strings, not JSON numbers. The duckdb-go CGO wrapper doesn't need this because it receives native C values. Our pure Go implementation parses JSON output and must handle these special representations.

**Note:** This is intentionally different from duckdb-go - it's required for our JSON-based architecture.

### Decision 5: Scanner/Valuer Implementations (matching duckdb-go exactly)

**What:** Only types that have Scanner/Valuer in duckdb-go get these methods:

```go
// UUID implements sql.Scanner
// Uses github.com/google/uuid for parsing
func (u *UUID) Scan(v any) error {
    switch val := v.(type) {
    case []byte:
        if len(val) != 16 {
            return u.Scan(string(val))
        }
        copy(u[:], val)
    case string:
        id, err := uuid.Parse(val)  // github.com/google/uuid
        if err != nil {
            return err
        }
        copy(u[:], id[:])
    default:
        return fmt.Errorf("invalid UUID value type: %T", val)
    }
    return nil
}

// UUID implements fmt.Stringer
func (u *UUID) String() string {
    buf := make([]byte, 36)
    hex.Encode(buf, u[:4])
    buf[8] = '-'
    hex.Encode(buf[9:13], u[4:6])
    buf[13] = '-'
    hex.Encode(buf[14:18], u[6:8])
    buf[18] = '-'
    hex.Encode(buf[19:23], u[8:10])
    buf[23] = '-'
    hex.Encode(buf[24:], u[10:])
    return string(buf)
}

// UUID implements driver.Valuer (note: POINTER receiver, matching duckdb-go)
func (u *UUID) Value() (driver.Value, error) {
    return u.String(), nil
}

// Map implements sql.Scanner ONLY (no Value() method in duckdb-go)
func (m *Map) Scan(v any) error {
    data, ok := v.(Map)
    if !ok {
        return fmt.Errorf("invalid type `%T` for scanning `Map`, expected `Map`", v)
    }
    *m = data
    return nil
}

// Interval has NO Scan/Value methods in duckdb-go - only inferInterval for binding
func inferInterval(val any) (Interval, error) {
    switch v := val.(type) {
    case Interval:
        return v, nil
    default:
        return Interval{}, fmt.Errorf("cannot cast %T to Interval", val)
    }
}

// Decimal has NO Scan/Value methods in duckdb-go - only Float64() and String()
func (d Decimal) Float64() float64 {
    scale := big.NewInt(int64(d.Scale))
    factor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), scale, nil))
    value := new(big.Float).SetInt(d.Value)
    value.Quo(value, factor)
    f, _ := value.Float64()
    return f
}

func (d Decimal) String() string {
    if d.Value.Sign() == 0 {
        return "0"
    }
    var signStr string
    scaleless := d.Value.String()
    if d.Value.Sign() < 0 {
        signStr = "-"
        scaleless = scaleless[1:]
    }
    zeroTrimmed := strings.TrimRightFunc(scaleless, func(r rune) bool { return r == '0' })
    scale := int(d.Scale) - (len(scaleless) - len(zeroTrimmed))
    if scale <= 0 {
        return signStr + zeroTrimmed + strings.Repeat("0", -1*scale)
    }
    if len(zeroTrimmed) <= scale {
        return fmt.Sprintf("%s0.%s%s", signStr, strings.Repeat("0", scale-len(zeroTrimmed)), zeroTrimmed)
    }
    return signStr + zeroTrimmed[:len(zeroTrimmed)-scale] + "." + zeroTrimmed[len(zeroTrimmed)-scale:]
}

// Union has NO methods in duckdb-go - it's a bare struct

// Composite[T] has ONLY Get() and Scan() - NO Set(), NO Value()
func (s Composite[T]) Get() T {
    return s.t
}

// Uses github.com/mitchellh/mapstructure for map-to-struct conversion
func (s *Composite[T]) Scan(v any) error {
    return mapstructure.Decode(v, &s.t)  // github.com/mitchellh/mapstructure
}
```

**Why:** Drop-in replacement requires matching duckdb-go exactly - not all types have Scanner/Valuer.

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

### Decision 7: External Dependencies for Types

**Required libraries:**
```go
import (
    "github.com/google/uuid"              // UUID.Scan() parsing
    "github.com/mitchellh/mapstructure"   // Composite[T].Scan() conversion
)
```

**Why these choices:**
- `google/uuid`: Google's official UUID library, widely used, well-maintained
- `mapstructure`: Well-tested library for map-to-struct conversion, matches duckdb-go approach

## Risks / Trade-offs

- **Risk:** JSON parsing edge cases with malformed CLI output
  - Mitigation: Comprehensive error messages, unit tests for each format

- **Trade-off:** big.Int allocations for HUGEINT/DECIMAL
  - Mitigation: Acceptable for correctness; can pool if profiling shows issues

- **Risk:** Nested type parsing without schema information
  - Mitigation: Use []any and map[string]any for untyped parsing; Composite[T] for typed

## Type Mapping Reference (matching duckdb-go)

| DuckDB Type | Go Type | Scan() | Value() | Notes |
|-------------|---------|--------|---------|-------|
| BOOLEAN | bool | native | native | Go primitive |
| TINYINT | int8 | native | native | Go primitive |
| SMALLINT | int16 | native | native | Go primitive |
| INTEGER | int32 | native | native | Go primitive |
| BIGINT | int64 | native | native | Go primitive |
| UTINYINT | uint8 | native | native | Go primitive |
| USMALLINT | uint16 | native | native | Go primitive |
| UINTEGER | uint32 | native | native | Go primitive |
| UBIGINT | uint64 | native | native | Go primitive |
| FLOAT | float32 | native | native | Go primitive |
| DOUBLE | float64 | native | native | Go primitive |
| VARCHAR | string | native | native | Go primitive |
| BLOB | []byte | native | native | Go primitive |
| DATE | time.Time | native | native | Go stdlib |
| TIME | time.Time | native | native | Go stdlib |
| TIMESTAMP | time.Time | native | native | Go stdlib |
| TIMESTAMP_S | time.Time | native | native | Go stdlib |
| TIMESTAMP_MS | time.Time | native | native | Go stdlib |
| TIMESTAMP_NS | time.Time | native | native | Go stdlib |
| TIMESTAMP_TZ | time.Time | native | native | Go stdlib |
| INTERVAL | Interval | ✗ | ✗ | No Scanner/Valuer in duckdb-go |
| HUGEINT | *big.Int | native | native | Go stdlib |
| UHUGEINT | *big.Int | native | native | Go stdlib (unsupported in duckdb-go) |
| UUID | UUID | ✓ | ✓ | Custom type with Scanner/Valuer |
| DECIMAL | Decimal | ✗ | ✗ | Only Float64(), String() methods |
| LIST | []any | native | native | Via Composite[T] |
| STRUCT | map[string]any | native | native | Via Composite[T] |
| MAP | Map | ✓ | ✗ | Only Scan(), no Value() |
| UNION | Union | ✗ | ✗ | Bare struct, no methods |
| ENUM | string | native | native | String representation |
| ARRAY | []any | native | native | Via Composite[T] |

**Legend:**
- `native` = Go's database/sql handles these natively
- `✓` = Custom method implemented
- `✗` = Method not present in duckdb-go (intentionally omitted)
