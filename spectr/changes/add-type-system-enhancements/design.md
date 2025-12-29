# Design: Type System Enhancements

## Context

The dukdb-go project has MORE type system features than duckdb-go:
- `SQLType()` method on TypeInfo (not in duckdb-go)
- Extended types: Uhugeint, Bit, TimeNS wrappers (not in duckdb-go)

However, there are gaps in **scanning** and **binding** complex types to typed Go values. Currently:
- LIST scanning returns `[]any` - requires manual type assertions
- STRUCT scanning returns `map[string]any` - no automatic field mapping
- MAP scanning limited - no typed key/value support
- UNION scanning incomplete - no type-safe member access
- ENUM scanning to custom Go types missing
- JSON scanning to structs missing

**Stakeholders**:
- Application developers using complex DuckDB types
- Users migrating from duckdb-go expecting similar scanning patterns
- Developers building type-safe data pipelines

**Constraints**:
- Must maintain backward compatibility with existing `[]any` scanning
- Must work with Go generics (Go 1.18+)
- Must not require code generation
- Must handle NULL values correctly
- Must use quartz.Clock for any time-related operations

## Goals / Non-Goals

**Goals**:
1. Provide typed generic scanners for all complex types
2. Enable struct tag-based field mapping for STRUCT types
3. Support custom Go enum types for ENUM columns
4. Allow JSON columns to unmarshal directly to Go structs
5. Provide parameter binding wrappers for all complex types
6. Maintain deterministic testing support via clock injection

**Non-Goals**:
1. Replace existing scanning logic (additive only)
2. Change TypeInfo interface (already complete)
3. Support code generation tools
4. Provide ORM-like features (joins, relationships)
5. Change wire format or storage representation

## Decisions

### Decision 1: Generic Scanner Architecture

**Options**:
A. Concrete types per DuckDB type (ListIntScanner, ListStringScanner, etc.)
B. Generic types with type parameter (ListScanner[T])
C. Reflection-based single Scanner type
D. Interface-based with manual type registration

**Choice**: B - Generic types with type parameter

**Rationale**:
- Type safety at compile time
- No runtime registration required
- Clean API: `ScanList(&mySlice)` not `ScanList(&mySlice, "int64")`
- Go generics (1.18+) widely available
- Consistent with modern Go patterns

```go
// Clean generic API
var ints []int64
rows.Scan(dukdb.ScanList(&ints))

// vs verbose concrete types
var ints []int64
rows.Scan(dukdb.ScanListInt64(&ints))

// vs reflection-based
var ints []int64
rows.Scan(dukdb.ScanList(&ints, reflect.TypeOf(int64(0))))
```

### Decision 2: Type Conversion Strategy

**Options**:
A. Strict exact type matching only
B. Implicit numeric conversion (int8 → int64, etc.)
C. Full Go reflect conversion rules
D. Custom conversion with hooks

**Choice**: B - Implicit numeric conversion with reflect fallback

**Rationale**:
- DuckDB may return int32 for INTEGER, but user wants int64
- Numeric widening is safe and expected
- Strict matching causes friction in common cases
- Reflect fallback handles edge cases

```go
func convertToType[T any](src any) (T, error) {
    var zero T
    // Fast path: exact match
    if v, ok := src.(T); ok {
        return v, nil
    }

    // Numeric conversion
    targetType := reflect.TypeOf(zero)
    srcValue := reflect.ValueOf(src)
    if isNumeric(targetType.Kind()) && srcValue.CanConvert(targetType) {
        return srcValue.Convert(targetType).Interface().(T), nil
    }

    return zero, fmt.Errorf("cannot convert %T to %T", src, zero)
}
```

### Decision 3: Struct Field Mapping

**Options**:
A. Field name matching only (case-insensitive)
B. Struct tags only (`duckdb:"field_name"`)
C. Struct tags with name fallback
D. JSON-style tags with full options

**Choice**: C - Struct tags with name fallback

**Rationale**:
- Minimal friction for simple cases (just match field names)
- Full control via tags when needed
- Case-insensitive matching handles DuckDB's lowercase convention
- No need for JSON-style options (omitempty, etc.) - this is scanning not serializing

```go
type Person struct {
    Name string            // Matches "name" column
    Age  int               // Matches "age" column
    ID   int `duckdb:"id"` // Explicit override
}

// Field resolution order:
// 1. Check for `duckdb:"name"` tag
// 2. Lowercase field name
// 3. Exact field name (case-insensitive)
```

### Decision 4: NULL Handling

**Options**:
A. Zero values for NULL
B. Pointer types (*T)
C. sql.Null* wrappers
D. Custom null wrappers

**Choice**: A + B - Zero values by default, pointer types for explicit NULL

**Rationale**:
- Zero values are Go convention for "empty"
- Pointer types allow distinguishing NULL from zero
- sql.Null* wrappers add verbosity
- Users can choose their preferred style

```go
// Style 1: Zero values (simple)
var names []string
rows.Scan(ScanList(&names))
// NULL element → empty string ""

// Style 2: Pointers (nullable)
var names []*string
rows.Scan(ScanList(&names))
// NULL element → nil pointer

// Both supported by same scanner
```

### Decision 5: Union Type Access

**Options**:
A. Tagged union struct with Value any
B. Sum type with methods per variant
C. Interface type with type switch
D. Generic accessor functions

**Choice**: A - Tagged union struct with typed accessor

**Rationale**:
- DuckDB unions have runtime tag (can't use compile-time types)
- Tag + Value struct is simple and introspectable
- As() method provides type-safe access
- Matches how DuckDB represents unions internally

```go
type UnionValue struct {
    Tag   string // Active member name
    Index int    // Active member index (0-based)
    Value any    // The actual value
}

// Usage
var u UnionValue
rows.Scan(ScanUnion(&u))

switch u.Tag {
case "int":
    var i int64
    u.As(&i) // Type-safe extraction
case "str":
    var s string
    u.As(&s)
}
```

### Decision 6: JSON Scanning

**Options**:
A. Return raw string/bytes only
B. Unmarshal to any (map/slice)
C. Generic unmarshal to user type
D. Custom JSON type with helpers

**Choice**: C - Generic unmarshal to user type

**Rationale**:
- Most users want structured data, not raw JSON
- Generic scanner provides type safety
- encoding/json is standard and well-understood
- Can still get raw string if needed (just use sql.NullString)

```go
type Config struct {
    Enabled bool `json:"enabled"`
    Timeout int  `json:"timeout"`
}

var c Config
rows.Scan(ScanJSON(&c))
// Internally: json.Unmarshal([]byte(src), &c)
```

### Decision 7: Parameter Binding Wrappers

**Options**:
A. No wrappers, use raw slices/maps
B. Wrapper types with Value() method
C. Generic bind functions
D. Builder pattern

**Choice**: B - Wrapper types with Value() method

**Rationale**:
- Implements driver.Valuer interface
- Clear intent: "this is a LIST parameter"
- Handles conversion to internal format
- Matches Go database/sql conventions

```go
// Wrapper types
type ListValue[T any] []T
type StructValue[T any] struct{ V T }
type MapValue[K comparable, V any] map[K]V

// Implement driver.Valuer
func (v ListValue[T]) Value() (driver.Value, error) {
    return toAnySlice(v), nil
}

// Usage
db.Exec("INSERT INTO t VALUES ($1)", ListValue[int](myInts))
```

### Decision 8: Error Messages

**Options**:
A. Simple type mismatch messages
B. Detailed messages with values
C. Structured errors with codes
D. Wrapped errors with context

**Choice**: D - Wrapped errors with context

**Rationale**:
- Helps debugging: "list element 3: cannot convert string to int64"
- errors.Is/As support for programmatic handling
- Consistent with Go error handling patterns
- No new error types needed

```go
// Error format for nested structures
return fmt.Errorf("list element %d: %w", i, err)
return fmt.Errorf("struct field %s: %w", name, err)
return fmt.Errorf("map key: %w", err)
return fmt.Errorf("map value for key %v: %w", key, err)
```

### Decision 9: Clock Integration for Timestamp Types

**Options**:
A. No clock integration (timestamps are data, not now())
B. Clock for Now() helpers only
C. Full clock integration in scanning
D. Clock optional via separate API

**Choice**: B - Clock for Now() helpers only

**Rationale**:
- Timestamps from database are data - no clock needed to scan them
- Only `NowNS()` type function needs clock for current time
- Keeps scanning simple and stateless
- Matches quartz.Clock intended use (timing, not data)

```go
// TimeNS with clock for Now() only
type TimeNS struct {
    ns int64
}

// Factory uses clock
func NowNS(clock quartz.Clock) TimeNS {
    if clock == nil {
        clock = quartz.NewReal()
    }
    t := clock.Now()
    midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
    return TimeNS{ns: t.Sub(midnight).Nanoseconds()}
}

// Scanning does NOT use clock - just data conversion
func (s *TimeNSScanner) Scan(src any) error {
    // Just convert, no clock needed
}
```

### Decision 10: Package Organization

**Options**:
A. All in root package (scan_types.go, bind_types.go, convert.go)
B. Separate types package (dukdb/types)
C. Separate scan and bind packages
D. Single types.go with everything

**Choice**: A - All in root package with logical file separation

**Rationale**:
- Single import: `import "dukdb"` gets everything
- Logical file separation keeps code organized
- No circular dependency issues
- Consistent with existing dukdb structure

```
dukdb/
├── scan_types.go    # ListScanner, StructScanner, etc.
├── bind_types.go    # ListValue, StructValue, etc.
├── convert.go       # convertToType, setFieldValue
├── type_info.go     # TypeInfo (existing)
├── type_extended.go # Uhugeint, Bit, TimeNS (existing)
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Generic scanner complexity | Medium | Clear documentation with examples |
| Type conversion edge cases | Medium | Comprehensive test matrix |
| Performance overhead | Low | Fast path for exact type match |
| Reflection slowness | Medium | Cache reflect.Type for repeated scans |
| Breaking NULL semantics | High | Document zero-value behavior clearly |

## Performance Considerations

1. **Fast path for exact types**: Check `src.(T)` before reflection
2. **Cached type info**: Don't call reflect.TypeOf repeatedly
3. **Pre-allocated slices**: Grow slices to known size before filling
4. **Minimal allocations**: Reuse buffers where possible

```go
// Fast path example
func (s *ListScanner[T]) Scan(src any) error {
    switch v := src.(type) {
    case []T:
        // Direct assignment - no conversion needed
        *s.Result = v
        return nil
    case []any:
        // Conversion needed
        *s.Result = make([]T, len(v))
        for i, elem := range v {
            // Check fast path first
            if t, ok := elem.(T); ok {
                (*s.Result)[i] = t
                continue
            }
            // Fall back to conversion
            converted, err := convertToType[T](elem)
            if err != nil {
                return fmt.Errorf("list element %d: %w", i, err)
            }
            (*s.Result)[i] = converted
        }
        return nil
    }
    return fmt.Errorf("cannot scan %T into ListScanner[%T]", src, *new(T))
}
```

## Migration Plan

### Phase 1: Core Infrastructure
1. Create `convert.go` with type conversion utilities
2. Add comprehensive tests for numeric widening

### Phase 2: Scanners
1. Implement ListScanner[T]
2. Implement StructScanner[T]
3. Implement MapScanner[K, V]
4. Implement UnionScanner
5. Implement EnumScanner[T]
6. Implement JSONScanner[T]

### Phase 3: Parameter Binding
1. Implement ListValue[T]
2. Implement StructValue[T]
3. Implement MapValue[K, V]

### Phase 4: Extended Types
1. Add clock integration to TimeNS.NowNS()
2. Add CurrentTimeNS(clock) utility
3. Ensure all extended types scan correctly

### Phase 5: Testing & Documentation
1. Full type matrix tests (all 37 types × all scanners)
2. NULL handling tests
3. Error message verification
4. Documentation with examples

### Decision 11: ARRAY Type Handling

**Options**:
A. Use ListScanner for both LIST and ARRAY
B. Separate ArrayScanner with compile-time size
C. Separate ArrayScanner with runtime size validation
D. No special handling for ARRAY

**Choice**: C - Separate ArrayScanner with runtime size validation

**Rationale**:
- ARRAY has fixed size unlike LIST (variable)
- Go generics don't support integer type parameters (no `[T, N]`)
- Runtime validation ensures size matches schema
- Allows detecting schema/data mismatch early

```go
// ScanArray creates scanner with size validation
func ScanArray[T any](dest *[]T, expectedSize int) sql.Scanner {
    return &ArrayScanner[T]{Result: dest, Expected: expectedSize}
}

// Error on size mismatch
if len(v) != s.Expected && s.Expected >= 0 {
    return fmt.Errorf("array size mismatch: expected %d, got %d", s.Expected, len(v))
}
```

### Decision 12: MAP NULL Key Handling

**Options**:
A. Allow NULL keys (convert to zero value)
B. Reject NULL keys with error
C. Skip NULL keys silently
D. Use pointer key type for nullable keys

**Choice**: B - Reject NULL keys with error

**Rationale**:
- Go maps require comparable keys (no nil)
- NULL as map key is semantically questionable
- Error provides clear feedback vs silent corruption
- Aligns with DuckDB MAP key constraints

```go
if key == nil {
    return fmt.Errorf("map key cannot be NULL")
}
```

### Decision 13: Pointer Field Support

**Options**:
A. No pointer support (always zero values)
B. Pointer fields become nil on NULL
C. Optional pointer wrapper type
D. sql.Null* wrapper only

**Choice**: B - Pointer fields become nil on NULL

**Rationale**:
- Go idiom: pointer = optional/nullable
- Zero value for `*string` is nil (correct NULL representation)
- No wrapper types needed
- Matches database/sql patterns

```go
// struct field: Name *string
// NULL value → field set to nil

// struct field: Name string
// NULL value → field set to "" (zero value)
```

### Decision 14: Type Conversion Matrix

The complete type conversion rules:

| Source Type | Target Type | Conversion |
|-------------|-------------|------------|
| intN | intM (M≥N) | Widening via reflect.Convert |
| uintN | uintM (M≥N) | Widening via reflect.Convert |
| float32 | float64 | Widening via reflect.Convert |
| int64 | time.Time | Unix timestamp conversion |
| string | time.Time | time.Parse with RFC3339 |
| []byte | [16]byte | Copy with length check (UUID) |
| string | [16]byte | UUID string parsing |
| map[string]any | struct | Field mapping via reflection |
| string | json struct | json.Unmarshal |
| any | *T | Allocate pointer, convert element |
| nil | any | Zero value or nil pointer |

**Unsupported conversions return error:**
- string → numeric (use strconv explicitly)
- numeric → string (use fmt explicitly)
- struct → struct (use explicit conversion)

### Decision 15: Embedded Struct Fields

**Options**:
A. Skip embedded fields
B. Flatten embedded fields into parent
C. Require explicit tags
D. Follow Go reflection semantics

**Choice**: D - Follow Go reflection semantics

**Rationale**:
- reflect.Type.NumField() includes embedded fields
- reflect.Type.FieldByName() handles promotion
- Consistent with how Go handles embedding
- No special handling needed - works automatically

```go
type Address struct {
    City string
}

type Person struct {
    Address // embedded
    Name string
}

// Both "city" and "name" are accessible in map lookup
```

### Decision 16: JSON Error Handling

**Options**:
A. Return json.Unmarshal errors directly
B. Wrap with context
C. Ignore parse errors
D. Custom error type

**Choice**: B - Wrap with context

**Rationale**:
- Raw json errors are often cryptic
- Wrapping provides scanner context
- errors.Unwrap() still accesses original error
- Consistent with other scanner error patterns

```go
func (s *JSONScanner[T]) Scan(src any) error {
    // ...
    if err := json.Unmarshal([]byte(v), s.Result); err != nil {
        return fmt.Errorf("json unmarshal: %w", err)
    }
    return nil
}
```

### Decision 17: Parameter Binding Validation

**Options**:
A. No validation in Value()
B. Full type validation in Value()
C. Defer to driver for validation
D. Optional validation

**Choice**: C - Defer to driver for validation

**Rationale**:
- Value() is called at bind time, types not yet known
- Driver knows target column type
- Validation in Value() duplicates driver work
- Keeps binding simple and fast

```go
func (v ListValue[T]) Value() (driver.Value, error) {
    // No type validation - just convert to []any
    // Driver validates against actual column type
    return toAnySlice(v), nil
}
```

### Decision 18: Nested Type Conversion

Nested structures (LIST of STRUCT, STRUCT with LIST field) use recursive conversion:

```go
// Error messages include full path
"list element 0: struct field name: cannot convert string to int64"
"list element 0: list element 3: cannot convert float64 to int32"
"map value for key 'users': list element 2: struct field age: ..."
```

Each level adds context to the error chain.

## Open Questions (Resolved)

1. **Should we support nested generics?** (e.g., `ListScanner[[]int]`)
   - Answer: Yes, generics naturally support nesting

2. **How to handle deeply nested types?**
   - Answer: Recursive conversion with nested error messages

3. **Should EnumScanner validate against known values?**
   - Answer: No, validation is application concern, scanner just converts

4. **Should StructScanner support embedded fields?**
   - Answer: Yes, reflect handles embedded fields automatically (Decision 15)

5. **How should JSON parse errors be reported?**
   - Answer: Wrapped with context (Decision 16)

6. **Should NULL map keys be allowed?**
   - Answer: No, rejected with error (Decision 12)

7. **How to distinguish NULL from zero value?**
   - Answer: Use pointer field types (Decision 13)
