# Change: Add Type System Enhancements

## Why

The dukdb-go type system currently has MORE features than duckdb-go (SQLType() method, extended types), but there are gaps in scan/value support for complex types. This proposal completes the type system to ensure all 37 DuckDB types can be:

1. **Created** via NewTypeInfo* factory functions
2. **Scanned** from query results into Go types
3. **Bound** as query parameters
4. **Converted** to SQL type strings

**Current State** (from exploration):
- `type_info.go`: All 37 types defined, TypeInfo interface complete
- `type_extended.go`: Uhugeint, Bit, TimeNS wrappers added
- `SQLType()` method exists (not in duckdb-go)
- Complex type scanning partially implemented

**Gaps Identified**:
1. LIST/ARRAY scanning returns `[]any` - no typed slices
2. STRUCT scanning returns `map[string]any` - no struct unmarshaling
3. MAP scanning limited - no generic map support
4. UNION scanning incomplete - no type-safe access
5. ENUM scanning to custom Go types missing
6. JSON scanning to struct missing

## What Changes

### 1. Typed List/Array Scanning (scan_types.go - NEW)

```go
// ListScanner enables scanning LIST columns to typed Go slices
type ListScanner[T any] struct {
    Result *[]T
}

func (s *ListScanner[T]) Scan(src any) error {
    if src == nil {
        *s.Result = nil
        return nil
    }

    switch v := src.(type) {
    case []any:
        *s.Result = make([]T, len(v))
        for i, elem := range v {
            // Type conversion with validation
            converted, err := convertToType[T](elem)
            if err != nil {
                return fmt.Errorf("list element %d: %w", i, err)
            }
            (*s.Result)[i] = converted
        }
        return nil
    default:
        return fmt.Errorf("cannot scan %T into ListScanner[%T]", src, *new(T))
    }
}

// Usage:
var ints []int64
rows.Scan(dukdb.ScanList(&ints))

// Helper function
func ScanList[T any](dest *[]T) sql.Scanner {
    return &ListScanner[T]{Result: dest}
}
```

### 2. Struct Scanning (scan_types.go)

```go
// StructScanner enables scanning STRUCT columns to Go structs
type StructScanner[T any] struct {
    Result *T
}

func (s *StructScanner[T]) Scan(src any) error {
    if src == nil {
        return nil
    }

    switch v := src.(type) {
    case map[string]any:
        // Use reflection to map fields
        rv := reflect.ValueOf(s.Result).Elem()
        rt := rv.Type()

        for i := 0; i < rt.NumField(); i++ {
            field := rt.Field(i)
            name := field.Name
            // Check for struct tag: `duckdb:"field_name"`
            if tag := field.Tag.Get("duckdb"); tag != "" {
                name = tag
            }

            if val, ok := v[strings.ToLower(name)]; ok {
                if err := setFieldValue(rv.Field(i), val); err != nil {
                    return fmt.Errorf("field %s: %w", name, err)
                }
            }
        }
        return nil
    default:
        return fmt.Errorf("cannot scan %T into StructScanner", src)
    }
}

// Usage:
type Person struct {
    Name string `duckdb:"name"`
    Age  int    `duckdb:"age"`
}
var p Person
rows.Scan(dukdb.ScanStruct(&p))
```

### 3. Map Scanning (scan_types.go)

```go
// MapScanner enables scanning MAP columns to Go maps
type MapScanner[K comparable, V any] struct {
    Result *map[K]V
}

func (s *MapScanner[K, V]) Scan(src any) error {
    if src == nil {
        *s.Result = nil
        return nil
    }

    switch v := src.(type) {
    case map[any]any:
        *s.Result = make(map[K]V, len(v))
        for key, val := range v {
            k, err := convertToType[K](key)
            if err != nil {
                return fmt.Errorf("map key: %w", err)
            }
            v, err := convertToType[V](val)
            if err != nil {
                return fmt.Errorf("map value: %w", err)
            }
            (*s.Result)[k] = v
        }
        return nil
    default:
        return fmt.Errorf("cannot scan %T into MapScanner", src)
    }
}

// Usage:
var m map[string]int
rows.Scan(dukdb.ScanMap(&m))
```

### 4. Union Scanning (scan_types.go)

```go
// UnionValue represents a scanned UNION value with type tag
type UnionValue struct {
    Tag   string  // Name of active member
    Index int     // Index of active member (0-based)
    Value any     // The actual value
}

// UnionScanner enables scanning UNION columns
type UnionScanner struct {
    Result *UnionValue
}

func (s *UnionScanner) Scan(src any) error {
    if src == nil {
        *s.Result = UnionValue{Tag: "", Index: -1, Value: nil}
        return nil
    }

    switch v := src.(type) {
    case UnionValue:
        *s.Result = v
        return nil
    case map[string]any:
        // Extract tag and value from struct representation
        if tag, ok := v["tag"].(string); ok {
            if idx, ok := v["index"].(int); ok {
                *s.Result = UnionValue{
                    Tag:   tag,
                    Index: idx,
                    Value: v["value"],
                }
                return nil
            }
        }
        return fmt.Errorf("invalid union structure")
    default:
        return fmt.Errorf("cannot scan %T into UnionScanner", src)
    }
}

// Type-safe accessor
func (u UnionValue) As(dest any) error {
    return setFieldValue(reflect.ValueOf(dest).Elem(), u.Value)
}
```

### 5. Enum Scanning (scan_types.go)

```go
// EnumScanner enables scanning ENUM columns to custom Go types
type EnumScanner[T ~string] struct {
    Result *T
}

func (s *EnumScanner[T]) Scan(src any) error {
    if src == nil {
        var zero T
        *s.Result = zero
        return nil
    }

    switch v := src.(type) {
    case string:
        *s.Result = T(v)
        return nil
    default:
        return fmt.Errorf("cannot scan %T into EnumScanner", src)
    }
}

// Usage:
type Status string
const (
    StatusActive   Status = "active"
    StatusInactive Status = "inactive"
)
var s Status
rows.Scan(dukdb.ScanEnum(&s))
```

### 6. JSON Scanning (scan_types.go)

```go
// JSONScanner enables scanning JSON columns to Go structs
type JSONScanner[T any] struct {
    Result *T
}

func (s *JSONScanner[T]) Scan(src any) error {
    if src == nil {
        return nil
    }

    switch v := src.(type) {
    case string:
        return json.Unmarshal([]byte(v), s.Result)
    case []byte:
        return json.Unmarshal(v, s.Result)
    default:
        return fmt.Errorf("cannot scan %T into JSONScanner", src)
    }
}

// Usage:
type Config struct {
    Enabled bool   `json:"enabled"`
    Timeout int    `json:"timeout"`
}
var c Config
rows.Scan(dukdb.ScanJSON(&c))
```

### 7. Parameter Binding for Complex Types (bind_types.go - NEW)

```go
// ListValue wraps a Go slice for binding as LIST parameter
type ListValue[T any] []T

func (v ListValue[T]) Value() (driver.Value, error) {
    return []any(toAnySlice(v)), nil
}

// StructValue wraps a Go struct for binding as STRUCT parameter
type StructValue[T any] struct {
    V T
}

func (v StructValue[T]) Value() (driver.Value, error) {
    rv := reflect.ValueOf(v.V)
    rt := rv.Type()

    result := make(map[string]any, rt.NumField())
    for i := 0; i < rt.NumField(); i++ {
        field := rt.Field(i)
        name := field.Name
        if tag := field.Tag.Get("duckdb"); tag != "" {
            name = tag
        }
        result[strings.ToLower(name)] = rv.Field(i).Interface()
    }
    return result, nil
}

// MapValue wraps a Go map for binding as MAP parameter
type MapValue[K comparable, V any] map[K]V

func (v MapValue[K, V]) Value() (driver.Value, error) {
    result := make(map[any]any, len(v))
    for k, val := range v {
        result[k] = val
    }
    return result, nil
}
```

### 8. Type Conversion Utilities (convert.go - NEW)

```go
// convertToType converts an any value to a specific type
func convertToType[T any](src any) (T, error) {
    var zero T
    if src == nil {
        return zero, nil
    }

    // Fast path for exact type match
    if v, ok := src.(T); ok {
        return v, nil
    }

    // Type-specific conversions
    targetType := reflect.TypeOf(zero)
    srcValue := reflect.ValueOf(src)

    // Numeric conversions
    if targetType.Kind() >= reflect.Int && targetType.Kind() <= reflect.Float64 {
        if srcValue.CanConvert(targetType) {
            return srcValue.Convert(targetType).Interface().(T), nil
        }
    }

    return zero, fmt.Errorf("cannot convert %T to %T", src, zero)
}

// setFieldValue sets a reflect.Value from an any value
func setFieldValue(field reflect.Value, val any) error {
    if val == nil {
        field.Set(reflect.Zero(field.Type()))
        return nil
    }

    valValue := reflect.ValueOf(val)
    if valValue.Type().AssignableTo(field.Type()) {
        field.Set(valValue)
        return nil
    }

    if valValue.Type().ConvertibleTo(field.Type()) {
        field.Set(valValue.Convert(field.Type()))
        return nil
    }

    return fmt.Errorf("cannot convert %T to %s", val, field.Type())
}
```

### 9. Clock Integration for Timestamp Types (type_extended.go - MODIFIED)

```go
// TimeNS represents time with nanosecond precision
// Note: No clock field stored - clock is only used for factory functions
type TimeNS struct {
    ns int64 // Nanoseconds since midnight
}

// NowNS returns current time as TimeNS using injected clock
func NowNS(clock quartz.Clock) TimeNS {
    if clock == nil {
        clock = quartz.NewReal()
    }
    t := clock.Now()
    midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
    return TimeNS{ns: t.Sub(midnight).Nanoseconds()}
}

// CurrentTimeNS returns deterministic timestamp using clock
func CurrentTimeNS(clock quartz.Clock) int64 {
    if clock == nil {
        clock = quartz.NewReal()
    }
    return clock.Now().UnixNano()
}
```

## Impact

- **Affected specs**: type-system (NEW)
- **Affected code**:
  - NEW: `scan_types.go` (~400 lines - typed scanners)
  - NEW: `bind_types.go` (~200 lines - parameter binding)
  - NEW: `convert.go` (~150 lines - type conversion)
  - MODIFIED: `type_extended.go` (~50 lines - clock integration)

- **Dependencies**:
  - `encoding/json` - JSON scanning
  - `reflect` - Struct field mapping
  - `quartz.Clock` - Deterministic timing

## Breaking Changes

None. All changes are additive. Existing type scanning continues to work.

## Scanner Type Coverage Matrix

The 37 DuckDB types are handled as follows:

**Primitive Types (22 types) - Use Standard sql.Scanner:**
- BOOLEAN → bool
- TINYINT/SMALLINT/INTEGER/BIGINT → int8/int16/int32/int64
- UTINYINT/USMALLINT/UINTEGER/UBIGINT → uint8/uint16/uint32/uint64
- HUGEINT → *big.Int or Hugeint
- UHUGEINT → *big.Int or Uhugeint
- FLOAT/DOUBLE → float32/float64
- DECIMAL → Decimal (precision/scale preserved)
- VARCHAR → string
- BLOB → []byte
- UUID → [16]byte or uuid.UUID
- BIT → Bit wrapper type
- DATE → time.Time (date only)
- TIME → time.Time (time only)
- TIME_TZ → time.Time (with timezone)
- TIMESTAMP → time.Time
- TIMESTAMP_S/MS/NS → time.Time (with precision)
- TIMESTAMP_TZ → time.Time (with timezone)
- INTERVAL → Interval struct

**Complex Types (6 types) - Use Typed Scanners:**
- LIST → ListScanner[T] or ArrayScanner[T, N] for typed slices
- STRUCT → StructScanner[T] for Go structs
- MAP → MapScanner[K, V] for typed maps
- ARRAY (fixed-size) → ArrayScanner[T, N] with size validation
- UNION → UnionScanner with UnionValue
- ENUM → EnumScanner[T ~string] for custom string types

**Special Types (2 types):**
- JSON → JSONScanner[T] for struct unmarshaling
- NULL → Handled by all scanners (returns zero/nil)

### 10. ArrayScanner for Fixed-Size Arrays (scan_types.go)

```go
// ArrayScanner enables scanning ARRAY columns (fixed-size) to typed Go arrays
type ArrayScanner[T any] struct {
    Result   *[]T
    Expected int // Expected fixed size (-1 for any size)
}

func (s *ArrayScanner[T]) Scan(src any) error {
    if src == nil {
        *s.Result = nil
        return nil
    }

    switch v := src.(type) {
    case []any:
        if s.Expected >= 0 && len(v) != s.Expected {
            return fmt.Errorf("array size mismatch: expected %d, got %d", s.Expected, len(v))
        }
        *s.Result = make([]T, len(v))
        for i, elem := range v {
            converted, err := convertToType[T](elem)
            if err != nil {
                return fmt.Errorf("array element %d: %w", i, err)
            }
            (*s.Result)[i] = converted
        }
        return nil
    default:
        return fmt.Errorf("cannot scan %T into ArrayScanner[%T]", src, *new(T))
    }
}

// ScanArray creates a scanner that validates fixed-size arrays
func ScanArray[T any](dest *[]T, size int) sql.Scanner {
    return &ArrayScanner[T]{Result: dest, Expected: size}
}
```

### 11. UUID Scanner (scan_types.go)

```go
// UUIDScanner enables scanning UUID columns to Go UUID types
type UUIDScanner struct {
    Result *[16]byte
}

func (s *UUIDScanner) Scan(src any) error {
    if src == nil {
        *s.Result = [16]byte{}
        return nil
    }

    switch v := src.(type) {
    case [16]byte:
        *s.Result = v
        return nil
    case []byte:
        if len(v) != 16 {
            return fmt.Errorf("UUID must be 16 bytes, got %d", len(v))
        }
        copy((*s.Result)[:], v)
        return nil
    case string:
        // Parse UUID string format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        parsed, err := parseUUID(v)
        if err != nil {
            return fmt.Errorf("invalid UUID string: %w", err)
        }
        *s.Result = parsed
        return nil
    default:
        return fmt.Errorf("cannot scan %T into UUIDScanner", src)
    }
}
```

### 12. Enhanced Type Conversion with Pointer Support (convert.go)

```go
// convertToType converts an any value to a specific type (enhanced)
func convertToType[T any](src any) (T, error) {
    var zero T
    if src == nil {
        return zero, nil
    }

    // Fast path for exact type match
    if v, ok := src.(T); ok {
        return v, nil
    }

    targetType := reflect.TypeOf(zero)
    srcValue := reflect.ValueOf(src)

    // Handle pointer types for nullable values
    if targetType.Kind() == reflect.Ptr {
        if src == nil {
            return zero, nil // nil pointer for NULL
        }
        // Create new pointer and convert element
        elemType := targetType.Elem()
        newPtr := reflect.New(elemType)
        if srcValue.Type().AssignableTo(elemType) {
            newPtr.Elem().Set(srcValue)
        } else if srcValue.Type().ConvertibleTo(elemType) {
            newPtr.Elem().Set(srcValue.Convert(elemType))
        } else {
            return zero, fmt.Errorf("cannot convert %T to *%s", src, elemType)
        }
        return newPtr.Interface().(T), nil
    }

    // Numeric conversions (int8-int64, uint8-uint64, float32, float64)
    if isNumericKind(targetType.Kind()) && isNumericKind(srcValue.Kind()) {
        if srcValue.CanConvert(targetType) {
            return srcValue.Convert(targetType).Interface().(T), nil
        }
    }

    // Special type conversions
    switch targetType {
    case reflect.TypeOf(time.Time{}):
        return convertToTime(src).(T), nil
    case reflect.TypeOf(big.Int{}):
        return convertToBigInt(src).(T), nil
    case reflect.TypeOf(Decimal{}):
        return convertToDecimal(src).(T), nil
    case reflect.TypeOf(Interval{}):
        return convertToInterval(src).(T), nil
    }

    return zero, fmt.Errorf("cannot convert %T to %T", src, zero)
}

func isNumericKind(k reflect.Kind) bool {
    return k >= reflect.Int && k <= reflect.Float64
}

// setFieldValue sets a reflect.Value from an any value (enhanced with nested type support)
func setFieldValue(field reflect.Value, val any) error {
    if val == nil {
        // For pointer fields, set to nil
        if field.Kind() == reflect.Ptr {
            field.Set(reflect.Zero(field.Type()))
            return nil
        }
        // For non-pointer fields, set to zero value
        field.Set(reflect.Zero(field.Type()))
        return nil
    }

    valValue := reflect.ValueOf(val)

    // Handle pointer fields
    if field.Kind() == reflect.Ptr {
        if valValue.Kind() == reflect.Ptr {
            if valValue.Type().AssignableTo(field.Type()) {
                field.Set(valValue)
                return nil
            }
        }
        newPtr := reflect.New(field.Type().Elem())
        if err := setFieldValue(newPtr.Elem(), val); err != nil {
            return err
        }
        field.Set(newPtr)
        return nil
    }

    // Handle nested struct fields (STRUCT with STRUCT field)
    if field.Kind() == reflect.Struct {
        if m, ok := val.(map[string]any); ok {
            for i := 0; i < field.NumField(); i++ {
                sf := field.Type().Field(i)
                name := sf.Name
                if tag := sf.Tag.Get("duckdb"); tag != "" {
                    name = tag
                }
                if v, exists := m[strings.ToLower(name)]; exists {
                    if err := setFieldValue(field.Field(i), v); err != nil {
                        return fmt.Errorf("field %s: %w", name, err)
                    }
                }
            }
            return nil
        }
    }

    // Handle nested slice fields (STRUCT with LIST field)
    if field.Kind() == reflect.Slice {
        if arr, ok := val.([]any); ok {
            elemType := field.Type().Elem()
            slice := reflect.MakeSlice(field.Type(), len(arr), len(arr))
            for i, elem := range arr {
                if err := setFieldValue(slice.Index(i), elem); err != nil {
                    return fmt.Errorf("element %d: %w", i, err)
                }
            }
            field.Set(slice)
            return nil
        }
    }

    // Handle nested map fields
    if field.Kind() == reflect.Map {
        if m, ok := val.(map[any]any); ok {
            newMap := reflect.MakeMap(field.Type())
            for k, v := range m {
                keyVal := reflect.New(field.Type().Key()).Elem()
                if err := setFieldValue(keyVal, k); err != nil {
                    return fmt.Errorf("map key: %w", err)
                }
                valVal := reflect.New(field.Type().Elem()).Elem()
                if err := setFieldValue(valVal, v); err != nil {
                    return fmt.Errorf("map value: %w", err)
                }
                newMap.SetMapIndex(keyVal, valVal)
            }
            field.Set(newMap)
            return nil
        }
    }

    // Direct assignment
    if valValue.Type().AssignableTo(field.Type()) {
        field.Set(valValue)
        return nil
    }

    // Convertible types
    if valValue.Type().ConvertibleTo(field.Type()) {
        field.Set(valValue.Convert(field.Type()))
        return nil
    }

    return fmt.Errorf("cannot convert %T to %s", val, field.Type())
}
```

### 13. MAP NULL Key Handling

The MapScanner explicitly rejects NULL keys (which are invalid in most key contexts):

```go
func (s *MapScanner[K, V]) Scan(src any) error {
    // ... existing nil check ...

    switch v := src.(type) {
    case map[any]any:
        *s.Result = make(map[K]V, len(v))
        for key, val := range v {
            // NULL keys are an error
            if key == nil {
                return fmt.Errorf("map key cannot be NULL")
            }
            k, err := convertToType[K](key)
            if err != nil {
                return fmt.Errorf("map key: %w", err)
            }
            // NULL values convert to zero value
            v, err := convertToType[V](val)
            if err != nil {
                return fmt.Errorf("map value for key %v: %w", key, err)
            }
            (*s.Result)[k] = v
        }
        return nil
    // ...
    }
}
```

### 14. Helper Functions (convert.go)

```go
// toAnySlice converts a typed slice to []any for driver binding
func toAnySlice[T any](src []T) []any {
    result := make([]any, len(src))
    for i, v := range src {
        result[i] = v
    }
    return result
}

// parseUUID parses a UUID string in RFC 4122 format
func parseUUID(s string) ([16]byte, error) {
    var uuid [16]byte
    // Remove dashes from standard format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    s = strings.ReplaceAll(s, "-", "")
    if len(s) != 32 {
        return uuid, fmt.Errorf("invalid UUID length: expected 32 hex chars, got %d", len(s))
    }
    _, err := hex.Decode(uuid[:], []byte(s))
    if err != nil {
        return uuid, fmt.Errorf("invalid UUID hex: %w", err)
    }
    return uuid, nil
}

// convertToTime converts various types to time.Time
func convertToTime(src any) (time.Time, error) {
    switch v := src.(type) {
    case time.Time:
        return v, nil
    case int64:
        return time.Unix(0, v), nil // nanoseconds
    case string:
        return time.Parse(time.RFC3339, v)
    default:
        return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", src)
    }
}

// convertToBigInt converts to *big.Int
func convertToBigInt(src any) (*big.Int, error) {
    switch v := src.(type) {
    case *big.Int:
        return v, nil
    case int64:
        return big.NewInt(v), nil
    case string:
        bi := new(big.Int)
        if _, ok := bi.SetString(v, 10); !ok {
            return nil, fmt.Errorf("invalid big.Int string: %s", v)
        }
        return bi, nil
    default:
        return nil, fmt.Errorf("cannot convert %T to *big.Int", src)
    }
}

// convertToDecimal converts to Decimal type
func convertToDecimal(src any) (Decimal, error) {
    switch v := src.(type) {
    case Decimal:
        return v, nil
    case float64:
        return NewDecimalFromFloat(v), nil
    case string:
        return ParseDecimal(v)
    default:
        return Decimal{}, fmt.Errorf("cannot convert %T to Decimal", src)
    }
}

// convertToInterval converts to Interval type
func convertToInterval(src any) (Interval, error) {
    switch v := src.(type) {
    case Interval:
        return v, nil
    case string:
        return ParseInterval(v)
    default:
        return Interval{}, fmt.Errorf("cannot convert %T to Interval", src)
    }
}
```

### 15. TimeNS Scanning from Database

TimeNS values scanned from database do NOT use the clock (clock is only for creation).
**Note:** The TimeNS struct does NOT store a clock field - clock is only passed to factory functions.

```go
// TimeNS represents time with nanosecond precision (no clock field)
type TimeNS struct {
    ns int64 // Nanoseconds since midnight
}

// TimeNSScanner scans TIME with nanosecond precision from database
type TimeNSScanner struct {
    Result *TimeNS
}

func (s *TimeNSScanner) Scan(src any) error {
    if src == nil {
        *s.Result = TimeNS{ns: 0}
        return nil
    }

    switch v := src.(type) {
    case time.Time:
        midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())
        *s.Result = TimeNS{ns: v.Sub(midnight).Nanoseconds()}
        return nil
    case int64:
        // Already in nanoseconds since midnight
        *s.Result = TimeNS{ns: v}
        return nil
    default:
        return fmt.Errorf("cannot scan %T into TimeNSScanner", src)
    }
}
```

## Deterministic Testing Requirements

All timestamp-related functions accept quartz.Clock:
- `NowNS(clock)` - Current time as TimeNS
- `CurrentTimeNS(clock)` - Current timestamp in nanoseconds

Scanning TimeNS from database does NOT use clock (it's reading data, not generating time).

Tests use mock clock for reproducible results.

## Design Decisions Summary

1. **Primitive types** use standard sql.Scanner (existing behavior)
2. **Complex types** get typed generic scanners
3. **ARRAY** gets separate scanner with size validation
4. **NULL keys in MAP** are rejected with error
5. **NULL values** become zero values (or nil for pointer types)
6. **Pointer field types** allow distinguishing NULL from zero
7. **Clock** is only for time creation, not scanning
