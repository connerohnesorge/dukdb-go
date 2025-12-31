package dukdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ListScanner enables scanning LIST columns to typed Go slices.
// It implements the sql.Scanner interface.
type ListScanner[T any] struct {
	Result *[]T
}

// Scan implements the sql.Scanner interface for ListScanner.
func (s *ListScanner[T]) Scan(src any) error {
	if src == nil {
		*s.Result = nil

		return nil
	}

	switch v := src.(type) {
	case []any:
		*s.Result = make([]T, len(v))
		for i, elem := range v {
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

// ScanList creates a scanner for scanning LIST columns to typed Go slices.
func ScanList[T any](dest *[]T) sql.Scanner {
	return &ListScanner[T]{Result: dest}
}

// ArrayScanner enables scanning ARRAY columns (fixed-size) to typed Go slices.
// It validates the array size against the expected fixed size.
type ArrayScanner[T any] struct {
	Result   *[]T
	Expected int // Expected fixed size (-1 for any size)
}

// Scan implements the sql.Scanner interface for ArrayScanner.
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

// ScanArray creates a scanner that validates fixed-size arrays.
// Pass size=-1 to accept any size.
func ScanArray[T any](
	dest *[]T,
	size int,
) sql.Scanner {
	return &ArrayScanner[T]{
		Result:   dest,
		Expected: size,
	}
}

// StructScanner enables scanning STRUCT columns to Go structs.
// Field mapping uses the duckdb struct tag for custom names.
type StructScanner[T any] struct {
	Result *T
}

// Scan implements the sql.Scanner interface for StructScanner.
func (s *StructScanner[T]) Scan(src any) error {
	if src == nil {
		return nil
	}

	switch v := src.(type) {
	case map[string]any:
		rv := reflect.ValueOf(s.Result).Elem()
		rt := rv.Type()

		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			name := field.Name
			// Check for struct tag: `duckdb:"field_name"`
			if tag := field.Tag.Get("duckdb"); tag != "" {
				name = tag
			}

			// Case-insensitive field matching
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

// ScanStruct creates a scanner for scanning STRUCT columns to Go structs.
func ScanStruct[T any](dest *T) sql.Scanner {
	return &StructScanner[T]{Result: dest}
}

// MapScanner enables scanning MAP columns to Go maps.
// NULL keys are rejected with an error.
type MapScanner[K comparable, V any] struct {
	Result *map[K]V
}

// Scan implements the sql.Scanner interface for MapScanner.
func (s *MapScanner[K, V]) Scan(src any) error {
	if src == nil {
		*s.Result = nil

		return nil
	}

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
			vConverted, err := convertToType[V](val)
			if err != nil {
				return fmt.Errorf("map value for key %v: %w", key, err)
			}
			(*s.Result)[k] = vConverted
		}

		return nil
	case Map:
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
			vConverted, err := convertToType[V](val)
			if err != nil {
				return fmt.Errorf("map value for key %v: %w", key, err)
			}
			(*s.Result)[k] = vConverted
		}

		return nil
	default:
		return fmt.Errorf("cannot scan %T into MapScanner", src)
	}
}

// ScanMap creates a scanner for scanning MAP columns to Go maps.
func ScanMap[K comparable, V any](
	dest *map[K]V,
) sql.Scanner {
	return &MapScanner[K, V]{Result: dest}
}

// UnionValue represents a scanned UNION value with type tag.
type UnionValue struct {
	Tag   string // Name of active member
	Index int    // Index of active member (0-based)
	Value any    // The actual value
}

// As extracts the union value into the destination type.
func (u UnionValue) As(dest any) error {
	return setFieldValue(
		reflect.ValueOf(dest).Elem(),
		u.Value,
	)
}

// UnionScanner enables scanning UNION columns.
type UnionScanner struct {
	Result *UnionValue
}

// Scan implements the sql.Scanner interface for UnionScanner.
func (s *UnionScanner) Scan(src any) error {
	if src == nil {
		*s.Result = UnionValue{
			Tag:   "",
			Index: -1,
			Value: nil,
		}

		return nil
	}

	switch v := src.(type) {
	case UnionValue:
		*s.Result = v

		return nil
	case Union:
		*s.Result = UnionValue{
			Tag:   v.Tag,
			Index: -1, // Union type doesn't carry index
			Value: v.Value,
		}

		return nil
	case map[string]any:
		// Extract tag and value from struct representation
		if tag, ok := v["tag"].(string); ok {
			idx := -1
			if i, ok := v["index"].(int); ok {
				idx = i
			}
			*s.Result = UnionValue{
				Tag:   tag,
				Index: idx,
				Value: v["value"],
			}

			return nil
		}

		return fmt.Errorf("invalid union structure: missing tag field")
	default:
		return fmt.Errorf("cannot scan %T into UnionScanner", src)
	}
}

// ScanUnion creates a scanner for scanning UNION columns.
func ScanUnion(dest *UnionValue) sql.Scanner {
	return &UnionScanner{Result: dest}
}

// EnumScanner enables scanning ENUM columns to custom Go types.
// T must be a string-based type (e.g., type Status string).
type EnumScanner[T ~string] struct {
	Result *T
}

// Scan implements the sql.Scanner interface for EnumScanner.
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
	case []byte:
		*s.Result = T(string(v))

		return nil
	default:
		return fmt.Errorf("cannot scan %T into EnumScanner", src)
	}
}

// ScanEnum creates a scanner for scanning ENUM columns to custom string types.
func ScanEnum[T ~string](dest *T) sql.Scanner {
	return &EnumScanner[T]{Result: dest}
}

// JSONScanner enables scanning JSON columns (stored as VARCHAR) to Go structs.
// Uses json.Unmarshal for deserialization.
type JSONScanner[T any] struct {
	Result *T
}

// Scan implements the sql.Scanner interface for JSONScanner.
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

// ScanJSON creates a scanner for scanning JSON columns to Go structs.
func ScanJSON[T any](dest *T) sql.Scanner {
	return &JSONScanner[T]{Result: dest}
}

// UUIDScanner enables scanning UUID columns to Go UUID types.
type UUIDScanner struct {
	Result *[16]byte
}

// Scan implements the sql.Scanner interface for UUIDScanner.
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
		parsed, err := parseUUID(v)
		if err != nil {
			return fmt.Errorf("invalid UUID string: %w", err)
		}
		*s.Result = parsed

		return nil
	case UUID:
		*s.Result = [16]byte(v)

		return nil
	default:
		return fmt.Errorf("cannot scan %T into UUIDScanner", src)
	}
}

// ScanUUID creates a scanner for scanning UUID columns.
func ScanUUID(dest *[16]byte) sql.Scanner {
	return &UUIDScanner{Result: dest}
}

// TimeNSScanner scans TIME with nanosecond precision from database.
// The scanned value is stored as TimeNS (nanoseconds since midnight).
type TimeNSScanner struct {
	Result *TimeNS
}

// Scan implements the sql.Scanner interface for TimeNSScanner.
func (s *TimeNSScanner) Scan(src any) error {
	if src == nil {
		*s.Result = TimeNS(0)

		return nil
	}

	switch v := src.(type) {
	case time.Time:
		midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())
		*s.Result = TimeNS(v.Sub(midnight).Nanoseconds())

		return nil
	case int64:
		// Already in nanoseconds since midnight
		*s.Result = TimeNS(v)

		return nil
	case TimeNS:
		*s.Result = v

		return nil
	default:
		return fmt.Errorf("cannot scan %T into TimeNSScanner", src)
	}
}

// ScanTimeNS creates a scanner for scanning TIME columns with nanosecond precision.
func ScanTimeNS(dest *TimeNS) sql.Scanner {
	return &TimeNSScanner{Result: dest}
}

// Interface assertions to verify that scanner types implement sql.Scanner.
var (
	_ sql.Scanner = (*ListScanner[int])(nil)
	_ sql.Scanner = (*ArrayScanner[int])(nil)
	_ sql.Scanner = (*StructScanner[struct{}])(
		nil,
	)
	_ sql.Scanner = (*MapScanner[string, int])(
		nil,
	)
	_ sql.Scanner = (*UnionScanner)(nil)
	_ sql.Scanner = (*EnumScanner[string])(nil)
	_ sql.Scanner = (*JSONScanner[struct{}])(nil)
	_ sql.Scanner = (*UUIDScanner)(nil)
	_ sql.Scanner = (*TimeNSScanner)(nil)
)
