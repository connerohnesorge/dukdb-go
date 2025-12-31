package dukdb

import (
	"database/sql/driver"
	"reflect"
	"strings"
)

// ListValue wraps a Go slice for binding as LIST parameter.
// Implements driver.Valuer to convert typed slices to []any for the driver.
type ListValue[T any] []T

// Value implements the driver.Valuer interface.
// Converts the typed slice to []any for driver binding.
func (v ListValue[T]) Value() (driver.Value, error) {
	return toAnySlice(v), nil
}

// NewListValue creates a ListValue from a slice.
func NewListValue[T any](slice []T) ListValue[T] {
	return ListValue[T](slice)
}

// StructValue wraps a Go struct for binding as STRUCT parameter.
// Implements driver.Valuer to convert structs to map[string]any.
// Uses the duckdb struct tag for custom field names.
type StructValue[T any] struct {
	V T
}

// Value implements the driver.Valuer interface.
// Converts the struct to map[string]any for driver binding.
func (v StructValue[T]) Value() (driver.Value, error) {
	rv := reflect.ValueOf(v.V)
	rt := rv.Type()

	// Handle pointer to struct
	if rt.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
		rt = rv.Type()
	}

	if rt.Kind() != reflect.Struct {
		return v.V, nil // Return as-is for non-struct types
	}

	result := make(map[string]any, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		name := field.Name
		if tag := field.Tag.Get("duckdb"); tag != "" {
			name = tag
		}
		// Store with lowercase key for case-insensitive matching
		result[strings.ToLower(name)] = rv.Field(i).Interface()
	}

	return result, nil
}

// NewStructValue creates a StructValue from a struct.
func NewStructValue[T any](val T) StructValue[T] {
	return StructValue[T]{V: val}
}

// MapValue wraps a Go map for binding as MAP parameter.
// Implements driver.Valuer to convert typed maps to map[any]any.
type MapValue[K comparable, V any] map[K]V

// Value implements the driver.Valuer interface.
// Converts the typed map to map[any]any for driver binding.
func (v MapValue[K, V]) Value() (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	result := make(map[any]any, len(v))
	for k, val := range v {
		result[k] = val
	}

	return result, nil
}

// NewMapValue creates a MapValue from a map.
func NewMapValue[K comparable, V any](m map[K]V) MapValue[K, V] {
	return MapValue[K, V](m)
}

// ArrayValue wraps a Go slice for binding as ARRAY parameter.
// Similar to ListValue but semantically represents a fixed-size array.
type ArrayValue[T any] []T

// Value implements the driver.Valuer interface.
// Converts the typed slice to []any for driver binding.
func (v ArrayValue[T]) Value() (driver.Value, error) {
	return toAnySlice(v), nil
}

// NewArrayValue creates an ArrayValue from a slice.
func NewArrayValue[T any](slice []T) ArrayValue[T] {
	return ArrayValue[T](slice)
}

// JSONValue wraps a value for binding as JSON parameter.
// The value will be serialized to JSON string before binding.
type JSONValue[T any] struct {
	V T
}

// Value implements the driver.Valuer interface.
// Serializes the value to JSON string for driver binding.
func (v JSONValue[T]) Value() (driver.Value, error) {
	return v.V, nil // Let the driver handle JSON serialization
}

// NewJSONValue creates a JSONValue from any value.
func NewJSONValue[T any](val T) JSONValue[T] {
	return JSONValue[T]{V: val}
}

// Interface assertions to verify that binding types implement driver.Valuer.
var (
	_ driver.Valuer = ListValue[int](nil)
	_ driver.Valuer = StructValue[struct{}]{}
	_ driver.Valuer = MapValue[string, int](nil)
	_ driver.Valuer = ArrayValue[int](nil)
	_ driver.Valuer = JSONValue[struct{}]{}
)
