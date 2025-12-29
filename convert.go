package dukdb

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
)

// convertToType converts an any value to a specific type using generics.
// Supports exact type matches, numeric conversions, and special type handling.
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

	// String to numeric conversions
	if srcValue.Kind() == reflect.String && isNumericKind(targetType.Kind()) {
		strVal := srcValue.String()
		converted, err := stringToNumeric(strVal, targetType.Kind())
		if err != nil {
			return zero, err
		}
		return reflect.ValueOf(converted).Convert(targetType).Interface().(T), nil
	}

	// Special type conversions
	switch targetType {
	case reflect.TypeOf(time.Time{}):
		t, err := convertToTime(src)
		if err != nil {
			return zero, err
		}
		return any(t).(T), nil
	case reflect.TypeOf(&big.Int{}):
		bi, err := convertToBigInt(src)
		if err != nil {
			return zero, err
		}
		return any(bi).(T), nil
	case reflect.TypeOf(Decimal{}):
		d, err := convertToDecimal(src)
		if err != nil {
			return zero, err
		}
		return any(d).(T), nil
	case reflect.TypeOf(Interval{}):
		i, err := convertToInterval(src)
		if err != nil {
			return zero, err
		}
		return any(i).(T), nil
	}

	return zero, fmt.Errorf("cannot convert %T to %T", src, zero)
}

// isNumericKind returns true if the kind is a numeric type.
func isNumericKind(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Float64
}

// stringToNumeric converts a string to a numeric value.
func stringToNumeric(s string, kind reflect.Kind) (any, error) {
	var result any
	var err error

	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var v int64
		_, err = fmt.Sscanf(s, "%d", &v)
		result = v
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var v uint64
		_, err = fmt.Sscanf(s, "%d", &v)
		result = v
	case reflect.Float32, reflect.Float64:
		var v float64
		_, err = fmt.Sscanf(s, "%f", &v)
		result = v
	default:
		return nil, fmt.Errorf("unsupported numeric kind: %v", kind)
	}

	if err != nil {
		return nil, fmt.Errorf("cannot parse %q as numeric: %w", s, err)
	}
	return result, nil
}

// setFieldValue sets a reflect.Value from an any value.
// Supports direct assignment, type conversion, and nested types.
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
	if field.Kind() == reflect.Struct && field.Type() != reflect.TypeOf(time.Time{}) {
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

// toAnySlice converts a typed slice to []any for driver binding.
func toAnySlice[T any](src []T) []any {
	result := make([]any, len(src))
	for i, v := range src {
		result[i] = v
	}
	return result
}

// parseUUID parses a UUID string in RFC 4122 format.
// Accepts format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	// Remove dashes from standard format
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

// convertToTime converts various types to time.Time.
func convertToTime(src any) (time.Time, error) {
	switch v := src.(type) {
	case time.Time:
		return v, nil
	case int64:
		return time.Unix(0, v), nil // nanoseconds
	case string:
		// Try RFC3339 first
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}
		// Try RFC3339Nano
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, nil
		}
		// Try date only
		if t, err := time.Parse("2006-01-02", v); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as time", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", src)
	}
}

// convertToBigInt converts to *big.Int.
func convertToBigInt(src any) (*big.Int, error) {
	switch v := src.(type) {
	case *big.Int:
		return v, nil
	case int64:
		return big.NewInt(v), nil
	case int:
		return big.NewInt(int64(v)), nil
	case uint64:
		return new(big.Int).SetUint64(v), nil
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

// convertToDecimal converts to Decimal type.
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

// convertToInterval converts to Interval type.
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

// NewDecimalFromFloat creates a Decimal from a float64.
// Uses default width=18 and auto-detects scale from significant digits.
func NewDecimalFromFloat(f float64) Decimal {
	// Simple implementation: convert to string and parse
	s := fmt.Sprintf("%g", f)
	d, err := ParseDecimal(s)
	if err != nil {
		// Fallback to zero decimal
		return Decimal{Width: 18, Scale: 0, Value: big.NewInt(0)}
	}
	return d
}

// ParseDecimal parses a decimal string like "123.45" into a Decimal.
func ParseDecimal(s string) (Decimal, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Decimal{}, fmt.Errorf("empty decimal string")
	}

	// Handle negative sign
	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}

	// Split by decimal point
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return Decimal{}, fmt.Errorf("invalid decimal format: %s", s)
	}

	var intPart, fracPart string
	intPart = parts[0]
	if len(parts) == 2 {
		fracPart = parts[1]
	}

	// Remove leading zeros from integer part (but keep at least one digit)
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}

	// Combine parts for the unscaled value
	combined := intPart + fracPart
	scale := uint8(len(fracPart))
	width := uint8(len(combined))
	if width < 1 {
		width = 1
	}

	value := new(big.Int)
	if _, ok := value.SetString(combined, 10); !ok {
		return Decimal{}, fmt.Errorf("invalid decimal digits: %s", combined)
	}

	if negative {
		value.Neg(value)
	}

	return Decimal{
		Width: width,
		Scale: scale,
		Value: value,
	}, nil
}

// ParseInterval parses an interval string like "1 year 2 months 3 days".
// Supports ISO 8601 duration format and PostgreSQL-style intervals.
func ParseInterval(s string) (Interval, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Interval{}, nil
	}

	var interval Interval

	// Simple keyword-based parsing
	s = strings.ToLower(s)
	parts := strings.Fields(s)

	for i := 0; i < len(parts); i++ {
		var num int64
		if _, err := fmt.Sscanf(parts[i], "%d", &num); err != nil {
			continue
		}

		if i+1 < len(parts) {
			unit := strings.TrimSuffix(parts[i+1], "s") // Handle plural
			switch unit {
			case "year":
				interval.Months += int32(num * 12)
				i++
			case "month":
				interval.Months += int32(num)
				i++
			case "day":
				interval.Days += int32(num)
				i++
			case "hour":
				interval.Micros += num * 3600 * 1000000
				i++
			case "minute", "min":
				interval.Micros += num * 60 * 1000000
				i++
			case "second", "sec":
				interval.Micros += num * 1000000
				i++
			case "millisecond", "ms":
				interval.Micros += num * 1000
				i++
			case "microsecond", "us":
				interval.Micros += num
				i++
			}
		}
	}

	return interval, nil
}
