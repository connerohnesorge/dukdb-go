package dukdb

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// scanValue converts a source value to a destination value.
// The dest must be a pointer to the target type.
// Implements type conversion from backend JSON values to Go types.
func scanValue(src any, dest any) error {
	// 1. Check if dest implements sql.Scanner
	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(src)
	}

	// 2. Verify dest is a pointer
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr {
		return &Error{
			Type: ErrorTypeInvalid,
			Msg:  "dest must be pointer",
		}
	}

	if dv.IsNil() {
		return &Error{
			Type: ErrorTypeInvalid,
			Msg:  "dest is nil pointer",
		}
	}

	dv = dv.Elem()

	// 3. Handle NULL (src is nil)
	if src == nil {
		dv.Set(reflect.Zero(dv.Type()))

		return nil
	}

	// 4. Type-specific conversion
	return convertValue(src, dv)
}

// convertValue performs the actual type conversion.
func convertValue(
	src any,
	dv reflect.Value,
) error {
	// Handle pointer destinations (e.g., *int, **string)
	if dv.Kind() == reflect.Ptr {
		// Special case: *big.Int - don't dereference, handle specially
		if dv.Type() == reflectTypeBigInt {
			return scanBigIntPtr(src, dv)
		}
		// Create new value for pointer target
		if dv.IsNil() {
			dv.Set(reflect.New(dv.Type().Elem()))
		}

		return convertValue(src, dv.Elem())
	}

	// Handle interface{} / any destination
	if dv.Kind() == reflect.Interface {
		dv.Set(reflect.ValueOf(src))

		return nil
	}

	// Try direct assignment if types match
	sv := reflect.ValueOf(src)
	if sv.Type().AssignableTo(dv.Type()) {
		dv.Set(sv)

		return nil
	}

	// Type-specific conversions
	switch dv.Kind() {
	case reflect.Bool:
		return scanBool(src, dv)
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		return scanInt(src, dv)
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		return scanUint(src, dv)
	case reflect.Float32, reflect.Float64:
		return scanFloat(src, dv)
	case reflect.String:
		return scanString(src, dv)
	case reflect.Slice:
		return scanSlice(src, dv)
	case reflect.Map:
		return scanMap(src, dv)
	case reflect.Struct:
		return scanStruct(src, dv)
	default:
		return &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"cannot scan %T into %s",
				src,
				dv.Type(),
			),
		}
	}
}

// scanBool converts src to bool.
func scanBool(src any, dv reflect.Value) error {
	switch v := src.(type) {
	case bool:
		dv.SetBool(v)

		return nil
	case int64:
		dv.SetBool(v != 0)

		return nil
	case float64:
		dv.SetBool(v != 0)

		return nil
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot convert %q to bool", v)}
		}
		dv.SetBool(b)

		return nil
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into bool", src)}
	}
}

// scanInt converts src to int type.
func scanInt(src any, dv reflect.Value) error {
	var i64 int64
	switch v := src.(type) {
	case int:
		i64 = int64(v)
	case int8:
		i64 = int64(v)
	case int16:
		i64 = int64(v)
	case int32:
		i64 = int64(v)
	case int64:
		i64 = v
	case uint:
		i64 = int64(v)
	case uint8:
		i64 = int64(v)
	case uint16:
		i64 = int64(v)
	case uint32:
		i64 = int64(v)
	case uint64:
		if v > math.MaxInt64 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("uint64 %d overflows int64", v)}
		}
		i64 = int64(v)
	case float32:
		i64 = int64(v)
	case float64:
		i64 = int64(v)
	case string:
		var err error
		i64, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Try parsing as float first
			f, ferr := strconv.ParseFloat(v, 64)
			if ferr != nil {
				return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot convert %q to int", v)}
			}
			i64 = int64(f)
		}
	case bool:
		if v {
			i64 = 1
		} else {
			i64 = 0
		}
	case *big.Int:
		if v == nil {
			i64 = 0
		} else if v.IsInt64() {
			i64 = v.Int64()
		} else {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("big.Int %s overflows int64", v.String())}
		}
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into int", src)}
	}

	// Check overflow for target type
	if err := checkIntOverflow(i64, dv.Type()); err != nil {
		return err
	}

	dv.SetInt(i64)

	return nil
}

// scanUint converts src to uint type.
func scanUint(src any, dv reflect.Value) error {
	var u64 uint64
	switch v := src.(type) {
	case int:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %d cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case int8:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %d cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case int16:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %d cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case int32:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %d cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case int64:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %d cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case uint:
		u64 = uint64(v)
	case uint8:
		u64 = uint64(v)
	case uint16:
		u64 = uint64(v)
	case uint32:
		u64 = uint64(v)
	case uint64:
		u64 = v
	case float32:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %f cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case float64:
		if v < 0 {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("negative value %f cannot be converted to uint", v)}
		}
		u64 = uint64(v)
	case string:
		var err error
		u64, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			// Try parsing as float first
			f, ferr := strconv.ParseFloat(v, 64)
			if ferr != nil || f < 0 {
				return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot convert %q to uint", v)}
			}
			u64 = uint64(f)
		}
	case bool:
		if v {
			u64 = 1
		} else {
			u64 = 0
		}
	case *big.Int:
		if v == nil {
			u64 = 0
		} else if v.IsUint64() {
			u64 = v.Uint64()
		} else {
			return &Error{Type: ErrorTypeOutOfRange, Msg: fmt.Sprintf("big.Int %s overflows uint64", v.String())}
		}
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into uint", src)}
	}

	// Check overflow for target type
	if err := checkUintOverflow(u64, dv.Type()); err != nil {
		return err
	}

	dv.SetUint(u64)

	return nil
}

// scanFloat converts src to float type.
func scanFloat(src any, dv reflect.Value) error {
	var f64 float64
	switch v := src.(type) {
	case float32:
		f64 = float64(v)
	case float64:
		f64 = v
	case int:
		f64 = float64(v)
	case int8:
		f64 = float64(v)
	case int16:
		f64 = float64(v)
	case int32:
		f64 = float64(v)
	case int64:
		f64 = float64(v)
	case uint:
		f64 = float64(v)
	case uint8:
		f64 = float64(v)
	case uint16:
		f64 = float64(v)
	case uint32:
		f64 = float64(v)
	case uint64:
		f64 = float64(v)
	case string:
		var err error
		f64, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot convert %q to float", v)}
		}
	case *big.Int:
		if v == nil {
			f64 = 0
		} else {
			f := new(big.Float).SetInt(v)
			f64, _ = f.Float64()
		}
	case Decimal:
		f64 = v.Float64()
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into float", src)}
	}

	dv.SetFloat(f64)

	return nil
}

// scanBigIntPtr converts src to *big.Int.
func scanBigIntPtr(
	src any,
	dv reflect.Value,
) error {
	var bi *big.Int
	switch v := src.(type) {
	case *big.Int:
		bi = v
	case int:
		bi = big.NewInt(int64(v))
	case int8:
		bi = big.NewInt(int64(v))
	case int16:
		bi = big.NewInt(int64(v))
	case int32:
		bi = big.NewInt(int64(v))
	case int64:
		bi = big.NewInt(v)
	case uint:
		bi = new(big.Int).SetUint64(uint64(v))
	case uint8:
		bi = new(big.Int).SetUint64(uint64(v))
	case uint16:
		bi = new(big.Int).SetUint64(uint64(v))
	case uint32:
		bi = new(big.Int).SetUint64(uint64(v))
	case uint64:
		bi = new(big.Int).SetUint64(v)
	case float64:
		bi = big.NewInt(int64(v))
	case string:
		bi = new(big.Int)
		if _, ok := bi.SetString(v, 10); !ok {
			return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot convert %q to big.Int", v)}
		}
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into *big.Int", src)}
	}

	dv.Set(reflect.ValueOf(bi))

	return nil
}

// scanString converts src to string.
func scanString(src any, dv reflect.Value) error {
	var s string
	switch v := src.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case int8:
		s = strconv.FormatInt(int64(v), 10)
	case int16:
		s = strconv.FormatInt(int64(v), 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case int64:
		s = strconv.FormatInt(v, 10)
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
	case uint8:
		s = strconv.FormatUint(uint64(v), 10)
	case uint16:
		s = strconv.FormatUint(uint64(v), 10)
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
	case uint64:
		s = strconv.FormatUint(v, 10)
	case float32:
		s = strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		s = strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		s = strconv.FormatBool(v)
	case time.Time:
		s = v.Format(time.RFC3339Nano)
	case *big.Int:
		if v == nil {
			s = "0"
		} else {
			s = v.String()
		}
	case Decimal:
		s = v.String()
	case UUID:
		s = v.String()
	case *UUID:
		if v == nil {
			s = ""
		} else {
			s = v.String()
		}
	case Interval:
		s = fmt.Sprintf("Interval{Months:%d, Days:%d, Micros:%d}", v.Months, v.Days, v.Micros)
	default:
		s = fmt.Sprintf("%v", v)
	}

	dv.SetString(s)

	return nil
}

// scanSlice converts src to slice.
func scanSlice(src any, dv reflect.Value) error {
	// Handle []byte destination specially
	if dv.Type().Elem().Kind() == reflect.Uint8 {
		return scanBytes(src, dv)
	}

	// Handle []any from LIST type
	switch v := src.(type) {
	case []any:
		slice := reflect.MakeSlice(dv.Type(), len(v), len(v))
		for i, elem := range v {
			if err := convertValue(elem, slice.Index(i)); err != nil {
				return err
			}
		}
		dv.Set(slice)

		return nil
	default:
		// Try to handle other slice types
		sv := reflect.ValueOf(src)
		if sv.Kind() == reflect.Slice {
			slice := reflect.MakeSlice(dv.Type(), sv.Len(), sv.Len())
			for i := 0; i < sv.Len(); i++ {
				if err := convertValue(sv.Index(i).Interface(), slice.Index(i)); err != nil {
					return err
				}
			}
			dv.Set(slice)

			return nil
		}

		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into slice", src)}
	}
}

// scanBytes converts src to []byte.
func scanBytes(src any, dv reflect.Value) error {
	var bytes []byte
	switch v := src.(type) {
	case []byte:
		bytes = v
	case string:
		// Check for hex-encoded blob format "\x..."
		if strings.HasPrefix(v, "\\x") {
			var err error
			bytes, err = hex.DecodeString(v[2:])
			if err != nil {
				return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot decode hex string: %v", err)}
			}
		} else {
			bytes = []byte(v)
		}
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into []byte", src)}
	}

	dv.SetBytes(bytes)

	return nil
}

// scanMap converts src to map.
func scanMap(src any, dv reflect.Value) error {
	switch v := src.(type) {
	case map[string]any:
		if dv.IsNil() {
			dv.Set(reflect.MakeMap(dv.Type()))
		}
		for key, val := range v {
			kv := reflect.ValueOf(key)
			vv := reflect.New(dv.Type().Elem()).Elem()
			if err := convertValue(val, vv); err != nil {
				return err
			}
			dv.SetMapIndex(kv, vv)
		}

		return nil
	case Map:
		if dv.Type() == reflectTypeMap {
			dv.Set(reflect.ValueOf(v))

			return nil
		}
		// Convert Map to destination map type
		if dv.IsNil() {
			dv.Set(reflect.MakeMap(dv.Type()))
		}
		for key, val := range v {
			kv := reflect.ValueOf(key)
			if !kv.Type().ConvertibleTo(dv.Type().Key()) {
				return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot convert map key %T to %s", key, dv.Type().Key())}
			}
			kv = kv.Convert(dv.Type().Key())
			vv := reflect.New(dv.Type().Elem()).Elem()
			if err := convertValue(val, vv); err != nil {
				return err
			}
			dv.SetMapIndex(kv, vv)
		}

		return nil
	case map[any]any:
		if dv.IsNil() {
			dv.Set(reflect.MakeMap(dv.Type()))
		}
		for key, val := range v {
			kv := reflect.ValueOf(key)
			if !kv.Type().ConvertibleTo(dv.Type().Key()) {
				return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot convert map key %T to %s", key, dv.Type().Key())}
			}
			kv = kv.Convert(dv.Type().Key())
			vv := reflect.New(dv.Type().Elem()).Elem()
			if err := convertValue(val, vv); err != nil {
				return err
			}
			dv.SetMapIndex(kv, vv)
		}

		return nil
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into map", src)}
	}
}

// scanStruct converts src to struct.
func scanStruct(src any, dv reflect.Value) error {
	// Handle specific struct types
	switch dv.Type() {
	case reflectTypeTime:
		return scanTime(src, dv)
	case reflectTypeUUID:
		return scanUUID(src, dv)
	case reflectTypeInterval:
		return scanInterval(src, dv)
	case reflectTypeDecimal:
		return scanDecimal(src, dv)
	case reflectTypeUnion:
		return scanUnion(src, dv)
	}

	// Handle map[string]any -> struct conversion
	if m, ok := src.(map[string]any); ok {
		return scanStructFromMap(m, dv)
	}

	return &Error{
		Type: ErrorTypeInvalid,
		Msg: fmt.Sprintf(
			"cannot scan %T into struct %s",
			src,
			dv.Type(),
		),
	}
}

// scanTime converts src to time.Time.
func scanTime(src any, dv reflect.Value) error {
	var t time.Time
	switch v := src.(type) {
	case time.Time:
		t = v
	case string:
		var err error
		// Try various formats
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"15:04:05.999999999",
			"15:04:05.999999",
			"15:04:05",
		}
		for _, format := range formats {
			t, err = time.Parse(format, v)
			if err == nil {
				break
			}
		}
		if err != nil {
			return &Error{Type: ErrorTypeConversion, Msg: fmt.Sprintf("cannot parse %q as time", v)}
		}
	case int64:
		// Assume microseconds since epoch
		t = time.UnixMicro(v)
	case float64:
		// Assume seconds since epoch
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		t = time.Unix(sec, nsec)
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into time.Time", src)}
	}

	dv.Set(reflect.ValueOf(t))

	return nil
}

// scanUUID converts src to UUID.
func scanUUID(src any, dv reflect.Value) error {
	var u UUID
	if err := u.Scan(src); err != nil {
		return &Error{
			Type: ErrorTypeConversion,
			Msg: fmt.Sprintf(
				"cannot scan into UUID: %v",
				err,
			),
		}
	}
	dv.Set(reflect.ValueOf(u))

	return nil
}

// scanInterval converts src to Interval.
func scanInterval(
	src any,
	dv reflect.Value,
) error {
	switch v := src.(type) {
	case Interval:
		dv.Set(reflect.ValueOf(v))

		return nil
	case map[string]any:
		var interval Interval
		if months, ok := v["months"]; ok {
			if m, err := toInt32(months); err == nil {
				interval.Months = m
			}
		}
		if days, ok := v["days"]; ok {
			if d, err := toInt32(days); err == nil {
				interval.Days = d
			}
		}
		if micros, ok := v["micros"]; ok {
			if m, err := toInt64(micros); err == nil {
				interval.Micros = m
			}
		}
		dv.Set(reflect.ValueOf(interval))

		return nil
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into Interval", src)}
	}
}

// scanDecimal converts src to Decimal.
func scanDecimal(
	src any,
	dv reflect.Value,
) error {
	switch v := src.(type) {
	case Decimal:
		dv.Set(reflect.ValueOf(v))

		return nil
	case string:
		dec, err := parseDecimal(v)
		if err != nil {
			return err
		}
		dv.Set(reflect.ValueOf(dec))

		return nil
	case map[string]any:
		// Handle JSON representation of decimal
		var dec Decimal
		if width, ok := v["width"]; ok {
			if w, err := toUint8(width); err == nil {
				dec.Width = w
			}
		}
		if scale, ok := v["scale"]; ok {
			if s, err := toUint8(scale); err == nil {
				dec.Scale = s
			}
		}
		if value, ok := v["value"]; ok {
			switch val := value.(type) {
			case string:
				dec.Value = new(big.Int)
				dec.Value.SetString(val, 10)
			case float64:
				dec.Value = big.NewInt(int64(val))
			case int64:
				dec.Value = big.NewInt(val)
			}
		}
		dv.Set(reflect.ValueOf(dec))

		return nil
	case float64:
		// Convert float to decimal
		dec := Decimal{
			Width: 18,
			Scale: 6,
			Value: big.NewInt(int64(v * 1e6)),
		}
		dv.Set(reflect.ValueOf(dec))

		return nil
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into Decimal", src)}
	}
}

// scanUnion converts src to Union.
func scanUnion(src any, dv reflect.Value) error {
	switch v := src.(type) {
	case Union:
		dv.Set(reflect.ValueOf(v))

		return nil
	case map[string]any:
		var u Union
		if tag, ok := v["tag"]; ok {
			if s, ok := tag.(string); ok {
				u.Tag = s
			}
		}
		if value, ok := v["value"]; ok {
			u.Value = value
		}
		dv.Set(reflect.ValueOf(u))

		return nil
	default:
		return &Error{Type: ErrorTypeInvalid, Msg: fmt.Sprintf("cannot scan %T into Union", src)}
	}
}

// scanStructFromMap converts a map[string]any to a struct using field tags or names.
func scanStructFromMap(
	m map[string]any,
	dv reflect.Value,
) error {
	t := dv.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		// Check for json tag first, then use field name
		name := field.Name
		if tag := field.Tag.Get("json"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" && parts[0] != "-" {
				name = parts[0]
			}
		}

		if val, ok := m[name]; ok {
			fieldVal := dv.Field(i)
			if err := convertValue(val, fieldVal); err != nil {
				return err
			}
		}
	}

	return nil
}

// Helper functions for type checking and conversion

func checkIntOverflow(
	v int64,
	t reflect.Type,
) error {
	switch t.Kind() {
	case reflect.Int8:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows int8",
					v,
				),
			}
		}
	case reflect.Int16:
		if v < math.MinInt16 ||
			v > math.MaxInt16 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows int16",
					v,
				),
			}
		}
	case reflect.Int32:
		if v < math.MinInt32 ||
			v > math.MaxInt32 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows int32",
					v,
				),
			}
		}
	}

	return nil
}

func checkUintOverflow(
	v uint64,
	t reflect.Type,
) error {
	switch t.Kind() {
	case reflect.Uint8:
		if v > math.MaxUint8 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows uint8",
					v,
				),
			}
		}
	case reflect.Uint16:
		if v > math.MaxUint16 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows uint16",
					v,
				),
			}
		}
	case reflect.Uint32:
		if v > math.MaxUint32 {
			return &Error{
				Type: ErrorTypeOutOfRange,
				Msg: fmt.Sprintf(
					"value %d overflows uint32",
					v,
				),
			}
		}
	}

	return nil
}

func toInt32(v any) (int32, error) {
	switch val := v.(type) {
	case int:
		return int32(val), nil
	case int32:
		return val, nil
	case int64:
		return int32(val), nil
	case float64:
		return int32(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", v)
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint8(v any) (uint8, error) {
	switch val := v.(type) {
	case int:
		return uint8(val), nil
	case int64:
		return uint8(val), nil
	case float64:
		return uint8(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint8", v)
	}
}

// parseDecimal parses a decimal string like "123.45" into a Decimal.
func parseDecimal(s string) (Decimal, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Decimal{}, nil
	}

	// Handle sign
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}

	// Split by decimal point
	parts := strings.Split(s, ".")
	var intPart, fracPart string

	switch len(parts) {
	case 1:
		intPart = parts[0]
		fracPart = ""
	case 2:
		intPart = parts[0]
		fracPart = parts[1]
	default:
		return Decimal{}, &Error{
			Type: ErrorTypeConversion,
			Msg: fmt.Sprintf(
				"invalid decimal format: %s",
				s,
			),
		}
	}

	// Combine into unscaled value
	unscaled := intPart + fracPart
	scale := uint8(len(fracPart))
	width := uint8(len(unscaled))

	value := new(big.Int)
	if _, ok := value.SetString(unscaled, 10); !ok {
		return Decimal{}, &Error{
			Type: ErrorTypeConversion,
			Msg: fmt.Sprintf(
				"invalid decimal value: %s",
				s,
			),
		}
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
