package types

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// TypeConverter handles value conversion between DuckDB and PostgreSQL formats.
type TypeConverter interface {
	// EncodeText converts a Go value to PostgreSQL text format for wire protocol.
	// Returns nil if the value is NULL.
	EncodeText(value any, oid uint32) ([]byte, error)

	// DecodeText converts PostgreSQL text format to a Go value.
	DecodeText(data []byte, oid uint32) (any, error)

	// EncodeBinary converts a Go value to PostgreSQL binary format (optional).
	EncodeBinary(value any, oid uint32) ([]byte, error)

	// DecodeBinary converts PostgreSQL binary format to a Go value (optional).
	DecodeBinary(data []byte, oid uint32) (any, error)
}

// DefaultTypeConverter is the default implementation of TypeConverter.
type DefaultTypeConverter struct{}

// NewTypeConverter creates a new TypeConverter instance.
func NewTypeConverter() TypeConverter {
	return &DefaultTypeConverter{}
}

// EncodeText converts a Go value to PostgreSQL text format.
func (c *DefaultTypeConverter) EncodeText(value any, oid uint32) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	switch oid {
	case OID_BOOL:
		return c.encodeBoolText(value)
	case OID_INT2, OID_INT4, OID_INT8:
		return c.encodeIntText(value)
	case OID_FLOAT4, OID_FLOAT8:
		return c.encodeFloatText(value)
	case OID_NUMERIC:
		return c.encodeNumericText(value)
	case OID_TEXT, OID_VARCHAR, OID_CHAR, OID_BPCHAR, OID_NAME:
		return c.encodeStringText(value)
	case OID_BYTEA:
		return c.encodeByteaText(value)
	case OID_DATE:
		return c.encodeDateText(value)
	case OID_TIME, OID_TIMETZ:
		return c.encodeTimeText(value)
	case OID_TIMESTAMP:
		return c.encodeTimestampText(value)
	case OID_TIMESTAMPTZ:
		return c.encodeTimestampTZText(value)
	case OID_INTERVAL:
		return c.encodeIntervalText(value)
	case OID_UUID:
		return c.encodeUUIDText(value)
	case OID_JSON, OID_JSONB:
		return c.encodeJSONText(value)
	case OID_OID:
		return c.encodeOIDText(value)
	default:
		// Default: convert to string representation
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// Boolean encoding.
func (c *DefaultTypeConverter) encodeBoolText(value any) ([]byte, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int:
		if v != 0 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int8:
		if v != 0 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int16:
		if v != 0 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int32:
		if v != 0 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case int64:
		if v != 0 {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	case string:
		lower := strings.ToLower(v)
		if lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1" || lower == "on" {
			return []byte("t"), nil
		}
		return []byte("f"), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as boolean", value)
	}
}

// Integer encoding.
func (c *DefaultTypeConverter) encodeIntText(value any) ([]byte, error) {
	switch v := value.(type) {
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	case float32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case float64:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case string:
		// Validate it's a valid integer
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid integer: %s", v)
		}
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as integer", value)
	}
}

// Float encoding.
func (c *DefaultTypeConverter) encodeFloatText(value any) ([]byte, error) {
	var f float64
	switch v := value.(type) {
	case float32:
		f = float64(v)
	case float64:
		f = v
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case string:
		var err error
		f, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", v)
		}
	default:
		return nil, fmt.Errorf("cannot encode %T as float", value)
	}

	// Handle special values
	if math.IsNaN(f) {
		return []byte("NaN"), nil
	}
	if math.IsInf(f, 1) {
		return []byte("Infinity"), nil
	}
	if math.IsInf(f, -1) {
		return []byte("-Infinity"), nil
	}

	return []byte(strconv.FormatFloat(f, 'g', -1, 64)), nil
}

// Numeric encoding (DECIMAL).
func (c *DefaultTypeConverter) encodeNumericText(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, 32)), nil
	case int, int8, int16, int32, int64:
		return c.encodeIntText(v)
	case uint, uint8, uint16, uint32, uint64:
		return c.encodeIntText(v)
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// String encoding.
func (c *DefaultTypeConverter) encodeStringText(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// Bytea encoding (hex format: \xDEADBEEF).
func (c *DefaultTypeConverter) encodeByteaText(value any) ([]byte, error) {
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return nil, fmt.Errorf("cannot encode %T as bytea", value)
	}

	// PostgreSQL hex format: \x followed by hex bytes
	result := make([]byte, 2+len(data)*2)
	result[0] = '\\'
	result[1] = 'x'
	hex.Encode(result[2:], data)
	return result, nil
}

// Date encoding (YYYY-MM-DD).
func (c *DefaultTypeConverter) encodeDateText(value any) ([]byte, error) {
	switch v := value.(type) {
	case time.Time:
		return []byte(v.Format("2006-01-02")), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as date", value)
	}
}

// Time encoding (HH:MM:SS.ssssss).
func (c *DefaultTypeConverter) encodeTimeText(value any) ([]byte, error) {
	switch v := value.(type) {
	case time.Time:
		return []byte(v.Format("15:04:05.999999")), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as time", value)
	}
}

// Timestamp encoding (YYYY-MM-DD HH:MM:SS.ssssss).
func (c *DefaultTypeConverter) encodeTimestampText(value any) ([]byte, error) {
	switch v := value.(type) {
	case time.Time:
		return []byte(v.Format("2006-01-02 15:04:05.999999")), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as timestamp", value)
	}
}

// TimestampTZ encoding (YYYY-MM-DD HH:MM:SS.ssssss+ZZ).
func (c *DefaultTypeConverter) encodeTimestampTZText(value any) ([]byte, error) {
	switch v := value.(type) {
	case time.Time:
		return []byte(v.Format("2006-01-02 15:04:05.999999-07")), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as timestamptz", value)
	}
}

// Interval encoding.
func (c *DefaultTypeConverter) encodeIntervalText(value any) ([]byte, error) {
	switch v := value.(type) {
	case time.Duration:
		// Format as PostgreSQL interval
		hours := int64(v.Hours())
		minutes := int64(v.Minutes()) % 60
		seconds := int64(v.Seconds()) % 60
		micros := int64(v.Microseconds()) % 1000000

		if micros > 0 {
			return []byte(fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros)), nil
		}
		return []byte(fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot encode %T as interval", value)
	}
}

// UUID encoding.
func (c *DefaultTypeConverter) encodeUUIDText(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case [16]byte:
		return []byte(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])), nil
	case []byte:
		if len(v) == 16 {
			return []byte(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])), nil
		}
		return nil, fmt.Errorf("invalid UUID length: %d", len(v))
	default:
		return nil, fmt.Errorf("cannot encode %T as UUID", value)
	}
}

// JSON encoding.
func (c *DefaultTypeConverter) encodeJSONText(value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// OID encoding.
func (c *DefaultTypeConverter) encodeOIDText(value any) ([]byte, error) {
	switch v := value.(type) {
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	default:
		return c.encodeIntText(value)
	}
}

// DecodeText converts PostgreSQL text format to a Go value.
func (c *DefaultTypeConverter) DecodeText(data []byte, oid uint32) (any, error) {
	if data == nil {
		return nil, nil
	}

	s := string(data)

	switch oid {
	case OID_BOOL:
		return c.decodeBoolText(s)
	case OID_INT2:
		return c.decodeInt16Text(s)
	case OID_INT4:
		return c.decodeInt32Text(s)
	case OID_INT8:
		return c.decodeInt64Text(s)
	case OID_FLOAT4:
		return c.decodeFloat32Text(s)
	case OID_FLOAT8:
		return c.decodeFloat64Text(s)
	case OID_NUMERIC:
		return s, nil // Keep as string for precision
	case OID_TEXT, OID_VARCHAR, OID_CHAR, OID_BPCHAR, OID_NAME:
		return s, nil
	case OID_BYTEA:
		return c.decodeByteaText(s)
	case OID_DATE:
		return c.decodeDateText(s)
	case OID_TIME, OID_TIMETZ:
		return c.decodeTimeText(s)
	case OID_TIMESTAMP:
		return c.decodeTimestampText(s)
	case OID_TIMESTAMPTZ:
		return c.decodeTimestampTZText(s)
	case OID_UUID:
		return s, nil // Keep as string
	case OID_JSON, OID_JSONB:
		return s, nil // Keep as string
	default:
		return s, nil // Default to string
	}
}

func (c *DefaultTypeConverter) decodeBoolText(s string) (bool, error) {
	lower := strings.ToLower(s)
	switch lower {
	case "t", "true", "yes", "y", "1", "on":
		return true, nil
	case "f", "false", "no", "n", "0", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean: %s", s)
	}
}

func (c *DefaultTypeConverter) decodeInt16Text(s string) (int16, error) {
	v, err := strconv.ParseInt(s, 10, 16)
	return int16(v), err
}

func (c *DefaultTypeConverter) decodeInt32Text(s string) (int32, error) {
	v, err := strconv.ParseInt(s, 10, 32)
	return int32(v), err
}

func (c *DefaultTypeConverter) decodeInt64Text(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func (c *DefaultTypeConverter) decodeFloat32Text(s string) (float32, error) {
	switch strings.ToLower(s) {
	case "nan":
		return float32(math.NaN()), nil
	case "infinity", "inf":
		return float32(math.Inf(1)), nil
	case "-infinity", "-inf":
		return float32(math.Inf(-1)), nil
	}
	v, err := strconv.ParseFloat(s, 32)
	return float32(v), err
}

func (c *DefaultTypeConverter) decodeFloat64Text(s string) (float64, error) {
	switch strings.ToLower(s) {
	case "nan":
		return math.NaN(), nil
	case "infinity", "inf":
		return math.Inf(1), nil
	case "-infinity", "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, 64)
}

func (c *DefaultTypeConverter) decodeByteaText(s string) ([]byte, error) {
	// PostgreSQL hex format: \xDEADBEEF
	if strings.HasPrefix(s, "\\x") {
		return hex.DecodeString(s[2:])
	}
	// Escape format (legacy)
	return []byte(s), nil
}

func (c *DefaultTypeConverter) decodeDateText(s string) (time.Time, error) {
	return time.Parse("2006-01-02", s)
}

func (c *DefaultTypeConverter) decodeTimeText(s string) (time.Time, error) {
	// Try various formats
	formats := []string{
		"15:04:05.999999",
		"15:04:05",
		"15:04:05.999999-07",
		"15:04:05-07",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time: %s", s)
}

func (c *DefaultTypeConverter) decodeTimestampText(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp: %s", s)
}

func (c *DefaultTypeConverter) decodeTimestampTZText(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02T15:04:05.999999-07:00",
		"2006-01-02T15:04:05-07:00",
		time.RFC3339,
		time.RFC3339Nano,
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamptz: %s", s)
}

// EncodeBinary converts a Go value to PostgreSQL binary format.
// Binary encoding is provided for performance-critical numeric types.
func (c *DefaultTypeConverter) EncodeBinary(value any, oid uint32) ([]byte, error) {
	// Binary encoding for performance-critical types
	if value == nil {
		return nil, nil
	}

	switch oid {
	case OID_INT2:
		return c.encodeInt16Binary(value)
	case OID_INT4:
		return c.encodeInt32Binary(value)
	case OID_INT8:
		return c.encodeInt64Binary(value)
	case OID_FLOAT4:
		return c.encodeFloat32Binary(value)
	case OID_FLOAT8:
		return c.encodeFloat64Binary(value)
	case OID_BOOL:
		return c.encodeBoolBinary(value)
	default:
		// Fall back to text encoding for unsupported types
		return nil, fmt.Errorf("binary encoding not supported for OID %d", oid)
	}
}

func (c *DefaultTypeConverter) encodeInt16Binary(value any) ([]byte, error) {
	var v int16
	switch val := value.(type) {
	case int16:
		v = val
	case int:
		v = int16(val)
	case int64:
		v = int16(val)
	case int32:
		v = int16(val)
	default:
		return nil, fmt.Errorf("cannot encode %T as int16", value)
	}
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(v))
	return buf, nil
}

func (c *DefaultTypeConverter) encodeInt32Binary(value any) ([]byte, error) {
	var v int32
	switch val := value.(type) {
	case int32:
		v = val
	case int:
		v = int32(val)
	case int64:
		v = int32(val)
	case int16:
		v = int32(val)
	default:
		return nil, fmt.Errorf("cannot encode %T as int32", value)
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf, nil
}

func (c *DefaultTypeConverter) encodeInt64Binary(value any) ([]byte, error) {
	var v int64
	switch val := value.(type) {
	case int64:
		v = val
	case int:
		v = int64(val)
	case int32:
		v = int64(val)
	case int16:
		v = int64(val)
	default:
		return nil, fmt.Errorf("cannot encode %T as int64", value)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf, nil
}

func (c *DefaultTypeConverter) encodeFloat32Binary(value any) ([]byte, error) {
	var v float32
	switch val := value.(type) {
	case float32:
		v = val
	case float64:
		v = float32(val)
	default:
		return nil, fmt.Errorf("cannot encode %T as float32", value)
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, math.Float32bits(v))
	return buf, nil
}

func (c *DefaultTypeConverter) encodeFloat64Binary(value any) ([]byte, error) {
	var v float64
	switch val := value.(type) {
	case float64:
		v = val
	case float32:
		v = float64(val)
	default:
		return nil, fmt.Errorf("cannot encode %T as float64", value)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return buf, nil
}

func (c *DefaultTypeConverter) encodeBoolBinary(value any) ([]byte, error) {
	var v bool
	switch val := value.(type) {
	case bool:
		v = val
	default:
		return nil, fmt.Errorf("cannot encode %T as bool", value)
	}
	if v {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

// DecodeBinary converts PostgreSQL binary format to a Go value.
func (c *DefaultTypeConverter) DecodeBinary(data []byte, oid uint32) (any, error) {
	if data == nil {
		return nil, nil
	}

	switch oid {
	case OID_INT2:
		if len(data) != 2 {
			return nil, fmt.Errorf("invalid int2 binary length: %d", len(data))
		}
		return int16(binary.BigEndian.Uint16(data)), nil
	case OID_INT4:
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid int4 binary length: %d", len(data))
		}
		return int32(binary.BigEndian.Uint32(data)), nil
	case OID_INT8:
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid int8 binary length: %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data)), nil
	case OID_FLOAT4:
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid float4 binary length: %d", len(data))
		}
		return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
	case OID_FLOAT8:
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid float8 binary length: %d", len(data))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
	case OID_BOOL:
		if len(data) != 1 {
			return nil, fmt.Errorf("invalid bool binary length: %d", len(data))
		}
		return data[0] != 0, nil
	default:
		return nil, fmt.Errorf("binary decoding not supported for OID %d", oid)
	}
}

// Global default converter instance.
var defaultConverter = NewTypeConverter()

// GetDefaultConverter returns the global default TypeConverter instance.
func GetDefaultConverter() TypeConverter {
	return defaultConverter
}

// Convenience functions.

// EncodeValue encodes a value to PostgreSQL text format using the default converter.
func EncodeValue(value any, oid uint32) ([]byte, error) {
	return defaultConverter.EncodeText(value, oid)
}

// DecodeValue decodes a PostgreSQL text value using the default converter.
func DecodeValue(data []byte, oid uint32) (any, error) {
	return defaultConverter.DecodeText(data, oid)
}
