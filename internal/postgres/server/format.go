// Package server provides PostgreSQL wire protocol server functionality.
// This file implements value formatters for converting Go values to PostgreSQL wire format.
package server

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

// ValueFormatter defines the interface for formatting values to PostgreSQL wire format.
// Implementations handle type-specific encoding for both text and binary formats.
type ValueFormatter interface {
	// FormatText converts a Go value to PostgreSQL text format bytes.
	// Returns nil for NULL values.
	FormatText(value any) ([]byte, error)

	// FormatBinary converts a Go value to PostgreSQL binary format bytes.
	// Returns nil for NULL values.
	// Returns an error if binary format is not supported for the type.
	FormatBinary(value any) ([]byte, error)
}

// FormatterRegistry provides access to type-specific formatters.
type FormatterRegistry struct {
	formatters map[uint32]ValueFormatter
}

// NewFormatterRegistry creates a new FormatterRegistry with all built-in formatters.
func NewFormatterRegistry() *FormatterRegistry {
	r := &FormatterRegistry{
		formatters: make(map[uint32]ValueFormatter),
	}
	r.registerBuiltinFormatters()
	return r
}

// registerBuiltinFormatters registers all built-in type formatters.
func (r *FormatterRegistry) registerBuiltinFormatters() {
	// Boolean
	r.formatters[types.OID_BOOL] = &BoolFormatter{}

	// Integer types
	r.formatters[types.OID_INT2] = &Int16Formatter{}
	r.formatters[types.OID_INT4] = &Int32Formatter{}
	r.formatters[types.OID_INT8] = &Int64Formatter{}
	r.formatters[types.OID_OID] = &OIDFormatter{}

	// Float types
	r.formatters[types.OID_FLOAT4] = &Float32Formatter{}
	r.formatters[types.OID_FLOAT8] = &Float64Formatter{}

	// String types
	stringFormatter := &StringFormatter{}
	r.formatters[types.OID_TEXT] = stringFormatter
	r.formatters[types.OID_VARCHAR] = stringFormatter
	r.formatters[types.OID_CHAR] = stringFormatter
	r.formatters[types.OID_BPCHAR] = stringFormatter
	r.formatters[types.OID_NAME] = stringFormatter

	// Binary type
	r.formatters[types.OID_BYTEA] = &ByteaFormatter{}

	// Date/Time types
	r.formatters[types.OID_DATE] = &DateFormatter{}
	r.formatters[types.OID_TIME] = &TimeFormatter{}
	r.formatters[types.OID_TIMETZ] = &TimeTZFormatter{}
	r.formatters[types.OID_TIMESTAMP] = &TimestampFormatter{}
	r.formatters[types.OID_TIMESTAMPTZ] = &TimestampTZFormatter{}
	r.formatters[types.OID_INTERVAL] = &IntervalFormatter{}

	// Numeric type
	r.formatters[types.OID_NUMERIC] = &NumericFormatter{}

	// UUID type
	r.formatters[types.OID_UUID] = &UUIDFormatter{}

	// JSON types
	jsonFormatter := &JSONFormatter{}
	r.formatters[types.OID_JSON] = jsonFormatter
	r.formatters[types.OID_JSONB] = jsonFormatter

	// Array types
	r.formatters[types.OID_BOOL_ARRAY] = &ArrayFormatter{elemOid: types.OID_BOOL}
	r.formatters[types.OID_INT2_ARRAY] = &ArrayFormatter{elemOid: types.OID_INT2}
	r.formatters[types.OID_INT4_ARRAY] = &ArrayFormatter{elemOid: types.OID_INT4}
	r.formatters[types.OID_INT8_ARRAY] = &ArrayFormatter{elemOid: types.OID_INT8}
	r.formatters[types.OID_FLOAT4_ARRAY] = &ArrayFormatter{elemOid: types.OID_FLOAT4}
	r.formatters[types.OID_FLOAT8_ARRAY] = &ArrayFormatter{elemOid: types.OID_FLOAT8}
	r.formatters[types.OID_TEXT_ARRAY] = &ArrayFormatter{elemOid: types.OID_TEXT}
	r.formatters[types.OID_VARCHAR_ARRAY] = &ArrayFormatter{elemOid: types.OID_VARCHAR}
	r.formatters[types.OID_UUID_ARRAY] = &ArrayFormatter{elemOid: types.OID_UUID}
	r.formatters[types.OID_JSON_ARRAY] = &ArrayFormatter{elemOid: types.OID_JSON}
	r.formatters[types.OID_JSONB_ARRAY] = &ArrayFormatter{elemOid: types.OID_JSONB}
}

// Get returns the formatter for the given type OID.
// Returns a default StringFormatter if no specific formatter is registered.
func (r *FormatterRegistry) Get(oid uint32) ValueFormatter {
	if f, ok := r.formatters[oid]; ok {
		return f
	}
	// Default to string formatter for unknown types
	return &StringFormatter{}
}

// Register adds a custom formatter for a type OID.
func (r *FormatterRegistry) Register(oid uint32, formatter ValueFormatter) {
	r.formatters[oid] = formatter
}

// FormatValue formats a value using the appropriate formatter for the given OID.
// Returns nil for NULL values.
func (r *FormatterRegistry) FormatValue(value any, oid uint32, binary bool) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	formatter := r.Get(oid)
	if binary {
		return formatter.FormatBinary(value)
	}
	return formatter.FormatText(value)
}

// Global formatter registry instance.
var defaultFormatterRegistry = NewFormatterRegistry()

// GetFormatterRegistry returns the global formatter registry.
func GetFormatterRegistry() *FormatterRegistry {
	return defaultFormatterRegistry
}

// FormatValue is a convenience function to format a value using the global registry.
func FormatValue(value any, oid uint32, binary bool) ([]byte, error) {
	return defaultFormatterRegistry.FormatValue(value, oid, binary)
}

// BoolFormatter formats boolean values.
type BoolFormatter struct{}

// FormatText formats a boolean value to text format ("t" or "f").
func (f *BoolFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	b, err := toBool(value)
	if err != nil {
		return nil, err
	}
	if b {
		return []byte("t"), nil
	}
	return []byte("f"), nil
}

// FormatBinary formats a boolean value to binary format (1 byte: 0 or 1).
func (f *BoolFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	b, err := toBool(value)
	if err != nil {
		return nil, err
	}
	if b {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

// Int16Formatter formats int16 (smallint) values.
type Int16Formatter struct{}

// FormatText formats an int16 value to text format.
func (f *Int16Formatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(v, 10)), nil
}

// FormatBinary formats an int16 value to binary format (2 bytes, big endian).
func (f *Int16Formatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(int16(v)))
	return buf, nil
}

// Int32Formatter formats int32 (integer) values.
type Int32Formatter struct{}

// FormatText formats an int32 value to text format.
func (f *Int32Formatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(v, 10)), nil
}

// FormatBinary formats an int32 value to binary format (4 bytes, big endian).
func (f *Int32Formatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(int32(v)))
	return buf, nil
}

// Int64Formatter formats int64 (bigint) values.
type Int64Formatter struct{}

// FormatText formats an int64 value to text format.
func (f *Int64Formatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatInt(v, 10)), nil
}

// FormatBinary formats an int64 value to binary format (8 bytes, big endian).
func (f *Int64Formatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toInt64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf, nil
}

// OIDFormatter formats OID values (same as int32 but unsigned).
type OIDFormatter struct{}

// FormatText formats an OID value to text format.
func (f *OIDFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toUint64(value)
	if err != nil {
		return nil, err
	}
	return []byte(strconv.FormatUint(v, 10)), nil
}

// FormatBinary formats an OID value to binary format (4 bytes, big endian).
func (f *OIDFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toUint64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf, nil
}

// Float32Formatter formats float32 (real) values.
type Float32Formatter struct{}

// FormatText formats a float32 value to text format.
func (f *Float32Formatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toFloat64(value)
	if err != nil {
		return nil, err
	}
	return formatFloatText(v), nil
}

// FormatBinary formats a float32 value to binary format (4 bytes, IEEE 754).
func (f *Float32Formatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toFloat64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, math.Float32bits(float32(v)))
	return buf, nil
}

// Float64Formatter formats float64 (double precision) values.
type Float64Formatter struct{}

// FormatText formats a float64 value to text format.
func (f *Float64Formatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toFloat64(value)
	if err != nil {
		return nil, err
	}
	return formatFloatText(v), nil
}

// FormatBinary formats a float64 value to binary format (8 bytes, IEEE 754).
func (f *Float64Formatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v, err := toFloat64(value)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return buf, nil
}

// StringFormatter formats string/text values.
type StringFormatter struct{}

// FormatText formats a string value to text format.
func (f *StringFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	s, err := toString(value)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

// FormatBinary formats a string value to binary format (raw bytes).
func (f *StringFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	s, err := toString(value)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

// ByteaFormatter formats bytea (binary data) values.
type ByteaFormatter struct{}

// FormatText formats bytea value to PostgreSQL hex format (\xDEADBEEF).
func (f *ByteaFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	data, err := toBytes(value)
	if err != nil {
		return nil, err
	}
	// PostgreSQL hex format: \x followed by hex bytes
	result := make([]byte, 2+len(data)*2)
	result[0] = '\\'
	result[1] = 'x'
	hex.Encode(result[2:], data)
	return result, nil
}

// FormatBinary formats bytea value to binary format (raw bytes).
func (f *ByteaFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return toBytes(value)
}

// DateFormatter formats date values.
type DateFormatter struct{}

// FormatText formats a date value to text format (YYYY-MM-DD).
func (f *DateFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		// If not a time, try string passthrough
		if s, ok := value.(string); ok {
			return []byte(s), nil
		}
		return nil, err
	}
	return []byte(t.Format("2006-01-02")), nil
}

// FormatBinary formats a date value to binary format (4 bytes, days since 2000-01-01).
func (f *DateFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		return nil, err
	}
	// PostgreSQL date is days since 2000-01-01
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	days := int32(t.Sub(epoch).Hours() / 24)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(days))
	return buf, nil
}

// TimeFormatter formats time without timezone values.
type TimeFormatter struct{}

// FormatText formats a time value to text format (HH:MM:SS.ssssss).
func (f *TimeFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		if s, ok := value.(string); ok {
			return []byte(s), nil
		}
		return nil, err
	}
	return []byte(t.Format("15:04:05.999999")), nil
}

// FormatBinary formats a time value to binary format (8 bytes, microseconds since midnight).
func (f *TimeFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		return nil, err
	}
	// Microseconds since midnight
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	micros := t.Sub(midnight).Microseconds()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(micros))
	return buf, nil
}

// TimeTZFormatter formats time with timezone values.
type TimeTZFormatter struct{}

// FormatText formats a time with timezone value to text format.
func (f *TimeTZFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		if s, ok := value.(string); ok {
			return []byte(s), nil
		}
		return nil, err
	}
	return []byte(t.Format("15:04:05.999999-07")), nil
}

// FormatBinary formats a time with timezone value to binary format.
func (f *TimeTZFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		return nil, err
	}
	// TimeTZ in binary is 12 bytes: 8 bytes time + 4 bytes zone offset
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	micros := t.Sub(midnight).Microseconds()
	_, offset := t.Zone()

	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(micros))
	// Zone offset is in seconds, negated (PostgreSQL convention)
	binary.BigEndian.PutUint32(buf[8:12], uint32(-offset))
	return buf, nil
}

// TimestampFormatter formats timestamp without timezone values.
type TimestampFormatter struct{}

// FormatText formats a timestamp value to text format.
func (f *TimestampFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		if s, ok := value.(string); ok {
			return []byte(s), nil
		}
		return nil, err
	}
	return []byte(t.Format("2006-01-02 15:04:05.999999")), nil
}

// FormatBinary formats a timestamp value to binary format (8 bytes, microseconds since 2000-01-01).
func (f *TimestampFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		return nil, err
	}
	// PostgreSQL timestamp is microseconds since 2000-01-01 00:00:00
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	micros := t.Sub(epoch).Microseconds()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(micros))
	return buf, nil
}

// TimestampTZFormatter formats timestamp with timezone values.
type TimestampTZFormatter struct{}

// FormatText formats a timestamptz value to text format.
func (f *TimestampTZFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		if s, ok := value.(string); ok {
			return []byte(s), nil
		}
		return nil, err
	}
	return []byte(t.Format("2006-01-02 15:04:05.999999-07")), nil
}

// FormatBinary formats a timestamptz value to binary format.
func (f *TimestampTZFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	t, err := toTime(value)
	if err != nil {
		return nil, err
	}
	// PostgreSQL timestamptz is microseconds since 2000-01-01 00:00:00 UTC
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	micros := t.UTC().Sub(epoch).Microseconds()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(micros))
	return buf, nil
}

// IntervalFormatter formats interval values.
type IntervalFormatter struct{}

// FormatText formats an interval value to text format.
func (f *IntervalFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case time.Duration:
		return formatDuration(v), nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot format %T as interval", value)
	}
}

// FormatBinary formats an interval value to binary format (16 bytes).
func (f *IntervalFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	d, ok := value.(time.Duration)
	if !ok {
		return nil, fmt.Errorf("cannot format %T as interval binary", value)
	}
	// PostgreSQL interval binary: 8 bytes microseconds + 4 bytes days + 4 bytes months
	buf := make([]byte, 16)
	micros := d.Microseconds()
	binary.BigEndian.PutUint64(buf[0:8], uint64(micros))
	// days and months are 0 for a simple duration
	binary.BigEndian.PutUint32(buf[8:12], 0)
	binary.BigEndian.PutUint32(buf[12:16], 0)
	return buf, nil
}

// NumericFormatter formats numeric/decimal values.
type NumericFormatter struct{}

// FormatText formats a numeric value to text format.
func (f *NumericFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, 32)), nil
	case int, int8, int16, int32, int64:
		i, _ := toInt64(v)
		return []byte(strconv.FormatInt(i, 10)), nil
	case uint, uint8, uint16, uint32, uint64:
		u, _ := toUint64(v)
		return []byte(strconv.FormatUint(u, 10)), nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// FormatBinary returns an error as NUMERIC binary format is complex.
func (f *NumericFormatter) FormatBinary(value any) ([]byte, error) {
	// NUMERIC binary format is complex (variable length with digits)
	// For now, we don't support binary encoding
	return nil, errors.New("binary encoding not supported for NUMERIC")
}

// UUIDFormatter formats UUID values.
type UUIDFormatter struct{}

// FormatText formats a UUID value to text format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func (f *UUIDFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case [16]byte:
		return formatUUID(v[:]), nil
	case []byte:
		if len(v) == 16 {
			return formatUUID(v), nil
		}
		return nil, fmt.Errorf("invalid UUID length: %d", len(v))
	default:
		return nil, fmt.Errorf("cannot format %T as UUID", value)
	}
}

// FormatBinary formats a UUID value to binary format (16 bytes).
func (f *UUIDFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case [16]byte:
		return v[:], nil
	case []byte:
		if len(v) == 16 {
			return v, nil
		}
		return nil, fmt.Errorf("invalid UUID length: %d", len(v))
	case string:
		return parseUUID(v)
	default:
		return nil, fmt.Errorf("cannot format %T as UUID binary", value)
	}
}

// JSONFormatter formats JSON and JSONB values.
type JSONFormatter struct{}

// FormatText formats a JSON value to text format.
func (f *JSONFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

// FormatBinary formats a JSON value to binary format.
func (f *JSONFormatter) FormatBinary(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		// JSONB binary format has a version byte prefix
		result := make([]byte, 1+len(v))
		result[0] = 1 // JSONB version 1
		copy(result[1:], v)
		return result, nil
	case []byte:
		result := make([]byte, 1+len(v))
		result[0] = 1 // JSONB version 1
		copy(result[1:], v)
		return result, nil
	default:
		return nil, fmt.Errorf("cannot format %T as JSONB binary", value)
	}
}

// ArrayFormatter formats PostgreSQL array values.
type ArrayFormatter struct {
	elemOid uint32
}

// FormatText formats an array value to PostgreSQL array text format ({value1,value2,...}).
func (f *ArrayFormatter) FormatText(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	// Handle various slice types
	switch v := value.(type) {
	case []any:
		return f.formatAnySlice(v)
	case []int:
		return f.formatIntSlice(v)
	case []int64:
		return f.formatInt64Slice(v)
	case []int32:
		return f.formatInt32Slice(v)
	case []float64:
		return f.formatFloat64Slice(v)
	case []float32:
		return f.formatFloat32Slice(v)
	case []string:
		return f.formatStringSlice(v)
	case []bool:
		return f.formatBoolSlice(v)
	case string:
		// Already formatted array string
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot format %T as array", value)
	}
}

// FormatBinary returns an error as array binary format is complex.
func (f *ArrayFormatter) FormatBinary(value any) ([]byte, error) {
	// PostgreSQL array binary format is complex (header + elements)
	return nil, errors.New("binary encoding not supported for arrays")
}

func (f *ArrayFormatter) formatAnySlice(values []any) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		if v == nil {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(formatArrayElement(v, f.elemOid))
		}
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatIntSlice(values []int) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(v))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatInt64Slice(values []int64) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatInt(v, 10))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatInt32Slice(values []int32) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatFloat64Slice(values []float64) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(string(formatFloatText(v)))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatFloat32Slice(values []float32) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(string(formatFloatText(float64(v))))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatStringSlice(values []string) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(quoteArrayString(v))
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

func (f *ArrayFormatter) formatBoolSlice(values []bool) ([]byte, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		if v {
			sb.WriteByte('t')
		} else {
			sb.WriteByte('f')
		}
	}
	sb.WriteByte('}')
	return []byte(sb.String()), nil
}

// Helper functions

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		i, _ := toInt64(v)
		return i != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		u, _ := toUint64(v)
		return u != 0, nil
	case string:
		lower := strings.ToLower(v)
		switch lower {
		case "t", "true", "yes", "y", "1", "on":
			return true, nil
		case "f", "false", "no", "n", "0", "off":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", v)
		}
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func toUint64(value any) (uint64, error) {
	switch v := value.(type) {
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

func toBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to bytes", value)
	}
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, errors.New("nil time pointer")
		}
		return *v, nil
	case string:
		// Try common formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"15:04:05.999999999",
			"15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time: %q", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", value)
	}
}

func formatFloatText(f float64) []byte {
	if math.IsNaN(f) {
		return []byte("NaN")
	}
	if math.IsInf(f, 1) {
		return []byte("Infinity")
	}
	if math.IsInf(f, -1) {
		return []byte("-Infinity")
	}
	return []byte(strconv.FormatFloat(f, 'g', -1, 64))
}

func formatDuration(d time.Duration) []byte {
	hours := int64(d.Hours())
	minutes := int64(d.Minutes()) % 60
	seconds := int64(d.Seconds()) % 60
	micros := d.Microseconds() % 1000000

	if micros > 0 {
		return []byte(fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros))
	}
	return []byte(fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds))
}

func formatUUID(b []byte) []byte {
	return []byte(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]))
}

func parseUUID(s string) ([]byte, error) {
	// Remove hyphens
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return nil, fmt.Errorf("invalid UUID: %q", s)
	}
	return hex.DecodeString(s)
}

func formatArrayElement(value any, elemOid uint32) string {
	switch v := value.(type) {
	case string:
		return quoteArrayString(v)
	case bool:
		if v {
			return "t"
		}
		return "f"
	case int, int8, int16, int32, int64:
		i, _ := toInt64(v)
		return strconv.FormatInt(i, 10)
	case uint, uint8, uint16, uint32, uint64:
		u, _ := toUint64(v)
		return strconv.FormatUint(u, 10)
	case float32, float64:
		f, _ := toFloat64(v)
		return string(formatFloatText(f))
	default:
		return fmt.Sprintf("%v", value)
	}
}

func quoteArrayString(s string) string {
	// Check if quoting is needed
	needsQuoting := s == ""
	for _, c := range s {
		if c == '"' || c == '\\' || c == '{' || c == '}' || c == ',' || c == ' ' {
			needsQuoting = true

			break
		}
	}
	if !needsQuoting {
		return s
	}

	// Quote and escape
	var sb strings.Builder
	sb.WriteByte('"')
	for _, c := range s {
		if c == '"' || c == '\\' {
			sb.WriteByte('\\')
		}
		sb.WriteRune(c)
	}
	sb.WriteByte('"')
	return sb.String()
}
