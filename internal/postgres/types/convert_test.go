package types

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTypeConverter(t *testing.T) {
	tc := NewTypeConverter()
	assert.NotNil(t, tc)
	assert.IsType(t, &DefaultTypeConverter{}, tc)
}

func TestGetDefaultConverter(t *testing.T) {
	tc := GetDefaultConverter()
	assert.NotNil(t, tc)
	assert.Equal(t, defaultConverter, tc)
}

// =============================================================================
// NULL Value Tests
// =============================================================================

func TestEncodeText_Nil(t *testing.T) {
	tc := NewTypeConverter()
	result, err := tc.EncodeText(nil, OID_INT4)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestDecodeText_Nil(t *testing.T) {
	tc := NewTypeConverter()
	result, err := tc.DecodeText(nil, OID_INT4)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestEncodeBinary_Nil(t *testing.T) {
	tc := NewTypeConverter()
	result, err := tc.EncodeBinary(nil, OID_INT4)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestDecodeBinary_Nil(t *testing.T) {
	tc := NewTypeConverter()
	result, err := tc.DecodeBinary(nil, OID_INT4)
	require.NoError(t, err)
	assert.Nil(t, result)
}

// =============================================================================
// Boolean Tests
// =============================================================================

func TestEncodeBoolText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"bool true", true, "t"},
		{"bool false", false, "f"},
		{"int 1", 1, "t"},
		{"int 0", 0, "f"},
		{"int -1", -1, "t"},
		{"int8 1", int8(1), "t"},
		{"int8 0", int8(0), "f"},
		{"int16 1", int16(1), "t"},
		{"int16 0", int16(0), "f"},
		{"int32 1", int32(1), "t"},
		{"int32 0", int32(0), "f"},
		{"int64 1", int64(1), "t"},
		{"int64 0", int64(0), "f"},
		{"string true", "true", "t"},
		{"string t", "t", "t"},
		{"string yes", "yes", "t"},
		{"string y", "y", "t"},
		{"string 1", "1", "t"},
		{"string on", "on", "t"},
		{"string false", "false", "f"},
		{"string f", "f", "f"},
		{"string no", "no", "f"},
		{"string n", "n", "f"},
		{"string 0", "0", "f"},
		{"string off", "off", "f"},
		{"string TRUE", "TRUE", "t"},
		{"string FALSE", "FALSE", "f"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, OID_BOOL)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestEncodeBoolText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.EncodeText(struct{}{}, OID_BOOL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeBoolText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"t", "t", true},
		{"true", "true", true},
		{"yes", "yes", true},
		{"y", "y", true},
		{"1", "1", true},
		{"on", "on", true},
		{"f", "f", false},
		{"false", "false", false},
		{"no", "no", false},
		{"n", "n", false},
		{"0", "0", false},
		{"off", "off", false},
		{"T", "T", true},
		{"F", "F", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.DecodeText([]byte(tt.input), OID_BOOL)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeBoolText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeText([]byte("invalid"), OID_BOOL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid boolean")
}

// =============================================================================
// Integer Tests
// =============================================================================

func TestEncodeIntText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		oid      uint32
		expected string
	}{
		{"int", 42, OID_INT4, "42"},
		{"int negative", -42, OID_INT4, "-42"},
		{"int8", int8(127), OID_INT2, "127"},
		{"int16", int16(32767), OID_INT2, "32767"},
		{"int32", int32(2147483647), OID_INT4, "2147483647"},
		{"int64", int64(9223372036854775807), OID_INT8, "9223372036854775807"},
		{"uint", uint(42), OID_INT4, "42"},
		{"uint8", uint8(255), OID_INT2, "255"},
		{"uint16", uint16(65535), OID_INT4, "65535"},
		{"uint32", uint32(4294967295), OID_INT8, "4294967295"},
		{"uint64", uint64(18446744073709551615), OID_INT8, "18446744073709551615"},
		{"float32", float32(42.0), OID_INT4, "42"},
		{"float64", float64(42.0), OID_INT4, "42"},
		{"string", "42", OID_INT4, "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, tt.oid)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestEncodeIntText_Error(t *testing.T) {
	tc := NewTypeConverter()

	_, err := tc.EncodeText("invalid", OID_INT4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid integer")

	_, err = tc.EncodeText(struct{}{}, OID_INT4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeIntText(t *testing.T) {
	tc := NewTypeConverter()

	// INT2
	result, err := tc.DecodeText([]byte("32767"), OID_INT2)
	require.NoError(t, err)
	assert.Equal(t, int16(32767), result)

	// INT4
	result, err = tc.DecodeText([]byte("2147483647"), OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, int32(2147483647), result)

	// INT8
	result, err = tc.DecodeText([]byte("9223372036854775807"), OID_INT8)
	require.NoError(t, err)
	assert.Equal(t, int64(9223372036854775807), result)
}

func TestDecodeIntText_Negative(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeText([]byte("-32768"), OID_INT2)
	require.NoError(t, err)
	assert.Equal(t, int16(-32768), result)

	result, err = tc.DecodeText([]byte("-2147483648"), OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, int32(-2147483648), result)
}

// =============================================================================
// Float Tests
// =============================================================================

func TestEncodeFloatText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"float32", float32(3.14), "3.140000104904175"},
		{"float64", float64(3.14159), "3.14159"},
		{"int", 42, "42"},
		{"int64", int64(42), "42"},
		{"string", "3.14", "3.14"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, OID_FLOAT8)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestEncodeFloatText_SpecialValues(t *testing.T) {
	tc := NewTypeConverter()

	// NaN
	result, err := tc.EncodeText(math.NaN(), OID_FLOAT8)
	require.NoError(t, err)
	assert.Equal(t, "NaN", string(result))

	// +Infinity
	result, err = tc.EncodeText(math.Inf(1), OID_FLOAT8)
	require.NoError(t, err)
	assert.Equal(t, "Infinity", string(result))

	// -Infinity
	result, err = tc.EncodeText(math.Inf(-1), OID_FLOAT8)
	require.NoError(t, err)
	assert.Equal(t, "-Infinity", string(result))
}

func TestEncodeFloatText_Error(t *testing.T) {
	tc := NewTypeConverter()

	_, err := tc.EncodeText("invalid", OID_FLOAT8)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid float")

	_, err = tc.EncodeText(struct{}{}, OID_FLOAT8)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeFloatText(t *testing.T) {
	tc := NewTypeConverter()

	// FLOAT4
	result, err := tc.DecodeText([]byte("3.14"), OID_FLOAT4)
	require.NoError(t, err)
	assert.InDelta(t, float32(3.14), result.(float32), 0.001)

	// FLOAT8
	result, err = tc.DecodeText([]byte("3.14159265358979"), OID_FLOAT8)
	require.NoError(t, err)
	assert.InDelta(t, 3.14159265358979, result.(float64), 0.0000001)
}

func TestDecodeFloatText_SpecialValues(t *testing.T) {
	tc := NewTypeConverter()

	// NaN - float32
	result, err := tc.DecodeText([]byte("NaN"), OID_FLOAT4)
	require.NoError(t, err)
	assert.True(t, math.IsNaN(float64(result.(float32))))

	// NaN - float64
	result, err = tc.DecodeText([]byte("nan"), OID_FLOAT8)
	require.NoError(t, err)
	assert.True(t, math.IsNaN(result.(float64)))

	// Infinity - float32
	result, err = tc.DecodeText([]byte("Infinity"), OID_FLOAT4)
	require.NoError(t, err)
	assert.True(t, math.IsInf(float64(result.(float32)), 1))

	// Infinity - float64
	result, err = tc.DecodeText([]byte("inf"), OID_FLOAT8)
	require.NoError(t, err)
	assert.True(t, math.IsInf(result.(float64), 1))

	// -Infinity
	result, err = tc.DecodeText([]byte("-Infinity"), OID_FLOAT8)
	require.NoError(t, err)
	assert.True(t, math.IsInf(result.(float64), -1))
}

// =============================================================================
// Numeric (DECIMAL) Tests
// =============================================================================

func TestEncodeNumericText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "123.456789", "123.456789"},
		{"float64", 123.456789, "123.456789"},
		{"float32", float32(123.45), "123.45"},
		{"int", 123, "123"},
		{"int64", int64(9223372036854775807), "9223372036854775807"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, OID_NUMERIC)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestDecodeNumericText(t *testing.T) {
	tc := NewTypeConverter()

	// Numeric values are kept as strings for precision
	result, err := tc.DecodeText([]byte("123456789.123456789"), OID_NUMERIC)
	require.NoError(t, err)
	assert.Equal(t, "123456789.123456789", result)
}

// =============================================================================
// String Tests
// =============================================================================

func TestEncodeStringText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		oid      uint32
		expected string
	}{
		{"text string", "hello world", OID_TEXT, "hello world"},
		{"varchar string", "hello", OID_VARCHAR, "hello"},
		{"char string", "h", OID_CHAR, "h"},
		{"bpchar string", "hello", OID_BPCHAR, "hello"},
		{"name string", "table_name", OID_NAME, "table_name"},
		{"bytes", []byte("hello"), OID_TEXT, "hello"},
		{"int as string", 42, OID_TEXT, "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, tt.oid)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestDecodeStringText(t *testing.T) {
	tc := NewTypeConverter()

	for _, oid := range []uint32{OID_TEXT, OID_VARCHAR, OID_CHAR, OID_BPCHAR, OID_NAME} {
		result, err := tc.DecodeText([]byte("hello world"), oid)
		require.NoError(t, err)
		assert.Equal(t, "hello world", result)
	}
}

// =============================================================================
// Bytea Tests
// =============================================================================

func TestEncodeByteaText(t *testing.T) {
	tc := NewTypeConverter()

	// bytes
	result, err := tc.EncodeText([]byte{0xDE, 0xAD, 0xBE, 0xEF}, OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, "\\xdeadbeef", string(result))

	// string
	result, err = tc.EncodeText("hello", OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, "\\x68656c6c6f", string(result))

	// empty bytes
	result, err = tc.EncodeText([]byte{}, OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, "\\x", string(result))
}

func TestEncodeByteaText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.EncodeText(42, OID_BYTEA)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeByteaText(t *testing.T) {
	tc := NewTypeConverter()

	// hex format
	result, err := tc.DecodeText([]byte("\\xdeadbeef"), OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, result)

	// escape format (legacy)
	result, err = tc.DecodeText([]byte("hello"), OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), result)
}

// =============================================================================
// Date/Time Tests
// =============================================================================

func TestEncodeDateText(t *testing.T) {
	tc := NewTypeConverter()

	// time.Time
	tm := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	result, err := tc.EncodeText(tm, OID_DATE)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15", string(result))

	// string passthrough
	result, err = tc.EncodeText("2024-01-15", OID_DATE)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15", string(result))
}

func TestEncodeDateText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.EncodeText(42, OID_DATE)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeDateText(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeText([]byte("2024-01-15"), OID_DATE)
	require.NoError(t, err)
	tm := result.(time.Time)
	assert.Equal(t, 2024, tm.Year())
	assert.Equal(t, time.January, tm.Month())
	assert.Equal(t, 15, tm.Day())
}

func TestEncodeTimeText(t *testing.T) {
	tc := NewTypeConverter()

	// time.Time
	tm := time.Date(0, 1, 1, 14, 30, 45, 123456000, time.UTC)
	result, err := tc.EncodeText(tm, OID_TIME)
	require.NoError(t, err)
	assert.Equal(t, "14:30:45.123456", string(result))

	// string passthrough
	result, err = tc.EncodeText("14:30:45", OID_TIME)
	require.NoError(t, err)
	assert.Equal(t, "14:30:45", string(result))
}

func TestDecodeTimeText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		input    string
		expected string
	}{
		{"14:30:45.123456", "14:30:45"},
		{"14:30:45", "14:30:45"},
	}

	for _, tt := range tests {
		result, err := tc.DecodeText([]byte(tt.input), OID_TIME)
		require.NoError(t, err)
		tm := result.(time.Time)
		assert.Equal(t, 14, tm.Hour())
		assert.Equal(t, 30, tm.Minute())
		assert.Equal(t, 45, tm.Second())
	}
}

func TestDecodeTimeText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeText([]byte("invalid"), OID_TIME)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid time")
}

func TestEncodeTimestampText(t *testing.T) {
	tc := NewTypeConverter()

	tm := time.Date(2024, 1, 15, 14, 30, 45, 123456000, time.UTC)
	result, err := tc.EncodeText(tm, OID_TIMESTAMP)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15 14:30:45.123456", string(result))
}

func TestDecodeTimestampText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []string{
		"2024-01-15 14:30:45.123456",
		"2024-01-15 14:30:45",
		"2024-01-15T14:30:45.123456",
		"2024-01-15T14:30:45",
	}

	for _, input := range tests {
		result, err := tc.DecodeText([]byte(input), OID_TIMESTAMP)
		require.NoError(t, err)
		tm := result.(time.Time)
		assert.Equal(t, 2024, tm.Year())
		assert.Equal(t, time.January, tm.Month())
		assert.Equal(t, 15, tm.Day())
		assert.Equal(t, 14, tm.Hour())
		assert.Equal(t, 30, tm.Minute())
		assert.Equal(t, 45, tm.Second())
	}
}

func TestDecodeTimestampText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeText([]byte("invalid"), OID_TIMESTAMP)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid timestamp")
}

func TestEncodeTimestampTZText(t *testing.T) {
	tc := NewTypeConverter()

	loc := time.FixedZone("UTC-5", -5*60*60)
	tm := time.Date(2024, 1, 15, 14, 30, 45, 123456000, loc)
	result, err := tc.EncodeText(tm, OID_TIMESTAMPTZ)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15 14:30:45.123456-05", string(result))
}

func TestDecodeTimestampTZText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []string{
		"2024-01-15 14:30:45-05",
		"2024-01-15 14:30:45.123456-05",
		"2024-01-15T14:30:45-05:00",
	}

	for _, input := range tests {
		result, err := tc.DecodeText([]byte(input), OID_TIMESTAMPTZ)
		require.NoError(t, err)
		tm := result.(time.Time)
		assert.Equal(t, 2024, tm.Year())
		assert.Equal(t, time.January, tm.Month())
		assert.Equal(t, 15, tm.Day())
	}
}

func TestDecodeTimestampTZText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeText([]byte("invalid"), OID_TIMESTAMPTZ)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid timestamptz")
}

// =============================================================================
// Interval Tests
// =============================================================================

func TestEncodeIntervalText(t *testing.T) {
	tc := NewTypeConverter()

	// Duration
	d := 2*time.Hour + 30*time.Minute + 45*time.Second
	result, err := tc.EncodeText(d, OID_INTERVAL)
	require.NoError(t, err)
	assert.Equal(t, "02:30:45", string(result))

	// Duration with microseconds
	d = 1*time.Hour + 500*time.Microsecond
	result, err = tc.EncodeText(d, OID_INTERVAL)
	require.NoError(t, err)
	assert.Equal(t, "01:00:00.000500", string(result))

	// String passthrough
	result, err = tc.EncodeText("1 day 2 hours", OID_INTERVAL)
	require.NoError(t, err)
	assert.Equal(t, "1 day 2 hours", string(result))
}

func TestEncodeIntervalText_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.EncodeText(42, OID_INTERVAL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

// =============================================================================
// UUID Tests
// =============================================================================

func TestEncodeUUIDText(t *testing.T) {
	tc := NewTypeConverter()

	// string
	result, err := tc.EncodeText("550e8400-e29b-41d4-a716-446655440000", OID_UUID)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))

	// [16]byte
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	result, err = tc.EncodeText(uuid, OID_UUID)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))

	// []byte with length 16
	result, err = tc.EncodeText(uuid[:], OID_UUID)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))
}

func TestEncodeUUIDText_Error(t *testing.T) {
	tc := NewTypeConverter()

	// Invalid length []byte
	_, err := tc.EncodeText([]byte{0x01, 0x02, 0x03}, OID_UUID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID length")

	// Invalid type
	_, err = tc.EncodeText(42, OID_UUID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestDecodeUUIDText(t *testing.T) {
	tc := NewTypeConverter()

	// UUID is kept as string
	result, err := tc.DecodeText([]byte("550e8400-e29b-41d4-a716-446655440000"), OID_UUID)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result)
}

// =============================================================================
// JSON Tests
// =============================================================================

func TestEncodeJSONText(t *testing.T) {
	tc := NewTypeConverter()

	// string
	result, err := tc.EncodeText(`{"key": "value"}`, OID_JSON)
	require.NoError(t, err)
	assert.Equal(t, `{"key": "value"}`, string(result))

	// bytes
	result, err = tc.EncodeText([]byte(`{"key": "value"}`), OID_JSONB)
	require.NoError(t, err)
	assert.Equal(t, `{"key": "value"}`, string(result))
}

func TestDecodeJSONText(t *testing.T) {
	tc := NewTypeConverter()

	// JSON is kept as string
	result, err := tc.DecodeText([]byte(`{"key": "value"}`), OID_JSON)
	require.NoError(t, err)
	assert.Equal(t, `{"key": "value"}`, result)

	result, err = tc.DecodeText([]byte(`{"key": "value"}`), OID_JSONB)
	require.NoError(t, err)
	assert.Equal(t, `{"key": "value"}`, result)
}

// =============================================================================
// OID Type Tests
// =============================================================================

func TestEncodeOIDText(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"uint32", uint32(12345), "12345"},
		{"int32", int32(12345), "12345"},
		{"int", 12345, "12345"},
		{"int64", int64(12345), "12345"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeText(tt.value, OID_OID)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

// =============================================================================
// Default/Unknown Type Tests
// =============================================================================

func TestEncodeUnknownType(t *testing.T) {
	tc := NewTypeConverter()

	// Unknown type defaults to string representation
	result, err := tc.EncodeText("some value", OID_UNKNOWN)
	require.NoError(t, err)
	assert.Equal(t, "some value", string(result))

	result, err = tc.EncodeText(42, OID_UNKNOWN)
	require.NoError(t, err)
	assert.Equal(t, "42", string(result))
}

func TestDecodeUnknownType(t *testing.T) {
	tc := NewTypeConverter()

	// Unknown type defaults to string
	result, err := tc.DecodeText([]byte("some value"), OID_UNKNOWN)
	require.NoError(t, err)
	assert.Equal(t, "some value", result)
}

// =============================================================================
// Binary Encoding Tests
// =============================================================================

func TestEncodeBinaryInt16(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected []byte
	}{
		{"int16", int16(12345), []byte{0x30, 0x39}},
		{"int16 negative", int16(-1), []byte{0xff, 0xff}},
		{"int", 12345, []byte{0x30, 0x39}},
		{"int64", int64(12345), []byte{0x30, 0x39}},
		{"int32", int32(12345), []byte{0x30, 0x39}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeBinary(tt.value, OID_INT2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeBinaryInt16_Error(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.EncodeBinary("invalid", OID_INT2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot encode")
}

func TestEncodeBinaryInt32(t *testing.T) {
	tc := NewTypeConverter()

	tests := []struct {
		name     string
		value    any
		expected []byte
	}{
		{"int32", int32(305419896), []byte{0x12, 0x34, 0x56, 0x78}},
		{"int32 negative", int32(-1), []byte{0xff, 0xff, 0xff, 0xff}},
		{"int", 305419896, []byte{0x12, 0x34, 0x56, 0x78}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tc.EncodeBinary(tt.value, OID_INT4)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeBinaryInt64(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.EncodeBinary(int64(1311768467463790320), OID_INT8)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}, result)
}

func TestEncodeBinaryFloat32(t *testing.T) {
	tc := NewTypeConverter()

	// float32(3.14) in big-endian
	result, err := tc.EncodeBinary(float32(3.14), OID_FLOAT4)
	require.NoError(t, err)
	assert.Len(t, result, 4)

	// Decode and verify
	decoded, err := tc.DecodeBinary(result, OID_FLOAT4)
	require.NoError(t, err)
	assert.InDelta(t, float32(3.14), decoded.(float32), 0.001)
}

func TestEncodeBinaryFloat64(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.EncodeBinary(float64(3.14159265358979), OID_FLOAT8)
	require.NoError(t, err)
	assert.Len(t, result, 8)

	// Decode and verify
	decoded, err := tc.DecodeBinary(result, OID_FLOAT8)
	require.NoError(t, err)
	assert.InDelta(t, 3.14159265358979, decoded.(float64), 0.0000001)
}

func TestEncodeBinaryBool(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.EncodeBinary(true, OID_BOOL)
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, result)

	result, err = tc.EncodeBinary(false, OID_BOOL)
	require.NoError(t, err)
	assert.Equal(t, []byte{0}, result)
}

func TestEncodeBinaryUnsupportedType(t *testing.T) {
	tc := NewTypeConverter()

	_, err := tc.EncodeBinary("hello", OID_TEXT)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binary encoding not supported")
}

// =============================================================================
// Binary Decoding Tests
// =============================================================================

func TestDecodeBinaryInt16(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeBinary([]byte{0x30, 0x39}, OID_INT2)
	require.NoError(t, err)
	assert.Equal(t, int16(12345), result)

	// Negative
	result, err = tc.DecodeBinary([]byte{0xff, 0xff}, OID_INT2)
	require.NoError(t, err)
	assert.Equal(t, int16(-1), result)
}

func TestDecodeBinaryInt16_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01}, OID_INT2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid int2 binary length")
}

func TestDecodeBinaryInt32(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeBinary([]byte{0x12, 0x34, 0x56, 0x78}, OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, int32(305419896), result)
}

func TestDecodeBinaryInt32_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01, 0x02}, OID_INT4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid int4 binary length")
}

func TestDecodeBinaryInt64(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeBinary([]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}, OID_INT8)
	require.NoError(t, err)
	assert.Equal(t, int64(1311768467463790320), result)
}

func TestDecodeBinaryInt64_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01, 0x02, 0x03, 0x04}, OID_INT8)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid int8 binary length")
}

func TestDecodeBinaryFloat32_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01, 0x02}, OID_FLOAT4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid float4 binary length")
}

func TestDecodeBinaryFloat64_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01, 0x02, 0x03, 0x04}, OID_FLOAT8)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid float8 binary length")
}

func TestDecodeBinaryBool(t *testing.T) {
	tc := NewTypeConverter()

	result, err := tc.DecodeBinary([]byte{1}, OID_BOOL)
	require.NoError(t, err)
	assert.True(t, result.(bool))

	result, err = tc.DecodeBinary([]byte{0}, OID_BOOL)
	require.NoError(t, err)
	assert.False(t, result.(bool))

	// Any non-zero byte is true
	result, err = tc.DecodeBinary([]byte{42}, OID_BOOL)
	require.NoError(t, err)
	assert.True(t, result.(bool))
}

func TestDecodeBinaryBool_InvalidLength(t *testing.T) {
	tc := NewTypeConverter()
	_, err := tc.DecodeBinary([]byte{0x01, 0x02}, OID_BOOL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid bool binary length")
}

func TestDecodeBinaryUnsupportedType(t *testing.T) {
	tc := NewTypeConverter()

	_, err := tc.DecodeBinary([]byte("hello"), OID_TEXT)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binary decoding not supported")
}

// =============================================================================
// Convenience Function Tests
// =============================================================================

func TestEncodeValue(t *testing.T) {
	result, err := EncodeValue(42, OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, "42", string(result))
}

func TestDecodeValue(t *testing.T) {
	result, err := DecodeValue([]byte("42"), OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, int32(42), result)
}

// =============================================================================
// Round-trip Tests
// =============================================================================

func TestRoundTripText_Int(t *testing.T) {
	tc := NewTypeConverter()

	for _, oid := range []uint32{OID_INT2, OID_INT4, OID_INT8} {
		encoded, err := tc.EncodeText(42, oid)
		require.NoError(t, err)

		decoded, err := tc.DecodeText(encoded, oid)
		require.NoError(t, err)

		switch oid {
		case OID_INT2:
			assert.Equal(t, int16(42), decoded)
		case OID_INT4:
			assert.Equal(t, int32(42), decoded)
		case OID_INT8:
			assert.Equal(t, int64(42), decoded)
		}
	}
}

func TestRoundTripText_Float(t *testing.T) {
	tc := NewTypeConverter()

	// float32
	encoded, err := tc.EncodeText(float32(3.14), OID_FLOAT4)
	require.NoError(t, err)
	decoded, err := tc.DecodeText(encoded, OID_FLOAT4)
	require.NoError(t, err)
	assert.InDelta(t, float32(3.14), decoded.(float32), 0.001)

	// float64
	encoded, err = tc.EncodeText(3.14159, OID_FLOAT8)
	require.NoError(t, err)
	decoded, err = tc.DecodeText(encoded, OID_FLOAT8)
	require.NoError(t, err)
	assert.InDelta(t, 3.14159, decoded.(float64), 0.00001)
}

func TestRoundTripText_Bool(t *testing.T) {
	tc := NewTypeConverter()

	for _, v := range []bool{true, false} {
		encoded, err := tc.EncodeText(v, OID_BOOL)
		require.NoError(t, err)

		decoded, err := tc.DecodeText(encoded, OID_BOOL)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
	}
}

func TestRoundTripText_Bytea(t *testing.T) {
	tc := NewTypeConverter()

	original := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	encoded, err := tc.EncodeText(original, OID_BYTEA)
	require.NoError(t, err)

	decoded, err := tc.DecodeText(encoded, OID_BYTEA)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestRoundTripBinary_Int(t *testing.T) {
	tc := NewTypeConverter()

	// int16
	encoded, err := tc.EncodeBinary(int16(12345), OID_INT2)
	require.NoError(t, err)
	decoded, err := tc.DecodeBinary(encoded, OID_INT2)
	require.NoError(t, err)
	assert.Equal(t, int16(12345), decoded)

	// int32
	encoded, err = tc.EncodeBinary(int32(123456789), OID_INT4)
	require.NoError(t, err)
	decoded, err = tc.DecodeBinary(encoded, OID_INT4)
	require.NoError(t, err)
	assert.Equal(t, int32(123456789), decoded)

	// int64
	encoded, err = tc.EncodeBinary(int64(9223372036854775807), OID_INT8)
	require.NoError(t, err)
	decoded, err = tc.DecodeBinary(encoded, OID_INT8)
	require.NoError(t, err)
	assert.Equal(t, int64(9223372036854775807), decoded)
}

func TestRoundTripBinary_Bool(t *testing.T) {
	tc := NewTypeConverter()

	for _, v := range []bool{true, false} {
		encoded, err := tc.EncodeBinary(v, OID_BOOL)
		require.NoError(t, err)

		decoded, err := tc.DecodeBinary(encoded, OID_BOOL)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
	}
}
