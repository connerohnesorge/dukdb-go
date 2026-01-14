package server

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

func TestBoolFormatter_FormatText(t *testing.T) {
	f := &BoolFormatter{}

	tests := []struct {
		input    any
		expected string
	}{
		{true, "t"},
		{false, "f"},
		{1, "t"},
		{0, "f"},
		{int64(1), "t"},
		{int64(0), "f"},
		{"true", "t"},
		{"false", "f"},
		{"t", "t"},
		{"f", "f"},
		{"yes", "t"},
		{"no", "f"},
		{"1", "t"},
		{"0", "f"},
		{"on", "t"},
		{"off", "f"},
	}

	for _, tt := range tests {
		result, err := f.FormatText(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, string(result))
	}
}

func TestBoolFormatter_FormatText_Nil(t *testing.T) {
	f := &BoolFormatter{}
	result, err := f.FormatText(nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBoolFormatter_FormatBinary(t *testing.T) {
	f := &BoolFormatter{}

	result, err := f.FormatBinary(true)
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, result)

	result, err = f.FormatBinary(false)
	require.NoError(t, err)
	assert.Equal(t, []byte{0}, result)
}

func TestIntFormatters_FormatText(t *testing.T) {
	tests := []struct {
		name      string
		formatter ValueFormatter
		input     any
		expected  string
	}{
		{"Int16 from int16", &Int16Formatter{}, int16(42), "42"},
		{"Int16 from int", &Int16Formatter{}, 42, "42"},
		{"Int16 negative", &Int16Formatter{}, int16(-100), "-100"},
		{"Int32 from int32", &Int32Formatter{}, int32(123456), "123456"},
		{"Int32 from int64", &Int32Formatter{}, int64(654321), "654321"},
		{"Int64 from int64", &Int64Formatter{}, int64(9223372036854775807), "9223372036854775807"},
		{"Int64 from string", &Int64Formatter{}, "12345", "12345"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.formatter.FormatText(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestIntFormatters_FormatBinary(t *testing.T) {
	// Int16 binary (big endian)
	f16 := &Int16Formatter{}
	result, err := f16.FormatBinary(int16(0x0102))
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02}, result)

	// Int32 binary (big endian)
	f32 := &Int32Formatter{}
	result, err = f32.FormatBinary(int32(0x01020304))
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, result)

	// Int64 binary (big endian)
	f64 := &Int64Formatter{}
	result, err = f64.FormatBinary(int64(0x0102030405060708))
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, result)
}

func TestFloatFormatters_FormatText(t *testing.T) {
	f32 := &Float32Formatter{}
	f64 := &Float64Formatter{}

	// Normal values
	result, err := f32.FormatText(float32(3.14))
	require.NoError(t, err)
	assert.Contains(t, string(result), "3.14")

	result, err = f64.FormatText(float64(2.718281828))
	require.NoError(t, err)
	assert.Contains(t, string(result), "2.718281828")

	// Special values
	result, err = f64.FormatText(math.NaN())
	require.NoError(t, err)
	assert.Equal(t, "NaN", string(result))

	result, err = f64.FormatText(math.Inf(1))
	require.NoError(t, err)
	assert.Equal(t, "Infinity", string(result))

	result, err = f64.FormatText(math.Inf(-1))
	require.NoError(t, err)
	assert.Equal(t, "-Infinity", string(result))
}

func TestStringFormatter_FormatText(t *testing.T) {
	f := &StringFormatter{}

	tests := []struct {
		input    any
		expected string
	}{
		{"hello world", "hello world"},
		{"", ""},
		{[]byte("bytes"), "bytes"},
		{123, "123"},
	}

	for _, tt := range tests {
		result, err := f.FormatText(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, string(result))
	}
}

func TestByteaFormatter_FormatText(t *testing.T) {
	f := &ByteaFormatter{}

	// Binary data should be hex encoded with \x prefix
	result, err := f.FormatText([]byte{0xde, 0xad, 0xbe, 0xef})
	require.NoError(t, err)
	assert.Equal(t, "\\xdeadbeef", string(result))

	// String input
	result, err = f.FormatText("hello")
	require.NoError(t, err)
	assert.Equal(t, "\\x68656c6c6f", string(result))
}

func TestByteaFormatter_FormatBinary(t *testing.T) {
	f := &ByteaFormatter{}

	data := []byte{0x01, 0x02, 0x03}
	result, err := f.FormatBinary(data)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestDateFormatter_FormatText(t *testing.T) {
	f := &DateFormatter{}

	// time.Time input
	tm := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	result, err := f.FormatText(tm)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15", string(result))

	// String passthrough
	result, err = f.FormatText("2023-12-25")
	require.NoError(t, err)
	assert.Equal(t, "2023-12-25", string(result))
}

func TestTimestampFormatter_FormatText(t *testing.T) {
	f := &TimestampFormatter{}

	tm := time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC)
	result, err := f.FormatText(tm)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15 10:30:45.123456", string(result))

	// Without microseconds
	tm2 := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)
	result, err = f.FormatText(tm2)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15 10:30:45", string(result))
}

func TestTimestampTZFormatter_FormatText(t *testing.T) {
	f := &TimestampTZFormatter{}

	// UTC timezone
	loc, _ := time.LoadLocation("UTC")
	tm := time.Date(2024, 1, 15, 10, 30, 45, 0, loc)
	result, err := f.FormatText(tm)
	require.NoError(t, err)
	assert.Equal(t, "2024-01-15 10:30:45+00", string(result))
}

func TestIntervalFormatter_FormatText(t *testing.T) {
	f := &IntervalFormatter{}

	// Duration input
	d := 3*time.Hour + 25*time.Minute + 10*time.Second
	result, err := f.FormatText(d)
	require.NoError(t, err)
	assert.Equal(t, "03:25:10", string(result))

	// Duration with microseconds
	d2 := 1*time.Hour + 2*time.Minute + 3*time.Second + 456789*time.Microsecond
	result, err = f.FormatText(d2)
	require.NoError(t, err)
	assert.Equal(t, "01:02:03.456789", string(result))

	// String passthrough
	result, err = f.FormatText("1 day 2 hours")
	require.NoError(t, err)
	assert.Equal(t, "1 day 2 hours", string(result))
}

func TestUUIDFormatter_FormatText(t *testing.T) {
	f := &UUIDFormatter{}

	// String input
	result, err := f.FormatText("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))

	// [16]byte input
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	result, err = f.FormatText(uuid)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))

	// []byte input
	result, err = f.FormatText(uuid[:])
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", string(result))
}

func TestUUIDFormatter_FormatBinary(t *testing.T) {
	f := &UUIDFormatter{}

	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}

	// [16]byte input
	result, err := f.FormatBinary(uuid)
	require.NoError(t, err)
	assert.Equal(t, uuid[:], result)

	// String input
	result, err = f.FormatBinary("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	assert.Equal(t, uuid[:], result)
}

func TestJSONFormatter_FormatText(t *testing.T) {
	f := &JSONFormatter{}

	// String input
	result, err := f.FormatText(`{"key": "value"}`)
	require.NoError(t, err)
	assert.Equal(t, `{"key": "value"}`, string(result))

	// []byte input
	result, err = f.FormatText([]byte(`{"array": [1, 2, 3]}`))
	require.NoError(t, err)
	assert.Equal(t, `{"array": [1, 2, 3]}`, string(result))
}

func TestJSONFormatter_FormatBinary(t *testing.T) {
	f := &JSONFormatter{}

	// JSONB binary format has version byte prefix
	result, err := f.FormatBinary(`{"key": "value"}`)
	require.NoError(t, err)
	assert.Equal(t, byte(1), result[0]) // Version byte
	assert.Equal(t, `{"key": "value"}`, string(result[1:]))
}

func TestArrayFormatter_FormatText(t *testing.T) {
	// Integer array
	intFormatter := &ArrayFormatter{elemOid: types.OID_INT4}
	result, err := intFormatter.FormatText([]int{1, 2, 3, 4, 5})
	require.NoError(t, err)
	assert.Equal(t, "{1,2,3,4,5}", string(result))

	// Int64 array
	result, err = intFormatter.FormatText([]int64{10, 20, 30})
	require.NoError(t, err)
	assert.Equal(t, "{10,20,30}", string(result))

	// Float array
	floatFormatter := &ArrayFormatter{elemOid: types.OID_FLOAT8}
	result, err = floatFormatter.FormatText([]float64{1.1, 2.2, 3.3})
	require.NoError(t, err)
	assert.Equal(t, "{1.1,2.2,3.3}", string(result))

	// String array
	textFormatter := &ArrayFormatter{elemOid: types.OID_TEXT}
	result, err = textFormatter.FormatText([]string{"hello", "world"})
	require.NoError(t, err)
	assert.Equal(t, "{hello,world}", string(result))

	// String array with special characters (needs quoting)
	result, err = textFormatter.FormatText([]string{"hello world", "a,b", "c\"d"})
	require.NoError(t, err)
	assert.Equal(t, `{"hello world","a,b","c\"d"}`, string(result))

	// Bool array
	boolFormatter := &ArrayFormatter{elemOid: types.OID_BOOL}
	result, err = boolFormatter.FormatText([]bool{true, false, true})
	require.NoError(t, err)
	assert.Equal(t, "{t,f,t}", string(result))

	// Mixed any array
	result, err = intFormatter.FormatText([]any{1, 2, nil, 4})
	require.NoError(t, err)
	assert.Equal(t, "{1,2,NULL,4}", string(result))
}

func TestArrayFormatter_EmptyArray(t *testing.T) {
	f := &ArrayFormatter{elemOid: types.OID_INT4}
	result, err := f.FormatText(make([]int, 0))
	require.NoError(t, err)
	assert.Equal(t, "{}", string(result))
}

func TestArrayFormatter_StringPassthrough(t *testing.T) {
	f := &ArrayFormatter{elemOid: types.OID_INT4}
	// Already formatted array string should pass through
	result, err := f.FormatText("{1,2,3}")
	require.NoError(t, err)
	assert.Equal(t, "{1,2,3}", string(result))
}

func TestFormatterRegistry_Get(t *testing.T) {
	registry := NewFormatterRegistry()

	// Known types should return specific formatters
	assert.IsType(t, &BoolFormatter{}, registry.Get(types.OID_BOOL))
	assert.IsType(t, &Int16Formatter{}, registry.Get(types.OID_INT2))
	assert.IsType(t, &Int32Formatter{}, registry.Get(types.OID_INT4))
	assert.IsType(t, &Int64Formatter{}, registry.Get(types.OID_INT8))
	assert.IsType(t, &Float32Formatter{}, registry.Get(types.OID_FLOAT4))
	assert.IsType(t, &Float64Formatter{}, registry.Get(types.OID_FLOAT8))
	assert.IsType(t, &StringFormatter{}, registry.Get(types.OID_TEXT))
	assert.IsType(t, &StringFormatter{}, registry.Get(types.OID_VARCHAR))
	assert.IsType(t, &ByteaFormatter{}, registry.Get(types.OID_BYTEA))
	assert.IsType(t, &UUIDFormatter{}, registry.Get(types.OID_UUID))
	assert.IsType(t, &JSONFormatter{}, registry.Get(types.OID_JSON))
	assert.IsType(t, &JSONFormatter{}, registry.Get(types.OID_JSONB))
	assert.IsType(t, &ArrayFormatter{}, registry.Get(types.OID_INT4_ARRAY))

	// Unknown type should return StringFormatter
	assert.IsType(t, &StringFormatter{}, registry.Get(99999))
}

func TestFormatterRegistry_FormatValue(t *testing.T) {
	registry := NewFormatterRegistry()

	// Text format
	result, err := registry.FormatValue(42, types.OID_INT4, false)
	require.NoError(t, err)
	assert.Equal(t, "42", string(result))

	// Binary format
	result, err = registry.FormatValue(int32(42), types.OID_INT4, true)
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 42}, result)

	// NULL value
	result, err = registry.FormatValue(nil, types.OID_INT4, false)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestFormatterRegistry_Register(t *testing.T) {
	registry := NewFormatterRegistry()

	// Create a custom formatter
	customFormatter := &StringFormatter{}

	// Register it for a custom OID
	registry.Register(99999, customFormatter)

	// Should now return our custom formatter
	assert.Equal(t, customFormatter, registry.Get(99999))
}

func TestFormatValue_GlobalFunction(t *testing.T) {
	// Test the convenience function
	result, err := FormatValue(true, types.OID_BOOL, false)
	require.NoError(t, err)
	assert.Equal(t, "t", string(result))

	result, err = FormatValue("hello", types.OID_TEXT, false)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(result))
}

func TestGetFormatterRegistry(t *testing.T) {
	// Should return the global registry
	registry := GetFormatterRegistry()
	assert.NotNil(t, registry)

	// Should be the same instance
	assert.Equal(t, defaultFormatterRegistry, registry)
}

func TestNilHandling(t *testing.T) {
	formatters := []ValueFormatter{
		&BoolFormatter{},
		&Int16Formatter{},
		&Int32Formatter{},
		&Int64Formatter{},
		&Float32Formatter{},
		&Float64Formatter{},
		&StringFormatter{},
		&ByteaFormatter{},
		&DateFormatter{},
		&TimeFormatter{},
		&TimeTZFormatter{},
		&TimestampFormatter{},
		&TimestampTZFormatter{},
		&IntervalFormatter{},
		&NumericFormatter{},
		&UUIDFormatter{},
		&JSONFormatter{},
		&ArrayFormatter{elemOid: types.OID_INT4},
	}

	for _, f := range formatters {
		// FormatText should return nil for nil input
		result, err := f.FormatText(nil)
		assert.NoError(t, err, "FormatText should handle nil without error")
		assert.Nil(t, result, "FormatText should return nil for nil input")

		// FormatBinary should return nil for nil input
		result, err = f.FormatBinary(nil)
		if err == nil { // Some formatters don't support binary
			assert.Nil(t, result, "FormatBinary should return nil for nil input")
		}
	}
}

func TestOIDFormatter_FormatText(t *testing.T) {
	f := &OIDFormatter{}

	tests := []struct {
		input    any
		expected string
	}{
		{uint32(12345), "12345"},
		{int32(54321), "54321"},
		{int(100), "100"},
		{int64(999), "999"},
	}

	for _, tt := range tests {
		result, err := f.FormatText(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, string(result))
	}
}

func TestNumericFormatter_FormatText(t *testing.T) {
	f := &NumericFormatter{}

	tests := []struct {
		input    any
		expected string
	}{
		{"123.456", "123.456"},
		{float64(123.456), "123.456"},
		{float32(12.34), "12.34"},
		{int(100), "100"},
		{int64(-999), "-999"},
	}

	for _, tt := range tests {
		result, err := f.FormatText(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, string(result))
	}
}

func TestNumericFormatter_FormatBinary_NotSupported(t *testing.T) {
	f := &NumericFormatter{}

	// NUMERIC binary format is complex and not supported
	_, err := f.FormatBinary("123.456")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binary encoding not supported")
}

func TestTimeFormatter_FormatText(t *testing.T) {
	f := &TimeFormatter{}

	tm := time.Date(2024, 1, 1, 14, 30, 45, 123456000, time.UTC)
	result, err := f.FormatText(tm)
	require.NoError(t, err)
	assert.Equal(t, "14:30:45.123456", string(result))

	// String passthrough
	result, err = f.FormatText("15:00:00")
	require.NoError(t, err)
	assert.Equal(t, "15:00:00", string(result))
}

func TestQuoteArrayString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// No quoting needed
		{"simple", "simple"},
		{"hello123", "hello123"},
		// Needs quoting - contains spaces
		{"hello world", `"hello world"`},
		// Needs quoting - contains comma
		{"a,b", `"a,b"`},
		// Needs quoting - contains braces
		{"{value}", `"{value}"`},
		// Needs quoting - contains quotes (escaped)
		{`say "hi"`, `"say \"hi\""`},
		// Needs quoting - contains backslash (escaped)
		{`path\to`, `"path\\to"`},
		// Empty string needs quoting
		{"", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteArrayString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
