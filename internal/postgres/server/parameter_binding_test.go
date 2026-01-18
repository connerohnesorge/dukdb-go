package server

import (
	"database/sql/driver"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParameterBinder_BindTextParameters(t *testing.T) {
	tests := []struct {
		name     string
		oid      uint32
		input    string
		expected any
		wantErr  bool
	}{
		// Boolean types
		{"bool true lowercase", OidBool, "true", true, false},
		{"bool true short", OidBool, "t", true, false},
		{"bool true yes", OidBool, "yes", true, false},
		{"bool true on", OidBool, "on", true, false},
		{"bool true 1", OidBool, "1", true, false},
		{"bool false lowercase", OidBool, "false", false, false},
		{"bool false short", OidBool, "f", false, false},
		{"bool false no", OidBool, "no", false, false},
		{"bool false off", OidBool, "off", false, false},
		{"bool false 0", OidBool, "0", false, false},
		{"bool invalid", OidBool, "maybe", false, true},

		// Integer types
		{"int2 positive", OidInt2, "12345", int16(12345), false},
		{"int2 negative", OidInt2, "-12345", int16(-12345), false},
		{"int2 zero", OidInt2, "0", int16(0), false},
		{"int2 max", OidInt2, "32767", int16(32767), false},
		{"int2 min", OidInt2, "-32768", int16(-32768), false},
		{"int2 overflow", OidInt2, "99999", int16(0), true},

		{"int4 positive", OidInt4, "123456789", int32(123456789), false},
		{"int4 negative", OidInt4, "-123456789", int32(-123456789), false},
		{"int4 zero", OidInt4, "0", int32(0), false},

		{"int8 positive", OidInt8, "9223372036854775807", int64(9223372036854775807), false},
		{"int8 negative", OidInt8, "-9223372036854775808", int64(-9223372036854775808), false},
		{"int8 zero", OidInt8, "0", int64(0), false},

		// Float types
		{"float4 positive", OidFloat4, "3.14159", float32(3.14159), false},
		{"float4 negative", OidFloat4, "-3.14159", float32(-3.14159), false},
		{"float4 zero", OidFloat4, "0.0", float32(0.0), false},
		{"float4 scientific", OidFloat4, "1.5e10", float32(1.5e10), false},

		{"float8 positive", OidFloat8, "3.141592653589793", float64(3.141592653589793), false},
		{"float8 negative", OidFloat8, "-3.141592653589793", float64(-3.141592653589793), false},
		{"float8 zero", OidFloat8, "0.0", float64(0.0), false},

		// String types
		{"text simple", OidText, "hello world", "hello world", false},
		{"text empty", OidText, "", "", false},
		{"text unicode", OidText, "hello \u4e16\u754c", "hello \u4e16\u754c", false},

		{"varchar simple", OidVarchar, "hello", "hello", false},
		{"char simple", OidChar, "x", "x", false},

		// Numeric type
		{"numeric integer", OidNumeric, "12345", "12345", false},
		{"numeric decimal", OidNumeric, "12345.67890", "12345.67890", false},
		{"numeric negative", OidNumeric, "-12345.67890", "-12345.67890", false},

		// OID type
		{"oid positive", OidOid, "12345", uint32(12345), false},
		{"oid zero", OidOid, "0", uint32(0), false},
		{"oid max", OidOid, "4294967295", uint32(4294967295), false},

		// UUID type
		{
			"uuid valid",
			OidUUID,
			"550e8400-e29b-41d4-a716-446655440000",
			"550e8400-e29b-41d4-a716-446655440000",
			false,
		},
		{
			"uuid without dashes",
			OidUUID,
			"550e8400e29b41d4a716446655440000",
			"550e8400-e29b-41d4-a716-446655440000",
			false,
		},
		{"uuid invalid length", OidUUID, "550e8400", "", true},

		// JSON types
		{"json object", OidJSON, `{"key": "value"}`, `{"key": "value"}`, false},
		{"json array", OidJSON, `[1, 2, 3]`, `[1, 2, 3]`, false},
		{"jsonb object", OidJSONB, `{"key": "value"}`, `{"key": "value"}`, false},

		// Unknown type (inference)
		{"unknown int", OidUnknown, "42", int32(42), false},
		{"unknown float", OidUnknown, "3.14", float64(3.14), false},
		{"unknown bool true", OidUnknown, "true", true, false},
		{"unknown bool false", OidUnknown, "false", false, false},
		{"unknown string", OidUnknown, "hello", "hello", false},
		{"unknown null", OidUnknown, "null", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := NewParameterBinder([]uint32{tt.oid})
			result, err := binder.bindTextParameter(tt.oid, []byte(tt.input))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParameterBinder_BindTextDateTimes(t *testing.T) {
	tests := []struct {
		name     string
		oid      uint32
		input    string
		validate func(t *testing.T, result any)
		wantErr  bool
	}{
		{
			name:  "date ISO format",
			oid:   OidDate,
			input: "2024-01-15",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 2024, dt.Year())
				assert.Equal(t, time.January, dt.Month())
				assert.Equal(t, 15, dt.Day())
			},
		},
		{
			name:  "date US format",
			oid:   OidDate,
			input: "01/15/2024",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 2024, dt.Year())
				assert.Equal(t, time.January, dt.Month())
				assert.Equal(t, 15, dt.Day())
			},
		},
		{
			name:  "time simple",
			oid:   OidTime,
			input: "14:30:45",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 14, dt.Hour())
				assert.Equal(t, 30, dt.Minute())
				assert.Equal(t, 45, dt.Second())
			},
		},
		{
			name:  "time with microseconds",
			oid:   OidTime,
			input: "14:30:45.123456",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 14, dt.Hour())
				assert.Equal(t, 30, dt.Minute())
				assert.Equal(t, 45, dt.Second())
			},
		},
		{
			name:  "timestamp ISO format",
			oid:   OidTimestamp,
			input: "2024-01-15 14:30:45",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 2024, dt.Year())
				assert.Equal(t, time.January, dt.Month())
				assert.Equal(t, 15, dt.Day())
				assert.Equal(t, 14, dt.Hour())
				assert.Equal(t, 30, dt.Minute())
				assert.Equal(t, 45, dt.Second())
			},
		},
		{
			name:  "timestamptz RFC3339",
			oid:   OidTimestampTZ,
			input: "2024-01-15T14:30:45Z",
			validate: func(t *testing.T, result any) {
				dt := result.(time.Time)
				assert.Equal(t, 2024, dt.Year())
				assert.Equal(t, time.January, dt.Month())
				assert.Equal(t, 15, dt.Day())
			},
		},
		{
			name:    "date invalid",
			oid:     OidDate,
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:    "time invalid",
			oid:     OidTime,
			input:   "not-a-time",
			wantErr: true,
		},
		{
			name:  "interval simple",
			oid:   OidInterval,
			input: "1 day 2 hours",
			validate: func(t *testing.T, result any) {
				assert.Equal(t, "1 day 2 hours", result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := NewParameterBinder([]uint32{tt.oid})
			result, err := binder.bindTextParameter(tt.oid, []byte(tt.input))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func TestParameterBinder_BindTextBytea(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		wantErr  bool
	}{
		{
			name:     "hex format simple",
			input:    "\\x48656c6c6f",
			expected: []byte("Hello"),
		},
		{
			name:     "hex format uppercase",
			input:    "\\X48454C4C4F",
			expected: []byte("HELLO"),
		},
		{
			name:     "escape format simple",
			input:    "Hello",
			expected: []byte("Hello"),
		},
		{
			name:     "escape format with backslash",
			input:    "Hello\\\\World",
			expected: []byte("Hello\\World"),
		},
		{
			name:     "escape format with octal",
			input:    "\\110\\145\\154\\154\\157",
			expected: []byte("Hello"),
		},
		{
			name:     "empty",
			input:    "",
			expected: []byte{},
		},
		{
			name:    "hex invalid length",
			input:   "\\x123",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := NewParameterBinder([]uint32{OidBytea})
			result, err := binder.bindTextParameter(OidBytea, []byte(tt.input))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParameterBinder_BindBinaryParameters(t *testing.T) {
	tests := []struct {
		name     string
		oid      uint32
		input    []byte
		expected any
		wantErr  bool
	}{
		// Boolean
		{"bool true", OidBool, []byte{1}, true, false},
		{"bool false", OidBool, []byte{0}, false, false},
		{"bool invalid length", OidBool, []byte{1, 2}, false, true},

		// Integers
		{"int2 positive", OidInt2, encodeInt16(12345), int16(12345), false},
		{"int2 negative", OidInt2, encodeInt16(-12345), int16(-12345), false},
		{"int2 invalid length", OidInt2, []byte{1}, int16(0), true},

		{"int4 positive", OidInt4, encodeInt32(123456789), int32(123456789), false},
		{"int4 negative", OidInt4, encodeInt32(-123456789), int32(-123456789), false},
		{"int4 invalid length", OidInt4, []byte{1, 2}, int32(0), true},

		{
			"int8 positive",
			OidInt8,
			encodeInt64(9223372036854775807),
			int64(9223372036854775807),
			false,
		},
		{
			"int8 negative",
			OidInt8,
			encodeInt64(-9223372036854775808),
			int64(-9223372036854775808),
			false,
		},
		{"int8 invalid length", OidInt8, []byte{1, 2, 3, 4}, int64(0), true},

		// Floats
		{"float4 positive", OidFloat4, encodeFloat32(3.14159), float32(3.14159), false},
		{"float4 negative", OidFloat4, encodeFloat32(-3.14159), float32(-3.14159), false},
		{"float4 invalid length", OidFloat4, []byte{1, 2}, float32(0), true},

		{
			"float8 positive",
			OidFloat8,
			encodeFloat64(3.141592653589793),
			float64(3.141592653589793),
			false,
		},
		{
			"float8 negative",
			OidFloat8,
			encodeFloat64(-3.141592653589793),
			float64(-3.141592653589793),
			false,
		},
		{"float8 invalid length", OidFloat8, []byte{1, 2, 3, 4}, float64(0), true},

		// Strings
		{"text simple", OidText, []byte("hello world"), "hello world", false},
		{"varchar simple", OidVarchar, []byte("hello"), "hello", false},

		// Bytea
		{"bytea simple", OidBytea, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}, false},

		// OID
		{"oid positive", OidOid, encodeUint32(12345), uint32(12345), false},
		{"oid invalid length", OidOid, []byte{1, 2}, uint32(0), true},

		// UUID (16 bytes binary)
		{
			"uuid valid",
			OidUUID,
			[]byte{
				0x55,
				0x0e,
				0x84,
				0x00,
				0xe2,
				0x9b,
				0x41,
				0xd4,
				0xa7,
				0x16,
				0x44,
				0x66,
				0x55,
				0x44,
				0x00,
				0x00,
			},
			"550e8400-e29b-41d4-a716-446655440000",
			false,
		},
		{"uuid invalid length", OidUUID, []byte{0x55, 0x0e, 0x84, 0x00}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binder := NewParameterBinder([]uint32{tt.oid})
			result, err := binder.bindBinaryParameter(tt.oid, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParameterBinder_BindBinaryDateTimes(t *testing.T) {
	// Test binary date/timestamp parsing
	t.Run("binary date", func(t *testing.T) {
		// 8766 days since 2000-01-01 = 2024-01-01
		data := encodeInt32(8766)
		binder := NewParameterBinder([]uint32{OidDate})
		result, err := binder.bindBinaryParameter(OidDate, data)

		require.NoError(t, err)
		dt := result.(time.Time)
		assert.Equal(t, 2024, dt.Year())
		assert.Equal(t, time.January, dt.Month())
		assert.Equal(t, 1, dt.Day())
	})

	t.Run("binary timestamp", func(t *testing.T) {
		// Microseconds since 2000-01-01 for a specific timestamp
		data := encodeInt64(757382400000000) // Some timestamp
		binder := NewParameterBinder([]uint32{OidTimestamp})
		result, err := binder.bindBinaryParameter(OidTimestamp, data)

		require.NoError(t, err)
		_, ok := result.(time.Time)
		assert.True(t, ok)
	})

	t.Run("binary timestamptz", func(t *testing.T) {
		data := encodeInt64(757382400000000)
		binder := NewParameterBinder([]uint32{OidTimestampTZ})
		result, err := binder.bindBinaryParameter(OidTimestampTZ, data)

		require.NoError(t, err)
		dt := result.(time.Time)
		assert.Equal(t, time.UTC, dt.Location())
	})

	t.Run("binary time", func(t *testing.T) {
		// 14:30:45.123456 as microseconds since midnight
		// 14*3600*1e6 + 30*60*1e6 + 45*1e6 + 123456 = 52245123456
		data := encodeInt64(52245123456)
		binder := NewParameterBinder([]uint32{OidTime})
		result, err := binder.bindBinaryParameter(OidTime, data)

		require.NoError(t, err)
		dt := result.(time.Time)
		assert.Equal(t, 14, dt.Hour())
		assert.Equal(t, 30, dt.Minute())
		assert.Equal(t, 45, dt.Second())
		assert.Equal(t, 123456000, dt.Nanosecond()) // Microseconds to nanoseconds
	})

	t.Run("binary time midnight", func(t *testing.T) {
		// 00:00:00 as microseconds since midnight
		data := encodeInt64(0)
		binder := NewParameterBinder([]uint32{OidTime})
		result, err := binder.bindBinaryParameter(OidTime, data)

		require.NoError(t, err)
		dt := result.(time.Time)
		assert.Equal(t, 0, dt.Hour())
		assert.Equal(t, 0, dt.Minute())
		assert.Equal(t, 0, dt.Second())
	})

	t.Run("binary time invalid length", func(t *testing.T) {
		data := []byte{1, 2, 3, 4} // Only 4 bytes, should be 8
		binder := NewParameterBinder([]uint32{OidTime})
		_, err := binder.bindBinaryParameter(OidTime, data)
		assert.Error(t, err)
	})
}

func TestParameterBinder_BindBinaryInterval(t *testing.T) {
	t.Run("interval 1 day", func(t *testing.T) {
		// 0 microseconds, 1 day, 0 months
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 0)   // microseconds
		binary.BigEndian.PutUint32(data[8:12], 1)  // days
		binary.BigEndian.PutUint32(data[12:16], 0) // months

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "1 day", result)
	})

	t.Run("interval 2 days", func(t *testing.T) {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 0)
		binary.BigEndian.PutUint32(data[8:12], 2)
		binary.BigEndian.PutUint32(data[12:16], 0)

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "2 days", result)
	})

	t.Run("interval 1 month", func(t *testing.T) {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 0)
		binary.BigEndian.PutUint32(data[8:12], 0)
		binary.BigEndian.PutUint32(data[12:16], 1)

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "1 mon", result)
	})

	t.Run("interval 1 year 2 months", func(t *testing.T) {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 0)
		binary.BigEndian.PutUint32(data[8:12], 0)
		binary.BigEndian.PutUint32(data[12:16], 14) // 1 year + 2 months

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "1 year 2 mons", result)
	})

	t.Run("interval 2 hours 30 minutes", func(t *testing.T) {
		// 2 hours 30 minutes = 9000 seconds = 9000000000 microseconds
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 9000000000) // 2:30:00
		binary.BigEndian.PutUint32(data[8:12], 0)
		binary.BigEndian.PutUint32(data[12:16], 0)

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "02:30:00", result)
	})

	t.Run("interval complex", func(t *testing.T) {
		// 1 year 2 months 3 days 04:05:06
		data := make([]byte, 16)
		// 4 hours 5 minutes 6 seconds = 14706000000 microseconds
		binary.BigEndian.PutUint64(data[0:8], 14706000000)
		binary.BigEndian.PutUint32(data[8:12], 3)   // days
		binary.BigEndian.PutUint32(data[12:16], 14) // 1 year 2 months

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "1 year 2 mons 3 days 04:05:06", result)
	})

	t.Run("interval zero", func(t *testing.T) {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 0)
		binary.BigEndian.PutUint32(data[8:12], 0)
		binary.BigEndian.PutUint32(data[12:16], 0)

		binder := NewParameterBinder([]uint32{OidInterval})
		result, err := binder.bindBinaryParameter(OidInterval, data)

		require.NoError(t, err)
		assert.Equal(t, "00:00:00", result)
	})

	t.Run("interval invalid length", func(t *testing.T) {
		data := []byte{1, 2, 3, 4} // Only 4 bytes, should be 16
		binder := NewParameterBinder([]uint32{OidInterval})
		_, err := binder.bindBinaryParameter(OidInterval, data)
		assert.Error(t, err)
	})
}

func TestParameterBinder_BindBinaryNumeric(t *testing.T) {
	t.Run("numeric zero", func(t *testing.T) {
		// Zero: ndigits=0, weight=0, sign=0, dscale=0
		data := make([]byte, 8)
		binary.BigEndian.PutUint16(data[0:2], 0) // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0) // weight
		binary.BigEndian.PutUint16(data[4:6], 0) // sign (positive)
		binary.BigEndian.PutUint16(data[6:8], 0) // dscale

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "0", result)
	})

	t.Run("numeric zero with scale", func(t *testing.T) {
		// Zero with 2 decimal places: 0.00
		data := make([]byte, 8)
		binary.BigEndian.PutUint16(data[0:2], 0) // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0) // weight
		binary.BigEndian.PutUint16(data[4:6], 0) // sign
		binary.BigEndian.PutUint16(data[6:8], 2) // dscale

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "0.00", result)
	})

	t.Run("numeric simple integer", func(t *testing.T) {
		// 1234: ndigits=1, weight=0, sign=0, dscale=0, digit=1234
		data := make([]byte, 10)
		binary.BigEndian.PutUint16(data[0:2], 1)     // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0)     // weight
		binary.BigEndian.PutUint16(data[4:6], 0)     // sign (positive)
		binary.BigEndian.PutUint16(data[6:8], 0)     // dscale
		binary.BigEndian.PutUint16(data[8:10], 1234) // digit

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "1234", result)
	})

	t.Run("numeric negative", func(t *testing.T) {
		// -1234: ndigits=1, weight=0, sign=0x4000, dscale=0, digit=1234
		data := make([]byte, 10)
		binary.BigEndian.PutUint16(data[0:2], 1)      // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0)      // weight
		binary.BigEndian.PutUint16(data[4:6], 0x4000) // sign (negative)
		binary.BigEndian.PutUint16(data[6:8], 0)      // dscale
		binary.BigEndian.PutUint16(data[8:10], 1234)  // digit

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "-1234", result)
	})

	t.Run("numeric NaN", func(t *testing.T) {
		// NaN: sign=0xC000
		data := make([]byte, 8)
		binary.BigEndian.PutUint16(data[0:2], 0)      // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0)      // weight
		binary.BigEndian.PutUint16(data[4:6], 0xC000) // sign (NaN)
		binary.BigEndian.PutUint16(data[6:8], 0)      // dscale

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "NaN", result)
	})

	t.Run("numeric large integer", func(t *testing.T) {
		// 12345678: ndigits=2, weight=1, sign=0, dscale=0, digits=[1234, 5678]
		data := make([]byte, 12)
		binary.BigEndian.PutUint16(data[0:2], 2)      // ndigits
		binary.BigEndian.PutUint16(data[2:4], 1)      // weight (first digit is 10000^1)
		binary.BigEndian.PutUint16(data[4:6], 0)      // sign (positive)
		binary.BigEndian.PutUint16(data[6:8], 0)      // dscale
		binary.BigEndian.PutUint16(data[8:10], 1234)  // first digit
		binary.BigEndian.PutUint16(data[10:12], 5678) // second digit

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "12345678", result)
	})

	t.Run("numeric with decimal places", func(t *testing.T) {
		// 123.45: ndigits=2, weight=0, sign=0, dscale=2, digits=[123, 4500]
		data := make([]byte, 12)
		binary.BigEndian.PutUint16(data[0:2], 2)      // ndigits
		binary.BigEndian.PutUint16(data[2:4], 0)      // weight
		binary.BigEndian.PutUint16(data[4:6], 0)      // sign
		binary.BigEndian.PutUint16(data[6:8], 2)      // dscale
		binary.BigEndian.PutUint16(data[8:10], 123)   // first digit
		binary.BigEndian.PutUint16(data[10:12], 4500) // second digit

		binder := NewParameterBinder([]uint32{OidNumeric})
		result, err := binder.bindBinaryParameter(OidNumeric, data)

		require.NoError(t, err)
		assert.Equal(t, "123.45", result)
	})

	t.Run("numeric invalid length too short", func(t *testing.T) {
		data := []byte{1, 2, 3} // Less than 8 bytes
		binder := NewParameterBinder([]uint32{OidNumeric})
		_, err := binder.bindBinaryParameter(OidNumeric, data)
		assert.Error(t, err)
	})

	t.Run("numeric invalid length mismatch", func(t *testing.T) {
		// Claims 2 digits but only provides 1
		data := make([]byte, 10)
		binary.BigEndian.PutUint16(data[0:2], 2) // ndigits=2
		binary.BigEndian.PutUint16(data[2:4], 0)
		binary.BigEndian.PutUint16(data[4:6], 0)
		binary.BigEndian.PutUint16(data[6:8], 0)
		binary.BigEndian.PutUint16(data[8:10], 1234) // only 1 digit

		binder := NewParameterBinder([]uint32{OidNumeric})
		_, err := binder.bindBinaryParameter(OidNumeric, data)
		assert.Error(t, err)
	})
}

func TestParameterBinder_BindBinaryJSON(t *testing.T) {
	t.Run("json object", func(t *testing.T) {
		data := []byte(`{"key": "value"}`)
		binder := NewParameterBinder([]uint32{OidJSON})
		result, err := binder.bindBinaryParameter(OidJSON, data)

		require.NoError(t, err)
		assert.Equal(t, `{"key": "value"}`, result)
	})

	t.Run("jsonb object", func(t *testing.T) {
		data := []byte(`{"key": "value"}`)
		binder := NewParameterBinder([]uint32{OidJSONB})
		result, err := binder.bindBinaryParameter(OidJSONB, data)

		require.NoError(t, err)
		assert.Equal(t, `{"key": "value"}`, result)
	})

	t.Run("json array", func(t *testing.T) {
		data := []byte(`[1, 2, 3]`)
		binder := NewParameterBinder([]uint32{OidJSON})
		result, err := binder.bindBinaryParameter(OidJSON, data)

		require.NoError(t, err)
		assert.Equal(t, `[1, 2, 3]`, result)
	})
}

func TestParameterBinder_NullHandling(t *testing.T) {
	binder := NewParameterBinder([]uint32{OidInt4})

	// Nil value should return nil
	result, err := binder.bindTextParameter(OidInt4, nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	result, err = binder.bindBinaryParameter(OidInt4, nil)
	require.NoError(t, err)
	assert.Nil(t, result)

	// Empty byte slice for text should return nil
	result, err = binder.bindTextParameter(OidInt4, []byte{})
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestConvertStringParams(t *testing.T) {
	tests := []struct {
		name       string
		params     []string
		paramTypes []uint32
		expected   []driver.NamedValue
		wantErr    bool
	}{
		{
			name:       "empty params",
			params:     nil,
			paramTypes: nil,
			expected:   nil,
		},
		{
			name:       "single int with type",
			params:     []string{"42"},
			paramTypes: []uint32{OidInt4},
			expected: []driver.NamedValue{
				{Ordinal: 1, Value: int32(42)},
			},
		},
		{
			name:       "multiple params with types",
			params:     []string{"42", "hello", "true"},
			paramTypes: []uint32{OidInt4, OidText, OidBool},
			expected: []driver.NamedValue{
				{Ordinal: 1, Value: int32(42)},
				{Ordinal: 2, Value: "hello"},
				{Ordinal: 3, Value: true},
			},
		},
		{
			name:       "inferred types",
			params:     []string{"42", "3.14", "hello"},
			paramTypes: nil,
			expected: []driver.NamedValue{
				{Ordinal: 1, Value: int32(42)},
				{Ordinal: 2, Value: float64(3.14)},
				{Ordinal: 3, Value: "hello"},
			},
		},
		{
			name:       "partial types",
			params:     []string{"42", "hello"},
			paramTypes: []uint32{OidInt4},
			expected: []driver.NamedValue{
				{Ordinal: 1, Value: int32(42)},
				{Ordinal: 2, Value: "hello"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertStringParams(tt.params, tt.paramTypes)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				require.Len(t, result, len(tt.expected))
				for i, expected := range tt.expected {
					assert.Equal(t, expected.Ordinal, result[i].Ordinal)
					assert.Equal(t, expected.Value, result[i].Value)
				}
			}
		})
	}
}

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'hello'", "hello"},
		{"\"hello\"", "hello"},
		{"hello", "hello"},
		{"'hello", "'hello"},
		{"hello'", "hello'"},
		{"''", ""},
		{"\"\"", ""},
		{"", ""},
		{"  'hello'  ", "hello"},
		{"  \"hello\"  ", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := StripQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferTextValue(t *testing.T) {
	tests := []struct {
		input    string
		expected any
	}{
		{"null", nil},
		{"NULL", nil},
		{"true", true},
		{"TRUE", true},
		{"t", true},
		{"yes", true},
		{"y", true},
		{"on", true},
		{"false", false},
		{"FALSE", false},
		{"f", false},
		{"no", false},
		{"n", false},
		{"off", false},
		{"42", int32(42)},
		{"-42", int32(-42)},
		{"9999999999", int64(9999999999)},
		{"3.14", float64(3.14)},
		{"-3.14", float64(-3.14)},
		{"hello", "hello"},
		{"hello world", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := inferTextValue(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper functions for creating binary encoded values

func encodeInt16(v int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(v))
	return buf
}

func encodeInt32(v int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	return buf
}

func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

func encodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	return buf
}

func encodeFloat32(v float32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, math.Float32bits(v))
	return buf
}

func encodeFloat64(v float64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return buf
}
