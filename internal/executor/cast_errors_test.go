package executor

import (
	"math"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCastErrorInterface(t *testing.T) {
	err := NewInvalidTextRepresentationError("integer", "abc")

	// Verify the error implements the interface expected by ToPgError
	assert.Equal(t, SQLStateInvalidTextRepresentation, err.GetSQLState())
	assert.Contains(t, err.GetMessage(), "invalid input syntax")
	assert.Contains(t, err.GetMessage(), "integer")
	assert.Contains(t, err.GetMessage(), "abc")
}

func TestCastToBoolean(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expected  bool
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"true bool", true, true, false, ""},
		{"false bool", false, false, false, ""},
		{"int 1", int64(1), true, false, ""},
		{"int 0", int64(0), false, false, ""},
		{"string true", "true", true, false, ""},
		{"string false", "false", false, false, ""},
		{"string t", "t", true, false, ""},
		{"string f", "f", false, false, ""},
		{"string yes", "yes", true, false, ""},
		{"string no", "no", false, false, ""},
		{"string 1", "1", true, false, ""},
		{"string 0", "0", false, false, ""},
		{"string on", "on", true, false, ""},
		{"string off", "off", false, false, ""},

		// Invalid conversions
		{"invalid string", "abc", false, true, SQLStateInvalidTextRepresentation},
		{"invalid string 2", "maybe", false, true, SQLStateInvalidTextRepresentation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := castToBoolean(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCastToInteger(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expected  int32
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"int64", int64(42), 42, false, ""},
		{"int", 42, 42, false, ""},
		{"float64", 42.5, 42, false, ""},
		{"string", "42", 42, false, ""},
		{"string negative", "-42", -42, false, ""},
		{"string with spaces", "  42  ", 42, false, ""},
		{"max int32", int64(math.MaxInt32), math.MaxInt32, false, ""},
		{"min int32", int64(math.MinInt32), math.MinInt32, false, ""},

		// Invalid conversions
		{"invalid string", "abc", 0, true, SQLStateInvalidTextRepresentation},
		{"empty string", "", 0, true, SQLStateInvalidTextRepresentation},
		{"overflow positive", int64(math.MaxInt32 + 1), 0, true, SQLStateNumericValueOutOfRange},
		{"overflow negative", int64(math.MinInt32 - 1), 0, true, SQLStateNumericValueOutOfRange},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := castToInteger(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCastToSmallInt(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expected  int16
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"int64", int64(42), 42, false, ""},
		{"max int16", int64(math.MaxInt16), math.MaxInt16, false, ""},
		{"min int16", int64(math.MinInt16), math.MinInt16, false, ""},

		// Overflow
		{"overflow positive", int64(math.MaxInt16 + 1), 0, true, SQLStateNumericValueOutOfRange},
		{"overflow negative", int64(math.MinInt16 - 1), 0, true, SQLStateNumericValueOutOfRange},
		{"large number", int64(999999), 0, true, SQLStateNumericValueOutOfRange},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := castToSmallInt(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCastToBigInt(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expected  int64
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"int64", int64(42), 42, false, ""},
		{"string", "9223372036854775807", math.MaxInt64, false, ""},
		{"string negative", "-9223372036854775808", math.MinInt64, false, ""},

		// Invalid conversions
		{"invalid string", "abc", 0, true, SQLStateInvalidTextRepresentation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := castToBigInt(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestCastToDouble(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expected  float64
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"float64", 3.14, 3.14, false, ""},
		{"int", 42, 42.0, false, ""},
		{"string", "3.14", 3.14, false, ""},
		{"string infinity", "infinity", math.Inf(1), false, ""},
		{"string -infinity", "-infinity", math.Inf(-1), false, ""},

		// Invalid conversions
		{"invalid string", "abc", 0, true, SQLStateInvalidTextRepresentation},
		{"empty string", "", 0, true, SQLStateInvalidTextRepresentation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := castToDouble(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
				if math.IsInf(tc.expected, 1) {
					assert.True(t, math.IsInf(result, 1), "expected +Inf, got %v", result)
				} else if math.IsInf(tc.expected, -1) {
					assert.True(t, math.IsInf(result, -1), "expected -Inf, got %v", result)
				} else if tc.name == "string nan" {
					assert.True(t, math.IsNaN(result))
				} else {
					assert.Equal(t, tc.expected, result)
				}
			}
		})
	}
}

func TestCastToDate(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"iso format", "2023-01-15", false, ""},
		{"us format", "01/15/2023", false, ""},
		{"compact format", "2023/01/15", false, ""},

		// Invalid conversions
		{"invalid string", "abc", true, SQLStateInvalidDatetimeFormat},
		{"partial date", "2023-01", true, SQLStateInvalidDatetimeFormat},
		{"invalid month", "2023-13-01", true, SQLStateInvalidDatetimeFormat},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := castToDate(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastToTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"iso format", "2023-01-15 10:30:45", false, ""},
		{"iso format with micros", "2023-01-15 10:30:45.123456", false, ""},
		{"rfc3339", "2023-01-15T10:30:45Z", false, ""},

		// Invalid conversions
		{"invalid string", "abc", true, SQLStateInvalidDatetimeFormat},
		{"date only", "2023-01-15", true, SQLStateInvalidDatetimeFormat},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := castToTimestamp(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastToUUID(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectErr bool
		errCode   string
	}{
		// Valid conversions
		{"valid uuid", "550e8400-e29b-41d4-a716-446655440000", false, ""},
		{"uppercase uuid", "550E8400-E29B-41D4-A716-446655440000", false, ""},
		{"uuid without dashes", "550e8400e29b41d4a716446655440000", false, ""},

		// Invalid conversions
		{"invalid string", "abc", true, SQLStateInvalidTextRepresentation},
		{"invalid format", "550e8400-e29b-41d4-a716", true, SQLStateInvalidTextRepresentation},
		{
			"too long",
			"550e8400-e29b-41d4-a716-446655440000-extra",
			true,
			SQLStateInvalidTextRepresentation,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := castToUUID(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError")
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastValueWithValidation(t *testing.T) {
	// Test that castValueWithValidation returns proper errors for various types
	tests := []struct {
		name       string
		input      any
		targetType dukdb.Type
		expectErr  bool
		errCode    string
	}{
		// Integer casts
		{"string to int - valid", "42", dukdb.TYPE_INTEGER, false, ""},
		{
			"string to int - invalid",
			"abc",
			dukdb.TYPE_INTEGER,
			true,
			SQLStateInvalidTextRepresentation,
		},
		{
			"string to int - overflow",
			"999999999999",
			dukdb.TYPE_INTEGER,
			true,
			SQLStateNumericValueOutOfRange,
		},

		// SmallInt casts
		{"string to smallint - valid", "42", dukdb.TYPE_SMALLINT, false, ""},
		{
			"string to smallint - overflow",
			"999999",
			dukdb.TYPE_SMALLINT,
			true,
			SQLStateNumericValueOutOfRange,
		},

		// Boolean casts
		{"string to bool - valid", "true", dukdb.TYPE_BOOLEAN, false, ""},
		{
			"string to bool - invalid",
			"abc",
			dukdb.TYPE_BOOLEAN,
			true,
			SQLStateInvalidTextRepresentation,
		},

		// Double casts
		{"string to double - valid", "3.14", dukdb.TYPE_DOUBLE, false, ""},
		{
			"string to double - invalid",
			"abc",
			dukdb.TYPE_DOUBLE,
			true,
			SQLStateInvalidTextRepresentation,
		},

		// Date casts
		{"string to date - valid", "2023-01-15", dukdb.TYPE_DATE, false, ""},
		{"string to date - invalid", "abc", dukdb.TYPE_DATE, true, SQLStateInvalidDatetimeFormat},

		// Timestamp casts
		{"string to timestamp - valid", "2023-01-15 10:30:45", dukdb.TYPE_TIMESTAMP, false, ""},
		{
			"string to timestamp - invalid",
			"abc",
			dukdb.TYPE_TIMESTAMP,
			true,
			SQLStateInvalidDatetimeFormat,
		},

		// UUID casts
		{
			"string to uuid - valid",
			"550e8400-e29b-41d4-a716-446655440000",
			dukdb.TYPE_UUID,
			false,
			"",
		},
		{
			"string to uuid - invalid",
			"abc",
			dukdb.TYPE_UUID,
			true,
			SQLStateInvalidTextRepresentation,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := castValueWithValidation(tc.input, tc.targetType)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError, got %T", err)
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastErrorMessage(t *testing.T) {
	// Test that error messages are user-friendly and PostgreSQL-compatible
	err := NewInvalidTextRepresentationError("integer", "abc")
	msg := err.Error()

	// Check that the error message contains expected components
	assert.Contains(t, msg, "22P02")                // SQLSTATE code
	assert.Contains(t, msg, "invalid input syntax") // PostgreSQL error message style
	assert.Contains(t, msg, "integer")              // Type name
	assert.Contains(t, msg, "abc")                  // The invalid value

	// Test overflow error
	err = NewNumericOutOfRangeError("smallint", int64(999999))
	msg = err.Error()
	assert.Contains(t, msg, "22003")        // SQLSTATE code
	assert.Contains(t, msg, "out of range") // PostgreSQL error message style
	assert.Contains(t, msg, "smallint")     // Type name
}

func TestIntervalParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		// PostgreSQL verbose format
		{"1 year", "1 year", false},
		{"2 months", "2 months", false},
		{"3 days", "3 days", false},
		{"1 year 2 months", "1 year 2 months", false},
		{"1 hour", "1 hour", false},
		{"30 minutes", "30 minutes", false},
		{"45 seconds", "45 seconds", false},

		// Time format
		{"HH:MM:SS", "01:30:45", false},
		{"HH:MM", "01:30", false},

		// ISO 8601 format
		{"ISO 8601 date", "P1Y2M3D", false},
		{"ISO 8601 time", "PT1H2M3S", false},
		{"ISO 8601 full", "P1Y2M3DT4H5M6S", false},

		// Invalid
		{"invalid", "abc", true},
		{"empty", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseIntervalString(tc.input)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastValueNil(t *testing.T) {
	// Test that nil values pass through without error
	result, err := castValueWithValidation(nil, dukdb.TYPE_INTEGER)
	require.NoError(t, err)
	assert.Nil(t, result)

	result, err = castValueWithValidation(nil, dukdb.TYPE_VARCHAR)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestUnsignedIntegerCasts(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		targetFn  func(any) (any, error)
		expectErr bool
		errCode   string
	}{
		// UTinyInt (0-255)
		{
			"utinyint valid",
			int64(100),
			func(v any) (any, error) { return castToUTinyInt(v) },
			false,
			"",
		},
		{
			"utinyint overflow",
			int64(256),
			func(v any) (any, error) { return castToUTinyInt(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},
		{
			"utinyint negative",
			int64(-1),
			func(v any) (any, error) { return castToUTinyInt(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},

		// USmallInt (0-65535)
		{
			"usmallint valid",
			int64(1000),
			func(v any) (any, error) { return castToUSmallInt(v) },
			false,
			"",
		},
		{
			"usmallint overflow",
			int64(65536),
			func(v any) (any, error) { return castToUSmallInt(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},
		{
			"usmallint negative",
			int64(-1),
			func(v any) (any, error) { return castToUSmallInt(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},

		// UInteger (0-4294967295)
		{
			"uinteger valid",
			int64(1000000),
			func(v any) (any, error) { return castToUInteger(v) },
			false,
			"",
		},
		{
			"uinteger overflow",
			int64(4294967296),
			func(v any) (any, error) { return castToUInteger(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},
		{
			"uinteger negative",
			int64(-1),
			func(v any) (any, error) { return castToUInteger(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},

		// UBigInt (0-18446744073709551615)
		{
			"ubigint valid",
			uint64(1000000000000),
			func(v any) (any, error) { return castToUBigInt(v) },
			false,
			"",
		},
		{
			"ubigint from negative",
			int64(-1),
			func(v any) (any, error) { return castToUBigInt(v) },
			true,
			SQLStateNumericValueOutOfRange,
		},
		{
			"ubigint invalid string",
			"abc",
			func(v any) (any, error) { return castToUBigInt(v) },
			true,
			SQLStateInvalidTextRepresentation,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.targetFn(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError, got %T", err)
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFloatCasts(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectErr bool
		errCode   string
	}{
		// Valid
		{"float32 valid", float64(3.14), false, ""},
		{"from int", int64(42), false, ""},
		{"from string", "3.14", false, ""},
		{"infinity", "infinity", false, ""},

		// Invalid
		{"invalid string", "abc", true, SQLStateInvalidTextRepresentation},
		{"empty string", "", true, SQLStateInvalidTextRepresentation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := castToFloat(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				castErr, ok := err.(*CastError)
				require.True(t, ok, "expected CastError, got %T", err)
				assert.Equal(t, tc.errCode, castErr.SQLState)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCastErrorHasHint(t *testing.T) {
	// Test that some errors include helpful hints
	err := NewInvalidDatetimeFormatError("timestamp", "abc")
	assert.NotEmpty(t, err.Hint)
	assert.Contains(t, strings.ToLower(err.Hint), "format")

	// UUID error should have a hint about format
	_, uuidErr := castToUUID("invalid-uuid")
	castErr, ok := uuidErr.(*CastError)
	require.True(t, ok)
	assert.NotEmpty(t, castErr.Hint)
}
