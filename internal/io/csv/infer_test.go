package csv

import (
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryParseBoolean(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
		ok       bool
	}{
		// True values
		{"true", true, true},
		{"True", true, true},
		{"TRUE", true, true},
		{"t", true, true},
		{"T", true, true},
		{"yes", true, true},
		{"Yes", true, true},
		{"YES", true, true},
		{"y", true, true},
		{"Y", true, true},
		{"1", true, true},

		// False values
		{"false", false, true},
		{"False", false, true},
		{"FALSE", false, true},
		{"f", false, true},
		{"F", false, true},
		{"no", false, true},
		{"No", false, true},
		{"NO", false, true},
		{"n", false, true},
		{"N", false, true},
		{"0", false, true},

		// Invalid values
		{"maybe", false, false},
		{"2", false, false},
		{"", false, false},
		{"truex", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, ok := TryParseBoolean(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch for %q", tt.input)
			if ok {
				assert.Equal(t, tt.expected, val, "value mismatch for %q", tt.input)
			}
		})
	}
}

func TestTryParseInteger(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		ok       bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"-1", -1, true},
		{"123", 123, true},
		{"-123", -123, true},
		{"2147483647", 2147483647, true},   // Max int32
		{"-2147483648", -2147483648, true}, // Min int32
		{"2147483648", 2147483648, true},   // Max int32 + 1
		{"9223372036854775807", 9223372036854775807, true},
		{"-9223372036854775808", -9223372036854775808, true},
		{"  42  ", 42, true}, // With whitespace

		// Invalid values
		{"", 0, false},
		{"abc", 0, false},
		{"1.5", 0, false},
		{"9223372036854775808", 0, false}, // Overflow
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, ok := TryParseInteger(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch for %q", tt.input)
			if ok {
				assert.Equal(t, tt.expected, val, "value mismatch for %q", tt.input)
			}
		})
	}
}

func TestTryParseDouble(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
		ok       bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"-1", -1, true},
		{"1.5", 1.5, true},
		{"-1.5", -1.5, true},
		{"3.14159", 3.14159, true},
		{"1e10", 1e10, true},
		{"1.5e-10", 1.5e-10, true},
		{"  3.14  ", 3.14, true}, // With whitespace

		// Invalid values
		{"", 0, false},
		{"abc", 0, false},
		{"1.2.3", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, ok := TryParseDouble(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch for %q", tt.input)
			if ok {
				assert.InDelta(t, tt.expected, val, 1e-9, "value mismatch for %q", tt.input)
			}
		})
	}
}

func TestTryParseDate(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Time
		ok       bool
	}{
		// ISO format
		{"2024-01-15", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), true},
		{"2020-12-31", time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC), true},

		// US format
		{"01/15/2024", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), true},
		{"12/31/2020", time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC), true},

		// DD-Mon-YYYY format
		{"15-Jan-2024", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), true},
		{"31-Dec-2020", time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC), true},

		// Invalid values
		{"", time.Time{}, false},
		{"not-a-date", time.Time{}, false},
		{"2024-13-01", time.Time{}, false}, // Invalid month
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, ok := TryParseDate(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch for %q", tt.input)
			if !ok {
				return
			}

			assert.Equal(t, tt.expected.Year(), val.Year())
			assert.Equal(t, tt.expected.Month(), val.Month())
			assert.Equal(t, tt.expected.Day(), val.Day())
		})
	}
}

func TestTryParseTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Time
		ok       bool
	}{
		// RFC3339
		{"2024-01-15T10:30:00Z", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), true},

		// ISO 8601 without timezone
		{"2024-01-15T10:30:00", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), true},

		// SQL format
		{"2024-01-15 10:30:00", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), true},

		// Invalid values
		{"", time.Time{}, false},
		{"not-a-timestamp", time.Time{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, ok := TryParseTimestamp(tt.input)
			assert.Equal(t, tt.ok, ok, "ok mismatch for %q", tt.input)
			if !ok {
				return
			}

			assert.Equal(t, tt.expected.Year(), val.Year())
			assert.Equal(t, tt.expected.Month(), val.Month())
			assert.Equal(t, tt.expected.Day(), val.Day())
			assert.Equal(t, tt.expected.Hour(), val.Hour())
			assert.Equal(t, tt.expected.Minute(), val.Minute())
			assert.Equal(t, tt.expected.Second(), val.Second())
		})
	}
}

func TestNewTypeInferrer(t *testing.T) {
	// Default sample size
	ti := NewTypeInferrer(0)
	assert.Equal(t, DefaultSampleSize, ti.sampleSize)

	// Custom sample size
	ti = NewTypeInferrer(100)
	assert.Equal(t, 100, ti.sampleSize)

	// Negative sample size defaults
	ti = NewTypeInferrer(-1)
	assert.Equal(t, DefaultSampleSize, ti.sampleSize)
}

func TestTypeInferrer_InferTypes(t *testing.T) {
	t.Run("empty samples", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		var samples [][]string
		types := ti.InferTypes(samples)
		assert.Nil(t, types)
	})

	t.Run("empty row", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{{}}
		types := ti.InferTypes(samples)
		assert.Nil(t, types)
	})

	t.Run("single column integers", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1"},
			{"2"},
			{"3"},
			{"100"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	})

	t.Run("single column bigints", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1"},
			{"2"},
			{"9223372036854775807"}, // Larger than int32
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_BIGINT, types[0])
	})

	t.Run("single column doubles", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1.5"},
			{"2.5"},
			{"3.14159"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_DOUBLE, types[0])
	})

	t.Run("single column booleans", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"true"},
			{"false"},
			{"yes"},
			{"no"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_BOOLEAN, types[0])
	})

	t.Run("single column dates", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"2024-01-15"},
			{"2024-02-20"},
			{"2024-12-31"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_DATE, types[0])
	})

	t.Run("single column timestamps", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"2024-01-15T10:30:00Z"},
			{"2024-02-20T15:45:30Z"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_TIMESTAMP, types[0])
	})

	t.Run("single column varchar fallback", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"hello"},
			{"world"},
			{"123abc"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])
	})

	t.Run("mixed integer and string falls back to varchar", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1"},
			{"2"},
			{"three"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])
	})

	t.Run("integers with nulls stay integer", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1"},
			{""}, // NULL
			{"3"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	})

	t.Run("only nulls becomes varchar", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{""},
			{""},
			{""},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])
	})

	t.Run("multiple columns different types", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1", "alice", "3.14", "true", "2024-01-15"},
			{"2", "bob", "2.71", "false", "2024-02-20"},
			{"3", "charlie", "1.41", "yes", "2024-03-25"},
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 5)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])
		assert.Equal(t, dukdb.TYPE_DOUBLE, types[2])
		assert.Equal(t, dukdb.TYPE_BOOLEAN, types[3])
		assert.Equal(t, dukdb.TYPE_DATE, types[4])
	})

	t.Run("sample size limit", func(t *testing.T) {
		// Create inferrer with sample size of 2
		ti := NewTypeInferrer(2)
		samples := [][]string{
			{"1"},
			{"2"},
			{"not a number"}, // This row should be ignored due to sample size
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		// Should be INTEGER because "not a number" is outside sample
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	})

	t.Run("variable row lengths", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1", "a", "x"},
			{"2", "b"}, // Missing third column
			{"3"},      // Missing second and third columns
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 3)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[2])
	})

	t.Run("integer vs double promotion", func(t *testing.T) {
		ti := NewTypeInferrer(1000)
		samples := [][]string{
			{"1"},
			{"2"},
			{"3.5"}, // Forces promotion to double
		}
		types := ti.InferTypes(samples)
		require.Len(t, types, 1)
		assert.Equal(t, dukdb.TYPE_DOUBLE, types[0])
	})
}

func TestConvertValue(t *testing.T) {
	t.Run("boolean conversion", func(t *testing.T) {
		val, ok := ConvertValue("true", dukdb.TYPE_BOOLEAN)
		assert.True(t, ok)
		assert.Equal(t, true, val)

		val2, ok2 := ConvertValue("false", dukdb.TYPE_BOOLEAN)
		assert.True(t, ok2)
		assert.Equal(t, false, val2)

		_, ok3 := ConvertValue("invalid", dukdb.TYPE_BOOLEAN)
		assert.False(t, ok3)
	})

	t.Run("integer conversion", func(t *testing.T) {
		val, ok := ConvertValue("42", dukdb.TYPE_INTEGER)
		assert.True(t, ok)
		assert.Equal(t, int32(42), val)

		val2, ok2 := ConvertValue("-100", dukdb.TYPE_INTEGER)
		assert.True(t, ok2)
		assert.Equal(t, int32(-100), val2)

		_, ok3 := ConvertValue("abc", dukdb.TYPE_INTEGER)
		assert.False(t, ok3)
	})

	t.Run("bigint conversion", func(t *testing.T) {
		val, ok := ConvertValue("9223372036854775807", dukdb.TYPE_BIGINT)
		assert.True(t, ok)
		assert.Equal(t, int64(9223372036854775807), val)
	})

	t.Run("double conversion", func(t *testing.T) {
		val, ok := ConvertValue("3.14159", dukdb.TYPE_DOUBLE)
		assert.True(t, ok)
		assert.InDelta(t, 3.14159, val.(float64), 1e-9)
	})

	t.Run("date conversion", func(t *testing.T) {
		val, ok := ConvertValue("2024-01-15", dukdb.TYPE_DATE)
		assert.True(t, ok)
		tm, ok := val.(time.Time)
		require.True(t, ok)
		assert.Equal(t, 2024, tm.Year())
		assert.Equal(t, time.January, tm.Month())
		assert.Equal(t, 15, tm.Day())
	})

	t.Run("timestamp conversion", func(t *testing.T) {
		val, ok := ConvertValue("2024-01-15T10:30:00Z", dukdb.TYPE_TIMESTAMP)
		assert.True(t, ok)
		tm, ok := val.(time.Time)
		require.True(t, ok)
		assert.Equal(t, 2024, tm.Year())
		assert.Equal(t, time.January, tm.Month())
		assert.Equal(t, 15, tm.Day())
		assert.Equal(t, 10, tm.Hour())
		assert.Equal(t, 30, tm.Minute())
	})

	t.Run("varchar conversion", func(t *testing.T) {
		val, ok := ConvertValue("hello world", dukdb.TYPE_VARCHAR)
		assert.True(t, ok)
		assert.Equal(t, "hello world", val)
	})

	t.Run("empty string is nil", func(t *testing.T) {
		val, ok := ConvertValue("", dukdb.TYPE_INTEGER)
		assert.True(t, ok)
		assert.Nil(t, val)

		val, ok = ConvertValue("  ", dukdb.TYPE_VARCHAR)
		assert.True(t, ok)
		assert.Nil(t, val)
	})
}
