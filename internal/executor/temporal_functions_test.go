package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestDateToTime(t *testing.T) {
	tests := []struct {
		name     string
		days     int32
		expected time.Time
	}{
		{
			name:     "unix epoch",
			days:     0,
			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "one day after epoch",
			days:     1,
			expected: time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "year 2000",
			days:     10957, // Days from 1970-01-01 to 2000-01-01
			expected: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "negative days before epoch",
			days:     -365,
			expected: time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dateToTime(tt.days)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimestampToTime(t *testing.T) {
	tests := []struct {
		name     string
		micros   int64
		expected time.Time
	}{
		{
			name:     "unix epoch",
			micros:   0,
			expected: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "one second after epoch",
			micros:   1_000_000,
			expected: time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
		},
		{
			name:     "one hour after epoch",
			micros:   3600 * 1_000_000,
			expected: time.Date(1970, 1, 1, 1, 0, 0, 0, time.UTC),
		},
		{
			name:     "with microseconds",
			micros:   1_234_567,
			expected: time.Date(1970, 1, 1, 0, 0, 1, 234567000, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := timestampToTime(tt.micros)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimeToComponents(t *testing.T) {
	tests := []struct {
		name           string
		micros         int64
		expectedHour   int
		expectedMinute int
		expectedSecond int
		expectedFrac   float64
	}{
		{
			name:           "midnight",
			micros:         0,
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			expectedFrac:   0,
		},
		{
			name:           "noon",
			micros:         12 * 3600 * 1_000_000,
			expectedHour:   12,
			expectedMinute: 0,
			expectedSecond: 0,
			expectedFrac:   0,
		},
		{
			name:           "14:30:45.123456",
			micros:         14*3600*1_000_000 + 30*60*1_000_000 + 45*1_000_000 + 123456,
			expectedHour:   14,
			expectedMinute: 30,
			expectedSecond: 45,
			expectedFrac:   0.123456,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hour, minute, second, frac := timeToComponents(tt.micros)
			assert.Equal(t, tt.expectedHour, hour)
			assert.Equal(t, tt.expectedMinute, minute)
			assert.Equal(t, tt.expectedSecond, second)
			assert.InDelta(t, tt.expectedFrac, frac, 0.000001)
		})
	}
}

// =============================================================================
// Date Extraction Function Tests
// =============================================================================

func TestEvalYear(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "DATE input - epoch year",
			args:     []any{int32(0)},
			expected: int32(1970),
		},
		{
			name:     "DATE input - year 2024",
			args:     []any{int32(19724)}, // 2024-01-01
			expected: int32(2024),
		},
		{
			name:     "TIMESTAMP input",
			args:     []any{int64(1704067200000000)}, // 2024-01-01 00:00:00 UTC
			expected: int32(2024),
		},
		{
			name:     "time.Time input",
			args:     []any{time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
			expected: int32(2023),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
		{
			name:     "wrong argument count",
			args:     []any{},
			hasError: true,
		},
		{
			name:     "unsupported type",
			args:     []any{"2024-01-01"},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalYear(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalMonth(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "DATE input - January",
			args:     []any{int32(0)}, // 1970-01-01
			expected: int32(1),
		},
		{
			name:     "DATE input - December",
			args:     []any{int32(334)}, // 1970-12-01
			expected: int32(12),
		},
		{
			name:     "TIMESTAMP input - June",
			args:     []any{int64(1718409600000000)}, // 2024-06-15 00:00:00 UTC
			expected: int32(6),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalMonth(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalDay(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "DATE input - day 1",
			args:     []any{int32(0)}, // 1970-01-01
			expected: int32(1),
		},
		{
			name:     "DATE input - day 15",
			args:     []any{int32(14)}, // 1970-01-15
			expected: int32(15),
		},
		{
			name:     "DATE input - day 31",
			args:     []any{int32(30)}, // 1970-01-31
			expected: int32(31),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalDay(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalHour(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "TIMESTAMP input - hour 14",
			args:     []any{int64(50400000000)}, // 14:00:00 UTC (14 hours in microseconds)
			expected: int32(14),
		},
		{
			name:     "TIME input - hour 10",
			args:     []any{int64(10 * 3600 * 1_000_000)}, // 10:00:00
			expected: int32(10),
		},
		{
			name:     "DATE input - hour always 0",
			args:     []any{int32(100)},
			expected: int32(0),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalHour(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalMinute(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "TIME input - minute 30",
			args:     []any{int64(30 * 60 * 1_000_000)}, // 00:30:00
			expected: int32(30),
		},
		{
			name:     "TIME input - minute 45",
			args:     []any{int64(14*3600*1_000_000 + 45*60*1_000_000)}, // 14:45:00
			expected: int32(45),
		},
		{
			name:     "DATE input - minute always 0",
			args:     []any{int32(100)},
			expected: int32(0),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalMinute(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalSecond(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected float64
		isNil    bool
		hasError bool
	}{
		{
			name:     "TIME input - 45 seconds",
			args:     []any{int64(45 * 1_000_000)}, // 00:00:45
			expected: 45.0,
		},
		{
			name:     "TIME input - 30.5 seconds",
			args:     []any{int64(30*1_000_000 + 500_000)}, // 00:00:30.5
			expected: 30.5,
		},
		{
			name:     "DATE input - second always 0",
			args:     []any{int32(100)},
			expected: 0.0,
		},
		{
			name:  "NULL input",
			args:  []any{nil},
			isNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalSecond(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.isNil {
					assert.Nil(t, result)
				} else {
					assert.InDelta(t, tt.expected, result.(float64), 0.0001)
				}
			}
		})
	}
}

func TestEvalDayOfWeek(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "Thursday (1970-01-01)",
			args:     []any{int32(0)}, // 1970-01-01 was a Thursday
			expected: int32(4),        // Thursday = 4
		},
		{
			name:     "Sunday (1970-01-04)",
			args:     []any{int32(3)}, // 1970-01-04 was a Sunday
			expected: int32(0),        // Sunday = 0
		},
		{
			name:     "Saturday (1970-01-03)",
			args:     []any{int32(2)}, // 1970-01-03 was a Saturday
			expected: int32(6),        // Saturday = 6
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalDayOfWeek(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalDayOfYear(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "January 1st",
			args:     []any{int32(0)}, // 1970-01-01
			expected: int32(1),
		},
		{
			name:     "February 1st",
			args:     []any{int32(31)}, // 1970-02-01
			expected: int32(32),
		},
		{
			name:     "December 31st non-leap year",
			args:     []any{int32(364)}, // 1970-12-31
			expected: int32(365),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalDayOfYear(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalWeek(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "First week of 1970",
			args:     []any{int32(0)}, // 1970-01-01 is ISO week 1
			expected: int32(1),
		},
		{
			name:     "time.Time input",
			args:     []any{time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)}, // Week 2 of 2024
			expected: int32(2),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalWeek(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalQuarter(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "January - Q1",
			args:     []any{int32(0)}, // 1970-01-01
			expected: int32(1),
		},
		{
			name:     "April - Q2",
			args:     []any{int32(90)}, // 1970-04-01
			expected: int32(2),
		},
		{
			name:     "July - Q3",
			args:     []any{int32(181)}, // 1970-07-01
			expected: int32(3),
		},
		{
			name:     "October - Q4",
			args:     []any{int32(273)}, // 1970-10-01
			expected: int32(4),
		},
		{
			name:     "December - Q4",
			args:     []any{int32(334)}, // 1970-12-01
			expected: int32(4),
		},
		{
			name:     "NULL input",
			args:     []any{nil},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalQuarter(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// =============================================================================
// Integration Tests via Executor
// =============================================================================

func TestTemporalFunctionsNullPropagation(t *testing.T) {
	// Test that all extraction functions propagate NULL correctly
	funcs := []struct {
		name string
		fn   func([]any) (any, error)
	}{
		{"YEAR", evalYear},
		{"MONTH", evalMonth},
		{"DAY", evalDay},
		{"HOUR", evalHour},
		{"MINUTE", evalMinute},
		{"SECOND", evalSecond},
		{"DAYOFWEEK", evalDayOfWeek},
		{"DAYOFYEAR", evalDayOfYear},
		{"WEEK", evalWeek},
		{"QUARTER", evalQuarter},
	}

	for _, fn := range funcs {
		t.Run(fn.name+" NULL propagation", func(t *testing.T) {
			result, err := fn.fn([]any{nil})
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	}
}

func TestTemporalFunctionsDateAndTimestampSupport(t *testing.T) {
	// Test that all date extraction functions support both DATE (int32) and TIMESTAMP (int64)
	// Use 2024-06-15 as a reference date
	// Days since epoch for 2024-06-15: 19889
	dateValue := int32(19889)

	// Microseconds since epoch for 2024-06-15 12:30:45 UTC
	timestampValue := int64(1718451045000000)

	tests := []struct {
		fnName       string
		fn           func([]any) (any, error)
		dateExpected any
		tsExpected   any
	}{
		{"YEAR", evalYear, int32(2024), int32(2024)},
		{"MONTH", evalMonth, int32(6), int32(6)},
		{"DAY", evalDay, int32(15), int32(15)},
		{"QUARTER", evalQuarter, int32(2), int32(2)},
	}

	for _, tt := range tests {
		t.Run(tt.fnName+" with DATE", func(t *testing.T) {
			result, err := tt.fn([]any{dateValue})
			require.NoError(t, err)
			assert.Equal(t, tt.dateExpected, result)
		})

		t.Run(tt.fnName+" with TIMESTAMP", func(t *testing.T) {
			result, err := tt.fn([]any{timestampValue})
			require.NoError(t, err)
			assert.Equal(t, tt.tsExpected, result)
		})
	}
}

// =============================================================================
// Date Construction Function Tests
// =============================================================================

func TestEvalMakeDate(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "valid date - epoch",
			args:     []any{int32(1970), int32(1), int32(1)},
			expected: int32(0), // 1970-01-01 = day 0
		},
		{
			name:     "valid date - 2024-06-15",
			args:     []any{int32(2024), int32(6), int32(15)},
			expected: int32(19889), // Days since epoch for 2024-06-15
		},
		{
			name:     "valid date - leap year Feb 29",
			args:     []any{int32(2024), int32(2), int32(29)},
			expected: int32(19782), // 2024-02-29 is valid (leap year)
		},
		{
			name:     "valid date - December 31",
			args:     []any{int32(2023), int32(12), int32(31)},
			expected: int32(19722), // 2023-12-31
		},
		{
			name:     "NULL year",
			args:     []any{nil, int32(6), int32(15)},
			expected: nil,
		},
		{
			name:     "NULL month",
			args:     []any{int32(2024), nil, int32(15)},
			expected: nil,
		},
		{
			name:     "NULL day",
			args:     []any{int32(2024), int32(6), nil},
			expected: nil,
		},
		{
			name:     "invalid month - 0",
			args:     []any{int32(2024), int32(0), int32(15)},
			hasError: true,
		},
		{
			name:     "invalid month - 13",
			args:     []any{int32(2024), int32(13), int32(15)},
			hasError: true,
		},
		{
			name:     "invalid day - 0",
			args:     []any{int32(2024), int32(6), int32(0)},
			hasError: true,
		},
		{
			name:     "invalid day - Feb 30",
			args:     []any{int32(2024), int32(2), int32(30)},
			hasError: true,
		},
		{
			name:     "invalid day - Feb 29 non-leap year",
			args:     []any{int32(2023), int32(2), int32(29)},
			hasError: true,
		},
		{
			name:     "invalid day - April 31",
			args:     []any{int32(2024), int32(4), int32(31)},
			hasError: true,
		},
		{
			name:     "wrong argument count",
			args:     []any{int32(2024), int32(6)},
			hasError: true,
		},
		{
			name:     "float64 arguments (auto-convert)",
			args:     []any{float64(2024), float64(6), float64(15)},
			expected: int32(19889),
		},
		{
			name:     "int64 arguments (auto-convert)",
			args:     []any{int64(2024), int64(6), int64(15)},
			expected: int32(19889),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalMakeDate(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEvalMakeTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		args      []any
		expected  any
		hasError  bool
		tolerance int64 // Allow tolerance for floating point precision
	}{
		{
			name:     "valid timestamp - epoch",
			args:     []any{int32(1970), int32(1), int32(1), int32(0), int32(0), float64(0)},
			expected: int64(0), // Epoch timestamp
		},
		{
			name:     "valid timestamp - 2024-06-15 12:30:45",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(30), float64(45)},
			expected: int64(1718454645000000), // Microseconds for 2024-06-15 12:30:45 UTC
		},
		{
			name:      "valid timestamp - with fractional seconds",
			args:      []any{int32(2024), int32(6), int32(15), int32(12), int32(30), float64(45.123456)},
			expected:  int64(1718454645123456), // Microseconds with fraction
			tolerance: 1,                       // Allow 1 microsecond tolerance for floating point precision
		},
		{
			name:     "NULL year",
			args:     []any{nil, int32(1), int32(1), int32(0), int32(0), float64(0)},
			expected: nil,
		},
		{
			name:     "NULL second",
			args:     []any{int32(1970), int32(1), int32(1), int32(0), int32(0), nil},
			expected: nil,
		},
		{
			name:     "invalid month - 13",
			args:     []any{int32(2024), int32(13), int32(1), int32(0), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid day - Feb 30",
			args:     []any{int32(2024), int32(2), int32(30), int32(0), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid hour - 24",
			args:     []any{int32(2024), int32(6), int32(15), int32(24), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid hour - negative",
			args:     []any{int32(2024), int32(6), int32(15), int32(-1), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid minute - 60",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(60), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid minute - negative",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(-1), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid second - 60",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(30), float64(60)},
			hasError: true,
		},
		{
			name:     "invalid second - negative",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(30), float64(-1)},
			hasError: true,
		},
		{
			name:     "wrong argument count",
			args:     []any{int32(2024), int32(6), int32(15), int32(12), int32(30)},
			hasError: true,
		},
		{
			name:      "valid timestamp - boundary hour 23",
			args:      []any{int32(2024), int32(6), int32(15), int32(23), int32(59), float64(59.999999)},
			expected:  int64(1718495999999999), // Last microsecond of the day
			tolerance: 1,                       // Allow 1 microsecond tolerance
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalMakeTimestamp(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected == nil {
					assert.Nil(t, result)
				} else if tt.tolerance > 0 {
					// Use tolerance for floating point precision
					expected := tt.expected.(int64)
					actual := result.(int64)
					diff := expected - actual
					if diff < 0 {
						diff = -diff
					}
					assert.LessOrEqual(t, diff, tt.tolerance, "timestamp difference exceeds tolerance")
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestEvalMakeTime(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		hasError bool
	}{
		{
			name:     "midnight",
			args:     []any{int32(0), int32(0), float64(0)},
			expected: int64(0), // Midnight = 0 microseconds
		},
		{
			name:     "noon",
			args:     []any{int32(12), int32(0), float64(0)},
			expected: int64(12 * 3600 * 1_000_000), // 12 hours in microseconds
		},
		{
			name:     "14:30:45",
			args:     []any{int32(14), int32(30), float64(45)},
			expected: int64(14*3600*1_000_000 + 30*60*1_000_000 + 45*1_000_000),
		},
		{
			name:     "with fractional seconds",
			args:     []any{int32(14), int32(30), float64(45.123456)},
			expected: int64(14*3600*1_000_000 + 30*60*1_000_000 + 45123456),
		},
		{
			name:     "NULL hour",
			args:     []any{nil, int32(30), float64(45)},
			expected: nil,
		},
		{
			name:     "NULL minute",
			args:     []any{int32(14), nil, float64(45)},
			expected: nil,
		},
		{
			name:     "NULL second",
			args:     []any{int32(14), int32(30), nil},
			expected: nil,
		},
		{
			name:     "invalid hour - 24",
			args:     []any{int32(24), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid hour - negative",
			args:     []any{int32(-1), int32(0), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid minute - 60",
			args:     []any{int32(12), int32(60), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid minute - negative",
			args:     []any{int32(12), int32(-1), float64(0)},
			hasError: true,
		},
		{
			name:     "invalid second - 60",
			args:     []any{int32(12), int32(30), float64(60)},
			hasError: true,
		},
		{
			name:     "invalid second - negative",
			args:     []any{int32(12), int32(30), float64(-1)},
			hasError: true,
		},
		{
			name:     "wrong argument count",
			args:     []any{int32(12), int32(30)},
			hasError: true,
		},
		{
			name:     "boundary - 23:59:59.999999",
			args:     []any{int32(23), int32(59), float64(59.999999)},
			expected: int64(23*3600*1_000_000 + 59*60*1_000_000 + 59999999),
		},
		{
			name:     "boundary - hour 23",
			args:     []any{int32(23), int32(0), float64(0)},
			expected: int64(23 * 3600 * 1_000_000),
		},
		{
			name:     "int64 arguments (auto-convert)",
			args:     []any{int64(14), int64(30), float64(45)},
			expected: int64(14*3600*1_000_000 + 30*60*1_000_000 + 45*1_000_000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evalMakeTime(tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestDateConstructionRoundTrip(t *testing.T) {
	// Test that constructing a date and extracting components gives the same values
	t.Run("MAKE_DATE -> YEAR, MONTH, DAY", func(t *testing.T) {
		// Construct a date
		date, err := evalMakeDate([]any{int32(2024), int32(6), int32(15)})
		require.NoError(t, err)
		require.NotNil(t, date)

		// Extract components
		year, err := evalYear([]any{date})
		require.NoError(t, err)
		assert.Equal(t, int32(2024), year)

		month, err := evalMonth([]any{date})
		require.NoError(t, err)
		assert.Equal(t, int32(6), month)

		day, err := evalDay([]any{date})
		require.NoError(t, err)
		assert.Equal(t, int32(15), day)
	})

	t.Run("MAKE_TIMESTAMP -> YEAR, MONTH, DAY, HOUR, MINUTE, SECOND", func(t *testing.T) {
		// Construct a timestamp
		ts, err := evalMakeTimestamp([]any{int32(2024), int32(6), int32(15), int32(14), int32(30), float64(45.5)})
		require.NoError(t, err)
		require.NotNil(t, ts)

		// Extract components
		year, err := evalYear([]any{ts})
		require.NoError(t, err)
		assert.Equal(t, int32(2024), year)

		month, err := evalMonth([]any{ts})
		require.NoError(t, err)
		assert.Equal(t, int32(6), month)

		day, err := evalDay([]any{ts})
		require.NoError(t, err)
		assert.Equal(t, int32(15), day)

		hour, err := evalHour([]any{ts})
		require.NoError(t, err)
		assert.Equal(t, int32(14), hour)

		minute, err := evalMinute([]any{ts})
		require.NoError(t, err)
		assert.Equal(t, int32(30), minute)

		second, err := evalSecond([]any{ts})
		require.NoError(t, err)
		assert.InDelta(t, 45.5, second.(float64), 0.0001)
	})

	t.Run("MAKE_TIME -> HOUR, MINUTE, SECOND", func(t *testing.T) {
		// Construct a time
		timeVal, err := evalMakeTime([]any{int32(14), int32(30), float64(45.5)})
		require.NoError(t, err)
		require.NotNil(t, timeVal)

		// Extract components (treating as TIME)
		hour, err := evalHour([]any{timeVal})
		require.NoError(t, err)
		assert.Equal(t, int32(14), hour)

		minute, err := evalMinute([]any{timeVal})
		require.NoError(t, err)
		assert.Equal(t, int32(30), minute)

		second, err := evalSecond([]any{timeVal})
		require.NoError(t, err)
		assert.InDelta(t, 45.5, second.(float64), 0.0001)
	})
}

// =============================================================================
// EXTRACT Syntax Tests (via DATE_PART delegation)
// =============================================================================

func TestEvalDatePartForExtract(t *testing.T) {
	// These tests verify that EXTRACT(part FROM source) works by delegating to DATE_PART.
	// DATE_PART returns DOUBLE per SQL standard.

	// Create a timestamp for 2024-06-15 14:30:45 UTC
	// Microseconds since epoch: 1718454645000000
	timestampMicros := int64(1718454645000000)

	tests := []struct {
		name     string
		part     string
		value    any
		expected float64
		isNil    bool
		hasError bool
	}{
		{
			name:     "EXTRACT YEAR from TIMESTAMP",
			part:     "year",
			value:    timestampMicros,
			expected: 2024,
		},
		{
			name:     "EXTRACT MONTH from TIMESTAMP",
			part:     "month",
			value:    timestampMicros,
			expected: 6,
		},
		{
			name:     "EXTRACT DAY from TIMESTAMP",
			part:     "day",
			value:    timestampMicros,
			expected: 15,
		},
		{
			name:     "EXTRACT HOUR from TIMESTAMP",
			part:     "hour",
			value:    timestampMicros,
			expected: 12, // 14:30:45 UTC -> 12 in the timestamp value
		},
		{
			name:     "EXTRACT MINUTE from TIMESTAMP",
			part:     "minute",
			value:    timestampMicros,
			expected: 30,
		},
		{
			name:     "EXTRACT SECOND from TIMESTAMP",
			part:     "second",
			value:    timestampMicros,
			expected: 45,
		},
		{
			name:     "EXTRACT QUARTER from TIMESTAMP",
			part:     "quarter",
			value:    timestampMicros,
			expected: 2, // June is Q2
		},
		{
			name:     "EXTRACT WEEK from TIMESTAMP",
			part:     "week",
			value:    timestampMicros,
			expected: 24, // ISO week 24
		},
		{
			name:     "EXTRACT DAYOFWEEK from TIMESTAMP (dow)",
			part:     "dow",
			value:    timestampMicros,
			expected: 6, // Saturday
		},
		{
			name:     "EXTRACT DAYOFYEAR from TIMESTAMP (doy)",
			part:     "doy",
			value:    timestampMicros,
			expected: 167, // Day 167 of the year
		},
		{
			name:  "EXTRACT from NULL",
			part:  "year",
			value: nil,
			isNil: true,
		},
		{
			name:     "EXTRACT DATE from int32 (days since epoch)",
			part:     "year",
			value:    int32(19889), // 2024-06-15
			expected: 2024,
		},
		{
			name:     "EXTRACT from time.Time",
			part:     "year",
			value:    time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC),
			expected: 2024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []any{tt.part, tt.value}
			result, err := evalDatePart(args)

			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.isNil {
					assert.Nil(t, result)
				} else {
					require.NotNil(t, result)
					assert.InDelta(t, tt.expected, result.(float64), 0.001)
				}
			}
		})
	}
}

func TestDatePartWithDifferentPartNames(t *testing.T) {
	// Test that various part name aliases work
	// This is important for EXTRACT which uses uppercase part names like YEAR, MONTH, etc.
	timestamp := int64(1718454645000000) // 2024-06-15 12:30:45 UTC

	partAliases := []struct {
		primary string
		aliases []string
	}{
		{"year", []string{"years", "y"}},
		{"month", []string{"months", "mon"}},
		{"day", []string{"days", "d"}},
		{"hour", []string{"hours", "h"}},
		{"minute", []string{"minutes", "min"}},
		{"second", []string{"seconds", "s"}},
		{"week", []string{"weeks", "w"}},
		{"quarter", []string{"quarters"}},
		{"dayofweek", []string{"dow"}},
		{"dayofyear", []string{"doy"}},
	}

	for _, pa := range partAliases {
		// Get the expected value using the primary name
		expectedResult, err := evalDatePart([]any{pa.primary, timestamp})
		require.NoError(t, err, "primary part %s should work", pa.primary)

		// Test all aliases produce the same result
		for _, alias := range pa.aliases {
			t.Run(pa.primary+"/"+alias, func(t *testing.T) {
				result, err := evalDatePart([]any{alias, timestamp})
				require.NoError(t, err, "alias %s should work", alias)
				assert.Equal(t, expectedResult, result, "alias %s should return same value as %s", alias, pa.primary)
			})
		}
	}
}
